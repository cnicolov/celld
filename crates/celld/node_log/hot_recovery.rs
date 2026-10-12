// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;

#[cfg(test)]
#[derive(Default)]
pub(super) struct HotPreparationPause {
    pub entered: tokio::sync::Notify,
    pub resume: tokio::sync::Notify,
}

impl NodeLogManager {
    pub fn enable_hot_recovery(
        &self,
        follower: Arc<FollowerStore>,
        root: &Path,
        max_bytes: u64,
        max_cells: usize,
    ) -> anyhow::Result<()> {
        let hot = Arc::new(crate::hot_recovery::HotStandbys::new(
            root, max_bytes, max_cells,
        )?);
        *follower.hot_standbys.lock().unwrap() = Some(hot.clone());
        *self.hot_follower.lock().unwrap() = Some(follower);
        let ltx = self.ltx.clone();
        let stop = self.task_stop.clone();
        self.child_tasks.spawn_owned("hot_standby_checkpoints", async move {
            loop {
                ltx.prune_hot_promotions();
                let notified = hot.wake.notified();
                if let Some(job) = hot.next_checkpoint() {
                    let header = match celld_ltx::ltx::Header::parse(&job.bytes) {
                        Ok(header) => header,
                        Err(_) => { hot.failed(&job); continue; }
                    };
                    // Publishing past a missing canonical base would raise
                    // the existing GC maximum past a hole, allowing a bundle
                    // holding that base to be discarded. Establish a complete
                    // prefix first; later jobs inherit only successful proofs.
                    if !header.is_snapshot() && hot.checkpoint_floor(&job) < header.min_txid.0 - 1 {
                        let prefix = crate::asyncrt::select_biased! {
                            "stopping a publisher leaves canonical-base proof pending";
                            _ = stop.stopped() => return,
                            prefix = ltx.hot_checkpoint_position(&job.cell, job.epoch) => prefix,
                        };
                        if !matches!(prefix, Ok(end) if end >= header.min_txid.0 - 1) {
                            warn!(cell = job.cell, epoch = job.epoch, required_txid = header.min_txid.0 - 1, ?prefix, "standby checkpoint withheld: canonical baseline is incomplete");
                            hot.failed(&job);
                            continue;
                        }
                    }
                    let upload = ltx.upload_raw_l0(&job.cell, job.epoch, header.min_txid.0, header.max_txid.0, &job.bytes);
                    let result = crate::asyncrt::select_biased! {
                        "stopping a standby publisher preserves its uncredited log witness";
                        _ = stop.stopped() => return,
                        result = upload => result,
                    };
                    if let Err(error) = result {
                        warn!(cell = job.cell, epoch = job.epoch, %error, "standby checkpoint failed; retaining ordinary recovery obligation");
                        hot.failed(&job);
                        continue;
                    }
                    if hot.checkpointed(&job) {
                        let Some(budget) = hot.seed_budget(&job) else { continue };
                        let seed = crate::asyncrt::select_biased! {
                            "a stopped publisher cannot complete a standby seed";
                            _ = stop.stopped() => return,
                            seed = ltx.hot_seed(&job.cell, job.epoch, budget) => seed,
                        };
                        match seed {
                            Ok(Some((snapshot, txid, page_size))) => {
                                let cache = hot.clone();
                                let seeded = crate::asyncrt::blocking(move || {
                                    let result = cache.seed(&job, snapshot.path(), txid, page_size);
                                    if result.is_err() { cache.failed(&job); }
                                    (job.cell, result)
                                }).await;
                                if let Ok((cell, Err(error))) = seeded {
                                    warn!(cell, %error, "standby seed failed");
                                }
                            }
                            Ok(None) => {},
                            Err(error) => warn!(cell = job.cell, %error, "standby baseline not yet readable"),
                        }
                    }
                } else {
                    crate::asyncrt::select_biased! {
                        "a stopped standby publisher admits no further checkpoints";
                        _ = stop.stopped() => return,
                        _ = notified => {},
                        _ = crate::asyncrt::sleep(std::time::Duration::from_secs(5)) => {},
                    }
                }
            }
        });
        Ok(())
    }

    pub async fn ensure_cell_recovered(&self, cell: &str, prior: &str) -> anyhow::Result<()> {
        self.ensure_predecessors_recovered().await?;
        match self.prepare_hot_recovery(cell, prior).await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => {
                warn!(cell, prior, %error, "hot recovery unavailable; recovering predecessor log")
            }
        }
        self.ensure_recovered(Some(prior)).await
    }

    pub(super) async fn prepare_hot_recovery(
        &self,
        cell: &str,
        prior: &str,
    ) -> anyhow::Result<bool> {
        let follower = self.hot_follower.lock().unwrap().clone();
        let Some(follower) = follower else {
            return Ok(false);
        };
        let Some(folded) = read_record(&self.bucket, prior).await? else {
            return Ok(false);
        };
        let session = format!("{prior}/{}", folded.wire.generation);
        // Slow checksum/prefix validation can outlast a core operation deadline.
        // Its retry and the eager whole-session sweep must not start a cold
        // activation underneath this still-running hot preparation.
        let recovery_lock = self.recovery_lock(&session);
        let _single_flight = recovery_lock.lock().await;
        let Some(mut folded) = read_record(&self.bucket, &session).await? else {
            return Ok(false);
        };
        let Some(owner) = self.ownership.read_owner(cell).await? else {
            return Ok(false);
        };
        if owner.node.as_deref() != Some(prior) {
            return Ok(false);
        }
        if folded.wire.expires_ms > crate::ownership_store::now_ms()
            || !folded.record.ensemble.contains(&self.node)
        {
            return Ok(false);
        }
        let source_epoch = folded.record.epoch;
        if self
            .ltx
            .has_hot_promotion(cell, &session, source_epoch, owner.epoch)
        {
            return Ok(true);
        }
        if folded.record.state == LogState::Sealed {
            return Ok(false);
        }
        // Establish the same expired-session fence used by cold recovery.
        // The record remains Recovering until its entire session is drained.
        if folded.record.state == LogState::Open {
            let Some(recovering) = log_tier::start_recovery(
                &folded.record,
                &self.node,
                crate::ownership_store::now_ms(),
            ) else {
                return Ok(false);
            };
            if write_dead_record(
                &self.bucket,
                &session,
                &folded.wire,
                &recovering,
                folded.may_have_fleet_acks,
                &folded.token,
            )
            .await?
            .is_none()
            {
                return Ok(false);
            }
            let Some(current) = read_record(&self.bucket, &session).await? else {
                return Ok(false);
            };
            folded = current;
        }
        let Some(promotion) = follower
            .prepare_hot(&session, cell, owner.epoch, source_epoch)
            .await?
        else {
            return Ok(false);
        };
        #[cfg(test)]
        let pause = self.hot_preparation_pause.lock().unwrap().take();
        #[cfg(test)]
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause.resume.notified().await;
        }
        // Bucket-only writes can outrun a follower. A current canonical prefix
        // above the image makes it stale, never silently eligible.
        let covered = self.ltx.hot_checkpoint_position(cell, owner.epoch).await?;
        if covered != promotion.txid {
            return Ok(false);
        }
        let bundles = self.bucket.list(&format!("log/{session}/bundle/")).await?;
        if bundles.len() > 128 {
            return Ok(false);
        }
        let mut footer_budget = 8 * 1024 * 1024_u64;
        for meta in bundles {
            // A retained bundle can be hundreds of MiB. Eligibility needs its
            // membership footer, not its payload; pin both ranged reads to
            // the listed ETag and fall back on missing/oversized metadata.
            if meta.size < 8 || meta.e_tag.is_none() {
                return Ok(false);
            }
            let read = |range| {
                self.bucket.store.get_opts(
                    &meta.location,
                    object_store::GetOptions {
                        range: Some(object_store::GetRange::Bounded(range)),
                        if_match: meta.e_tag.clone(),
                        ..Default::default()
                    },
                )
            };
            let trailer = read(meta.size - 8..meta.size).await?.bytes().await?;
            anyhow::ensure!(
                trailer.len() == 8 && &trailer[4..] == b"CLB1",
                "unreadable standby bundle footer"
            );
            let len = u64::from(u32::from_le_bytes(trailer[..4].try_into().unwrap()));
            if len > footer_budget || len > 1024 * 1024 || len > meta.size - 8 {
                return Ok(false);
            }
            footer_budget -= len;
            let data_len = meta.size - 8 - len;
            let footer = read(data_len..meta.size).await?.bytes().await?;
            anyhow::ensure!(
                footer.len() as u64 == len + 8,
                "short standby bundle footer"
            );
            for row in celld_ltx::bundle::decode_rows(&footer)? {
                anyhow::ensure!(
                    row.offset
                        .checked_add(row.len)
                        .is_some_and(|end| end <= data_len),
                    "standby bundle row overruns its payload"
                );
                if row.cell == cell
                    && (row.cell_epoch > owner.epoch
                        || (row.cell_epoch == owner.epoch && row.txid > promotion.txid))
                {
                    return Ok(false);
                }
            }
        }
        let Some(current) = read_record(&self.bucket, prior).await? else {
            return Ok(false);
        };
        if current.wire.generation != folded.wire.generation
            || current.record.epoch != source_epoch
            || current.wire.expires_ms > crate::ownership_store::now_ms()
        {
            return Ok(false);
        }
        info!(
            event = "hot_recovery_certified",
            cell,
            session,
            cell_epoch = owner.epoch,
            txid = promotion.txid,
            "canonical checkpoint and sealed local standby certify this cell"
        );
        self.ltx.install_hot_promotion(cell, promotion);
        Ok(true)
    }
}

impl FollowerStore {
    pub fn hot_recovery_status(&self) -> serde_json::Value {
        self.hot_standbys.lock().unwrap().as_ref().map_or_else(
            || serde_json::json!({ "enabled": false, "candidates": [] }),
            |hot| hot.status(),
        )
    }

    async fn prepare_hot(
        &self,
        session: &str,
        cell: &str,
        cell_epoch: u64,
        log_epoch: u64,
    ) -> anyhow::Result<Option<crate::hot_recovery::Promotion>> {
        let guard = self.guard(session);
        let _held = guard.lock().await;
        let state = self.load(session)?;
        if state.fragment_epoch != log_epoch || !self.recovered_disk_range_complete(session) {
            return Ok(None);
        }
        let hot = self.hot_standbys.lock().unwrap().clone();
        let Some(hot) = hot else { return Ok(None) };
        let Some(promotion) = hot.take(session, cell, cell_epoch, state) else {
            return Ok(None);
        };
        self.persist(
            session,
            FollowerState {
                sealed_to: state.sealed_to.max(log_epoch),
                ..state
            },
        )?;
        Ok(Some(
            crate::asyncrt::blocking(move || {
                promotion.verify()?;
                anyhow::Ok(promotion)
            })
            .await??,
        ))
    }
}
