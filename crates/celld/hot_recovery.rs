// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Disposable, continuously materialized follower SQLite images. The peer log
//! remains the durability witness. Images are trusted only in the process that
//! applied them; restart discards this cache. Canonical per-cell checkpoints
//! must cover an image before it can authorize a cell-scoped takeover.

use crate::node_log::{Entry, FollowerState};
use anyhow::{Context, Result};
use celld_ltx::{ltx, CHECKSUM_FLAG};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

type Key = (String, String, u64);

struct Image {
    serial: u64,
    path: PathBuf,
    txid: u64,
    checksum: u64,
    page_size: u32,
    commit: u32,
    expected: u64,
    covered: u64,
    bytes: u64,
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use celld_ltx::TXID;

    fn entry(cell: &str, seq: u64, txid: u64, min: u64, commit: u32, pre: u64) -> Entry {
        let pages: Vec<_> = (1..=commit)
            .map(|pgno| (pgno, vec![txid as u8; 4096]))
            .collect();
        let post = pages.iter().fold(CHECKSUM_FLAG, |checksum, (pgno, page)| {
            CHECKSUM_FLAG | (checksum ^ ltx::checksum_page(*pgno, page))
        });
        Entry {
            seq,
            cell: cell.into(),
            cell_epoch: 1,
            txid,
            bytes: ltx::encode_file(
                &ltx::Header {
                    version: ltx::VERSION,
                    page_size: 4096,
                    commit,
                    min_txid: TXID(min),
                    max_txid: TXID(txid),
                    pre_apply_checksum: pre,
                    ..Default::default()
                },
                &pages,
                post,
            )
            .unwrap(),
        }
    }

    fn range(end: u64) -> FollowerState {
        FollowerState {
            fragment_epoch: 1,
            end,
            ..Default::default()
        }
    }

    fn covered(cache: &HotStandbys) {
        while let Some(job) = cache.next_checkpoint() {
            cache.checkpointed(&job);
        }
    }

    #[test]
    fn hot_recovery_requires_checkpoint_coverage_and_a_complete_observed_range() {
        crate::test_runtime::init();
        let root = tempfile::tempdir().unwrap();
        let cache = Arc::new(HotStandbys::new(root.path(), 16384, 4).unwrap());
        cache.observe(
            "owner/g",
            range(0),
            range(1),
            &[entry("cell", 1, 1, 1, 1, 0)],
        );
        assert!(cache.take("owner/g", "cell", 1, range(1)).is_none());
        covered(&cache);
        assert!(cache.take("owner/g", "cell", 1, range(2)).is_none());
        assert!(cache.take("owner/g", "cell", 2, range(1)).is_none());
        let promotion = cache.take("owner/g", "cell", 1, range(1)).unwrap();
        promotion.verify().unwrap();
    }

    #[test]
    fn hot_recovery_applies_growth_and_truncation_and_detects_cache_damage() {
        crate::test_runtime::init();
        let root = tempfile::tempdir().unwrap();
        let cache = Arc::new(HotStandbys::new(root.path(), 16384, 4).unwrap());
        let first = entry("cell", 1, 1, 1, 2, 0);
        let pre = ltx::decode_file(&first.bytes)
            .unwrap()
            .trailer
            .post_apply_checksum;
        let second = entry("cell", 2, 2, 2, 3, pre);
        let pre = ltx::decode_file(&second.bytes)
            .unwrap()
            .trailer
            .post_apply_checksum;
        let third = entry("cell", 3, 3, 3, 1, pre);
        cache.observe("owner/g", range(0), range(3), &[first, second, third]);
        covered(&cache);
        let promotion = cache.take("owner/g", "cell", 1, range(3)).unwrap();
        assert_eq!(std::fs::read(&promotion.path).unwrap(), vec![3; 4096]);
        promotion.verify().unwrap();
        std::fs::write(&promotion.path, vec![99; 4096]).unwrap();
        assert!(promotion.verify().is_err());
    }

    #[test]
    fn hot_recovery_rejects_ltx_gaps_and_drops_cache_certificates_on_restart() {
        crate::test_runtime::init();
        let root = tempfile::tempdir().unwrap();
        let cache = Arc::new(HotStandbys::new(root.path(), 16384, 4).unwrap());
        let first = entry("cell", 1, 1, 1, 1, 0);
        let pre = ltx::decode_file(&first.bytes)
            .unwrap()
            .trailer
            .post_apply_checksum;
        cache.observe(
            "owner/g",
            range(0),
            range(2),
            &[first, entry("cell", 2, 3, 3, 1, pre)],
        );
        covered(&cache);
        assert!(cache.take("owner/g", "cell", 1, range(2)).is_none());
        cache.observe(
            "owner/g",
            range(2),
            range(3),
            &[entry("cell", 3, 4, 1, 1, 0)],
        );
        covered(&cache);
        assert_eq!(cache.status()["candidates"][0]["ready"], true);
        drop(cache);
        let cache = Arc::new(HotStandbys::new(root.path(), 16384, 4).unwrap());
        assert!(cache.take("owner/g", "cell", 1, range(3)).is_none());
        assert_eq!(
            std::fs::read_dir(root.path().join("hot-standby"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn hot_recovery_budget_includes_prepared_promotions() {
        crate::test_runtime::init();
        let root = tempfile::tempdir().unwrap();
        let cache = Arc::new(HotStandbys::new(root.path(), 4096, 1).unwrap());
        cache.observe("owner/g", range(0), range(1), &[entry("a", 1, 1, 1, 1, 0)]);
        covered(&cache);
        let promotion = cache.take("owner/g", "a", 1, range(1)).unwrap();
        cache.observe("owner/g", range(1), range(2), &[entry("b", 2, 1, 1, 1, 0)]);
        assert_eq!(cache.status()["candidates"].as_array().unwrap().len(), 0);
        assert_eq!(cache.status()["bytes"], 4096);
        drop(promotion);
        cache.observe("owner/g", range(2), range(3), &[entry("b", 3, 1, 1, 1, 0)]);
        covered(&cache);
        assert!(cache.take("owner/g", "b", 1, range(3)).is_some());
    }

    #[test]
    fn cold_standby_seeding_does_not_require_an_empty_checkpoint_queue() {
        crate::test_runtime::init();
        let root = tempfile::tempdir().unwrap();
        let cache = HotStandbys::new(root.path(), 16384, 4).unwrap();
        let first = entry("cell", 1, 1, 1, 1, 0);
        let pre = ltx::decode_file(&first.bytes)
            .unwrap()
            .trailer
            .post_apply_checksum;
        let second = entry("cell", 2, 2, 2, 1, pre);
        let pre = ltx::decode_file(&second.bytes)
            .unwrap()
            .trailer
            .post_apply_checksum;
        cache.observe(
            "owner/g",
            range(1),
            range(3),
            &[second, entry("cell", 3, 3, 3, 1, pre)],
        );
        let job = cache.next_checkpoint().unwrap();
        assert!(
            cache.checkpointed(&job),
            "a published prefix must allow seeding even while writes are queued"
        );
    }

    #[test]
    fn cold_standby_replays_writes_observed_during_a_canonical_seed() {
        crate::test_runtime::init();
        let root = tempfile::tempdir().unwrap();
        let cache = Arc::new(HotStandbys::new(root.path(), 16384, 4).unwrap());
        let first = entry("cell", 1, 1, 1, 1, 0);
        let pre = ltx::decode_file(&first.bytes)
            .unwrap()
            .trailer
            .post_apply_checksum;
        let second = entry("cell", 2, 2, 2, 1, pre);
        let pre = ltx::decode_file(&second.bytes)
            .unwrap()
            .trailer
            .post_apply_checksum;
        cache.observe("owner/g", range(1), range(2), &[second]);
        let job = cache.next_checkpoint().unwrap();
        cache.checkpointed(&job);
        // The publisher fixes its seed at t2, then a new append arrives while
        // the canonical pages are being streamed from the object store.
        cache.observe(
            "owner/g",
            range(2),
            range(3),
            &[entry("cell", 3, 3, 3, 1, pre)],
        );
        let source = root.path().join("seed.sqlite");
        std::fs::write(&source, vec![2; 4096]).unwrap();
        let queued_bytes = cache.status()["queued_bytes"].clone();
        cache.seed(&job, &source, 2, 4096).unwrap();
        let status = cache.status();
        assert_eq!(status["queued_bytes"], queued_bytes);
        assert_eq!(status["candidates"][0]["txid"], 3);
        assert_eq!(status["candidates"][0]["covered_txid"], 2);
        assert_eq!(status["candidates"][0]["ready"], false);
        let retained = FollowerState {
            base: 1,
            ..range(3)
        };
        assert!(cache.take("owner/g", "cell", 1, retained).is_none());
        covered(&cache);
        let promotion = cache.take("owner/g", "cell", 1, retained).unwrap();
        assert_eq!(std::fs::read(&promotion.path).unwrap(), vec![3; 4096]);
        promotion.verify().unwrap();
    }

    #[test]
    fn cold_standby_seed_replay_rejects_gaps_and_checksum_changes() {
        crate::test_runtime::init();
        for gap in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let cache = Arc::new(HotStandbys::new(root.path(), 16384, 4).unwrap());
            let first = entry("cell", 1, 1, 1, 1, 0);
            let pre = ltx::decode_file(&first.bytes)
                .unwrap()
                .trailer
                .post_apply_checksum;
            let second = entry("cell", 2, 2, 2, 1, pre);
            let pre = ltx::decode_file(&second.bytes)
                .unwrap()
                .trailer
                .post_apply_checksum;
            cache.observe("owner/g", range(1), range(2), &[second]);
            let job = cache.next_checkpoint().unwrap();
            cache.checkpointed(&job);
            cache.observe(
                "owner/g",
                range(2),
                range(3),
                &[if gap {
                    entry("cell", 3, 4, 4, 1, pre)
                } else {
                    entry("cell", 3, 3, 3, 1, pre ^ 1)
                }],
            );
            let source = root.path().join("seed.sqlite");
            std::fs::write(&source, vec![2; 4096]).unwrap();
            assert!(cache.seed(&job, &source, 2, 4096).is_err());
            // The checkpoint worker discards a failed seed; no certificate or
            // additional queue/disk reservation survives that failure.
            cache.failed(&job);
            assert!(cache.take("owner/g", "cell", 1, range(3)).is_none());
            assert_eq!(cache.status()["bytes"], 0);
            assert_eq!(cache.status()["queued_bytes"], 0);
        }
    }

    #[test]
    fn cold_standby_seed_replay_accounts_for_peak_size_before_truncation() {
        crate::test_runtime::init();
        let root = tempfile::tempdir().unwrap();
        let cache = HotStandbys::new(root.path(), 12288, 4).unwrap();
        let first = entry("cell", 1, 1, 1, 1, 0);
        let pre = ltx::decode_file(&first.bytes)
            .unwrap()
            .trailer
            .post_apply_checksum;
        let second = entry("cell", 2, 2, 2, 1, pre);
        let pre = ltx::decode_file(&second.bytes)
            .unwrap()
            .trailer
            .post_apply_checksum;
        cache.observe("owner/g", range(1), range(2), &[second]);
        let job = cache.next_checkpoint().unwrap();
        cache.checkpointed(&job);
        let third = entry("cell", 3, 3, 3, 3, pre);
        let pre = ltx::decode_file(&third.bytes)
            .unwrap()
            .trailer
            .post_apply_checksum;
        cache.observe(
            "owner/g",
            range(2),
            range(4),
            &[third, entry("cell", 4, 4, 4, 1, pre)],
        );
        // The shrinking cold image now reserves one page. Another image fits,
        // but replay's earlier growth would exceed the aggregate disk budget.
        cache.observe(
            "owner/g",
            range(4),
            range(5),
            &[entry("other", 5, 1, 1, 1, 0)],
        );
        let source = root.path().join("seed.sqlite");
        std::fs::write(&source, vec![2; 4096]).unwrap();
        assert!(cache.seed(&job, &source, 2, 4096).is_err());
        assert!(
            source.exists(),
            "reject over-budget replay before installing its seed"
        );
        cache.failed(&job);
        assert_eq!(cache.status()["bytes"], 4096);
    }

    #[test]
    fn a_seed_from_a_superseded_fragment_cannot_replace_or_discard_its_new_image() {
        crate::test_runtime::init();
        let root = tempfile::tempdir().unwrap();
        let cache = Arc::new(HotStandbys::new(root.path(), 16384, 4).unwrap());
        let first = entry("cell", 1, 1, 1, 1, 0);
        let pre = ltx::decode_file(&first.bytes)
            .unwrap()
            .trailer
            .post_apply_checksum;
        cache.observe(
            "owner/g",
            range(1),
            range(2),
            &[entry("cell", 2, 2, 2, 1, pre)],
        );
        let job = cache.next_checkpoint().unwrap();
        cache.checkpointed(&job);
        // Reconfiguration invalidates the cache while its seed is in flight.
        let next = FollowerState {
            fragment_epoch: 2,
            end: 1,
            ..Default::default()
        };
        cache.observe("owner/g", range(0), next, &[entry("cell", 1, 3, 1, 1, 0)]);
        let source = root.path().join("old-seed.sqlite");
        std::fs::write(&source, vec![2; 4096]).unwrap();
        cache.seed(&job, &source, 2, 4096).unwrap();
        cache.failed(&job);
        assert_eq!(cache.status()["candidates"][0]["txid"], 3);
        assert_eq!(cache.status()["candidates"][0]["covered_txid"], 0);
        assert!(source.exists());
        covered(&cache);
        let promotion = cache.take("owner/g", "cell", 1, next).unwrap();
        assert_eq!(std::fs::read(&promotion.path).unwrap(), vec![3; 4096]);
        promotion.verify().unwrap();
    }

    #[test]
    fn a_seed_checksum_scan_does_not_hold_the_cache_mutex() {
        crate::test_runtime::init();
        let root = tempfile::tempdir().unwrap();
        let cache = Arc::new(HotStandbys::new(root.path(), 16384, 4).unwrap());
        let pre = CHECKSUM_FLAG | ltx::checksum_page(1, &vec![1; 4096]);
        cache.observe(
            "owner/g",
            range(1),
            range(2),
            &[entry("cell", 2, 2, 2, 1, pre)],
        );
        let job = cache.next_checkpoint().unwrap();
        cache.checkpointed(&job);
        let source = root.path().join("seed.sqlite");
        std::fs::write(&source, vec![2; 4096]).unwrap();
        let (started, scanning) = std::sync::mpsc::channel();
        let (resume, resumed) = std::sync::mpsc::channel();
        let seeding = cache.clone();
        let seed = std::thread::spawn(move || {
            seeding.seed_scanned(&job, &source, 2, 4096, || {
                started.send(()).unwrap();
                resumed.recv().unwrap();
                HotStandbys::scan_seed(&source, 4096)
            })
        });
        scanning
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let (sampled, sample) = std::sync::mpsc::channel();
        let sampling = cache.clone();
        let status = std::thread::spawn(move || {
            // Writes can arrive while the canonical checksum is being scanned.
            let pre = CHECKSUM_FLAG | ltx::checksum_page(1, &vec![2; 4096]);
            sampling.observe(
                "owner/g",
                range(2),
                range(3),
                &[entry("cell", 3, 3, 3, 1, pre)],
            );
            sampled.send(sampling.status()).unwrap();
        });
        let responsive = sample
            .recv_timeout(std::time::Duration::from_secs(1))
            .is_ok();
        // Always release/join both workers before asserting, even on failure.
        resume.send(()).unwrap();
        seed.join().unwrap().unwrap();
        status.join().unwrap();
        assert!(
            responsive,
            "a paused multi-GiB checksum scan blocked cache status"
        );
        let retained = FollowerState {
            base: 1,
            ..range(3)
        };
        assert!(cache.take("owner/g", "cell", 1, retained).is_none());
        covered(&cache);
        cache
            .take("owner/g", "cell", 1, retained)
            .unwrap()
            .verify()
            .unwrap();
    }

    #[test]
    fn a_fragment_change_during_the_checksum_scan_fences_its_seed() {
        crate::test_runtime::init();
        let root = tempfile::tempdir().unwrap();
        let cache = Arc::new(HotStandbys::new(root.path(), 16384, 4).unwrap());
        let pre = CHECKSUM_FLAG | ltx::checksum_page(1, &vec![1; 4096]);
        cache.observe(
            "owner/g",
            range(1),
            range(2),
            &[entry("cell", 2, 2, 2, 1, pre)],
        );
        let old_job = cache.next_checkpoint().unwrap();
        cache.checkpointed(&old_job);
        let source = root.path().join("seed.sqlite");
        std::fs::write(&source, vec![2; 4096]).unwrap();
        let new_range = |end| FollowerState {
            fragment_epoch: 2,
            end,
            ..Default::default()
        };
        cache
            .seed_scanned(&old_job, &source, 2, 4096, || {
                let scan = HotStandbys::scan_seed(&source, 4096)?;
                cache.observe(
                    "owner/g",
                    new_range(0),
                    new_range(1),
                    &[entry("cell", 1, 1, 1, 1, 0)],
                );
                covered(&cache);
                Ok(scan)
            })
            .unwrap();
        // Even a completed old scan cannot replace or discard a new incarnation.
        cache.failed(&old_job);
        assert!(
            source.exists(),
            "obsolete seed was installed over the new image"
        );
        let promotion = cache.take("owner/g", "cell", 1, new_range(1)).unwrap();
        assert_eq!(promotion.txid, 1);
        promotion.verify().unwrap();
    }
}

struct Session {
    epoch: u64,
    start: u64,
    end: u64,
}

pub(crate) struct Checkpoint {
    key: Key,
    image_serial: u64,
    pub cell: String,
    pub epoch: u64,
    pub txid: u64,
    pub bytes: Vec<u8>,
}

#[derive(Default)]
struct State {
    sessions: BTreeMap<String, Session>,
    images: BTreeMap<Key, Image>,
    queue: VecDeque<Checkpoint>,
    queued_bytes: u64,
    bytes: u64,
    serial: u64,
    prepared: usize,
}

pub(crate) struct HotStandbys {
    root: PathBuf,
    max_bytes: u64,
    max_cells: usize,
    queue_bytes: u64,
    state: Mutex<State>,
    pub wake: Notify,
}

pub(crate) struct Promotion {
    pub owner: String,
    pub session: String,
    pub log_epoch: u64,
    pub source_epoch: u64,
    pub txid: u64,
    pub commit: u32,
    pub path: PathBuf,
    pub expires_mono_ms: u64,
    checksum: u64,
    page_size: u32,
    reservation: Option<(Arc<HotStandbys>, u64)>,
}

impl Drop for Promotion {
    fn drop(&mut self) {
        let _ = crate::asyncrt::fs().remove_file(&self.path);
        if let Some((cache, bytes)) = self.reservation.take() {
            let mut state = cache.state.lock().unwrap();
            state.bytes = state.bytes.saturating_sub(bytes);
            state.prepared = state.prepared.saturating_sub(1);
        }
    }
}

impl Promotion {
    #[allow(clippy::disallowed_methods)]
    pub fn verify(&self) -> Result<()> {
        let mut file = std::fs::File::open(&self.path)?;
        anyhow::ensure!(
            file.metadata()?.len() == u64::from(self.commit) * u64::from(self.page_size),
            "standby file length changed"
        );
        let mut rolling = CHECKSUM_FLAG;
        let mut page = vec![0; self.page_size as usize];
        let lock = ltx::lock_pgno(self.page_size);
        for pgno in 1..=self.commit {
            file.read_exact(&mut page)?;
            if pgno != lock {
                rolling = CHECKSUM_FLAG | (rolling ^ ltx::checksum_page(pgno, &page));
            }
        }
        anyhow::ensure!(rolling == self.checksum, "standby file checksum changed");
        file.sync_all()?;
        Ok(())
    }
}

// Raw page I/O is confined to this host adapter. No SQLite connection opens a
// standby, and its manifest is process-local, so a torn cache cannot survive
// restart as a promotion certificate.
#[allow(clippy::disallowed_methods)]
impl HotStandbys {
    pub fn status(&self) -> serde_json::Value {
        let state = self.state.lock().unwrap();
        serde_json::json!({
            "enabled": true, "bytes": state.bytes, "max_bytes": self.max_bytes,
            "queued_bytes": state.queued_bytes,
            "prepared": state.prepared,
            "candidates": state.images.iter().map(|((session, cell, epoch), image)| serde_json::json!({
                "session": session, "cell": cell, "cell_epoch": epoch,
                "txid": image.txid, "covered_txid": image.covered,
                "ready": image.txid != 0 && image.txid >= image.expected && image.covered >= image.txid,
            })).collect::<Vec<_>>(),
        })
    }

    pub fn new(root: &Path, max_bytes: u64, max_cells: usize) -> Result<Self> {
        let root = root.join("hot-standby");
        if root.exists() {
            std::fs::remove_dir_all(&root).context("discard previous process's standby cache")?;
        }
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            max_bytes,
            max_cells,
            queue_bytes: 64 * 1024 * 1024,
            state: Mutex::new(State::default()),
            wake: Notify::new(),
        })
    }

    fn discard(state: &mut State, key: &Key) {
        if let Some(image) = state.images.remove(key) {
            state.bytes = state.bytes.saturating_sub(image.bytes);
            let _ = std::fs::remove_file(image.path);
        }
        state.queue.retain(|job| &job.key != key);
        state.queued_bytes = state.queue.iter().map(|job| job.bytes.len() as u64).sum();
    }

    pub fn observe(
        &self,
        session: &str,
        previous: FollowerState,
        next: FollowerState,
        entries: &[Entry],
    ) {
        let mut state = self.state.lock().unwrap();
        let stream = state
            .sessions
            .entry(session.to_string())
            .or_insert(Session {
                epoch: next.fragment_epoch,
                start: previous.end,
                end: previous.end,
            });
        let discontinuity = stream.epoch != next.fragment_epoch || stream.end != previous.end;
        if discontinuity {
            // A missed observation cannot certify a contiguous live stream.
            *stream = Session {
                epoch: next.fragment_epoch,
                start: previous.end,
                end: previous.end,
            };
        }
        stream.end = next.end;
        if discontinuity {
            let keys: Vec<_> = state
                .images
                .keys()
                .filter(|key| key.0 == session)
                .cloned()
                .collect();
            for key in keys {
                Self::discard(&mut state, &key);
            }
        }
        for entry in entries {
            let key = (session.to_string(), entry.cell.clone(), entry.cell_epoch);
            if let Err(error) = self.apply(&mut state, &key, entry) {
                Self::discard(&mut state, &key);
                tracing::warn!(session, cell = entry.cell, %error, "standby unavailable; ordinary recovery remains required");
            }
        }
        self.wake.notify_one();
    }

    fn apply(&self, state: &mut State, key: &Key, entry: &Entry) -> Result<()> {
        let header = ltx::Header::parse(&entry.bytes)?;
        let bytes = u64::from(header.commit) * u64::from(header.page_size);
        if !state.images.contains_key(key) {
            if state.images.len() + state.prepared >= self.max_cells {
                if let Some(victim) = state.images.keys().next().cloned() {
                    Self::discard(state, &victim);
                }
            }
            anyhow::ensure!(
                state.images.len() + state.prepared < self.max_cells,
                "standby cell budget reached"
            );
            state.serial += 1;
            state.images.insert(
                key.clone(),
                Image {
                    serial: state.serial,
                    path: self.root.join(format!("{}.sqlite", state.serial)),
                    txid: 0,
                    checksum: 0,
                    page_size: header.page_size,
                    commit: 0,
                    expected: 0,
                    covered: 0,
                    bytes: 0,
                },
            );
        }
        while state
            .bytes
            .saturating_sub(state.images[key].bytes)
            .saturating_add(bytes)
            > self.max_bytes
        {
            let Some(victim) = state
                .images
                .keys()
                .find(|candidate| *candidate != key)
                .cloned()
            else {
                break;
            };
            Self::discard(state, &victim);
        }
        let image = state.images.get_mut(key).unwrap();
        anyhow::ensure!(
            state
                .bytes
                .saturating_sub(image.bytes)
                .saturating_add(bytes)
                <= self.max_bytes,
            "standby disk budget reached"
        );
        anyhow::ensure!(
            state.queued_bytes.saturating_add(entry.bytes.len() as u64) <= self.queue_bytes,
            "standby checkpoint queue budget reached"
        );
        Self::materialize(image, entry.txid, &entry.bytes)?;
        let retained_bytes = if image.txid == 0 {
            bytes
        } else {
            u64::from(image.commit) * u64::from(image.page_size)
        };
        state.bytes = state
            .bytes
            .saturating_sub(image.bytes)
            .saturating_add(retained_bytes);
        image.bytes = retained_bytes;
        state.queued_bytes += entry.bytes.len() as u64;
        state.queue.push_back(Checkpoint {
            key: key.clone(),
            image_serial: image.serial,
            cell: entry.cell.clone(),
            epoch: entry.cell_epoch,
            txid: entry.txid,
            bytes: entry.bytes.clone(),
        });
        Ok(())
    }

    fn materialize(image: &mut Image, txid: u64, bytes: &[u8]) -> Result<()> {
        let (decoded, pages) = ltx::decode_file_with_pages(bytes)?;
        let header = decoded.header;
        anyhow::ensure!(header.max_txid.0 == txid, "standby entry TXID mismatch");
        let lock = ltx::lock_pgno(header.page_size);
        let mut seen = BTreeSet::new();
        anyhow::ensure!(
            pages.iter().all(|(pgno, _)| *pgno > 0
                && *pgno <= header.commit
                && *pgno != lock
                && seen.insert(*pgno)),
            "standby has duplicate or out-of-range pages"
        );
        image.expected = image.expected.max(header.max_txid.0);
        // A non-snapshot first entry needs a canonical bucket seed. The bounded
        // publisher replays its queued tail if writes arrive during the seed.
        if (image.txid != 0 || header.is_snapshot()) && header.max_txid.0 > image.txid {
            anyhow::ensure!(
                header.page_size == image.page_size,
                "standby page size changed"
            );
            if !header.is_snapshot() {
                anyhow::ensure!(
                    header.min_txid.0 == image.txid + 1,
                    "standby LTX gap: image t{}, incoming t{}-{}",
                    image.txid,
                    header.min_txid.0,
                    header.max_txid.0
                );
                anyhow::ensure!(
                    header.no_checksum() || header.pre_apply_checksum == image.checksum,
                    "standby LTX checksum chain changed"
                );
            }
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&image.path)?;
            let mut rolling = if header.is_snapshot() {
                CHECKSUM_FLAG
            } else {
                image.checksum
            };
            let lock = ltx::lock_pgno(header.page_size);
            if header.is_snapshot() {
                anyhow::ensure!(
                    pages.len() as u64
                        == u64::from(header.commit) - u64::from(lock <= header.commit),
                    "standby snapshot is missing pages"
                );
                file.set_len(0)?;
            } else {
                for pgno in header.commit.saturating_add(1)..=image.commit {
                    if pgno != lock {
                        let mut old = vec![0; header.page_size as usize];
                        file.seek(SeekFrom::Start(
                            u64::from(pgno - 1) * u64::from(header.page_size),
                        ))?;
                        file.read_exact(&mut old)?;
                        rolling = CHECKSUM_FLAG | (rolling ^ ltx::checksum_page(pgno, &old));
                    }
                }
            }
            let mut added = 0_u32;
            for (pgno, page) in &pages {
                let offset = u64::from(*pgno - 1) * u64::from(header.page_size);
                if !header.is_snapshot() && *pgno <= image.commit {
                    let mut old = vec![0; header.page_size as usize];
                    file.seek(SeekFrom::Start(offset))?;
                    file.read_exact(&mut old)?;
                    rolling = CHECKSUM_FLAG | (rolling ^ ltx::checksum_page(*pgno, &old));
                } else {
                    added += 1;
                }
                rolling = CHECKSUM_FLAG | (rolling ^ ltx::checksum_page(*pgno, page));
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(page)?;
            }
            if !header.is_snapshot() && header.commit > image.commit {
                let needed = header.commit
                    - image.commit
                    - u32::from(image.commit < lock && lock <= header.commit);
                anyhow::ensure!(added == needed, "standby is missing newly committed pages");
            }
            anyhow::ensure!(
                header.no_checksum() || rolling == decoded.trailer.post_apply_checksum,
                "standby rolling checksum mismatch"
            );
            file.set_len(u64::from(header.commit) * u64::from(header.page_size))?;
            image.txid = header.max_txid.0;
            image.checksum = rolling;
            image.commit = header.commit;
        }
        Ok(())
    }

    pub fn next_checkpoint(&self) -> Option<Checkpoint> {
        let mut state = self.state.lock().unwrap();
        let job = state.queue.pop_front()?;
        state.queued_bytes -= job.bytes.len() as u64;
        Some(job)
    }

    pub fn checkpointed(&self, job: &Checkpoint) -> bool {
        let mut state = self.state.lock().unwrap();
        if let Some(image) = state
            .images
            .get_mut(&job.key)
            .filter(|image| image.serial == job.image_serial)
        {
            image.covered = image.covered.max(job.txid);
            return image.txid == 0;
        }
        false
    }

    pub fn checkpoint_floor(&self, job: &Checkpoint) -> u64 {
        self.state
            .lock()
            .unwrap()
            .images
            .get(&job.key)
            .filter(|image| image.serial == job.image_serial)
            .map_or(0, |image| image.covered)
    }

    pub fn failed(&self, job: &Checkpoint) {
        let mut state = self.state.lock().unwrap();
        if state
            .images
            .get(&job.key)
            .is_some_and(|image| image.serial == job.image_serial)
        {
            Self::discard(&mut state, &job.key);
        }
    }

    pub fn seed_budget(&self, job: &Checkpoint) -> Option<u64> {
        self.state
            .lock()
            .unwrap()
            .images
            .get(&job.key)
            .filter(|image| image.serial == job.image_serial && image.txid == 0)
            .map(|image| image.bytes)
    }

    pub fn seed(&self, job: &Checkpoint, source: &Path, txid: u64, page_size: u32) -> Result<()> {
        self.seed_scanned(job, source, txid, page_size, || {
            Self::scan_seed(source, page_size)
        })
    }

    fn scan_seed(source: &Path, page_size: u32) -> Result<(u64, u32, u64)> {
        anyhow::ensure!(
            ltx::is_valid_page_size(page_size),
            "invalid standby seed page size"
        );
        let mut file = std::fs::File::open(source)?;
        let bytes = file.metadata()?.len();
        anyhow::ensure!(
            bytes % u64::from(page_size) == 0,
            "standby seed has a partial page"
        );
        let commit = u32::try_from(bytes / u64::from(page_size))?;
        let mut checksum = CHECKSUM_FLAG;
        let lock = ltx::lock_pgno(page_size);
        let mut page = vec![0; page_size as usize];
        for pgno in 1..=commit {
            file.read_exact(&mut page)?;
            if pgno != lock {
                checksum = CHECKSUM_FLAG | (checksum ^ ltx::checksum_page(pgno, &page));
            }
        }
        Ok((bytes, commit, checksum))
    }

    fn seed_scanned(
        &self,
        job: &Checkpoint,
        source: &Path,
        txid: u64,
        page_size: u32,
        scan: impl FnOnce() -> Result<(u64, u32, u64)>,
    ) -> Result<()> {
        if self.seed_budget(job).is_none() {
            return Ok(());
        }
        anyhow::ensure!(txid >= job.txid, "standby seed predates its checkpoint");
        // The source is private to this checkpoint worker. A multi-GiB page
        // checksum scan must not hold the cache mutex: appends, status, and
        // fragment changes must remain responsive while it runs. Recheck the
        // exact image incarnation before installing the completed scan.
        let (bytes, commit, checksum) = scan()?;
        let mut state = self.state.lock().unwrap();
        let Some(image) = state
            .images
            .get(&job.key)
            .filter(|image| image.serial == job.image_serial)
        else {
            return Ok(());
        };
        // Do not replace a cache the append path already advanced.
        if image.txid != 0 {
            return Ok(());
        }
        // This worker pauses queue consumption during the download/scan. Every
        // later observed commit is still in the bounded queue, so a seed at
        // least as new as this published job can replay that fixed tail.
        let mut peak_bytes = bytes;
        for pending in state.queue.iter().filter(|pending| pending.key == job.key) {
            if pending.txid > txid {
                let header = ltx::Header::parse(&pending.bytes)?;
                peak_bytes = peak_bytes.max(u64::from(header.commit) * u64::from(header.page_size));
            }
        }
        anyhow::ensure!(
            state
                .bytes
                .saturating_sub(image.bytes)
                .saturating_add(peak_bytes)
                <= self.max_bytes,
            "standby seed exceeds disk budget"
        );
        let old_bytes = image.bytes;
        let State {
            images,
            queue,
            bytes: retained_bytes,
            ..
        } = &mut *state;
        let image = images.get_mut(&job.key).unwrap();
        std::fs::rename(source, &image.path)?;
        image.txid = txid;
        image.covered = image.covered.max(txid);
        image.checksum = checksum;
        image.commit = commit;
        image.page_size = page_size;
        // Materialize without re-enqueueing or claiming checkpoint coverage
        // for the replay. Those jobs still need their normal bucket uploads.
        for pending in queue.iter().filter(|pending| pending.key == job.key) {
            if pending.txid > txid {
                Self::materialize(image, pending.txid, &pending.bytes)?;
            }
        }
        anyhow::ensure!(
            image.txid >= image.expected,
            "standby seed tail is incomplete"
        );
        image.bytes = u64::from(image.commit) * u64::from(image.page_size);
        *retained_bytes = retained_bytes
            .saturating_sub(old_bytes)
            .saturating_add(image.bytes);
        Ok(())
    }

    pub fn take(
        self: &Arc<Self>,
        session: &str,
        cell: &str,
        epoch: u64,
        state: FollowerState,
    ) -> Option<Promotion> {
        let owner = session.split_once('/')?.0.to_string();
        let mut cache = self.state.lock().unwrap();
        let stream = cache.sessions.get(session)?;
        if stream.epoch != state.fragment_epoch
            || stream.start > state.base
            || stream.end != state.end
        {
            return None;
        }
        let key = (session.to_string(), cell.to_string(), epoch);
        let image = cache.images.get(&key)?;
        if image.txid == 0 || image.txid < image.expected || image.covered < image.txid {
            return None;
        }
        let image = cache.images.remove(&key)?;
        cache.prepared += 1;
        Some(Promotion {
            owner,
            session: session.to_string(),
            log_epoch: state.fragment_epoch,
            source_epoch: epoch,
            txid: image.txid,
            commit: image.commit,
            path: image.path,
            expires_mono_ms: crate::asyncrt::mono_ms().saturating_add(60_000),
            checksum: image.checksum,
            page_size: image.page_size,
            reservation: Some((self.clone(), image.bytes)),
        })
    }
}
