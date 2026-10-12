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
}

struct Session {
    epoch: u64,
    start: u64,
    end: u64,
}

pub(crate) struct Checkpoint {
    key: Key,
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
        let (decoded, pages) = ltx::decode_file_with_pages(&entry.bytes)?;
        let header = decoded.header;
        anyhow::ensure!(
            header.max_txid.0 == entry.txid,
            "standby entry TXID mismatch"
        );
        let lock = ltx::lock_pgno(header.page_size);
        let mut seen = BTreeSet::new();
        anyhow::ensure!(
            pages.iter().all(|(pgno, _)| *pgno > 0
                && *pgno <= header.commit
                && *pgno != lock
                && seen.insert(*pgno)),
            "standby has duplicate or out-of-range pages"
        );
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
        image.expected = image.expected.max(header.max_txid.0);
        // A non-snapshot first entry needs a canonical bucket seed. The bounded
        // publisher seeds it once its queued checkpoints have caught up.
        if (image.txid != 0 || header.is_snapshot()) && header.max_txid.0 > image.txid {
            anyhow::ensure!(
                header.page_size == image.page_size,
                "standby page size changed"
            );
            if !header.is_snapshot() {
                anyhow::ensure!(header.min_txid.0 == image.txid + 1, "standby LTX gap");
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
            file.set_len(bytes)?;
            image.txid = header.max_txid.0;
            image.checksum = rolling;
            image.commit = header.commit;
        }
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
            cell: entry.cell.clone(),
            epoch: entry.cell_epoch,
            txid: entry.txid,
            bytes: entry.bytes.clone(),
        });
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
        let queued = state.queue.iter().any(|next| next.key == job.key);
        if let Some(image) = state.images.get_mut(&job.key) {
            image.covered = image.covered.max(job.txid);
            return image.txid == 0 && !queued;
        }
        false
    }

    pub fn checkpoint_floor(&self, job: &Checkpoint) -> u64 {
        self.state
            .lock()
            .unwrap()
            .images
            .get(&job.key)
            .map_or(0, |image| image.covered)
    }

    pub fn failed(&self, job: &Checkpoint) {
        Self::discard(&mut self.state.lock().unwrap(), &job.key);
    }

    pub fn seed_budget(&self, job: &Checkpoint) -> Option<u64> {
        self.state
            .lock()
            .unwrap()
            .images
            .get(&job.key)
            .filter(|image| image.txid == 0)
            .map(|image| image.bytes)
    }

    pub fn seed(&self, job: &Checkpoint, source: &Path, txid: u64, page_size: u32) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let Some(image) = state.images.get(&job.key) else {
            return Ok(());
        };
        // Do not replace a cache the append path already advanced.
        if image.txid != 0 || txid < image.expected {
            return Ok(());
        }
        anyhow::ensure!(
            ltx::is_valid_page_size(page_size),
            "invalid standby seed page size"
        );
        let bytes = std::fs::metadata(source)?.len();
        anyhow::ensure!(
            bytes % u64::from(page_size) == 0,
            "standby seed has a partial page"
        );
        anyhow::ensure!(
            state
                .bytes
                .saturating_sub(image.bytes)
                .saturating_add(bytes)
                <= self.max_bytes,
            "standby seed exceeds disk budget"
        );
        let old_bytes = image.bytes;
        let image = state.images.get_mut(&job.key).unwrap();
        std::fs::rename(source, &image.path)?;
        let mut file = std::fs::File::open(&image.path)?;
        let commit = u32::try_from(file.metadata()?.len() / u64::from(page_size))?;
        let mut checksum = CHECKSUM_FLAG;
        let lock = ltx::lock_pgno(page_size);
        let mut page = vec![0; page_size as usize];
        for pgno in 1..=commit {
            file.read_exact(&mut page)?;
            if pgno != lock {
                checksum = CHECKSUM_FLAG | (checksum ^ ltx::checksum_page(pgno, &page));
            }
        }
        image.txid = txid;
        image.covered = image.covered.max(txid);
        image.checksum = checksum;
        image.commit = commit;
        image.page_size = page_size;
        image.bytes = bytes;
        state.bytes = state.bytes.saturating_sub(old_bytes).saturating_add(bytes);
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
