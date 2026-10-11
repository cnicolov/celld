// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Bounded follower reads. An index retains verified ranges, never LTX bytes.
//! The disk scan costs at most one batch of payload memory; later pages read
//! only the files intersecting their cursor. State changes invalidate the index.

use super::*;

#[derive(Clone, Copy)]
enum Kind {
    Committed,
    Entry,
    Entries,
}

struct TailFile {
    path: PathBuf,
    first: u64,
    last: u64,
    kind: Kind,
}

pub(super) struct TailIndex {
    state: FollowerState,
    pub(super) range: TailRange,
    files: Vec<TailFile>,
}

fn legacy_entries(bytes: &[u8], single: bool) -> (Vec<Entry>, bool) {
    let mut input = bytes;
    let mut entries = Vec::new();
    while !input.is_empty() {
        match decode_entry(&mut input) {
            Ok(entry) => entries.push(entry),
            Err(_) => return (entries, false),
        }
        if single && !input.is_empty() {
            return (entries, false);
        }
    }
    (entries, true)
}

impl FollowerStore {
    pub(super) fn tail_index(&self, leader: &str) -> anyhow::Result<Arc<TailIndex>> {
        let state = self.load(leader)?;
        if let Some(index) = self.tail_indexes.lock().unwrap().get(leader) {
            if index.state == state {
                return Ok(index.clone());
            }
        }
        let scan = self.scan_committed_batches(leader)?;
        let mut complete =
            scan.recovery_gap(state).is_none() && self.recovered_disk_range_complete(leader);
        let mut files: Vec<TailFile> = scan
            .batches
            .into_iter()
            .filter(|batch| batch.state.fragment_epoch == state.fragment_epoch)
            .map(|batch| TailFile {
                path: batch.path,
                first: batch.first,
                last: batch.last,
                kind: Kind::Committed,
            })
            .collect();
        for item in self.filesystem.read_dir(&self.dir(leader))? {
            let Some(name) = item.file_name.to_str() else {
                continue;
            };
            let (first, last, kind) = if let Some(stem) = name.strip_suffix(".entry") {
                let Ok(seq) = stem.parse::<u64>() else {
                    complete = false;
                    continue;
                };
                (seq, seq, Kind::Entry)
            } else if name.ends_with(".entries") {
                let Some((first, last)) =
                    follower_batch_range(name, ".entries").filter(|(first, last)| first <= last)
                else {
                    complete = false;
                    continue;
                };
                (first, last, Kind::Entries)
            } else {
                continue;
            };
            if last <= state.base || first > state.end {
                continue;
            }
            let bytes = self.filesystem.read(&item.path)?;
            let (entries, decoded) = legacy_entries(&bytes, matches!(kind, Kind::Entry));
            let valid = decoded
                && entries.first().is_some_and(|entry| entry.seq == first)
                && entries.last().is_some_and(|entry| entry.seq == last)
                && entries
                    .windows(2)
                    .all(|pair| pair[0].seq.checked_add(1) == Some(pair[1].seq));
            complete &= valid;
            if valid {
                files.push(TailFile {
                    path: item.path,
                    first,
                    last,
                    kind,
                });
            }
        }
        files.sort_by_key(|file| (file.first, file.last));
        let mut covered = state.base;
        for file in &files {
            if file.last <= covered {
                continue;
            }
            if file.first > covered.saturating_add(1) {
                break;
            }
            covered = covered.max(file.last);
        }
        complete &= state.fragment_epoch != 0 && state.base <= state.end && covered >= state.end;
        let index = Arc::new(TailIndex {
            state,
            range: TailRange {
                fragment_epoch: state.fragment_epoch,
                base: state.base,
                end: state.end,
                complete,
            },
            files,
        });
        self.tail_indexes
            .lock()
            .unwrap()
            .insert(leader.to_string(), index.clone());
        Ok(index)
    }

    fn page_file_entries(&self, file: &TailFile, epoch: u64) -> anyhow::Result<Vec<Entry>> {
        let bytes = self.filesystem.read(&file.path)?;
        let entries = match file.kind {
            Kind::Committed => {
                let batch = decode_committed_batch(&bytes)?;
                anyhow::ensure!(
                    batch.state.fragment_epoch == epoch,
                    "tail file changed fragment epoch"
                );
                batch.entries
            }
            kind => {
                let (entries, complete) = legacy_entries(&bytes, matches!(kind, Kind::Entry));
                anyhow::ensure!(complete, "tail file became unreadable");
                entries
            }
        };
        anyhow::ensure!(
            entries.first().is_some_and(|entry| entry.seq == file.first)
                && entries.last().is_some_and(|entry| entry.seq == file.last)
                && entries
                    .windows(2)
                    .all(|pair| pair[0].seq.checked_add(1) == Some(pair[1].seq)),
            "tail file changed its retained range"
        );
        Ok(entries)
    }

    pub(super) fn read_tail_page(&self, req: &TailPageReq) -> anyhow::Result<TailResp> {
        anyhow::ensure!(
            req.max_bytes > 0 && req.max_bytes <= TAIL_PAGE_BYTES,
            "invalid tail page byte limit"
        );
        let index = self.tail_index(&req.leader)?;
        let mut range = req.range.unwrap_or(index.range);
        anyhow::ensure!(
            range.fragment_epoch == index.range.fragment_epoch
                && range.base == index.range.base
                && range.end <= index.range.end,
            "tail range changed during transfer; restart from the new retained range"
        );
        range.complete &= index.range.complete;
        let mut cursor = req.after.unwrap_or(range.base);
        anyhow::ensure!(
            range.base <= cursor && cursor <= range.end,
            "tail cursor is outside the retained range"
        );
        let mut entries = Vec::new();
        let mut bytes = 0_usize;
        for file in &index.files {
            if file.last <= cursor || file.first > range.end {
                continue;
            }
            for entry in self.page_file_entries(file, range.fragment_epoch)? {
                if entry.seq <= cursor || entry.seq > range.end {
                    continue;
                }
                if cursor.checked_add(1) != Some(entry.seq) {
                    range.complete = false;
                    return Ok(TailResp { range, entries });
                }
                let size = 34 + entry.cell.len() + entry.bytes.len();
                // Keep a single indivisible LTX entry even when it is larger
                // than the page target. Memory is bounded by a page plus the
                // largest already-admitted batch, not by the whole fragment.
                if !entries.is_empty() && bytes.saturating_add(size) > req.max_bytes {
                    return Ok(TailResp { range, entries });
                }
                bytes += size;
                cursor = entry.seq;
                entries.push(entry);
                if cursor == range.end {
                    return Ok(TailResp { range, entries });
                }
            }
        }
        if cursor < range.end {
            range.complete = false;
        }
        Ok(TailResp { range, entries })
    }
}
