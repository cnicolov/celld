// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! A certified follower tail spooled to the node's recovery disk. Recovery
//! folds bundles first, then replays tail pages in sequence order.

use super::*;

pub(super) struct RecoveryTail {
    file: celld_ltx::HostFile,
    offset: u64,
    length: u64,
    pub(super) range: Option<TailRange>,
}

impl RecoveryTail {
    fn new(file: celld_ltx::HostFile) -> Self {
        Self {
            file,
            offset: 0,
            length: 0,
            range: None,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let length = u32::try_from(bytes.len())?;
        self.file.write_all(&length.to_le_bytes())?;
        self.file.write_all(bytes)?;
        self.length += 4 + bytes.len() as u64;
        Ok(())
    }

    pub(super) fn next(&mut self) -> anyhow::Result<Option<Vec<Entry>>> {
        if self.offset == self.length {
            return Ok(None);
        }
        let header = self.file.read_exact_at(self.offset, 4)?;
        let length = u32::from_le_bytes(header.try_into().unwrap()) as usize;
        let bytes = self.file.read_exact_at(self.offset + 4, length)?;
        self.offset += 4 + length as u64;
        let tail = decode_tail_page(&bytes)?;
        Ok(Some(tail.entries))
    }
}

impl NodeLogManager {
    pub(super) async fn fetch_tail(
        &self,
        member: &str,
        addr: &str,
        leader: &str,
        expected: Option<TailRange>,
        mut claim: Option<(&str, &mut ClaimBeat)>,
    ) -> anyhow::Result<RecoveryTail> {
        let mut spool = RecoveryTail::new(self.ltx.recovery_scratch()?);
        let mut range = expected;
        let mut after = expected.map(|range| range.base);
        let started = mono_ms();
        let mut pages = 0_u64;
        let mut retries = 0_u32;
        loop {
            if let Some((dead, beat)) = claim.as_mut() {
                self.beat_claim(dead, beat).await?;
            }
            let req = TailPageReq {
                leader: leader.to_string(),
                after,
                max_bytes: TAIL_PAGE_BYTES,
                range,
            };
            let read = self.transport.post(
                member,
                addr,
                "/peer/log/tail-page",
                serde_json::to_vec(&req)?,
                None,
            );
            tokio::pin!(read);
            let bytes = loop {
                crate::asyncrt::select_biased! {
                    "completed recovery I/O wins a tie with a claim heartbeat";
                    bytes = &mut read => break bytes,
                    _ = crate::asyncrt::sleep(RECOVERY_HEARTBEAT), if claim.is_some() => {
                        let (dead, beat) = claim.as_mut().unwrap();
                        self.beat_claim(dead, beat).await?;
                    },
                }
            };
            let bytes = match bytes {
                Ok(bytes) => {
                    retries = 0;
                    bytes
                }
                Err(error) if retries < 2 => {
                    retries += 1;
                    warn!(member, leader, ?after, retries, %error, "retrying the current tail page; earlier pages retained in recovery scratch");
                    if let Some((dead, beat)) = claim.as_mut() {
                        self.beat_claim(dead, beat).await?;
                    }
                    crate::asyncrt::sleep(RECOVERY_CLAIM_POLL).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            // Validate and spool one page before requesting the next.
            let page = decode_tail_page(&bytes)?;
            anyhow::ensure!(page.range.complete, "follower tail is incomplete");
            let snapshot = *range.get_or_insert(page.range);
            spool.range = Some(snapshot);
            anyhow::ensure!(page.range == snapshot, "tail range changed during transfer");
            let cursor = after.unwrap_or(snapshot.base);
            let end = page.entries.last().map_or(cursor, |entry| entry.seq);
            anyhow::ensure!(
                end <= snapshot.end && tail_covers_sealed_range(cursor, end, &page.entries),
                "tail page contains a gap or an out-of-range entry"
            );
            anyhow::ensure!(
                end > cursor || cursor == snapshot.end,
                "tail page did not advance"
            );
            spool.push(&bytes)?;
            pages += 1;
            // Fast pages never fire the in-flight timer. Check the monotonic
            // heartbeat deadline at progress boundaries as well, or a long
            // healthy transfer can reset that timer until its claim expires.
            if let Some((dead, beat)) = claim.as_mut() {
                self.beat_claim(dead, beat).await?;
            }
            if end == snapshot.end {
                info!(
                    event = "recovery_tail_transfer",
                    member,
                    leader,
                    pages,
                    bytes = spool.length,
                    elapsed_ms = mono_ms().saturating_sub(started),
                    "complete follower tail transferred to recovery scratch"
                );
                return Ok(spool);
            }
            after = Some(end);
        }
    }
}
