# Hot recovery

Status: implemented, opt-in fork feature. Single-region fleet durability; JS
starts only after promotion. This is separate from the payload/K2 proposal.

## Lifecycle

1. Each follower still persists the opaque owner-session log before answering.
   With `CELLD_HOT_RECOVERY=1`, it also applies verified LTX pages to private
   SQLite images under `<data-dir>/hot-standby/`. No SQLite connection or JS
   runtime opens an image while it is a standby. Materialization runs on the
   blocking pool and currently adds work to the follower acknowledgement path.
2. An owned background worker publishes the same LTX segments into the
   canonical per-cell bucket prefixes. These checkpoints do **not** credit or
   truncate the original session log. They make a successor's baseline durable
   before, rather than during, failover. Both followers can publish identical
   objects, so this trades extra local I/O and object-store writes for latency.
   A delta is never published past a missing canonical baseline: the existing
   GC watermarks must not advance over a hole, even after cache eviction.
3. If a follower joins after the initial snapshot, the worker seeds its image
   from the canonical epoch chain using bounded page runs. Missing/gapped bases
   are not eligible. An in-progress seed must fit the image's disk reservation.
4. For an expired owner, the existing `RecoverNodeLog` interlock can prepare a
   **cell-scoped** certificate instead of waiting for the entire dead session.
   It CAS-fences the session, seals the local follower fragment, checks the
   image's full rolling checksum, validates canonical chain coverage, and
   rejects any newer bucket-only commit or retained bundle row.
5. Only after the actor wins the cell ownership CAS may activation rename the
   image into the successor epoch. It seeds a continuation and uploads the
   successor's marker before JS can run. There is no cold tail transfer or
   whole-database upload on this promotion path.
6. Ordinary recovery still drains and seals the **whole** predecessor session
   in the background or on predecessor restart. Follower GC and disk retirement
   use the original session obligations, not the cell certificate.

## Promotion proof

For the exact `(owner generation, log fragment epoch, cell, cell epoch)`:

- The owner lease has expired, and this node is a recorded ensemble member.
- The fragment's un-tiered range was observed contiguously in this process,
  through its current persisted end; a missing observation or fragment change
  invalidates older images.
- The sealed image contains every observed cell commit, and each commit's
  LTX file checksum, page bounds, contiguity, and rolling checksum were checked.
- A complete canonical restore plan ends at **exactly** the image's TXID.
  A larger bucket prefix is a stale-standby rejection, not permission to lose
  bucket-only writes. A prefix with a hole is not a restore proof.
- Retained bundle footers contain no newer row for this cell. Checks are
  ETag-pinned ranged reads, not downloads of large bundle payloads.
- The predecessor generation and fragment epoch still match after preparation.
- Activation matches the certificate's prior owner and successor epoch, and
  the ownership CAS has won. An existing destination/WAL is not overwritten
  with a prepared image.

This proves only this cell's baseline. It never changes `Recovering` to
`Sealed` and never authorizes session-log GC. Normal epoch fencing and the
serial actor's ownership/lease checks still govern every write.

## Bounds and fallback

| Setting | Default | Scope |
| --- | ---: | --- |
| `CELLD_HOT_RECOVERY` | `0` | Enable with `1`; all other values are rejected |
| `CELLD_HOT_STANDBY_MB` | `2048` | Total image/reservation budget per node |
| `CELLD_HOT_STANDBY_CELLS` | `128` | Images plus prepared promotions per node |

The checkpoint queue is limited to 64 MiB. Promotion checks at most 128 retained
bundles, at most 1 MiB per footer and 8 MiB of total footer data. Exceeding a
limit, failed checkpoint publication, cache corruption, lag, or a stale source
falls back to the existing conservative session recovery path. Prepared images
expire after 60 seconds and are reaped by the checkpoint worker, so a lost CAS
cannot pin the standby budget indefinitely. Session logs remain durable and
unchanged when a cache is dropped.

The image budget counts prepared images and logical pending seed sizes. A seed
uses a temporary file inside the managed cache directory, then renames it.
Concurrent resize/eviction can temporarily free its logical reservation before
the staging file is discarded, so provision up to one additional image-budget
of scratch space; this is not a filesystem quota.
Decode memory also includes the largest admitted LTX entry and decompressed
pages. A 600 MiB image is not retained as a 600 MiB heap buffer.

Standby manifests are deliberately process-local. Restart clears the managed
cache and requires ordinary predecessor recovery before serving. This version
accelerates failure of an owner while an eligible follower stays alive; it does
not promise hot recovery after every node restarts together. Placement does not
yet preferentially select a warm follower. Promotion still scans the local
image for checksum verification, so its work scales with local database size.

## Operations and qualification

The internal operator listener exposes `GET /hot-recovery` with image TXIDs,
checkpoint coverage, preparation count, queue bytes, and readiness. Readiness
is advisory; the expiry, fence, range, bucket, and ownership checks still run
at promotion. Events are `hot_recovery_certified` and `hot_recovery_promoted`.

Run the real three-process fault suite with:

```sh
cargo build -p celld --bin celld
python3 scripts/recovery-import/run.py --mib 600 --hot-recovery
```

The hot owner-kill case requires warm followers before the fault, promotion
within 30 seconds, a promotion event, and zero peer-tail bytes before the first
recovered read. It then resumes writes, restarts the old owner, verifies every
client digest through every node, and checks SQLite, GC, and retirement.
`--scenario missing-witness --hot-recovery` exercises the cold fallback after
correlated restart. Measurements are recorded in `docs/testing.md`.

This feature is not qualified against a live supported S3/GCS/Azure store yet.
Keep it opt-in until provider behavior and additional object-store write cost
have been measured. External upstream simulation suites are also unavailable
in this checkout; the fork's adapter regressions are independently runnable.
