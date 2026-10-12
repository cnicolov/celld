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
    Seeding does not wait for a quiet writer: appends received during download
    remain in the bounded checkpoint queue and are replayed with the normal LTX
    gap/checksum checks before the image becomes usable. Replayed commits are
    not checkpoint-covered until their bucket publications succeed. The replay
    also checks peak image size, including growth followed by truncation.
    In-flight jobs are tied to an image incarnation, so a seed from a replaced
     fragment cannot overwrite or discard its successor cache.
     The seed's whole-file checksum scan runs outside the cache mutex so cache
     status and follower append observations remain responsive during multi-GiB
     validation. Installation reacquires the lock and rechecks the image
     incarnation and bounded queued tail before applying it. A fragment change
     during the scan fences its result just like one during download; scan
     completion grants no coverage to commits not yet published to the bucket.
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

Hot preparation uses the same process-local session single-flight lock as cold
recovery. A multi-GiB checksum/prefix check can outlast a core operation deadline;
its retry waits and reuses only a still-live certificate for the same session,
log fragment, and cell epoch. The eager session sweep skips an in-flight hot
preparation, avoiding a cold activation that consumes the promotion before the
certificate is installed. Certificate expiry and all lease/CAS proof checks
remain unchanged; this serializes local work, not cross-node ownership.

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
Image materialization and checkpoint coverage are separate: continuous writes
can outrun the serialized checkpoint publisher even though replication and the
private SQLite image keep up. An uncovered image is not eligible for hot
promotion; ordinary recovery remains the fallback.
Even `ready=true` certifies only the commits observed by that cache. Bucket-only
acknowledgements during degraded replication can advance the owner beyond it;
the canonical-prefix equality check rejects that stale image at promotion.
The eager-restore debug qualification also exposed repeated log rotations when
full-snapshot materialization outlasted the append/stall envelope. Moving that
advisory work off the acknowledgement path would require a bounded, ordered
worker with preserved seal/coverage proofs; it is not fixed or bypassed here.

Run the real three-process fault suite with:

```sh
cargo build -p celld --bin celld
python3 scripts/recovery-import/run.py --mib 600 --hot-recovery
```

For a RustFS-backed multi-GiB owner-crash test, first complete the large
[follower/rejoin workload](../placement.md#follower-disappearance-and-rejoin-during-continuous-writes),
then reuse its **stopped, passing test-owned** dataset instead of allocating
another full copy:

```sh
python3 scripts/hot-owner-failover-smoke.py \
  --source-state /path/to/celld-hot-follower-rejoin-ARTIFACTS \
  --endpoint http://127.0.0.1:19000 --standby-mb 8192 --timeout 900
```

This starts fresh processes on those same recovery disks, so old process-local
cache certificates cannot be reused. It warms two full-size images from real
new commits, then runs two **owner SIGKILL** cases. The checkpointed case stops
client writes and drains checkpoint coverage before the crash. It compares
standby TXIDs to the owner's captured local LTX position and checks the last
acknowledged row in both quiet images; sparse new marker commits may be needed
to refresh followers after a fragment repair. It must emit
`hot_recovery_certified` and `hot_recovery_promoted` before the first independently
verified recovered read, with zero cold peer-tail requests or response bytes
before that read. The continuous case keeps unique writes going through a surviving ingress
and labels the observed recovery path as hot or cold, rather than mistaking safe
fallback for a hot-promotion pass.

After each crash, every original preload digest and acknowledged later write
is verified through both survivors and again through the returning old owner.
The old owner must not change the successor's ownership fence. Resource phases,
first-read errors/time, hot events, sampled owner epochs, and peer-tail counters
are preserved in the new report. Verification groups eight original 1 MiB
receipts into bounded 8 MiB responses; each original receipt is still checked.
The test redeploys only its own fixture to permit that read size. Its peer proxy
forces one ordinary HTTP request per connection so keep-alive reuse cannot hide
a later tail request; cell-RPC upgrade handshakes remain unchanged. These
transport settings and local debug timings are not production benchmarks.
If retrying a completed reuse, pass each previous run with `--previous-state`
so all earlier acknowledgement receipts remain checked and every attempted ID,
including ambiguous failures, remains reserved. `--paged-min-mib` is an explicit
test-path override recorded in the report; omit it for the default threshold.
Source and execution binaries must match by default. `--binary-change-reason`
explicitly permits a rebuilt binary and records both hashes and the reason;
all nodes still run one binary, so this does not test a mixed-version rollout.

Owner verification uses a 120-second explicit read deadline for both preload
and acknowledged tail, and records range-level progress/failures. Workload write
deadlines are unchanged. `--verification-only` can audit all receipts from
stopped prior attempts without another workload write or fault; it is recorded
as **post-restart verification**, never as a replacement owner-crash result.
The harness launches every fleet node before waiting for Worker readiness:
predecessor recovery can require another restarting node's original witness
disk. Sequential readiness waits would prevent that witness from ever starting.
Cleanup return codes are part of the final report and must be zero, so a
self-fence, shutdown abort, or forced kill is not hidden behind successful
digest events. The sampler times its existing bucket-owner read independently
of node lease effects, without extra requests, to help distinguish backend
latency from in-process delay. The paged hydrator's open file pins
its registration through each step, allowing concurrent unregister to cancel
that background step safely rather than dereferencing a vanished name lookup.

The recorded 4 GiB checkpointed fault passed genuine hot promotion and full
digest/fence checks. Its busy fault recovered cold, but in-run verification
timed out before the second returning-owner fence. A separate post-restart audit
subsequently verified all 279,214 known acknowledged rows through three ingresses
and exited cleanly with the unchanged 4-second TTL. That audit does not replace
the incomplete busy-fault result or resolve the earlier lease-contention failure;
its cold full-data read still took about 32 minutes. See
[testing results](../testing.md) for reports, timings, resource costs, and the
remaining limits.

```sh
python3 scripts/test-hot-owner-failover-smoke.py
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
