# Testing

celld makes three promises:

- An acknowledged write is durable.
- A cell has one writer at a time.
- Code written for Cloudflare Workers and Durable Objects operates the same on
  celld.

We test the API contract by differential execution against workerd, the
coordination protocol by model checking and deterministic simulation, and the
full system by fault injection on live fleets.

## Conformance: two runtimes, one output

We run the same Workers and Durable Objects programs on workerd, Cloudflare's
production runtime, and on celld, and the outputs must match. workerd supplies
the reference output, so the expected behavior does not depend on celld.

Each new API adds fixtures to the corpus. We also port the workerd Durable
Objects contract tests, its web-platform globals tests, and the upstream Web
Platform Tests. Before a release, we replay storage, SQL, alarm, stream,
WebSocket, and lifecycle scenarios through the full `celld` binary in each
deployment mode.

## Specification: exhaustive at small size

The coordination protocol is specified in TLA+. Heyang Zhou wrote the
specifications against celld v0.1.0. His model checking found four bugs and a
split-brain that lost an acknowledged write. All are fixed. None had surfaced
in our own review or testing.

At a small configuration, the checker visits every reachable state. The model
grants a linearizable object store and perfect shared clocks, so a violation
needs no clock skew or storage anomaly. The invariants are one writer for each
epoch and no lost acknowledged write. The checker also verifies that the epoch
in the key stops a stale owner's late writes from losing an acknowledged write.

Each configuration pins an expected verdict, and most verdicts are failures:
each models a past protocol bug or a deliberately broken checker, and must
produce its counterexample.

A redesign of the write-acknowledgment fence was checked before it was built.
The check found an eight-state counterexample against the old version: a
dormant cell resumes at its old epoch while its release is in flight,
acknowledges a write, and the following takeover restores without it. The fix
shipped, and the counterexample stays as a pinned failure.

The checker also removed code. celld once sealed a cell's durable history at
restore. The verdicts showed that the seal only prevented the return of an
unacknowledged write, which celld does not promise, and that it could turn a
recoverable ordering error into permanent data loss.

We update the specifications by hand with protocol changes. They do not run in
continuous integration. A separate record tracks what the model does not yet
describe, including guarantees weaker than the code's.

## Simulation: the protocol under adversarial schedules

The dangerous bugs are in coordination: a crash during an ownership handoff, a
lease renewal that races a takeover, an alarm against a partially restored
cell. These windows are too narrow and too rare to wait for.

The coordination protocol is therefore a
[pure decision core](https://github.com/denoland/celld/tree/main/crates/logic)
with no I/O. The clock, the randomness, and the object store are interfaces
that a simulator drives. The simulated store injects latency, compare-and-swap
races, and lost responses. The clocks drift, and a node can crash at each
await point. Scripted adversaries play the cells, such as a handler that never
returns or a write stream that stops halfway. V8 is not deterministic, so it
stays out of the simulation.

A seeded scheduler drives each run, so a failure reproduces exactly. We check
safety (two writers in one epoch, a lost acknowledged write, an expired lease
that returns) and liveness (each armed alarm fires, ownership settles on one
node after a crash). A property must survive tens of thousands of seeds, and
the core protocols have run through millions of schedules. Deliberately broken
protocol variants verify that the checkers detect the faults.

## Live fleets: what simulation cannot see

Simulation cannot see real S3 tail latency, real kernel and filesystem
behavior, or V8 under memory pressure. A permanent fleet lab runs standard VMs
from standard providers against a real bucket. Workloads rotate: chat rooms
with many WebSocket connections, working sets that shift across tens of
thousands of checksummed cells, deployment cutovers under load, and nodes
filled to the memory limit. The lab qualifies each release. We archive each
run's configuration, verification sweeps, node journals, kernel logs, and
phase timings, including failed runs.

Faults land between verification passes. A pass fetches each cell through
different nodes and compares the status, the body, and the full message
ledger. A cell can be briefly unavailable while ownership moves, but its
committed state must stay complete and a live node must serve it again.

The scenarios:

- `SIGKILL` a node mid-write-stream and delete its local database, so recovery
  comes only from the bucket. Every acknowledged write returns, because the
  output gate held each response until the write was durable.
- Freeze an owner, write to its cells through other nodes, and unfreeze it.
  The node sees that its lease moved and refuses to serve the old state. Each
  write lands exactly once.
- Cut a node off from the bucket. It fences itself.
- Throttle the bucket to 429 on every request. The engine slows to the store's
  rate and does not amplify the throttle.
- Stop a full host at the provider level mid-workload. Its cells move to other
  nodes, and the returning host rejoins with no duplicate residency.

Across every run of every scenario, the verification sweeps show zero body
faults, zero status faults, and zero lost messages.

## A few numbers we trust

- **The epoch fence holds under contention.** Five hundred concurrent
  claimants made 5,500 attempts on the same cells: one writer for each epoch,
  zero violations.
- **A warm resident request is local.** A request to a resident cell does zero
  bucket operations and returns in p50 ~1.1 ms and p99 ~7 ms (a fixed-host
  measurement). Only a cold activation touches object storage.
- **A durable write waits for a durability proof.** A single node proves each
  write through the bucket, so one storage round trip is the minimum. A fleet
  of two or more nodes can instead prove a write when each follower holds it
  on disk. A lab fleet measured about 600 ms for a bucket proof and about 25 ms
  for a fleet proof. The bucket upload races every fleet proof, so a slow
  follower cannot make a write slower than a bucket proof. Concurrent writes
  to one cell share one upload.
- **A restore is normal work.** Placement treats the restore of an inactive
  cell as ordinary work. The measured restore times come from the retired
  external replicator, so this page gives no number until a fleet run measures
  `celld-ltx`.
- **Ten small nodes held real scale.** Ten nodes, each with 4 vCPU and 8 GB,
  held 10,000 resident cells and 20,000 concurrent WebSocket connections. With
  two of the ten nodes stopped, every cell was available again on another node
  in ~11 s at the tail (with reserve headroom).

## Fork recovery regressions and bulk import

The fork's adapter regressions run without the external simulation harness:

```sh
cargo test -p celld --lib recovery_tests
cargo clippy -p celld --all-targets -- -D warnings
```

They cover persisted identity, incomplete/slow paged tails, claim heartbeats,
missing witnesses, unreadable retained bundles, bundle-before-tail ordering,
same-cursor retries, superseded-fragment GC with stale-append fencing, disk
obligations, and the production S3 client's HTTP 409/412 classification and
renewal outcome.

Hot-recovery regressions additionally cover checkpoint coverage, exact range
and epoch matching, live-lease rejection, newer bucket-only commits, LTX
growth/truncation and gaps, cache damage/restart, prepared-image budgets, and
bounded canonical seeding when the first observed entry is a delta. A missing
canonical baseline also withholds delta publication, preventing GC watermarks
from advancing over a hole. Seed regressions exercise a nonempty checkpoint
queue, writes arriving during the canonical download, replay gap/checksum
rejection, peak disk-budget enforcement across growth and truncation, and an
in-flight seed racing fragment replacement:

```sh
cargo test -p celld --lib hot_recovery
```

The bulk-import suite launches **three actual celld processes**, deploys a
Durable Object, and imports high-entropy payloads into SQLite BLOB rows. It
uses the SQLite-backed local development object store for fleet CAS and a TCP
peer proxy for controlled recovery bandwidth. No Docker service is required.
Rust, Python 3.8+, and esbuild on `PATH` are required.

```sh
cargo build -p celld --bin celld

# 600 MiB of application payload; retained LTX tails are usually larger.
python3 scripts/recovery-import/run.py --mib 600

# Delayed witnesses: lose the owner and both followers, then restore their disks.
python3 scripts/recovery-import/run.py --mib 600 --scenario missing-witness --port 19970

# A smaller run whose individual pages take longer than the old 10s deadline.
python3 scripts/recovery-import/run.py --mib 8 --tail-kib-per-second 256

# Warm SQLite images + canonical checkpoints; require hot promotion.
python3 scripts/recovery-import/run.py --mib 600 --hot-recovery --port 21470

# Restart all processes: standby certificates must not bypass startup recovery.
python3 scripts/recovery-import/run.py --mib 8 --hot-recovery --scenario missing-witness --port 21570
```

The suite paces bucket tiering at one hour using `CELLD_LOG_FLUSH_MS`, requires
a two-follower ensemble, and checks that **each actual retained tail is at
least 90% of the imported payload** before injecting the fault. A run that
failed to create large peer files fails this gate. The owner is killed after
the large import portion and before a final post-failover batch. Its local
database/cache is discarded while `node-id` is retained, forcing peer recovery.

The missing-witness scenario restarts the owner first, asserts that serving
remains blocked for 30 seconds (past the old long-dead grace), with no automatic
loss record or successor lease, then returns both followers with their original
identities and disks. Every acknowledged batch has a client-generated SHA-256
digest recorded in an fsynced ledger. The suite
reads every batch through **each node** and compares its complete bytes, resumes
the import to catch sequence reuse, checks SQLite using live `quick_check` and
offline `integrity_check` on SQLite backup images, verifies old-tail GC, and
runs the disk retirement command before and after graceful shutdown.
It invokes the production follower-GC sweep through the internal operator
listener instead of waiting for its ten-minute periodic timer.

The missing-witness case also makes an acknowledged write under **every
process's own log session**, after requiring two recorded followers for each
writer, before the fault. Those auxiliary cell writes have a separate fsynced
`witness-acks.jsonl` ledger and are verified through all
nodes. Each restarting process must therefore recover real data; inactive
empty logs cannot conceal a startup dependency cycle. Authenticated tail pages
must remain available on the startup listener while every node is recovering.

Default recovery bandwidth is 4 MiB/s, with a 4-second node lease and a
10-minute recovery-test deadline. `--tail-kib-per-second 0` disables the throttle;
`--recovery-timeout` changes the test deadline. `--port` selects the base of the
public, internal, and proxy listener ports. `--output` selects a **new** artifact
directory; the default is a uniquely named directory in the platform temp root.

`--hot-recovery` enables the feature on every node and waits for checkpointed
images before injecting the fault. In the owner-kill case it additionally
requires a promotion event, zero peer-tail bytes before the recovered read, and
crash-to-read recovery below 30 seconds. Subsequent background/startup recovery
still transfers the retained session tail; its GC and retirement checks remain
unchanged. See [the design](designs/hot-recovery.md) for lifecycle and cost.

Artifacts include the acknowledgement ledger, every node journal, per-second
RSS and disk/peerlog sizes, pre-crash leases, fault/verification events, and a
JSON report containing the binary SHA-256, import/recovery times, and memory peaks. Failed runs are
retained. Qualification against real S3/GCS/Azure still requires a live fleet:
the local test exercises production recovery adapters but not provider behavior.

### Measured bulk-import runs (2026-10-10)

The 600 MiB runs below passed on macOS using the debug binary and the local
development store. The first two ran concurrently on the same host. Tail
traffic was limited to 4 MiB/s. These are fault-recovery measurements, not
production latency targets.

| Scenario | Imported payload | Retained tail on **each** follower | Tail pages | Crash-to-read recovery | Recoverer's peak RSS during recovery |
| --- | ---: | ---: | ---: | ---: | ---: |
| Owner killed, local owner cache discarded | 600 MiB | 647,257,854 bytes | 203 | 262.3 s | 262 MiB |
| Owner and both witnesses killed; witnesses returned after 14 s | 600 MiB | 647,257,854 bytes | 203 | 286.2 s | 247 MiB |
| All three sessions active; witnesses returned after 32 s | 600 MiB | 647,257,854 bytes | 203 | 298.2 s | 245 MiB |
| All-active repeat with enforced two-follower preconditions | 600 MiB | 647,257,854 bytes | 203 | 297.9 s | 265 MiB |

For each run, all **601 acknowledged batches** (600 MiB plus a 1 MiB
post-failover batch, 38,464 rows) matched the client digest through all three
nodes. Live SQLite `quick_check` and full `integrity_check` on a backup of the
recovered owner image returned `ok`. Old-session follower payloads were
reclaimed, a live node's disk was refused retirement, and all three stopped
disks passed the retirement proof after graceful shutdown. RSS was sampled
once per second; the recovery peak includes restore and excludes the later
full-data verification pass.

Each all-active run additionally verified two acknowledged auxiliary batches
through all three nodes and checked five SQLite backup images. Both small
writer sessions recovered from the remaining witness disks while the large
owner session was pending, exercising authenticated paged reads on recovering
nodes. The pre-crash lease snapshot confirms two followers under each writer.

Full artifacts were retained in the platform temp `opencode` directory:

- `celld-import-600m-owner-2/`
- `celld-import-600m-witness-1/`
- `celld-import-600m-active-witness-1/`
- `celld-import-600m-active-witness-2/`

The current missing-witness harness waits **30 seconds**, extending the
blackout past the old long-dead grace. The focused adapter regressions also
test fully expired witnesses, corruption, cursor retry/order, and claim expiry.

The strengthened **8 MiB slow-link** case also passed: all three process
sessions had acknowledged writes and two recorded followers before the fault.
Serving stayed blocked for 32 seconds without the witnesses; crash-to-read
recovery took 100.8 seconds at **256 KiB/s**. The main 14,917,986-byte tail took
58.8 seconds over six pages, with individual pages exceeding the old whole
request timeout. All 9 main batches and 2 auxiliary batches matched through
each node. Five backed-up SQLite images passed full integrity checks, GC
reclaimed the old main-session tails, and all stopped disks passed retirement.
Artifacts: `celld-import-active-slow-witness-2/`.

### Hot-recovery measurements (2026-10-11)

These opt-in runs also use the debug binary, local development store, and
4-second node leases; they are not production latency targets. The
small owner-kill case recovered **8 MiB in 4.26 seconds**, with zero peer-tail
bytes before promotion. Every node verified all 9 acknowledged batches,
SQLite checks passed, and GC/retirement preserved the session obligations.
Artifacts: `celld-hot-8m-1/` in the platform temp `opencode` directory.

The first **600 MiB** hot owner-kill run recovered in **8.47 seconds**, versus
the earlier **262.3-second** cold case (about 31x faster under this test setup).
Each follower retained a 647,257,854-byte log and a 648,908,800-byte materialized
SQLite image. Promotion transferred **zero** peer-tail bytes and the promoted
node peaked at **106 MiB RSS** during the recovery phase. Import took 72.20
seconds; materialization and additional checkpoint writes have a real cost.
The run co-ran with the small slow-link suite, so this is not an isolated
throughput comparison.

All **601 acknowledged batches** matched the client digest through all three
nodes, including the post-failover write. SQLite checks, old-session GC, and
retirement passed. The original owner subsequently recovered its session in
the background/startup path; hot promotion does not erase that obligation.
Artifacts: `celld-hot-600m-1/`.

The final-build **600 MiB repeat**, including the canonical-baseline publication
guard, also passed: **8.78 seconds**, one hot promotion, **zero** peer-tail bytes
before the recovered read, and **109 MiB** promoted-node peak RSS during recovery.
Import took 69.61 seconds. Both followers again retained the same large log and
SQLite image sizes; all 601 batches matched through every node, and SQLite,
GC, and retirement checks passed. Artifacts: `celld-hot-600m-2/`. Its binary
SHA-256 matches the final correlated-restart run below.

With hot recovery enabled, a correlated restart still used conservative cold
recovery: **100.37 seconds** for 8 MiB at 256 KiB/s, including a 32.01-second
witness blackout. All three writers had acknowledged writes and two followers
before the fault. All 9 main and 2 auxiliary batches matched through each node;
five SQLite backups, GC, and retirement checks passed. Artifacts:
`celld-hot-slow-witness-1/`.

The final-build repeat, including the canonical-baseline publication guard,
passed in **101.40 seconds** with a 32.01-second blackout. Its harness explicitly
asserted **zero hot promotions** after the correlated restart, then verified
all main/auxiliary data, five SQLite backups, GC, and retirement again. Artifacts:
`celld-hot-slow-witness-2/`; its report records the tested binary's SHA-256.

### Hot follower disappearance/rejoin on RustFS (2026-10-12)

```sh
python3 scripts/hot-follower-rejoin-smoke.py \
  --endpoint http://127.0.0.1:19000 --down-seconds 60
```

This launches three real same-version processes in one synthetic `gpu` placement
pool against an isolated RustFS bucket. After both followers are hot, four
clients continuously write unique 16 KiB SQLite rows, targeting 20 writes/second.
One confirmed follower is SIGKILLed, kept down for a minute, and restarted on
the same address, identity, and recovery disk. The owner is never restarted.

The final debug-build run passed with **2,573 acknowledged rows, zero request
errors, and no missing or corrupt acknowledged payloads**. The stopped follower
was absent for 60.27 seconds; **1,180 writes** completed during that measured
window. The surviving owner's ensemble shrank to one follower in **4.32 seconds**
and returned to two followers **27.81 seconds** after restart began (including
1.84 seconds of startup). That wait reflects the normal 30-second recruitment
cadence, not a cell ownership transfer.

The restarted node rebuilt a SQLite image through the pre-restart canonical
TXID by **30.12 seconds**, while writes continued. At that observation its image
was at TXID 1,786 and its checkpoint was at 1,762: materialization is not the same
as hot-promotion readiness. Advisory readiness was first sampled around **31.2
seconds** after restart. At the end of a further 15-second restored-load window,
both images and checkpoints reached TXID 2,049 and both checkpoint queues were
empty. The final readiness check needed only 3 ms after traffic stopped.

All 2,573 payloads matched independent client SHA-256 digests through **each of
the three ingress nodes** and in copies of **both private standby databases**;
both copies passed SQLite `quick_check`. The cell stayed on `gpu-a`, epoch 1,
across 128 sampled ownership records and the phase-boundary checks. One operator
probe failed during restart; this was a sampling error, not a workload error.
Overall write latency was p50 **41.0 ms**, p95 **94.0 ms**, p99 **278.4 ms**, and
maximum **595.2 ms**. These local debug timings are not production targets.

This workload exposed and now regression-tests two seed-liveness problems:
waiting for an empty checkpoint queue under continuous writes, and discarding
a downloaded seed whenever its expected TXID moved. The worker now seeds after
a published prefix and replays the existing bounded queued tail without falsely
crediting its bucket coverage. Image-incarnation fencing also prevents a stale
seed/failure from overwriting or discarding a replacement cache. Gap/checksum,
promotion-range, canonical-prefix, and peak disk-budget checks remain enforced.

Artifacts are retained in the platform temp `opencode` directory:

- Passing run: `celld-hot-follower-rejoin-m8p7ec6y/`, bucket
  `celld-follower-rejoin-13567bd000a2`. The report records binary SHA-256, events,
  phase latency/error counts, rejoin timings, and copied standby evidence.
- Initial pre-fault seed-starvation failure: `celld-hot-follower-rejoin-wpm8wg8t/`.
- Intermediate outage/rejoin run with checkpoint lag and a failed strict
  readiness-under-load gate: `celld-hot-follower-rejoin-2lyb74y5/`.

All test-owned processes were stopped; existing demos were not redeployed or
restarted. This qualifies a same-version follower crash/rejoin under this local
workload, **not** mixed-version rolling upgrades, physical GPU execution,
unlimited checkpoint throughput, or live S3/GCS/Azure provider behavior.

### 4 GiB follower disappearance/rejoin on RustFS (2026-10-12)

```sh
python3 scripts/hot-follower-rejoin-smoke.py \
  --binary /path/to/celld --preload-mib 4096 --standby-mb 8192 \
  --down-seconds 60 --after-seconds 30 --warmup-timeout 600 \
  --rejoin-timeout 900 --checkpoint-timeout 900
```

The same-version local debug run passed with **4 GiB of incompressible imported
application data** (262,144 rows), followed by **15,035 acknowledged 16 KiB
writes, zero request errors, and no missing or corrupt acknowledged data**.
Preload took 694.01 seconds. All **277,179 acknowledged rows** matched the
independent client SHA-256 receipts through every ingress and in both copied
standby databases; both copies passed full SQLite `integrity_check`. Each final
standby occupied 4,683,649,024 bytes (4.36 GiB). The owner stayed `gpu-a`, epoch 1,
across 1,574 ownership samples and the phase-boundary checks.

The follower was down for 61.19 seconds before restart began. The ensemble shrank
in **3.33 seconds**, returned to two followers **17.03 seconds** after restart
began, but rebuilding through the pre-restart canonical TXID took **688.91
seconds** while writes continued. At that point the rebuilt image was at TXID
17,033, with checkpoint coverage only through 14,963 and `ready=false`. After
another 30-second write window, both caches again reported `ready=false` and
nonempty queues. They became fully checkpointed **107.46 seconds after writes
stopped**, through TXID 17,807, with both queues empty. This is a data-integrity
and eventual-readiness pass, **not continuous hot-promotion readiness**.

Overall write latency was p50 **47.77 ms**, p95 **114.78 ms**, p99 **456.91 ms**,
maximum **2,857.28 ms**. Sampled total-process RSS peaked at **8.40 GiB on the
owner** during rejoin, versus about **302 MiB during preload** and **249 MiB on
each follower** during the later rebuild. The owner-memory spike and long image
rebuild are unresolved scalability limits; bounded follower seed streaming does
not imply bounded owner checkpoint/compaction memory. These measurements are
sampled process RSS, not per-cell memory or production latency targets. Twelve
operator probes timed out during rejoin; none were workload request failures.
Minimum sampled host free space stayed above 33.53 GiB after the earlier cleanup.

Artifacts: `celld-hot-follower-rejoin-wip8q3ia/`, bucket
`celld-follower-rejoin-ad7c61c2e25c`, in the platform temp `opencode` directory.
The source report records binary SHA-256
`49539482fbf96cea17668e7a4b6fd0fe799c24df8f6bbf523d16b584e0749a31`.
Reports, receipt ledgers, samples, logs, and both verification copies are retained.
All test-owned processes stopped. This case does **not** establish owner hot
failover; `scripts/hot-owner-failover-smoke.py` separately kills the actual owner
and reuses only this stopped, passing suite-owned dataset.

### Owner-crash harness setup regression (2026-10-12)

The first reuse of the passing 8 MiB follower dataset timed out **before any
owner fault or new workload write**. Both followers had joined, but the harness
required the owner's wire `log.active` flag before sending its seed write.
That legacy wire name means **may_have_fleet_acks**, not shipper liveness; a
read-only activation cannot set it. Fragment repair resets the flag as well,
as the second setup attempt (`celld-hot-owner-failover-3ys65w4e/`) demonstrated.
Recruitment gates now require an open two-follower ensemble, not acknowledgement
history; the measured faults retain the full checkpoint/image gates. A regression
asserts read-only recruitment, missing-member rejection, and sealed-log
rejection. No production Rust change was needed for this setup failure.
Artifacts: `celld-hot-owner-failover-jldu7vvl/`; this failed setup is not an
owner-recovery qualification result.

### Small owner-crash validation and eager-restore limit (2026-10-12)

The first actual checkpointed owner kill (`celld-hot-owner-failover-ia57jc3j/`)
recovered the latest acknowledged row in **4.49 seconds**, but correctly used
**cold** recovery: 2 peer-tail requests and 9,336,597 response-wire bytes, with
no hot certification/promotion. The caches advertised `ready=true` through TXID
9, while the owner's canonical bucket chain was newer. Bucket-only acknowledgements
had outrun the observed follower image during fragment degradation. The harness
now compares cache coverage with the owner's actual epoch-scoped local LTX
position and checks the latest acknowledged row inside each quiet standby.
Cache-local advisory readiness alone is never sufficient evidence.

Sparse catch-up writes still could not stabilize the default eager-restore
case (`celld-hot-owner-failover-v1o3kh9u/`): **108 additional writes were
acknowledged**, but the log repeatedly reconfigured (38 rotations in roughly
three minutes), and the strict checkpoint gate timed out **before the next
owner fault**. A restored eager baseline's full snapshot repeatedly exceeded
the debug deployment's append/stall envelope while synchronous standby page
materialization delayed replies. This path remains an unresolved performance
limit; no lease deadline or safety check was weakened to obtain a pass.

The path-matching rerun used **`--paged-min-mib 1`**, explicitly selecting the
paged restore path that the 4 GiB dataset selects at the default 256 MiB
threshold. It reused the original 8 MiB incompressible preload plus the existing
rows and all acknowledged receipts from prior attempts; it did not replay
ambiguous request IDs. This is **not a pass for the small eager-restore default**.
Both actual owner SIGKILL cases passed:

| Case | Crash-to-first-verified-read | Path | Cold peer-tail requests / bytes | Known acknowledged rows checked through all 3 nodes |
| --- | ---: | --- | ---: | ---: |
| Checkpointed | 3.398 s | Hot | 0 / 0 | 2,727 |
| Continuous writes | 3.698 s | Hot | 0 / 0 | 2,931 |

Each case emitted one certification and one promotion before the recovered
read, advanced ownership (`gpu-a/e7` → `gpu-b/e8` → `gpu-a/e9`), verified every
original import digest and acknowledged subsequent row through both survivors
and the returning old owner, and checked the returning owner's fence and SQLite
`quick_check`. Sampled owner epochs showed one owner each. The final rebuild
marker was also verified through all nodes (2,932 known acknowledged rows total).
The continuous case attempted 264 requests: **203 acknowledged, 61 failed**;
the failure window was not described as zero-error availability. Checkpointed
warmup/resumed traffic acknowledged 99/98 writes without errors.

Sampled promoted-node RSS peaked at **92.77 MiB** in the checkpointed fault and
**97.47 MiB** in the continuous fault. Later bounded 8 MiB response verification
peaked at **416.16 MiB**, so these are phase-specific process measurements, not
a universal memory cap. Minimum sampled host free space stayed above 37.97 GiB.
Artifacts: `celld-hot-owner-failover-hxbdwsel/`, reusing bucket
`celld-follower-rejoin-9736bef5bc71`. All its processes stopped. The separate
4 GiB owner suite uses **no paged-threshold override**; its results must be
reported independently rather than inferred from this small validation.

### Multi-GiB seed scan lock regression (2026-10-12)

The first 4 GiB owner suite (`celld-hot-owner-failover-cykdia85/`) built both
4.36 GiB standbys, then timed out after **900 seconds of checkpoint catch-up,
before either owner was killed**. Warmup acknowledged 182 writes and sparse
catch-up acknowledged another 713, with zero workload request errors. The
retained ledger includes the setup marker and all **278,075** known acknowledged
rows. The log opened 10 ensembles, operator cache probes timed out, sampled RSS
stayed below 0.36 GiB per process, and minimum sampled host free space was 37.08
GiB. This is a failed readiness qualification, not an owner-failover result.

Inspection found a separate mutex-liveness bug: installing a downloaded seed
re-read and checksummed the whole multi-GiB file **while holding the cache
mutex**. The deterministic regression pauses that scan and demonstrates blocked
status/append observations before the fix. The scan now runs outside the mutex,
then rechecks the exact image incarnation, current queued tail, and peak disk
budget before installation. Replayed jobs still need bucket coverage. A second
regression changes fragments during the scan and proves its obsolete result
cannot overwrite or discard the replacement image. All 73 celld library/binary
tests and 13 Python harness regressions passed after this change.

The old test binary was retained; the rebuilt binary SHA-256 is
`83fa1f302ecfea6a43310d44ca10a4b4d7acc72d28e80b431e89db94a13d04ba`.
Reusing a dataset with another binary now requires **`--binary-change-reason`**;
reports record that reason and the source/execution hashes. Every process in
each rerun uses the same rebuilt binary. This is not a mixed-version rollout
qualification. Prior-attempt receipt ledgers are supplied with `--previous-state`.

The corrected binary passed the small paged-path validation again
(`celld-hot-owner-failover-cp9n5am6/`, `--paged-min-mib 1`): checkpointed hot
recovery **3.703 s**, continuous-write hot recovery **3.615 s**, one certification
and promotion per case, **zero cold peer-tail requests/bytes**, all acknowledged
digests through three ingresses, and both returning owners fenced. The final
marker brought the checked total to 3,332 rows. Continuous traffic acknowledged
200 of 257 requests (57 failed during the fault window). The default small
eager-restore limit remains unqualified. The 4 GiB rerun is separate and uses
no restore-threshold override; moving this scan alone did not prove its complete
readiness/recovery loop. The subsequent outcomes are recorded below.

### Slow hot-preparation / recovery-sweep race (2026-10-12)

With the seed-scan lock fix, the 4 GiB rerun
(`celld-hot-owner-failover-ui0bp11n/`) reached exact owner/standby coverage at
TXID **18,848**, with both queues empty and the latest acknowledged row present
in each private image. It acknowledged 186 warmup writes without errors, then
SIGKILLed `gpu-a/e3`. The verified latest row returned through `gpu-b/e4` in
**26.773 seconds**, but the required hot gate failed: **one certification,
zero promotions**, and **one cold-tail request / 19,084 response-wire bytes**.
The full post-fault digest verification, old-owner return, and continuous-write
case were **not reached**. The ledgers retain 278,262 known acknowledged rows;
this run does not claim all those rows were reverified after its fault.

The trace showed the eager whole-session sweep sealing the session while the
cell's hot preparation was still verifying the multi-GiB image. A retry/cold
activation consumed the absent promotion before its late certificate was
installed. The paused-preparation regression reproduced this ordering: the
session became `Sealed` and the retry bypassed the pending hot preparation.

Hot preparation now shares the existing **session recovery single-flight lock**.
Retries wait, then reuse only an unexpired prepared image matching the exact
session generation, log fragment epoch, and cell epoch. Same-process eager
sweeps skip that in-flight session rather than gathering a cold tail underneath
the certificate. The regression passes with zero peer pages and a retained
usable promotion. Operation deadlines, lease/CAS checks, checksum validation,
canonical/bundle proofs, and certificate expiry were **not relaxed**.

The rebuilt binary SHA-256 is
`dab6b184f7e5d0136993572d7a9b37de0497a6aebb07c7de257e7c2f5cc5a21c`.
All **74 celld library/binary tests**, **13 Python regressions**, formatting,
and Clippy (`monitor-ui`, all targets, `-D warnings`) passed. The small paged-path
rerun (`celld-hot-owner-failover-fhpk9l6x/`, explicit `--paged-min-mib 1`) also
passed both actual owner crashes: **3.943 s checkpointed**, **3.511 s continuous**,
one certification/promotion each, zero cold peer-tail traffic, all acknowledged
digests through every ingress, and returning-owner fences. Its final checked
total was 3,737 rows. Continuous traffic acknowledged 202 of 257 requests
(55 failed in the fault window). All small-run processes stopped. The separate
4 GiB original-scenario rerun retains both prior large-attempt ledgers and uses
the default restore threshold; its independent outcomes are recorded below.

### 4 GiB owner crash outcomes: hot pass, busy verification incomplete (2026-10-12)

The next default-threshold RustFS run
(`celld-hot-owner-failover-9_aql7mx/`, binary `dab6b184…`) passed the actual
**checkpointed 4 GiB owner SIGKILL**:

- Both images matched the owner's captured TXID **19,038**, with checkpoint
  coverage and empty queues; each image occupied 4,705,312,768 bytes.
- Ownership advanced **`gpu-a/e5` → `gpu-b/e6`**. The first verified recovered
  read completed in **25.621 seconds**, after one hot certification and one
  hot promotion, with **zero cold peer-tail requests or response-wire bytes**.
- All **278,757 known acknowledged rows** matched independent client digests
  through both survivors and the restarted old owner. The returning owner
  remained fenced; the current owner stayed `gpu-b/e6`, and SQLite `quick_check`
  returned `ok`. Warmup/resumed traffic acknowledged **197 / 296** writes with
  no request errors in those windows.
- Sampled promoted-node RSS peaked at **89.78 MiB during the fault**, and
  **676.33 MiB during subsequent full verification**. The slow checksum proof
  and local debug timings are not production latency targets. Verification
  through the promoted node took about 15 minutes; it is separate from the
  first recovered read.

The **continuous-write owner crash was not a complete qualification pass**.
It advanced **`gpu-b/e6` → `gpu-a/e7`** and verified the latest pre-crash
acknowledged row in **9.723 seconds via cold recovery** (one tail request,
20,838 response-wire bytes; no hot certificate/promotion). Both standby images
were unready before this fault, with nonempty queues, so no hot-ready claim is
made. Traffic attempted 535 requests: **456 acknowledged, 79 failed**, including
the fault's unavailability window. There were **279,214** known acknowledged
rows when writes stopped.

All original 4 GiB preload digests passed on the recovered ingress before the
subsequent small tail-row verifier timed out on a **15-second implicit HTTP
deadline**. No digest mismatch or missing row was reported, but the remainder
of the acknowledged tail, other ingresses, and second returning-owner fence
were **not fully verified**. The original report remains `passed=false`.
Cold fault RSS peaked at **463.05 MiB**, later verification at **1,061.84 MiB**;
minimum sampled host free space was **24.45 GiB**, with no reserve violation.
The full digests are not inferred from the successful first recovered read.

Cleanup then exposed a **separate paged-hydrator null-pointer abort** on
`gpu-a`, at `crates/ltx/src/paged_vfs.rs`: VFS unregistration raced a repeated
name lookup inside the page loop. All suite processes subsequently stopped;
the failure, receipt ledgers, samples, and logs remain retained.

### Hydrator retirement and post-restart audit (2026-10-12)

A subprocess regression reproduces the exact non-unwinding abort by retiring
the registration between its initial lookup and next page iteration. The
hydrator now uses the registration **pinned by its own open main file**, rather
than repeatedly looking it up by name, and treats retirement as a controlled
`vfs is gone` error. The same subprocess test passes and also checks normal
hydration progress. This fixes the observed shutdown race, not the separate
large-data read-latency limit.

Verification reads now use explicit deadlines: **60 seconds** in the follower
suite and **120 seconds** consistently for both preload and tail in the owner
suite. Owner verification records every requested range and receive/failure in
`verification-reads.jsonl`; workload write deadlines and all recovery safety
checks remain unchanged. The owner report now records cleanup return codes and
requires clean zero exits: self-fencing, aborts/segfaults, forced cleanup kills,
or cleanup errors cannot make a passing report. Failed reports are not rewritten.

The rebuilt binary SHA-256 is
`77b49dbe0512aaf536c79c11e78e9781c27e29452b6bc4d33705e3f0cba94f1e`.
All **75 celld library/binary tests**, **20 Python regressions**, formatting,
and Clippy for `celld` and `celld-ltx` (all targets, `-D warnings`) passed. Small
post-restart verification (`celld-hot-owner-failover-ps9g1_78/`, explicit paged
threshold) checked all 3,737 known acknowledged rows through every node and
SQLite `quick_check`; all three process cleanup return codes were zero.
Additional verifier regressions cover a shared preload/tail deadline, precise
timeout range logging with no hidden retries, and a checkpointed hot gate that
rejects even a cold-tail request returning zero response bytes.

`--verification-only`, with every relevant `--previous-state`, starts a stopped
suite-owned dataset and checks all prior client receipts **without new workload
writes or owner faults**. Its report mode is `post_restart_verification`, not
`owner_faults`. The first large audit (`celld-hot-owner-failover-dnu820xk/`)
failed **before any verification**: startup waited 900 seconds for `gpu-a`'s
Worker health before launching `gpu-b`/`gpu-c`. The predecessor required its
original witness `gpu-c`; production recovery correctly refused to seal or
discard acknowledged writes with that witness offline. The process was stopped
by the startup failure handler, and no suite processes were left running. This
is a harness startup-order failure, not a digest result or relaxed recovery.

The regression now exercises the real measured-node construction/start path
with readiness dependent on launching all three peers; it failed with that
same dependency before the fix. Fleet startup now launches **all three peer
listeners before waiting for any Worker health**. Individual returning-owner
startup still waits synchronously, and failed readiness leaves all launched
nodes available to the report's cleanup loop. The sampler also starts before
fleet boot so startup failures retain resource evidence. Small live verification
(`celld-hot-owner-failover-uxpwshm6/`) rechecked all 3,737 rows through every
ingress, with one sampled owner per epoch, `quick_check=ok`, and all cleanup
return codes zero.

The corrected-startup large audit (`celld-hot-owner-failover-h411qrly/`, same
binary and default restore threshold) reached `gpu-a/e8` and verification, then
failed when the node **self-fenced with exit code 3** at its proven 4-second lease
deadline. It verified **28 preload ranges / 14,336 rows / 224 MiB** before the
next range (`first=14336`, 512 rows) disconnected after 9.262 seconds. No digest
mismatch was reported, but none of the remaining preload, acknowledged tail,
other ingresses, or SQLite `quick_check` was fully verified in this audit.
It remains `passed=false`; every test process stopped, and the other two
processes exited zero. Owner RSS peaked at **870.25 MiB**, minimum sampled free
space was **31.64 GiB**, and no disk-reserve violation occurred.

All three nodes experienced slow/ambiguous renewals in the same interval.
`gpu-a`'s final CAS attempt returned ambiguous after 1,420 ms with 1,228 ms of
previous authority remaining; its ambiguity reconciliation did not finish before
expiry. The watchdog fenced at the expected deadline. Shared bucket/host
contention is a hypothesis, **not an established root cause**; the retained logs
do not separate bucket latency from runtime scheduling sufficiently to justify
a production change. The TTL, attempt bounds, and proof requirements were not
relaxed to make the audit pass.

The instrumented read-only audit **passed**
(`celld-hot-owner-failover-y_k7mmij/`) with the **same binary**, 4-second TTL,
and default restore threshold, retaining every prior large attempt:

- All **279,214 known acknowledged rows** (262,144 preload plus 17,070 tail)
  matched their original client digests through **all three ingresses**. Each
  ingress completed 780 bounded read ranges totaling 4,574,642,176 bytes; there
  were no failed ranges or digest mismatches. These are routed ingress checks
  of the recovered owner, not three independent local standby-image checks.
- SQLite `quick_check` returned `ok`. The sampled and final owner stayed
  **`gpu-a/e9`**, with no conflicting owner in that epoch. All three process
  cleanup return codes were **zero**, no self-fence/abort occurred, and no
  suite processes remained running afterward.
- The first ingress's cold full-data read took **1,926.760 seconds (32.11 min)**;
  subsequent ingresses took **26.669 / 26.793 seconds** against that hydrated
  owner. The background fill logged 1,151,863 pages, 6,596 faults, and 1,515
  steps in 1,928.054 seconds. This substantial cold-read cost remains a
  performance limit, not a production latency qualification.
- Sampled owner RSS peaked at **879.23 MiB**, versus **75.80 / 75.53 MiB** for
  the other processes. Minimum sampled free space was **30.69 GiB**, with no
  disk-reserve violation. After cleanup, about **35.37 GiB** was free.

The sampler times its existing ownership-object GET (`owner_read_ms`) and
whole sample collection (`collection_elapsed_ms`) without adding bucket
requests. Across 933 timed reads, the GET p50/p95/p99/max was
**4.454 / 9.796 / 22.509 / 59.430 ms**. There were no ambiguous renewals or
sampling errors in this successful attempt. Collection itself sometimes took
up to **12.917 seconds**, so resource sampling is not guaranteed 1 Hz. The
earlier shared-renewal slowdown was **not reproduced or fixed by this retry**;
its contention cause remains unresolved.

This is complete **post-restart acknowledged-data durability evidence**,
not a rewrite of either failed audit or the failed continuous-fault report:
it does **not** establish zero-error availability, resolve contention, or
complete the original second returning-owner fence. The actual checkpointed
4 GiB hot failover passed separately; the busy fault still recovered cold and
its original in-run verification/fencing qualification remains incomplete.

### What the larger recovery loop exposed

- The whole-tail decoder required the entire certified range in each body.
  Valid partial pages therefore needed their own `CLP1` wire format; a page
  cannot be accepted as whole-tail evidence.
- Fast individual pages repeatedly reset the heartbeat timer. A healthy
  209-second tail transfer could finish after its recovery claim had expired.
  Recovery now checks the monotonic heartbeat deadline at page boundaries as
  well as while awaiting slow I/O.
- A 30-second witness blackout outlasted the four-attempt startup ladder
  (about 24 seconds with the test's 4-second lease). The owner exited before
  its witnesses returned. Startup now keeps the predecessor obligation and
  retries with capped backoff, keeping its follower listener available.
- The startup-only listener omitted the new paged-tail endpoint. Making
  acknowledged writes under every process session before a correlated restart
  exposes that dependency; authenticated paged reads now run during startup.

The regression workload retains failed runs, including the acknowledgement
ledger and recovery journals, so these failures remain attributable and
repeatable rather than disappearing after a successful rerun.

On 2026-10-12, a user-requested space cleanup pruned the large database/payload
files from the stopped, passing `celld-hot-600m-{1,2}` and completed 600 MiB import
runs. Their reports, client receipts, logs, fixtures, and per-run
`cleanup-manifest.json` remain; those older raw database-copy paths are no longer
available. Three stopped small RustFS follower-test buckets were also removed
with `bucket-cleanup.json` records. Failed bulk-import evidence, the active 4 GiB
dataset, live demos, unrelated notes, and the shared build directory were not
removed. The detailed cleanup audit is `celld-space-cleanup-20261012.json` in the
platform temp `opencode` directory.

## Known failure edges

A fleet at its resident limit has no space for a lost node's cells, so losing
multiple nodes degrades service. We test recovery with and without reserve
capacity to measure this limit.

Report a schedule that breaks a guarantee, an untested fault, or a measurement
that you cannot reproduce in the
[issue tracker](https://github.com/denoland/celld/issues).
