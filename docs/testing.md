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
from advancing over a hole:

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

## Known failure edges

A fleet at its resident limit has no space for a lost node's cells, so losing
multiple nodes degrades service. We test recovery with and without reserve
capacity to measure this limit.

Report a schedule that breaks a guarantee, an untested fault, or a measurement
that you cannot reproduce in the
[issue tracker](https://github.com/denoland/celld/issues).
