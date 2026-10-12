# Workflow subscriptions

Build the current source, then run the focused checks from the repository root:

```sh
cargo build -p celld --profile lab
node --test tests/workflow-subscribe/history.test.mjs
node tests/workflow-subscribe/run.mjs target/lab/celld
```

For detailed telemetry assertions against the local motel collector:

```sh
motel daemon
CELLD_SUBSCRIBE_OTEL=http://127.0.0.1:27686 node tests/workflow-subscribe/run.mjs
```

The base URL is reported by `motel endpoints`. The test starts celld with that
collector, a unique service name, parent-based root sampling, and a 100-ms flush. It checks
the received Workflow/Step/attempt hierarchy, timeout error status, retry intervals,
wait/sleep/pause spans, parallel occurrence identity, shared trace identity across
restart and SIGKILL, W3C parent propagation, parallel downstream fetches, and
console-log correlation after awaits, fractional durations, and unsampled propagation.

Check typed attribute preservation through the Parquet exporter:

```sh
cargo test -p celld --lib --profile lab workflow_telemetry_tests
```

For a persistent interactive demo, run `node scripts/workflow-motel.mjs`; see
[Workflow timelines](../../docs/telemetry.md#workflow-timelines) for example calls.

Node 22 or newer and `pgrep` are required. The integration harness allocates a free
local port and an isolated temporary project, starts `celld dev --no-watch`, kills
its supervised runtime child to exercise recovery, and cleans up its own processes
and state. It never uses the installed celld binary unless passed explicitly.

The integration suite covers:

- Historical catch-up and live reads over real Worker-to-cell RPC.
- Native retry errors/delays, dynamic retry configuration, and timeout errors.
- Result-size validation and failed retry-policy callbacks.
- Sleep, sleepUntil, event waits, and timed-out waits.
- Parallel and repeated Step names, and filtered multi-page catch-up.
- Cursor and filter validation; terminal events excluded by filters.
- Disposal during a pending read, repeat disposal, and concurrent-next rejection.
- Pause/resume, termination fencing, and monotonic history across explicit restart.
- SIGKILL recovery: completed checkpoints remain single-execution, incomplete
  callbacks re-enter the same attempt slot, and history survives the restart.
- Native-history WebSocket delivery and cursor reconnect.
- Retention expiry and instance deletion.

The storage-fault tests evaluate the actual engine helpers with injectable storage.
They verify ledger/history/cursor rollback together, no notifications on failed
commits, generation/termination fencing, durability-gated delivery, pending-reader
cleanup, routed cancellation arriving before a read, and expired/recreated instance
identity. They complement the real-runtime checks rather than replacing SQLite/RPC
coverage. Multi-node takeover and residency/capacity measurements require a fleet
test environment and are not exercised by this local suite.
