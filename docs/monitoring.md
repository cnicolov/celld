# Built-in resource monitor

The monitor is a read-only, cluster dashboard. The runtime and HTTP API are
native Rust; the Leptos dashboard is Rust compiled to WebAssembly. Trunk generates
the browser loader, so there is no handwritten application JavaScript or npm
frontend. The HTML, CSS, loader and WebAssembly are embedded in the celld binary.

## Open it

`celld dev` enables monitoring by default and prints its private `/monitor/` URL
after the Worker is ready. Set `CELLD_MONITOR=0` to disable it.

For a regular node, set `CELLD_MONITOR=1` and use `/monitor/` on its
`--internal-listen` address. `/monitor/api` returns that node's measurements as JSON;
`/monitor/cluster` aggregates snapshots from the fleet.
The monitor is **not** served on the Worker listener. Like the other internal
operator routes, it has no authentication: bind to loopback or a trusted private
network, or protect it with an authenticated proxy that preserves the original
`Host` header (including its port). Cross-origin API reads are
refused; opening the static dashboard from a link is allowed. This is defense in
depth, not an authentication boundary.

## What the numbers mean

- **Scheduled-turn CPU:** executing-thread CPU accumulated around completed
  synchronous turns, regardless of trace sampling or whether a CPU limit exists.
  Queueing and asynchronous I/O waits are excluded. Synchronous native operations
  and foreground GC are included. A shared-isolate microtask checkpoint can run
  another cell's continuation, so this is **not exact per-cell execution CPU**.
  Background V8 work and some startup/maintenance work are not charged to a cell.
  Rates are percentages of one core; process CPU sums are not deduplicated host
  capacity when multiple celld nodes share a machine.
- **Per-cell SQLite memory:** SQLite's connection-local cache, schema and prepared
  statement allocation counters. This is neither the database file size nor the
  cell's complete native-memory footprint.
- **Owned native buffers:** retained `Vec`/`String` payload capacity, counted once
  as it moves through native HTTP response queues (including blocked/gated writes
  and tee copies), WebSocket output gates, pre-upgrade/output queues, isolate-polled
  input queues, and cell-dispatched input messages. Counts return on dequeue,
  cancellation, rejected gates, failed sends and queue destruction. Ownership comes
  from the creating stream's continuation or the socket's registered cell, not
  whichever cell happens to own the current shared-isolate turn. Facet buffers
  belong to their root cell. Live buffers keep their ownership across hibernation;
  they are not reset or divided when a cell shares or changes isolates.
  Buffer readings remain live while SQLite/heap samples are stale. The tracked
  native total adds SQLite and these buffers, **not** the shared V8 heap or RSS.
  JS stream queues, V8 backing stores, reqwest/hyper/OS/TLS transport buffers,
  WebSocket attachments, queue/container overhead, completed host-op payloads and
  other native allocations remain excluded. Stateless Worker buffers have no cell
  owner and are not attributed to a cell. This is a partial footprint, not total
  per-cell RAM.
- **Shared heap:** V8's used heap, physical committed heap and external memory,
  shown per isolate with its sharing cells. These categories are not additive and
  are never divided among cells. Process RSS is a separate node measurement.
- **In-flight requests:** admitted fetch/RPC requests, not all events or alarms.

Sampling never activates dormant cells, runs SQL, forces GC, or waits for a busy
isolate. Busy isolates keep the previous sample and timestamp, marked stale.
Unavailable readings show `—`, not a fabricated zero. Counters belong to one
residency; replacements, generation changes and node restarts do not inherit a
previous cell's CPU history.

The dashboard polls every two seconds. Search and class filters are applied by the
API; pages contain 100 cells (API maximum 250). CPU and memory sorting ranks the
**current page**, not the entire cluster. Charts retain up to ten minutes in the
open browser tab, are partitioned by node, and reset on node restart. They are not
persistent history.

V8 Inspector, CPU profiles, heap snapshots,
dedicated-isolate diagnostics and persistent Parquet resource history are not
implemented in this slice. Their dashboard actions are disabled.

## Cluster coverage

The default view is **All nodes**. The Overview lists responding and unreachable
nodes, and cells/isolate views identify the node. Select a node or click a cell
for its resource detail and charts. Membership reuses the periodic shared fleet
sample, with its age and discovery errors displayed; monitoring does not trigger
additional fleet scans. Standalone nodes show a one-node view.

Fan-out uses HMAC-authenticated `GET /peer/monitor`, never public Worker ingress.
Every participating node must run a compatible monitor build with
`CELLD_MONITOR=1`. Collection is bounded to 64 nodes, eight concurrent peer reads,
a two-second deadline, 1,000 matching cells per node, and 8 MiB per peer page.
Results are cached for two seconds. Unreachable nodes and collection/discovery
limits mark the result **partial**. Filtering accepts a known node ID, not a URL.
The API accepts `q`, `class`, `node`, `offset` and `limit` (maximum 250).

Snapshots are not atomic across nodes or pages. During a handoff the same logical
cell can appear on two nodes; the monitor intentionally does not hide either
residency. Totals cover responding nodes, not unreachable nodes. Shared V8 heaps
are never converted to per-cell memory.

## Trace queries and logs

Enable `CELLD_OTEL=1` on nodes to export spans and console logs to the shared
bucket. The dashboard's **Traces** view can query span names/URLs/errors, cell,
node, error status, minimum duration, time window and trace ID. Open a matching
trace for the parent-linked span timeline and correlated console logs, including
spans on other nodes. The cell detail has a **Query cell traces** shortcut.
Queries discover export nodes from the bucket, so former nodes remain queryable.
`CELLD_OTEL_BUCKET` is respected. OTLP collector data is not read by this view;
query that external backend instead.

The authenticated fetch tunnel also carries the caller's sampled trace context
in a reserved sidecar header, independently of application headers. Cell spans
on another node join the originating Worker's trace. This requires both peers
to run the updated build; older peers ignore the observational header.

`GET /monitor/traces` accepts `minutes` (default 15, maximum 1440), or `from`/`to`
(Unix milliseconds, range at most 24 hours), plus `trace_id`, `cell` (substring),
`node` (exact), `q`, `errors=1` and `min_ms`. Filters apply to **spans**, not entire
traces; grouped list durations are the longest matching span, not a sum. Trace-ID
drill-down clears the list filters but keeps its time window.
Typing a Trace ID and submitting the form instead starts a fresh relative
window. Trace drill-down URLs include a `#trace=...&from=...&to=...` fragment;
copying/reloading the URL preserves the selected historical window.
`signal=logs` (**Logs only** in the dashboard) instead searches log bodies and
structured annotations, with optional node/cell/trace filters. Errors-only then
means severity 17 or higher; duration is ignored. It includes structured logs
from unsampled traces. Span details expose application attributes/events/links;
log details expose typed bodies and annotations from `metadata_json`.
Native Workflow scalars remain in their compatible `attributes_json` column;
the monitor combines them with application metadata for display and search.

This is an on-demand bounded Parquet browser, not arbitrary SQL. One query per
node runs at a time, with a 15-second deadline, at most 64 export nodes, 256 storage
listings, 64 files, 64 MiB downloaded, 200,000 decoded rows and 500 returned
spans/logs. Each file's declared uncompressed size is capped at 128 MiB. Newest
exports are preferred within the bounded listing inventory, with the requested
window scanned before the one-minute late-flush allowance. Partial scans and
unreadable exports are disclosed. Narrow the time window/node when limits are
reached; use DuckDB for full-history SQL and aggregation.

`playwriter -s <session> --timeout 90000 -f scripts/monitor-traces-ui-smoke.js`
checks manual Trace ID queries, copied/reloaded links, logs-only trace filters
and historical list-row drill-down in real Chrome against the local Effect demo.

Data is available **after export**, not live: the default flush interval is five
minutes, plus upload delay. Sampling, telemetry drops, clock skew and exports
later than the configured flush window plus grace can leave gaps. Timelines use
node wall clocks; resource rates use each process's monotonic clock. See
[telemetry.md](telemetry.md) for the export schema and SQL examples.

## Local three-node RustFS demo

Build celld as below, then run a dedicated local RustFS instance and demo bucket:

```sh
docker run -d --name celld-monitor-rustfs \
  -p 127.0.0.1:19000:9000 -p 127.0.0.1:19001:9001 \
  -e RUSTFS_ACCESS_KEY=celld-demo -e RUSTFS_SECRET_KEY=celld-demo-secret \
  -e RUSTFS_CONSOLE_ENABLE=true rustfs/rustfs:latest
AWS_ACCESS_KEY_ID=celld-demo AWS_SECRET_ACCESS_KEY=celld-demo-secret \
  AWS_DEFAULT_REGION=us-east-1 aws --endpoint-url http://127.0.0.1:19000 \
  s3api create-bucket --bucket celld-monitor-demo
python3 scripts/monitor-cluster-demo.py
# Same three-node fixture with native Effect application spans/structured logs:
python3 scripts/monitor-cluster-demo.py --effect
```

Open **http://127.0.0.1:19180/monitor/**. Three native celld processes use RustFS
for real fleet leases, durable storage, deployment and two-second Parquet exports.
The fixture seeds twelve carts and generates cross-node checkout traffic, CPU
work, async waits, intentional errors and correlated logs. It verifies three
authenticated snapshots and rejects unauthenticated peer reads. State/logs/PIDs
are printed and stored in `cluster.json` in its temporary directory. Ctrl-C stops
only its nodes; RustFS and the demo bucket remain. This deploys a fixture only to
the supplied demo bucket; never point it at a production fleet.
Choose one command, not both: they use the same default ports. `--effect` installs
Effect 4.0.3 in the temporary fixture and uses `celld:telemetry`, without an HTTP
trace/log receiver or collector. Search for `effect.checkout` and open a trace
to see its native handler, cross-node cell spans and structured logs.

For a bounded load test, run `python3 scripts/monitor-cluster-load.py --seconds 15
--output load-results.json`. It exercises 10, 50 and 100 concurrent clients against
all three ingress nodes, twelve carts, bounded CPU work and a 5 ms asynchronous
wait. It measures request latency/throughput, cluster snapshots and concurrent
trace-query responsiveness. Each phase has a 50,000-request safety cap; capped
phases are marked and are not sustained measurements. The Python clients and
debug-build nodes share one machine, so this is not a production capacity benchmark.
The supervisor's background traffic still generates intentional errors, independently
of the load clients. Trace scans remain bounded and may report partial results.

## Build from source

```sh
rustup target add wasm32-unknown-unknown
cargo install trunk --version 0.21.14 --locked
trunk build --release --locked --config crates/monitor/Trunk.toml
cargo build -p celld --features monitor-ui
```

Rebuild the dashboard **before** rebuilding celld when changing its Rust, HTML or
CSS. The `monitor-ui` feature fails the native build if dashboard assets are
missing. A normal `cargo build -p celld` without assets still supports the API,
and `/monitor/` explains how to build the UI instead of showing a blank page.
Release and Docker builds include the dashboard automatically.

The default asset directory is `target/monitor-ui`. `CELLD_MONITOR_ASSETS` can
point the native build to another Trunk distribution directory. This variable is
build-time only; the running binary does not read frontend files from disk.

## Verify

```sh
cargo test -p celld-monitor
cargo test -p celld monitor
cargo clippy -p celld -p celld-monitor --all-targets -- -D warnings
cargo clippy -p celld-monitor --target wasm32-unknown-unknown -- -D warnings
python3 scripts/monitor-smoke.py
# With the three-node demo running:
python3 scripts/monitor-cluster-smoke.py
python3 scripts/monitor-cluster-load.py --seconds 10
```

The smoke test needs esbuild on `PATH`. It launches a real dev node with temporary
state and verifies shared-isolate accounting, CPU without trace instrumentation,
SQLite counters, nonblocking busy-isolate samples, filtering/pagination, read-only
and same-origin guards, real slow HTTP/WebSocket consumers and buffer cleanup,
asset serving, generation replacement, and disabled mode.
