# Telemetry

celld can record traces and logs for the requests it serves. Telemetry is off
by default and costs nothing when off. `CELLD_OTEL=1` writes Parquet files
under the `telemetry/` prefix of the fleet bucket, which DuckDB can query
directly. `CELLD_OTEL=http://collector:4318` sends the same data to an
OpenTelemetry collector.

The schema is version `v0-unstable`, so column names can change before a
stable release. Each file carries the version in the object metadata name
`celld-schema`, or `celld_schema` on an `az://` bucket.

## Configuration

| variable | default | effect |
| --- | --- | --- |
| `CELLD_OTEL` | `0` | `0` disables telemetry. `1` writes Parquet to the fleet bucket. An HTTP(S) collector base URL selects OTLP/HTTP protobuf. |
| `CELLD_OTEL_BUCKET` | the fleet bucket | A different bucket for the Parquet files, on the same endpoint and credentials. |
| `CELLD_OTEL_RETENTION` | `30d` | celld deletes telemetry files older than this. `none` disables the deletion, so your own lifecycle rules can control the data. |
| `CELLD_OTEL_FLUSH_MS` | `300000` | celld writes a Parquet file after this many milliseconds of buffered events. |
| `CELLD_OTEL_FLUSH_BYTES` | `5242880` | The estimated buffered bytes that trigger a flush before the interval ends. |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | A standard sampler name. `traceidratio` with `OTEL_TRACES_SAMPLER_ARG` records a fraction of the traces. |
| `OTEL_EXPORTER_OTLP_HEADERS` | unset | A comma-separated list of `name=value` headers for the collector. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | `10000` | The collector request timeout in milliseconds. |
| `OTEL_SERVICE_NAME` | `celld` | The service name in the exported resource. |

The bucket sink requires `CELLD_BUCKET`. The OTLP sink does not.

A collector URL must have no query or fragment. celld appends `/v1/traces` and
`/v1/logs` to its path and ignores `OTEL_EXPORTER_OTLP_ENDPOINT`.

`CELLD_OTEL_SINK` is removed, and a node with it set does not start. Put the
collector base URL in `CELLD_OTEL`, or keep `CELLD_OTEL=1` for the bucket.

## What celld records

celld records a span for each stateless Worker request, each cell event (a
fetch, an alarm, an RPC, a WebSocket message), each outbound `fetch()`, and
each cell start. A span carries the request id, the cell, the isolate, the
queue wait, the outbound URL and status, and the known durability facts.

Each `console.log` line becomes a log record with the trace id and span id of
its handler, across `await`. The record carries the console method's severity
in `severity_number` and `severity_text` (OTLP fields and Parquet columns). The
body holds only the message.

| method | severity number | severity text |
| --- | --- | --- |
| `console.debug` | `5` | `DEBUG` |
| `console.log`, `console.info` | `9` | `INFO` |
| `console.warn` | `13` | `WARN` |
| `console.error` | `17` | `ERROR` |

Log files from earlier celld versions have no severity columns. Read a mix of
versions with `union_by_name = true`.

celld reads the W3C `traceparent` header on incoming requests and sends it on
outbound `fetch()`. A malformed header starts a new trace. A Worker call to a
Durable Object stays in one trace.

The sampler decides at the start of a request, and an unsampled request
records no native spans or console logs. Structured application logs are
independent of trace sampling. A ratio of `0` records no traces and `1` records all. An
intermediate ratio makes the same trace-id decision on each node. celld keeps
a valid incoming context that the sampler rejects: an outbound `fetch()` or
Durable Object call keeps the trace id, uses a new span id, and keeps the
sampled flag clear.

Under load, telemetry sheds before requests do, and celld counts what it
sheds. celld records no metrics yet.

## Workflow timelines

When telemetry is enabled at instance creation, celld persists the admitted trace
identity and sampling decision with the Workflow. Native checkpoints retain open
span identities and start times, so later alarms, eviction, crash recovery and
explicit restart do not split the detailed execution timeline into unrelated traces.

| Span | Interval / evidence |
| --- | --- |
| `workflow` | One execution generation, from queue admission to its terminal outcome; includes parked time |
| `workflow.step` | One `step.do` occurrence, including its retry delays |
| `workflow.attempt` | One physical callback execution; native timeout and result-validation failures set ERROR status |
| `workflow.retry_delay` | Observed interval between an attempt failure and the next attempt start, with the configured delay in an attribute |
| `workflow.sleep` | A durable sleep or sleepUntil interval |
| `workflow.wait` | An event wait; a native wait timeout sets ERROR status |
| `workflow.pause` | The interval from observed pause to resume or termination |
| `workflow.transition` | An instantaneous lifecycle observation, including queued, running, waiting, paused and terminal transitions |

Step spans are children of the Workflow execution; attempt and retry-delay spans
are children of their Step. Sleep, wait and pause spans are children of the execution.
Span names are stable; identifiers and user-authored Step names are scalar attributes:

```text
celld.workflow.name
celld.workflow.instance_id
celld.workflow.generation
celld.workflow.history_epoch
celld.workflow.step_name
celld.workflow.step_count
celld.workflow.attempt
celld.workflow.callback_execution
celld.workflow.start_event_id
celld.workflow.event_id
celld.workflow.outcome
```

Additional attributes describe retry policy/delay, timeout, sleep duration and event
type. Exported spans do not include Workflow parameters or Step output values.
Fractional millisecond policies remain numeric OTLP double attributes.
Their scalar attributes are OTLP span attributes and are stored in the Parquet
`attributes_json` column. When querying files written before this column existed,
use `union_by_name = true`.

The callback's trace context survives awaits and concurrent branches without changing
its ALS frame or native I/O ownership. Console logs and outbound fetch spans therefore
join the exact physical attempt. Durable Object RPCs propagate that parent locally
and through the peer tunnel's W3C `traceparent` header. Generic runtime alarm spans
remain operational spans; the persisted Workflow tree supplies the cross-alarm
execution timeline.

After a process interruption, another `attempt_started` can have the same native
attempt number. Telemetry gives that callback a new physical span and increments
`callback_execution`. The previous span is marked interrupted, with
`end_observed_at_recovery=true`: its end is the recovery observation, not an invented
precise crash timestamp. Explicit restart closes any open prior-generation spans and
creates a sibling execution span in the same trace. Completed copied checkpoints do
not execute or emit new completion spans.

Attempt spans end at the native settlement observation. A timeout settles the
attempt but does not cancel its callback; any later callback activity retains that
physical attempt's context. A Step's final error can differ from its attempt error
when computing the retry policy itself fails; each span records its own failure.

Finished span descriptions are staged in the same SQLite transaction as native
history and ledger settlement. The adapter waits for `storage.sync()` before sending
them to the existing telemetry pipeline. The bounded exporter remains best effort:
sampling, saturation or a process dying before a buffered batch is exported can
lose spans. A replay of a staged description retains its trace/span identity, but
OTLP delivery is not an exactly-once audit. Use `WorkflowInstance.subscribe()` for
the authoritative retained native event history. Ordinary finished spans become
visible when their interval settles; lifecycle observations can arrive sooner.

### Inspect locally with motel

Build celld, then start the local collector and deterministic Workflow demo:

```sh
cargo build -p celld --profile lab
node scripts/workflow-motel.mjs
```

The launcher runs `motel daemon`, discovers its collector URL, and starts celld on
port 9889 with `CELLD_OTEL` set to that base URL, service `celld-workflows`, all traces
sampled, and a one-second flush. Optional arguments select another project and port;
`CELLD_BINARY`, `CELLD_OTEL`, and the standard telemetry variables override defaults.

```sh
curl 'http://127.0.0.1:9889/create?id=timeline-1&mode=retry'
curl 'http://127.0.0.1:9889/events?id=timeline-1'
motel search-spans celld-workflows workflow attr.celld.workflow.instance_id=timeline-1
```

The demo also accepts modes `timeout`, `sleep`, `wait`, `wait-timeout`, `parallel`,
`parallel-fetch` and `crash`. For a review wait, send `/signal?id=ID` to approve it.
For repeat runs, use fresh IDs or `/restart?id=ID`.

Verify the actual OTLP export and runtime behavior against motel:

```sh
motel daemon
CELLD_SUBSCRIBE_OTEL=http://127.0.0.1:27686 node tests/workflow-subscribe/run.mjs
```

Use the base URL reported by `motel endpoints` if your collector uses another port.
The suite queries received spans for hierarchy, timeouts, wait/pause intervals,
repeated occurrences, restart/recovery identity, W3C propagation and async console
correlation. It uses its own temporary project, port and service name.

## Native application telemetry

The built-in `celld:telemetry` module exposes a narrow in-process interface:

| function | contract |
| --- | --- |
| `currentContext()` | Current `{ traceId, spanId, sampled }`, or `undefined` when telemetry is off/outside an entry. |
| `withContext(context, callback)` | Activate context for the synchronous callback. Restore on return/throw; V8 promise reactions inherit the context. Preserve the ALS frame and native resource owner. Cannot upgrade an unsampled context. |
| `recordSpan(record)` | Admit a finished, sampled span to the existing bounded native queue. |
| `recordLog(record)` | Admit a structured application log, independently of trace sampling. |

This is not an HTTP receiver and does not expose the private runtime internals.
Submission returns a boolean queue-admission result, not a durable flush
acknowledgement. Invalid/oversized records are refused; queue saturation sheds
and counts telemetry rather than delaying requests. When disabled, submission
returns false and does not serialize application values.

Records use hexadecimal IDs and decimal nanosecond timestamp strings; native
span/log timestamps retain the existing microsecond precision. A finished span
supplies `traceId`, `spanId`, optional `parentSpanId`, `name`, OTLP-numbered `kind`,
`startTimeUnixNano`, `endTimeUnixNano`, `sampled`, `statusCode` (0/1/2), optional
`error`, and optional `metadata`. A log supplies optional `traceId`/`spanId`,
`timeUnixNano`, `severityNumber` (1/5/9/13/17/21), display `body`, and optional
`metadata`. A missing log context defaults to the native entry.

`metadata` preserves structured `body`, typed `attributes`, span `events`/`links`,
`serviceName`, `scopeName`, and `statusCode`. Parquet stores it in the new optional
`metadata_json` column; older files without that column remain readable. Native
cell ownership is derived by the host, never accepted as an application column.
The OTLP sink also preserves these types and instrumentation identities.

The facade bounds serialization to depth 6, 1,024 visited values, 64 entries per
object/array, 4,096 characters per string and a 12,000-character aggregate budget.
The native seam rejects payloads above 32 KiB. Cycles become `[circular]`, depth
limits become `[truncated]`, BigInts become decimal strings, and Errors retain
name/message/stack. Throwing getters do not fail the application request.

## Effect application traces and logs

[`examples/effect/telemetry.js`](../examples/effect/telemetry.js) is the Effect
**4.0.3** tracer/logger adapter. Install its `layer({ serviceName: "my-app" })`
with `Effect.provide`. The layer may be shared between requests: native parenting
is resolved at span creation, not at layer construction. No OTLP exporter,
collector URL, credentials or trace/log HTTP endpoints are needed.

Effect root spans join celld's current handler span unless explicitly made roots.
The tracer context hook activates each fiber's span for one synchronous
evaluation, so native `fetch()` and console continuations can follow the current
Effect child across awaits without an isolate-global current span. The adapter
caps each span at 64 attributes, 32 events and 32 links. Its logger replaces
existing loggers by default to avoid duplicate console fallback; set
`mergeWithExisting: true` deliberately if both are wanted.

The fixture checks real persisted spans/logs, including nested and parallel
fibers, native awaits, typed annotations, all six severities, structured bodies,
exception causes, interruption, incoming/downstream propagation, Durable Object
calls, native context restoration, ALS isolation and independent unsampled logs:

```sh
python3 scripts/effect-tracer-smoke.py --require-dashboard
```

It installs the pinned library in temporary state, runs an isolated dev node,
and retains the report and node log. It does not alter the RustFS demo fleet.
The loopback HTTP helper only answers an application downstream request; the
native test asserts that it receives **no HTTP telemetry exports**.

`--transport otlp` retains the original official `OtlpTracer`/`OtlpLogger`
compatibility checks, including request-scope flushing, retry and nonfatal
exporter refusal. That alternative exports to a separate test collector and is
not ingested by celld monitor; `--require-dashboard` intentionally fails there.

## Query the bucket with DuckDB

With `CELLD_MONITOR=1`, the built-in **Traces** view can also run bounded,
read-only queries over these exports and show cross-node span timelines with
correlated console/application logs. **Logs only** searches messages/annotations
even when no span was sampled. See [monitoring.md](monitoring.md#trace-queries-and-logs).
Use DuckDB below for unrestricted SQL and full-history queries.

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET celld_telemetry (
  TYPE s3, KEY_ID '...', SECRET '...',
  ENDPOINT 's3.example.com', URL_STYLE 'path'
);
CREATE VIEW traces AS SELECT * FROM
  read_parquet('s3://YOUR-BUCKET/telemetry/traces/*/*/*/*/*/*.parquet');
CREATE VIEW logs AS SELECT * FROM
  read_parquet('s3://YOUR-BUCKET/telemetry/logs/*/*/*/*/*/*.parquet');

-- The slowest requests.
SELECT name, duration_us, trace_id FROM traces
  ORDER BY duration_us DESC LIMIT 20;

-- The error lines.
SELECT time_unix_us, body FROM logs WHERE severity_number >= 17;

-- Every log line, inside the span that wrote it.
SELECT l.body, t.name, t.duration_us FROM logs l
  JOIN traces t ON l.trace_id = t.trace_id AND l.span_id = t.span_id;
```

A non-AWS S3-compatible endpoint needs `URL_STYLE 'path'`. A plain-HTTP
endpoint also needs `USE_SSL false`.

Files are partitioned by node and hour:
`telemetry/traces/<node>/<yyyy>/<mm>/<dd>/<hh>/<id>.parquet`.

## Flushing and delivery

celld writes one Parquet file per flush, at 5 minutes
(`CELLD_OTEL_FLUSH_MS=300000`) or 5 MiB of estimated buffered events
(`CELLD_OTEL_FLUSH_BYTES=5242880`), whichever comes first. The event that
reaches the target can take the batch past it.

Keep the defaults if you run no compaction job. They produce large files, but
data can arrive up to 5 minutes late. `CELLD_OTEL_FLUSH_MS=5000` gives a
five-second interval, plus upload or collector delay. A short interval makes
many small files and slows queries within hours, so start the compaction job
first. For a near-live view, use the OTLP sink with the same short interval.

The OTLP sink makes at most five attempts per batch on a transient failure
(HTTP 408, 429, 502, 503, or 504). It uses exponential backoff with jitter and
honors `Retry-After`, with each delay capped at 30 seconds. A permanent
refusal drops the batch.

The exporter holds one retrying batch, and the input channel holds 8192 new
events. When the channel is full, celld drops and counts new telemetry, so
request handling continues.

The retention sweep runs at startup and six hours after each completed sweep.

## Compaction

celld does not compact its own files. Run a compaction job on a maintenance
node once an hour, for the hour that just ended. Do not compact the current
hour, because a node still writes to it.

```sql
COPY (
  SELECT * FROM
    read_parquet('s3://YOUR-BUCKET/telemetry/traces/<node>/2026/08/09/22/*.parquet')
  ORDER BY start_unix_us
) TO 's3://YOUR-BUCKET/telemetry/traces/<node>/2026/08/09/22/compacted.parquet'
  (FORMAT parquet, COMPRESSION zstd);
```

Delete the source files after DuckDB writes the compacted file.
