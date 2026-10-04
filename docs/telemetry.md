# Telemetry

celld can record traces and logs for the requests it serves. The
feature is off by default, and the off state costs nothing. Set
`CELLD_OTEL=1` to write telemetry to the fleet bucket.
Set `CELLD_OTEL=http://collector:4318` to send telemetry to an OTLP collector.

The default sink is the fleet bucket. celld writes Parquet files under
the `telemetry/` prefix, so a fleet with a bucket has observability
with no other service. DuckDB can query these files directly. An
alternative sink sends the same data to an OpenTelemetry collector.

The schema is version `v0-unstable`. The column names can change
before a stable release, and each file carries the schema version in
its object metadata under the name `celld-schema`. Azure Blob Storage
does not accept a hyphen in a metadata name, so on an `az://` bucket
the name is `celld_schema`.

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

The `bucket` sink requires the node to have a fleet bucket
(`CELLD_BUCKET`). The `otlp` sink works on a node without one.

Use a full HTTP(S) collector base URL without a query or a fragment.
celld adds `/v1/traces` and `/v1/logs` to its path for the two signals.
`CELLD_OTEL` supplies the collector address, so celld does not read
`OTEL_EXPORTER_OTLP_ENDPOINT`.

`CELLD_OTEL_SINK` is removed. Remove this setting before starting the node.
For a collector, copy its full base URL into `CELLD_OTEL`. For the fleet bucket, keep `CELLD_OTEL=1`.


## What celld records

celld records a span for each request a stateless Worker serves, for
each event a cell serves (a fetch, an alarm, an RPC, a WebSocket
message), for each outbound `fetch()`, and for each cell start. A
span carries the request id, the cell, the isolate, the
queue wait, the outbound URL and status, and the durability facts the
runtime already knows.

celld also records each `console.log` line as a log record. The log
record carries the trace id and the span id of the handler that wrote
it, so a query can join the logs to the traces. The correlation
survives `await`.

The log record also carries the OpenTelemetry severity of the console
method:

| method | severity number | severity text |
| --- | --- | --- |
| `console.debug` | `5` | `DEBUG` |
| `console.log`, `console.info` | `9` | `INFO` |
| `console.warn` | `13` | `WARN` |
| `console.error` | `17` | `ERROR` |

The OTLP export sets these values in the `severity_number` and
`severity_text` fields. The Parquet export writes them to columns with
the same names. The body contains only the message, so a query must use
the severity to find the level.

A log file from an earlier celld version has no severity columns. A
DuckDB query over files from both versions must therefore read them
with `union_by_name = true`.

celld reads the W3C `traceparent` header on incoming requests, so its
spans join the trace of the system in front of it. celld sends a
`traceparent` header on outbound `fetch()`, so downstream systems can
join too. celld ignores a malformed header and starts a new trace. A
Worker call to a Durable Object stays in one trace.

The sampler decides at the start of a request. An unsampled request
records nothing and costs almost nothing. Under load, telemetry sheds
before requests do, and celld counts what it sheds.

A sampling ratio of `0` records no traces, and a ratio of `1` records
every trace. An intermediate ratio makes the same trace-id decision on
each node.

celld keeps a valid incoming context when the sampler rejects it. An
outbound `fetch()` or Durable Object call keeps the trace id, uses a new
span id, and keeps the sampled flag clear.

celld records no metrics yet. The spans carry the durations and the
queue waits, so many questions a metric answers have an answer in the
traces, and a metrics signal can come later without a change to the
trace schema.

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

## Query the bucket with DuckDB

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

An S3-compatible endpoint such as minio needs `URL_STYLE 'path'` in
the secret, and a plain-HTTP endpoint also needs `USE_SSL false`. AWS
itself does not need either.

The files are partitioned by node and by hour:
`telemetry/traces/<node>/<yyyy>/<mm>/<dd>/<hh>/<id>.parquet`. A query
that reads one day therefore touches only that day's files.

## File size and compaction

celld writes one Parquet file for each flush, on a time limit or a
size target, whichever it reaches first. The default interval is 5 minutes
(`CELLD_OTEL_FLUSH_MS=300000`). The default size target is 5 MiB of estimated
buffered events (`CELLD_OTEL_FLUSH_BYTES=5242880`). The event that reaches
the target can take the batch past it.

Keep the defaults if you run no compaction job. They make files large
enough for a fast query with no other moving part. The default batching
delay can reach 5 minutes, so the default suits an investigation after the fact.

Set `CELLD_OTEL_FLUSH_MS=5000` for a five-second batching interval.
An upload, a retry, or collector processing can add a delivery delay.
A short flush makes many small files, and DuckDB
opens every file a query reads, so you must also run the compaction
job below. Turn on the compaction job first, then shorten the flush,
or queries grow slow within hours.

The `otlp` sink is the other route to a near-live view. It sends each
batch to a collector, so set the same short `CELLD_OTEL_FLUSH_MS`.

The `otlp` sink makes no more than five attempts for a batch when a
transient failure occurs. It uses exponential backoff with jitter, and it
uses an applicable `Retry-After` value from the collector. The exporter caps
each delay at 30 seconds, so a collector cannot suspend a batch indefinitely.
HTTP 408, 429, 502, 503, and 504 responses are transient failures. A
permanent refusal drops the batch immediately.

The exporter owns one retrying batch, and the input channel holds 8192
new events. An outage cannot make either bound grow. celld drops and counts
new telemetry when the channel is full, so request handling continues.

The retention sweep runs at startup and six hours after each completed sweep. It deletes expired
objects and preserves objects within the configured retention window.

## Compaction

Run a compaction job on a maintenance node, not on a celld node. celld
does not compact its own files, because the serving path must not do
storage maintenance and each node writes only the files it produced.

The job rewrites one past hour of small files into one large file.
DuckDB does the work:

```sql
COPY (
  SELECT * FROM
    read_parquet('s3://YOUR-BUCKET/telemetry/traces/<node>/2026/08/09/22/*.parquet')
  ORDER BY start_unix_us
) TO 's3://YOUR-BUCKET/telemetry/traces/<node>/2026/08/09/22/compacted.parquet'
  (FORMAT parquet, COMPRESSION zstd);
```

Run the job once an hour, for the hour that just ended. Do not compact
the current hour, because a node still writes to it. Delete the source
files after DuckDB writes the compacted file. The compacted file is
also smaller, because zstd compresses one sorted batch better than
many separate files.
