# Metrics

celld can report operational metrics: isolate heap and turns, cell events,
durability, the bucket, memory pressure, and the fleet. Metrics are off by
default and cost nothing when off. Like the monitor, they are observations:
no placement, ownership, or admission decision reads them.

## Enable metrics

| variable | default | effect |
| --- | --- | --- |
| `CELLD_METRICS` | `0` | `1` serves `/metrics` on the internal listener and records the instruments below. |
| `OTEL_METRIC_EXPORT_INTERVAL` | `60000` | Milliseconds between OTLP pushes. |

With `CELLD_METRICS=1`, two outputs share one collection:

- `GET /metrics` on the internal listener returns the Prometheus text format
  (0.0.4). Like `/state`, it is never served on the Worker listener and it has
  no authentication, so keep the internal listener private.
- When `CELLD_OTEL` is a collector base URL, celld also pushes the metrics to
  `<base>/v1/metrics` over OTLP/HTTP, with the headers, timeout, and retry of
  the trace and log export. The bucket sink (`CELLD_OTEL=1`) has no metrics
  form; scrape `/metrics` instead.

Sums and histograms are cumulative from process start. A restart begins new
series, which Prometheus and OTLP backends treat as a counter reset.

Every point carries the resource of the trace and log exports:
`service.name` (`OTEL_SERVICE_NAME`, default `celld`), `service.version`,
`service.instance.id` (the node ID), and `celld.region`. A backend can
therefore join a node's metrics, traces, and logs. `/metrics` reports the
resource as a `target_info` series.

## Names

Names follow the OpenTelemetry semantic conventions: `process.*`,
`http.server.request.duration`, and the FaaS conventions for cell events. Every
other name is in the `celld.*` namespace. Units are UCUM, and failures carry
`error.type`.

`/metrics` translates the names as the OpenTelemetry specification describes:
dots become underscores, the unit becomes a suffix (`_seconds`, `_bytes`,
`_ratio` for a unitless gauge), and a counter ends in `_total`. For example,
`celld.isolate.heap.utilization` is `celld_isolate_heap_utilization_ratio`,
and the attribute `celld.script` is the label `celld_script`.

No attribute names a cell instance. The cardinality is fixed by the
deployment: scripts, Durable Object classes, event kinds, and queues.

## Catalog

### Process and memory

| metric | type | unit | attributes | meaning |
| --- | --- | --- | --- | --- |
| `process.cpu.time` | counter | s | `cpu.mode` | Process CPU time. |
| `process.memory.usage` | updown | By | | Process RSS. |
| `process.uptime` | gauge | s | | Time since the process started. |
| `celld.memory.usage` | updown | By | `celld.memory.source` | `in_use` (allocator-adjusted RSS), `cgroup_working_set`, `cgroup_current`. |
| `celld.memory.limit` | updown | By | `celld.memory.limit.kind` | `ordinary` (the shedding ceiling, `CELLD_MAX_RSS_MB`), `hard`, `cgroup`. |
| `celld.memory.utilization` | gauge | 1 | | The pressure measurement over the ordinary ceiling. Shedding starts at 1. |
| `celld.memory.shedding` | updown | 1 | `celld.memory.shedding.reason` | 1 while the node sheds cells. |

### Isolates and turns

| metric | type | unit | attributes | meaning |
| --- | --- | --- | --- | --- |
| `celld.isolate.heap.usage` | updown | By | `celld.script`, `celld.isolate.kind` | V8 heap used, summed over the script's isolates. |
| `celld.isolate.heap.limit` | updown | By | `celld.script`, `celld.isolate.kind` | The isolate heap limit (`CELLD_V8_HEAP_LIMIT_MB`). |
| `celld.isolate.heap.utilization` | gauge | 1 | `celld.script`, `celld.isolate.kind` | Heap use over the limit for the script's fullest isolate. |
| `celld.isolate.count` | updown | {isolate} | `celld.script`, `celld.isolate.kind`, `celld.isolate.state` | `serving`, `condemned` (at its heap limit), `retiring`. |
| `celld.isolate.condemnations` | counter | {condemnation} | `celld.script`, `celld.isolate.condemnation.reason` | `heap_limit`, or `guest_fatal` for a fatal guest error. |
| `celld.isolate.heap.recoveries` | counter | {recovery} | `celld.script` | Condemned isolates whose heap fell below 75% of the limit. |
| `celld.isolate.admission.refusals` | counter | {refusal} | `celld.script` | Hibernatable WebSockets refused at 90% of the heap limit. |
| `celld.turn.duration` | histogram | s | `celld.script` | Time a synchronous turn held its isolate. |
| `celld.turn.wait.duration` | histogram | s | `celld.script` | Time a turn waited for its isolate. A rise means one cell's long turns block the others. |

Cells share an isolate, so heap is reported per isolate, never per cell. A
running isolate reports the heap it had at its last turn end, sampled at most
once a second; an idle one is read when the collection runs. Worker Loader
isolates are not listed.

### Events

| metric | type | unit | attributes | meaning |
| --- | --- | --- | --- | --- |
| `faas.invoke_duration` | histogram | s | `faas.trigger`, `celld.cell.event`, `celld.cell.class` | A cell event, from admission to its outcome. |
| `faas.invoke_duration` | histogram | s | `faas.trigger=pubsub`, `celld.script`, `messaging.destination.name` | A queue consumer batch. |
| `faas.errors` | counter | {error} | as above, and `error.type` | A failed event or batch. |
| `http.server.request.duration` | histogram | s | `http.request.method`, `http.response.status_code`, `error.type` | A stateless Worker request, to its response headers. |

`faas.trigger` is `http` for a fetch, `timer` for an alarm (cron included),
`pubsub` for a queue batch, and `other` for RPC, WebSocket, and operator REPL
events (`celld.cell.event=repl`).
`error.type` is `exception`, `timeout`, `stuck`, `storage`, `overloaded`,
`gate_failed`, `cell_stopped`, or `_OTHER`. The invocation count is the
histogram's count, so an error rate is `faas.errors` over it.

### Cells, durability, the bucket, and the fleet

| metric | type | unit | attributes | meaning |
| --- | --- | --- | --- | --- |
| `celld.cell.count` | updown | {cell} | `celld.cell.phase` | Cells by lifecycle phase, as `/state` reports them. |
| `celld.cell.activation.duration` | histogram | s | `celld.cell.activation.outcome`, `celld.cell.activation.source`, `error.type` | A cold route, until the cell serves here or its owner is known. |
| `celld.cell.handoffs` | counter | {handoff} | `error.type` | Cells handed to another node, rebalances included. |
| `celld.cell.rebalances` | counter | {rebalance} | `error.type` | Cells moved by rebalancing. |
| `celld.durability.proof.duration` | histogram | s | `celld.durability.proof`, `error.type` | Time a write waited for its proof: `fleet` or `bucket`, or `error.type=timeout`. |
| `celld.durability.follower.count` | updown | {follower} | | Followers that can prove a write now. 0 means every write waits for the bucket. |
| `celld.log.head.stalls` | counter | {stall} | | Replicated log rounds outstanding past half the backstop. |
| `celld.bucket.operation.duration` | histogram | s | `celld.bucket.operation`, `error.type` | Object-store requests for leases, ownership, deployments, and blobs. `error.type=cas_conflict` is a lost conditional write. |
| `celld.node.lease.renewals` | counter | {renewal} | `celld.node.lease.attempt`, `error.type` | Lease acquire and renew attempts. `error.type` is the outcome of one that did not apply. |
| `celld.ownership.record.losses` | counter | {loss} | `celld.cell.class`, `celld.ownership.path` | Claims (`claim`) or named creations (`create`) refused because the cell's `own.json` is missing while its LTX is in the bucket. Requests for the cell fail until an operator restores the record; see [What celld guarantees](guarantees.md). |
| `celld.fleet.node.count` | updown | {node} | `celld.node.state` | Live nodes in the last successful fleet sample, this node included. |
| `celld.websocket.count` | updown | {connection} | | WebSocket connections held by the node. |
| `celld.telemetry.dropped` | counter | {event} | | Spans and logs shed by the telemetry exporter. |

LTX replication uses its own object-store client, so its uploads are not in
`celld.bucket.operation.duration`; their latency is in
`celld.durability.proof.duration`. A node that fences itself exits at once,
so no metric reports a fence: alert on the node's absence and on failed lease
renewals instead.

## Alerts

These thresholds follow celld's own lines. An isolate refuses new state at
90% of its heap limit, is condemned at the limit, and recovers below 75%.

```yaml
groups:
  - name: celld
    rules:
      - alert: CelldIsolateHeapHigh
        expr: max by (instance, celld_script) (celld_isolate_heap_utilization_ratio) > 0.75
        for: 2m
      - alert: CelldIsolateHeapNearLimit
        expr: max by (instance, celld_script) (celld_isolate_heap_utilization_ratio) > 0.9
        for: 1m
      - alert: CelldIsolateCondemned
        expr: increase(celld_isolate_condemnations_total[5m]) > 0
      - alert: CelldShedding
        expr: max by (instance) (celld_memory_shedding) == 1
      - alert: CelldDurabilityOnBucket
        # Expected on a one-node fleet.
        expr: celld_durability_follower_count == 0
        for: 2m
      - alert: CelldTurnWaitHigh
        expr: histogram_quantile(0.99, sum by (instance, celld_script, le) (rate(celld_turn_wait_duration_seconds_bucket[5m]))) > 0.25
        for: 5m
      - alert: CelldLeaseRenewalFailing
        expr: increase(celld_node_lease_renewals_total{error_type!=""}[5m]) > 0
      - alert: CelldOwnershipRecordLost
        # The cell is unavailable until its own.json is restored.
        expr: increase(celld_ownership_record_losses_total[5m]) > 0
```

An OTLP backend that stores the OpenTelemetry names, such as SigNoz, takes the
same conditions on the dotted names. A pushed node that stops reporting has no
`up` series, so alert on the absence of its data, or on
`celld.fleet.node.count` from the surviving nodes.

## Cost

When metrics are off, each recording site reads one `OnceLock`. When on,
recording into an existing series allocates nothing and takes only a shared
lock: about 100 ns for a turn and 180 ns for a cell event on one thread,
measured in a release build. Contention on one series raises that to a few
microseconds per recording, which only many cores recording at full speed
reach. A collection takes a few milliseconds and walks the core's cells once,
as `/state` does.

A node with one script and one Durable Object class reports about 200
series. Histograms dominate: each is 17 series with the default buckets. At a
15-second scrape, 200 series are about 35 million samples a month.
