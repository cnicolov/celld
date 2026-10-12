# Native tracing and structured logs

A dependency-free Worker using all four exports from `celld:telemetry` directly:
`currentContext`, `withContext`, `recordSpan` and `recordLog`. No npm install,
collector, OpenTelemetry SDK or HTTP telemetry receiver is needed.

From the repository root, start the built binary:

```sh
CELLD_OTEL=1 CELLD_OTEL_FLUSH_MS=1000 target/debug/celld dev examples/telemetry --port 8080
```

In another terminal:

```sh
curl 'http://127.0.0.1:8080/?customer=alice'
curl 'http://127.0.0.1:8080/?customer=bob&fail=1'
```

Open the printed `/monitor/` URL, select **Traces**, and search for
`example.checkout`, or paste the response's `traceId`. After the export flush
you should see:

```text
native Worker fetch
└─ example.checkout
   └─ example.inventory
```

The inventory timer demonstrates context across a native await. Typed log
annotations and a structured body appear under the correct span; native console
capture also inherits the inventory span. The failure request returns HTTP 500,
records both custom spans as errors, and adds an error-severity log. The success
response includes the checkout, inventory and native parent span IDs for comparison.

`withContext` scopes the synchronous callback, not the whole asynchronous
lifetime; promise continuations created inside inherit its context. Do not store
one mutable current span globally or cache the handler's context at module load.

Spans follow native sampling. Structured `recordLog` entries can still be found
in **Logs only** for an unsampled trace, even though its spans and native console
logs are absent. `CELLD_OTEL=0` disables recording without changing application
behavior; response trace IDs then become `null`.

Recording returns best-effort queue admission, not proof of a durable export.
The example never fails a request because a record was refused. The operator
listener must remain private or behind an authenticated proxy.

For the Effect tracer/logger layer, see [`../effect/`](../effect/).
Full API and serialization limits: [`docs/telemetry.md`](../../docs/telemetry.md#native-application-telemetry).

Run the real-runtime regression check from the repository root:

```sh
python3 scripts/telemetry-example-smoke.py
```

It runs temporary dev nodes, including overlapping requests and unsampled/disabled
telemetry, without changing any existing demo or fleet.
