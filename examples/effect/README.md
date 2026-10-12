# Effect telemetry in celld

```sh
npm install --ignore-scripts --no-audit --no-fund
CELLD_OTEL=1 CELLD_OTEL_FLUSH_MS=2000 celld dev .
```

Open the printed `/monitor/` URL, choose **Traces**, search for `effect.checkout`,
and open a trace. It contains the native handler, Effect child spans and
correlated structured logs. **Logs only** also finds logs whose trace was not
sampled.

`telemetry.js` is a reusable Effect **4.0.3** adapter. Copy it into an application
and install `layer({ serviceName: "my-app" })` with `Effect.provide`. A layer can
be shared by requests: it reads the native parent when each span is created.
`mergeWithExisting` defaults to false to avoid duplicate console records.

The adapter uses the built-in `celld:telemetry` module. No collector, OTLP
exporter, credentials or trace/log HTTP receiver is involved. Sampling follows
the native request decision; structured logs remain independent of sampling.
Enabling the adapter does not enable telemetry when `CELLD_OTEL=0`.

Span/log submission only admits records to the bounded native queue. It is not
a durable acknowledgement, and the monitor sees them after the configured
flush interval plus upload delay.
