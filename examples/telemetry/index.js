// Copyright 2026 Deno Land Inc. Apache-2.0 license.
import { currentContext, withContext, recordSpan, recordLog } from "celld:telemetry";

const scope = { serviceName: "native-tracing", scopeName: "example" };
const now = () => String(BigInt(Date.now()) * 1000000n);

function newSpanId() {
  let bytes;
  do { bytes = crypto.getRandomValues(new Uint8Array(8)); }
  while (bytes.every(byte => byte === 0));
  return Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
}

// Parent each operation to the current native handler or application span.
// There is no isolate-global "current span": concurrent requests stay separate.
function withSpan(name, attributes, operation) {
  const parent = currentContext();
  if (!parent) return operation(); // CELLD_OTEL=0: keep running without tracing.

  const context = { ...parent, spanId: newSpanId() };
  const startTimeUnixNano = now();
  // withContext restores the caller as soon as its callback returns; promise
  // continuations created inside inherit the child context across native awaits.
  return withContext(context, async () => {
    let statusCode = 1; // OTLP: 1 = OK, 2 = ERROR.
    let error;
    try {
      return await operation();
    } catch (cause) {
      statusCode = 2;
      error = cause instanceof Error ? cause.message : "operation failed";
      throw cause;
    } finally {
      // Queue admission is best-effort, not a durable acknowledgement.
      recordSpan({
        ...context, parentSpanId: parent.spanId, name, kind: 1, // INTERNAL
        startTimeUnixNano, endTimeUnixNano: now(), statusCode, error,
        metadata: { ...scope, attributes },
      });
    }
  });
}

function log(message, attributes, severityNumber = 9) {
  recordLog({
    timeUnixNano: now(), severityNumber, body: message,
    // Omitting IDs uses the current context, including after an await.
    metadata: { ...scope, body: { message, details: attributes }, attributes },
  });
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const customer = url.searchParams.get("customer") || "alice";
    const handler = currentContext();
    try {
      const checkout = await withSpan("example.checkout", { customer }, async () => {
        log("checkout started", { customer });
        const inventory = await withSpan("example.inventory", { "cart.items": 2 }, async () => {
          await new Promise(resolve => setTimeout(resolve, 10));
          log("inventory ready", { customer, "cart.items": 2, available: true });
          console.log("native console inside inventory", customer);
          if (url.searchParams.get("fail") === "1") {
            log("inventory unavailable", { customer, available: false }, 17);
            throw new Error("inventory unavailable");
          }
          return currentContext();
        });
        log("checkout completed", { customer, checkedOut: true });
        return { context: currentContext(), inventory };
      });
      return Response.json({
        customer, checkedOut: true,
        traceId: checkout.context?.traceId ?? null,
        spanId: checkout.context?.spanId ?? null,
        inventorySpanId: checkout.inventory?.spanId ?? null,
        parentSpanId: handler?.spanId ?? null,
        sampled: checkout.context?.sampled ?? null,
      });
    } catch {
      return Response.json({ error: "inventory unavailable", traceId: handler?.traceId ?? null }, { status: 500 });
    }
  },
};
