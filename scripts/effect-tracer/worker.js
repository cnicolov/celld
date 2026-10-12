// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Test fixture only. The dashboard and celld backend remain Rust.
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import * as Layer from "effect/Layer";
import * as References from "effect/References";
import * as Tracer from "effect/Tracer";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as OtlpTracer from "effect/observability/OtlpTracer";
import * as OtlpLogger from "effect/observability/OtlpLogger";
import * as OtlpSerialization from "effect/observability/OtlpSerialization";
import * as Native from "celld:telemetry";
import * as NativeTelemetry from "./telemetry.js";
import { AsyncLocalStorage } from "node:async_hooks";

function incomingParent(request) {
  const match = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/.exec(request.headers.get("traceparent") || "");
  return match ? Tracer.externalSpan({ traceId: match[1], spanId: match[2], sampled: (parseInt(match[3], 16) & 1) === 1 }) : undefined;
}

function header(span) {
  return `00-${span.traceId}-${span.spanId}-${span.sampled ? "01" : "00"}`;
}

async function traced(request, env, name, work) {
  const url = new URL(request.url);
  const mode = url.searchParams.get("mode") || "success";
  const token = url.searchParams.get("token") || "fixture";
  const native = env.TELEMETRY !== "otlp";
  const parent = native || mode === "unbridged" ? undefined : incomingParent(request);
  const result = { token, mode, effectVersion: "4.0.3", nativeParent: native ? Native.currentContext() : undefined };
  if (native && !result.nativeParent) {
    let touched = false;
    const record = Object.defineProperty({}, "body", { enumerable: true, get() { touched = true; throw new Error("disabled serialization probe"); } });
    result.disabledChecks = { refused: !Native.recordLog(record), untouched: !touched };
  }
  if (native && result.nativeParent) result.samplingProbe = Native.withContext({ ...result.nativeParent, sampled: true }, () => Native.currentContext());
  if (native && mode === "bridge") {
    const original = Native.currentContext();
    const child = { ...original, spanId: "1234567890abcdef" };
    const storage = new AsyncLocalStorage();
    let caught = false;
    try { Native.withContext(child, () => { throw new Error("scope restoration probe"); }); } catch { caught = true; }
    const restoredAfterThrow = Native.currentContext();
    const resumed = await storage.run(token, () => Native.withContext(child, async () => {
      await new Promise(resolve => setTimeout(resolve, 2));
      return { context: Native.currentContext(), frame: storage.getStore() };
    }));
    result.bridgeChecks = {
      caught, original, restoredAfterThrow, resumed, restoredAfterAwait: Native.currentContext(),
      malformedSpanRejected: !Native.recordSpan({ name: "invalid" }),
      malformedLogRejected: !Native.recordLog({ body: "invalid" }),
      internalsHidden: typeof globalThis.__telemetry_span === "undefined" && process.getBuiltinModule("celld:telemetry") === undefined,
    };
    const probe = { ...original, traceId: crypto.randomUUID().replaceAll("-", "") };
    const cycle = { token };
    cycle.self = cycle;
    const record = { traceId: probe.traceId, spanId: probe.spanId, timeUnixNano: String(BigInt(Date.now()) * 1000000n), severityNumber: 9, body: "native serialization probe" };
    result.recordChecks = Native.withContext(probe, () => ({
      traceId: probe.traceId,
      accepted: Native.recordLog({ ...record, metadata: { scopeName: "fixtureprobe", cell: "forged-cell", body: { cycle, bigint: 123n, error: new Error("serializer"), huge: "λ".repeat(20000) } } }),
      oversizedRejected: !Native.recordLog({ ...record, metadata: { one: "\0".repeat(4096), two: "\0".repeat(4096) } }),
      getterRejected: !Native.recordLog(Object.defineProperty({ ...record }, "metadata", { enumerable: true, get() { throw new Error("telemetry getter"); } })),
    }));
  }
  const endpoint = `${env.COLLECTOR}${mode === "retry" ? "/retry" : mode === "refuse" ? "/refuse" : ""}`;
  const options = {
    resource: { serviceName: "celld-effect-tracer-fixture" },
    exportInterval: "60 seconds",
    shutdownTimeout: "3 seconds",
  };
  const telemetry = native ? NativeTelemetry.layer({ serviceName: "celld-effect-tracer-fixture" }) : Layer.merge(
    OtlpTracer.layer({ ...options, url: `${endpoint}/v1/traces` }),
    // Keep genuine Effect OTLP logs distinct from console fallback records.
    OtlpLogger.layer({ ...options, url: `${endpoint}/v1/logs`, mergeWithExisting: false }),
  ).pipe(Layer.provide(Layer.merge(FetchHttpClient.layer, OtlpSerialization.layerJson)));
  const program = Effect.gen(function* () {
    const span = yield* Effect.currentSpan;
    result.traceId = span.traceId;
    result.spanId = span.spanId;
    yield* Effect.annotateCurrentSpan({ "test.token": token, "test.mode": mode });
    console.info("effect root", token, span.spanId);
    yield* Effect.logTrace(`effect.trace ${token}`);
    yield* Effect.logDebug(`effect.debug ${token}`);
    yield* Effect.logInfo(`effect.info ${token}`);
    yield* Effect.logWarning(`effect.warn ${token}`);
    yield* Effect.logError(`effect.error ${token}`);
    yield* Effect.logFatal(`effect.fatal ${token}`);
    yield* Effect.logInfo("effect.batch", token, 2);
    return yield* work({ request, env, token, mode, result });
  }).pipe(
    Effect.withSpan(name, { parent, sampled: parent?.sampled }),
    Effect.annotateLogs({ "test.token": token, "test.mode": mode, "customer.id": "alice", "cart.items": 2, "test.verified": true }),
    Effect.provideService(References.MinimumLogLevel, "Trace"),
  );
  // Providing a layer scopes it to this request. Closing it flushes all ended
  // spans before the response, with the exporter's bounded shutdown timeout.
  const exit = await Effect.runPromiseExit(program.pipe(Effect.provide(telemetry)));
  result.ok = Exit.isSuccess(exit);
  return Response.json(result, { status: result.ok ? 200 : 500 });
}

function branch(token, name, delay) {
  return Effect.gen(function* () {
    const before = yield* Effect.currentSpan;
    yield* Effect.sleep(delay);
    const after = yield* Effect.currentSpan;
    yield* Effect.promise(async () => {
      await new Promise(resolve => setTimeout(resolve, 2));
      console.info("effect branch after await", token, name, after.spanId);
    });
    yield* Effect.logInfo(`effect.after.await ${name} ${token}`).pipe(Effect.annotateLogs({ "test.branch": name }));
    yield* Effect.sync(() => after.event("after.await", BigInt(Date.now()) * 1000000n, { token }));
    yield* Effect.succeed("nested").pipe(Effect.withSpan(`${name}.nested`));
    return { name, before: before.spanId, after: after.spanId, traceId: after.traceId };
  }).pipe(Effect.withSpan(name, { attributes: { "test.token": token } }));
}

function checkout({ env, token, mode, result }) {
  return Effect.gen(function* () {
    result.branches = yield* Effect.all([
      branch(token, "effect.cart", 5),
      branch(token, "effect.price", 10),
    ], { concurrency: "unbounded" });
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.get(`${env.COLLECTOR}/downstream?token=${encodeURIComponent(token)}`);
    result.downstream = yield* response.json;
    if (mode === "cell") {
      result.cell = yield* Effect.gen(function* () {
        const current = yield* Effect.currentSpan;
        result.cellCallerSpanId = current.spanId;
        return yield* Effect.promise(async () => {
          const response = await env.CELLS.get(env.CELLS.idFromName(token)).fetch(new Request(
            `http://effect-cell/?token=${encodeURIComponent(token)}`,
            { headers: { traceparent: header(current) } },
          ));
          if (!response.ok) throw new Error(`cell returned ${response.status}`);
          return response.json();
        });
      }).pipe(Effect.withSpan("effect.call.cell"));
    }
    if (mode === "failure") {
      yield* Effect.gen(function* () {
        const error = new Error(`payment declined: ${token}`);
        yield* Effect.logError(`effect.payment.failed ${token}`, Cause.fail(error));
        return yield* Effect.fail(error);
      }).pipe(Effect.withSpan("effect.payment.failed"));
    }
    if (mode === "defect") {
      yield* Effect.die(new Error(`payment defect: ${token}`)).pipe(Effect.withSpan("effect.payment.defect"));
    }
    if (mode === "cancel") {
      yield* Effect.never.pipe(Effect.withSpan("effect.cancelled"), Effect.timeoutOption(10));
    }
    return "checked out";
  }).pipe(Effect.provide(FetchHttpClient.layer));
}

export class EffectCell {
  constructor(_state, env) { this.env = env; }
  fetch(request) {
    return traced(request, this.env, "effect.cell", ({ token }) => branch(token, "effect.cell.work", 5));
  }
}

export default {
  fetch(request, env) { return traced(request, env, "effect.checkout", checkout); },
};
