// Exercise the actual engine helpers with injectable storage faults. The live
// integration suite covers V8/SQLite/RPC; these tests force failure boundaries
// that are hard to hit reliably by killing a process.
import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";
import { test } from "node:test";
import assert from "node:assert/strict";

const source = readFileSync(new URL("../../crates/celld/js/harness.js", import.meta.url), "utf8");
const start = source.indexOf("const __WF_HISTORY_PREFIX");
const end = source.indexOf("class WorkflowInstance {", start);
let nextSpan = 0;
const exported = [];
const runtime = runInNewContext(source.slice(start, end) + `
({ commit: __wfStepCommit, append: __wfHistoryAppend, Cell: __WorkflowCell })`, {
  __wfError: (message) => new Error(message),
  __wfOwnObject: (v) => v !== null && typeof v === "object" && !Array.isArray(v),
  __wfUnknownKey: (v, allowed) => Object.keys(v).find((key) => !allowed.includes(key)),
  __wfTerminal: (v) => ["complete", "errored", "terminated"].includes(v),
  __runtimeClass() {},
  __celld: {},
  __wfLedgerPrefix: (generation) => `__wf.${generation}.`,
  __workflow_trace_new(parent) {
    const p = parent === undefined ? undefined : JSON.parse(parent);
    return JSON.stringify({ trace_id: p?.trace_id ?? Array(16).fill(1),
      span_id: [...Array(7).fill(0), ++nextSpan], parent_span_id: p?.span_id ?? null,
      sampled: true });
  },
  __workflow_span(json) { exported.push(JSON.parse(json)); },
  __wait_until_active: () => false,
  __WF_NAME_RE: /^[a-zA-Z0-9_][a-zA-Z0-9-_]*$/,
  AbortController, setTimeout, clearTimeout, Symbol,
});

function storage() {
  const values = new Map();
  let fault;
  const kv = {
    get(key) { return structuredClone(values.get(key)); },
    put(key, value) {
      if (fault?.(key)) throw new Error("injected storage failure");
      values.set(key, structuredClone(value));
    },
    delete(key) { return values.delete(key); },
    list(options) {
      return [...values].filter(([key]) => key.startsWith(options.prefix) &&
        key > (options.startAfter ?? "")).sort(([a], [b]) => a.localeCompare(b))
        .slice(0, options.limit).map(([key, v]) => [key, structuredClone(v)]);
    },
  };
  return {
    values, kv, setFault(fn) { fault = fn; },
    transactionSync(fn) {
      const before = structuredClone(values);
      try { return fn(); } catch (error) {
        values.clear();
        for (const [key, v] of before) values.set(key, v);
        throw error;
      }
    },
    async sync() {},
  };
}
function setup() {
  const s = storage();
  const meta = { instanceId: "id", generation: "generation", historyEpoch: "epoch",
    historyStart: 0, status: "running" };
  s.kv.put("__wf.meta", meta);
  let wakes = 0;
  const driver = { storage: s, generation: meta.generation, historyChanged() { wakes++; } };
  return { s, meta, driver, wakes: () => wakes };
}

test("failed history commit rolls back ledger, event sequence and notifications", () => {
  for (const brokenKey of ["__wf.history.event.0000000000000002", "__wf.history.sequence"]) {
    const { s, driver, wakes } = setup();
    s.setFault((key) => key === brokenKey);
    assert.throws(() => runtime.commit(driver, "step", { status: "completed" }, [
      { type: "attempt_completed" }, { type: "step_completed" },
    ]), /injected storage failure/);
    assert.equal(s.values.size, 1);
    assert.equal(wakes(), 0);
  }
});

function instrumented() {
  const value = setup();
  value.meta.workflowName = "probe";
  value.meta.telemetry = { trace: { trace_id: Array(16).fill(1),
    span_id: Array(8).fill(2), parent_span_id: null, sampled: true }, start_ms: Date.now() };
  value.s.kv.put("__wf.meta", value.meta);
  value.key = "__wf.generation.step.1.supplier";
  value.events = (events) => runtime.commit(value.driver, value.key, { status: "running" }, events);
  value.spans = () => [...value.s.values].filter(([key]) => key.startsWith("__wf.telemetry.pending."))
    .flatMap(([, spans]) => spans);
  return value;
}

test("telemetry follows native attempts, timeout errors, retry intervals and Step parents", () => {
  const probe = instrumented();
  probe.events([
    { type: "step_started", stepName: "supplier", config: {
      retries: { limit: 1, delay: 20 }, timeout: 10 } },
    { type: "attempt_started", stepName: "supplier", attempt: 1 },
    { type: "attempt_errored", stepName: "supplier", attempt: 1,
      retryDelayMs: 20, error: { name: "Error", message: "timed out" } },
    { type: "attempt_started", stepName: "supplier", attempt: 2 },
    { type: "attempt_completed", stepName: "supplier", attempt: 2 },
    { type: "step_completed", stepName: "supplier" },
  ]);
  const spans = probe.spans();
  const attempts = spans.filter((span) => span.name === "workflow.attempt");
  assert.equal(attempts.length, 2);
  assert.equal(attempts[0].error, "timed out");
  assert.equal(attempts[1].error, undefined);
  assert.equal(spans.filter((span) => span.name === "workflow.retry_delay").length, 1);
  const step = spans.find((span) => span.name === "workflow.step");
  assert.deepEqual(attempts[0].trace.parent_span_id, step.trace.span_id);
  assert.equal(step.attributes["celld.workflow.step_count"], 1);
  assert.equal(step.error, undefined);
});

test("callback recovery has a distinct physical span without inventing native retries", () => {
  const probe = instrumented();
  probe.events([
    { type: "step_started", stepName: "supplier", config: { retries: { limit: 0, delay: 0 }, timeout: 100 } },
    { type: "attempt_started", stepName: "supplier", attempt: 1 },
    { type: "attempt_started", stepName: "supplier", attempt: 1 },
    { type: "attempt_completed", stepName: "supplier", attempt: 1 },
  ]);
  const attempts = probe.spans().filter((span) => span.name === "workflow.attempt");
  assert.deepEqual(attempts.map((span) => span.attributes["celld.workflow.attempt"]), [1, 1]);
  assert.deepEqual(attempts.map((span) => span.attributes["celld.workflow.callback_execution"]), [1, 2]);
  assert.equal(attempts[0].attributes["celld.workflow.end_observed_at_recovery"], true);
  assert.notDeepEqual(attempts[0].trace.span_id, attempts[1].trace.span_id);
});

test("a Step reports its final retry-policy failure while its attempt reports the callback failure", () => {
  const probe = instrumented();
  probe.events([
    { type: "step_started", stepName: "supplier", config: { retries: { limit: 1, delay: "[dynamic]" }, timeout: 100 } },
    { type: "attempt_started", stepName: "supplier", attempt: 1 },
  ]);
  runtime.commit(probe.driver, probe.key, {
    status: "failed", error: { name: "TypeError", message: "policy failed" },
  }, [
    { type: "attempt_errored", stepName: "supplier", attempt: 1,
      error: { name: "Error", message: "callback failed" } },
    { type: "step_errored", stepName: "supplier" },
  ]);
  const spans = probe.spans();
  assert.equal(spans.find((span) => span.name === "workflow.attempt").error, "callback failed");
  const step = spans.find((span) => span.name === "workflow.step");
  assert.equal(step.error, "policy failed");
  assert.equal(step.attributes["error.type"], "TypeError");
});

test("failed telemetry staging cannot expose a partial checkpoint or history", () => {
  const probe = instrumented();
  const before = structuredClone(probe.s.values);
  probe.s.setFault((key) => key.startsWith("__wf.telemetry.pending."));
  assert.throws(() => probe.events([{ type: "sleep_started", stepName: "supplier", durationMs: 10 },
    { type: "sleep_completed", stepName: "supplier" }]), /injected storage failure/);
  assert.deepEqual(probe.s.values, before);
  assert.equal(probe.wakes(), 0);
});

test("native telemetry export is durability-gated and removes staged descriptions atomically", async () => {
  const probe = instrumented();
  probe.events([{ type: "sleep_started", stepName: "supplier", durationMs: 10 },
    { type: "sleep_completed", stepName: "supplier" }]);
  const before = exported.length;
  let release;
  probe.s.sync = () => new Promise((resolve) => { release = resolve; });
  const cell = new runtime.Cell({ storage: probe.s });
  cell._historyChanged();
  await Promise.resolve();
  assert.equal(exported.length, before);
  release();
  await cell._telemetryFlushing;
  assert.equal(exported.length, before + 1);
  assert.equal(probe.spans().length, 0);
});

test("failed durability proof preserves staged spans without exporting or failing the observer", async () => {
  const probe = instrumented();
  probe.events([{ type: "sleep_started", stepName: "supplier", durationMs: 10 },
    { type: "sleep_completed", stepName: "supplier" }]);
  const before = exported.length;
  probe.s.sync = async () => { throw new Error("replication failed"); };
  const cell = new runtime.Cell({ storage: probe.s });
  cell._historyChanged();
  await cell._telemetryFlushing;
  assert.equal(exported.length, before);
  assert.equal(probe.spans().length, 1);
  probe.s.sync = async () => {};
  cell._historyChanged();
  await cell._telemetryFlushing;
  assert.equal(exported.length, before + 1);
  assert.equal(probe.spans().length, 0);
});

test("stale generations and terminated callbacks cannot write ledger or history", () => {
  for (const replacement of [{ generation: "replacement" }, { status: "terminated" }]) {
    const { s, meta, driver, wakes } = setup();
    s.kv.put("__wf.meta", { ...meta, ...replacement });
    assert.equal(runtime.commit(driver, "step", {}, [{ type: "step_completed" }]), false);
    assert.equal(s.values.size, 1);
    assert.equal(wakes(), 0);
  }
});

test("readers cannot deliver locally committed history before durability succeeds", async () => {
  const { s, meta } = setup();
  runtime.append(s.kv, meta, [{ type: "attempt_started" }]);
  const cell = new runtime.Cell({ storage: s });
  let release;
  s.sync = () => new Promise((resolve) => { release = resolve; });
  let delivered = false;
  const result = cell.__wfHistoryNext({ epoch: "epoch", cursor: 0, reader: "reader" })
    .then((value) => { delivered = true; return value; });
  await Promise.resolve();
  assert.equal(delivered, false);
  release();
  assert.equal((await result).value.type, "attempt_started");
  assert.equal(cell._historyWaiters.size, 0);
});

test("failed durability gate rejects delivery and cleans live-reader resources", async () => {
  const { s, meta } = setup();
  runtime.append(s.kv, meta, [{ type: "attempt_started" }]);
  s.sync = async () => { throw new Error("replication failed"); };
  const cell = new runtime.Cell({ storage: s });
  await assert.rejects(cell.__wfHistoryNext({ epoch: "epoch", cursor: 0, reader: "reader" }), /replication failed/);
  assert.equal(cell._historyWaiters.size, 0);
});

test("live reader registers before catch-up and disposes its pending wait", async () => {
  const { s, meta } = setup();
  const cell = new runtime.Cell({ storage: s });
  const pending = cell.__wfHistoryNext({ epoch: "epoch", cursor: 0, reader: "first" });
  assert.equal(cell._historyWaiters.size, 1);
  runtime.append(s.kv, meta, [{ type: "wait_completed" }]);
  cell._historyChanged();
  assert.equal((await pending).value.type, "wait_completed");
  const cancelled = cell.__wfHistoryNext({ epoch: "epoch", cursor: 1, reader: "second" });
  cell.__wfHistoryCancel("epoch", "second");
  assert.equal((await cancelled).done, true);
  assert.equal(cell._historyWaiters.size, 0);
});

test("deleted/recreated instance and expired history cannot alias an old handle", async () => {
  for (const replacement of [{ historyEpoch: "new" }, { status: "complete", expiresMs: 0 }]) {
    const { s, meta } = setup();
    s.kv.put("__wf.meta", { ...meta, ...replacement });
    const cell = new runtime.Cell({ storage: s });
    await assert.rejects(cell.__wfHistoryNext({ epoch: "epoch", cursor: 0, reader: "reader" }), /expired|replaced/);
    assert.equal(cell._historyWaiters.size, 0);
  }
});

test("routed cancellation arriving before its read releases the reader", async () => {
  const { s } = setup();
  const cell = new runtime.Cell({ storage: s });
  cell.__wfHistoryCancel("epoch", "reader");
  assert.equal(cell._historyCancelled.size, 1);
  const result = await cell.__wfHistoryNext({ epoch: "epoch", cursor: 0, reader: "reader" });
  assert.equal(result.done, true);
  assert.equal(cell._historyCancelled.size, 0);
  assert.equal(cell._historyWaiters.size, 0);
  assert.equal(cell._historyReaders.size, 0);
});
