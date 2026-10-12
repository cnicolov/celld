// Integration tests against the real V8, SQLite, RPC and celld dev stack.
// node tests/workflow-subscribe/run.mjs [path/to/celld]
import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { mkdtemp, cp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { once } from "node:events";
import { resolve } from "node:path";
import { tmpdir } from "node:os";
import { randomBytes } from "node:crypto";

const binary = resolve(process.argv[2] ?? "target/lab/celld");
const collector = process.env.CELLD_SUBSCRIBE_OTEL;
const service = collector ? `celld-workflows-test-${Date.now()}` : undefined;
const directory = await mkdtemp(resolve(tmpdir(), "celld-subscribe-"));
for (const file of ["worker.js", "wrangler.jsonc"]) {
  await cp(new URL(file, import.meta.url), resolve(directory, file));
}
const listener = createServer().listen(0, "127.0.0.1");
await once(listener, "listening");
const port = listener.address().port;
await new Promise((done) => listener.close(done));
const base = `http://127.0.0.1:${port}`;
let child;
let logs = "";
const delay = (ms) => new Promise((done) => setTimeout(done, ms));
async function request(path, id, params = {}, headers) {
  const url = new URL(path, base);
  if (id) url.searchParams.set("id", id);
  for (const [key, value] of Object.entries(params)) url.searchParams.set(key, value);
  const response = await fetch(url, { signal: AbortSignal.timeout(20000), headers });
  const value = await response.json();
  if (!response.ok) throw new Error(value.error);
  return value;
}
async function until(read, predicate) {
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    const value = await read();
    if (predicate(value)) return value;
    await delay(20);
  }
  throw new Error("condition timed out");
}
async function start() {
  child = spawn(binary, ["dev", directory, "--port", String(port), "--no-watch", "--logs"], {
    env: { ...process.env, CELLD_OTEL: collector ?? "0", CELLD_V8_HEAP_LIMIT_MB: "128",
      ...(collector ? { CELLD_OTEL_FLUSH_MS: "100", OTEL_SERVICE_NAME: service,
        OTEL_TRACES_SAMPLER: "parentbased_always_on" } : {}),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout.on("data", (data) => { logs += data; });
  child.stderr.on("data", (data) => { logs += data; });
  await until(() => request("/health").catch(() => null), (v) => v?.ok);
}
async function stop(crash = false) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  const node = Number(execFileSync("pgrep", ["-P", String(child.pid)], { encoding: "utf8" }).trim());
  if (crash) process.kill(node, "SIGKILL");
  else child.kill("SIGTERM");
  const clean = await Promise.race([exited.then(() => true), delay(1500).then(() => false)]);
  if (!clean) {
    try { process.kill(node, "SIGKILL"); } catch {}
    child.kill("SIGKILL");
    await exited;
  }
  child = undefined;
}
const create = (id, mode, extra = {}) => request("/create", id, { mode, ...extra });
const drain = (id, options = {}, max) => request("/events", id, {
  options: JSON.stringify(options), ...(max === undefined ? {} : { max }),
});
const types = (events, type) => events.filter((event) => event.type === type);
async function observed(id) {
  const url = new URL("/api/spans/search", collector);
  url.searchParams.set("service", service);
  url.searchParams.set("attr.celld.workflow.instance_id", id);
  url.searchParams.set("limit", "500");
  const result = await (await fetch(url)).json();
  return result.data ?? [];
}
const operation = (spans, name) => spans.filter((value) => value.span.operationName === name);
const supplier = (spans) => spans.filter((value) => value.span.tags["celld.workflow.step_name"] === "supplier");
async function socketHistory(id, options = {}) {
  const url = new URL("/socket", base.replace("http:", "ws:"));
  url.searchParams.set("id", id);
  url.searchParams.set("options", JSON.stringify(options));
  const socket = new WebSocket(url);
  const events = [];
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.close();
      reject(new Error("WebSocket history timed out"));
    }, 10000);
    socket.addEventListener("message", (event) => events.push(JSON.parse(event.data)));
    socket.addEventListener("error", () => { clearTimeout(timer); reject(new Error("WebSocket error")); });
    socket.addEventListener("close", (event) => {
      clearTimeout(timer);
      if (event.code === 1000) resolve();
      else reject(new Error(`WebSocket closed ${event.code}`));
    });
  });
  return events;
}
function check(events, terminal) {
  assert.ok(events.length > 0);
  assert.equal(events.at(-1).type, terminal);
  for (let i = 0; i < events.length; i++) {
    assert.equal(events[i].eventId, i === 0 ? events[0].eventId : events[i - 1].eventId + 1);
    assert.ok(Number.isSafeInteger(events[i].timestamp));
  }
}

try {
  await start();
  await create("retry", "retry");
  const retry = (await drain("retry")).events;
  check(retry, "workflow_completed");
  assert.deepEqual(types(retry, "attempt_started").filter((e) => e.stepName === "supplier").map((e) => e.attempt), [1, 2, 3]);
  assert.deepEqual(types(retry, "attempt_errored").map((e) => e.retryDelayMs), [30, 30]);
  assert.equal(types(retry, "step_started").length, 2);
  assert.equal(types(retry, "step_started")[1].config.retries.delay, "[dynamic]");
  assert.equal(types(retry, "step_completed")[1].output.attempt, 3);
  const retained = await drain("retry");
  assert.deepEqual(retained.events, retry);
  assert.equal(retained.again.done, true);
  const resumed = await drain("retry", { cursor: retry[5].eventId });
  assert.deepEqual(resumed.events, retry.slice(6));
  assert.deepEqual((await drain("retry", { cursor: retry.at(-1).eventId })).events, []);
  assert.deepEqual((await drain("retry", { filter: ["step_completed"] })).events, types(retry, "step_completed"));
  assert.deepEqual((await drain("retry", { filter: [] })).events, []);
  assert.equal((await request("/using", "retry")).value.type, "workflow_queued");
  for (const options of [{ cursor: -1 }, { cursor: 99999 }, { filter: ["wrong"] }, { cursor: 0.5 }, { extra: 1 }]) {
    await assert.rejects(drain("retry", options));
  }
  console.log("PASS live/history, retries, dynamic config, cursors, filters and terminal completion");

  await create("timeout", "timeout");
  const timeout = (await drain("timeout")).events;
  check(timeout, "workflow_errored");
  assert.deepEqual(types(timeout, "attempt_errored").map((e) => e.attempt), [1, 2, 3]);
  assert.ok(types(timeout, "attempt_errored").every((e) => e.error.message.includes("timed out")));
  assert.equal(types(timeout, "step_errored").length, 1);
  for (const mode of ["large", "policy-error"]) {
    await create(mode, mode);
    const history = (await drain(mode)).events;
    check(history, "workflow_errored");
    assert.equal(types(history, "attempt_errored").length, 1);
    assert.equal(types(history, "step_errored").length, 1);
    assert.equal(types(history, "step_completed").length, 1); // stable only
  }
  console.log("PASS native timeouts, result validation and retry-policy failure evidence");

  await create("wait", "wait");
  const waiting = await drain("wait", { filter: ["wait_started"] }, 1);
  const pending = drain("wait", { cursor: waiting.events[0].eventId });
  await delay(50);
  await request("/signal", "wait");
  const signalled = await pending;
  assert.equal(types(signalled.events, "wait_completed").length, 1);
  await create("wait-timeout", "wait-timeout");
  assert.equal(types((await drain("wait-timeout")).events, "wait_timed_out").length, 1);
  await create("sleep", "sleep");
  const sleeps = (await drain("sleep")).events;
  assert.equal(types(sleeps, "sleep_started").length, 2);
  assert.equal(types(sleeps, "sleep_completed").length, 2);
  await create("parallel", "parallel");
  const parallel = (await drain("parallel")).events;
  check(parallel, "workflow_completed");
  assert.equal(types(parallel, "step_completed").filter((e) => e.stepName === "same").length, 2);
  await create("many", "many");
  const many = (await drain("many")).events;
  assert.ok(many.length > 128);
  assert.deepEqual((await drain("many", { filter: ["workflow_completed"] })).events, [many.at(-1)]);
  console.log("PASS pending live wait, wait timeout, sleeps, parallel occurrences and bounded catch-up");

  await create("controls", "terminate");
  const started = await drain("controls", { filter: ["attempt_started"] }, 2);
  const last = started.events.at(-1).eventId;
  const disposed = await request("/dispose", "controls", { options: JSON.stringify({ cursor: last, filter: [] }) });
  assert.equal(disposed.next.done, true);
  assert.equal(disposed.later.done, true);
  assert.equal(disposed.concurrent, true);
  await request("/pause", "controls");
  await until(() => request("/status", "controls"), (v) => v.status === "paused");
  await request("/resume", "controls");
  const controls = (await drain("controls")).events;
  assert.equal(types(controls, "workflow_waiting_for_pause").length, 1);
  assert.equal(types(controls, "workflow_paused").length, 1);
  assert.equal(types(controls, "workflow_started").length, 1);
  await create("terminated", "terminate");
  await drain("terminated", { filter: ["attempt_started"] }, 2);
  await request("/terminate", "terminated");
  const terminated = (await drain("terminated")).events;
  check(terminated, "workflow_terminated");
  await delay(1600);
  assert.deepEqual((await drain("terminated")).events, terminated); // no late settlement
  await request("/restart", "retry");
  const restarted = (await drain("retry")).events;
  assert.ok(restarted[0].eventId > retry.at(-1).eventId);
  check(restarted, "workflow_completed");
  assert.deepEqual((await drain("retry", { cursor: 0 })).events, retry);
  console.log("PASS disposal/cancellation, pause/resume, termination fencing and restart history");

  await create("crash", "crash");
  const before = await drain("crash", { filter: ["attempt_started"] }, 2);
  await until(() => request("/evidence", "crash"), (v) => v.length === 2);
  if (collector) {
    await until(() => observed("crash"), (spans) => operation(spans, "workflow.step").length === 1);
  }
  await stop(true);
  await start();
  const after = (await drain("crash", { cursor: before.events.at(-1).eventId })).events;
  assert.deepEqual(types(after, "attempt_started").map((e) => e.attempt), [1]);
  assert.equal(types(after, "step_started").length, 0);
  const evidence = await request("/evidence", "crash");
  assert.deepEqual(evidence, ["stable", { attempt: 1 }, { attempt: 1 }]);
  assert.equal(after.at(-1).type, "workflow_completed");
  const crash = (await drain("crash")).events;
  check(crash, "workflow_completed");
  assert.equal(types(crash, "step_started").length, 2);
  console.log("PASS durable history across SIGKILL/restart and same-attempt callback recovery");

  await create("socket", "retry");
  const socket = await socketHistory("socket");
  check(socket, "workflow_completed");
  assert.deepEqual(socket, (await drain("socket")).events);
  assert.deepEqual(await socketHistory("socket", { cursor: socket[5].eventId }), socket.slice(6));
  console.log("PASS native-history WebSocket delivery and cursor reconnect");

  await create("expiry", "parallel", { retention: 500 });
  await drain("expiry");
  await delay(600);
  await assert.rejects(drain("expiry"), /does not exist/);
  await request("/delete", "parallel");
  await assert.rejects(drain("parallel"), /does not exist/);
  console.log("PASS history retention and instance deletion");

  if (collector) {
    const retrySpans = await until(() => observed("retry"), (spans) => operation(spans, "workflow").length === 2);
    const roots = operation(retrySpans, "workflow");
    assert.equal(roots[0].traceId, roots[1].traceId);
    assert.notEqual(roots[0].span.spanId, roots[1].span.spanId);
    const retryAttempts = supplier(operation(retrySpans, "workflow.attempt"));
    assert.equal(retryAttempts.length, 6);
    assert.equal(retryAttempts.filter((value) => value.span.status === "error").length, 4);
    assert.equal(operation(retrySpans, "workflow.retry_delay").length, 4);
    for (const attempt of retryAttempts) {
      const parent = operation(retrySpans, "workflow.step").find((value) => value.span.spanId === attempt.span.parentSpanId);
      assert.ok(parent);
      assert.equal(parent.traceId, attempt.traceId);
    }
    const timeoutSpans = await until(() => observed("timeout"), (spans) => operation(spans, "workflow").length === 1);
    const timeoutAttempts = supplier(operation(timeoutSpans, "workflow.attempt"));
    assert.equal(timeoutAttempts.length, 3);
    assert.ok(timeoutAttempts.every((value) => value.span.status === "error" && value.span.durationMs >= 40));
    assert.equal(operation(timeoutSpans, "workflow")[0].span.status, "error");
    const recovered = await until(() => observed("crash"), (spans) => operation(spans, "workflow").length === 1);
    const physical = supplier(operation(recovered, "workflow.attempt"));
    assert.equal(physical.length, 2);
    assert.ok(physical.every((value) => Number(value.span.tags["celld.workflow.attempt"]) === 1));
    assert.ok(physical.some((value) => String(value.span.tags["celld.workflow.end_observed_at_recovery"]) === "true"));
    assert.equal(new Set(recovered.map((value) => value.traceId)).size, 1);
    const sleeps = await observed("sleep");
    assert.equal(operation(sleeps, "workflow.sleep").length, 2);
    assert.equal(operation(await observed("wait"), "workflow.wait").length, 1);
    const timedWait = operation(await observed("wait-timeout"), "workflow.wait");
    assert.equal(timedWait[0].span.status, "error");
    assert.equal(operation(await observed("controls"), "workflow.pause").length, 1);
    const policySpans = await observed("policy-error");
    const policyStep = operation(policySpans, "workflow.step")
      .find((value) => value.span.tags["celld.workflow.step_name"] === "policy");
    const policyAttempt = operation(policySpans, "workflow.attempt")
      .find((value) => value.span.tags["celld.workflow.step_name"] === "policy");
    assert.equal(policyStep.span.status, "error");
    assert.equal(policyAttempt.span.status, "error");
    const same = operation(await observed("parallel"), "workflow.step")
      .filter((value) => value.span.tags["celld.workflow.step_name"] === "same");
    assert.deepEqual(same.map((value) => Number(value.span.tags["celld.workflow.step_count"])).sort(), [1, 2]);

    await create("fractional", "fractional");
    await drain("fractional");
    const fractional = await until(() => observed("fractional"), (spans) => operation(spans, "workflow").length === 1);
    assert.equal(operation(fractional, "workflow.step").length, 2); // stable + fractional
    const fractionalStep = operation(fractional, "workflow.step")
      .find((value) => value.span.tags["celld.workflow.step_name"] === "fractional");
    assert.equal(Number(fractionalStep.span.tags["celld.workflow.retry_delay"]), 30.5);
    assert.equal(Number(fractionalStep.span.tags["celld.workflow.timeout"]), 5000.5);
    assert.equal(operation(fractional, "workflow.sleep").length, 1);

    const externalTrace = randomBytes(16).toString("hex");
    await request("/create", "trace-parent", { mode: "parallel-fetch" }, {
      traceparent: `00-${externalTrace}-1234567890abcdef-01`,
    });
    const linked = (await drain("trace-parent")).events;
    const parentSpans = await until(() => observed("trace-parent"), (spans) => operation(spans, "workflow").length === 1);
    assert.ok(parentSpans.every((value) => value.traceId === externalTrace));
    const details = await (await fetch(new URL(`/api/traces/${externalTrace}/spans`, collector))).json();
    const traceSpans = (details.data ?? details.spans ?? details).map((value) => value.span ?? value);
    const attempts = operation(parentSpans, "workflow.attempt")
      .filter((value) => value.span.tags["celld.workflow.step_name"] === "downstream");
    assert.equal(attempts.length, 2);
    const fetches = traceSpans.filter((span) => span.operationName === "fetch");
    assert.equal(fetches.length, 2);
    assert.ok(fetches.every((span) => attempts.some((value) => value.span.spanId === span.parentSpanId)));
    const output = linked.at(-1).output;
    assert.ok(output.every((value) => value.traceparent.split("-")[1] === externalTrace));
    const logs = await (await fetch(new URL(`/api/traces/${externalTrace}/logs`, collector))).json();
    const records = (logs.data ?? logs.logs ?? logs).map((value) => value.log ?? value);
    const callbackLogs = records.filter((value) => (value.body ?? "").includes("workflow-fetch trace-parent"));
    assert.equal(callbackLogs.length, 2);
    assert.ok(callbackLogs.every((value) => attempts.some((attempt) => attempt.span.spanId === value.spanId)));

    const unsampledTrace = randomBytes(16).toString("hex");
    await request("/create", "trace-unsampled", { mode: "parallel-fetch" }, {
      traceparent: `00-${unsampledTrace}-1234567890abcdef-00`,
    });
    const unsampled = (await drain("trace-unsampled")).events;
    assert.ok(unsampled.at(-1).output.every((value) => {
      const parts = value.traceparent.split("-");
      return parts[1] === unsampledTrace && parts[3] === "00";
    }));
    // Another finished Workflow proves the exporter has passed this interval.
    await create("sampling-fence", "parallel");
    await drain("sampling-fence");
    await until(() => observed("sampling-fence"), (spans) => operation(spans, "workflow").length === 1);
    assert.equal((await observed("trace-unsampled")).length, 0);
    console.log(`PASS motel ${service}: native hierarchy, fractional policies, timeouts, waits, pause, restart, crash recovery, W3C propagation/sampling and async console correlation`);
  }
} catch (error) {
  console.error(logs.slice(-8000));
  console.error(error);
  process.exitCode = 1;
} finally {
  await stop();
  await rm(directory, { recursive: true, force: true });
}
process.exit(process.exitCode ?? 0);
