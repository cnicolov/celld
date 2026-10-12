// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Run through the user's Chrome: playwriter -s <session> --timeout 90000 -f scripts/monitor-traces-ui-smoke.js
// Optional state.monitorBase/state.workerBase override the loopback demo URLs.
const base = state.monitorBase || "http://127.0.0.1:19180";
const worker = state.workerBase || "http://127.0.0.1:19080";
const assert = require("node:assert").strict;
const page = state.regressionPage = await context.newPage();
const observe = async () => {
  console.log("URL:", page.url());
  const view = await snapshot({ page, search: /Trace ID|Run query|Scanning exports|effect.checkout|Correlated logs|Matching logs/i });
  console.log(view.split("\n").filter(line => /Trace ID|Run query|Scanning exports|effect.checkout|Correlated logs|Matching logs|heading|alert/.test(line)).slice(0, 12).join("\n"));
  const logs = await getLatestLogs({ page, sinceLastCall: true });
  if (logs.length) console.log(logs);
};
try {
await page.goto(base + "/monitor/", { waitUntil: "domcontentloaded" });
await observe();
const initialResponse = page.waitForResponse(res => res.url().includes("/monitor/traces?"), { timeout: 20000 });
await page.getByRole("button", { name: "Traces", exact: true }).click();
await observe();
const initial = await initialResponse;
assert.equal(initial.status(), 200, "The initial list query must succeed to test stale-window reuse");
assert.ok((await initial.json()).spans.length > 0, "The initial list query did not load trace data");
await page.getByRole("button", { name: "Run query", exact: true }).waitFor({ state: "visible", timeout: 20000 });

// Create a trace AFTER the initial list query. Entering it must not reuse that
// query's now-expired end time; selecting a list row still must preserve it.
const started = Date.now();
const trace = require("node:crypto").randomBytes(16).toString("hex");
const response = await fetch(worker + "/checkout?customer=alice&wait=10", {
  headers: { traceparent: `00-${trace}-0123456789abcdef-01` },
});
assert.equal(response.status, 200);
await response.text();
let persisted;
const deadline = Date.now() + 20000;
do {
  const result = await fetch(base + `/monitor/traces?from=${started - 1000}&to=${Date.now()}&trace_id=${trace}`);
  persisted = result.ok ? await result.json() : undefined;
  if (persisted?.spans.some(span => span.name === "effect.checkout") && persisted.logs.some(log => log.metadata?.scopeName === "effect")) break;
  assert.ok(Date.now() < deadline, "Fresh native records were not exported");
  await new Promise(resolve => setTimeout(resolve, 250));
} while (true);
const input = page.getByRole("textbox", { name: "Trace ID", exact: true });
await input.click();
await input.fill(trace);
await observe();
const pending = page.waitForResponse(res => res.url().includes("/monitor/traces?") && res.url().includes(trace), { timeout: 20000 });
await page.getByRole("button", { name: "Run query", exact: true }).click();
await observe();
const result = await pending;
const params = new URL(result.url()).searchParams;
assert.ok(!params.has("to") || Number(params.get("to")) >= started, "Entering a fresh Trace ID reused the old list query window");
assert.equal(result.status(), 200);
const records = await result.json();
assert.ok(records.spans.some(span => span.name === "effect.checkout"), "Effect span missing from manual Trace ID query");
assert.ok(records.logs.some(log => log.metadata?.scopeName === "effect"), "Structured logs missing from manual Trace ID query");
await page.getByRole("heading", { name: "Correlated logs", exact: true }).waitFor({ state: "visible", timeout: 5000 });
assert.ok(await page.getByRole("row").filter({ hasText: "effect.checkout" }).count());
await observe();
console.log("PASS: a manually entered fresh Trace ID uses the current window and renders real Effect spans/logs");
state.regressionTrace = trace;
state.regressionRecords = records;

// Reloading a copied URL must open the actual trace, not the Cells landing
// page, and retain its historical bounds.
const shared = page.url();
assert.ok(shared.includes(`#trace=${trace}&from=`), "Trace drill-down has no shareable URL");
const linkedResponse = page.waitForResponse(res => res.url().includes("/monitor/traces?") && res.url().includes(trace), { timeout: 20000 });
await page.reload({ waitUntil: "domcontentloaded" });
await observe();
const linked = await linkedResponse;
assert.equal(linked.status(), 200);
const linkedParams = new URL(linked.url()).searchParams;
assert.equal(Number(linkedParams.get("from")), Math.floor(records.from_unix_us / 1000));
assert.equal(Number(linkedParams.get("to")), Math.floor(records.to_unix_us / 1000));
await page.getByRole("heading", { name: "Correlated logs", exact: true }).waitFor({ state: "visible", timeout: 5000 });
assert.ok(await page.getByRole("row").filter({ hasText: "effect.checkout" }).count());
await observe();
console.log("PASS: a shared trace URL reloads the real timeline with its historical window");

await page.getByRole("checkbox", { name: "Logs only", exact: true }).click();
await observe();
const logResponse = page.waitForResponse(res => res.url().includes("signal=logs") && res.url().includes(trace), { timeout: 20000 });
await page.getByRole("button", { name: "Run query", exact: true }).click();
await observe();
const logs = await logResponse;
assert.equal(logs.status(), 200);
const logRecords = (await logs.json()).logs;
assert.ok(logRecords.length > 0 && logRecords.every(log => log.trace_id === trace));
await page.getByRole("heading", { name: "Matching logs", exact: true }).waitFor({ state: "visible", timeout: 5000 });
assert.ok(await page.getByRole("row").filter({ hasText: "effect.checkout.started" }).count());
await observe();
console.log("PASS: logs-only mode honours an explicit Trace ID and renders structured application logs");

await page.getByRole("checkbox", { name: "Logs only", exact: true }).click();
await observe();
await input.click();
await input.fill("");
await observe();
const search = page.getByRole("searchbox", { name: "Search spans / logs / attributes", exact: true });
await search.click();
await search.fill("effect.checkout");
await observe();
const listResponse = page.waitForResponse(res => res.url().includes("q=effect.checkout"), { timeout: 20000 });
await page.getByRole("button", { name: "Run query", exact: true }).click();
await observe();
const list = await (await listResponse).json();
await page.getByRole("button", { name: "effect.checkout", exact: true }).first().waitFor({ state: "visible", timeout: 5000 });
await observe();
const selectedResponse = page.waitForResponse(res => {
  if (!res.url().includes("/monitor/traces?")) return false;
  return Boolean(new URL(res.url()).searchParams.get("trace_id"));
}, { timeout: 20000 });
await page.getByRole("button", { name: "effect.checkout", exact: true }).first().click();
await observe();
const selected = await selectedResponse;
const selectedParams = new URL(selected.url()).searchParams;
assert.equal(Number(selectedParams.get("from")), Math.floor(list.from_unix_us / 1000));
assert.equal(Number(selectedParams.get("to")), Math.floor(list.to_unix_us / 1000));
assert.ok((await selected.json()).spans.some(span => span.name === "effect.checkout"));
await page.getByRole("heading", { name: "Correlated logs", exact: true }).waitFor({ state: "visible", timeout: 5000 });
await observe();
console.log("PASS: selecting a list row preserves the list's original historical window");
state.regressionReport = { passed: true, trace, checks: ["fresh Trace ID", "shared URL reload", "logs-only trace filter", "historical row drill-down"] };
console.log(JSON.stringify(state.regressionReport));
} finally {
// Only this script's own tab is closed; the user's monitor remains open.
await page.close();
}
