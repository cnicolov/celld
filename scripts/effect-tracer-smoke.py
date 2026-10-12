#!/usr/bin/env python3
# Copyright 2026 Deno Land Inc. Apache-2.0 license.
"""Test real Effect 4 traces/logs in an isolated celld dev Worker and Durable Object.

Needs npm and esbuild. Installs the pinned library in temporary state, not the
repository. Defaults to the native isolate bridge and real Parquet dashboard
queries. --transport otlp preserves the separate OTLP/JSON compatibility test;
never deploys to, restarts, or changes the running RustFS demo.
"""
import argparse
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from html import escape
from datetime import datetime, timezone
import os
from pathlib import Path
import queue
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen
import uuid

ROOT = Path(__file__).resolve().parents[1]


def request(url, headers=None):
    try:
        with urlopen(Request(url, headers=headers or {}), timeout=30) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def value(data):
    if "arrayValue" in data:
        return [value(item) for item in data["arrayValue"]["values"]]
    if "kvlistValue" in data:
        return {item["key"]: value(item["value"]) for item in data["kvlistValue"]["values"]}
    return next(iter(data.values()), None)


def attributes(record):
    return {a["key"]: value(a["value"]) for a in record.get("attributes", [])}


def otlp_value(data):
    if isinstance(data, bool):
        return {"boolValue": data}
    if isinstance(data, int):
        return {"intValue": data}
    if isinstance(data, float):
        return {"doubleValue": data}
    if isinstance(data, list):
        return {"arrayValue": {"values": [otlp_value(item) for item in data]}}
    if isinstance(data, dict):
        return {"kvlistValue": {"values": [{"key": key, "value": otlp_value(item)} for key, item in data.items()]}}
    return {"stringValue": data}


def native_records(page):
    """Run identical assertions against persisted native and external records."""
    spans, logs = [], []
    for span in page["spans"]:
        meta = span.get("metadata") or {}
        if meta.get("scopeName") != "effect":
            continue
        spans.append({
            "traceId": span["trace_id"], "spanId": span["span_id"], "parentSpanId": span["parent_span_id"],
            "name": span["name"], "kind": span["kind"],
            "startTimeUnixNano": str(span["start_unix_us"] * 1000),
            "endTimeUnixNano": str((span["start_unix_us"] + span["duration_us"]) * 1000),
            "status": {"code": meta["statusCode"]},
            "attributes": [{"key": key, "value": otlp_value(item)} for key, item in meta.get("attributes", {}).items()],
            "events": meta.get("events", []),
        })
    for log in page["logs"]:
        meta = log.get("metadata") or {}
        if meta.get("scopeName") != "effect":
            continue
        logs.append({
            "traceId": log["trace_id"], "spanId": log["span_id"],
            "severityNumber": log["severity_number"], "severityText": log["severity_text"].title(),
            "timeUnixNano": str(log["time_unix_us"] * 1000), "observedTimeUnixNano": str(log["time_unix_us"] * 1000),
            "body": otlp_value(meta["body"]),
            "attributes": [{"key": key, "value": otlp_value(item)} for key, item in meta.get("attributes", {}).items()],
        })
    return spans, logs


def capture_html(state, report, captures):
    """Static view of actual captured data, never a substitute for ingestion."""
    selected = next(case for case in report["cases"] if case["mode"] == "failure")
    trace = selected["response"]["traceId"]
    all_spans, all_logs = [], []
    for capture in captures:
        if capture["status"] != 200:
            continue
        for resource in capture["body"].get("resourceSpans", []):
            all_spans.extend(span for scope in resource["scopeSpans"] for span in scope["spans"])
        for resource in capture["body"].get("resourceLogs", []):
            all_logs.extend(log for scope in resource["scopeLogs"] for log in scope["logRecords"])
    spans = sorted((span for span in all_spans if span["traceId"] == trace), key=lambda span: int(span["startTimeUnixNano"]))
    logs = sorted((log for log in all_logs if log.get("traceId") == trace), key=lambda log: int(log["timeUnixNano"]))
    names = {span["spanId"]: span["name"] for span in spans}
    start = min(int(span["startTimeUnixNano"]) for span in spans)
    duration = max(int(span["endTimeUnixNano"]) for span in spans) - start
    span_rows, log_rows = [], []
    for span in spans:
        failed = span["status"]["code"] == 2
        ms = (int(span["endTimeUnixNano"]) - int(span["startTimeUnixNano"])) / 1e6
        left = (int(span["startTimeUnixNano"]) - start) / max(duration, 1) * 100
        width = max(.5, ms * 1e6 / max(duration, 1) * 100)
        parent = names.get(span.get("parentSpanId"), "incoming remote parent")
        span_rows.append(f'<tr><td><b>{escape(span["name"])}</b><small>parent: {escape(parent)}</small></td><td>{ms:.2f} ms</td><td class="timeline"><div><i class="{"failed" if failed else ""}" style="left:{left:.2f}%;width:{width:.2f}%"></i></div></td><td><span class="pill {"error" if failed else "ok"}">{"ERROR" if failed else "OK"}</span></td></tr>')
    for log in logs:
        level = log["severityText"].upper()
        at = datetime.fromtimestamp(int(log["timeUnixNano"]) / 1e9, timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        message = value(log["body"])
        message = json.dumps(message) if not isinstance(message, str) else message
        attrs = attributes(log)
        detail = ", ".join(f"{key}={item}" for key, item in attrs.items() if key not in ("test.token", "effect.fiberId"))
        log_rows.append(f'<tr><td class="mono">{at}</td><td><span class="pill {level.lower()}">{level}</span><small>OTLP {log["severityNumber"]}</small></td><td><b>{escape(names.get(log.get("spanId"), "unknown span"))}</b><small class="mono">{escape(log.get("spanId", ""))}</small></td><td class="message">{escape(message)}<small>{escape(detail)}</small></td></tr>')
    html = f'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><link rel="icon" href="data:,"><title>Effect traces &amp; logs — real capture</title>
<style>body{{margin:0;background:#f5f7fa;color:#22262e;font:14px system-ui,sans-serif}}main{{max-width:1400px;margin:40px auto;padding:0 28px}}header{{display:flex;align-items:center;justify-content:space-between}}h1{{margin:0;font-size:32px;letter-spacing:-1px}}h2{{font-size:19px}}p,small{{color:#697386}}small{{display:block;font-size:11px;margin-top:6px;overflow-wrap:anywhere}}a{{color:#354b7c}}.notice{{border:1px solid #ebcb94;background:#fff8e9;border-radius:10px;padding:16px 20px;margin:22px 0}}.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px}}.card,.panel{{background:white;border:1px solid #dce2eb;border-radius:10px;padding:20px}}.card strong{{display:block;font-size:29px;margin-top:8px}}.panel{{margin-top:22px}}.panelhead{{display:flex;align-items:center;justify-content:space-between;gap:24px}}.mono{{font-family:ui-monospace,monospace}}.pill{{font-size:10px;font-weight:700;letter-spacing:.6px;padding:5px 9px;background:#edf0f5;border-radius:5px;white-space:nowrap}}.error,.fatal{{background:#fde9e9;color:#ae3434}}.warn{{background:#fff0d9;color:#996200}}.ok{{background:#e9f6ed;color:#267d40}}.info{{background:#e8efff;color:#365fa4}}.debug,.trace{{background:#eef0f4;color:#6a7384}}table{{width:100%;border-collapse:collapse;margin-top:18px}}th{{text-align:left;background:#f7f9fc;color:#697386;font-size:11px;letter-spacing:.4px;padding:12px}}td{{padding:14px 12px;border-bottom:1px solid #edf0f4;vertical-align:top}}.timeline{{width:35%}}.timeline div{{height:15px;background:#f0f3f7;position:relative;border-radius:3px;overflow:hidden}}.timeline i{{position:absolute;height:100%;background:#637ba8;border-radius:3px}}.timeline i.failed{{background:#cf6767}}.message{{max-width:600px;overflow-wrap:anywhere}}footer{{margin:28px 0;color:#697386;font-size:12px}}@media(max-width:800px){{.cards{{grid-template-columns:repeat(2,1fr)}}.panel{{overflow:auto}}table{{min-width:800px}}}}</style>
<main><header><div><h1>Effect traces &amp; logs</h1><p>Actual OTLP capture · Effect 4.0.3 · celld Worker + Durable Object</p></div><span class="pill">READ-ONLY TEST CAPTURE</span></header>
<div class="notice"><b>Captured telemetry, not dashboard ingestion.</b> These are genuine Effect exporter records from the test. Effect application spans and logs are not yet ingested into celld's Parquet dashboard. <a href="http://127.0.0.1:19180/monitor/">Open the live celld dashboard</a>.</div>
<div class="cards"><div class="card">Fixture requests<strong>{len(report["cases"])}</strong><small>32 overlapping requests</small></div><div class="card">Captured Effect spans<strong>{len(all_spans)}</strong><small>Nested spans and native awaits</small></div><div class="card">Captured Effect logs<strong>{len(all_logs)}</strong><small>Trace, debug, info, warn, error, fatal</small></div><div class="card">Context checks<strong>{report.get("effect_log_correlation_checks", 0)}</strong><small>Request-local span IDs and annotations</small></div></div>
<section class="panel"><div class="panelhead"><div><h2>Payment failure · span timeline</h2><small class="mono">{trace}</small></div><span class="pill error">EXPECTED HTTP 500</span></div><table><thead><tr><th>SPAN / PARENT</th><th>DURATION</th><th>TIMELINE</th><th>STATUS</th></tr></thead><tbody>{"".join(span_rows)}</tbody></table></section>
<section class="panel" id="logs"><div class="panelhead"><div><h2>Correlated Effect logs</h2><p>Actual severity, body, annotations and child-span IDs. Severity samples are deliberately test-generated.</p></div><span class="pill">{len(logs)} RECORDS</span></div><table><thead><tr><th>TIME UTC</th><th>SEVERITY</th><th>CURRENT EFFECT SPAN</th><th>MESSAGE / ATTRIBUTES</th></tr></thead><tbody>{"".join(log_rows)}</tbody></table></section>
<footer>Source: real captured OTLP/HTTP JSON · raw records: <a href="collector.json">collector.json</a> · assertions: <a href="report.json">report.json</a></footer></main></html>'''
    (state / "capture.html").write_text(html)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/celld")
    parser.add_argument("--output", type=Path, help="New artifact directory")
    parser.add_argument("--require-dashboard", action="store_true", help="Fail if Effect application spans or logs are missing from /monitor/traces")
    parser.add_argument("--transport", choices=["native", "otlp"], default="native", help="Native isolate bridge (default), or the original external OTLP compatibility test")
    parser.add_argument("--disabled", action="store_true", help="Check that the native adapter is inert with CELLD_OTEL=0")
    args = parser.parse_args()
    if args.disabled and args.transport != "native":
        parser.error("--disabled tests the native bridge only")
    state = args.output or Path(tempfile.mkdtemp(prefix="celld-effect-tracer-", dir=os.environ.get("TMPDIR")))
    if args.output:
        state.mkdir(parents=True, exist_ok=False)
    project = state / "project"
    project.mkdir()
    shutil.copyfile(ROOT / "scripts/effect-tracer/worker.js", project / "index.js")
    shutil.copyfile(ROOT / "examples/effect/telemetry.js", project / "telemetry.js")
    (project / "package.json").write_text(json.dumps({"private": True, "type": "module", "dependencies": {"effect": "4.0.3"}}))
    subprocess.run(["npm", "install", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=project, check=True)
    lock = threading.Lock()
    spans, effect_logs, attempts, captures = [], [], defaultdict(int), []

    class Capture(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            token = parse_qs(urlparse(self.path).query)["token"][0]
            self.reply(200, {"token": token, "traceparent": self.headers.get("traceparent")})

        def do_POST(self):
            size = int(self.headers.get("Content-Length", 0))
            assert 0 < size <= 2 * 1024 * 1024, size
            body = json.loads(self.rfile.read(size))
            received = [span for resource in body.get("resourceSpans", []) for scope in resource["scopeSpans"] for span in scope["spans"]]
            received_logs = [log for resource in body.get("resourceLogs", []) for scope in resource["scopeLogs"] for log in scope["logRecords"]]
            with lock:
                attempts[self.path] += 1
                status = 400 if self.path.startswith("/refuse/") else 503 if self.path.startswith("/retry/") and attempts[self.path] == 1 else 200
                captures.append({"path": self.path, "status": status, "body": body})
                if status == 200:
                    spans.extend(received)
                    effect_logs.extend(received_logs)
            self.reply(status, {})

    collector = ThreadingHTTPServer(("127.0.0.1", 0), Capture)
    serving = threading.Thread(target=collector.serve_forever, daemon=True)
    serving.start()
    collector_url = f"http://127.0.0.1:{collector.server_port}"
    (project / "wrangler.json").write_text(json.dumps({
        "name": "effect-tracer-fixture", "main": "index.js", "compatibility_date": "2026-01-01",
        "vars": {"COLLECTOR": collector_url, "TELEMETRY": args.transport},
        "durable_objects": {"bindings": [{"name": "CELLS", "class_name": "EffectCell"}]},
        "migrations": [{"tag": "v1", "new_sqlite_classes": ["EffectCell"]}],
    }))
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        worker = f"http://127.0.0.1:{listener.getsockname()[1]}"
    env = {key: value for key, value in os.environ.items() if not key.startswith("CELLD_")}
    env.update(CELLD_MONITOR="1", CELLD_OTEL="1", CELLD_OTEL_FLUSH_MS="500", CELLD_OTEL_RETENTION="none", NO_COLOR="1")
    if args.disabled:
        env["CELLD_OTEL"] = "0"
    node = subprocess.Popen([str(args.binary.resolve()), "dev", str(project.resolve()), "--port", str(urlparse(worker).port), "--no-watch", "--logs"],
                            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    ready = queue.Queue()
    logs = deque(maxlen=100)

    def read_logs():
        operator = None
        with (state / "node.log").open("w") as file:
            for line in node.stdout:
                file.write(line)
                file.flush()
                logs.append(line.rstrip())
                match = re.search(r"celld internal listening on (\S+)", line)
                if match:
                    operator = "http://" + match[1]
                if re.match(r"\s+ready\s+http://", line):
                    ready.put(operator)

    reader = threading.Thread(target=read_logs, daemon=True)
    reader.start()
    cases, findings = [], []
    native = args.transport == "native"
    report = {"effect_version": "4.0.3", "exporter": "Native isolate bridge" if native else "Effect OtlpTracer + OtlpLogger, OTLP/HTTP JSON", "cases": cases, "findings": findings}
    query_lock = threading.Lock()
    print("ARTIFACTS:", state, flush=True)
    try:
        try:
            operator = ready.get(timeout=60)
        except queue.Empty:
            raise AssertionError("Effect fixture did not become ready:\n" + "\n".join(logs))
        assert operator
        if args.disabled:
            status, body = request(worker + "/?token=disabled")
            assert status == 200 and body["ok"] and not body.get("nativeParent"), body
            assert body["disabledChecks"] == {"refused": True, "untouched": True}, body
            assert not captures, captures
            status, _ = request(operator + "/monitor/traces?minutes=1")
            assert status == 503, status
            report.update(runtime_checks_passed=True, telemetry_disabled=True)
            print("PASS: disabled native adapter leaves requests working, emits no records and never reads record getters", flush=True)
            return

        def run(mode, sampled=True):
            token, trace, parent = uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex[:16]
            status, body = request(worker + "/?" + urlencode({"mode": mode, "token": token}),
                                   {"traceparent": f"00-{trace}-{parent}-{'01' if sampled else '00'}"})
            assert status == (500 if mode in ("failure", "defect") else 200), (mode, status, body)
            assert body["token"] == token
            if native:
                assert body["samplingProbe"]["sampled"] is sampled, body
            if native and mode == "bridge":
                checks = body["bridgeChecks"]
                assert checks["caught"] and checks["malformedSpanRejected"] and checks["malformedLogRejected"] and checks["internalsHidden"], checks
                assert checks["original"] == checks["restoredAfterThrow"] == checks["restoredAfterAwait"], checks
                assert checks["resumed"]["context"]["spanId"] == "1234567890abcdef" and checks["resumed"]["frame"] == token, checks
            if native or mode != "unbridged":
                assert body["traceId"] == trace, (mode, trace, body)
            for branch in body["branches"]:
                assert branch["before"] == branch["after"] and branch["traceId"] == body["traceId"], branch
            assert len({branch["before"] for branch in body["branches"]}) == 2
            downstream = body["downstream"]["traceparent"].split("-")
            assert downstream[1] == body["traceId"] and downstream[3] == ("01" if sampled else "00"), (mode, downstream, body)
            assert body["downstream"]["token"] == token
            if native:
                expected_spans = 0 if not sampled else 10 if mode == "cell" else 7 if mode in ("failure", "defect", "cancel") else 6
                expected_logs = 17 if mode == "cell" else 10 if mode == "failure" else 9
                until = time.monotonic() + 20
                while True:
                    with query_lock:
                        status_code, persisted = request(operator + "/monitor/traces?" + urlencode({"minutes": 1, "trace_id": body["traceId"]}))
                    assert status_code == 200 and not persisted["truncated"], persisted
                    recorded, recorded_logs = native_records(persisted)
                    if len(recorded) >= expected_spans and len(recorded_logs) >= expected_logs:
                        break
                    assert time.monotonic() < until, (mode, body, persisted)
                    time.sleep(.1)
                assert len(recorded) == expected_spans, (mode, recorded)
                if mode == "bridge":
                    checks = body["recordChecks"]
                    assert checks["accepted"] and checks["oversizedRejected"] and checks["getterRejected"], checks
                    status_code, probe = request(operator + "/monitor/traces?" + urlencode({"minutes": 1, "trace_id": checks["traceId"]}))
                    assert status_code == 200 and len(probe["logs"]) == 1, probe
                    meta = probe["logs"][0]["metadata"]
                    assert "cell" not in meta, meta
                    assert meta["body"]["cycle"]["self"] == "[circular]" and meta["body"]["bigint"] == "123", meta
                    assert meta["body"]["error"]["message"] == "serializer" and len(meta["body"]["huge"]) <= 4096, meta
                with lock:
                    spans.extend(recorded)
                    effect_logs.extend(recorded_logs)
            else:
                with lock:
                    recorded = [span for span in spans if span["traceId"] == body["traceId"]]
                    recorded_logs = [log for log in effect_logs if log.get("traceId") == body["traceId"]]
            if not sampled or (mode == "refuse" and not native):
                assert not recorded, (mode, recorded)
            else:
                by_id = {span["spanId"]: span for span in recorded}
                root = by_id[body["spanId"]]
                assert root["name"] == "effect.checkout" and attributes(root)["test.token"] == token
                assert root["status"]["code"] == (2 if mode in ("failure", "defect") else 1), root
                if native:
                    assert root["parentSpanId"] == body["nativeParent"]["spanId"], (root, body)
                    handler = next(span for span in persisted["spans"] if span["span_id"] == body["nativeParent"]["spanId"])
                    assert handler["parent_span_id"] == parent
                elif mode != "unbridged":
                    assert root["parentSpanId"] == parent
                for branch in body["branches"]:
                    span = by_id[branch["before"]]
                    assert span["parentSpanId"] == root["spanId"] and attributes(span)["test.token"] == token
                    assert any(event["name"] == "after.await" for event in span["events"])
                    nested = next(span for span in recorded if span["name"] == branch["name"] + ".nested")
                    assert nested["parentSpanId"] == branch["before"]
                assert by_id[downstream[2]]["kind"] == 3, (downstream, recorded)
                for span in recorded:
                    assert int(span["endTimeUnixNano"]) >= int(span["startTimeUnixNano"]), span
                    assert len(span["spanId"]) == 16
                if mode in ("failure", "defect"):
                    failed = next(span for span in recorded if span["name"].startswith("effect.payment."))
                    assert failed["status"]["code"] == 2 and any(event["name"] == "exception" for event in failed["events"])
                if mode == "cancel":
                    cancelled = next(span for span in recorded if span["name"] == "effect.cancelled")
                    assert attributes(cancelled)["effect.fiber.interrupted"] is True and cancelled["status"]["code"] == 0
                if mode == "cell":
                    assert body["cell"]["ok"] and body["cell"]["traceId"] == trace
                    if native:
                        assert by_id[body["cell"]["spanId"]]["parentSpanId"] == body["cell"]["nativeParent"]["spanId"]
                        cell_handler = next(span for span in persisted["spans"] if span["span_id"] == body["cell"]["nativeParent"]["spanId"])
                        assert cell_handler["parent_span_id"] == body["cellCallerSpanId"], (cell_handler, body)
                    else:
                        assert by_id[body["cell"]["spanId"]]["parentSpanId"] == body["cellCallerSpanId"]
            if mode == "refuse" and not native:
                assert not recorded_logs
            else:
                assert len(recorded_logs) == (17 if mode == "cell" else 10 if mode == "failure" else 9), (mode, recorded_logs)
                assert {(log["severityText"], log["severityNumber"]) for log in recorded_logs} == {("Trace", 1), ("Debug", 5), ("Info", 9), ("Warn", 13), ("Error", 17), ("Fatal", 21)}, recorded_logs
                for log in recorded_logs:
                    attrs = attributes(log)
                    assert attrs["test.token"] == token and attrs["customer.id"] == "alice" and attrs["cart.items"] == 2 and attrs["test.verified"] is True, log
                    assert int(log["timeUnixNano"]) > 0 and int(log["observedTimeUnixNano"]) > 0, log
                    if sampled:
                        assert log["spanId"] in by_id, log
                batches = [log for log in recorded_logs if value(log["body"]) == ["effect.batch", token, 2]]
                expected_roots = {body["spanId"]}
                if mode == "cell":
                    expected_roots.add(body["cell"]["spanId"])
                assert {log["spanId"] for log in batches} == expected_roots, batches
                for branch in body["branches"]:
                    entry = next(log for log in recorded_logs if attributes(log).get("test.branch") == branch["name"])
                    assert entry["spanId"] == branch["after"] and value(entry["body"]) == f"effect.after.await {branch['name']} {token}", entry
                if mode == "failure":
                    error = next(log for log in recorded_logs if "exception.message" in attributes(log))
                    assert error["severityNumber"] == 17 and attributes(error)["exception.message"] == f"payment declined: {token}" and error["spanId"] == failed["spanId"], error
            case = {"mode": mode, "sampled": sampled, "status": status, "spans": len(recorded), "effect_logs": len(recorded_logs), "input_trace": trace, "response": body}
            with lock:
                cases.append(case)
            return case

        modes = ["success", "failure", "defect", "cancel", "cell", "unbridged"]
        if not native:
            modes += ["retry", "refuse"]
        else:
            modes += ["bridge"]
        for mode in modes:
            run(mode)
            print("PASS:", mode, flush=True)
        unsampled = run("success", sampled=False)
        if not native:
            assert attempts["/retry/v1/traces"] == 2
            assert attempts["/refuse/v1/traces"] == 1
            assert attempts["/retry/v1/logs"] == 2 and attempts["/refuse/v1/logs"] == 1
        else:
            assert not captures, "Native adapter made an HTTP telemetry export"
        print("PASS: sampling, handler parenting, downstream propagation and transport checks", flush=True)
        with ThreadPoolExecutor(max_workers=8) as pool:
            concurrent = list(pool.map(lambda _: run("success"), range(32)))
        assert len({case["response"]["spanId"] for case in concurrent}) == 32
        assert len({case["response"]["traceId"] for case in concurrent}) == 32
        print("PASS: 32 overlapping requests, parallel fibers, native awaits and persisted application records", flush=True)

        # Wait for this isolated node's real Parquet export, not the collector.
        selected = concurrent[-1]
        deadline = time.monotonic() + 15
        while True:
            status, dashboard = request(operator + "/monitor/traces?" + urlencode({"minutes": 1, "trace_id": selected["input_trace"]}))
            assert status == 200, dashboard
            if any(span["name"] == "celld.fetch" for span in dashboard["spans"]) and dashboard["logs"]:
                break
            assert time.monotonic() < deadline, dashboard
            time.sleep(.2)
        report["dashboard_sample"] = dashboard
        assert not dashboard["truncated"], "Cannot distinguish missing Effect ingestion from a partial dashboard scan"
        for case in concurrent:
            while True:
                status, correlated = request(operator + "/monitor/traces?" + urlencode({"minutes": 1, "trace_id": case["input_trace"]}))
                assert status == 200 and not correlated["truncated"], correlated
                handler = next((span for span in correlated["spans"] if span["name"] == "celld.fetch"), None)
                if handler and len(correlated["logs"]) >= 3:
                    break
                assert time.monotonic() < deadline, correlated
                time.sleep(.05)
            console = [log for log in correlated["logs"] if not log.get("metadata")]
            assert len(console) == 3, correlated
            expected_console_spans = {case["response"]["spanId"]} | {branch["before"] for branch in case["response"]["branches"]} if native else {handler["span_id"]}
            assert all(case["response"]["token"] in log["body"] and log["span_id"] in expected_console_spans for log in console), correlated
            if native:
                for branch in case["response"]["branches"]:
                    entry = next(log for log in console if branch["name"] in log["body"])
                    assert entry["span_id"] == branch["before"], (branch, entry)
        status, silent = request(operator + "/monitor/traces?" + urlencode({"minutes": 1, "trace_id": unsampled["input_trace"]}))
        assert status == 200 and not silent["truncated"] and not silent["spans"], silent
        assert len(silent["logs"]) == (9 if native else 0) and all(log.get("metadata") for log in silent["logs"]), silent
        if native:
            status, unsampled_logs = request(operator + "/monitor/traces?" + urlencode({"minutes": 1, "signal": "logs", "q": unsampled["response"]["token"]}))
            assert status == 200 and not unsampled_logs["truncated"] and len(unsampled_logs["logs"]) == 9, unsampled_logs
            report["unsampled_logs_browsable"] = True
        report["native_console_correlation_checks"] = len(concurrent)
        report["effect_log_correlation_checks"] = len(concurrent)
        report["unsampled_effect_logs_exported"] = unsampled["effect_logs"]
        print("PASS: console attribution for all 32 concurrent requests; unsampled native spans/logs suppressed", flush=True)
        status, all_effect = request(operator + "/monitor/traces?minutes=1&q=effect.")
        assert status == 200 and not all_effect["truncated"], all_effect
        report["dashboard_unfiltered_effect_names"] = [span["name"] for span in all_effect["spans"]]
        report["dashboard_application_spans"] = any(span["name"].startswith("effect.") for span in dashboard["spans"])
        if not report["dashboard_application_spans"]:
            findings.append("Effect application spans reach its OTLP collector but are not ingested into celld's Parquet dashboard.")
        unbridged = next(case for case in cases if case["mode"] == "unbridged")
        report["automatic_incoming_parent_bridge"] = unbridged["response"]["traceId"] == unbridged["input_trace"]
        if not report["automatic_incoming_parent_bridge"]:
            findings.append("Effect needs an explicit incoming traceparent parent; celld's ambient request span is not automatically exposed to the Effect tracer.")
        report["console_logs_use_effect_span_ids"] = any(log["span_id"] in {span["spanId"] for span in spans} for log in dashboard["logs"])
        if not report["console_logs_use_effect_span_ids"]:
            findings.append("Console logs remain correlated to celld's handler span, not to the current Effect application span.")
        report["dashboard_effect_logs"] = any((log.get("metadata") or {}).get("scopeName") == "effect" for log in dashboard["logs"])
        if not report["dashboard_effect_logs"]:
            findings.append("Effect OtlpLogger records reach its collector with child-span IDs, but are not ingested into the dashboard; captured console records are separate.")
        report["runtime_checks_passed"] = True
        report["accepted_effect_spans"] = len(spans)
        report["accepted_effect_logs"] = len(effect_logs)
        print("PASS: six Effect log severities, annotations, array messages, error causes, child-span correlation and unsampled logs", flush=True)
        print("PASS: celld automatic spans and console logs persisted and queryable", flush=True)
        for finding in findings:
            print("GAP:", finding, flush=True)
        if args.require_dashboard:
            missing = [label for label, present in [("spans", report["dashboard_application_spans"]), ("logs", report["dashboard_effect_logs"])] if not present]
            assert not missing, "Effect application " + " and ".join(missing) + " are missing from the dashboard"
    except BaseException as error:
        report["failure"] = str(error)
        raise
    finally:
        (state / "report.json").write_text(json.dumps(report, indent=2))
        with lock:
            (state / "collector.json").write_text(json.dumps(captures, indent=2))
            if report.get("runtime_checks_passed") and not native:
                capture_html(state, report, captures)
        if node.poll() is None:
            node.send_signal(signal.SIGTERM)
            try:
                node.wait(timeout=25)
            except subprocess.TimeoutExpired:
                node.kill()
                node.wait()
        reader.join(timeout=3)
        node.stdout.close()
        collector.shutdown()
        collector.server_close()
        serving.join(timeout=3)


if __name__ == "__main__":
    main()
