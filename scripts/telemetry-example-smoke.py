#!/usr/bin/env python3
# Copyright 2026 Deno Land Inc. Apache-2.0 license.
"""Verify the native tracing example against isolated real celld dev processes."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import runpy
import shutil
import tempfile
from urllib.parse import urlencode
import uuid

ROOT = Path(__file__).resolve().parents[1]
helpers = runpy.run_path(str(ROOT / "scripts/monitor-smoke.py"))
Node, request, wait_for = (helpers[name] for name in ("Node", "request", "wait_for"))


def fetch(node, customer, *, fail=False, sampled=True):
    trace, parent = uuid.uuid4().hex, uuid.uuid4().hex[:16]
    status, _, data = request(node.worker + "/?" + urlencode({"customer": customer, "fail": int(fail)}), headers={
        "traceparent": f"00-{trace}-{parent}-{'01' if sampled else '00'}",
    })
    body = json.loads(data)
    assert status == (500 if fail else 200), (status, body)
    assert body["traceId"] == trace, body
    if not fail:
        assert body["customer"] == customer and body["checkedOut"] and body["sampled"] is sampled, body
        assert len({body["spanId"], body["inventorySpanId"], body["parentSpanId"]}) == 3, body
    return {"customer": customer, "trace": trace, "parent": parent, "fail": fail, "sampled": sampled, "response": body}


def check_export(node, case):
    def exported():
        status, _, data = request(node.internal + "/monitor/traces?" + urlencode({"minutes": 1, "trace_id": case["trace"]}))
        page = json.loads(data)
        assert status == 200 and not page["truncated"], (status, page)
        spans = [span for span in page["spans"] if (span.get("metadata") or {}).get("scopeName") == "example"]
        logs = [log for log in page["logs"] if (log.get("metadata") or {}).get("scopeName") == "example"]
        return (page, spans, logs) if len(spans) == (2 if case["sampled"] else 0) and len(logs) == 3 else None

    page, spans, logs = wait_for(exported, timeout=20)
    by_name = {span["name"]: span for span in spans}
    by_message = {log["body"]: log for log in logs}
    checkout_id = by_message["checkout started"]["span_id"]
    inventory_id = by_message["inventory ready"]["span_id"]
    assert checkout_id != inventory_id
    assert all(log["trace_id"] == case["trace"] for log in logs)
    assert all(log["metadata"]["attributes"]["customer"] == case["customer"] for log in logs)
    ready = by_message["inventory ready"]["metadata"]
    assert ready["attributes"]["cart.items"] == 2 and ready["attributes"]["available"] is True, ready
    assert ready["body"]["details"] == ready["attributes"], ready
    final = by_message["inventory unavailable" if case["fail"] else "checkout completed"]
    assert final["span_id"] == (inventory_id if case["fail"] else checkout_id), final
    assert final["severity_number"] == (17 if case["fail"] else 9), final

    console = [log for log in page["logs"] if "native console inside inventory" in log["body"]]
    if not case["sampled"]:
        assert not page["spans"] and not console, page
        status, _, data = request(node.internal + "/monitor/traces?" + urlencode({"minutes": 1, "trace_id": case["trace"], "signal": "logs"}))
        assert status == 200 and len(json.loads(data)["logs"]) == 3, data
        return

    checkout, inventory = by_name["example.checkout"], by_name["example.inventory"]
    assert checkout["span_id"] == checkout_id and inventory["span_id"] == inventory_id
    assert inventory["parent_span_id"] == checkout_id
    handler = next(span for span in page["spans"] if span["span_id"] == checkout["parent_span_id"])
    assert handler["parent_span_id"] == case["parent"], handler
    assert all(span["ok"] is not case["fail"] for span in spans), spans
    assert all(span["metadata"]["statusCode"] == (2 if case["fail"] else 1) for span in spans), spans
    assert len(console) == 1 and console[0]["span_id"] == inventory_id, console
    if case["fail"]:
        assert all(span["error"] == "inventory unavailable" for span in spans), spans
    else:
        body = case["response"]
        assert body["spanId"] == checkout_id and body["inventorySpanId"] == inventory_id
        assert body["parentSpanId"] == handler["span_id"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/celld")
    args = parser.parse_args()
    state = Path(tempfile.mkdtemp(prefix="celld-telemetry-example-", dir=os.environ.get("TMPDIR")))
    project = state / "project"
    shutil.copytree(ROOT / "examples/telemetry", project)
    print("ARTIFACTS:", state, flush=True)
    cases = []
    for enabled in (True, False):
        node = Node(args.binary.resolve(), project, monitor=True, settings={
            "CELLD_OTEL": "1" if enabled else "0", "CELLD_OTEL_FLUSH_MS": "500",
            "CELLD_OTEL_RETENTION": "none", "OTEL_TRACES_SAMPLER": "parentbased_always_on",
        })
        try:
            node.wait_ready()
            if enabled:
                cases = [fetch(node, "alice"), fetch(node, "bob", fail=True)]
                with ThreadPoolExecutor(max_workers=12) as executor:
                    cases.extend(executor.map(lambda index: fetch(node, f"parallel-{index}"), range(12)))
                cases.append(fetch(node, "unsampled", sampled=False))
                assert len({case["trace"] for case in cases}) == len(cases)
                for case in cases:
                    check_export(node, case)
                print("PASS: native handler parenting, nested spans, native awaits, typed logs, errors and console attribution", flush=True)
                print("PASS: 12 overlapping requests keep separate contexts; unsampled structured logs remain queryable", flush=True)
            else:
                for fail in (False, True):
                    status, _, data = request(node.worker + "/?" + urlencode({"fail": int(fail)}))
                    body = json.loads(data)
                    assert status == (500 if fail else 200) and body["traceId"] is None, (status, body)
                assert request(node.internal + "/monitor/traces?minutes=1")[0] == 503
                print("PASS: disabled telemetry leaves success and failure behavior intact", flush=True)
        except Exception:
            print("\n".join(node.logs))
            raise
        finally:
            node.close()
            (state / f"node-{'enabled' if enabled else 'disabled'}.log").write_text("\n".join(node.logs))
    (state / "report.json").write_text(json.dumps({"passed": True, "cases": cases}, indent=2))


if __name__ == "__main__":
    main()
