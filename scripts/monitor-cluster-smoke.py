#!/usr/bin/env python3
# Copyright 2026 Deno Land Inc. Apache-2.0 license.
"""Read-only checks against the running RustFS monitor demo."""
import argparse
from collections import defaultdict
import json
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def request(base, path, headers=None):
    try:
        with urlopen(Request(base + path, headers=headers or {}), timeout=20) as response:
            return response.status, response.read()
    except HTTPError as error:
        return error.code, error.read()


def read(base, path):
    status, body = request(base, path)
    assert status == 200, (path, status, body)
    return json.loads(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:19180")
    args = parser.parse_args()
    base = args.base
    c = read(base, "/monitor/cluster")
    assert len(c["nodes"]) == 3 and not c["partial"], c
    assert all(n["snapshot"] is not None for n in c["nodes"])
    assert c["total_matches"] >= 12
    assert len({n["snapshot"]["process"] for n in c["nodes"]}) == 3
    for member in c["nodes"]:
        scoped = read(base, "/monitor/cluster?" + urlencode({"node": member["id"]}))
        assert len(scoped["nodes"]) == 1 and scoped["nodes"][0]["id"] == member["id"]
    paged = read(base, "/monitor/cluster?limit=2&offset=2")
    assert sum(len(n["snapshot"]["cells"]) for n in paged["nodes"]) == 2
    empty = read(base, "/monitor/cluster?q=no-such-demo-cell")
    assert empty["total_matches"] == 0
    print("PASS: three distinct node processes, cluster/node filters and global pagination")
    for path in ["/monitor/cluster", "/monitor/traces"]:
        assert request(base, path, {"Origin": "http://attacker.invalid"})[0] == 403
    assert request(base, "/peer/monitor")[0] in (400, 401, 403)
    assert request(base, "/monitor/traces?sql=select+1")[0] == 400
    print("PASS: authenticated peer wire, cross-origin refusal and no arbitrary SQL")
    deadline = time.monotonic() + 40
    selected = None
    while time.monotonic() < deadline:
        p = read(base, "/monitor/traces?minutes=5")
        grouped = defaultdict(list)
        for span in p["spans"]:
            grouped[span["trace_id"]].append(span)
        selected = next((trace for trace, spans in grouped.items() if len({s["node"] for s in spans}) > 1), None)
        if selected:
            break
        time.sleep(1)
    assert selected, "No exported cross-node trace found"
    detail = read(base, "/monitor/traces?" + urlencode({"minutes": 5, "trace_id": selected}))
    assert detail["spans"] and all(s["trace_id"] == selected for s in detail["spans"])
    assert len({s["node"] for s in detail["spans"]}) > 1
    assert detail["logs"] and all(log["trace_id"] == selected for log in detail["logs"])
    assert any(s["parent_span_id"] for s in detail["spans"])
    print(f"PASS: persisted cross-node trace {selected}: {len(detail['spans'])} spans, {len(detail['logs'])} correlated logs")
    errors = read(base, "/monitor/traces?minutes=5&errors=1")
    assert errors["spans"] and all(not s["ok"] for s in errors["spans"])
    chosen = p["spans"][0]
    node = read(base, "/monitor/traces?" + urlencode({"minutes": 5, "node": chosen["node"]}))
    assert node["spans"] and all(s["node"] == chosen["node"] for s in node["spans"])
    slow = read(base, "/monitor/traces?minutes=5&min_ms=20")
    assert slow["spans"] and all(s["duration_us"] >= 20_000 for s in slow["spans"])
    assert all(len(page["spans"]) <= 500 and page["scanned_files"] <= 64 for page in [p, detail, errors, node, slow])
    print("PASS: trace error/node/duration filters and bounded response")


if __name__ == "__main__":
    main()
