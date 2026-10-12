#!/usr/bin/env python3
# Copyright 2026 Deno Land Inc. Apache-2.0 license.
"""Bounded load test for the local monitor demo, not a production benchmark."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen


def fetch(url):
    started = time.perf_counter()
    try:
        with urlopen(url, timeout=15) as response:
            body = response.read()
            return response.status, (time.perf_counter() - started) * 1000, body
    except HTTPError as error:
        return error.code, (time.perf_counter() - started) * 1000, error.read()
    except OSError:
        return 0, (time.perf_counter() - started) * 1000, b""


def percentile(values, fraction):
    values = sorted(values)
    return round(values[min(len(values) - 1, int((len(values) - 1) * fraction))], 2) if values else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-port", type=int, default=19080)
    parser.add_argument("--operator-port", type=int, default=19180)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--max-requests", type=int, default=50000,
                        help="Safety cap per phase; capped phases are not sustained measurements")
    parser.add_argument("--output")
    args = parser.parse_args()
    if not 1 <= args.seconds <= 60 or not 1 <= args.max_requests <= 100000:
        parser.error("Use 1–60 seconds and a request cap of 1–100000 per phase")
    operators = [f"http://127.0.0.1:{args.operator_port + i}" for i in range(3)]
    workers = [f"http://127.0.0.1:{args.worker_port + i}" for i in range(3)]
    customers = ["alice", "bob", "charlie", "diana", "elena", "frank", "grace", "henry", "ivy", "jack", "kate", "liam"]
    results = {"kind": "local debug-build load test", "workload": "12 carts; bounded CPU + 5 ms async wait; no writes or intentional errors",
               "seconds_per_phase": args.seconds, "max_requests_per_phase": args.max_requests,
               "limitations": "Closed-loop Python clients and all three debug-build nodes share this Mac; not a production capacity benchmark. Background demo traffic includes intentional errors.",
               "phases": []}
    for concurrency in [10, 50, 100]:
        latencies, statuses, monitors, trace_queries = [], Counter(), [], []
        lock = threading.Lock()
        begin, done = threading.Event(), threading.Event()
        issued = 0
        client_finished = []
        started = deadline = 0

        def client(client_id):
            nonlocal issued
            begin.wait()
            while time.perf_counter() < deadline:
                with lock:
                    if issued >= args.max_requests:
                        break
                    index = issued
                    issued += 1
                url = workers[(index + client_id) % 3] + "/checkout?" + urlencode({"customer": customers[index % len(customers)], "work": 1, "wait": 5})
                status, latency, _ = fetch(url)
                with lock:
                    statuses[status] += 1
                    latencies.append(latency)
            with lock:
                client_finished.append(time.perf_counter())
                if len(client_finished) == concurrency:
                    done.set()

        def monitor():
            begin.wait()
            while not done.is_set() and time.perf_counter() < deadline:
                status, latency, body = fetch(operators[0] + "/monitor/cluster")
                sample = {"status": status, "latency_ms": round(latency, 2)}
                if status == 200:
                    c = json.loads(body)
                    sample.update(partial=c["partial"], nodes=sum(n["snapshot"] is not None for n in c["nodes"]),
                        rss_bytes=sum((n["snapshot"]["node"]["rss_bytes"] or 0) for n in c["nodes"] if n["snapshot"]),
                        cpu_percent=sum((n["snapshot"]["node"]["cpu_percent_of_one_core"] or 0) for n in c["nodes"] if n["snapshot"]),
                        queued_turns=sum(cell["turns"]["queued"] for n in c["nodes"] if n["snapshot"] for cell in n["snapshot"]["cells"]))
                monitors.append(sample)
                done.wait(.5)

        def trace_monitor():
            begin.wait()
            while not done.is_set() and time.perf_counter() < deadline:
                status, latency, body = fetch(operators[0] + "/monitor/traces?minutes=1&errors=1")
                sample = {"status": status, "latency_ms": round(latency, 2)}
                if status == 200:
                    page = json.loads(body)
                    sample.update(spans=len(page["spans"]), scanned_files=page["scanned_files"], partial=page["truncated"])
                trace_queries.append(sample)
                done.wait(2)

        with ThreadPoolExecutor(max_workers=concurrency + 2) as pool:
            tasks = [pool.submit(client, i) for i in range(concurrency)] + [pool.submit(monitor), pool.submit(trace_monitor)]
            started = time.perf_counter()
            deadline = started + args.seconds
            begin.set()
            for task in tasks:
                task.result()
        elapsed = time.perf_counter() - started
        active_elapsed = max(client_finished) - started
        phase = {"concurrency": concurrency, "requests": len(latencies), "elapsed_s": round(elapsed, 2),
            "active_request_s": round(active_elapsed, 2), "request_cap_reached": issued == args.max_requests,
            "requests_per_s": round(len(latencies) / active_elapsed, 2), "status_counts": dict(statuses),
            "latency_ms": {"p50": percentile(latencies, .5), "p95": percentile(latencies, .95), "p99": percentile(latencies, .99), "max": round(max(latencies), 2)},
            "monitor": {"samples": len(monitors), "failures": sum(m["status"] != 200 for m in monitors), "partial_samples": sum(m.get("partial", False) for m in monitors),
                "p95_ms": percentile([m["latency_ms"] for m in monitors], .95), "max_ms": max(m["latency_ms"] for m in monitors),
                "max_queued_turns": max((m.get("queued_turns", 0) for m in monitors), default=0), "max_rss_bytes": max((m.get("rss_bytes", 0) for m in monitors), default=0),
                "max_cpu_percent": max((m.get("cpu_percent", 0) for m in monitors), default=0)},
            "trace_queries": {"samples": len(trace_queries), "failures": sum(q["status"] != 200 for q in trace_queries),
                "p95_ms": percentile([q["latency_ms"] for q in trace_queries], .95), "results": trace_queries}}
        results["phases"].append(phase)
        print(json.dumps(phase, indent=2), flush=True)
    status, latency, body = fetch(operators[0] + "/monitor/traces?minutes=5&errors=1")
    results["post_load_trace_query"] = {"status": status, "latency_ms": round(latency, 2)}
    if status == 200:
        page = json.loads(body)
        results["post_load_trace_query"].update(spans=len(page["spans"]), scanned_files=page["scanned_files"], partial=page["truncated"])
    if args.output:
        with open(args.output, "w") as output:
            json.dump(results, output, indent=2)
    print("POST-LOAD TRACE QUERY:", json.dumps(results["post_load_trace_query"]), flush=True)


if __name__ == "__main__":
    main()
