#!/usr/bin/env python3
# Copyright 2026 Deno Land Inc. Apache-2.0 license.
"""Run three real celld nodes against a local RustFS bucket, with trace traffic.

Start RustFS separately (see docs/monitoring.md). This only touches the supplied
demo bucket and newly created node state. Ctrl-C stops these nodes, not RustFS.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import tempfile
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
SOURCE = """
export class Cart {
  constructor(state) {
    this.state = state;
    state.storage.sql.exec("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, payload BLOB)");
  }
  async fetch(request) {
    const url = new URL(request.url);
    const customer = url.searchParams.get("customer") || "alice";
    console.log("cart request", customer, "version", 1);
    if (url.searchParams.get("seed")) {
      this.state.storage.sql.exec("INSERT OR REPLACE INTO items VALUES (1, zeroblob(?))", Number(url.searchParams.get("seed")));
      this.scratch = new Uint8Array(Number(url.searchParams.get("seed")));
    }
    let result = 0;
    const rounds = Number(url.searchParams.get("work") || 0) * 100000;
    for (let i = 0; i < rounds; i++) result += Math.sqrt(result + 1);
    if (url.searchParams.get("wait")) await new Promise(resolve => setTimeout(resolve, Number(url.searchParams.get("wait"))));
    if (url.searchParams.get("fail")) {
      console.error("checkout rejected", customer, "payment declined");
      throw new Error("demo payment declined for " + customer);
    }
    return Response.json({customer, cell: this.state.id.toString(), result, version: 1});
  }
}
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const customer = url.searchParams.get("customer") || "alice";
    console.info("checkout ingress", customer);
    return env.CARTS.get(env.CARTS.idFromName(customer)).fetch(request);
  }
};
"""

EFFECT_WRAPPER = """
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Telemetry from "./telemetry.js";
const telemetry = Telemetry.layer({ serviceName: "monitor-effect-demo" });
function traced(work, name, request) {
  const url = new URL(request.url);
  const customer = url.searchParams.get("customer") || "alice";
  return Effect.runPromise(Effect.gen(function* () {
    yield* Effect.annotateCurrentSpan({ "customer.id": customer, "demo.native_bridge": true });
    yield* Effect.logInfo("effect.checkout.started", { customer, operation: name });
    const exit = yield* Effect.exit(Effect.promise(work));
    if (Exit.isFailure(exit)) {
      yield* Effect.logError("effect.checkout.failed", exit.cause);
      return yield* Effect.failCause(exit.cause);
    }
    yield* Effect.logInfo("effect.checkout.completed");
    return exit.value;
  }).pipe(Effect.withSpan(name), Effect.annotateLogs({ "customer.id": customer, "demo.native_bridge": true }), Effect.provide(telemetry)));
}
export class Cart extends BasicCart {
  fetch(request) { return traced(() => super.fetch(request), "effect.cart", request); }
}
export default {
  fetch(request, env) { return traced(() => rawWorker.fetch(request, env), "effect.checkout", request); }
};
"""


def get(url, timeout=20):
    try:
        with urlopen(url, timeout=timeout) as response:
            return response.status, response.read()
    except HTTPError as error:
        return error.code, error.read()


def wait(check, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, ValueError):
            pass
        time.sleep(0.2)
    raise RuntimeError("Demo readiness timed out; inspect the node logs")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/celld")
    parser.add_argument("--endpoint", default="http://127.0.0.1:19000")
    parser.add_argument("--bucket", default="celld-monitor-demo")
    parser.add_argument("--worker-port", type=int, default=19080)
    parser.add_argument("--operator-port", type=int, default=19180)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--effect", action="store_true", help="Add Effect spans and structured logs through the native isolate bridge")
    args = parser.parse_args()
    state = args.state or Path(tempfile.mkdtemp(prefix="celld-cluster-demo-", dir=os.environ.get("TMPDIR")))
    state.mkdir(parents=True, exist_ok=True)
    project = state / "project"
    project.mkdir(exist_ok=True)
    source = SOURCE
    if args.effect:
        source = source.replace("export class Cart", "class BasicCart").replace("export default {", "const rawWorker = {") + EFFECT_WRAPPER
        shutil.copyfile(ROOT / "examples/effect/telemetry.js", project / "telemetry.js")
        (project / "package.json").write_text(json.dumps({"private": True, "type": "module", "dependencies": {"effect": "4.0.3"}}))
        subprocess.run(["npm", "install", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=project, check=True)
    (project / "index.js").write_text(source)
    (project / "wrangler.json").write_text(json.dumps({
        "name": "monitor-cluster-demo", "main": "index.js", "compatibility_date": "2026-01-01",
        "durable_objects": {"bindings": [{"name": "CARTS", "class_name": "Cart"}]},
        "migrations": [{"tag": "v1", "new_sqlite_classes": ["Cart"]}],
    }))
    env = {key: value for key, value in os.environ.items() if not key.startswith("CELLD_")}
    env.update(AWS_ACCESS_KEY_ID=os.environ.get("AWS_ACCESS_KEY_ID", "celld-demo"),
               AWS_SECRET_ACCESS_KEY=os.environ.get("AWS_SECRET_ACCESS_KEY", "celld-demo-secret"),
               AWS_REGION="us-east-1", AWS_DEFAULT_REGION="us-east-1", S3_ENDPOINT=args.endpoint,
               CELLD_MONITOR="1", CELLD_OTEL="1", CELLD_OTEL_FLUSH_MS="2000", CELLD_OTEL_SAMPLE_RATIO="1",
               CELLD_OTEL_RETENTION="none", CELLD_TOKIO_THREADS="2", CELLD_REBALANCE_INTERVAL_MS="2000",
               CELLD_PLACEMENT_WEIGHT="1", NO_COLOR="1")
    subprocess.run([str(args.binary), "deploy", str(project), "--bucket", args.bucket, "--endpoint", args.endpoint, "--region", "us-east-1"], env=env, check=True)
    processes, logs = [], []
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    workers = [f"http://127.0.0.1:{args.worker_port + i}" for i in range(3)]
    operators = [f"http://127.0.0.1:{args.operator_port + i}" for i in range(3)]
    try:
        for i in range(3):
            node_env = env | {"CELLD_NODE": f"monitor-{chr(97 + i)}", "CELLD_WATCH": str(state / f"node-{i}")}
            log = (state / f"node-{i}.log").open("w")
            logs.append(log)
            process = subprocess.Popen([str(args.binary), "--no-control-plane", "--bucket", args.bucket,
                "--endpoint", args.endpoint, "--region", "us-east-1", "--listen", f"127.0.0.1:{args.worker_port + i}",
                "--internal-listen", f"127.0.0.1:{args.operator_port + i}", "--advertise", f"127.0.0.1:{args.operator_port + i}"],
                env=node_env, stdout=log, stderr=subprocess.STDOUT)
            processes.append(process)
        (state / "cluster.json").write_text(json.dumps({"workers": workers, "operators": operators, "pids": [p.pid for p in processes], "bucket": args.bucket, "endpoint": args.endpoint}, indent=2))
        for worker in workers:
            wait(lambda: get(worker + "/.well-known/celld/health")[0] == 200)
        print(f"CLUSTER STATE: {state}", flush=True)
        print(f"DASHBOARD: {operators[0]}/monitor/", flush=True)
        # Start cells through different ingress nodes, then produce cross-node
        # traces by accessing those same cells through another node.
        customers = ["alice", "bob", "charlie", "diana", "elena", "frank", "grace", "henry", "ivy", "jack", "kate", "liam"]
        for i, customer in enumerate(customers):
            status, body = get(workers[i % 3] + "/checkout?" + urlencode({"customer": customer, "seed": (i + 1) * 32768}))
            if status != 200:
                raise RuntimeError(f"Seed request failed: {status} {body!r}")
        def cluster_ready():
            status, body = get(operators[0] + "/monitor/cluster")
            c = json.loads(body) if status == 200 else {}
            return c if len(c.get("nodes", [])) == 3 and not c.get("partial", True) else None
        c = wait(cluster_ready)
        assert sum(n["snapshot"]["node"]["resident_cells"] for n in c["nodes"]) >= len(customers)
        status, _ = get(operators[0] + "/peer/monitor")
        assert status in (400, 401, 403), "Unauthenticated peer monitor read was accepted"
        print("VERIFIED: 3 authenticated node snapshots; unauthenticated peer reads refused", flush=True)
        with ThreadPoolExecutor(max_workers=3) as pool:
            tick = 0
            while not stopping:
                if any(p.poll() is not None for p in processes):
                    raise RuntimeError("A demo node exited; inspect its log")
                batch = []
                for i in range(3):
                    params = {"customer": customers[(tick + i) % len(customers)], "work": 3 if i == 0 else 0, "wait": 30 if i == 1 else 0}
                    if tick % 7 == 0 and i == 2:
                        params["fail"] = 1
                    batch.append(pool.submit(get, workers[(tick + i + 1) % 3] + "/checkout?" + urlencode(params)))
                for task in batch:
                    task.result()
                tick += 1
                time.sleep(0.8)
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=25)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
