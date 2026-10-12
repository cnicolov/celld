#!/usr/bin/env python3
# Copyright 2026 Deno Land Inc. Apache-2.0 license.
"""Real RustFS + three-process placement, GPU standby, and bucket-fallback smoke.

Pool names are test labels, not a claim that this machine has physical GPUs.
Requires boto3 and a running local RustFS. Creates an isolated bucket and leaves
its evidence intact; never redeploys or restarts an existing cluster.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]


def get(url, timeout=15):
    try:
        with urlopen(url, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except HTTPError as error:
        body = error.read()
        try:
            body = json.loads(body)
        except ValueError:
            body = body.decode(errors="replace")
        return error.code, body


def wait(check, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, URLError, ValueError) as error:
            last = error
        time.sleep(0.2)
    raise AssertionError(f"condition timed out: {last}; inspect the retained node logs")


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class Node:
    def __init__(self, binary, state, env, bucket, endpoint, name, pool):
        self.name = name
        self.worker_port, self.operator_port = port(), port()
        while self.operator_port == self.worker_port:
            self.operator_port = port()
        self.worker = f"http://127.0.0.1:{self.worker_port}"
        self.operator = f"http://127.0.0.1:{self.operator_port}"
        self.log = state / f"{name}.log"
        self.command = [str(binary), "--no-control-plane", "--bucket", bucket,
                        "--endpoint", endpoint, "--region", "us-east-1",
                        "--listen", f"127.0.0.1:{self.worker_port}",
                        "--internal-listen", f"127.0.0.1:{self.operator_port}",
                        "--advertise", f"127.0.0.1:{self.operator_port}"]
        self.env = env | {"CELLD_NODE": name, "CELLD_PLACEMENT_POOL": pool,
                          "CELLD_WATCH": str(state / name)}
        self.process = None
        self.start()

    def start(self):
        with self.log.open("ab") as log:
            self.process = subprocess.Popen(self.command, env=self.env, stdout=log, stderr=log)
        def ready():
            if self.process.poll() is not None:
                raise RuntimeError(f"{self.name} exited during startup; inspect {self.log}")
            return get(self.worker + "/.well-known/celld/health")[0] == 200
        try:
            wait(ready)
        except Exception:
            self.kill()
            raise

    def kill(self):
        if self.process and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=15)

    def close(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=45)
            except subprocess.TimeoutExpired:
                self.kill()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/celld")
    parser.add_argument("--endpoint", default="http://127.0.0.1:19000")
    args = parser.parse_args()
    temporary_root = Path(os.environ.get("TMPDIR", tempfile.gettempdir()))
    if (temporary_root / "opencode").is_dir():
        temporary_root /= "opencode"
    state = Path(tempfile.mkdtemp(prefix="celld-placement-smoke-", dir=temporary_root))
    bucket = "celld-placement-" + uuid.uuid4().hex[:12]
    project = state / "project"
    shutil.copytree(ROOT / "examples/placement", project)
    env = {key: value for key, value in os.environ.items() if not key.startswith("CELLD_")}
    env.update(AWS_ACCESS_KEY_ID=os.environ.get("AWS_ACCESS_KEY_ID", "celld-demo"),
               AWS_SECRET_ACCESS_KEY=os.environ.get("AWS_SECRET_ACCESS_KEY", "celld-demo-secret"),
               AWS_REGION="us-east-1", AWS_DEFAULT_REGION="us-east-1", S3_ENDPOINT=args.endpoint,
               CELLD_DURABILITY="fleet", CELLD_HOT_RECOVERY="1", CELLD_MONITOR="1",
               CELLD_TTL_MS="4000", CELLD_LOG_FLUSH_MS="500", CELLD_WAKER_TICK_MS="500",
               CELLD_REBALANCE_INTERVAL_MS="1000", CELLD_IDLE_EVICT_S="10", CELLD_TOKIO_THREADS="2",
               NO_COLOR="1")
    s3 = boto3.client("s3", endpoint_url=args.endpoint, region_name="us-east-1",
                      aws_access_key_id=env["AWS_ACCESS_KEY_ID"], aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
                      config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    s3.create_bucket(Bucket=bucket)
    print(f"ARTIFACTS: {state}\nBUCKET: {bucket}", flush=True)
    subprocess.run([str(args.binary.resolve()), "deploy", str(project), "--bucket", bucket,
                    "--endpoint", args.endpoint, "--region", "us-east-1"], env=env, check=True)

    def object_json(key):
        try:
            return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise

    def owner(cell_id):
        return object_json(f"cells/GPUCell:{cell_id}/own.json")

    nodes, checks = [], []

    def passed(message):
        checks.append(message)
        print("PASS:", message, flush=True)

    try:
        cpu = Node(args.binary.resolve(), state, env, bucket, args.endpoint, "cpu-a", "default")
        nodes.append(cpu)

        def call(node=cpu, **params):
            return get(node.worker + "/?" + urlencode(params))

        status, registered = call(mode="register")
        assert status == 200 and owner(registered["id"])["node"] == "", (status, registered)
        status, unavailable = call(mode="read")
        assert status == 503 and "PlacementUnavailable" in unavailable["error"], (status, unavailable)
        assert owner(registered["id"])["epoch"] == 0
        passed("persistent lazy creation succeeds without a GPU; execution fails closed on CPU")

        a = Node(args.binary.resolve(), state, env, bucket, args.endpoint, "gpu-a", "gpu")
        nodes.append(a)
        result = wait(lambda: (response if (response := call())[0] == 200 else None))
        cell_id = result[1]["id"]
        assert result[1]["count"] == 1 and owner(cell_id)["node"] == "gpu-a", result
        assert owner(cell_id)["placement_pool"] == "gpu"
        assert (object_json("nodes/gpu-a.json").get("log") or {}).get("ensemble", []) == []
        assert s3.list_objects_v2(Bucket=bucket, Prefix=f"cells/GPUCell:{cell_id}/ltx/").get("Contents"), "bucket gate must persist the lone-GPU write"
        assert list((state / "cpu-a").glob("**/hot-standby/**/*.sqlite")) == []
        passed("one GPU plus CPU recruits no incompatible follower and acknowledges bucket-backed writes")

        with ThreadPoolExecutor(max_workers=8) as executor:
            responses = list(executor.map(lambda _: call(mode="register"), range(16)))
        assert all(status == 200 and body["id"] == cell_id for status, body in responses)
        status, conflict = call(placement="default", mode="register")
        assert status == 503 and "immutable" in conflict["error"], conflict
        status, invalid = call(placement="../gpu", mode="register")
        assert status == 503 and "placement pool" in invalid["error"], invalid
        uniques = [call(mode="register", unique=1)[1]["id"] for _ in range(2)]
        assert len(set(uniques)) == 2
        status, legacy = call(name="legacy-default", mode="legacy")
        assert status == 200 and owner(legacy["id"])["node"] == "cpu-a", (status, legacy)
        assert call(name="legacy-default", mode="register")[0] == 503
        assert call(mode="rpc") == (200, {"id": cell_id, "count": 1})
        passed("named/raced creation, random IDs, RPC, immutable pool conflicts, validation and legacy default routing")

        a.kill()
        wait(lambda: call(mode="read")[0] == 503, timeout=20)
        a.start()
        wait(lambda: call(mode="read") == (200, {"id": cell_id, "count": 1, "alarms": 0}), timeout=60)
        passed("bucket-only acknowledged state survives owner SIGKILL and restart")

        b = Node(args.binary.resolve(), state, env, bucket, args.endpoint, "gpu-b", "gpu")
        nodes.append(b)
        wait(lambda: (object_json("nodes/gpu-a.json") or {}).get("log", {}).get("ensemble") == ["gpu-b"])
        status, body = call()
        assert status == 200 and body["count"] == 2, (status, body)
        # Continued writes ensure any handoff has an observed standby entry.
        for _ in range(3):
            assert call()[0] == 200
        count = call(mode="read")[1]["count"]
        current = owner(cell_id)
        primary = a if current["node"] == a.name else b
        follower = b if primary is a else a
        wait(lambda: any(image["cell"] == f"GPUCell:{cell_id}" and image["ready"]
                         for image in get(follower.operator + "/hot-recovery")[1]["candidates"]))
        assert get(cpu.operator + "/hot-recovery")[1]["candidates"] == []
        passed("two GPUs recruit a same-pool follower and materialize its SQLite standby; CPU stays excluded")

        # Idle eviction and rebalance are enabled: every resulting owner remains
        # GPU-bound. Alarm discovery must work even with a CPU fleet-GC waker.
        assert call(mode="alarm")[0] == 200
        wait(lambda: call(mode="read")[1].get("alarms", 0) == 1)
        assert owner(cell_id)["node"] in (a.name, b.name)
        passed("alarm waking and enabled idle handoff/rebalancing preserve GPU ownership")

        current = owner(cell_id)
        primary = a if current["node"] == a.name else b
        follower = b if primary is a else a
        wait(lambda: any(image["cell"] == f"GPUCell:{cell_id}" and image["cell_epoch"] == current["epoch"] and image["ready"]
                         for image in get(follower.operator + "/hot-recovery")[1]["candidates"]))
        primary.kill()
        recovered = wait(lambda: (response if (response := call(mode="read"))[0] == 200 else None), timeout=60)
        assert recovered[1]["count"] == count and recovered[1]["alarms"] == 1, recovered
        assert owner(cell_id)["node"] == follower.name
        assert "hot_recovery_promoted" in follower.log.read_text(), follower.log
        assert call()[1]["count"] == count + 1
        passed("GPU owner SIGKILL promotes the GPU standby, keeps exact state, and resumes bucket fallback")

        follower.kill()
        wait(lambda: call(mode="read")[0] == 503, timeout=20)
        assert owner(cell_id)["node"] in (a.name, b.name)
        assert get(cpu.operator + "/hot-recovery")[1]["candidates"] == []
        passed("loss of all GPU nodes never authorizes CPU execution or CPU hot replication")
        follower.start()
        wait(lambda: call(mode="read")[1].get("count") == count + 1)
        time.sleep(11)
        assert call(mode="read")[1]["count"] == count + 1
        assert owner(cell_id)["placement_pool"] == "gpu" and owner(cell_id)["node"] == follower.name
        passed("post-promotion bucket fallback survives another SIGKILL and an idle eviction/reactivation")
        (state / "report.json").write_text(json.dumps({"passed": True, "bucket": bucket, "checks": checks}, indent=2))
    finally:
        for node in reversed(nodes):
            node.close()


if __name__ == "__main__":
    main()
