#!/usr/bin/env python3
# Copyright 2026 Deno Land Inc. Apache-2.0 license.
"""Keep writing while a hot follower is killed, absent for a minute, and restarted.

Owns three same-pool celld processes and a new RustFS bucket. Preserves logs,
request receipts, sampled ensembles/TXIDs, and copied standby integrity evidence.
Requires boto3; pool names are synthetic labels, not GPU hardware qualification.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
helpers = runpy.run_path(str(ROOT / "scripts/placement-pools-smoke.py"))
Node, get, wait = (helpers[key] for key in ("Node", "get", "wait"))
ROW_BYTES = 16384
BATCH_ROWS = 64
MIB = 1024 * 1024
GIB = 1024 * MIB


def http(url, body=None, timeout=15):
    request = Request(url, data=body, headers={"Content-Type": "application/octet-stream"})
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except HTTPError as error:
        return error.code, error.read()


def payload(seq):
    seed = hashlib.sha256(f"hot-follower-rejoin/{seq}".encode()).digest()
    return seed * (ROW_BYTES // len(seed))


def preload_payload(first):
    # Reproducible, incompressible bytes; never manufacture a large database
    # out of zeros/repeated blocks or retain the full dataset in client memory.
    return hashlib.shake_256(f"hot-follower-rejoin/preload/{first}".encode()).digest(BATCH_ROWS * ROW_BYTES)


def verify_preload(read_range, receipts):
    for receipt in receipts:
        body = read_range(receipt["first"], receipt["rows"])
        assert len(body) == receipt["bytes"] and hashlib.sha256(body).hexdigest() == receipt["sha256"], receipt


def disk_sample(root):
    sizes = {"disk_bytes": 0, "peerlog_bytes": 0, "hot_standby_bytes": 0, "seed_bytes": 0}
    for path in root.rglob("*"):
        try:
            if not path.is_file():
                continue
            size = path.stat().st_size
            parts = path.relative_to(root).parts
            sizes["disk_bytes"] += size
            if parts[0] == "peerlog":
                sizes["peerlog_bytes"] += size
            elif parts[0] == "hot-standby":
                sizes["hot_standby_bytes"] += size
                if len(parts) > 1 and parts[1].startswith(".seed-"):
                    sizes["seed_bytes"] += size
        except OSError:
            # A cache/tail can be retired between listing and stat.
            continue
    return sizes


class Traffic:
    def __init__(self, url, state, workers, rate):
        self.url, self.state, self.workers, self.rate = url, state, workers, rate
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.next_seq, self.phase = 0, "warmup"
        self.receipts, self.threads = [], []
        self.ledger = (state / "receipts.jsonl").open("w", buffering=1)

    def set_phase(self, phase):
        with self.lock:
            self.phase = phase

    def start(self):
        for index in range(self.workers):
            thread = threading.Thread(target=self.write, args=(index,), daemon=True)
            self.threads.append(thread)
            thread.start()

    def write(self, index):
        self.stop.wait(index / self.rate)
        while not self.stop.is_set():
            start = time.monotonic()
            with self.lock:
                seq, phase = self.next_seq, self.phase
                self.next_seq += 1
            body = payload(seq)
            receipt = {"seq": seq, "sent_phase": phase, "sent_at": time.time(),
                       "sha256": hashlib.sha256(body).hexdigest(), "ok": False}
            try:
                status, response = http(self.url + "/import?" + urlencode({"first": seq}), body)
                receipt["status"] = status
                if status != 200:
                    raise RuntimeError(f"HTTP {status}: {response[:512]!r}")
                reply = json.loads(response)
                if reply != {"first": seq, "rows": 1, "bytes": ROW_BYTES}:
                    raise RuntimeError(f"invalid receipt: {reply}")
                receipt["ok"] = True
            except Exception as error:
                # Never replay an ambiguous write. Unacknowledged rows may exist,
                # but every successful receipt must survive with its exact bytes.
                receipt["error"] = str(error)
            receipt.update(latency_ms=(time.monotonic() - start) * 1000, finished_at=time.time())
            with self.lock:
                receipt["phase"] = self.phase
                self.receipts.append(receipt)
                self.ledger.write(json.dumps(receipt) + "\n")
            self.stop.wait(max(0, self.workers / self.rate - (time.monotonic() - start)))

    def summary(self, phase=None):
        with self.lock:
            receipts = [receipt for receipt in self.receipts if phase is None or receipt["phase"] == phase]
        latencies = sorted(receipt["latency_ms"] for receipt in receipts)
        def percentile(fraction):
            return round(latencies[min(len(latencies) - 1, int((len(latencies) - 1) * fraction))], 2) if latencies else None
        return {"attempts": len(receipts), "acknowledged": sum(receipt["ok"] for receipt in receipts),
                "errors": sum(not receipt["ok"] for receipt in receipts),
                "p50_ms": percentile(.5), "p95_ms": percentile(.95), "p99_ms": percentile(.99),
                "max_ms": round(max(latencies), 2) if latencies else None}

    def close(self):
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=20)
            if thread.is_alive():
                raise RuntimeError("writer did not stop after its request deadline")
        if not self.ledger.closed:
            self.ledger.close()


def verify_reads(node, acknowledged, preload_receipts, read_timeout=60, progress=None):
    def read_range(first, count):
        request = {"node": node.name, "first": first, "count": count, "read_timeout": read_timeout}
        if progress:
            progress({"event": "verification_read_started", **request})
        try:
            status, body = http(node.worker + "/read?" + urlencode({"first": first, "count": count}), timeout=read_timeout)
        except OSError as error:
            if progress:
                progress({"event": "verification_read_failed", "error": str(error), **request})
            raise
        assert status == 200, (node.name, first, status, body[:256])
        if progress:
            progress({"event": "verification_read_received", "bytes": len(body), **request})
        return body
    verify_preload(read_range, preload_receipts)
    ids = sorted(acknowledged)
    cursor = 0
    while cursor < len(ids):
        end = cursor + 1
        while end < len(ids) and end - cursor < 64 and ids[end] == ids[end - 1] + 1:
            end += 1
        first, count = ids[cursor], end - cursor
        body = read_range(first, count)
        assert len(body) == count * ROW_BYTES, (node.name, first, len(body))
        for offset, seq in enumerate(ids[cursor:end]):
            actual = body[offset * ROW_BYTES:(offset + 1) * ROW_BYTES]
            assert hashlib.sha256(actual).hexdigest() == acknowledged[seq], (node.name, seq)
        cursor = end


def verify_standby(node, state, acknowledged, preload_receipts, preload_rows, minimum_free=0):
    images = list((state / node.name / "hot-standby").glob("*.sqlite"))
    assert len(images) == 1, (node.name, images)
    if shutil.disk_usage(state).free < minimum_free + images[0].stat().st_size:
        raise RuntimeError("insufficient host disk reserve for the standby verification copy")
    copy = state / f"{node.name}-verified-standby.sqlite"
    shutil.copyfile(images[0], copy)
    with sqlite3.connect(f"file:{copy}?mode=ro&immutable=1", uri=True) as database:
        assert database.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        def read_range(first, count):
            rows = database.execute("SELECT id, payload FROM records WHERE id >= ? AND id < ? ORDER BY id",
                                    (first, first + count)).fetchall()
            assert [row[0] for row in rows] == list(range(first, first + count)), (node.name, first)
            return b"".join(row[1] for row in rows)
        verify_preload(read_range, preload_receipts)
        actual = {seq: hashlib.sha256(data).hexdigest()
                  for seq, data in database.execute("SELECT id, payload FROM records WHERE id >= ? ORDER BY id", (preload_rows,))}
        rows = database.execute("SELECT count(*) FROM records").fetchone()[0]
    for seq, digest in acknowledged.items():
        assert actual.get(seq) == digest, (node.name, seq, "standby missing/corrupt acknowledged row")
    return {"rows": rows, "acknowledged_rows_verified": len(acknowledged) + preload_rows,
            "preload_rows_verified": preload_rows, "database_bytes": copy.stat().st_size,
            "integrity_check": "ok", "copy": str(copy)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/celld")
    parser.add_argument("--endpoint", default="http://127.0.0.1:19000")
    parser.add_argument("--down-seconds", type=float, default=60)
    parser.add_argument("--after-seconds", type=float, default=15)
    parser.add_argument("--rejoin-timeout", type=float, default=120)
    parser.add_argument("--checkpoint-timeout", type=float, default=120)
    parser.add_argument("--warmup-timeout", type=float, default=90)
    parser.add_argument("--preload-mib", type=int, default=0, help="Import this many MiB of incompressible rows before the fault")
    parser.add_argument("--standby-mb", type=int, default=256, help="Per-node standby image budget in MiB")
    parser.add_argument("--minimum-free-gib", type=float, default=8, help="Abort before consuming this host disk reserve")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--rate", type=float, default=20, help="Total target writes/second")
    args = parser.parse_args()
    assert min(args.down_seconds, args.after_seconds, args.rejoin_timeout,
               args.checkpoint_timeout, args.warmup_timeout, args.standby_mb,
               args.minimum_free_gib, args.workers, args.rate) > 0
    assert args.preload_mib >= 0
    if args.preload_mib and args.standby_mb < args.preload_mib * 1.2:
        parser.error("--standby-mb must allow at least 20% SQLite/growth overhead above --preload-mib")
    temporary_root = Path(os.environ.get("TMPDIR", tempfile.gettempdir()))
    if (temporary_root / "opencode").is_dir():
        temporary_root /= "opencode"
    state = Path(tempfile.mkdtemp(prefix="celld-hot-follower-rejoin-", dir=temporary_root))
    minimum_free = int(args.minimum_free_gib * GIB)
    estimated_disk = args.preload_mib * MIB * 12 + minimum_free
    if shutil.disk_usage(state).free < estimated_disk:
        parser.error(f"need about {estimated_disk / GIB:.1f} GiB free for copies, logs, bucket data, seed scratch, and reserve")
    bucket = "celld-follower-rejoin-" + uuid.uuid4().hex[:12]
    project = state / "project"
    project.mkdir()
    fixture = ROOT / "scripts/recovery-import"
    source = (fixture / "worker.js").read_text().rsplit("export default {", 1)[0]
    source += """export default {
  async fetch(request, env) {
    const cell = await env.IMPORT.create({ name: "follower-rejoin", placement: "gpu" });
    if (new URL(request.url).pathname === "/identity") return Response.json({ id: cell.id.toString() });
    return cell.fetch(request);
  },
};
"""
    (project / "worker.js").write_text(source)
    config = json.loads((fixture / "wrangler.json").read_text())
    config["name"] = "hot-follower-rejoin"
    (project / "wrangler.json").write_text(json.dumps(config))
    env = {key: value for key, value in os.environ.items() if not key.startswith("CELLD_")}
    env.update(AWS_ACCESS_KEY_ID=os.environ.get("AWS_ACCESS_KEY_ID", "celld-demo"),
               AWS_SECRET_ACCESS_KEY=os.environ.get("AWS_SECRET_ACCESS_KEY", "celld-demo-secret"),
               AWS_REGION="us-east-1", AWS_DEFAULT_REGION="us-east-1", S3_ENDPOINT=args.endpoint,
               CELLD_DURABILITY="fleet", CELLD_HOT_RECOVERY="1", CELLD_MONITOR="1",
               CELLD_TTL_MS="4000", CELLD_LOG_FLUSH_MS="500", CELLD_REBALANCE_INTERVAL_MS="0",
               CELLD_HOT_STANDBY_MB=str(args.standby_mb), CELLD_TOKIO_THREADS="2", NO_COLOR="1")
    s3 = boto3.client("s3", endpoint_url=args.endpoint, region_name="us-east-1",
                      aws_access_key_id=env["AWS_ACCESS_KEY_ID"], aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
                      aws_session_token=env.get("AWS_SESSION_TOKEN"),
                      config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    s3.create_bucket(Bucket=bucket)
    print(f"ARTIFACTS: {state}\nBUCKET: {bucket}", flush=True)
    subprocess.run([str(args.binary.resolve()), "deploy", str(project), "--bucket", bucket,
                    "--endpoint", args.endpoint, "--region", "us-east-1"], env=env, check=True)
    report = {"passed": False, "bucket": bucket, "output": str(state), "down_seconds": args.down_seconds,
              "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              "workers": args.workers, "target_writes_per_second": args.rate,
              "preload_mib": args.preload_mib, "standby_budget_mib": args.standby_mb,
              "minimum_free_gib": args.minimum_free_gib, "events": []}
    nodes, traffic, sampler = [], None, None
    stop_sampling = threading.Event()
    owner_key = None
    preload_receipts, resource_peaks = [], {}
    preload_rows = args.preload_mib * MIB // ROW_BYTES

    def event(name, **fields):
        entry = {"event": name, "at": time.time(), **fields}
        report["events"].append(entry)
        print(json.dumps(entry), flush=True)
        with (state / "events.jsonl").open("a") as output:
            output.write(json.dumps(entry) + "\n")

    def object_json(key):
        try:
            return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise

    def owner():
        return object_json(owner_key)

    def ensemble():
        return ((object_json(f"nodes/{primary.name}.json") or {}).get("log") or {}).get("ensemble", [])

    def hot_status(node, timeout=15):
        status, page = get(node.operator + "/hot-recovery", timeout=timeout)
        assert status == 200 and page["enabled"], (node.name, status, page)
        return page

    def image(node, page=None):
        page = hot_status(node) if page is None else page
        candidates = [candidate for candidate in page["candidates"]
                      if candidate["cell"] == scope and candidate["cell_epoch"] == initial_owner["epoch"]]
        return candidates[0] if len(candidates) == 1 else None

    def checkpoint_ready(node):
        page = hot_status(node)
        candidate = image(node, page)
        return candidate and candidate["ready"] and page["queued_bytes"] == 0

    def sample():
        with (state / "samples.jsonl").open("w", buffering=1) as output:
            while not stop_sampling.is_set():
                with traffic.lock:
                    phase = traffic.phase
                sample = {"at": time.time(), "phase": phase, "traffic": traffic.summary(),
                          "host_free_bytes": shutil.disk_usage(state).free, "nodes": {}}
                if sample["host_free_bytes"] < minimum_free:
                    report["disk_reserve_violation"] = sample["host_free_bytes"]
                    traffic.stop.set()
                try:
                    sample["owner"] = owner()
                    for node in nodes:
                        lease = object_json(f"nodes/{node.name}.json")
                        details = {"lease": lease, "pid": node.process.pid, "running": node.process.poll() is None,
                                   **disk_sample(state / node.name)}
                        details["rss_bytes"] = 0
                        if details["running"]:
                            rss = subprocess.run(["ps", "-o", "rss=", "-p", str(node.process.pid)],
                                                 capture_output=True, text=True, timeout=5)
                            details["rss_bytes"] = int(rss.stdout.strip() or 0) * 1024
                        peaks = resource_peaks.setdefault(node.name, {}).setdefault(phase, {})
                        for key in ("rss_bytes", "disk_bytes", "peerlog_bytes", "hot_standby_bytes", "seed_bytes"):
                            peaks[key] = max(peaks.get(key, 0), details[key])
                        sample["nodes"][node.name] = details
                        if details["running"]:
                            try:
                                details["hot_status"] = hot_status(node, timeout=2)
                                details["standby"] = image(node, details["hot_status"])
                            except Exception as error:
                                details["sampling_error"] = str(error)
                except Exception as error:
                    sample["sampling_error"] = str(error)
                output.write(json.dumps(sample) + "\n")
                stop_sampling.wait(1)

    try:
        for suffix in ("a", "b", "c"):
            node = Node(args.binary.resolve(), state, env, bucket, args.endpoint, "gpu-" + suffix, "gpu")
            nodes.append(node)
        identity = get(nodes[0].worker + "/identity")[1]["id"]
        scope = "ImportStore:" + identity
        owner_key = f"cells/{scope}/own.json"
        bootstrap = preload_payload(0)[:ROW_BYTES] if preload_rows else payload(0)
        status, body = http(nodes[0].worker + "/import?first=0", bootstrap)
        assert status == 200, (status, body)
        report["bootstrap_acknowledged"] = True
        initial_owner = owner()
        primary = next(node for node in nodes if node.name == initial_owner["node"])
        followers = [node for node in nodes if node is not primary]
        traffic = Traffic(primary.worker, state, args.workers, args.rate)
        # Include the acknowledged bootstrap write in all integrity checks.
        traffic.next_seq = max(1, preload_rows)
        if not preload_rows:
            traffic.receipts.append({"seq": 0, "ok": True, "sha256": hashlib.sha256(payload(0)).hexdigest(),
                                     "phase": "bootstrap", "latency_ms": 0})
            traffic.ledger.write(json.dumps(traffic.receipts[0]) + "\n")
        traffic.set_phase("preload" if preload_rows else "warmup")
        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        if preload_rows:
            # Do not fill a large database under a one-follower startup ensemble
            # and then confuse its later initial recruitment with this fault.
            wait(lambda: set(ensemble()) == {node.name for node in followers}, timeout=args.warmup_timeout)
            started = time.monotonic()
            with (state / "preload-receipts.jsonl").open("w", buffering=1) as ledger:
                for first in range(0, preload_rows, BATCH_ROWS):
                    if shutil.disk_usage(state).free < minimum_free:
                        raise RuntimeError("host disk reserve reached during preload; stopping test-owned nodes")
                    body = preload_payload(first)
                    status, response = http(primary.worker + "/import?" + urlencode({"first": first}), body, timeout=120)
                    expected = {"first": first, "rows": BATCH_ROWS, "bytes": len(body)}
                    assert status == 200 and json.loads(response) == expected, (status, response[:512])
                    receipt = {**expected, "sha256": hashlib.sha256(body).hexdigest(), "at": time.time()}
                    preload_receipts.append(receipt)
                    ledger.write(json.dumps(receipt) + "\n")
                    ledger.flush()
                    os.fsync(ledger.fileno())
                    completed_mib = (first + BATCH_ROWS) * ROW_BYTES // MIB
                    # Keep the bounded 64 MiB checkpoint queue from being the
                    # preload's accidental capacity limit. Fault-phase traffic
                    # is continuous and is not gated on checkpoint readiness.
                    if completed_mib % 16 == 0:
                        wait(lambda: all(hot_status(node)["queued_bytes"] <= 16 * MIB for node in followers),
                             timeout=args.checkpoint_timeout)
                    if completed_mib % 128 == 0 or completed_mib == args.preload_mib:
                        event("preload_progress", acknowledged_mib=completed_mib,
                              elapsed_seconds=round(time.monotonic() - started, 3))
            report["preload_seconds"] = round(time.monotonic() - started, 3)
            report["preload_acknowledged_rows"] = preload_rows
            report["preload_acknowledged_bytes"] = args.preload_mib * MIB
            wait(lambda: all(checkpoint_ready(node) for node in followers), timeout=args.checkpoint_timeout)
            for node in followers:
                files = list((state / node.name / "hot-standby").glob("*.sqlite"))
                assert len(files) == 1 and files[0].stat().st_size >= args.preload_mib * MIB, (node.name, files)
            event("large_database_ready", preload_mib=args.preload_mib, rows=preload_rows,
                  standby_bytes={node.name: hot_status(node)["bytes"] for node in followers})
        traffic.set_phase("warmup")
        assert "disk_reserve_violation" not in report, "host disk reserve reached"
        traffic.start()
        wait(lambda: set(ensemble()) == {node.name for node in followers}
             and all((candidate := image(node)) and candidate["ready"] for node in followers)
             and traffic.summary()["acknowledged"] >= 40, timeout=args.warmup_timeout)
        event("three_node_hot_ready", primary=primary.name, followers=[node.name for node in followers],
              owner=owner(), standbys={node.name: image(node) for node in followers}, traffic=traffic.summary())

        stopped, surviving = followers
        stopped_pid = stopped.process.pid
        traffic.set_phase("follower_down")
        stopped.kill()
        killed = time.monotonic()
        event("follower_killed", follower=stopped.name, pid=stopped_pid, ensemble=ensemble())
        wait(lambda: ensemble() == [surviving.name], timeout=30)
        event("one_follower_ensemble", after_kill_seconds=round(time.monotonic() - killed, 3), ensemble=ensemble())
        remaining = args.down_seconds - (time.monotonic() - killed)
        if remaining > 0:
            stop_sampling.wait(remaining)
        down = traffic.summary("follower_down")
        assert down["acknowledged"] >= max(10, int(args.down_seconds * args.rate * .2)), down
        assert owner() == initial_owner, "a follower fault must not change the live cell owner"
        # Pin a pre-restart canonical TXID rather than accepting any nonempty
        # image. The surviving cache may itself be reseeding after a log swap.
        prefix = f"cells/{scope}/ltx/e{initial_owner['epoch']}/"
        objects = [entry for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
                   for entry in page.get("Contents", [])]
        baseline = max(int(entry["Key"].rsplit("/", 1)[1].removesuffix(".ltx").split("-")[-1], 16)
                       for entry in objects if entry["Key"].endswith(".ltx"))
        event("outage_complete", elapsed_seconds=round(time.monotonic() - killed, 3),
              survivor=surviving.name, survivor_standby=image(surviving), traffic=down)

        traffic.set_phase("follower_rejoining")
        restarting = time.monotonic()
        stopped.start()
        event("follower_restarted", follower=stopped.name, old_pid=stopped_pid, pid=stopped.process.pid,
              same_recovery_directory=str(state / stopped.name), standby=image(stopped),
              startup_seconds=round(time.monotonic() - restarting, 3))
        wait(lambda: set(ensemble()) == {node.name for node in followers}, timeout=args.rejoin_timeout)
        report["ensemble_rejoin_seconds"] = round(time.monotonic() - restarting, 3)
        event("two_follower_ensemble", after_restart_seconds=report["ensemble_rejoin_seconds"], ensemble=ensemble())
        wait(lambda: (candidate := image(stopped)) and candidate["txid"] >= baseline,
             timeout=args.rejoin_timeout)
        report["materialized_rejoin_seconds"] = round(time.monotonic() - restarting, 3)
        event("follower_materialized_again", after_restart_seconds=report["materialized_rejoin_seconds"], ensemble=ensemble(),
              standby=image(stopped), must_cover_pre_restart_txid=baseline, traffic=traffic.summary("follower_rejoining"))
        traffic.set_phase("restored")
        stop_sampling.wait(args.after_seconds)
        assert traffic.summary("restored")["acknowledged"] >= 5
        traffic.close()
        assert "disk_reserve_violation" not in report, "host disk reserve reached"
        draining = time.monotonic()
        report["standbys_at_writes_stopped"] = {node.name: hot_status(node) for node in followers}
        event("writes_stopped", traffic=traffic.summary(), standbys=report["standbys_at_writes_stopped"])
        wait(lambda: all(checkpoint_ready(node) for node in followers),
             timeout=args.checkpoint_timeout)
        report["checkpoint_catchup_seconds"] = round(time.monotonic() - draining, 3)
        event("both_hot_checkpoints_caught_up", after_writes_stopped_seconds=report["checkpoint_catchup_seconds"],
              standbys={node.name: image(node) for node in followers})
        assert owner() == initial_owner
        acknowledged = {receipt["seq"]: receipt["sha256"] for receipt in traffic.receipts if receipt["ok"]}
        traffic.set_phase("verification")
        for node in nodes:
            verify_reads(node, acknowledged, preload_receipts)
            event("ingress_integrity_verified", node=node.name, acknowledged_rows=preload_rows + len(acknowledged))
        report["standby_integrity"] = {node.name: verify_standby(node, state, acknowledged, preload_receipts, preload_rows, minimum_free)
                                       for node in followers}
        stop_sampling.set()
        sampler.join(timeout=30)
        samples = [json.loads(line) for line in (state / "samples.jsonl").read_text().splitlines()]
        assert all(sample.get("owner", initial_owner) == initial_owner for sample in samples), "sampled cell owner changed"
        report["owner_samples_checked"] = len(samples)
        report["sampling_errors"] = sum("sampling_error" in sample or
                                       any("sampling_error" in node for node in sample["nodes"].values()) for sample in samples)
        report["traffic"] = {phase: traffic.summary(phase) for phase in
                             ("warmup", "follower_down", "follower_rejoining", "restored")}
        report["total"] = traffic.summary()
        report["passed"] = True
        event("integrity_verified", acknowledged_rows=preload_rows + len(acknowledged), through_ingress_nodes=3,
              private_standbys=report["standby_integrity"], total=report["total"])
    except Exception as error:
        report["error"] = repr(error)
        raise
    finally:
        try:
            if traffic:
                traffic.close()
                report["total"] = traffic.summary()
                report["traffic"] = {phase: traffic.summary(phase) for phase in
                                     ("warmup", "follower_down", "follower_rejoining", "restored")}
        finally:
            stop_sampling.set()
            if sampler:
                sampler.join(timeout=30)
            report["resource_peaks_by_phase"] = resource_peaks
            report["preload_acknowledged_rows"] = sum(receipt["rows"] for receipt in preload_receipts)
            report["preload_acknowledged_bytes"] = sum(receipt["bytes"] for receipt in preload_receipts)
            report["acknowledged_rows"] = (max(report["preload_acknowledged_rows"], int(report.get("bootstrap_acknowledged", False)))
                                            if preload_rows else 0) + (traffic.summary()["acknowledged"] if traffic else 0)
            report["host_free_bytes_at_finish"] = shutil.disk_usage(state).free
            (state / "report.json").write_text(json.dumps(report, indent=2))
            for node in reversed(nodes):
                node.close()


if __name__ == "__main__":
    main()
