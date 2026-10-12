#!/usr/bin/env python3
# Copyright 2026 Deno Land Inc. Apache-2.0 license.
"""Reuse a completed large-data fault run for checkpointed and busy owner crashes.

Only reuses this suite's own bucket/disks after its processes have stopped.
Requires real hot promotion in the checkpointed case; classifies the busy case.
Verifies all client digests after each crash, then returns the old owner and
checks the current ownership fence. No previous test evidence is deleted.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import socket
import socketserver
import sqlite3
import subprocess
import tempfile
import threading
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
BASE = runpy.run_path(str(ROOT / "scripts/hot-follower-rejoin-smoke.py"))
Node, get, wait = (BASE[key] for key in ("Node", "get", "wait"))
Traffic, http, payload, disk_sample = (BASE[key] for key in ("Traffic", "http", "payload", "disk_sample"))
ROW_BYTES, MIB, GIB = (BASE[key] for key in ("ROW_BYTES", "MIB", "GIB"))
READ_ROWS = 512


def ensemble_joined(log, expected):
    # Wire "active" means may_have_fleet_acks, not a live/recruited shipper.
    # Read-only activation cannot set it before the first measured seed write.
    # A subsequent fragment rotation resets this flag again. Recruitment must
    # never wait for acknowledgement history before allowing new writes.
    return log.get("state") == "open" and set(log.get("ensemble", [])) == set(expected)


def receipt_state(paths, minimum_next=0):
    acknowledged, next_seq = {}, minimum_next
    for path in paths:
        if not path.exists():
            continue
        receipts = [json.loads(path.read_text())] if path.suffix == ".json" else [json.loads(line) for line in path.read_text().splitlines()]
        for receipt in receipts:
            seq = receipt["seq"]
            next_seq = max(next_seq, seq + 1)
            if receipt.get("ok", True):
                digest = receipt["sha256"]
                assert acknowledged.get(seq, digest) == digest, "conflicting acknowledged payloads for one row ID"
                acknowledged[seq] = digest
    return acknowledged, next_seq


def owner_position(root, cell, epoch):
    directory = root / cell / "ltx" / f"e{epoch}" / ".db.sqlite-litestream" / "ltx"
    return max((int(path.stem.split("-")[1], 16) for path in directory.glob("*/*.ltx")), default=0)


def checkpoint_matches_owner(candidate, txid):
    return (txid > 0 and candidate["ready"] and candidate["txid"] >= txid
            and candidate["covered_txid"] >= candidate["txid"])


def require_checkpointed_hot_recovery(case):
    assert case["hot_certifications_before_read"] and case["hot_promotions_before_read"], "checkpointed crash fell back to cold recovery"
    assert case["peer_before_first_read"] == {"tail_requests": 0, "tail_wire_bytes": 0}, "cold peer-tail traffic before the checkpointed recovered read"


def clean_process_exits(cleanup):
    return all(item.get("returncode") == 0 and "error" not in item for item in cleanup)


def peer_header(data):
    """Force one ordinary HTTP request/socket so pooled tails cannot evade counting.

    Preserve upgrade handshakes used by the cell RPC tunnel. This changes only
    test transport connection reuse, not peer payloads or the production code.
    """
    header, body = data.split(b"\r\n\r\n", 1)
    lines = header.split(b"\r\n")
    tail = lines[0].startswith((b"POST /peer/log/tail-page ", b"POST /peer/log/tail "))
    if not any(line.lower().startswith(b"upgrade:") for line in lines[1:]):
        lines = [line for line in lines if not line.lower().startswith(b"connection:")]
        lines.append(b"Connection: close")
    return b"\r\n".join(lines) + b"\r\n\r\n" + body, tail


class PeerProxy(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, target_port):
        self.target_port, self.tail_requests, self.tail_wire_bytes = target_port, 0, 0
        self.lock = threading.Lock()
        super().__init__(("127.0.0.1", 0), ProxyHandler)
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    def counters(self):
        with self.lock:
            return {"tail_requests": self.tail_requests, "tail_wire_bytes": self.tail_wire_bytes}

    def close(self):
        self.shutdown()
        self.server_close()
        self.thread.join(timeout=5)


class ProxyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            header = b""
            while b"\r\n\r\n" not in header:
                block = self.request.recv(65536)
                if not block:
                    return
                header += block
                if len(header.split(b"\r\n\r\n", 1)[0]) > 256 * 1024:
                    raise ValueError("oversized peer header")
            header, tail = peer_header(header)
            if tail:
                with self.server.lock:
                    self.server.tail_requests += 1
            with socket.create_connection(("127.0.0.1", self.server.target_port), timeout=10) as peer:
                peer.settimeout(None)
                peer.sendall(header)

                def upload():
                    try:
                        while block := self.request.recv(65536):
                            peer.sendall(block)
                    except OSError:
                        pass
                    finally:
                        try:
                            peer.shutdown(socket.SHUT_WR)
                        except OSError:
                            pass

                uploader = threading.Thread(target=upload, daemon=True)
                uploader.start()
                while block := peer.recv(65536):
                    if tail:
                        with self.server.lock:
                            self.server.tail_wire_bytes += len(block)
                    self.request.sendall(block)
                try:
                    self.request.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                uploader.join(timeout=1)
        except (OSError, ValueError):
            pass


class MeasuredNode(Node):
    def __init__(self, binary, state, env, bucket, endpoint, name, data_root, boot_timeout, defer_ready=False):
        self.data_root, self.boot_timeout, self.proxy = data_root, boot_timeout, None
        self.defer_ready = defer_ready
        super().__init__(binary, state, env, bucket, endpoint, name, "gpu")

    def start(self):
        self.env["CELLD_WATCH"] = str(self.data_root / self.name)
        if self.proxy is None:
            while True:
                self.proxy = PeerProxy(self.operator_port)
                if self.proxy.server_address[1] not in (self.worker_port, self.operator_port):
                    break
                self.proxy.close()
            self.command[self.command.index("--advertise") + 1] = f"127.0.0.1:{self.proxy.server_address[1]}"
        with self.log.open("ab") as log:
            self.process = subprocess.Popen(self.command, env=self.env, stdout=log, stderr=log)
        if not self.defer_ready:
            self.wait_ready()

    def wait_ready(self):
        def ready():
            if self.process.poll() is not None:
                raise RuntimeError(f"{self.name} exited during startup; inspect {self.log}")
            return get(self.worker + "/.well-known/celld/health")[0] == 200
        try:
            wait(ready, timeout=self.boot_timeout)
        except Exception:
            self.kill()
            self.proxy.close()
            raise
        self.defer_ready = False

    def close(self):
        try:
            super().close()
        finally:
            if self.proxy:
                self.proxy.close()
                self.proxy = None


def start_fleet(nodes, create_node):
    # A restarted owner can need its original witnesses before serving health.
    # Launch every peer listener before waiting for any Worker to become ready.
    for suffix in ("a", "b", "c"):
        nodes.append(create_node("gpu-" + suffix, defer_ready=True))
    for node in nodes:
        node.wait_ready()


def verify_all(node, preload, acknowledged, progress=None, read_timeout=120):
    # Eight original 1 MiB receipts per bounded 8 MiB response. Every original
    # client digest is still checked separately; no whole-database buffer.
    for start in range(0, len(preload), 8):
        group = preload[start:start + 8]
        first, count = group[0]["first"], sum(receipt["rows"] for receipt in group)
        assert count <= READ_ROWS
        request = {"node": node.name, "first": first, "count": count, "read_timeout": read_timeout}
        if progress:
            progress({"event": "verification_read_started", **request})
        try:
            status, body = http(node.worker + f"/read?first={first}&count={count}", timeout=read_timeout)
        except OSError as error:
            if progress:
                progress({"event": "verification_read_failed", "error": str(error), **request})
            raise
        assert status == 200 and len(body) == sum(receipt["bytes"] for receipt in group), (node.name, first, status)
        offset, next_id = 0, first
        for receipt in group:
            assert receipt["first"] == next_id, "preload ledger is not contiguous"
            part = body[offset:offset + receipt["bytes"]]
            assert hashlib.sha256(part).hexdigest() == receipt["sha256"], (node.name, receipt["first"])
            offset += receipt["bytes"]
            next_id += receipt["rows"]
        if progress:
            progress({"event": "verification_read_received", "bytes": len(body), **request})
    BASE["verify_reads"](node, acknowledged, [], read_timeout=read_timeout, progress=progress)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-state", type=Path, required=True)
    parser.add_argument("--previous-state", type=Path, action="append", default=[],
                        help="Retain receipts and reserve attempted row IDs from a completed prior reuse (repeatable)")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/celld")
    parser.add_argument("--binary-change-reason",
                        help="Explicitly permit all nodes to reuse the source with a different binary; records the reason and both hashes")
    parser.add_argument("--verification-only", action="store_true",
                        help="Restart a stopped reused dataset and verify prior receipts without new writes or owner faults; a separate post-restart result")
    parser.add_argument("--endpoint", default="http://127.0.0.1:19000")
    parser.add_argument("--standby-mb", type=int, default=8192)
    parser.add_argument("--paged-min-mib", type=int,
                        help="Explicit paged-restore threshold override for small path-matching tests; omit for production default")
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--warm-seconds", type=float, default=10)
    parser.add_argument("--after-seconds", type=float, default=15)
    parser.add_argument("--rate", type=float, default=20)
    parser.add_argument("--minimum-free-gib", type=float, default=8)
    args = parser.parse_args()
    source = args.source_state.resolve()
    source_report = json.loads((source / "report.json").read_text())
    assert source_report["passed"], "only reuse a completed passing source run"
    assert min(args.timeout, args.warm_seconds, args.after_seconds, args.rate, args.minimum_free_gib) > 0
    assert args.paged_min_mib is None or args.paged_min_mib >= 0
    assert not args.verification_only or args.previous_state, "post-restart verification must name its prior evidence"
    binary = args.binary.resolve()
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    source_digest = source_report["binary_sha256"]
    assert digest == source_digest or (args.binary_change_reason and args.binary_change_reason.strip()), "source and failover binaries must match unless a binary-change reason is recorded"
    bucket = source_report["bucket"]
    assert bucket.startswith("celld-follower-rejoin-"), "refuse a bucket not owned by this test suite"
    commands = subprocess.check_output(["ps", "-axo", "command="], text=True).splitlines()
    assert not any("--bucket " + bucket in command for command in commands), "source fleet is still running"
    minimum_free = int(args.minimum_free_gib * GIB)
    assert shutil.disk_usage(source).free >= minimum_free + source_report["preload_acknowledged_bytes"] * 1.2
    preload = [json.loads(line) for line in (source / "preload-receipts.jsonl").read_text().splitlines()]
    preload_rows = sum(receipt["rows"] for receipt in preload)
    assert preload_rows > 0, "owner-crash suite requires a preloaded source dataset"
    assert preload_rows * ROW_BYTES == source_report["preload_acknowledged_bytes"]
    receipt_paths = [source / "receipts.jsonl"]
    for previous in args.previous_state:
        previous = previous.resolve()
        prior_report = json.loads((previous / "report.json").read_text())
        assert (Path(prior_report["source_state"]).resolve() == source
                and prior_report["bucket"] == bucket
                and (prior_report["binary_sha256"] == digest or args.binary_change_reason)), "prior receipts belong to another dataset/binary"
        receipt_paths.extend([previous / "seed-receipt.json", previous / "extra-seed-receipts.jsonl"])
        receipt_paths.extend(sorted(previous.glob("*/receipts.jsonl")))
    acknowledged, next_seq = receipt_state(receipt_paths, minimum_next=preload_rows)
    temporary_root = Path(os.environ.get("TMPDIR", tempfile.gettempdir()))
    if (temporary_root / "opencode").is_dir():
        temporary_root /= "opencode"
    state = Path(tempfile.mkdtemp(prefix="celld-hot-owner-failover-", dir=temporary_root))
    shutil.copyfile(Path(__file__), state / "executed-harness.py")
    project = state / "project"
    shutil.copytree(source / "project", project)
    worker = project / "worker.js"
    code = worker.read_text()
    assert "count > 64" in code
    worker.write_text(code.replace("count > 64", f"count > {READ_ROWS}"))
    env = {key: value for key, value in os.environ.items() if not key.startswith("CELLD_")}
    env.update(AWS_ACCESS_KEY_ID=os.environ.get("AWS_ACCESS_KEY_ID", "celld-demo"),
               AWS_SECRET_ACCESS_KEY=os.environ.get("AWS_SECRET_ACCESS_KEY", "celld-demo-secret"),
               AWS_REGION="us-east-1", AWS_DEFAULT_REGION="us-east-1", S3_ENDPOINT=args.endpoint,
               CELLD_DURABILITY="fleet", CELLD_HOT_RECOVERY="1", CELLD_MONITOR="1",
               CELLD_TTL_MS="4000", CELLD_LOG_FLUSH_MS="500", CELLD_REBALANCE_INTERVAL_MS="0",
                CELLD_HOT_STANDBY_MB=str(args.standby_mb), CELLD_TOKIO_THREADS="2", NO_COLOR="1")
    if args.paged_min_mib is not None:
        env["CELLD_LTX_PAGED_MIN_MB"] = str(args.paged_min_mib)
    s3 = boto3.client("s3", endpoint_url=args.endpoint, region_name="us-east-1",
                      aws_access_key_id=env["AWS_ACCESS_KEY_ID"], aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
                      aws_session_token=env.get("AWS_SESSION_TOKEN"),
                      config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    subprocess.run([str(binary), "deploy", str(project), "--bucket", bucket,
                    "--endpoint", args.endpoint, "--region", "us-east-1"], env=env, check=True)
    print(f"ARTIFACTS: {state}\nREUSED BUCKET: {bucket}", flush=True)
    report = {"passed": False, "source_state": str(source), "bucket": bucket, "binary_sha256": digest,
              "source_binary_sha256": source_digest, "binary_change_reason": args.binary_change_reason,
              "previous_states": [str(path.resolve()) for path in args.previous_state],
              "source_report_sha256": hashlib.sha256((source / "report.json").read_bytes()).hexdigest(),
              "harness_sha256": hashlib.sha256((state / "executed-harness.py").read_bytes()).hexdigest(),
              "base_harness_sha256": hashlib.sha256((ROOT / "scripts/hot-follower-rejoin-smoke.py").read_bytes()).hexdigest(),
              "preload_rows": preload_rows, "preload_bytes": preload_rows * ROW_BYTES,
              "output": str(state), "events": [], "cases": [], "read_batch_rows": READ_ROWS,
              "standby_budget_mib": args.standby_mb,
              "paged_min_mib_override": args.paged_min_mib,
              "mode": "post_restart_verification" if args.verification_only else "owner_faults",
              "verification_read_timeout_seconds": 120,
              "peer_transport": "counted one-request HTTP connections; RPC upgrades preserved"}
    nodes, current_traffic, sampler = [], None, None
    stop_sampling = threading.Event()
    peaks, phase = {}, "startup"
    owner_key = None

    def event(name, **fields):
        item = {"event": name, "at": time.time(), **fields}
        report["events"].append(item)
        print(json.dumps(item), flush=True)
        with (state / "events.jsonl").open("a") as output:
            output.write(json.dumps(item) + "\n")

    def object_json(key):
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())

    def owner():
        return object_json(owner_key)

    def standbys(prior):
        result = {}
        for node in nodes:
            if node.name == prior["node"]:
                continue
            status, page = get(node.operator + "/hot-recovery", timeout=5)
            assert status == 200
            matching = [item for item in page["candidates"] if item["cell"] == scope and item["cell_epoch"] == prior["epoch"]]
            if len(matching) != 1 or not matching[0]["ready"] or page["queued_bytes"]:
                return None
            assert page["bytes"] >= preload_rows * ROW_BYTES
            result[node.name] = page
        return result

    def ensemble_ready(prior):
        log = object_json(f"nodes/{prior['node']}.json").get("log") or {}
        return ensemble_joined(log, {node.name for node in nodes if node.name != prior["node"]})

    def settle_checkpoints(prior, stage):
        # Local advisory readiness can lag the owner when writes acknowledged
        # through bucket fallback were never appended to this follower. Compare
        # with the owner's real captured LTX position, then issue sparse new
        # commits to trigger recruitment/seeding after a fragment repair.
        nonlocal next_seq
        last_write = 0
        primary = next(node for node in nodes if node.name == prior["node"])
        with (stage / "receipts.jsonl").open("a", buffering=1) as ledger:
            def settled():
                nonlocal last_write, next_seq
                assert shutil.disk_usage(source).free >= minimum_free, "host disk reserve reached during checkpoint catch-up"
                target = owner_position(source / primary.name, scope, prior["epoch"])
                pages = standbys(prior)
                if pages and ensemble_ready(prior) and all(checkpoint_matches_owner(page["candidates"][0], target) for page in pages.values()):
                    return {"owner_txid": target, "standbys": pages}
                if time.monotonic() - last_write >= 1:
                    seq = next_seq
                    next_seq += 1
                    seed = payload(seq)
                    receipt = {"seq": seq, "sha256": hashlib.sha256(seed).hexdigest(), "ok": False,
                               "phase": "checkpoint_catchup", "sent_at": time.time()}
                    try:
                        status, body = http(primary.worker + f"/import?first={seq}", seed)
                        receipt["status"] = status
                        assert status == 200 and json.loads(body) == {"first": seq, "rows": 1, "bytes": ROW_BYTES}
                        receipt["ok"] = True
                        acknowledged[seq] = receipt["sha256"]
                    except (OSError, ValueError, AssertionError) as error:
                        receipt["error"] = str(error)
                    receipt["finished_at"] = time.time()
                    ledger.write(json.dumps(receipt) + "\n")
                    os.fsync(ledger.fileno())
                    last_write = time.monotonic()
                return None
            result = wait(settled, timeout=args.timeout)
        # A bounded read of the latest client-confirmed row also distinguishes
        # a stale ready image from one that actually contains the quiet tail.
        seq = max(acknowledged)
        for node in nodes:
            if node is primary:
                continue
            images = list((source / node.name / "hot-standby").glob("*.sqlite"))
            assert len(images) == 1
            with sqlite3.connect(f"file:{images[0]}?mode=ro&immutable=1", uri=True) as database:
                row = database.execute("SELECT payload FROM records WHERE id = ?", (seq,)).fetchone()
                assert row and hashlib.sha256(row[0]).hexdigest() == acknowledged[seq], "ready standby lacks the latest acknowledged row"
        return result

    def counters():
        counts = [node.proxy.counters() for node in nodes]
        return {key: sum(count[key] for count in counts) for key in ("tail_requests", "tail_wire_bytes")}

    def sample():
        with (state / "samples.jsonl").open("w", buffering=1) as output:
            while not stop_sampling.is_set():
                collection_started = time.monotonic()
                sample = {"at": time.time(), "phase": phase, "nodes": {}, "host_free_bytes": shutil.disk_usage(source).free}
                if sample["host_free_bytes"] < minimum_free:
                    report["disk_reserve_violation"] = sample["host_free_bytes"]
                    if current_traffic:
                        current_traffic.stop.set()
                try:
                    if owner_key:
                        read_started = time.monotonic()
                        try:
                            sample["owner"] = owner()
                        finally:
                            sample["owner_read_ms"] = round((time.monotonic() - read_started) * 1000, 3)
                    sample["peer"] = counters()
                    for node in nodes:
                        process = node.process
                        details = {"pid": process.pid, "running": process.poll() is None, "rss_bytes": 0,
                                   **disk_sample(source / node.name)}
                        if details["running"]:
                            rss = subprocess.run(["ps", "-o", "rss=", "-p", str(process.pid)],
                                                 capture_output=True, text=True, timeout=5)
                            details["rss_bytes"] = int(rss.stdout.strip() or 0) * 1024
                        sample["nodes"][node.name] = details
                        peak = peaks.setdefault(phase, {}).setdefault(node.name, {})
                        for key in ("rss_bytes", "disk_bytes", "hot_standby_bytes", "seed_bytes"):
                            peak[key] = max(peak.get(key, 0), details[key])
                except Exception as error:
                    sample["sampling_error"] = str(error)
                sample["collection_elapsed_ms"] = round((time.monotonic() - collection_started) * 1000, 3)
                output.write(json.dumps(sample) + "\n")
                stop_sampling.wait(1)

    def merge_traffic(traffic):
        nonlocal next_seq
        traffic.close()
        for receipt in traffic.receipts:
            if receipt["ok"]:
                acknowledged[receipt["seq"]] = receipt["sha256"]
        next_seq = traffic.next_seq
        with (traffic.state / "receipts.jsonl").open("ab") as ledger:
            os.fsync(ledger.fileno())

    def verify_node(node):
        with (state / "verification-reads.jsonl").open("a", buffering=1) as ledger:
            def progress(item):
                ledger.write(json.dumps({"at": time.time(), "phase": phase, **item}) + "\n")
            verify_all(node, preload, acknowledged, progress=progress)

    def recover_read(ingress, seq, expected, case, offsets, baseline):
        deadline = time.monotonic() + args.timeout
        attempts = []
        while time.monotonic() < deadline:
            try:
                status, body = http(ingress.worker + f"/read?first={seq}&count=1", timeout=min(deadline - time.monotonic(), 180))
                if status == 200:
                    assert len(body) == ROW_BYTES and hashlib.sha256(body).hexdigest() == expected, "latest acknowledged row lost/corrupt"
                    counts = counters()
                    case["peer_before_first_read"] = {key: counts[key] - baseline[key] for key in baseline}
                    logs = "\n".join(node.log.read_bytes()[offsets[node.name]:].decode(errors="replace") for node in nodes)
                    case["hot_certifications_before_read"] = logs.count('event="hot_recovery_certified"')
                    case["hot_promotions_before_read"] = logs.count('event="hot_recovery_promoted"')
                    case["recovery_path"] = "hot" if case["hot_promotions_before_read"] else "cold"
                    case["read_attempts"] = attempts
                    return owner()
                attempts.append({"at": time.time(), "status": status, "error": body[:256].decode(errors="replace")})
            except (OSError, TimeoutError) as error:
                attempts.append({"at": time.time(), "error": str(error)})
            time.sleep(.2)
        raise AssertionError(f"first recovered read timed out: {attempts[-3:]}")

    try:
        def create_node(name, **options):
            assert (source / name / "node-id").read_text().strip() == name
            return MeasuredNode(binary, state, env, bucket, args.endpoint, name, source, args.timeout, **options)
        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        start_fleet(nodes, create_node)
        identity = get(nodes[0].worker + "/identity")[1]["id"]
        scope = "ImportStore:" + identity
        owner_key = f"cells/{scope}/own.json"
        # Activates the persisted database; this is setup, not measured failover.
        status, body = http(nodes[0].worker + "/read?first=0&count=64", timeout=180)
        assert status == 200 and hashlib.sha256(body).hexdigest() == preload[0]["sha256"]
        initial = owner()
        wait(lambda: ensemble_ready(initial), timeout=args.timeout)
        for node in nodes:
            lease = object_json(f"nodes/{node.name}.json")
            assert lease["addr"] == f"127.0.0.1:{node.proxy.server_address[1]}", "peer traffic bypasses its measured proxy"
            assert lease["placement_pool"] == "gpu" and lease["hot_recovery"]
        if args.verification_only:
            report["owner_before_verification"] = initial
            for node in nodes:
                phase = "post_restart_verification"
                verify_node(node)
                assert owner() == initial, "ownership moved during post-restart read-only verification"
                event("post_restart_all_digests_verified", node=node.name, rows=preload_rows + len(acknowledged))
            status, integrity = get(nodes[0].worker + "/integrity", timeout=180)
            assert status == 200 and integrity == [{"quick_check": "ok"}]
            assert "disk_reserve_violation" not in report, "host disk reserve reached"
            report["passed"] = True
            report["all_nodes_all_digests_verified"] = True
            event("post_restart_verification_passed", owner=initial, acknowledged_rows=preload_rows + len(acknowledged))
        else:
            # A fresh process has no hot certificate. A real new commit recruits
            # images from the canonical 4 GiB chain before either measured crash.
            seed = payload(next_seq)
            status, body = http(nodes[0].worker + f"/import?first={next_seq}", seed, timeout=180)
            assert status == 200 and json.loads(body) == {"first": next_seq, "rows": 1, "bytes": ROW_BYTES}
            acknowledged[next_seq] = hashlib.sha256(seed).hexdigest()
            (state / "seed-receipt.json").write_text(json.dumps({"seq": next_seq, "sha256": acknowledged[next_seq]}))
            next_seq += 1
            phase = "initial_seed"
            warm = wait(lambda: standbys(initial), timeout=args.timeout)
            event("full_size_standbys_ready", owner=initial, standbys=warm)

        for kind in (() if args.verification_only else ("checkpointed", "continuous")):
            prior = owner()
            primary = next(node for node in nodes if node.name == prior["node"])
            ingress = next(node for node in nodes if node is not primary)
            wait(lambda: ensemble_ready(prior) and standbys(prior), timeout=args.timeout)
            stage = state / kind
            stage.mkdir()
            current_traffic = Traffic(ingress.worker, stage, 4, args.rate)
            current_traffic.next_seq = next_seq
            current_traffic.set_phase(kind + "_warmup")
            phase = kind + "_warmup"
            current_traffic.start()
            stop_sampling.wait(args.warm_seconds)
            if kind == "checkpointed":
                merge_traffic(current_traffic)
                catchup = state / "checkpointed-catchup"
                catchup.mkdir()
                phase = "checkpointed_catchup"
                checkpoint_proof = settle_checkpoints(prior, catchup)
                event("checkpointed_owner_position_matched", owner=prior, **checkpoint_proof)
            # In the busy case capture a client-confirmed row before the kill,
            # without imposing a checkpoint/quiet-writer barrier.
            with current_traffic.lock:
                latest = [receipt for receipt in current_traffic.receipts if receipt["ok"]]
            assert latest, "warmup produced no acknowledged writes"
            seq = max(acknowledged) if kind == "checkpointed" else max(receipt["seq"] for receipt in latest)
            expected = acknowledged[seq] if kind == "checkpointed" else next(receipt["sha256"] for receipt in latest if receipt["seq"] == seq)
            case = {"kind": kind, "prior_owner": prior, "crashed_pid": primary.process.pid,
                    "warmup_traffic": current_traffic.summary(),
                    "owner_txid_before_crash": owner_position(source / primary.name, scope, prior["epoch"]),
                    "prior_lease": object_json(f"nodes/{primary.name}.json"),
                    "standbys_before_crash": {node.name: get(node.operator + "/hot-recovery", timeout=5)[1]
                                               for node in nodes if node is not primary}}
            report["cases"].append(case)
            offsets = {node.name: node.log.stat().st_size for node in nodes}
            baseline = counters()
            event("owner_kill", kind=kind, prior_owner=prior, pid=primary.process.pid, latest_acknowledged_seq=seq)
            current_traffic.set_phase(kind + "_fault")
            phase = kind + "_fault"
            primary.kill()
            killed = time.monotonic()
            successor = recover_read(ingress, seq, expected, case, offsets, baseline)
            case["crash_to_read_seconds"] = round(time.monotonic() - killed, 3)
            assert successor["node"] != prior["node"] and successor["epoch"] > prior["epoch"]
            assert successor["placement_pool"] == "gpu"
            case["successor_owner"] = successor
            event("first_recovered_read", kind=kind, prior_owner=prior, successor_owner=successor,
                  crash_to_read_seconds=case["crash_to_read_seconds"], recovery_path=case["recovery_path"],
                  peer_before_first_read=case["peer_before_first_read"],
                  hot_certifications=case["hot_certifications_before_read"],
                  hot_promotions=case["hot_promotions_before_read"], failed_read_attempts=len(case["read_attempts"]))
            if kind == "checkpointed":
                require_checkpointed_hot_recovery(case)
                # Resume new writes after proving the hot promotion.
                stage = state / "checkpointed-resumed"
                stage.mkdir()
                current_traffic = Traffic(ingress.worker, stage, 4, args.rate)
                current_traffic.next_seq = next_seq
                current_traffic.set_phase("checkpointed_resumed")
                current_traffic.start()
            phase = kind + "_resumed"
            stop_sampling.wait(args.after_seconds)
            merge_traffic(current_traffic)
            case["traffic"] = current_traffic.summary()
            case["traffic_by_phase"] = {name: current_traffic.summary(name) for name in
                                        (kind + "_warmup", kind + "_fault", "checkpointed_resumed")
                                        if any(receipt["phase"] == name for receipt in current_traffic.receipts)}
            case["known_acknowledged_rows"] = preload_rows + len(acknowledged)
            phase = kind + "_verification"
            for node in nodes:
                if node is not primary:
                    verify_node(node)
                    event("survivor_all_digests_verified", kind=kind, node=node.name, rows=case["known_acknowledged_rows"])
            phase = kind + "_old_owner_return"
            primary.start()
            assert (source / primary.name / "node-id").read_text().strip() == primary.name
            verify_node(primary)
            assert owner() == successor, "returning old owner changed the current cell ownership fence"
            status, integrity = get(ingress.worker + "/integrity", timeout=180)
            assert status == 200 and integrity == [{"quick_check": "ok"}]
            case["old_owner_fenced"] = True
            case["all_nodes_all_digests_verified"] = True
            event("old_owner_return_verified", kind=kind, owner=owner(), rows=case["known_acknowledged_rows"])
            # Build fresh same-epoch images before the next fault. A promoted
            # cache is not silently treated as its successor's standby.
            wait(lambda: ensemble_ready(successor), timeout=args.timeout)
            seed = payload(next_seq)
            status, body = http(ingress.worker + f"/import?first={next_seq}", seed, timeout=180)
            assert status == 200 and json.loads(body) == {"first": next_seq, "rows": 1, "bytes": ROW_BYTES}
            acknowledged[next_seq] = hashlib.sha256(seed).hexdigest()
            with (state / "extra-seed-receipts.jsonl").open("a") as ledger:
                ledger.write(json.dumps({"seq": next_seq, "sha256": acknowledged[next_seq]}) + "\n")
                ledger.flush()
                os.fsync(ledger.fileno())
            next_seq += 1
            phase = kind + "_standby_rebuild"
            wait(lambda: standbys(successor), timeout=args.timeout)
            for node in nodes:
                BASE["verify_reads"](node, {next_seq - 1: acknowledged[next_seq - 1]}, [])
            assert "disk_reserve_violation" not in report, "host disk reserve reached"
        if not args.verification_only:
            report["passed"] = True
            event("owner_failover_qualification_passed", acknowledged_rows=preload_rows + len(acknowledged),
                  cases=[{key: case[key] for key in ("kind", "crash_to_read_seconds", "recovery_path", "peer_before_first_read", "old_owner_fenced")}
                         for case in report["cases"]])
    except Exception as error:
        report["error"] = repr(error)
        raise
    finally:
        try:
            if current_traffic:
                current_traffic.close()
        finally:
            stop_sampling.set()
            if sampler:
                sampler.join(timeout=30)
            samples = [json.loads(line) for line in (state / "samples.jsonl").read_text().splitlines()] if sampler else []
            by_epoch = {}
            for sample in samples:
                current = sample.get("owner")
                if current and current.get("node"):
                    by_epoch.setdefault(str(current["epoch"]), set()).add(current["node"])
            report["sampled_owners_by_epoch"] = {epoch: sorted(owners) for epoch, owners in by_epoch.items()}
            if any(len(owners) > 1 for owners in by_epoch.values()):
                report["passed"] = False
                report["error"] = "conflicting sampled owners in one cell epoch"
            report["resource_peaks_by_phase"] = peaks
            report["acknowledged_rows"] = preload_rows + len(acknowledged)
            report["host_free_bytes_at_finish"] = shutil.disk_usage(source).free
            cleanup = []
            for node in reversed(nodes):
                item = {"node": node.name, "pid": node.process.pid}
                try:
                    node.close()
                except Exception as error:
                    item["error"] = str(error)
                    report["passed"] = False
                    report["cleanup_error"] = "test process cleanup failed; inspect retained logs"
                item["returncode"] = node.process.returncode
                cleanup.append(item)
            report["process_cleanup"] = cleanup
            if not clean_process_exits(cleanup):
                report["passed"] = False
                report["cleanup_error"] = "test process exited unsuccessfully or cleanup failed; inspect retained logs"
            (state / "report.json").write_text(json.dumps(report, indent=2))
    assert report["passed"], report.get("error", report.get("cleanup_error", "owner failover qualification failed"))


if __name__ == "__main__":
    main()
