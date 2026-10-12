#!/usr/bin/env python3
"""Bulk import / crash / recover using three production celld processes."""

import argparse
from contextlib import closing
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from peer_proxy import PeerProxy

ROW_BYTES = 16384
BATCH_ROWS = 64
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent


def request(port, path, body=None, timeout=30):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body)
    if body is not None:
        req.add_header("Content-Type", "application/octet-stream")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        error.msg = f"{error.msg}: {error.read()[:1024]!r}"
        raise


def wait_for(description, check, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, ValueError, sqlite3.Error) as error:
            last = str(error)
        time.sleep(0.25)
    raise RuntimeError(f"timed out waiting for {description}: {last}")


def ready(port):
    return json.loads(request(port, "/.well-known/celld/health", timeout=2)).get("ok") is True


def disk_bytes(directory, pattern="*"):
    total = 0
    for path in directory.rglob(pattern):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            pass  # Fragment GC legitimately removes files during sampling.
    return total


class Fleet:
    def __init__(self, args):
        self.args = args
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.project = self.output / "project"
        self.project.mkdir()
        for name in ("worker.js", "wrangler.json"):
            shutil.copyfile(HERE / name, self.project / name)
        self.database = self.project / ".celld/dev/objects.sqlite3"
        self.nodes = []
        self.ids = []
        self.stop_sampling = threading.Event()
        self.peaks = {}
        self.recovery_peaks = {}
        self.phase = "import"
        self.sampler = None
        self.proxies = []
        # Use only this run's storage topology; do not inherit fleet settings.
        self.environment = {k: v for k, v in os.environ.items() if not k.startswith("CELLD_")}
        self.environment.update(CELLD_TOKIO_THREADS="4", RUST_LOG="info", NO_COLOR="1")
        self.report = {"mib": args.mib, "scenario": args.scenario, "output": str(self.output),
                       "tail_kib_per_second": args.tail_kib_per_second, "hot_recovery": args.hot_recovery}
        binary_hash = hashlib.sha256()
        with self.args.binary.open("rb") as binary:
            for chunk in iter(lambda: binary.read(1024 * 1024), b""):
                binary_hash.update(chunk)
        self.report["binary_sha256"] = binary_hash.hexdigest()

    def event(self, name, **fields):
        entry = {"event": name, "at": time.time(), **fields}
        print(json.dumps(entry), flush=True)
        with (self.output / "events.jsonl").open("a") as stream:
            stream.write(json.dumps(entry) + "\n")

    def leases(self):
        with closing(sqlite3.connect(self.database)) as db:
            return [json.loads(row[0]) for row in db.execute("SELECT body FROM objects WHERE key LIKE 'nodes/%'")]

    def seed(self):
        with (self.output / "seed.log").open("wb") as log:
            process = subprocess.Popen([
                str(self.args.binary), "dev", str(self.project), "--port", str(self.args.port + 9),
                "--no-watch", "--logs",
            ], env={**self.environment, "CELLD_TTL_MS": "1000"}, stdout=log, stderr=log, start_new_session=True)
            try:
                wait_for("deployment seed", lambda: ready(self.args.port + 9))
            finally:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=50)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
        if process.returncode:
            raise RuntimeError(f"deployment seeding failed; see {self.output / 'seed.log'}")
        # The deployment-only node is gone and must not enter an ensemble.
        time.sleep(2)

    def start(self, index):
        directory = self.output / f"node-{index}"
        directory.mkdir(exist_ok=True)
        env = {**self.environment, "CELLD_INTERNAL_DEV_STORE": str(self.database),
               "CELLD_WATCH": str(directory), "CELLD_DURABILITY": "fleet",
               "CELLD_LOG_FLUSH_MS": "3600000", "CELLD_REBALANCE_INTERVAL_MS": "0",
               "CELLD_TTL_MS": "4000"}
        if self.args.hot_recovery:
            env["CELLD_HOT_RECOVERY"] = "1"
        log = (self.output / f"node-{index}.log").open("ab")
        process = subprocess.Popen([
            str(self.args.binary), "--no-control-plane", "--bucket", "celld-dev",
            "--listen", f"127.0.0.1:{self.args.port + index}",
            "--internal-listen", f"127.0.0.1:{self.args.port + 20 + index}",
            "--advertise", f"127.0.0.1:{self.args.port + 30 + index}",
        ], env=env, stdout=log, stderr=log)
        log.close()
        if index == len(self.nodes):
            self.nodes.append(process)
        else:
            self.nodes[index] = process
        self.event("node-start", index=index, pid=process.pid)

    def stop(self, index):
        process = self.nodes[index]
        if process.poll() is None:
            process.kill()
            process.wait(timeout=15)
        self.event("node-kill", index=index)

    def owner(self):
        with closing(sqlite3.connect(self.database)) as db:
            owner = db.execute("SELECT body FROM objects WHERE key LIKE 'cells/ImportStore:%/own.json'").fetchone()
        if owner is None:
            return None
        node = json.loads(owner[0])["node"]
        for lease in self.leases():
            log = lease.get("log") or {}
            members = log.get("ensemble", [])
            if lease["node"] == node and log.get("state") == "open" and len(members) == 2:
                if lease["node"] in self.ids and all(member in self.ids for member in members):
                    return self.ids.index(lease["node"])
        return None

    def retirement(self, index):
        result = subprocess.run([
            str(self.args.binary), "disk", "check", str(self.output / f"node-{index}"), "--bucket", "celld-dev",
        ], env={**self.environment, "CELLD_INTERNAL_DEV_STORE": str(self.database), "RUST_LOG": "error"},
            capture_output=True, text=True, timeout=30)
        lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
        if not lines:
            raise RuntimeError(f"disk retirement check produced no result: {result.stderr}")
        report = json.loads(lines[-1])
        if (result.returncode == 0) != report["safe"]:
            raise AssertionError("disk retirement verdict and exit code disagree")
        return report

    def activate_witness_sessions(self):
        ledger = []
        for index in range(3):
            def complete_ensemble(index=index):
                lease = next(lease for lease in self.leases() if lease["node"] == self.ids[index])
                log = lease.get("log") or {}
                return log.get("state") == "open" and set(log.get("ensemble", [])) == set(self.ids) - {self.ids[index]}
            wait_for(f"node {index}'s two-follower ensemble", complete_ensemble)
        with (self.output / "witness-acks.jsonl").open("a") as stream:
            for index in range(3):
                for attempt in range(16):
                    lease = next(lease for lease in self.leases() if lease["node"] == self.ids[index])
                    if (lease.get("log") or {}).get("active"):
                        break
                    name = f"witness-{index}-{attempt}"
                    payload = hashlib.shake_256(name.encode()).digest(ROW_BYTES * BATCH_ROWS)
                    response = json.loads(request(self.args.port + index, f"/import?name={name}&first=0", payload))
                    if response != {"first": 0, "rows": BATCH_ROWS, "bytes": len(payload)}:
                        raise AssertionError(f"bad witness-session acknowledgement: {response}")
                    ack = {**response, "name": name, "sha256": hashlib.sha256(payload).hexdigest()}
                    ledger.append(ack)
                    stream.write(json.dumps(ack) + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                else:
                    raise AssertionError(f"could not exercise node {index}'s own log session")
        # Every boot now requires a real witness. Empty inactive sessions can
        # finish without tail pages, hiding a startup-listener dependency cycle.
        for lease in self.leases():
            if lease["node"] not in self.ids:
                continue
            log = lease.get("log") or {}
            if not log.get("active") or log.get("state") != "open" or set(log.get("ensemble", [])) != set(self.ids) - {lease["node"]}:
                raise AssertionError(f"witness-session preconditions changed before the fault: {lease}")
        self.event("all-sessions-active", auxiliary_batches=len(ledger))
        self.report["auxiliary_acknowledged_bytes"] = sum(ack["bytes"] for ack in ledger)
        return ledger

    def sample(self):
        with (self.output / "resources.jsonl").open("a") as stream:
            while not self.stop_sampling.is_set():
                sample = {"at": time.time(), "phase": self.phase, "nodes": []}
                for index, process in enumerate(self.nodes[:]):
                    rss = 0
                    if process.poll() is None:
                        result = subprocess.run(["ps", "-o", "rss=", "-p", str(process.pid)], capture_output=True, text=True)
                        rss = int(result.stdout.strip() or 0) * 1024
                    self.peaks[str(index)] = max(rss, self.peaks.get(str(index), 0))
                    if self.phase == "recovery":
                        self.recovery_peaks[str(index)] = max(rss, self.recovery_peaks.get(str(index), 0))
                    directory = self.output / f"node-{index}"
                    sample["nodes"].append({"index": index, "pid": process.pid, "rss_bytes": rss,
                                             "disk_bytes": disk_bytes(directory),
                                             "peerlog_bytes": disk_bytes(directory / "peerlog")})
                stream.write(json.dumps(sample) + "\n")
                stream.flush()
                self.stop_sampling.wait(1)

    def close(self):
        self.stop_sampling.set()
        if self.sampler:
            self.sampler.join(timeout=10)
        for index in range(len(self.nodes)):
            self.stop(index)
        for proxy in self.proxies:
            proxy.close()
        self.report["tail_proxy"] = [{"connections": proxy.tail_connections,
                                      "wire_bytes": proxy.tail_wire_bytes} for proxy in self.proxies]
        self.report["peak_rss_bytes"] = self.peaks
        self.report["recovery_peak_rss_bytes"] = self.recovery_peaks
        (self.output / "report.json").write_text(json.dumps(self.report, indent=2) + "\n")

    def run(self):
        self.seed()
        for index in range(3):
            self.proxies.append(PeerProxy(self.args.port + 30 + index, self.args.port + 20 + index,
                                          self.args.tail_kib_per_second))
        for index in range(3):
            self.start(index)
            wait_for(f"node {index} health", lambda index=index: ready(self.args.port + index))
            self.ids.append((self.output / f"node-{index}/node-id").read_text().strip())
        self.sampler = threading.Thread(target=self.sample, daemon=True)
        self.sampler.start()
        request(self.args.port, "/stats")
        owner = wait_for("two-follower ensemble", lambda: None if self.owner() is None else self.owner() + 1) - 1
        followers = [index for index in range(3) if index != owner]
        lease = next(lease for lease in self.leases() if lease["node"] == self.ids[owner])
        generation = lease.get("ownership_index_generation") or lease["probe_public_key"]
        session = f"{self.ids[owner]}/{generation}"
        self.event("ensemble-ready", owner=owner, followers=followers, session=session)
        started = time.monotonic()
        ledger = []
        batches = math.ceil(self.args.mib * 1024 * 1024 / (ROW_BYTES * BATCH_ROWS))
        with (self.output / "acks.jsonl").open("a") as ack_file:
            for batch in range(batches):
                # Incompressible reproducible payloads; the client is the oracle.
                payload = hashlib.shake_256(f"celld-import:{batch}".encode()).digest(ROW_BYTES * BATCH_ROWS)
                response = json.loads(request(self.args.port + owner, f"/import?first={batch * BATCH_ROWS}", payload))
                expected = {"first": batch * BATCH_ROWS, "rows": BATCH_ROWS, "bytes": len(payload)}
                if response != expected:
                    raise AssertionError(f"unexpected import acknowledgement: {response}")
                ack = {**expected, "sha256": hashlib.sha256(payload).hexdigest()}
                ledger.append(ack)
                ack_file.write(json.dumps(ack) + "\n")
                ack_file.flush()
                os.fsync(ack_file.fileno())
                if (batch + 1) % 50 == 0 or batch + 1 == batches:
                    self.event("import-progress", acknowledged_mib=(batch + 1), seconds=time.monotonic() - started)
        self.report["import_seconds"] = time.monotonic() - started
        self.report["acknowledged_bytes"] = sum(ack["bytes"] for ack in ledger)
        tails = {str(index): disk_bytes(self.output / f"node-{index}/peerlog" / session, "*.batch") for index in followers}
        self.report["retained_tail_bytes"] = tails
        self.event("tail-size", **tails)
        if min(tails.values()) < self.report["acknowledged_bytes"] * 0.9:
            raise AssertionError("import did not produce the required retained follower tail")
        witness_ledger = self.activate_witness_sessions() if self.args.scenario == "missing-witness" else []
        if self.args.hot_recovery:
            for index in followers:
                def warm(index=index):
                    result = json.loads(request(self.args.port + 20 + index, "/hot-recovery"))
                    return result if any(item["session"] == session and item["ready"] for item in result["candidates"]) else None
                status = wait_for(f"node {index}'s materialized and checkpointed standby", warm, timeout=120)
                self.event("hot-standby-ready", node=index, status=status)
        (self.output / "before-crash-leases.json").write_text(json.dumps(self.leases(), indent=2))
        self.phase = "recovery"
        # Lose the owner cache: this forces recovery from the actual peer disks.
        if self.args.scenario == "missing-witness":
            for index in followers:
                self.stop(index)
        self.stop(owner)
        for child in (self.output / f"node-{owner}").iterdir():
            if child.name != "node-id":
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
        crash = time.monotonic()
        if self.args.scenario == "missing-witness":
            self.start(owner)
            # An old witness lease is now expired; that must not authorize loss.
            # Exceed the old minimum 20s "long-dead" grace, not just expiry.
            time.sleep(30)
            if self.nodes[owner].poll() is not None:
                raise AssertionError(f"owner exited instead of keeping predecessor recovery pending: status {self.nodes[owner].returncode}")
            with closing(sqlite3.connect(self.database)) as db:
                loss_keys = [row[0] for row in db.execute("SELECT key FROM objects WHERE key LIKE 'log/%.loss.json'")]
            if loss_keys:
                raise AssertionError(f"missing witnesses were declared lost: {loss_keys}")
            prior = next(lease for lease in self.leases() if lease["node"] == self.ids[owner])
            if prior.get("ownership_index_generation") != generation or (prior.get("log") or {}).get("state") == "sealed":
                raise AssertionError("startup replaced or sealed a session without recovering its witnesses")
            try:
                request(self.args.port + owner, "/stats", timeout=2)
            except (OSError, urllib.error.URLError):
                pass
            else:
                raise AssertionError("a cell was served without a complete recovery witness")
            self.report["witness_blackout_seconds"] = time.monotonic() - crash
            self.event("missing-witness-blocked", seconds=self.report["witness_blackout_seconds"])
            for index in followers:
                self.start(index)
            survivor = owner
        else:
            survivor = followers[0]
        expected_rows = batches * BATCH_ROWS

        def recovered():
            if self.nodes[survivor].poll() is not None:
                raise AssertionError(f"recovery process exited: node {survivor}, status {self.nodes[survivor].returncode}")
            result = json.loads(request(self.args.port + survivor, "/stats", timeout=3))
            if result.get("rows") != expected_rows:
                raise AssertionError(f"acknowledged records disappeared: {result}")
            return result

        stats = wait_for("complete imported database", recovered, timeout=self.args.recovery_timeout)
        self.phase = "verification"
        self.report["recovery_seconds"] = time.monotonic() - crash
        self.event("recovered", seconds=self.report["recovery_seconds"], stats=stats)
        if self.args.hot_recovery:
            self.report["hot_promotions_before_read"] = sum(
                log.read_text().count("hot_recovery_promoted") for log in self.output.glob("node-*.log")
            )
            if self.args.scenario == "missing-witness" and self.report["hot_promotions_before_read"]:
                raise AssertionError("a restarted process reused a stale standby promotion certificate")
        if self.args.hot_recovery and self.args.scenario == "owner-kill":
            self.report["promotion_tail_wire_bytes"] = sum(proxy.tail_wire_bytes for proxy in self.proxies)
            if self.report["recovery_seconds"] > 30:
                raise AssertionError(f"hot recovery exceeded its 30-second gate: {self.report['recovery_seconds']}")
            if not self.report["hot_promotions_before_read"]:
                raise AssertionError("hot recovery fell back to a cold restore")
            if self.report["promotion_tail_wire_bytes"]:
                raise AssertionError("hot promotion transferred a cold follower tail")
        # Resume the import at the successor to catch application sequence reuse.
        payload = hashlib.shake_256(b"celld-import:post-crash").digest(ROW_BYTES * BATCH_ROWS)
        response = json.loads(request(self.args.port + survivor, f"/import?first={expected_rows}", payload))
        if response != {"first": expected_rows, "rows": BATCH_ROWS, "bytes": len(payload)}:
            raise AssertionError(f"bad successor acknowledgement: {response}")
        ack = {**response, "sha256": hashlib.sha256(payload).hexdigest()}
        ledger.append(ack)
        with (self.output / "acks.jsonl").open("a") as stream:
            stream.write(json.dumps(ack) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        if self.args.scenario == "owner-kill":
            self.start(owner)
        for index in range(3):
            wait_for(f"restarted node {index}", lambda index=index: ready(self.args.port + index), timeout=self.args.recovery_timeout)
            if (self.output / f"node-{index}/node-id").read_text().strip() != self.ids[index]:
                raise AssertionError("node identity changed across a restart")
            for ack in ledger:
                actual = request(self.args.port + index, f"/read?first={ack['first']}&count={ack['rows']}", timeout=60)
                if len(actual) != ack["bytes"] or hashlib.sha256(actual).hexdigest() != ack["sha256"]:
                    raise AssertionError(f"node {index}: payload corruption in acknowledged range {ack['first']}")
            integrity = json.loads(request(self.args.port + index, "/integrity", timeout=60))
            if integrity != [{"quick_check": "ok"}]:
                raise AssertionError(f"node {index}: SQLite integrity check failed: {integrity}")
            for ack in witness_ledger:
                actual = request(self.args.port + index, f"/read?name={ack['name']}&first=0&count={ack['rows']}", timeout=60)
                if len(actual) != ack["bytes"] or hashlib.sha256(actual).hexdigest() != ack["sha256"]:
                    raise AssertionError(f"node {index}: lost auxiliary witness-session write {ack['name']}")
                integrity = json.loads(request(self.args.port + index, f"/integrity?name={ack['name']}", timeout=60))
                if integrity != [{"quick_check": "ok"}]:
                    raise AssertionError(f"node {index}: witness-session integrity failed: {integrity}")
            self.event("verified", node=index, batches=len(ledger), integrity=integrity)
        # Invoke the production sweep; the periodic default is ten minutes.
        for index in followers:
            request(self.args.port + 20 + index, "/gc/follower-log", b"", timeout=60)
        wait_for("old follower fragment GC", lambda: all(
            disk_bytes(self.output / f"node-{index}/peerlog" / session, "*.batch") == 0 for index in followers
        ), timeout=120)
        self.event("old-tail-reclaimed", session=session)
        self.report["verified_bytes_per_node"] = sum(ack["bytes"] for ack in ledger)
        self.report["verified_auxiliary_bytes_per_node"] = sum(ack["bytes"] for ack in witness_ledger)
        if sum(proxy.tail_wire_bytes for proxy in self.proxies) < self.report["acknowledged_bytes"] * 0.9:
            raise AssertionError("recovery did not transfer the required large follower tail over the peer network")
        # Full reads and quick_check hydrate every used page. Snapshot while
        # the backing files exist: a clean reload intentionally deletes paged
        # cache files. SQLite's online backup API gives a coherent image.
        images = self.output / "sqlite-images"
        images.mkdir()
        for index, database in enumerate(self.output.glob("node-*/ImportStore:*/ltx/*/db.sqlite")):
            with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as source:
                with closing(sqlite3.connect(images / f"image-{index}.sqlite")) as destination:
                    source.backup(destination)
                    # backup copies the source's WAL header. Convert this
                    # standalone image to DELETE mode before a read-only check.
                    destination.execute("PRAGMA journal_mode=DELETE").fetchall()
        if not list(images.glob("*.sqlite")):
            raise AssertionError("no recovered SQLite image was available for a full integrity check")
        # The check on a live disk must refuse retirement even after old-tail GC.
        pinned = self.retirement(survivor)
        if pinned["safe"]:
            raise AssertionError("disk retirement accepted a still-live node")
        self.event("retirement-blocked", result=pinned)
        for index in range(3):
            request(self.args.port + 20 + index, "/shutdown?handoff=preserve", b"")
            self.nodes[index].wait(timeout=50)
            if self.nodes[index].returncode:
                raise AssertionError(f"node {index} failed graceful shutdown")
        retirement = []
        for index in range(3):
            proof = wait_for(f"node {index} disk retirement", lambda index=index: (
                result if (result := self.retirement(index))["safe"] else None
            ), timeout=30)
            retirement.append(proof)
        self.report["disk_retirement"] = retirement
        integrity_files = []
        for database in images.glob("*.sqlite"):
            with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as db:
                result = db.execute("PRAGMA integrity_check").fetchall()
            if result != [("ok",)]:
                raise AssertionError(f"offline SQLite integrity failed for {database}: {result}")
            integrity_files.append(str(database.relative_to(self.output)))
        if not integrity_files:
            raise AssertionError("no recovered SQLite image was available for the full integrity check")
        self.report["full_integrity_checked"] = integrity_files
        self.event("retirement-and-integrity-verified", files=integrity_files)
        self.report["passed"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/celld")
    parser.add_argument("--mib", type=int, default=600)
    parser.add_argument("--port", type=int, default=19870)
    parser.add_argument("--scenario", choices=["owner-kill", "missing-witness"], default="owner-kill")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--hot-recovery", action="store_true", help="materialize and checkpoint follower SQLite; require hot promotion")
    parser.add_argument("--recovery-timeout", type=int, default=600)
    parser.add_argument("--tail-kib-per-second", type=int, default=4096,
                        help="throttle recovery-tail responses (0 disables the throttle)")
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    if args.mib < 1:
        parser.error("--mib must be positive")
    if not 1 <= args.port <= 65503:
        parser.error("--port must leave room for all listeners (1..65503)")
    if args.tail_kib_per_second < 0:
        parser.error("--tail-kib-per-second cannot be negative")
    if args.recovery_timeout <= 0:
        parser.error("--recovery-timeout must be positive")
    if args.output is None:
        args.output = Path(tempfile.gettempdir()) / f"celld-import-{time.time_ns()}"
    fleet = Fleet(args)
    try:
        fleet.run()
    except BaseException as error:
        fleet.report.update(passed=False, error=str(error))
        fleet.event("failed", error=str(error))
        raise
    finally:
        fleet.close()
    print(f"PASS — report and full artifacts: {fleet.output}")


if __name__ == "__main__":
    main()
