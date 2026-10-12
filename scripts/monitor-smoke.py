#!/usr/bin/env python3
# Copyright 2026 Deno Land Inc. Apache-2.0 license.
"""Exercise the embedded monitor against real celld processes, without a bucket."""

import argparse
import base64
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import queue
import re
import signal
import socket
import subprocess
import tempfile
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
SOURCE = """
export class MonitorCell {
  constructor(state) {
    this.state = state;
    state.storage.sql.exec("CREATE TABLE IF NOT EXISTS sample (id INTEGER PRIMARY KEY, data BLOB)");
  }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.searchParams.get("mode") === "stream") {
      let remaining = 256;
      return new Response(new ReadableStream({
        pull(controller) {
          if (remaining-- > 0) controller.enqueue(new Uint8Array(262144));
          else controller.close();
        }
      }), {headers: {"content-type": "application/octet-stream"}});
    }
    if (url.searchParams.get("mode") === "ws") {
      const pair = new WebSocketPair();
      this.state.acceptWebSocket(pair[1]);
      return new Response(null, {status: 101, webSocket: pair[0]});
    }
    const size = Number(url.searchParams.get("bytes") || 0);
    if (size) {
      this.buffer = new Uint8Array(size);
      this.state.storage.sql.exec("INSERT OR REPLACE INTO sample VALUES (1, zeroblob(?))", size);
    }
    // Worker clocks can be frozen within a turn. Use bounded work, never a
    // Date.now()-based busy loop (work_ms is only an approximate workload knob).
    const rounds = Number(url.searchParams.get("work_ms") || 0) * 120000;
    let result = 0;
    for (let i = 0; i < rounds; i++) result += Math.sqrt(result + 1);
    const wait = Number(url.searchParams.get("wait_ms") || 0);
    if (wait) await new Promise(resolve => setTimeout(resolve, wait));
    return Response.json({id: this.state.id.toString(), result, version: 1});
  }
  webSocketMessage(ws, message) {
    if (message === "flood") {
      const chunk = new Uint8Array(131072);
      for (let i = 0; i < 256; i++) ws.send(chunk);
    }
  }
  webSocketClose(ws, code, reason) { ws.close(code, reason); }
}
export default {
  async fetch(request, env) {
    const name = new URL(request.url).searchParams.get("name") || "a";
    return env.CELLS.get(env.CELLS.idFromName(name)).fetch(request);
  }
};
"""


def request(url, method="GET", headers=None, timeout=10):
    try:
        with urlopen(Request(url, method=method, headers=headers or {}), timeout=timeout) as response:
            return response.status, response.headers, response.read()
    except HTTPError as error:
        return error.code, error.headers, error.read()


def wait_for(check, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError("condition did not become true before deadline")


class Node:
    def __init__(self, binary, project, monitor):
        self.internal = None
        self.ready = queue.Queue()
        self.logs = deque(maxlen=100)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        self.worker = f"http://127.0.0.1:{port}"
        # Never accidentally use a caller's fleet, credentials or runtime tuning.
        env = {key: value for key, value in os.environ.items() if not key.startswith("CELLD_")}
        env.update(CELLD_MONITOR=str(int(monitor)), NO_COLOR="1")
        self.process = subprocess.Popen(
            [str(binary), "dev", str(project), "--port", str(port), "--logs"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, start_new_session=True,
        )
        self.reader = threading.Thread(target=self.read_logs, daemon=True)
        self.reader.start()

    def read_logs(self):
        for line in self.process.stdout:
            self.logs.append(line.rstrip())
            match = re.search(r"celld internal listening on (\S+)", line)
            if match:
                self.internal = f"http://{match.group(1)}"
            if re.match(r"\s+ready\s+http://", line):
                self.ready.put(self.internal)

    def wait_ready(self):
        try:
            internal = self.ready.get(timeout=40)
        except queue.Empty:
            raise AssertionError("node did not become ready:\n" + "\n".join(self.logs))
        assert internal, "node did not announce its internal origin"
        return internal

    def close(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                # Only our own newly created process group, never a user node.
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=5)
        self.reader.join(timeout=2)
        self.process.stdout.close()

    def fetch(self, **params):
        status, _, body = request(self.worker + "/?" + urlencode(params))
        assert status == 200, (status, body)
        return json.loads(body)

    def snapshot(self, query=""):
        status, _, body = request(self.internal + "/monitor/api" + query)
        assert status == 200, (status, body)
        return json.loads(body)


def check_enabled(node, project):
    first = node.snapshot()
    assert first["cells"] == [], "reading the monitor activated a cell"
    node.fetch(name="a", bytes=524288)
    node.fetch(name="b", bytes=32768)

    def sampled():
        sample = node.snapshot()
        cells = sample["cells"]
        return sample if len(cells) == 2 and all(cell["sqlite"] for cell in cells) else None

    baseline = wait_for(sampled)
    a, b = baseline["cells"]
    # The identifiers are hashes; pick the bigger database as cell a.
    if a["sqlite"]["cache_bytes"] < b["sqlite"]["cache_bytes"]:
        a, b = b, a
    assert a["isolate"] == b["isolate"], "fixture did not exercise a shared isolate"
    heap = next(heap for heap in baseline["isolates"] if heap["id"] == a["isolate"])
    assert heap["resident_cells"] == 2 and heap["memory"]["heap_used_bytes"] > 0
    assert set(a["sqlite"]) == {"cache_bytes", "schema_bytes", "statement_bytes"}
    assert "heap_used_bytes" not in a, "shared heap was claimed as cell memory"
    assert a["sqlite"]["cache_bytes"] > b["sqlite"]["cache_bytes"]

    node.fetch(name="a", work_ms=150)
    after = node.snapshot()
    measured_a = next(cell for cell in after["cells"] if cell["id"] == a["id"])
    measured_b = next(cell for cell in after["cells"] if cell["id"] == b["id"])
    assert measured_a["turns"]["cpu_nanos"] > a["turns"]["cpu_nanos"] + 20_000_000
    assert measured_b["turns"]["cpu_nanos"] == b["turns"]["cpu_nanos"]
    print("PASS: real per-residency turn CPU and SQLite memory; shared heap not split")

    # A synchronous guest turn must not strand the operator endpoint waiting
    # for V8. On this multi-threaded node it returns the cached heap immediately.
    with ThreadPoolExecutor(max_workers=1) as executor:
        running = executor.submit(node.fetch, name="a", work_ms=1800)
        busy = wait_for(lambda: next(
            (heap for heap in node.snapshot()["isolates"] if heap["id"] == a["isolate"] and heap["busy"]), None
        ), timeout=1.5)
        assert busy["memory_stale"] and busy["memory"] is not None
        start = time.monotonic()
        concurrent = node.snapshot()
        assert time.monotonic() - start < 0.75, "monitor waited behind the busy isolate"
        current_heap = next(heap for heap in concurrent["isolates"] if heap["id"] == a["isolate"])
        assert current_heap["memory"]["sampled_unix_ms"] == busy["memory"]["sampled_unix_ms"]
        running.result(timeout=5)
    print("PASS: busy isolates retain stale memory without blocking the operator API")

    before_wait = next(cell for cell in node.snapshot()["cells"] if cell["id"] == a["id"])
    node.fetch(name="a", wait_ms=1100)
    after_wait = next(cell for cell in node.snapshot()["cells"] if cell["id"] == a["id"])
    assert after_wait["turns"]["cpu_nanos"] - before_wait["turns"]["cpu_nanos"] < 250_000_000
    print("PASS: asynchronous wait is not charged as CPU")

    check_buffers(node, a["id"], b["id"])

    page = node.snapshot("?limit=1&offset=1&class=MonitorCell")
    assert page["total_matches"] == 2 and len(page["cells"]) == 1 and page["offset"] == 1
    search = node.snapshot("?" + urlencode({"q": a["id"]}))
    assert len(search["cells"]) == 1 and search["cells"][0]["id"] == a["id"]
    assert request(node.internal + "/monitor/api?limit=251")[0] == 400
    assert request(node.internal + "/monitor/api", method="POST")[0] == 405
    assert request(node.internal + "/monitor/api", headers={"Origin": "http://attacker.invalid"})[0] == 403
    assert request(node.internal + "/monitor/api", headers={"Host": "attacker.invalid"})[0] == 403
    assert request(node.internal + "/monitor/api", headers={"Sec-Fetch-Site": "cross-site"})[0] == 403
    navigation = {"Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "document"}
    assert request(node.internal + "/monitor/", headers=navigation)[0] == 200
    assert request(node.internal + "/monitor/api", headers=navigation)[0] == 403
    status, headers, body = request(node.internal + "/monitor/")
    assert status == 200, "build dashboard assets and rebuild celld --features monitor-ui first"
    assert headers["X-Content-Type-Options"] == "nosniff" and headers["Cache-Control"] == "no-store"
    assert "wasm-unsafe-eval" in headers["Content-Security-Policy"]
    assets = re.findall(rb'(?:src|href)="(/monitor/[^" ]+\.(?:js|css|wasm))"', body)
    assert assets, "Trunk asset links are missing"
    for asset in assets:
        assert request(node.internal + asset.decode())[0] == 200
    assert request(node.internal + "/monitor/", method="HEAD")[2] == b""
    status, _, body = request(node.worker + "/monitor/api")
    assert status == 200 and json.loads(body).get("schema_version") is None, "monitor leaked onto Worker listener"
    print("PASS: bounded queries, read-only/origin guards, embedded assets and private listener")

    # Dev reload intentionally restarts the node; history must distinguish this
    # process/deployment even when the same named cell and epoch return.
    old = node.snapshot()
    source = project / "index.js"
    source.write_text(SOURCE.replace("version: 1", "version: 2"))
    node.wait_ready()
    assert node.fetch(name="a")["version"] == 2
    replacement = node.snapshot()
    assert replacement["process"] != old["process"]
    assert replacement["node"]["deployment"] != old["node"]["deployment"]
    # Clean dev restart proactively reactivates all previously resident cells.
    assert {cell["id"] for cell in replacement["cells"]} == {cell["id"] for cell in old["cells"]}
    assert all(cell["turns"]["cpu_nanos"] < 250_000_000 for cell in replacement["cells"])
    print("PASS: deployment/node replacement does not inherit CPU history")


def check_buffers(node, owner_id, other_id):
    def buffers(cell_id):
        return next(cell for cell in node.snapshot()["cells"] if cell["id"] == cell_id)["buffers"]

    assert buffers(owner_id) == {"http_stream_bytes": 0, "websocket_bytes": 0}
    # Stop reading after the response headers: native response queues must
    # retain payloads once the client's TCP receive window fills.
    response = urlopen(node.worker + "/?name=a&mode=stream", timeout=10)
    try:
        retained = wait_for(lambda: buffers(owner_id)["http_stream_bytes"] or None)
        # The bridge may split application chunks into smaller native writes.
        assert retained > 0, retained
        assert buffers(other_id)["http_stream_bytes"] == 0
    finally:
        response.close()
    wait_for(lambda: buffers(owner_id)["http_stream_bytes"] == 0)
    print("PASS: real slow HTTP consumer charges only its cell; cancel frees buffers")

    port = int(node.worker.rsplit(":", 1)[1])
    with socket.create_connection(("127.0.0.1", port), timeout=10) as ws:
        ws.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        key = base64.b64encode(os.urandom(16)).decode()
        ws.sendall((f"GET /?name=a&mode=ws HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
                    f"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
                    "Sec-WebSocket-Version: 13\r\n\r\n").encode())
        headers = b""
        while b"\r\n\r\n" not in headers:
            piece = ws.recv(4096)
            assert piece, headers
            headers += piece
        assert b" 101 " in headers.split(b"\r\n", 1)[0], headers
        # One masked client text frame, per RFC 6455. Deliberately do not read
        # the flood; queued native frames must remain visible in the monitor.
        payload, mask = b"flood", os.urandom(4)
        ws.sendall(bytes([0x81, 0x80 | len(payload)]) + mask + bytes(
            byte ^ mask[index % 4] for index, byte in enumerate(payload)
        ))
        retained = wait_for(lambda: (value if (value := buffers(owner_id)["websocket_bytes"]) >= 131072 else None))
        assert retained >= 131072, retained
        assert buffers(other_id)["websocket_bytes"] == 0
        ws.shutdown(socket.SHUT_RDWR)
    wait_for(lambda: buffers(owner_id)["websocket_bytes"] == 0)
    print("PASS: real slow WebSocket consumer charges only its cell; close frees buffers")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/celld")
    args = parser.parse_args()
    binary = args.binary.resolve()
    assert binary.is_file(), f"build celld first: {binary}"
    scratch = Path(os.environ.get("TMPDIR", "/tmp")) / "opencode"
    if not scratch.is_dir():
        scratch = None
    with tempfile.TemporaryDirectory(prefix="celld-monitor-smoke-", dir=scratch) as temporary:
        project = Path(temporary)
        (project / "index.js").write_text(SOURCE)
        (project / "wrangler.jsonc").write_text(json.dumps({
            "name": "monitor-smoke", "main": "index.js", "compatibility_date": "2026-01-01",
            "durable_objects": {"bindings": [{"name": "CELLS", "class_name": "MonitorCell"}]},
            "migrations": [{"tag": "v1", "new_sqlite_classes": ["MonitorCell"]}],
        }))
        node = Node(binary, project, monitor=True)
        try:
            node.wait_ready()
            check_enabled(node, project)
        except Exception:
            print("\n".join(node.logs))
            raise
        finally:
            node.close()
        node = Node(binary, project, monitor=False)
        try:
            node.wait_ready()
            node.fetch(name="a")
            assert request(node.internal + "/monitor/")[0] == 404
            assert request(node.internal + "/monitor/api")[0] == 404
            print("PASS: CELLD_MONITOR=0 disables the dashboard and resource API")
        finally:
            node.close()


if __name__ == "__main__":
    main()
