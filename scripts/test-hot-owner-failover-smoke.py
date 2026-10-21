#!/usr/bin/env python3
# Copyright 2026 Deno Land Inc. Apache-2.0 license.
"""Peer-tail observability and bounded large-data failover verifier checks."""

import hashlib
import http.client
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import runpy
import socket
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

H = runpy.run_path(str(Path(__file__).with_name("hot-owner-failover-smoke.py")))


class EchoHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self.send_response(101)
        self.send_header("Connection", "Upgrade")
        self.send_header("Upgrade", "test-echo")
        self.end_headers()
        body = self.connection.recv(4)
        self.connection.sendall(body)
        self.close_connection = True

    def log_message(self, *args):
        pass


class FailoverHarnessTests(unittest.TestCase):
    def test_all_witness_processes_launch_before_waiting_for_worker_readiness(self):
        launched, checked, nodes = [], [], []
        def popen(command, **options):
            name = options["env"]["CELLD_NODE"]
            launched.append(name)
            return Mock(pid=100 + len(launched), poll=Mock(return_value=None))
        def get(url):
            if len(launched) != 3:
                raise AssertionError("worker readiness needs recovery witnesses that have not started")
            checked.append(url)
            return 200, {}
        globals = H["MeasuredNode"].start.__globals__
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(globals["subprocess"], "Popen", side_effect=popen), \
             patch.dict(globals, PeerProxy=lambda port: SimpleNamespace(server_address=("127.0.0.1", 1), close=Mock()),
                        get=get, wait=lambda check, timeout: check()):
            root = Path(directory)
            def create_node(name, **options):
                return H["MeasuredNode"]("unused", root, {}, "suite-bucket", "http://unused", name, root, 1, **options)
            H["start_fleet"](nodes, create_node)
            self.assertEqual(launched, ["gpu-a", "gpu-b", "gpu-c"])
            self.assertEqual(len(checked), 3)
            # Returning-owner startup still waits for health synchronously.
            def returning_ready(url):
                checked.append(url)
                return 200, {}
            with patch.dict(globals, get=returning_ready):
                nodes[0].start()
            self.assertEqual(len(checked), 4)
            for node in nodes:
                node.proxy.close()

    def test_a_readiness_failure_retains_every_launched_node_for_cleanup(self):
        nodes = []
        def create_node(name, defer_ready):
            self.assertTrue(defer_ready)
            node = SimpleNamespace(name=name, wait_ready=Mock(side_effect=TimeoutError("startup blocked")))
            return node
        with self.assertRaises(TimeoutError):
            H["start_fleet"](nodes, create_node)
        self.assertEqual([node.name for node in nodes], ["gpu-a", "gpu-b", "gpu-c"])
        nodes[0].wait_ready.assert_called_once()
        nodes[1].wait_ready.assert_not_called()
        nodes[2].wait_ready.assert_not_called()

    def test_self_fencing_and_forced_cleanup_cannot_be_reported_as_clean_exits(self):
        self.assertTrue(H["clean_process_exits"]([{"node": "gpu-a", "returncode": 0}]))
        for code in (3, -6, -9, -11, None):
            with self.subTest(returncode=code):
                self.assertFalse(H["clean_process_exits"]([{"node": "gpu-a", "returncode": code}]))
        self.assertFalse(H["clean_process_exits"]([{"node": "gpu-a", "returncode": 0, "error": "proxy cleanup failed"}]))

    def test_read_only_startup_can_recruit_before_its_first_fleet_ack(self):
        log = {"state": "open", "active": False, "ensemble": ["gpu-b", "gpu-c"]}
        self.assertTrue(H["ensemble_joined"](log, {"gpu-b", "gpu-c"}))
        self.assertFalse(H["ensemble_joined"](log, {"gpu-b"}))
        self.assertFalse(H["ensemble_joined"](log | {"state": "sealed"}, {"gpu-b", "gpu-c"}))

    def test_retries_keep_acknowledgements_and_do_not_replay_ambiguous_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger, seed = Path(directory) / "receipts.jsonl", Path(directory) / "seed-receipt.json"
            ledger.write_text(json.dumps({"seq": 10, "ok": True, "sha256": "a"}) + "\n"
                              + json.dumps({"seq": 15, "ok": False, "sha256": "b"}) + "\n")
            seed.write_text(json.dumps({"seq": 11, "sha256": "c"}))
            self.assertEqual(H["receipt_state"]([ledger, seed]), ({10: "a", 11: "c"}, 16))
            seed.write_text(json.dumps({"seq": 10, "sha256": "corrupt"}))
            with self.assertRaises(AssertionError):
                H["receipt_state"]([ledger, seed])

    def test_advisory_readiness_does_not_certify_writes_only_seen_by_the_owner(self):
        candidate = {"ready": True, "txid": 9, "covered_txid": 9}
        self.assertFalse(H["checkpoint_matches_owner"](candidate, 28))
        self.assertFalse(H["checkpoint_matches_owner"](candidate, 0))
        self.assertTrue(H["checkpoint_matches_owner"](candidate, 9))
        self.assertFalse(H["checkpoint_matches_owner"](candidate | {"covered_txid": 8}, 9))

    def test_checkpointed_recovery_requires_proofs_and_no_tail_requests_even_without_bytes(self):
        case = {"hot_certifications_before_read": 1, "hot_promotions_before_read": 1,
                "peer_before_first_read": {"tail_requests": 0, "tail_wire_bytes": 0}}
        H["require_checkpointed_hot_recovery"](case)
        for invalid in (case | {"hot_certifications_before_read": 0},
                        case | {"hot_promotions_before_read": 0},
                        case | {"peer_before_first_read": {"tail_requests": 1, "tail_wire_bytes": 0}},
                        case | {"peer_before_first_read": {"tail_requests": 0, "tail_wire_bytes": 1}}):
            with self.subTest(case=invalid), self.assertRaises(AssertionError):
                H["require_checkpointed_hot_recovery"](invalid)

    def test_owner_position_is_epoch_scoped_and_uses_the_latest_ltx_range(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ltx = root / "Counter:test/ltx/e4/.db.sqlite-litestream/ltx/0"
            ltx.mkdir(parents=True)
            (ltx / "0000000000000001-000000000000001c.ltx").touch()
            (ltx / "0000000000000002-0000000000000002.ltx").touch()
            self.assertEqual(H["owner_position"](root, "Counter:test", 4), 28)
            self.assertEqual(H["owner_position"](root, "Counter:test", 5), 0)

    def test_peer_headers_close_normal_requests_but_preserve_body_and_upgrades(self):
        request = b"POST /peer/log/tail-page HTTP/1.1\r\nConnection: keep-alive\r\nContent-Length: 3\r\n\r\nabc"
        transformed, tail = H["peer_header"](request)
        self.assertTrue(tail)
        self.assertIn(b"Connection: close\r\n", transformed)
        self.assertTrue(transformed.endswith(b"\r\n\r\nabc"))
        upgraded = b"GET /peer/tunnel HTTP/1.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"
        self.assertEqual(H["peer_header"](upgraded), (upgraded, False))

    def test_a_reused_http_client_cannot_hide_a_later_tail_request(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), EchoHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        proxy = H["PeerProxy"](server.server_address[1])
        connection = http.client.HTTPConnection("127.0.0.1", proxy.server_address[1], timeout=5)
        try:
            connection.request("POST", "/peer/log/append", b"append")
            self.assertEqual(connection.getresponse().read(), b"append")
            self.assertEqual(proxy.counters()["tail_wire_bytes"], 0)
            connection.request("POST", "/peer/log/tail-page", b"tail")
            self.assertEqual(connection.getresponse().read(), b"tail")
            self.assertEqual(proxy.counters()["tail_requests"], 1)
            self.assertGreater(proxy.counters()["tail_wire_bytes"], 4)
        finally:
            connection.close()
            proxy.close()
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_legacy_tail_endpoint_is_also_measured(self):
        request = b"POST /peer/log/tail HTTP/1.1\r\nContent-Length: 0\r\n\r\n"
        self.assertTrue(H["peer_header"](request)[1])

    def test_proxy_relays_upgraded_binary_rpc_connections(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), EchoHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        proxy = H["PeerProxy"](server.server_address[1])
        try:
            with socket.create_connection(proxy.server_address, timeout=5) as connection:
                connection.sendall(b"GET /peer/tunnel HTTP/1.1\r\nConnection: Upgrade\r\nUpgrade: test-echo\r\n\r\n")
                response = b""
                while b"\r\n\r\n" not in response:
                    response += connection.recv(4096)
                self.assertTrue(response.startswith(b"HTTP/1.1 101"))
                connection.sendall(b"\x00\xff\x80\x01")
                echoed = b""
                while len(echoed) < 4:
                    block = connection.recv(4 - len(echoed))
                    self.assertTrue(block, "RPC upgrade disconnected")
                    echoed += block
                self.assertEqual(echoed, b"\x00\xff\x80\x01")
                self.assertEqual(proxy.counters()["tail_wire_bytes"], 0)
        finally:
            proxy.close()
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_larger_read_batches_still_check_each_original_client_digest(self):
        blobs = [H["BASE"]["preload_payload"](first) for first in (0, 64)]
        receipts = [{"first": first, "rows": 64, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
                    for first, body in zip((0, 64), blobs)]
        node = SimpleNamespace(name="node", worker="http://unused")
        globals = H["verify_all"].__globals__
        with patch.dict(globals, http=lambda url, timeout: (200, b"".join(blobs))):
            H["verify_all"](node, receipts, {})
        bad = blobs[0] + b"x" + blobs[1][1:]
        with patch.dict(globals, http=lambda url, timeout: (200, bad)):
            with self.assertRaises(AssertionError):
                H["verify_all"](node, receipts, {})

    def test_preload_and_acknowledged_tail_share_the_explicit_deadline_and_progress(self):
        preload = H["BASE"]["preload_payload"](0)
        tail = H["payload"](64)
        receipt = {"first": 0, "rows": 64, "bytes": len(preload), "sha256": hashlib.sha256(preload).hexdigest()}
        requested, progress = [], []
        def response(url, timeout):
            requested.append((url, timeout))
            return 200, preload if "first=0&" in url else tail
        with patch.dict(H["verify_all"].__globals__, http=response), \
             patch.dict(H["BASE"]["verify_reads"].__globals__, http=response):
            H["verify_all"](SimpleNamespace(name="node", worker="http://unused"), [receipt],
                            {64: hashlib.sha256(tail).hexdigest()}, progress=progress.append)
        self.assertEqual([timeout for _, timeout in requested], [120, 120])
        self.assertEqual([item["event"] for item in progress],
                         ["verification_read_started", "verification_read_received"] * 2)
        self.assertEqual([item["first"] for item in progress], [0, 0, 64, 64])

    def test_a_tail_timeout_records_the_exact_range_and_is_not_retried_or_hidden(self):
        body = H["payload"](64)
        requested, progress = [], []
        def response(url, timeout):
            requested.append(timeout)
            raise TimeoutError("injected stalled read")
        with patch.dict(H["BASE"]["verify_reads"].__globals__, http=response), self.assertRaises(TimeoutError):
            H["verify_all"](SimpleNamespace(name="node", worker="http://unused"), [],
                            {64: hashlib.sha256(body).hexdigest()}, progress=progress.append)
        self.assertEqual(requested, [120])
        self.assertEqual([item["event"] for item in progress], ["verification_read_started", "verification_read_failed"])
        self.assertEqual({key: progress[-1][key] for key in ("node", "first", "count", "read_timeout", "error")},
                         {"node": "node", "first": 64, "count": 1, "read_timeout": 120, "error": "injected stalled read"})


if __name__ == "__main__":
    unittest.main()
