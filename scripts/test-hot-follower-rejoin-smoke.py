#!/usr/bin/env python3
# Copyright 2026 Deno Land Inc. Apache-2.0 license.
"""Bounded preload/standby verification regressions for the live fault harness."""

import hashlib
from pathlib import Path
import runpy
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zlib

HARNESS = runpy.run_path(str(Path(__file__).with_name("hot-follower-rejoin-smoke.py")))
ROW_BYTES = HARNESS["ROW_BYTES"]


class LargeDatabaseHarnessTests(unittest.TestCase):
    def test_acknowledged_tail_reads_have_an_explicit_verification_deadline(self):
        body = HARNESS["payload"](64)
        node = SimpleNamespace(name="node", worker="http://unused")
        requested = []
        def response(url, timeout):
            requested.append(timeout)
            return 200, body
        with patch.dict(HARNESS["verify_reads"].__globals__, http=response):
            HARNESS["verify_reads"](node, {64: hashlib.sha256(body).hexdigest()}, [])
        self.assertEqual(requested, [60])

    def test_preload_is_reproducible_unique_and_incompressible(self):
        first = HARNESS["preload_payload"](0)
        self.assertEqual(len(first), 64 * ROW_BYTES)
        self.assertEqual(first, HARNESS["preload_payload"](0))
        self.assertNotEqual(first, HARNESS["preload_payload"](64))
        self.assertGreater(len(zlib.compress(first)), len(first) * .99)

    def test_chunk_verification_rejects_missing_and_corrupt_bytes(self):
        body = HARNESS["preload_payload"](0)
        receipt = {"first": 0, "rows": 64, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
        HARNESS["verify_preload"](lambda first, count: body, [receipt])
        with self.assertRaises(AssertionError):
            HARNESS["verify_preload"](lambda first, count: body[:-1], [receipt])
        with self.assertRaises(AssertionError):
            HARNESS["verify_preload"](lambda first, count: b"x" + body[1:], [receipt])

    def test_standby_checks_preload_and_acknowledged_tail_without_loading_the_database(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            node = SimpleNamespace(name="follower")
            cache = state / node.name / "hot-standby"
            cache.mkdir(parents=True)
            body = HARNESS["preload_payload"](0)
            receipt = {"first": 0, "rows": 64, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
            tail = HARNESS["payload"](64)
            with sqlite3.connect(cache / "1.sqlite") as database:
                database.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)")
                database.executemany("INSERT INTO records VALUES (?, ?)",
                                     [(row, body[row * ROW_BYTES:(row + 1) * ROW_BYTES]) for row in range(64)])
                database.execute("INSERT INTO records VALUES (?, ?)", (64, tail))
            result = HARNESS["verify_standby"](node, state, {64: hashlib.sha256(tail).hexdigest()}, [receipt], 64)
            self.assertEqual(result["acknowledged_rows_verified"], 65)
            self.assertEqual(result["preload_rows_verified"], 64)
            self.assertEqual(result["integrity_check"], "ok")
            with sqlite3.connect(cache / "1.sqlite") as database:
                database.execute("UPDATE records SET payload = ? WHERE id = 0", (b"wrong",))
            with self.assertRaises(AssertionError):
                HARNESS["verify_standby"](node, state, {64: hashlib.sha256(tail).hexdigest()}, [receipt], 64)

    def test_disk_sample_separates_staging_images_and_durable_tails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for path, size in (("peerlog/session/1.batch", 5), ("hot-standby/1.sqlite", 11),
                               ("hot-standby/.seed-test/db.sqlite", 13), ("node-id", 7)):
                file = root / path
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_bytes(b"x" * size)
            self.assertEqual(HARNESS["disk_sample"](root), {
                "disk_bytes": 36, "peerlog_bytes": 5, "hot_standby_bytes": 24, "seed_bytes": 13,
            })


if __name__ == "__main__":
    unittest.main()
