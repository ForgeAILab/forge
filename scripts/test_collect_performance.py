import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from collect_performance import MAX_LINE_BYTES, distribution, route_group, summarize_har, summarize_log


class PerformanceCollectorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def test_log_allowlist_and_statistics(self):
        path = self.root / "tracing.jsonl"
        events = [
            {"target": "forge::perf", "fields": {
                "operation": "operations_log_snapshot", "elapsed_ms": elapsed,
                "success": True, "cache_hit": hit, "scanned_lines": lines,
                "scanned_bytes": lines * 200, "parsed_entries": lines,
                "prompt": "SECRET_PROMPT", "path": "/SECRET_PATH",
            }} for elapsed, hit, lines in [(20, False, 100), (1, True, 0)]
        ]
        path.write_text("not json\n" + "\n".join(map(json.dumps, events)))
        report = summarize_log(path)
        item = report["operations"]["operations_log_snapshot"]
        self.assertEqual(item["count"], 2)
        self.assertEqual(item["cache_hits"], 1)
        self.assertEqual(item["scanned_lines"], 100)
        self.assertEqual(item["p95_ms"], 20)
        self.assertEqual(report["ignored_lines"], 1)
        self.assertNotIn("SECRET", json.dumps(report))

    def test_har_never_copies_secrets_or_unknown_routes(self):
        path = self.root / "capture.har"
        entries = [
            {"request": {"url": url, "method": "GET", "headers": [{"value": "SECRET_TOKEN"}],
                         "postData": {"text": "SECRET_BODY"}},
             "response": {"status": 200, "bodySize": 123, "content": {"text": "SECRET_RESPONSE"}},
             "time": 42}
            for url in [
                "http://SECRET_HOST/api/v1/agent-chats/SECRET_CHAT/turns/SECRET_TURN/logs?token=SECRET",
                "http://SECRET_HOST/api/v1/unknown/SECRET_PATH",
                "http://SECRET_HOST/api/v1/events?token=SECRET",
                "http://SECRET_HOST/SECRET_NOT_API",
            ]
        ]
        path.write_text(json.dumps({"log": {"entries": entries}}))
        report = summarize_har(path)
        self.assertEqual(report["requests"], 3)
        self.assertEqual(report["routes"]["GET events"]["latency"]["count"], 0)
        self.assertIn("GET other_api", report["routes"])
        self.assertNotIn("SECRET", json.dumps(report))

    def test_oversized_lines_and_invalid_numbers_are_skipped(self):
        path = self.root / "tracing.jsonl"
        invalid = {"target": "forge::perf", "fields": {
            "operation": "operations_status", "elapsed_ms": float("nan")}}
        path.write_text("x" * (MAX_LINE_BYTES + 10) + "\n" + json.dumps(invalid) + "\n")
        report = summarize_log(path)
        self.assertEqual(report["oversized_lines"], 1)
        self.assertEqual(report["samples"], 0)
        self.assertEqual(distribution([])["p95_ms"], None)

    def test_route_group_and_percentiles(self):
        self.assertEqual(route_group("http://localhost/api/v1/operations/status?secret=x"), "operations/status")
        self.assertIsNone(route_group("http://localhost/not-api"))
        self.assertEqual(distribution([1, 2, 100])["p95_ms"], 100)

    def test_cli_writes_once_and_does_not_expose_input_paths(self):
        path = self.root / "input.jsonl"
        path.write_text(json.dumps({"target": "forge::perf", "fields": {
            "operation": "operations_status", "elapsed_ms": 1, "success": True}}) + "\n")
        out = self.root / "report.json"
        command = [sys.executable, str(Path(__file__).with_name("collect_performance.py")),
                   "--log", str(path), "--scenario", "idle", "--revision", "e6bd266", "--out", str(out)]
        first = subprocess.run(command, text=True, capture_output=True)
        self.assertEqual(first.returncode, 0, first.stderr)
        report = out.read_text()
        self.assertNotIn(str(self.root), report)
        self.assertEqual(json.loads(report)["server"]["samples"], 1)
        second = subprocess.run(command, text=True, capture_output=True)
        self.assertNotEqual(second.returncode, 0)
        self.assertEqual(report, out.read_text())


if __name__ == "__main__":
    unittest.main()
