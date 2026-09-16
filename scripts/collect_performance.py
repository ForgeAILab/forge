#!/usr/bin/env python3
"""Summarize Forge performance samples and optional HAR locally; never upload data.

Python 3.10+. Output is a strict allowlist of aggregates, not a redacted copy
of input. Raw logs and HAR files must NOT be shared as support attachments.
"""
from __future__ import annotations

import argparse
import collections
import json
import math
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlsplit

MAX_FILE_BYTES = 128 * 1024 * 1024
MAX_LINE_BYTES = 64 * 1024
MAX_SAMPLES = 200_000
OPERATIONS = {"operations_status", "operations_log_snapshot"}
SCENARIOS = ("idle", "streaming", "long-log", "multitask", "reconnect", "other")
ROUTES = (
    (r"/api/v1/operations/status", "operations/status"),
    (r"/api/v1/events", "events"),
    (r"/api/v1/agent-chats", "agent-chats"),
    (r"/api/v1/agent-chats/[^/]+/turns/[^/]+/logs", "agent-chats/:chat/turns/:turn/logs"),
    (r"/api/v1/agent-chats/[^/]+/messages", "agent-chats/:chat/messages"),
    (r"/api/v1/agent-chats/[^/]+/turns", "agent-chats/:chat/turns"),
    (r"/api/v1/agent-chats/[^/]+", "agent-chats/:chat"),
    (r"/api/v1/projects/[^/]+/tasks", "projects/:project/tasks"),
    (r"/api/v1/projects/[^/]+/agent-handoffs", "projects/:project/agent-handoffs"),
    (r"/api/v1/projects/[^/]+", "projects/:project"),
    (r"/api/v1/projects", "projects"),
)


def number(value: Any) -> float | None:
    if type(value) not in (int, float):
        return None
    try:
        result = float(value)
    except (OverflowError, ValueError):
        return None
    return result if math.isfinite(result) and result >= 0 else None


def distribution(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "p50_ms": None, "p95_ms": None, "max_ms": None}
    return {
        "count": len(ordered),
        "p50_ms": round(ordered[math.ceil(len(ordered) * 0.50) - 1], 3),
        "p95_ms": round(ordered[math.ceil(len(ordered) * 0.95) - 1], 3),
        "max_ms": round(ordered[-1], 3),
    }


def checked_size(path: Path) -> None:
    if path.stat().st_size > MAX_FILE_BYTES:
        raise ValueError("input exceeds 128 MiB; capture a shorter test window")


def summarize_log(path: Path) -> dict[str, Any]:
    checked_size(path)
    groups: dict[str, Any] = {}
    accepted = ignored = oversized = 0
    limit_reached = False
    with path.open("rb") as source:
        while True:
            raw = source.readline(MAX_LINE_BYTES + 1)
            if not raw:
                break
            if len(raw) > MAX_LINE_BYTES:
                oversized += 1
                while raw and not raw.endswith(b"\n"):
                    raw = source.readline(MAX_LINE_BYTES + 1)
                continue
            try:
                event = json.loads(raw)
            except (ValueError, UnicodeError):
                ignored += 1
                continue
            if not isinstance(event, dict) or event.get("target") != "forge::perf":
                ignored += 1
                continue
            fields = event.get("fields")
            if (not isinstance(fields, dict)
                    or not isinstance(fields.get("operation"), str)
                    or fields["operation"] not in OPERATIONS):
                ignored += 1
                continue
            elapsed = number(fields.get("elapsed_ms"))
            if elapsed is None:
                ignored += 1
                continue
            if accepted >= MAX_SAMPLES:
                limit_reached = True
                break
            accepted += 1
            group = groups.setdefault(fields["operation"], {
                "elapsed": [], "success_count": 0, "failure_count": 0,
                "cache_hits": 0, "scanned_bytes": 0, "scanned_lines": 0,
                "parsed_entries": 0,
            })
            group["elapsed"].append(elapsed)
            if fields.get("success") is True:
                group["success_count"] += 1
            elif fields.get("success") is False:
                group["failure_count"] += 1
            if fields.get("cache_hit") is True:
                group["cache_hits"] += 1
            for key in ("scanned_bytes", "scanned_lines", "parsed_entries"):
                value = number(fields.get(key))
                if value is not None:
                    group[key] += int(value)
    for group in groups.values():
        group.update(distribution(group.pop("elapsed")))
    return {
        "operations": groups,
        "samples": accepted,
        "ignored_lines": ignored,
        "oversized_lines": oversized,
        "sample_limit_reached": limit_reached,
    }


def route_group(url: Any) -> str | None:
    if not isinstance(url, str):
        return None
    try:
        path = urlsplit(url).path.rstrip("/")
    except ValueError:
        return None
    if not path.startswith("/api/v1/"):
        return None
    for pattern, label in ROUTES:
        if re.fullmatch(pattern, path):
            return label
    return "other_api"  # Never copy an unknown path into the report.


def summarize_har(path: Path) -> dict[str, Any]:
    checked_size(path)
    with path.open(encoding="utf-8-sig") as source:
        data = json.load(source)
    log = data.get("log") if isinstance(data, dict) else None
    entries = log.get("entries") if isinstance(log, dict) else None
    if not isinstance(entries, list):
        raise ValueError("HAR must contain a log.entries array")
    groups: dict[str, Any] = {}
    ignored = accepted = 0
    limit_reached = False
    for entry in entries:
        if not isinstance(entry, dict):
            ignored += 1
            continue
        request = entry.get("request")
        response = entry.get("response")
        if not isinstance(request, dict) or not isinstance(response, dict):
            ignored += 1
            continue
        route = route_group(request.get("url"))
        if route is None:
            ignored += 1
            continue
        if accepted >= MAX_SAMPLES:
            limit_reached = True
            break
        accepted += 1
        method = request.get("method")
        if method not in ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"):
            method = "OTHER"
        group = groups.setdefault(f"{method} {route}", {
            "requests": 0, "elapsed": [], "status_counts": collections.Counter(),
            "reported_body_bytes": 0, "body_size_samples": 0,
        })
        group["requests"] += 1
        status = response.get("status")
        if type(status) is int and 0 <= status <= 599:
            group["status_counts"][str(status)] += 1
        elapsed = number(entry.get("time"))
        # An SSE connection's lifetime is not an ordinary request latency.
        if elapsed is not None and route != "events":
            group["elapsed"].append(elapsed)
        body_size = number(response.get("bodySize"))
        if body_size is not None:
            group["reported_body_bytes"] += int(body_size)
            group["body_size_samples"] += 1
    for group in groups.values():
        group["latency"] = distribution(group.pop("elapsed"))
        group["status_counts"] = dict(group["status_counts"])
    return {
        "routes": groups, "requests": accepted, "ignored_entries": ignored,
        "sample_limit_reached": limit_reached,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, help="Forge JSON tracing log (not execution JSONL)")
    parser.add_argument("--har", type=Path, help="optional browser Network HAR; processed locally")
    parser.add_argument("--scenario", choices=SCENARIOS, default="other")
    parser.add_argument("--revision", default="unknown", help="tested git commit, 7-40 hex characters")
    parser.add_argument("--out", type=Path, required=True, help="new aggregate JSON file; never overwritten")
    args = parser.parse_args()
    if not args.log and not args.har:
        parser.error("provide --log, --har, or both")
    if args.revision != "unknown" and not re.fullmatch(r"[0-9a-fA-F]{7,40}", args.revision):
        parser.error("--revision must be a git commit hash")
    try:
        report: dict[str, Any] = {
            "schema": "forge.performance-report/1", "scenario": args.scenario,
            "revision": args.revision.lower(),
            "notes": [
                "Only allowlisted aggregates; no URLs, headers, bodies, paths or message text.",
                "Nearest-rank percentiles. These samples do not measure CPU or SQLite lock waits.",
                "HAR body size is browser-reported and may be unavailable or affected by caching.",
                "Check sample counts and limits; an empty report is not evidence of good performance.",
            ],
        }
        if args.log:
            report["server"] = summarize_log(args.log)
        if args.har:
            report["browser"] = summarize_har(args.har)
        # Exclusive creation avoids accidental overwrite or following an
        # existing symlink. POSIX mode 0600 also protects the aggregate file.
        with open(args.out, "x", encoding="utf-8", opener=lambda p, f: os.open(p, f, 0o600)) as target:
            json.dump(report, target, indent=2, sort_keys=True, allow_nan=False)
            target.write("\n")
    except (OSError, ValueError, TypeError, RecursionError):
        parser.exit(1, "Cannot produce report: check input format/size and choose a new writable --out file.\n")
    print("Aggregate report written. Share the report, not the raw log or HAR.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
