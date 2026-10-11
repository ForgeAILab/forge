#!/usr/bin/env python3
"""Measure the current compact tool-definition arrays per surface.

Prints UTF-8 bytes and ceil(bytes / 4) estimated tokens for each native and
MCP surface. Run from the repository root with the normal Cargo environment.
With no log arguments, runs just the two serialization tests. --logs consumes
their saved --nocapture output instead. --baseline PATH adds a comparison
against a JSON file of the same shape (surface name -> definition array), for
example one captured from another revision; surfaces missing from it are
printed without a comparison.
Provider request envelopes/tokenizers are deliberately outside this estimate.
"""
import argparse
import json
import math
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
TESTS = [
    ("forge-agent-host", "serialized_tool_definitions"),
    ("mcp-server", "serialized_mcp_tool_definitions"),
]


def compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def definitions(log):
    result = {}
    for line in log.splitlines():
        if line.startswith("TOOL_DEFINITIONS "):
            _, name, raw = line.split(" ", 2)
            result[name] = json.loads(raw)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs", nargs="+", type=Path)
    parser.add_argument("--baseline", type=Path)
    args = parser.parse_args()
    current = {}
    if args.logs:
        for path in args.logs:
            current.update(definitions(path.read_text()))
    else:
        for crate, test in TESTS:
            command = ["cargo", "test", "-p", crate, "--lib", test, "--", "--nocapture"]
            run = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=True)
            found = definitions(run.stdout)
            if not found:
                raise SystemExit(f"{crate} {test}: no TOOL_DEFINITIONS lines captured")
            current.update(found)
    if not current:
        raise SystemExit("No TOOL_DEFINITIONS lines found")
    baseline = json.loads(args.baseline.read_text()) if args.baseline else None
    header = "surface tools bytes tokens"
    if baseline is not None:
        header += " baseline_bytes baseline_tokens saved_percent"
    print(header)
    for name, tools in current.items():
        size = len(compact(tools))
        row = f"{name} {len(tools)} {size} {math.ceil(size / 4)}"
        if baseline is not None:
            if name in baseline:
                old = len(compact(baseline[name]))
                row += f" {old} {math.ceil(old / 4)} {(old - size) * 100 / old:.1f}"
            else:
                row += " - - -"
        print(row)


if __name__ == "__main__":
    main()
