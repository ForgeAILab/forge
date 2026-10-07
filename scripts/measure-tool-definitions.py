#!/usr/bin/env python3
"""Measure real compact tool-definition arrays, with ceil(UTF-8 bytes / 4) tokens.

Run from the repository root with the normal Cargo environment. With no log
arguments, runs just the two serialization tests. --logs consumes their saved
--nocapture output instead. The before fixture was captured at 6695a256 using
these same compositions/serializers before production code was changed.
Provider request envelopes/tokenizers are deliberately outside this estimate.
"""
import argparse
import json
import math
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


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
    args = parser.parse_args()
    before = json.loads((ROOT / "crates/agent-host/tests/fixtures/tool_definitions_before_normalization.json").read_text())
    mcp = json.loads((ROOT / "crates/mcp-server/tests/fixtures/mcp_tool_definitions.json").read_text())
    before.update(mcp)
    after = {}
    if args.logs:
        for path in args.logs:
            after.update(definitions(path.read_text()))
    else:
        for crate, test in [("forge-agent-host", "serialized_tool_definitions"), ("mcp-server", "serialized_mcp_tool_definitions")]:
            command = ["cargo", "test", "-p", crate, "--lib", test, "--", "--nocapture"]
            run = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=True)
            after.update(definitions(run.stdout))
    if set(before) != set(after):
        raise SystemExit(f"Incomplete captures: expected {sorted(before)}, got {sorted(after)}")
    print("surface tools before_bytes before_tokens after_bytes after_tokens saved_percent")
    for name, old in before.items():
        new = after[name]
        old_bytes, new_bytes = len(compact(old)), len(compact(new))
        if name in mcp and compact(old) != compact(new):
            raise SystemExit(f"{name}: MCP definitions changed")
        print(f"{name} {len(new)} {old_bytes} {math.ceil(old_bytes / 4)} {new_bytes} {math.ceil(new_bytes / 4)} {(old_bytes - new_bytes) * 100 / old_bytes:.1f}")


if __name__ == "__main__":
    main()
