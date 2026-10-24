#!/usr/bin/env python3
"""Check tracked Rust/TypeScript files against caps and a shrinking allowlist.

Run --update after shrinking or deleting an allowlisted file. Maintainers only:
--init creates the one-time baseline from scratch; never use it to accept growth.
"""

import argparse
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys


ALLOWLIST = "scripts/file-size-allowlist.txt"
HEADER = """# File-size ratchet: <max_lines> <path>, sorted by path.
# Never raise or add entries; split files instead.
# After shrinking/deleting files: python3 scripts/check-file-sizes.py --update
# Maintainers only: --init recreates the one-time baseline, never to accept growth.
"""
SPLIT_GUIDANCE = (
    "Split by responsibility into a module directory (Rust: keep foo.rs as the "
    "module root with foo/<part>.rs children; TS: sibling modules). "
    "Never use _part2-style files, and never raise or add an allowlist entry."
)


def cap_for(path):
    parts = PurePosixPath(path).parts
    if not parts or {"node_modules", "target", "dist"}.intersection(parts):
        return None
    if path.startswith("web/src/types/generated/bindings/"):
        return None
    suffix = PurePosixPath(path).suffix
    if parts[0] == "crates" and suffix == ".rs":
        return 2000
    if parts[0] == "web" and suffix in {".ts", ".tsx"}:
        return 1000
    return None


def source_counts(root):
    tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=root)
    counts = {}
    for raw in tracked.split(b"\0"):
        path = os.fsdecode(raw)
        if cap_for(path) is None:
            continue
        try:
            # Count newline bytes, like wc -l; an unterminated final line adds none.
            counts[path] = (root / path).read_bytes().count(b"\n")
        except FileNotFoundError:
            continue
    return counts


def read_allowlist(path):
    entries = {}
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        content = line.partition("#")[0].strip()
        if not content:
            continue
        fields = content.split(maxsplit=1)
        location = f"{ALLOWLIST}:{number}"
        if len(fields) != 2 or not fields[0].isascii() or not fields[0].isdigit():
            raise ValueError(f"{location}: expected <max_lines> <path>")
        maximum, name = int(fields[0]), fields[1].strip()
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts or str(relative) != name:
            raise ValueError(f"{location}: expected a repository-relative path")
        if name in entries:
            raise ValueError(f"{location}: duplicate entry for {name}")
        entries[name] = maximum
    return entries


def write_allowlist(path, entries):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(HEADER + "".join(
        f"{entries[name]} {name}\n" for name in sorted(entries)
    ), encoding="utf-8")


def violations(counts, entries):
    errors = []
    update = "Run python3 scripts/check-file-sizes.py --update."
    if list(entries) != sorted(entries):
        errors.append(f"{ALLOWLIST}: entries must be sorted by path. {update}")
    for path in sorted(counts.keys() | entries.keys()):
        count = counts.get(path)
        maximum = entries.get(path)
        if count is None:
            errors.append(
                f"{path}: 0 lines (deleted or no longer scanned), limit {maximum}; "
                f"stale allowlist entry. {update}"
            )
        elif maximum is not None:
            if count <= cap_for(path):
                errors.append(
                    f"{path}: {count} lines, limit {maximum} (allowlist), "
                    f"cap {cap_for(path)}; stale entry is within the cap. {update}"
                )
            elif count > maximum:
                errors.append(
                    f"{path}: {count} lines, limit {maximum} (allowlist); file grew."
                )
            elif count < maximum:
                errors.append(
                    f"{path}: {count} lines, limit {maximum} (allowlist); "
                    f"stale entry must ratchet down. {update}"
                )
        elif count > cap_for(path):
            errors.append(f"{path}: {count} lines, limit {cap_for(path)}; file exceeds cap.")
    return errors


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--update", action="store_true",
                       help="lower existing entries and drop resolved/deleted files; never add or raise")
    modes.add_argument("--init", action="store_true",
                       help="maintainers only: write the one-time baseline from scratch")
    args = parser.parse_args(argv)
    try:
        root = Path(subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True
        ).strip())
        counts = source_counts(root)
        path = root / ALLOWLIST
        if args.init:
            entries = {name: count for name, count in counts.items() if count > cap_for(name)}
            write_allowlist(path, entries)
        else:
            entries = read_allowlist(path)
            if args.update:
                entries = {
                    name: min(maximum, counts[name])
                    for name, maximum in entries.items()
                    if name in counts and counts[name] > cap_for(name)
                }
                write_allowlist(path, entries)
        if args.init or args.update:
            entries = dict(sorted(entries.items()))
        errors = violations(counts, entries)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"File-size guard failed: {error}", file=sys.stderr)
        return 1
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        print(SPLIT_GUIDANCE, file=sys.stderr)
        return 1
    rust = sum(cap_for(name) == 2000 for name in entries)
    action = "initialized" if args.init else "updated" if args.update else "checked"
    print(f"File-size guard OK: {len(counts)} source files checked; "
          f"{len(entries)} allowlist entries {action} ({rust} Rust, {len(entries) - rust} TS/TSX).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
