#!/usr/bin/env python3
"""The Rust test shards CI runs, and the proof that they cover the workspace.

This file is the only place a shard's package list is written down:

    ci_rust_shards.py args <shard>   cargo package selection, one argument per
                                     line (read by scripts/ci-rust.sh)
    ci_rust_shards.py check          fail unless every workspace package is
                                     tested by exactly one shard and the CI
                                     matrix runs exactly these shards

`check` only runs `cargo metadata --no-deps`; it compiles nothing.
"""

import json
import pathlib
import re
import subprocess
import sys

# Shard name -> packages. One shard is the catch-all (None): it takes every
# workspace package no other shard names, so a new crate is tested without an
# edit here. The three named crates hold most of the test time; see docs/ci.md.
SHARDS = {
    "services": ["services"],
    "api": ["api"],
    "db": ["db"],
    "rest": None,
}

ROOT = pathlib.Path(__file__).resolve().parent.parent
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def named_packages():
    return [pkg for pkgs in SHARDS.values() if pkgs is not None for pkg in pkgs]


def cargo_args(shard):
    packages = SHARDS[shard]
    args = []
    if packages is None:
        args.append("--workspace")
        for pkg in named_packages():
            args += ["--exclude", pkg]
    else:
        for pkg in packages:
            args += ["-p", pkg]
    return args


def selected(shard, workspace):
    """The packages cargo tests for this shard's arguments."""
    packages = SHARDS[shard]
    if packages is None:
        return set(workspace) - set(named_packages())
    return set(packages)


def workspace_packages():
    out = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    meta = json.loads(out)
    members = set(meta["workspace_members"])
    return {p["name"]: p for p in meta["packages"] if p["id"] in members}


def workflow_shards():
    match = re.search(r"^\s*shard:\s*\[([^\]]*)\]\s*$", WORKFLOW.read_text(), re.M)
    if match is None:
        return None
    return [name.strip() for name in match.group(1).split(",") if name.strip()]


def check():
    errors = []
    workspace = workspace_packages()

    catch_alls = [name for name, pkgs in SHARDS.items() if pkgs is None]
    if len(catch_alls) != 1:
        errors.append(f"expected exactly one catch-all shard, found {catch_alls}")

    # `cargo test --exclude <unknown>` is only a warning, so a renamed crate
    # would silently move into the catch-all shard. Refuse it here.
    for pkg in named_packages():
        if pkg not in workspace:
            errors.append(f"shard package '{pkg}' is not a workspace package")

    rows = []
    for pkg in sorted(workspace):
        owners = [s for s in SHARDS if pkg in selected(s, workspace)]
        if len(owners) != 1:
            errors.append(f"package '{pkg}' is covered by {len(owners)} shards: {owners}")
        kinds = [k for t in workspace[pkg]["targets"] for k in t["kind"]]
        rows.append((pkg, ",".join(owners) or "-", kinds.count("test")))

    matrix = workflow_shards()
    if matrix is None:
        errors.append(f"no 'shard: [..]' matrix line found in {WORKFLOW}")
    elif sorted(matrix) != sorted(SHARDS) or len(matrix) != len(set(matrix)):
        errors.append(
            f"CI matrix runs {matrix}, but the shards defined here are {list(SHARDS)}"
        )

    print(f"{'package':<22}{'shard':<10}integration test targets")
    for pkg, owner, tests in rows:
        print(f"{pkg:<22}{owner:<10}{tests}")
    for shard in SHARDS:
        print(f"{shard} selects: {' '.join(cargo_args(shard))}")

    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
            print(f"::error title=Rust test shards::{error}")
        return 1
    print(f"ok: {len(rows)} workspace packages, each in exactly one of {len(SHARDS)} shards")
    return 0


def main(argv):
    if len(argv) == 3 and argv[1] == "args":
        if argv[2] not in SHARDS:
            print(f"unknown shard '{argv[2]}'; known: {', '.join(SHARDS)}", file=sys.stderr)
            return 2
        print("\n".join(cargo_args(argv[2])))
        return 0
    if len(argv) == 2 and argv[1] == "check":
        return check()
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
