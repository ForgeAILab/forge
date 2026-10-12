#!/usr/bin/env python3
"""The Rust test shards CI runs, and the proof that they cover the workspace.

This file defines every shard's package, target and libtest selection:

    ci_rust_shards.py args <shard>   cargo and libtest selection, one argument per
                                     line (read by scripts/ci-rust.sh)
    ci_rust_shards.py check          fail unless every workspace package is
                                     covered once (services lib and api targets are partitioned),
                                     every target with `required-features` is
                                     selected with those features, and the CI
                                     matrix runs exactly these shards

`check` only runs `cargo metadata --no-deps`; it compiles nothing.
"""

import json
import pathlib
import re
import subprocess
import sys

# Complementary filters share one list of substrings: unit-a runs every
# services unit test whose name contains any of them (libtest ORs positional
# filters), unit-b skips every one of them (libtest ANDs --skip), so no test is
# lost or run twice.
# The last CI runs with `task_service::` alone took about 390s (408 tests) and
# 475s (1813 tests). `task_dispatcher::` and `workspace_root::` were added
# when unit-a was bounded by one serial test,
# task_service::tests::condition_matrix::condition_offer_apply_dispatch_matrix
# (about 300s on one thread, three threads mostly idle). That test and the
# other serial loops are now parallel cases (longest case about 25s), and a
# shard's wall time is its summed per-test time over four threads again.
# Measured locally with four threads, one run each on a shared machine:
# 348s / 198s before the split, 176s (689 tests) / 204s (1542 tests) after.
# Per-test sums of one four-thread run of all 2231 tests (411s): 793s / 847s.
# No module moves that closer: `usage_projection::` (60s) gives 853s / 788s,
# and the smaller ones change less than a run varies. What bounds unit-b is
# the `integration_steps::` and `integration_worker::` modules (515s).
SERVICES_UNIT_FILTERS = ["task_service::", "task_dispatcher::", "workspace_root::"]
SERVICES_UNIT_A = ["-p", "services", "--lib", "--", *SERVICES_UNIT_FILTERS]
SERVICES_UNIT_B = ["-p", "services", "--lib", "--",
                   *[arg for name in SERVICES_UNIT_FILTERS for arg in ("--skip", name)]]
# Targets with `required-features` are skipped without a word when the feature
# is absent and the target is not named (`--test '*'`, `--all-targets`), so
# each shard that selects one passes the feature; `check` proves it against
# `cargo metadata`. The unit shards must not: with no feature and no self
# dev-dependency, `--lib` compiles the services lib once (the test lib) instead
# of twice in series.
SERVICES_INTEGRATION = ["-p", "services", "--features", "test-support",
                        "--test", "*", "--examples"]
SHARDS = {
    "services-unit-a": SERVICES_UNIT_A,
    "services-unit-b": SERVICES_UNIT_B,
    # --tests includes the lib again; --test '*' selects integration bins only.
    # Retain the examples that --all-targets previously built (no lib rerun).
    "services-integration": SERVICES_INTEGRATION,
    "api-a": ["-p", "api"],
    "api-b": ["-p", "api"],
    "db": ["-p", "db", "--features", "test-template", "--all-targets"],
    # Catch-all by exclusion: a new workspace crate needs no edit here.
    "rest": None,
}

# Green CI 38227423555 measured model_workflow at 304.87s and all other API
# targets at 384.67s. After consolidation, local successful runs at 4d7d91c9
# predict 262.28s / 228.86s with four threads (13.6% apart). Each target uses
# its mean successful duration; model and plan each have only one such run.
# The model alone exceeds the remaining work, so this is the optimal binary
# partition. Keep failure evidence separate from the duration calculation.
# API targets are assigned by binary, never by a libtest substring. Metadata
# coverage below rejects a new/renamed/missing target until it is assigned.
API_TARGETS = {
    "api-a": ["model_workflow"],
    "api-b": ["main", "daemon_workspace_roundtrip", "daemon_plan_transport",
              "environment_provisioning", "environment_surface_lists",
              "integration_queue_task_summary", "task_list_read_path"],
}
# Non-integration targets (including future bins/examples/benches) run once.
API_OTHER_TARGETS_SHARD = "api-b"

ROOT = pathlib.Path(__file__).resolve().parent.parent
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
CI_SCRIPT = ROOT / "scripts" / "ci-rust.sh"


def named_packages():
    return sorted({args[1] for args in SHARDS.values() if args is not None})


def cargo_args(shard):
    args = SHARDS[shard]
    if shard in API_TARGETS:
        args = list(args)
        for target in API_TARGETS[shard]:
            args += ["--test", target]
        if shard == API_OTHER_TARGETS_SHARD:
            args += ["--lib", "--bins", "--examples", "--benches"]
        return args
    if args is not None:
        return list(args)
    args = ["--workspace", "--all-targets"]
    for pkg in named_packages():
        args += ["--exclude", pkg]
    return args


def cargo_part(args):
    """The arguments cargo reads: everything before the libtest separator."""
    return args[:args.index("--")] if "--" in args else list(args)


def passed_features(args, package=None):
    """The `package/feature` names a cargo selection enables."""
    args = cargo_part(args)
    names = set()
    for index, arg in enumerate(args):
        if arg == "--features" and index + 1 < len(args):
            value = args[index + 1]
        elif arg.startswith("--features="):
            value = arg.split("=", 1)[1]
        else:
            continue
        for name in re.split(r"[ ,]+", value):
            if name:
                names.add(name if "/" in name or package is None else f"{package}/{name}")
    return names


def required_features(workspace):
    """Every `package/feature` some workspace target cannot be built without."""
    return sorted({f"{pkg}/{feature}" for pkg, data in workspace.items()
                   for target in data["targets"]
                   for feature in target.get("required-features", [])})


def workspace_feature_args(workspace):
    """Feature arguments of a whole-workspace build that skips no target."""
    names = required_features(workspace)
    return ["--features", ",".join(names)] if names else []


def selects_only_lib(args):
    args = cargo_part(args)
    return "--lib" in args and not any(
        arg in ("--test", "--tests", "--all-targets", "--examples", "--example",
                "--bins", "--bin", "--benches", "--bench") for arg in args)


def feature_errors(workspace):
    """A gated target cargo would skip in silence: a shard without its feature."""
    errors = []
    for pkg, data in workspace.items():
        for target in data["targets"]:
            needed = {f"{pkg}/{feature}" for feature in target.get("required-features", [])}
            if not needed:
                continue
            if target["kind"] == ["lib"]:
                errors.append(f"{pkg} lib must not have required-features")
                continue
            for shard in SHARDS:
                if pkg not in selected(shard, workspace):
                    continue
                args = cargo_args(shard)
                if selects_only_lib(args):
                    continue
                named = None if SHARDS[shard] is None else pkg
                missing = sorted(needed - passed_features(args, named))
                if missing:
                    errors.append(f"{shard} selects {pkg} target '{target['name']}' "
                                  f"without --features {','.join(missing)}: "
                                  "cargo would skip it without an error")
    # The unit shards exist to compile the services lib once. A feature there
    # is harmless to coverage but brings back nothing; a self dev-dependency
    # brings the second, serial lib build back.
    for pkg, data in workspace.items():
        for dep in data.get("dependencies", []):
            if dep.get("name") == pkg and dep.get("kind") == "dev":
                errors.append(f"{pkg} dev-depends on itself: `cargo test -p {pkg} --lib` "
                              "would compile the lib twice in series; use "
                              "required-features on its targets instead")
    return errors


def selected(shard, workspace):
    """The packages cargo tests for this shard's arguments."""
    args = SHARDS[shard]
    if args is None:
        return set(workspace) - set(named_packages())
    return {args[1]}


def coverage_errors(workspace):
    errors = []
    # The two selections must remain the exact filter/skip partition. The
    # integration selection must never grow --tests or --all-targets.
    filters = SERVICES_UNIT_FILTERS
    if not filters or len(set(filters)) != len(filters) or any(
            not name or name.startswith("-") for name in filters):
        errors.append(f"services unit filters must be distinct test-name substrings: {filters}")
    skips = []
    for name in filters:
        skips += ["--skip", name]
    expected = {
        "services-unit-a": ["-p", "services", "--lib", "--", *filters],
        "services-unit-b": ["-p", "services", "--lib", "--", *skips],
        "services-integration": ["-p", "services", "--features", "test-support",
                                 "--test", "*", "--examples"],
    }
    for shard, args in expected.items():
        if SHARDS.get(shard) != args:
            errors.append(f"{shard} must select {args}")
    errors.extend(feature_errors(workspace))
    for pkg, data in workspace.items():
        owners = [s for s in SHARDS if pkg in selected(s, workspace)]
        if pkg == "services":
            if set(owners) != set(expected):
                errors.append(f"services must be covered by {list(expected)}, got {owners}")
            # Fail closed if services adds a bin or benchmark: the
            # lib/integration/examples partition would otherwise omit it.
            for target in data["targets"]:
                if target["kind"] not in (["lib"], ["test"], ["example"], ["custom-build"]):
                    errors.append(f"uncovered services target: {target['name']} {target['kind']}")
        elif pkg == "api":
            if set(owners) != set(API_TARGETS):
                errors.append(f"api must be covered by {list(API_TARGETS)}, got {owners}")
            if API_OTHER_TARGETS_SHARD not in owners:
                errors.append("api non-integration targets must have one owning shard")
            targets = {t["name"] for t in data["targets"] if t["kind"] == ["test"]}
            assigned = [name for names in API_TARGETS.values() for name in names]
            for name in sorted(targets | set(assigned)):
                count = assigned.count(name)
                if name not in targets or count != 1:
                    errors.append(f"api integration target '{name}' has {count} assignments "
                                  f"(exists: {name in targets})")
            # Refuse unexpected cargo selection instead of trusting a package
            # match that could include every test again or silently drop one.
            for shard in API_TARGETS:
                if SHARDS.get(shard) != ["-p", "api"]:
                    errors.append(f"{shard} must select only the assigned api targets")
            for target in data["targets"]:
                if target["kind"] not in (["lib"], ["test"], ["bin"], ["example"],
                                           ["bench"], ["custom-build"]):
                    errors.append(f"uncovered api target: {target['name']} {target['kind']}")
        elif len(owners) != 1:
            errors.append(f"package '{pkg}' is covered by {len(owners)} shards: {owners}")
        elif "--all-targets" not in cargo_args(owners[0]):
            errors.append(f"package '{pkg}' must include --all-targets")
    return errors


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


def script_errors(workspace):
    """scripts/ci-rust.sh lints and (with no shard) tests the whole workspace."""
    wanted = " ".join(workspace_feature_args(workspace))
    line = f"workspace_features=({wanted})"
    if line not in CI_SCRIPT.read_text().splitlines():
        return [f"{CI_SCRIPT.name} must contain the line `{line}` so that clippy and the "
                "whole-workspace test run build every target with required-features"]
    return []


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

    errors.extend(coverage_errors(workspace))
    errors.extend(script_errors(workspace))
    rows = []
    for pkg in sorted(workspace):
        owners = [s for s in SHARDS if pkg in selected(s, workspace)]
        kinds = [k for t in workspace[pkg]["targets"] for k in t["kind"]]
        rows.append((pkg, ",".join(owners) or "-", kinds.count("test")))

    matrix = workflow_shards()
    if matrix is None:
        errors.append(f"no 'shard: [..]' matrix line found in {WORKFLOW}")
    elif sorted(matrix) != sorted(SHARDS) or len(matrix) != len(set(matrix)):
        errors.append(
            f"CI matrix runs {matrix}, but the shards defined here are {list(SHARDS)}"
        )

    print(f"{'package':<22}{'shard(s)':<57}integration test targets")
    for pkg, owner, tests in rows:
        print(f"{pkg:<22}{owner:<57}{tests}")
    for shard in SHARDS:
        print(f"{shard} selects: {' '.join(cargo_args(shard))}")

    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
            print(f"::error title=Rust test shards::{error}")
        return 1
    gated = [f"{pkg}:{target['name']}" for pkg, data in sorted(workspace.items())
             for target in data["targets"] if target.get("required-features")]
    print(f"ok: {len(rows)} workspace packages, covered without overlap across "
          f"{len(SHARDS)} shards (services lib filter/skip; api target partition); "
          f"{len(gated)} feature-gated targets selected with their features "
          f"({', '.join(gated)})")
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
