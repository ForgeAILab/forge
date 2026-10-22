#!/usr/bin/env bash
# Runs every Rust gate in one pass and reports every failure it finds.
#
# The old script was `set -e` plus a fail-fast `cargo test`, so a red run named
# exactly one breakage and hid the rest: fmt hid clippy, clippy hid the suite,
# and the first failing test binary hid every later one. Three separate
# failures queued up behind each other that way while cutting v0.11.0, costing
# a full CI round-trip each to discover. Now a stage failing only records the
# failure, and the run ends with the complete list.
#
# `cargo check` is deliberately absent: `clippy --workspace --all-targets` with
# the workspace features already type-checks every target, and the test build
# compiles them again.
# With no stage skipped any more, that third pass was pure wall clock.
#
# Usage: ci-rust.sh               every stage, whole workspace (local default)
#        ci-rust.sh lint          fmt + clippy
#        ci-rust.sh test <shard>  the tests of one shard
# CI runs `lint` and one `test <shard>` per shard as parallel jobs. The shards
# are defined in scripts/ci_rust_shards.py, which also checks that together
# they cover every workspace package, services unit tests and API targets.
set -uo pipefail

mode="${1:-all}"
shard="${2:-}"
# Targets with `required-features` (the services, services-agents and db
# integration tests) are skipped without a word when the feature is absent, so
# every whole-workspace command names them. `ci_rust_shards.py check` fails
# unless this line lists exactly the features `cargo metadata` reports as
# required.
workspace_features=(--features db/test-template,services-agents/test-support,services/test-support)
test_select=(--workspace --all-targets "${workspace_features[@]}")
test_filters=()
case "$mode" in
  all | lint)
    if (($# > 1)); then
      echo "usage: $0 [lint | test <shard>]" >&2
      exit 2
    fi
    ;;
  test)
    if (($# != 2)); then
      echo "usage: $0 [lint | test <shard>]" >&2
      exit 2
    fi
    # No `mapfile`: this must run under the macOS system bash (3.2).
    selection="$(python3 "$(dirname "$0")/ci_rust_shards.py" args "$shard")" || exit 2
    test_select=()
    seen_separator=0
    while IFS= read -r arg; do
      if [[ $arg == -- && $seen_separator == 0 ]]; then
        seen_separator=1
      elif ((seen_separator)); then
        test_filters+=("$arg")
      else
        test_select+=("$arg")
      fi
    done <<<"$selection"
    ;;
  *)
    echo "usage: $0 [lint | test <shard>]" >&2
    exit 2
    ;;
esac

export FORGE_SKIP_WEB_BUILD=1
# GitHub runner images ship git-lfs, whose global `filter.lfs.*` entries make
# the hardened commit finalization fail closed (by design for real users) and
# whose other global settings leak into fixtures. Run the suite against an
# empty global config and no system config so hosts cannot change test
# outcomes. Fixtures pin `init -b main` and set repo-local identity, so they
# need nothing from the host.
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_NOSYSTEM=1
# The workspace has many integration-test binaries. Full debug
# info overflows the hosted runner's disk and the linker dies with a bus error.
# Line tables keep file:line in panics and backtraces at a fraction of the size.
# In GitHub Actions the job-level `env:` of .github/workflows/ci.yml sets these
# for every cargo invocation of the job (this script and `make types`), so all
# of them share one set of build fingerprints; the defaults here are for runs
# outside it.
export CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-line-tables-only}"
export CARGO_PROFILE_TEST_DEBUG="${CARGO_PROFILE_TEST_DEBUG:-line-tables-only}"

failed=()

run_stage() {
  local name="$1"
  shift
  printf '\n=== %s ===\n' "$name"
  if "$@"; then
    printf '=== %s: ok ===\n' "$name"
  else
    failed+=("$name")
    printf '=== %s: FAILED ===\n' "$name"
  fi
}

stages=0
if [[ $mode == all || $mode == lint ]]; then
  stages=$((stages + 2))
  run_stage 'cargo fmt' cargo fmt --all -- --check
  run_stage 'cargo clippy' cargo clippy --workspace --all-targets "${workspace_features[@]}" \
    -- -D warnings
fi
if [[ $mode == all || $mode == test ]]; then
  if [[ -n ${CI_RUST_TIMINGS:-} ]]; then
    # One-off measurement: build the test binaries as a stage of their own and
    # write target/cargo-timings/. The test stage below reuses the build.
    stages=$((stages + 1))
    run_stage 'cargo test build' cargo test "${test_select[@]}" --no-run --timings
  fi
  stages=$((stages + 1))
  # --no-fail-fast: keep going across test binaries so one red crate does not
  # mask the others. libtest already reports every failure within a binary.
  # The conditional array expansion also handles empty filters in bash 3.2.
  run_stage 'cargo test' cargo test "${test_select[@]}" --no-fail-fast -- \
    --test-threads=4 ${test_filters[@]+"${test_filters[@]}"}
fi

if ((${#failed[@]} > 0)); then
  printf '\nRust CI failed (%d of %d stages):\n' "${#failed[@]}" "$stages"
  printf '  - %s\n' "${failed[@]}"
  if [[ -n ${GITHUB_ACTIONS:-} ]]; then
    for stage in "${failed[@]}"; do
      printf '::error title=Rust CI::%s failed\n' "$stage"
    done
  fi
  exit 1
fi

printf '\nRust CI passed (%d of %d stages).\n' "$stages" "$stages"
