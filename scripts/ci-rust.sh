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
# `cargo check` is deliberately absent: `clippy --workspace --all-targets`
# already type-checks every target, and the test build compiles them again.
# With no stage skipped any more, that third pass was pure wall clock.
set -uo pipefail

export FORGE_SKIP_WEB_BUILD=1

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

run_stage 'cargo fmt' cargo fmt --all -- --check
run_stage 'cargo clippy' cargo clippy --workspace --all-targets -- -D warnings
# --no-fail-fast: keep going across test binaries so one red crate does not
# mask the others. libtest already reports every failure within a binary.
run_stage 'cargo test' cargo test --workspace --all-targets --no-fail-fast

if ((${#failed[@]} > 0)); then
  printf '\nRust CI failed (%d of 3 stages):\n' "${#failed[@]}"
  printf '  - %s\n' "${failed[@]}"
  if [[ -n ${GITHUB_ACTIONS:-} ]]; then
    for stage in "${failed[@]}"; do
      printf '::error title=Rust CI::%s failed\n' "$stage"
    done
  fi
  exit 1
fi

printf '\nRust CI passed (3 of 3 stages).\n'
