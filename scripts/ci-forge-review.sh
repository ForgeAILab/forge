#!/usr/bin/env bash
# Runs both suites and reports both outcomes. Rust failing used to abort before
# web ran, so a local `make ci` — like CI itself — surfaced one half at a time.
set -uo pipefail

failed=()

./scripts/ci-rust.sh || failed+=('rust')
./scripts/ci-web.sh || failed+=('web')

if ((${#failed[@]} > 0)); then
  printf '\nForge review CI failed: %s\n' "${failed[*]}"
  exit 1
fi

printf '\nForge review CI passed.\n'
