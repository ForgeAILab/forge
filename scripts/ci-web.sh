#!/usr/bin/env bash
# Runs every web gate in one pass and reports every failure it finds, matching
# scripts/ci-rust.sh. Install still aborts the run — nothing downstream can say
# anything useful without node_modules.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)/web"

if ! command -v pnpm >/dev/null 2>&1; then
  corepack enable
  corepack prepare pnpm@10 --activate
fi

pnpm install --frozen-lockfile

set +e
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

run_stage 'pnpm lint' pnpm lint
run_stage 'pnpm typecheck' pnpm typecheck
run_stage 'pnpm test' pnpm test
run_stage 'pnpm build' pnpm build

if ((${#failed[@]} > 0)); then
  printf '\nWeb CI failed (%d of 4 stages):\n' "${#failed[@]}"
  printf '  - %s\n' "${failed[@]}"
  exit 1
fi

printf '\nWeb CI passed (4 of 4 stages).\n'
