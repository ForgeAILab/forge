#!/usr/bin/env bash
# Container health check of the demo image: healthy only when the entrypoint
# has finished creating the accounts (marker file) AND forge answers /healthz.
set -euo pipefail

marker="${DEMO_MARKER_FILE:-/tmp/forge-demo-seeded}"
port="${DEMO_PORT:-8080}"

[[ -f "$marker" ]] || exit 1
status="$(
  {
    line=""
    printf 'GET /healthz HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n' >&3
    # Bounded: a forge that accepts and never answers is unhealthy.
    IFS= read -r -t 4 line <&3 || true
    printf '%s' "$line"
  } 2>/dev/null 3<>"/dev/tcp/127.0.0.1/$port"
)" || exit 1
[[ "$status" =~ ^HTTP/[0-9.]+\ 200 ]]
