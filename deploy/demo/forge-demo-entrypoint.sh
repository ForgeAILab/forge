#!/usr/bin/env bash
# Entrypoint of the public demo image. See docs/demo-instance.md.
#
# Every container start is a new, seeded instance:
#   1. empty the data directory
#   2. start forge on LOOPBACK only and wait for /healthz
#   3. register the administrator, then the test account, check both, and add
#      the test account to the seeded Demo project
#   4. stop that forge, write the marker file
#   5. exec forge on the public address
#
# Forge does not listen on a reachable address before step 5, so nobody can
# register first and become the administrator, whatever the proxy in front
# does with a container that is still starting. Any failure before step 5
# exits non-zero without the marker and without ever listening publicly.
#
# The passwords arrive in the environment. They are moved into shell
# variables and removed from the environment before any child starts, are
# sent only through bash's own network redirection (no curl, no argv), and
# are never printed.

set -euo pipefail
# Byte semantics: ${#body} must be the Content-Length.
export LC_ALL=C

log() { printf '[demo-seed] %s\n' "$*"; }
die() {
  printf '[demo-seed] ERROR: %s\n' "$*" >&2
  exit 1
}

# --- configuration --------------------------------------------------------

forge_bin="${DEMO_FORGE_BIN:-forge}"
data_dir="${DEMO_DATA_DIR:-/data}"
marker="${DEMO_MARKER_FILE:-/tmp/forge-demo-seeded}"
port="${DEMO_PORT:-8080}"
seed_timeout="${DEMO_SEED_TIMEOUT_SECONDS:-180}"
# Seconds one HTTP request may take before it counts as unanswered.
request_timeout=15
project_role="${DEMO_TEST_PROJECT_ROLE-member}"
admin_name="${DEMO_ADMIN_NAME:-Demo Admin}"
test_name="${DEMO_TEST_NAME:-Demo Tester}"

rm -f "$marker"

for name in DEMO_ADMIN_EMAIL DEMO_ADMIN_PASSWORD DEMO_TEST_EMAIL DEMO_TEST_PASSWORD; do
  [[ -n "${!name:-}" ]] || die "$name is not set"
done
admin_email="$DEMO_ADMIN_EMAIL"
admin_password="$DEMO_ADMIN_PASSWORD"
test_email="$DEMO_TEST_EMAIL"
test_password="$DEMO_TEST_PASSWORD"
# From here on no child process, forge included, has the passwords in its
# environment.
unset DEMO_ADMIN_PASSWORD DEMO_TEST_PASSWORD

# The values pass through an env file that Compose interpolates, where `$`,
# quotes and `#` change a value. Refusing them here turns a silently altered
# password into a clear error.
safe='^[A-Za-z0-9._~+=,-]{8,128}$'
[[ "$admin_password" =~ $safe ]] || die "DEMO_ADMIN_PASSWORD must be 8-128 characters from A-Z a-z 0-9 . _ ~ + = , -"
[[ "$test_password" =~ $safe ]] || die "DEMO_TEST_PASSWORD must be 8-128 characters from A-Z a-z 0-9 . _ ~ + = , -"
email='^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$'
[[ "$admin_email" =~ $email ]] || die "DEMO_ADMIN_EMAIL is not a plain email address"
[[ "$test_email" =~ $email ]] || die "DEMO_TEST_EMAIL is not a plain email address"
lower() { printf '%s' "$1" | tr '[:upper:]' '[:lower:]'; }
[[ "$(lower "$admin_email")" != "$(lower "$test_email")" ]] || die "DEMO_ADMIN_EMAIL and DEMO_TEST_EMAIL must differ"
for name in admin_name test_name; do
  [[ "${!name}" != *[[:cntrl:]]* ]] || die "a display name contains a control character"
done
[[ -z "$project_role" || "$project_role" =~ ^(owner|admin|member|viewer)$ ]] ||
  die "DEMO_TEST_PROJECT_ROLE must be owner, admin, member, viewer or empty"
[[ "$seed_timeout" =~ ^[0-9]+$ ]] || die "DEMO_SEED_TIMEOUT_SECONDS must be a number"
[[ "$port" =~ ^[0-9]+$ ]] || die "DEMO_PORT must be a number"
[[ "$data_dir" == /* && "$data_dir" != / ]] || die "DEMO_DATA_DIR must be an absolute path other than /"

forge_args=(--data-dir "$data_dir" --demo --no-embedded-daemon --no-mcp)

# --- HTTP over bash /dev/tcp (the image has no curl) ----------------------

http_status=""
http_body=""

# http <method> <path> [bearer token] [json body]
# Sets http_status and http_body; returns non-zero when no complete response
# arrived within $request_timeout seconds. A server that accepts the
# connection and never answers must not hang the container start.
http() {
  local method="$1" path="$2" token="${3:-}" body="${4:-}" response
  http_status=""
  http_body=""
  response="$(
    {
      reply=""
      read_status=0
      printf '%s %s HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n' "$method" "$path" >&3
      if [[ -n "$token" ]]; then
        printf 'Authorization: Bearer %s\r\n' "$token" >&3
      fi
      if [[ -n "$body" ]]; then
        printf 'Content-Type: application/json\r\nContent-Length: %d\r\n' "${#body}" >&3
      fi
      printf '\r\n%s' "$body" >&3
      # Reads to end of stream (the server closes: HTTP/1.0). `read` returns
      # 1 at end of stream with the text in place, and more than 128 when the
      # timeout ran out first.
      IFS= read -r -d '' -t "$request_timeout" reply <&3 || read_status=$?
      ((read_status <= 128)) || exit 1
      printf '%s' "$reply"
    } 2>/dev/null 3<>"/dev/tcp/127.0.0.1/$port"
  )" || return 1
  [[ "$response" =~ ^HTTP/[0-9.]+\ ([0-9]{3}) ]] || return 1
  http_status="${BASH_REMATCH[1]}"
  http_body="${response#*$'\r\n\r\n'}"
}

json_string() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "$value"
}

# The server's error code, never the request.
api_error() {
  if [[ "$http_body" =~ \"code\":\"([A-Za-z0-9_]+)\" ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
  else
    printf 'no error code'
  fi
}

# register <label> <email> <password> <display name>
register() {
  local label="$1" body
  body="{\"email\":$(json_string "$2"),\"password\":$(json_string "$3"),\"display_name\":$(json_string "$4")}"
  http POST /api/v1/auth/register "" "$body" || die "registering the $label account: no response"
  case "$http_status" in
    201) log "registered the $label account" ;;
    409) die "registering the $label account: the email already exists, so the data directory was not empty" ;;
    *) die "registering the $label account failed: HTTP $http_status ($(api_error))" ;;
  esac
}

login_token=""
login_user_id=""

# verify_login <label> <email> <password> <expected is_admin: true|false>
verify_login() {
  local label="$1" expect_admin="$4" body token
  body="{\"email\":$(json_string "$2"),\"password\":$(json_string "$3")}"
  http POST /api/v1/auth/login "" "$body" || die "signing in as the $label account: no response"
  [[ "$http_status" == 200 ]] || die "signing in as the $label account failed: HTTP $http_status ($(api_error))"
  [[ "$http_body" =~ \"access_token\":\"([^\"]+)\" ]] || die "signing in as the $label account returned no access token"
  token="${BASH_REMATCH[1]}"
  http GET /api/v1/auth/me "$token" || die "reading the $label account: no response"
  [[ "$http_status" == 200 ]] || die "reading the $label account failed: HTTP $http_status ($(api_error))"
  [[ "$http_body" =~ \"is_admin\":[[:space:]]*$expect_admin ]] ||
    die "the $label account has is_admin != $expect_admin"
  [[ "$http_body" =~ \"id\":\"([0-9a-fA-F-]+)\" ]] || die "reading the $label account returned no user id"
  login_token="$token"
  login_user_id="${BASH_REMATCH[1]}"
  log "verified the $label account (is_admin=$expect_admin)"
}

# The first registered user becomes the owner of the seeded Demo project, and
# a project is visible to its owner and members only.
# share_demo_project <admin token> <test token> <test user id> <role>
share_demo_project() {
  local admin_token="$1" test_token="$2" test_user_id="$3" role="$4" project_id
  http GET /api/v1/projects "$admin_token" || die "listing projects: no response"
  [[ "$http_status" == 200 ]] || die "listing projects failed: HTTP $http_status ($(api_error))"
  [[ "$http_body" =~ \{\"id\":\"([0-9a-fA-F-]+)\",\"name\":\"Demo\" ]] || die "the seeded Demo project was not found"
  project_id="${BASH_REMATCH[1]}"
  http POST "/api/v1/projects/$project_id/members" "$admin_token" \
    "{\"user_id\":\"$test_user_id\",\"role\":\"$role\"}" || die "adding the test account to the Demo project: no response"
  [[ "$http_status" == 200 || "$http_status" == 201 ]] ||
    die "adding the test account to the Demo project failed: HTTP $http_status ($(api_error))"
  http GET /api/v1/projects "$test_token" || die "listing projects as the test account: no response"
  [[ "$http_status" == 200 && "$http_body" == *'"name":"Demo"'* ]] ||
    die "the test account still cannot see the Demo project"
  log "the test account is a $role of the Demo project"
}

# --- 1. empty data directory ----------------------------------------------

mkdir -p "$data_dir"
# A directory that is a mount point of its own (the log directory) cannot be
# removed, only emptied; what matters is that no file survives.
find "$data_dir" -mindepth 1 -delete 2>/dev/null || true
[[ -z "$(find "$data_dir" -mindepth 1 ! -type d -print -quit)" ]] || die "could not empty $data_dir"

# The root filesystem is read-only in the demo. HOME is where git and
# `bash -lc` look for and keep their files; give them the temp filesystem.
export HOME="${DEMO_HOME:-/tmp/home}"
mkdir -p "$HOME"

# Forge has a tuned default log filter. Replace it only on request.
unset RUST_LOG
if [[ -n "${DEMO_RUST_LOG:-}" ]]; then
  export RUST_LOG="$DEMO_RUST_LOG"
fi

# --- 2. forge on loopback -------------------------------------------------

forge_pid=""
stop_forge() {
  local waited=0
  if [[ -n "$forge_pid" ]] && kill -0 "$forge_pid" 2>/dev/null; then
    kill -TERM "$forge_pid" 2>/dev/null || true
    # A forge that does not stop within 30s is killed: hanging here would
    # leave the container unhealthy for ever without saying why.
    while kill -0 "$forge_pid" 2>/dev/null; do
      if ((waited >= 30)); then
        log "forge did not stop within 30s; killing it"
        kill -KILL "$forge_pid" 2>/dev/null || true
        break
      fi
      sleep 1
      waited=$((waited + 1))
    done
    wait "$forge_pid" 2>/dev/null || true
  fi
  forge_pid=""
}
# Until the final exec, a failure or a stop signal takes the loopback forge
# down with it.
trap stop_forge EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

log "starting forge on 127.0.0.1:$port to create the accounts"
env -u LC_ALL FORGE_SERVER_BIND="127.0.0.1:$port" "$forge_bin" "${forge_args[@]}" &
forge_pid=$!

deadline=$((SECONDS + seed_timeout))
request_timeout=5
until http GET /healthz && [[ "$http_status" == 200 ]]; do
  kill -0 "$forge_pid" 2>/dev/null || die "forge exited while starting"
  ((SECONDS < deadline)) || die "forge did not answer /healthz within ${seed_timeout}s"
  sleep 1
done
request_timeout=15
log "forge is healthy"

# --- 3. accounts ----------------------------------------------------------

# Order matters: the first registered user is the administrator.
register administrator "$admin_email" "$admin_password" "$admin_name"
register test "$test_email" "$test_password" "$test_name"
verify_login administrator "$admin_email" "$admin_password" true
admin_token="$login_token"
verify_login test "$test_email" "$test_password" false
if [[ -n "$project_role" ]]; then
  share_demo_project "$admin_token" "$login_token" "$login_user_id" "$project_role"
else
  log "DEMO_TEST_PROJECT_ROLE is empty: the test account is not added to the Demo project"
fi
unset admin_password test_password admin_token login_token

# --- 4. stop, mark --------------------------------------------------------

log "accounts are in place; restarting forge on the public address"
stop_forge
if http GET /healthz; then
  die "the loopback forge is still answering after it was stopped"
fi
: >"$marker"

# --- 5. public forge ------------------------------------------------------

trap - EXIT TERM INT
# Forge gets the locale the image was started with, not this script's.
unset LC_ALL
FORGE_SERVER_BIND="0.0.0.0:$port" exec "$forge_bin" "${forge_args[@]}"
