# Getting Started

This guide takes you from a blank machine to a real task driven through `todo → done`
against your own git repo.

## Install

### npm bootstrapper (macOS / Linux)

```bash
npx @forgeailab/forge --demo
```

The npm package is a small bootstrapper. It downloads the matching Forge GitHub
release archive for macOS, glibc Linux, or musl Linux, caches it under
`~/.forge/npx`, and starts the local server with the bundled web UI assets. The
browser does not open automatically; pass `--open` to opt in.

### Homebrew (macOS / Linux, recommended)

```bash
brew install forgeailab/tap/forge
```

The tap repo is [`ForgeAILab/homebrew-tap`](https://github.com/ForgeAILab/homebrew-tap).
The formula installs both `forge` and `forge-ctl` and places the web UI assets under
the Homebrew `share/forge` prefix.

### Install script (curl)

```bash
curl -fsSL https://raw.githubusercontent.com/ForgeAILab/forge/main/install.sh | bash
```

Or grab a tarball directly from [Releases](https://github.com/ForgeAILab/forge/releases).
Archives ship `forge`, `forge-ctl`, and the built web UI assets. The installer puts
the UI under `/usr/local/share/forge/web/dist` and selects the musl Linux archive
on musl-based systems such as Alpine. For a manual install, run `forge` from the
extracted archive root or set `FORGE_WEB_DIST_DIR` to the extracted `web/dist`
directory.

### Build from source

```bash
git clone https://github.com/ForgeAILab/forge.git
cd forge
cargo build
cargo run -p forge-cli         # plain start, data in ~/.forge/
cargo run -p forge-cli -- --demo  # seed labelled demo data (idempotent)
```

The embedded host pins Agent Runtime revision
`a7075b1d2dd1cee05db63bc480ff46b0f97ec239` and requires Rust 1.86 or newer.
Cargo fetches that revision normally. Contributors developing both repositories
side by side may add this gitignored local override to `.cargo/config.toml`:

```toml
[patch."https://github.com/ForgeAILab/agent-runtime.git"]
agent-runtime = { path = "../agent-runtime/crates/agent-runtime" }
agent-runtime-core = { path = "../agent-runtime/crates/agent-runtime-core" }
agent-runtime-lcm = { path = "../agent-runtime/crates/agent-runtime-lcm" }
```

Do not commit the local patch or replace the immutable dependency revision.

### Docker

```bash
docker compose up -d
# Forge available at http://localhost:8080
```

Data persists in the `forge-data` Docker volume. Set `RUST_LOG=debug` in
`docker-compose.yml` for verbose output.

## First boot

By default the server:

- Binds loopback on an OS-selected port the first time, then reuses that port
  from `~/.forge/server.json` on later starts.
- Creates `~/.forge/forge.db` (SQLite, WAL mode).
- Boots an embedded daemon that auto-registers and reports installed CLIs
  (`shell` always, plus `codex` / `claude_code` / `cursor` / `gemini` /
  `opencode` / `smith` when on `PATH`).
- Upserts default executor profiles from the adapter registry.

Open the `management_url` printed in the server logs for the web UI. For raw
API calls, set:

```bash
FORGE_URL=$(jq -r .server_url ~/.forge/server.json)
```

## Configuration

Precedence: **CLI flags > env vars > config file > defaults**.

```bash
cargo run -p forge-cli                          # plain start
cargo run -p forge-cli -- --demo                # seed demo data
cargo run -p forge-cli -- --no-embedded-daemon  # external daemon mode
cargo run -p forge-cli -- --no-mcp              # disable MCP endpoint
FORGE_DATA_DIR=./test cargo run -p forge-cli    # override data dir via env
```

Useful env vars: `FORGE_DATA_DIR`, `FORGE_WORKSPACE_ROOT`,
`FORGE_WORKSPACE_CLEANUP_DELAY_SECONDS`, `FORGE_PUBLIC_SEARCH_ENDPOINT`,
`FORGE_PUBLIC_SEARCH_TIMEOUT_MS`, `FORGE_PUBLIC_SEARCH_MAX_RESPONSE_BYTES`,
`FORGE_WEB_DIST_DIR`, `RUST_LOG`.

### Optional bounded public web search

Main and Project Agent Chats can use a direct `forge_public_web_search` tool
for quick public facts when a non-authenticated HTTPS endpoint is configured.
The endpoint is opt-in and receives only `q` and `limit` query parameters;
Forge sends no cookies, browser state, credentials, or filesystem data.
Configure it in `forge.yaml`:

```yaml
public_search:
  endpoint: https://search.example.test/forge
  timeout_ms: 5000
  max_response_bytes: 262144
```

The endpoint must return bounded JSON in the form
`{"results":[{"url":"https://…","title":"…","snippet":"…"}]}`.
Forge caps queries at 512 characters, results at 10, title/snippet lengths,
the response body, and the request deadline. Result text is marked as
untrusted external data and is never persisted as a user decision. Forge
disables redirects and ambient proxy/cookie/auth state, revalidates DNS
addresses at connect time, and rejects private, special-use, and
IPv4-mapped IPv6 targets. The tool is omitted when
`public_search.endpoint` is unset; invalid configuration is rejected before
startup. Direct `web.search` proposals are not persisted as `AgentAction`
rows.

The equivalent environment variables are `FORGE_PUBLIC_SEARCH_ENDPOINT`,
`FORGE_PUBLIC_SEARCH_TIMEOUT_MS`, and `FORGE_PUBLIC_SEARCH_MAX_RESPONSE_BYTES`.

JWT signing uses `server.jwt_secret` in the config file or `FORGE_JWT_SECRET`
when set. Otherwise Forge reads or creates `<data_dir>/jwt_secret.bin` on first
start (mode `0600` on Unix). Set an explicit secret in production deployments.

### Local development data dir

`make dev` and friends point data at `./test/` (gitignored) so dev state never
pollutes `~/.forge`. See the project [Makefile](../Makefile).

## Configuring agents

The embedded daemon auto-detects installed CLIs. Verify what's available:

```bash
curl -sS "$FORGE_URL/api/v1/daemons" | jq '.items[].cli_inventory'
```

Register an agent against one of the reported CLIs:

```bash
curl -sS -X POST "$FORGE_URL/api/v1/agents" \
  -H 'content-type: application/json' \
  -d '{
    "name": "claude-coder",
    "executor_type": "claude_code",
    "daemon_id": "<daemon-id-from-above>"
  }'
```

For Cursor, use `"executor_type": "cursor"`. Forge runs `cursor-agent` in
headless print mode with stream JSON output; set `CURSOR_API_KEY` or run
`cursor-agent login` first so the daemon reports it as authenticated.

The agent form discovers model and reasoning choices from the selected
adapter. Codex exposes the current GPT-5.6 family and model-specific effort
levels (including `max` and `ultra` where supported); Claude Code exposes the
current Claude 5 family and its supported `xhigh`, `max`, and `ultracode`
choices. The model field also accepts custom ids for provider-specific or
account-specific models. Gemini advertises the CLI's stable `auto`, `pro`,
`flash`, and `flash-lite` aliases alongside its current Gemini 3.x and 2.5
models. Cursor and OpenCode keep the custom-model field open because their
catalogs are provider- or installation-defined.

The `shell` executor is always available and useful for scripted tests — see the
walkthrough below.

## Connecting a direct embedded agent

Open **Agent Settings** (`/agents`) to choose a provider and a method advertised
by the server. Forge labels each method stable, experimental, or unavailable.
API keys remain the universal fallback. ChatGPT browser/device login and xAI
device login are experimental. Gemini uses Google's documented OAuth endpoints
only when `FORGE_GEMINI_OAUTH_CLIENT_ID` is configured; set
`FORGE_GEMINI_OAUTH_CLIENT_SECRET` too when the registered client requires it.
Forge does not import Codex, Grok CLI, Gemini CLI, or Code Assist credential
caches.

Browser login redirects through the exact configured CORS origin. Device login
shows a provider URL and user code while Forge polls a finite operation. Closing
the dialog does not broaden its lease; reopening Agent Settings shows the
terminal result after the provider callback. A successful login creates a
protected renewable **provider entry** — it does not create an agent and does
not bind anything. You can add the same provider more than once (for example
two OpenAI accounts); every entry appears on the Agent Settings `Providers`
tab with its usage.

Agents are created afterwards from an entry, on the `Agents` tab or over the
API. An entry stores the credential through a protected write-only boundary;
responses contain an opaque credential handle, bounded health, and
capabilities, never the credential value.

```bash
read -rsp 'Provider API key: ' PROVIDER_KEY; printf '\n'

# 1. Add a provider entry (stores the key; creates no agent).
ENTRY=$(curl -sS -X POST "$FORGE_URL/api/v1/providers" \
  -H 'content-type: application/json' \
  -d "$(jq -n --arg key "$PROVIDER_KEY" '{
    provider: "openai",
    label: "primary",
    credential: $key
  }')")
unset PROVIDER_KEY
ENTRY_ID=$(jq -r .id <<<"$ENTRY")

# 2. Create a direct agent that uses the entry.
CONNECTED=$(curl -sS -X POST "$FORGE_URL/api/v1/embedded-agents" \
  -H 'content-type: application/json' \
  -d "$(jq -n --arg entry "$ENTRY_ID" '{
    name: "my-forge-agent",
    description: "A persistent account assistant",
    credential_id: $entry,
    model: "gpt-5.6-terra"
  }')")

AGENT_ID=$(jq -r .agent.id <<<"$CONNECTED")
```

The same entry can also power a CLI harness: register a `codex` or `gemini`
agent with `credential_id` and Forge injects the key into the harness
environment at dispatch (`GET /api/v1/providers/catalog` declares which
combinations are supported). Harnesses without an entry keep using their own
CLI login, and the `Providers` tab lists those CLI runtimes with their
authentication state. Creating an agent does not select a chat binding or grant
Project/Task authority. Main and Project Agent Chat sessions remain
filesystem-denied; only an identity admitted through the existing Task
Worker/reviewer assignment and workflow can receive a Task Workspace.

To recover from an OAuth failure, cancel the visible operation and start a new
one. Disconnecting a credential immediately invalidates its local lease. Forge
reports whether remote provider revocation was `not_supported`, `succeeded`, or
`failed`; after a failure, also revoke the app in the provider's account
controls. Refresh tokens rotate inside encrypted storage and are never returned
by a read endpoint.

## Main Chat and the Project Agent Workspace

The approved product model has one global Main Agent binding/chat per account
and exactly one Project Agent binding/chat per operational Project. Main Chat
appears directly below the Project switcher. Each Project's **Agent Workspace**
keeps its durable conversation beside Project-record editing controls; on small
screens, use the Conversation/Project segments without losing either draft. A connected
but unbound identity stays available for later selection and does not appear as
an extra chat-switcher entry. The revised binding and chat resources are:

- Main binding: `/api/v1/account/main-agent`
- Project binding: `/api/v1/projects/{project_id}/project-agent`
- Chat switcher and messages: `/api/v1/agent-chats` and
  `/api/v1/agent-chats/{chat_id}/messages`
- Main-to-Project handoff: `/api/v1/projects/{project_id}/agent-handoffs`

These resources are the public `V071+` replacement contract. Do not build new
integrations against retired collaboration routes that may still exist as
historical source data in an upgraded database.

The Main Agent handles discovery, configured web search, Project lifecycle,
bounded portfolio summaries, and explicit handoff. It cannot create, edit,
assign, transition, review, merge, or deliver Tasks. The Project Agent manages
Tasks only in its bound Project through `TaskService`; repository changes still
happen only in admitted Task Worker/reviewer Workspaces. A handoff is an
immutable, bounded, provenance-linked packet with at most one target turn, not
shared hidden context or a second chat.

## Starting a Project from Main Chat

Use the global Main Chat for Product Genesis by typing an idea into the composer
and selecting "Start a product" or initiating discovery from the empty state.
Discovery creates a typed session with sensible defaults (`mvp` maturity, no
agent preference), and setup parameters can be tailored in-conversation through
the runtime questionnaire. The server-owned `forge.main.project-discovery/v2`
skill keeps the discovery turn bounded (at most two consequential questions),
separates facts from user decisions, research, assumptions, and hypotheses, and
maintains a revisioned Project Charter. It recommends the Project name, mode
(`compact` or `standard`), scope, non-goals, success signal, constraints, and
selected Project Agent; only the user approves the exact Charter revision.

The Main Chat shows the exact Charter content/render digests and approval target.
Approval creates a single-use receipt. Project creation then uses the typed
`CreateProjectFromCharterApproval` operation with that receipt and an
idempotency key. A ready Genesis brief or a generic
`product_genesis_session_id` is not enough. The atomic operation creates the
Project, Project Agent binding and Chat, Charter attachment, handoff, target
message/turn, events, and `handed_off` Genesis state together. If it fails,
Forge leaves Genesis `ready_for_project`, keeps the receipt active, and creates
no partial Project or handoff; retrying with the same key returns the original
result after a committed response loss.

Use the “Continue with Project Agent” action to enter the Project Chat. The
Project Agent acknowledges the approved Charter, avoids re-asking settled
questions, and creates only the typed Documents needed by the Project:
`research`, `delivery_brief`, `product_spec`, `design`, `architecture`, or
`execution_plan`. Before a repository-capable Task can run, the user approves
one exact execution baseline digest covering the governing artifacts,
acceptance/evidence matrix, risk/capability classes, release policy, and
adaptive envelope. The Project Agent can then manage Tasks inside that envelope;
repository access remains limited to scheduler-issued Task Workspaces.

Milestones are outcome contracts, not editable percentages. Their definition
revisions and live lifecycle are distinct. The Project Agent may request a
standalone readiness evaluation; Forge stores an immutable `ReadinessSnapshot`
and moves a successful unreleased milestone to `ready_for_release`. Readiness
creates no release pins. The user releases by naming the exact snapshot ID and
digest; Forge recomputes it inside the release transaction and creates one
immutable `Mxxx-rN` manifest plus evidence pins. A release is a frozen Forge
evidence record, not a deploy or merge. Corrections append a later revision.

For screenshots, videos, and reports, reuse existing Task media from the same
Project whenever possible. When it is reused, the Project evidence attachment
keeps the existing asset ID, Task media ID, Task URL, storage key, and file
bytes in place; it does not copy bytes or change the on-disk layout. Deleting
the Task makes its Task URL unavailable, but a release pin keeps the asset
referenced through the authorized Project evidence URL while the shared asset
remains available.
Evidence attachment metadata is `available`, `quarantined`, `redacted`, or
`purged`; removing an attachment marks it purged, and the Project media route
serves bytes only while the shared asset is available and authorized. Ordinary
garbage collection re-checks active attachments and immutable release pins
under a scheduler lease. The schema defines audited mandatory-purge tombstone
and `evidence_unavailable` semantics, and V076's internal repository persists
those audited projections. A Project owner/admin may use
`POST /api/v1/projects/{id}/media/{asset_id}/redact` or
`POST /api/v1/projects/{id}/media/{asset_id}/purge` with explicit user
authorization, the current asset version, an idempotency key, and a reason.
Redaction blocks serving through the Project media URL and renders pinned
release evidence unavailable; the legacy Task media URL keeps its existing
behavior while its Task attachment remains active. Purge also removes the
shared bytes, so neither former URL serves them. Neither disposition rewrites
the release manifest.

## Existing-data migration

The correction is forward-only. Migrations `V059`–`V070` remain unchanged; the
replacement begins at `V071` or later. Legacy conversation/collaboration
messages, IDs, ordering, ordinary bodies, provenance, runtime metadata,
sessions, LCM/memory references, and turn-job history are preserved. Multiple
source threads merge deterministically by timestamp, source ID, and source
sequence. If no single safe Main/Project binding can be inferred, Forge marks
the account or Project `agent_setup_required` instead of guessing or promoting
a Task Worker. Expired/ambiguous leases become finite retry or terminal states,
never silent success. V075 then quarantines the retired Room and membership
tables as `legacy_*`, converts Room-scoped semantic memory to Agent Chat scope,
and rejects any new Room authority record while retaining source provenance.

The Charter, Project artifact, milestone, release, and shared-media metadata
for this change are added by the forward-only
`V076__project_charter_milestones_media.sql` migration. V001–V075 remain
immutable; existing media IDs, URLs, storage keys, metadata, and file bytes are
preserved in place, with no file move/duplication or on-disk layout break.

Projects that predate the Charter model are explicitly
`legacy_unverified`/`charter_setup_required`; migration never fabricates an
approved Charter from old chat, Tasks, memory, or inferred names. The Project
Chat, Tasks, evidence capture, and Document maintenance remain usable. The
Project Agent may draft an adoption Charter from authorized current state, but
only explicit user approval of its exact revision establishes Project truth and
unblocks release. Existing task media IDs, URLs, storage keys, and file bytes
remain in place; migration does not move or duplicate files or claim an on-disk
layout break. If a migration or server restart fails, old media references and
bytes remain usable and physical cleanup is retried separately after checking
attachments and release pins.

## End-to-end walkthrough

This drives a task from `todo → done` against a real local repo, using the
`shell` executor so you don't need any AI CLI installed.

```bash
# 1. Create a project + repo pointing at a real git checkout.
PROJECT_ID=$(curl -sS -X POST "$FORGE_URL/api/v1/projects" \
  -H 'content-type: application/json' \
  -d '{"name":"demo"}' | jq -r .id)

curl -sS -X POST "$FORGE_URL/api/v1/projects/$PROJECT_ID/repos" \
  -H 'content-type: application/json' \
  -d '{"name":"my-repo","url":"/abs/path/to/repo","default_branch":"main"}'

# 2. Use the auto-reported daemon and register a shell agent.
DAEMON_ID=$(curl -sS "$FORGE_URL/api/v1/daemons" | jq -r '.items[0].id')
AGENT_ID=$(curl -sS -X POST "$FORGE_URL/api/v1/agents" \
  -H 'content-type: application/json' \
  -d "{\"name\":\"demo-agent\",\"executor_type\":\"shell\",\"daemon_id\":\"$DAEMON_ID\"}" \
  | jq -r .id)

# 3. Create a task with inline CI steps.
TASK_ID=$(curl -sS -X POST "$FORGE_URL/api/v1/projects/$PROJECT_ID/tasks" \
  -H 'content-type: application/json' \
  -d '{
    "title":"greet",
    "description":"echo hi > greeting.txt && git add . && git -c user.email=a@b -c user.name=a commit -m hi",
    "review_config":{"ci_steps":["test -f greeting.txt"]}
  }' | jq -r .id)

# 4. Claim the task — the executor auto-dispatches.
curl -sS -X POST "$FORGE_URL/api/v1/tasks/$TASK_ID/claim" \
  -H 'content-type: application/json' \
  -d "{\"agent_id\":\"$AGENT_ID\",\"overrides\":null}"

# 5. Transition to review. The review runner fires the CI steps inline and
#    returns {task, review} in one response.
curl -sS -X POST "$FORGE_URL/api/v1/tasks/$TASK_ID/transition" \
  -H 'content-type: application/json' \
  -d '{"status":"review","version":2}'

# 6. Transition to merging. The merge runs, the task auto-advances to done,
#    and the worktree is cleaned up synchronously.
curl -sS -X POST "$FORGE_URL/api/v1/tasks/$TASK_ID/transition" \
  -H 'content-type: application/json' \
  -d '{"status":"merging","version":3}'
```

The same flow is exercised end-to-end by `cargo test -p api --test happy_path`.

## Using `forge-ctl`

For interactive work, the CLI is friendlier than raw curl:

```bash
printf '%s\n' "$FORGE_PASSWORD" | forge-ctl login \
  --email you@example.com \
  --password-stdin

forge-ctl project create --name "My Project"
forge-ctl task list --project-id <ID>
forge-ctl agent register --name "Claude" --executor-type shell

# Create a task, claim it, follow the SSE stream until terminal state:
forge-ctl run --project <ID> --repo <ID> --agent <ID> \
              --title "fix login bug" \
              --description "patch the session handler"
# Exits 0 on done; 1 on blocked / merge_failed / cancelled.
```

Full CLI reference → [docs/cli.md](cli.md).

## Linking an external daemon

`forge-ctl daemon link` registers the current machine with a running Forge
server, saves daemon credentials, reports local CLI availability, and keeps
sending heartbeats. While it is running, it also keeps the daemon command
stream open so Forge can browse local paths and dispatch agents on that
machine. Forge marks the daemon offline when that command stream disconnects,
and after a server restart until the daemon reconnects.
In the web UI: **Daemons → Link daemon** generates a token and prints the full
command:

```bash
forge-ctl daemon link \
  --token fg_... \
  --workspace-root "$HOME/.forge/workspaces"
```

The token is used only for initial ownership; the daemon receives and stores its
own registration token afterward. Use `--once` for a one-shot
registration/report only; `--once` does not keep the command stream open for
filesystem browsing or execution dispatch.

After the first link, restart the daemon from its saved credentials with:

```bash
forge-ctl daemon start \
  --workspace-root "$HOME/.forge/workspaces"
```

`daemon start` does not register or claim the daemon again; it just reports
local CLI availability and keeps the command stream open. `daemon link` and
`daemon start` create the configured workspace root if it does not already
exist, so filesystem browsing can open the launch directory immediately.

Execution dispatch expects the server-created task worktree to exist at the same
absolute path on the daemon host. For containers, mount the server workspace
root into the container at that same path. A daemon on an unrelated filesystem
can still serve filesystem browsing under its own `--workspace-root`, but it
cannot run server-created task worktrees yet.

## Where to next

- **API surface** → [api.md](api.md)
- **How it's wired together** → [architecture.md](architecture.md)
- **Run agents from your AI tooling** → [api.md#mcp-tools](api.md#mcp-tools)
- **Contribute** → [../CONTRIBUTING.md](../CONTRIBUTING.md)
