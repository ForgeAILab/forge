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
caches, including Smith's `~/.smith` credential store. A Smith harness can use
its own logged-in runtime, model, and discovered provider, but that does not
turn the harness credential into a Forge-native provider entry. Native Main and
Project Agent typed tools require a provider connected through Forge; embedding
Smith's host or importing its credentials would be a separate architecture
change, not an implicit fallback.

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
authentication state. Explicit negative status such as `Not logged in` wins
over executable detection, so an installed but unauthenticated harness is
shown as unavailable with its login recovery action. Creating an agent does not
select a chat binding or grant Project/Task authority. Main and Project Agent
Chat sessions remain filesystem-denied; only an identity admitted through the
existing Task Worker/reviewer assignment and workflow can receive a Task
Workspace.

To recover from an OAuth failure, cancel the visible operation and start a new
one. Disconnecting a credential immediately invalidates its local lease. Forge
reports whether remote provider revocation was `not_supported`, `succeeded`, or
`failed`; after a failure, also revoke the app in the provider's account
controls. Refresh tokens rotate inside encrypted storage and are never returned
by a read endpoint.

The `Providers` tab also provides reversible availability switches. Disable a
provider entry to disable every Agent using that credential, disable one exact
daemon/CLI runtime to stop only that installation, or pause an individual Agent
from the `Agents` tab. Re-enable the same row to restore it; configuration,
credentials, bindings, and history are preserved. Forge considers only enabled,
healthy sources when selecting Main, Project, Worker, or reviewer Agents.

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

## End-to-end: Main Chat to a traceable Task

This is the supported path for turning a rough idea into repository-backed work.
The web UI presents the same records and gates described below; the endpoint
names are included so that API clients can follow the same flow. A Main or
Project Agent Chat is a coordinator: it never receives a repository or
filesystem lease. Only a separately selected Task Worker or reviewer can work
in a Task Workspace.

### 1. Main Chat: rough idea → exact Charter

First connect a provider, create an account-owned agent, and bind it as the
account's Main Agent in Agent Settings. Then tell Main Chat naturally that you
want to begin a new Project—for example, “Help me start a Project for release
note automation.” The Main baseline recognizes clear new-Project intent and
invokes the typed `genesis.start` operation. If it is unclear whether you mean a
new Project or an existing one, the Main Agent asks one short clarifying
question; ordinary portfolio questions remain ordinary chat.

`/start-product <idea>` is still available as an explicit shortcut. It calls
`POST /api/v1/account/main-agent/product-genesis` with a fresh
`idempotency_key`; normal messages continue through normal Agent Chat admission,
with no browser-side intent matching. Without a valid Main binding the server
returns `setup_required` and creates no session or turn. Starting Product
Genesis begins discovery only—it does not approve a Charter or create a
Project.

The `forge.main.project-discovery/v2` skill keeps discovery bounded (at most two
consequential questions per turn) and records facts, decisions, research,
assumptions, and hypotheses separately. It proposes the Project name/mode,
scope, non-goals, success signal, constraints, and a Project Agent. Review the
rendered Charter, its content/render digests, and the selected identity's
current Profile. Do not treat a chat answer or a ready-looking brief as
approval: only the exact revision can be approved.

Click **Approve Charter** (or call
`POST /api/v1/account/main-agent/product-genesis/{session_id}/charter/revisions/{revision_id}/approve`)
for that revision. The receipt is single-use and binds the exact Charter
revision and digests, user, selected Project Agent identity/Profile,
operating-skill revision, policy digest, and expected version. Genesis approval
omits `expected_project_version` because there is not yet a Project.

### 2. Exact approval → atomic Project and handoff

Click **Create Project and hand off**. The API form is a typed
`CreateProjectFromCharterApproval` request to `POST /api/v1/projects`:

```json
{
  "approval_id": "<single-use-charter-approval>",
  "idempotency_key": "<new-key-for-this-create>",
  "authorization": { "<explicit-user-authorization-fields>": "..." }
}
```

`product_genesis_session_id` or a generic “ready” brief is not a substitute for
the receipt. One transaction creates the Project, Project Agent binding and
Chat, Charter attachment, immutable Main-to-Project handoff, target message and
turn, durable provisioning operation, events, and `handed_off` Genesis state.
The response includes the Project, binding, Chat, handoff, target-turn IDs, and
the current `execution_setup` projection. A response-loss retry with the same
key returns those original IDs and refreshes setup; it never creates another
Project, handoff, repository, or provisioning operation.

Use **Continue with Project Agent** to open the Project Chat. The handoff is
visible there as a bounded, provenance-linked message. It contains approved
discovery references, not credentials, hidden Main history, or Main authority.

Forge asks for two different user decisions during Product creation, not
repeated approval of the same thing:

1. **Approve the Project Charter** — approve this exact outcome, scope, and
   Project Agent selection.
2. **Create Project and hand off** — explicitly consume that receipt to create
   the Project and start its Project Agent.

After creation, the approved Charter is implementation authority. Forge does
not ask for setup, plan, or per-Task implementation approval. Each Task follows
its configured workflow and review mode.

### 3. Read the three readiness dimensions

Open the Project's **Execution readiness** panel or call
`GET /api/v1/projects/{id}/execution-setup`. Forge deliberately reports three
independent dimensions:

| Dimension | What it answers | Important states |
| --- | --- | --- |
| `coordination_state` | Can the singular Project Chat admit a turn? | `ready`, `setup_required`, `unavailable` |
| `execution_setup_state` | Is the repository ready? Project Worker/reviewer choices are optional defaults. | `provisioning`, `ready`, `setup_required`, `failed`, `unavailable` |
| `execution_gate` | Is Charter-backed execution available? Legacy baseline states are display compatibility only. | normally `active`; `unavailable` only when the projection cannot be read |

Project creation can therefore succeed while repository setup is
`provisioning` or `setup_required`. Project Agent coordination is useful but
optional for Task execution. Once the repository is ready, the approved Charter
authorizes implementation; Task assignments and workflow states decide what
runs. Check each dimension's `availability`; never infer a successful read from
another dimension.

### 4. Select Worker/reviewer defaults and a repository

The Project Agent can continue with documents and planning while setup is
incomplete. The Project owner/admin completes setup from the panel, or through
these owner/admin-only commands. Every write uses the `project_version` shown by
the latest projection and a fresh non-empty idempotency key:

```http
POST /api/v1/projects/{id}/execution-setup/worker
{"identity_id":"<worker>","expected_project_version":<version>,"idempotency_key":"..."}

POST /api/v1/projects/{id}/execution-setup/independent-reviewer
{"identity_id":"<reviewer>","expected_project_version":<version>,"idempotency_key":"..."}

POST /api/v1/projects/{id}/execution-setup/repository
{"repo_id":"<repo>","expected_project_version":<version>,"idempotency_key":"..."}
```

This setup path chooses optional defaults. Those selections seed Tasks; they do
not lock individual Task execution. On a Task, you may explicitly assign any
enabled, available configured Agent to Worker or reviewer, including the active
Main Agent, the Project Agent, and the same identity for both roles. Selecting
or confirming a role after that role's
attempt failed also starts one fresh attempt; there is no extra Resume approval.
A read-only reviewer who needs to make a correction still needs Worker
authority for the write.

A missing Worker or reviewer default does not keep the Project in setup. Assign
an enabled Agent on the Task when its workflow reaches that role. Project
defaults remain convenient bulk choices and can be changed at any time.

### 5. Repository provisioning and setup recovery

Genesis provisioning is a durable, checkpointed operation covering preflight,
filesystem initialization, repository registration, Project linkage, and role
assignment. While `execution_setup_state` is `provisioning`, the panel shows
the checkpoint and attempt count. Refresh
`GET /api/v1/projects/{id}/execution-setup`; provisioning is not success until
the server reports `ready`.

For a recorded failure, read `provisioning.last_error_code`,
`last_error_message`, `retryable`, `next_retry_at`, and `version`. Fix the
reported repository, filesystem, or eligibility problem, then retry the same
durable operation:

```http
POST /api/v1/projects/{id}/execution-setup/provisioning/retry
{"expected_operation_version":<provisioning-version>,"idempotency_key":"<new-retry-key>"}
```

Retries reconcile the deterministic directory, repository row, Project pointer,
and role assignments. They do not create a second repository or reset the
Project. If the finite retry budget is exhausted, setup is visibly `failed`
with a configuration/retry action; the approved Charter, Project Chat, and
handoff remain intact. A stale version or changed idempotency input is a
conflict: refetch the projection and use a new key, rather than guessing a
version.

### 6. Project Chat, current Profile, and the execution baseline

The Project binding names an identity, not a permanently frozen Profile. When a
new user message, handoff, retry, or autonomous wake is admitted, Forge resolves
that identity's current Profile, operating skill, and policy, then freezes those
revisions on the turn. Editing/selecting the Project Agent's Profile before the
next turn therefore affects that turn; a queued or leased turn keeps the Profile
that it was admitted with. This is why a Profile change does not require
rebinding the Project Chat.

The Project Agent acknowledges the handoff, chooses useful defaults, creates
the first Tasks, and lets their workflows dispatch. It may also draft an
execution baseline as an optional traceability snapshot linking the exact
Charter, Documents, plan items, milestones, acceptance/evidence, release policy,
and rollback information. That snapshot is useful for review and release
evidence, but it does not authorize implementation.

Baseline lifecycle is intentionally four separate operations:

1. **Save a draft.** `POST /api/v1/projects/{id}/execution-baseline` with
   `operation: "save_draft"` appends an immutable `draft` revision. It does not
   request or imply user authorization.
2. **Propose for approval.** The Project Agent calls
   `POST /api/v1/projects/{id}/execution-baseline/{baseline_id}/revisions` with
   `operation: "propose_for_approval"` and the current baseline version. A
   complete candidate returns `requires_user_authorization: true` and a frozen
   `approval_target` containing the exact revision and digests.
3. **Approve the exact revision.** The interactive user reviews that target and
   calls `POST .../revisions/{revision_id}/approve`. Approval is bound to the
   current Project version, content digest, render digest, and user receipt.
4. **Activate.** The same user calls `POST .../execution-baseline/{baseline_id}/activate`
   with the exact `baseline_id`, `revision_id`, `approval_id`, expected Project
   and baseline versions, and digests. Activation atomically advances the active
   traceability pointer and emits the activation event; it never changes Task
   runnable state. A Project Agent action or chat sentence cannot approve or
   activate a baseline.

The web interface combines steps 3 and 4 behind **Approve traceability plan**.
Skipping it does not stop Tasks. If the optional traceability revision cannot be
approved or activated, use its version/digest diagnostics without treating the
problem as an execution blocker:

- `baseline_approval_required`: a legacy projection may still report this;
  implementation continues from the Charter while the optional plan is reviewed.
- `version_conflict` or `digest_conflict`: refresh the Project/baseline
  projection, compare the current Charter, Documents, milestones, and setup,
  and re-propose from the current revision. Retry the same idempotency key only
  for a lost response to the identical command.
- `reconciliation_required`: open the plain-language reconciliation card. It
  states the replacement effect and offers **Accept** or **Reject**; technical
  record details stay collapsed.
- `setup_required` or `unavailable`: complete or refresh execution setup first;
  a missing Worker/reviewer/repository must not be disguised as a baseline
  problem.

### 7. Traceable Task execution, review, and Project Agent wake

After Project creation, the Project Agent creates implementation Tasks in the
bound Project through `POST /api/v1/projects/{id}/tasks`. Each Task is linked to
the current approved Charter. Baseline/revision, plan-item, milestone, and
Document links are optional traceability.

When repository setup, dependencies, assignment, workflow, source availability,
and the current Task version all pass, the
scheduler assigns the selected Worker and issues one Task-scoped Workspace
lease. `POST /api/v1/tasks/{id}/start`/`resume` and normal workflow scheduling
use the same admission checks. An Agent serving as Main or Project Agent may
also receive that lease when explicitly assigned, but the Task session is
isolated from its chat session. Inspect the linked Task, transitions, executions, and Workspace
diff through `GET /api/v1/tasks/{id}`, `GET /api/v1/tasks/{id}/transitions`,
`GET /api/v1/tasks/{id}/executions`, and
`GET /api/v1/tasks/{id}/diff`.

The normal workflow moves work through its configured active state into review
and delivery. Choose **Agent review** (`default`), **No review**, or **Human
required**; `autonomous_v1` remains a compatibility preset. Review guards run
the configured checks. In human-required mode, either the user or the bound
Project Agent may accept or reject. A Task may override the Project default. The
execution record exposes owner health, lease expiry/hard deadline, heartbeat,
and semantic progress separately. A quiet provider/tool call remains healthy
while its owner lease is current, and a hard deadline still bounds it.

Terminal execution events—including explicit stops and cancellations—feed
Attention and the durable `agent-wake-turns`
consumer. A relevant Project incident is recorded as exactly one of
`turn_admitted`, `deterministically_suppressed`, `deferred`, or
`setup_required`. An admitted wake uses the same responder resolver, current
Profile selection, canonical Project scope, and turn runner as a user message
or handoff; it cannot silently use a stale Profile or disappear because the
chat was temporarily unavailable. The Project Chat shows the resulting wake
turn, task outcome, review request, or durable retry/setup action.

The resulting audit trail is intentionally traceable: Charter revision and
digests → approval receipt → Project-creation confirmation/handoff/target turn
IDs → setup and provisioning operation → optional baseline revision/approval/activation → Task governance
and transitions → Worker execution/Workspace lease → checks/review → Attention
and wake disposition. Preserve these IDs when diagnosing a response loss or
conflict; they are the links between chat, Project truth, and repository work.

### Recovery quick reference

| Symptom | What it means | Safe next action |
| --- | --- | --- |
| `coordination_state: setup_required` | Main/Project binding or Chat admission is incomplete | Fix the authorized binding/Profile in Agent Settings, then refresh the Chat; no turn is fabricated. |
| Missing Worker | The Task's current workflow role has no enabled Agent assignment | Assign any enabled configured Agent on the Task; it may also be Main or Project Agent. |
| Missing reviewer default | No Project-wide reviewer default is selected | Nothing is blocked until a Task workflow needs the role; assign any enabled Agent on that Task, including its Worker. |
| `execution_setup_state: provisioning` | Durable setup is still reconciling | Refresh the projection; wait for `ready` or follow the recorded retry action. |
| `execution_setup_state: failed` | A checkpoint stopped with a typed error | Fix the recorded cause and retry the same provisioning operation with its current version and a new idempotency key. |
| `execution_gate: baseline_approval_required` | A legacy projection is showing optional plan review | Review it with **Approve traceability plan** if useful; Task execution already follows the approved Charter. |
| `version_conflict`, `digest_conflict`, or stale projection | Another command changed the authoritative revision | Refetch current state and re-propose/retry with the correct version; do not overwrite immutable history. |
| Wake `deferred` or `setup_required` | Delivery could not safely admit a turn yet | Follow the durable retry/setup action; the event remains traceable and is reconsidered after state changes. |
| `execution_gate: reconciliation_required` | Two traceability records disagree, or the active plan is invalid | Open Project Overview, read the one-sentence replacement effect, then choose **Accept** or **Reject**. Task-scoped conflicts remain scoped and do not freeze unrelated work. |
| An adaptive split/sequence/replace is needed | The Task shape no longer fits the work | The Project Agent can use any of the three operations under the current Charter; optional plan operation lists do not grant or deny them. |
| `validation_error` naming `adaptive_envelope.allowed_task_operations` | A plan tried to grant something outside `split`, `sequence`, `replace` | Correct the envelope to those verbs. Command names such as `task.propose` are not adaptive verbs. |
| A blocked Task showing "implementation committed" | Work is committed and waiting on review, not unstarted | Follow the single next action on the Task's blocker; progress language is derived from real attempt/commit evidence and never regresses to "not started". |

### After execution: milestones, readiness, and release evidence

Milestones are outcome contracts, not editable percentages. Their definition
revisions and live lifecycle are distinct. The Project Agent may request a
standalone readiness evaluation; Forge stores an immutable `ReadinessSnapshot`
and moves a successful unreleased milestone to `ready_for_release`. Readiness
creates no release pins. The user releases by naming the exact snapshot ID and
digest; Forge recomputes it inside the release transaction and creates one
immutable `Mxxx-rN` manifest plus evidence pins. A release is a frozen Forge
evidence record, not a deploy or merge. Corrections append a later revision.

Every required acceptance check also requires check-linked evidence. The
Project Overview shows the exact check rows and evidence coverage separately.
When a check is intentionally `manual`, an authenticated user records an
explicit Pass or Fail from the Overview after reviewing the immutable check;
Forge never treats Project-Agent narration as that attestation. A manual result
does not satisfy missing evidence. Automated, document, media, waiver, and Git
checks instead wait for their authoritative producer and do not expose the
manual-attestation control.

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
