# forge-ctl

`forge-ctl` is the CLI client for the Forge REST API. The server must be
running first. By default, `forge-ctl` uses the server from the stored CLI
login, then falls back to the server URL persisted by the last `forge` launch
under the Forge data directory.

## Global flags

```text
--server <URL>            Forge server URL  (default: stored login, then local server)
--output <FORMAT>         table | json      (default: table)
```

## Subcommands

| Command | What it does |
|---------|--------------|
| `login`   | Authenticate the CLI and store a reusable token |
| `logout`  | Remove stored CLI credentials |
| `whoami`  | Show stored CLI login state |
| `project` | Create / list / show projects |
| `repo`    | Add / list repos under a project |
| `memory`  | Search and retrieve project-scoped memory |
| `task`    | Create, list, show, transition, cancel, archive tasks, preview prompts |
| `agent`   | Register / list / show agents |
| `embedded` | Manage provider entries, embedded agents, profiles/sessions, singular bindings/chats, and handoffs |
| `daemon`  | Link, start, and report an external daemon |
| `run`     | Create + claim a task and follow the SSE stream until terminal state |
| `mcp`     | Helpers for the MCP JSON-RPC endpoint |

Use `forge-ctl <command> --help` for the full set of flags on each subcommand.

## Common flows

### Authenticate the CLI

`forge-ctl login` exchanges your account credentials for a CLI personal access
token and stores it under the Forge data directory. Later commands, including
`forge-ctl mcp install`, reuse that stored token automatically for the same
server URL.

When run in a terminal, `forge-ctl login` prompts for the password without
displaying it. For scripts or piped input, pass `--password-stdin`; an implicit
password prompt fails with guidance when standard input is not a terminal.

```bash
printf '%s\n' "$FORGE_PASSWORD" | forge-ctl login \
  --email you@example.com \
  --password-stdin

forge-ctl whoami
```

Use `forge-ctl logout` to remove the local credentials file.

### Quick scripted run

```bash
forge-ctl run --project <ID> --repo <ID> --agent <ID> \
              --title "fix login bug" \
              --description "patch the session handler"
# Exits 0 on done; 1 on blocked / merge_failed / cancelled.
```

This creates the task, claims it (which auto-dispatches the executor), then
streams events until the task reaches a terminal state. Useful in CI or shell
pipelines.

### Manual task management

```bash
forge-ctl project create --name "My Project"
forge-ctl repo create --project-id <ID> --name "main-repo" \
                      --kind local --local-path /abs/path/to/repo \
                      --default-branch main

forge-ctl agent register --name "Claude" --executor-type shell

forge-ctl task list --project-id <ID>
forge-ctl task show <TASK_ID>
forge-ctl task prompt-preview <TASK_ID> --role coder
forge-ctl task cancel <TASK_ID>
```

`task prompt-preview` is read-only. Add `--trigger accept|reject|fail|retry`
to preview the prompt for a transition target instead of the task's current
state.

### Linking an external daemon

`forge-ctl daemon link` registers the current machine with a running Forge
server, saves daemon credentials, reports installed CLI inventory, keeps
sending heartbeats, and serves filesystem and execution commands over the
daemon command stream. In the web UI: **Daemons → Link daemon** generates the
token and prints the full command:

```bash
forge-ctl daemon link \
  --token fg_... \
  --workspace-root "$HOME/.forge/workspaces"
```

The token is used only for initial ownership; the daemon receives and stores
its own registration token afterward. Add `--once` for a one-shot
registration/report that does not keep the command stream open.
The configured workspace root is created automatically before the daemon
registers or reports.

After a daemon has been linked once, use `forge-ctl daemon start` to run it
again from the saved daemon credentials without registering or claiming it
again:

```bash
forge-ctl daemon start \
  --workspace-root "$HOME/.forge/workspaces"
```

`daemon start` keeps the same heartbeat and command stream open as `daemon
link`. Use `daemon report` only for a one-shot status update; it does not keep
the command stream open.
Forge marks the daemon offline when that command stream disconnects, and uses
stream heartbeats to keep the daemon's last-seen timestamp fresh while it is
connected. When the Forge server starts, external daemons are considered
offline until their command stream reconnects.

Execution dispatch requires the task worktree path created by the server to
exist at the same absolute path on the daemon host. Use a local daemon or mount
the server workspace root into the daemon host/container at the same path. A
daemon on an unrelated filesystem can browse its own `--workspace-root`, but it
cannot run server-created task worktrees yet.

### Installing MCP client config

`forge-ctl mcp install` writes the Forge MCP URL into a supported MCP client
config file. MCP requests require authentication; after `forge-ctl login`, the
stored CLI token is used automatically. You can still pass `--token` or set
`FORGE_TOKEN` to override the stored token:

```bash
forge-ctl mcp install --agent claude
forge-ctl mcp install --agent codex --project-id <PROJECT_ID>
forge-ctl mcp install --agent cursor --scope user --token fg_...
```

Supported agents are `claude`, `codex`, and `cursor`. Supported config scopes
are `project`, `local`, and `user`; the optional `--project-id` scopes MCP tool
calls to one Forge project.

### Direct embedded agents, bindings, and Agent Chats

`forge-ctl embedded` manages account-owned provider entries, embedded agents,
and their scope-bound native sessions. Adding a provider stores its credential
as an entry and never creates an agent; creating an agent references an entry;
neither creates Main or Project authority — select that explicitly through a
singular binding. Provider credentials are accepted only through a hidden
terminal prompt or `--credential-stdin` and are never printed by the CLI.

```bash
# Add an API-key provider entry (the credential is prompted for)
forge-ctl embedded provider add --provider openai --label "primary"

# Pipe a credential without putting it in shell history or process arguments
printf '%s\n' "$OPENAI_API_KEY" | forge-ctl embedded provider add \
  --provider openai --label "primary" --credential-stdin

# Sign in with OAuth from this machine (see "OAuth logins" below)
forge-ctl embedded provider login --provider openai --label "chatgpt"
forge-ctl embedded provider login --provider openai --method device

forge-ctl embedded provider list
forge-ctl embedded provider rename <ENTRY_ID> --label "work" --version <VERSION>
forge-ctl embedded provider remove <ENTRY_ID> --version <VERSION>

# Create a direct agent on an entry
forge-ctl embedded create \
  --name "Forge Assistant" \
  --credential-id <ENTRY_ID> \
  --model gpt-5.6

forge-ctl embedded profile list <IDENTITY_ID>
forge-ctl embedded profile connect <IDENTITY_ID> --version <VERSION> \
  --credential-id <ENTRY_ID> --model gpt-5.6
forge-ctl embedded profile select <IDENTITY_ID> <PROFILE_ID> --version <VERSION>

# Every session names one canonical scope; only Task scopes can receive a workspace.
forge-ctl embedded session create <IDENTITY_ID> --scope main \
  --chat-id <MAIN_CHAT_ID>
forge-ctl embedded session create <IDENTITY_ID> --scope project \
  --chat-id <PROJECT_CHAT_ID>
forge-ctl embedded session create <IDENTITY_ID> --scope task \
  --task-id <TASK_ID> --role worker
forge-ctl embedded session list <IDENTITY_ID>
forge-ctl embedded session rotate <SESSION_ID> --version <VERSION>
forge-ctl embedded session suspend <SESSION_ID> --version <VERSION>
forge-ctl embedded session resume <SESSION_ID> --version <VERSION>
forge-ctl embedded session cancel <SESSION_ID>
forge-ctl embedded session steer <SESSION_ID> "Use the latest accepted requirement"
forge-ctl embedded session effective-permissions \
  --identity-id <IDENTITY_ID> --scope project --chat-id <PROJECT_CHAT_ID>
```

#### OAuth logins

Some providers' OAuth clients whitelist only a `localhost` callback — OpenAI's
Codex client accepts `http://localhost:1455/auth/callback` (or `:1457`) and
nothing else. The listener therefore has to run on the machine the browser runs
on:

| Where Forge runs | What to use |
| --- | --- |
| Same machine as the browser | The web UI's **Continue with ChatGPT**. Forge binds the callback port for the duration of the ceremony. |
| Another host | `forge-ctl embedded provider login`. The CLI binds the port locally and relays only the authorization code to the server. |
| No browser available | `--method device`, which prints a code to enter elsewhere. |

`login` never sees the PKCE verifier or the resulting tokens: the server keeps
both and performs the exchange, exactly as it does for the web flow. Browser
login from a remote origin is rejected with an error pointing here, because no
listener could answer the callback.

Main and Project bindings are singular, versioned resources. A binding names
only the agent — turns follow the agent's current settings, so editing the
agent applies without rebinding. Replacing a binding preserves the existing
Agent Chat and historical attribution. A missing binding leaves the chat
available for setup but admits no model turn until a new binding is selected.

```bash
forge-ctl embedded main get
forge-ctl embedded main set <IDENTITY_ID> --version <VERSION>

forge-ctl embedded project get <PROJECT_ID>
forge-ctl embedded project set <PROJECT_ID> <IDENTITY_ID> --version <VERSION>
```

Agent Chats are singular timelines: one global Main Chat and one Project Agent
Chat per authorized Project. Chat reads expose bounded provenance and finite
turn state. Sending a message admits the responder from the server-side binding;
the CLI never supplies an authority identity.

```bash
forge-ctl embedded chat list --limit 50
forge-ctl embedded chat get <CHAT_ID>
forge-ctl embedded chat messages <CHAT_ID> --limit 50
forge-ctl embedded chat messages <CHAT_ID> \
  --before-sequence <SEQUENCE> --limit 50
forge-ctl embedded chat send <CHAT_ID> "Summarize the accepted requirements" \
  --dedupe-key <DEDUPE_KEY>
```

Main-to-Project handoffs are explicit, immutable, bounded publications. The
server guards source references, records provenance, and schedules at most one
Project Agent turn. A repeated dedupe key returns the original outcome.

```bash
forge-ctl embedded handoff list <PROJECT_ID> --limit 50
forge-ctl embedded handoff get <PROJECT_ID> <HANDOFF_ID>
forge-ctl embedded handoff create <PROJECT_ID> \
  --content "Approved brief and next steps" \
  --source-message-id <MESSAGE_ID> \
  --source-turn-job-id <TURN_JOB_ID> \
  --dedupe-key <DEDUPE_KEY>
```

Context inspection is metadata-only. The server returns source IDs, revisions,
selection reasons, dispositions, and fingerprints; it does not return source
fragments, protected checkpoints, secrets, or inaccessible memory bodies.

```bash
forge-ctl embedded context list <IDENTITY_ID> --limit 20
forge-ctl embedded context list <IDENTITY_ID> \
  --context-scope-id <CONTEXT_SCOPE_ID>
forge-ctl embedded context get <MANIFEST_ID> \
  --identity-id <IDENTITY_ID> --context-scope-id <CONTEXT_SCOPE_ID>
```

Provider entry disconnect (`embedded provider remove`) uses optimistic
concurrency. Pass the entry `version` returned by `provider list`; a stale
version is rejected instead of revoking a connection changed by another
session. Removal reports the agents that referenced the entry — they become
visibly unhealthy and are never silently rebound.

Commitments are durable identity-owned obligations. Create/list operations use
the identity path and an explicitly authorized canonical scope; lifecycle
mutations require the optimistic `--version` returned by the previous response.
Completion requires evidence, and transfer/cancellation require a reason.

```bash
forge-ctl embedded commitment list <IDENTITY_ID> \
  --scope-type project --scope-id <PROJECT_ID> --limit 50
forge-ctl embedded commitment create <IDENTITY_ID> \
  --scope-type project --scope-id <PROJECT_ID> \
  --title "Deliver the accepted plan" --correlation-id <CORRELATION_ID>
forge-ctl embedded commitment get <COMMITMENT_ID>
forge-ctl embedded commitment update <COMMITMENT_ID> \
  --version <VERSION> --status blocked \
  --blocked-reason "Waiting for review" --reason "Dependency" \
  --dedupe-key <DEDUPE_KEY>
forge-ctl embedded commitment complete <COMMITMENT_ID> \
  --version <VERSION> --evidence-type task-delivery \
  --evidence-id <EVIDENCE_ID> --dedupe-key <DEDUPE_KEY>
forge-ctl embedded commitment transfer <COMMITMENT_ID> \
  --version <VERSION> --to-identity-id <IDENTITY_ID> \
  --reason "Reassigning ownership" --dedupe-key <DEDUPE_KEY>
forge-ctl embedded commitment cancel <COMMITMENT_ID> \
  --version <VERSION> --reason "No longer required" \
  --dedupe-key <DEDUPE_KEY>
forge-ctl embedded commitment evidence <COMMITMENT_ID>
```

Use `--output json` for machine-readable responses. Nested profile, session,
binding, chat, handoff, context-manifest, and commitment resources are emitted as JSON
even with the default table output so provenance, lifecycle, and capability
fields are not lost.

### JSON output for scripting

```bash
forge-ctl --output json task list --project-id <ID> | jq '.items[].title'
```

Every subcommand respects `--output json` and emits the same payload structure
the REST API does — the tables shown in the default mode are just a render of
that JSON.
