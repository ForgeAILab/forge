# forge-ctl

`forge-ctl` is the CLI client for the Forge REST API. The server must be
running first. By default, `forge-ctl` uses the server from the stored CLI
login, then falls back to the server URL persisted by the last `forge` launch
under the Forge data directory.

`forge-ctl --version` reports the workspace release version, matching the
`forge` server built from the same source checkout.

## Install

`forge-ctl` ships in every Forge distribution channel next to the `forge`
server, so installing Forge installs the CLI:

| Channel | Command | Result |
| --- | --- | --- |
| Homebrew | `brew install forgeailab/tap/forge` | `forge` and `forge-ctl` on `PATH` |
| npm (global) | `npm install -g @forgeailab/forge` | `forge` and `forge-ctl` shims that fetch the matching release archive on first run |
| npx (no install) | `npx -p @forgeailab/forge forge-ctl <command>` | same bootstrapper, one-off |
| Install script | `curl -fsSL https://raw.githubusercontent.com/ForgeAILab/forge/main/install.sh \| bash` | binaries under `${PREFIX:-/usr/local}/bin` |

`npx @forgeailab/forge ctl <command>` is equivalent to the `-p` form. The npm
bootstrapper caches release archives under `~/.forge/npx`; `--release <tag>` or
`FORGE_NPX_TAG` pins a specific release.

### From a source checkout

```bash
cargo install --path crates/forge-client --locked   # forge-ctl only
make install-local                                  # forge, forge-ctl, forge-solo
```

Both put binaries in `~/.cargo/bin`. Installing `forge` or `forge-solo` from
source runs the `pnpm` web build unless `FORGE_SKIP_WEB_BUILD=1` is set, and a
`forge` built that way needs `FORGE_WEB_DIST_DIR` at runtime if the UI assets
are not alongside it.

A source install and a Homebrew install can coexist; `PATH` order decides which
one runs. Check with `which -a forge-ctl` and compare `forge-ctl --version`
against the running server.

## forge-solo

`forge-solo` is a separate additive binary for one existing Git repository. It
composes the Forge core in-process and opens a keyboard-driven TUI; it does not
start `forge`, open a TCP listener, serve web assets, expose MCP, perform OAuth
callbacks, connect a remote daemon, or use the `forge-ctl` HTTP client. Its
state is isolated from the normal server store.

### Invocation

```text
forge-solo [PATH] [--data-dir PATH] [--agent EXECUTOR]
forge-solo --help
forge-solo --version
```

`PATH` is optional and defaults to `.`. Solo resolves a nested path to the
canonical primary Git worktree and rejects non-Git directories, linked
worktrees, and Forge-managed Task worktrees before writing state. The exact
`--agent` values are `codex`, `claude_code`, `cursor`, `opencode`, `gemini`,
and `smith`; each must pass the existing structured availability and
authentication check. `shell`, `embedded`, and `null` are not user-selectable
Solo chat harnesses. With no `--agent`, one eligible harness is preselected
but still confirmed, while several candidates are shown in a picker. A failed
health check is never replaced silently.

`--data-dir PATH` selects an exact repository-scoped Solo data root for tests
or recovery. The default is `<Forge data root>/solo/<repository-id>/`, with
the repository identifier read from the owner-only
`<git-common-dir>/forge-solo-id` marker. The marker format is:

```text
version = 1
repository_id = "<uuid>"
```

The data root contains the ordinary Forge SQLite database, protected state,
JSONL logs, media, generated worktrees, and `runtime.lock`; nothing except the
small marker is written into the tracked checkout. A malformed marker, a
repository/data-root mismatch, or an active lock fails closed. Solo starts
ordinary crash recovery before enabling the composer.

### Model providers in the config file

Solo can run without any local CLI harness login by reading provider
declarations from the user-owned Forge config file (`~/.forge/forge.yaml`).
Each provider sets a supported `kind` (`openai`, `openai_compatible`,
`openrouter`, `xai`, or `gemini`), exactly one of `api_key` / `api_key_env`,
an optional `base_url` (required for `openai_compatible`), and a list of
models. Solo materializes them as protected provider entries plus direct
embedded Agents named `<provider>/<model>`, idempotently on every launch, and
offers them in the Agent picker alongside CLI harnesses. A provider that
cannot be connected is skipped with a log line; it never blocks startup. See
[getting-started.md](getting-started.md#config-declared-model-providers) for
the full schema and an example.

### Solo key bindings

The key map is available from `?` or `F1` while no other modal is active.
Printable characters and editing keys go to the composer when it is focused,
except for the listed contextual commands. `Enter` submits a non-empty,
enabled composer draft, or confirms the selected setup/modal option. It never
treats a word typed in chat as an approval.

Solo opens on the Kanban. `F2` switches between that task workspace and the
single Main Chat without hiding either inside a side rail. Wide terminals draw
all five lifecycle lanes; compact terminals draw one readable lane at a time
and keep the five lane counts visible above it.

| Key | Action |
|---|---|
| `Enter` | Submit a non-empty composer draft; confirm the selected setup/question/approval/review/cancellation action. Request changes first opens a required guidance field; type the changes and press Enter again to send. |
| `Shift+Enter` | Insert a newline in the composer. |
| `Backspace`, `Delete` | Delete before/after the composer cursor. |
| `Left`, `Right` | Move the composer cursor in Main Chat, or move between non-empty Kanban lanes. |
| `Home`, `End` | Move the composer cursor when it is focused; otherwise jump to the oldest/latest timeline position. (`Ctrl` does not change this mapping.) |
| `Tab`, `Shift+Tab` | In Main Chat, move focus forward/backward across the composer, timeline, and activity. |
| `Enter` (Kanban) | Open the selected Task's details, or its authoritative review card when one is available. Opening a card does not execute its action. |
| `Up`, `Down` | Scroll the focused timeline/activity, move the selected task, or move a setup/modal selection. |
| `PageUp`, `PageDown` | Scroll the timeline, or move a modal selection when a modal is focused. |
| `F2` | Switch between Kanban and Main Chat. This is the portable terminal shortcut. |
| `Ctrl+1`, `Ctrl+2` | Open Kanban or Main Chat directly when the terminal reports control-number keys distinctly. |
| `Ctrl+[`, `Ctrl+]` | Select the previous/next primary view when the terminal reports those control sequences distinctly. |
| `t`, `a`, `v` (Kanban) | Open Tasks, Attention, or Approvals. |
| `a` (Main Chat, outside composer) | Expand/collapse the live activity row when one is present. |
| `Shift+R` (outside composer) | Expand/collapse ordinary reasoning (collapsed by default). |
| `r` | Retry the current failed or cancelled turn if it has not been superseded; when setup is unavailable and retryable, refresh setup. |
| `?` (outside composer), `F1` | Open help when no other modal is active; close help when it is focused. |
| `Esc` | Close the focused modal; in the setup picker it returns focus without changing the selected candidate; otherwise it is ignored. It never cancels a live turn by itself. |
| `Ctrl+C` (when no modal is active) | With a live turn, open cancellation for that exact turn; otherwise request quit. During bounded shutdown, a second `Ctrl+C` forces terminal restoration and leaves durable recovery for the next launch. |
| `q` (outside composer), `Ctrl+Q` (when no modal is active) | Request quit; if a live turn is active, Solo opens its exact-turn cancellation card first. |

On question, approval, review, and cancellation cards, use `Up`/`Down` to
select an option/action and `Enter` to submit it. On approval and review cards,
`y`/`a` accepts the selected permitted action and `n` rejects or requests
changes when that action is permitted. These shortcuts are modal-only. A stale
target returns a conflict and refreshes the card; it does not partially apply
the action.

The TUI has typed question-card support and renders approvals, Task checks,
commit evidence, Attention, and canonical recovery actions from durable state.
The current CLI-only Solo startup does not attach the protected Agent Runtime
interaction broker, so protected questionnaires remain parked rather than
showing an answerable card. Solo does not infer meaning from state-name or
prose text.

### Exit and recovery semantics

Preflight, repository, marker, data-root, lock, migration, or Agent errors are
printed as bounded normal-terminal diagnostics before raw mode or the alternate
screen is entered. A normal quit asks the shared runtime supervisor to settle
owned work within its deadline, then restores the cursor, raw mode, and
original screen. When the input source is configured with termination signals,
SIGINT follows the Ctrl-C cancellation/quit flow and SIGTERM requests quit;
terminal restoration occurs when the controller exits. A panic hook performs
best-effort restoration immediately after TUI entry. Forced quit does not
delete the database or claim an unfinished turn or Task succeeded; the next
launch runs ordinary Forge recovery and restores the same Project/chat state.

The latest failed turn remains visible with its failure and eligible `r`
retry action. It is not treated as running or cancellable. A newer turn
supersedes that retry target. New launches generate a fresh command namespace;
retrying an unchanged draft after a failed send reuses the original command's
idempotency key.

### Choosing the right command

| Command | Process and scope | Interactive surface |
|---|---|---|
| `forge` | Full local server with account/portfolio/Project surfaces, REST, web, MCP, and optional daemon transport. | Browser and API clients; uses the normal Forge data root. |
| `forge-ctl` | Authenticated HTTP client for a running `forge` server. | Shell commands and scripts; no local domain runtime. |
| `forge-solo` | One repository, one isolated Project/chat, shared Forge workflow and recovery in one process. | TUI only; no server, browser, MCP, remote daemon, portfolio, or multi-Project navigation. |

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
| `project` | Create / list / show projects, re-check environment checks |
| `analytics` | Inspect account-wide usage, cost coverage, and pricing provenance |
| `repo`    | Add / list repos and manage their machine-local locations |
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
forge-ctl task action <TASK_ID> cancel
```

`task prompt-preview` is read-only. Add `--trigger accept|reject|fail|retry`
to preview the prompt for a transition target instead of the task's current
state.

### Project environment checks

```bash
forge-ctl project env-check <PROJECT_ID> --name cargo --command 'cargo --version' --scope machine
forge-ctl project env-check <PROJECT_ID> --name tests --command 'cargo check' --scope workspace --role coder --timeout-seconds 120
```

Adds or replaces one named check while preserving other Project settings.
`--scope` is `workspace` (default) or `machine`; machine checks need no checkout
and can gate cloning. Commands must be read-only. `--role` may be repeated;
omitting it applies to all roles. The server validates timeout bounds 1–300.
The update uses the current Project version; a concurrent edit returns 409.
Project settings also accept `placement.provision = when_verified` (default)
or `never`, editable in the web Environment settings.

### Project owner escalations

```bash
forge-ctl project escalations list <PROJECT_ID>
forge-ctl project escalations list <PROJECT_ID> --status open
forge-ctl project escalations answer <PROJECT_ID> <ESCALATION_ID> --answer "Token added to the vault"
```

`list` reads `GET /api/v1/projects/{id}/escalations` (first page, oldest
first); table output shows each escalation's id, status, version, need, and
answer. `answer` reads the escalation's current version and posts the answer;
it wakes the Project Agent without spending its wake budget. Both are
owner-only: another Project member gets 403 and anyone else 404. A stale or
second answer returns 409.

### Project environment readiness and re-check

```bash
forge-ctl project env-status <PROJECT_ID>
forge-ctl project env-recheck <PROJECT_ID>
forge-ctl project env-recheck <PROJECT_ID> --machine server
forge-ctl project env-recheck <PROJECT_ID> --machine <RUNTIME_ID>
forge-ctl --output json project env-status <PROJECT_ID>
forge-ctl --output json project env-recheck <PROJECT_ID> --machine server
```

`env-status` reads the Project's recorded `environment_readiness`. Table output
shows machine name/ID, status, failing checks, output, and checked/next-check
times; no rows produce an empty-state message. JSON output is the readiness
array. This read does not start checks.

`env-recheck` runs every configured check through
`POST /api/v1/projects/{id}/environment/recheck`, on all known machines or the
selected machine. `server` names the host; daemon machines use runtime IDs from
`env-status`. Table output groups checks and unavailable-machine errors by
machine, followed by the updated Project. JSON output is `{machines, project}`.
Passing a machine resumes a matching environment pause; user/repository pauses
remain. On a daemon that supports machine probes the re-check runs there
directly; other daemons need a ready workspace recorded on that machine, and a
machine without one returns an unavailable result and keeps its facts.

Both commands exit 0 after a successful API response, including checks that
report failure or unavailability. Inspect `passed` and `error` for those
outcomes. Request/HTTP errors exit 1, and invalid CLI arguments exit 2. Authority
is the same as the API. This command does not wait for the scheduled re-check
interval (default 600 seconds).

### Repository locations

Use `repo location` to manage physical checkouts of a Repo. Every command
requires `--repo-id` and Project owner/admin access (or a server administrator).
Registration verifies the location and prints its persisted status and error.

```bash
forge-ctl repo location list --repo-id <REPO_ID> --include-total
forge-ctl repo location add --repo-id <REPO_ID> \
  --owner server --path /srv/checkouts/app --default
forge-ctl repo location add --repo-id <REPO_ID> \
  --owner daemon --daemon-id <DAEMON_ID> --runtime-id <RUNTIME_ID> \
  --path /Volumes/Data/codes/app --kind primary_checkout
forge-ctl repo location verify --repo-id <REPO_ID> <LOCATION_ID> --version <VERSION>
forge-ctl repo location set-default --repo-id <REPO_ID> <LOCATION_ID> --version <VERSION>
forge-ctl repo location remove --repo-id <REPO_ID> <LOCATION_ID>
```

`add` defaults to `--owner server` and `--kind primary_checkout`. Other kinds
are `managed_clone` and `shared_mount`; a shared mount uses `--owner server`
with both daemon/runtime IDs. Daemon/runtime IDs must be visible and belong
together. Configure the daemon's `--workspace-root` to include the checkout
path supplied to `add`; a daemon path must lie within that runtime's advertised
root, and the server never opens it. Verification uses `repo_location.verify`
over the daemon command stream; an offline daemon reports `unavailable` with
`daemon_unavailable`. Shared mounts also require the daemon to read back a
server-written probe under the server's worktree root at the same path.
Verification is retried after an accepted daemon handshake, including for
previously ready locations. A root escape reports `invalid` with
`outside_workspace_root`.

Table output includes full location, daemon, and runtime IDs, the path, kind,
default flag, status, version, and last error. Use `--output json` for the full
response and verification timestamps. `list` accepts `--limit`, `--cursor`,
and `--include-total`; pass the returned next cursor unchanged. A successful
request can return an `invalid` or `unavailable` location: inspect its status
to determine whether verification succeeded.

Pass the current version shown by `list` to `verify` and `set-default`.
A stale version returns 409. Setting a new default clears the old default
atomically, which also changes that old location's version. `remove` returns
409 naming the Task while any non-cleaned placement still uses the location.

### Usage and cost analytics

Project and account analytics preserve provider-reported and Forge-estimated
money as decimal strings. Table output labels complete, partial, pending, and
unavailable coverage; JSON output returns the full typed `CostSummary` and
source provenance. Optional RFC3339 windows are half-open `[from, to)`, and
offsets containing `+` are URL-encoded by the client.

```bash
forge-ctl project analytics <PROJECT_ID>
forge-ctl project analytics <PROJECT_ID> \
  --from '2026-09-01T00:00:00-04:00' \
  --to '2026-10-01T00:00:00-04:00'

forge-ctl analytics usage
forge-ctl --output json analytics usage \
  --from '2026-09-01T00:00:00Z' --to '2026-10-01T00:00:00Z'
```

An unknown or incomplete cost is never printed as zero. `Cost unknown` means
settled activity could not be priced; `$0.00` is reserved for complete,
explicitly known zero.

### Machine capacity flags

`forge --max-concurrent-runs N` overrides `FORGE_SERVER_MAX_CONCURRENT_RUNS`,
which overrides `server.max_concurrent_runs` in Forge YAML. Unset means
`max(2, logical_cores / 2)`; `0` means unlimited. Administrators can change this
setting live through Settings.

`forge-daemon --max-concurrent-runs N` and
`forge-ctl daemon link|start|report --max-concurrent-runs N` override the daemon's
local `max_concurrent_runs` in `daemon.yaml` beside its credentials. The same
automatic default is computed on the daemon machine. Each registration/report
sends the resolved typed value. Legacy session-cap labels are no longer read.

Each report also sends the free bytes and inodes of the filesystem holding
`--workspace-root`, and whether the daemon's garbage collector runs on that
root. The reply carries the server's free-space floor
(`workspace.min_free_*`, `workspace.gc_free_*` in the server's `forge.yaml`),
which the daemon uses for its own collector; there is no daemon-side key for
it. While the reading is under the floor the server places no new worktree
on that machine; work already in a worktree there, checks included, carries
on. The daemon checks its own disk as well: asked to make a worktree while
its reading is under the floor, it refuses with `disk_pressure` and the Task
waits. A reading older than five minutes (a daemon that stopped reporting)
no longer counts.
`forge-ctl daemon` tables print the reading in the `Disk` column: free space,
or `LOW (bytes|inodes) <free>, floor <floor>` while the machine is under its
floor, or `-` before the first reading. `--output json` carries the same as
`disk` and `workspace_floor`.

### Moving the server workspace root

This is a flag of the server binary (`forge`), not of `forge-ctl`: it works
on the data directory with the server stopped.

```text
forge [--data-dir <DIR>] --migrate-workspace-root [<NEW_ROOT>]
```

Moves the server's workspace root (Task worktrees, repository clones under
`.repos/`, execution logs under `.forge/logs/`, garbage-collection state) to
`<NEW_ROOT>`; without a path, to the configured root (`workspace.root`,
`FORGE_WORKSPACE_ROOT`) when one is set, else to `<data dir>/worktrees`; then
exits. Only what this data directory's database made moves (its Tasks'
roots, its repositories' clones, its Projects' logs, what its stored paths
name); anything else in the old root, including another data directory's
worktrees in a root they shared, stays and is listed.
It cannot be combined with `--demo`, `--no-mcp`, `--no-embedded-daemon`,
`--reclaim-workspace-gc` or `--convert-db-to-incremental-vacuum`.

| Exit code | Meaning |
|---|---|
| `0` | Moved, or nothing to move (no root in use yet, or already there). A summary is printed. |
| `1` | Refused (nothing changed), interrupted, or `db pending` (files moved, database unchanged): run the same command again. The reason is on stderr. |

It refuses while a Forge server or another maintenance command holds the
data directory, while work is recorded in flight (a running execution or
check run, a claimed or suspended task step, an integration attempt that is
neither finished nor parked, an active workspace lease), when a submodule or
nested worktree names the old root by absolute path, when `<NEW_ROOT>` is
not empty (unless the old root is gone), is a file, is inside the old root or
contains it, cannot be a workspace root (the home directory or a parent of
it, a Git repository, a top-level directory), or, for a move across
filesystems, has less free space than the old root's size (hard links
counted in full) plus `workspace.min_free_bytes`.

Every file is kept, Git worktree links are repaired and checked, and every
stored path is rewritten in one transaction. The old root keeps a `MOVED`
file. Daemon-owned workspaces are not touched. A copy never removes a
socket, pipe or device and does not keep extended attributes. In your own
repository the only change is `git worktree repair`, run there. An
interrupted run leaves `<data dir>/workspace-root-migration.json`; the
server refuses to start (`migration in progress`) until the command is run
again and finishes. The move rolls forward only; there is no `--abort`. See
[getting started](getting-started.md#where-the-server-keeps-workspaces-and-how-to-move-them)
for when a move is needed and what a start does after an upgrade. Forge Solo
has no such flag: its root always follows its data root, and a refused Solo
start prints the `forge --data-dir <Solo data root> --migrate-workspace-root`
command to run.

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

Anyone who can edit server-side review steps, Project hooks, or environment
checks can run their permitted shell commands on the daemon's machine.

The daemon run policy is not a security boundary against a compromised or
malicious server: the shell executor and owner operations are not gated by it.
The server can read anything under the daemon's workspace root. Choose a root
containing only files you intend to expose to that server. Processes also have
the daemon user's `HOME`, credentials, and network access.

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

Repository locations identify a checkout on the server or a particular daemon
runtime. Register daemon checkouts through `repo location add` and verify them
before placement. A daemon may execute a server-owned workspace only through
a verified `shared_mount` location; matching absolute paths alone are
insufficient. Daemon-owned placement requires CLI Agents for every role that
touches its worktree.

The daemon reads `workspace.run.allow` from `daemon.yaml` beside its credentials
(default: `[ci_step]`). Hook and environment setup purposes require local opt-in.
This dispatch policy has the trust limits described above.

Upgrade the server first, then every daemon using `forge-ctl` from that server
release (protocol revision 7 or newer), restarting each with its existing
`--workspace-root`.
A connection below revision 7 receives `daemon_upgrade_required` and cannot use any
command RPC: execution, repository verification, filesystem browsing
(`fs.list`/`fs.branches`), workspace operations, or PTY terminals. Operator status
shows `upgrade_required`; pinned Agents and refused Task admissions carry
`daemon_upgrade_required` with instructions to install the daemon from the
server's release. Repository locations retain upgrade reasons after a verification
attempt, without changing their verification status. Task admission is an upgrade refusal only
when an otherwise eligible owner is blocked solely by the upgrade (disregarding
facts absent from the older handshake), and no owner is blocked solely by
capacity or a transient condition. It creates no Execution or retry-budget charge.
Upgrade refusals are cleared by the heartbeat sweep once a refused daemon
reconnects at revision 7, waking Task dispatch automatically. Upgrading the daemon
is the required human action. The old daemon logs the instruction through its
existing warning handler; a new binary also prints it to stderr on connect.
A socket awaiting its handshake is `daemon_not_ready`, not an upgrade refusal.
Existing ready placements become disconnected while an upgrade is needed, with
an attention item and frozen leases. They wait up to `max_disconnect` (24 hours
by default), then fail with `owner_disconnected_timeout`.

### Removing a disconnected machine

```bash
forge-ctl daemon remove <daemon-id>
forge-ctl --output json daemon remove <daemon-id>
```

Stop the daemon first. Connected machines return `machine_connected` (409), and
the embedded server machine returns `local_machine` (409). The registration owner
or an administrator may remove it. Removal revokes the old credential, clears
pending remote cleanup, releases the workspaces the machine owned and retires the
Agents pinned to it. Every Task that had a workspace there is re-placed on
another machine with a fresh workspace from its last server-known branch;
**work on the removed machine that was not pushed is abandoned**.
Table output reports the machine name, the cleared cancellations, the released
workspaces (`placements_failed`), the Tasks that will re-place, the retired Agents
and the queued Tasks; JSON output returns every removal count. Execution history
retains the name. Physical files remain on the missing machine. Connecting again
requires `daemon link` to register a new identity. See
[the API contract](api.md#removing-a-machine).

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

# Create a direct agent on a ChatGPT login entry; supported efforts are
# model-specific, and omission uses the provider default
forge-ctl embedded create \
  --name "Forge Assistant" \
  --credential-id <ENTRY_ID> \
  --model gpt-5.6-terra \
  --reasoning-effort ultra

forge-ctl embedded profile list <IDENTITY_ID>
forge-ctl embedded profile connect <IDENTITY_ID> --version <VERSION> \
  --credential-id <ENTRY_ID> --model gpt-5.6-terra --reasoning-effort ultra
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

Task JSON uses the REST condition contract. Task list, get and action results
remove `error_annotation`, `blocked` and `failed`, and expose `condition.kind`,
typed reasons/continuation and `condition.details`. Use
`condition.details.diagnostic` or the computed `workflow_exception` for a
failure explanation, `condition.details.interruption` for process interruption
evidence, and `awaiting_human` for the current human wait. The list value now
agrees with detail. CLI flag names and human table columns do not change.


### Per-run build budget and CPU priority

Every CLI agent process (and its children), native tool command, review CI
step, Project hook and environment check/setup command uses the executing
machine's build budget. Unset `build_jobs_per_run` computes
`max(1, logical_cores / run_cap)`: use the configured positive machine cap, or
the automatic cap `max(2, logical_cores / 2)` when the cap is unset or `0`.
`build_jobs_per_run: 0` disables Forge's defaults; a positive value is exact.
Forge supplies `CARGO_BUILD_JOBS=k`, `RUST_TEST_THREADS=k`, `MAKEFLAGS=-j<k>`,
`CMAKE_BUILD_PARALLEL_LEVEL=k` and `GOFLAGS=-p=<k>`. Each Project's
`environment.env` takes precedence, then the Forge/daemon process environment,
then these defaults. These are cooperative tool limits, not a hard CPU quota.

On Unix, run children start with a niceness increment of `run_nice` (default
`10`, range `0`–`19`; `0` disables it, resulting niceness capped at `19`). Their
children inherit that priority; Forge's own priority stays unchanged. Failure
to lower priority logs once and does not fail work. On Windows niceness is a
no-op. Updates affect newly spawned processes; existing children keep their
launch environment and priority.

Server controls are `server.build_jobs_per_run` and `server.run_nice` in YAML,
`FORGE_SERVER_BUILD_JOBS_PER_RUN` and `FORGE_SERVER_RUN_NICE` in the operator
environment, and `forge --build-jobs-per-run N --run-nice N`. Precedence is file,
then environment, then flag. Forge Settings changes these values live, with
cores, effective cap and budget shown beside the machine cap; launch overrides
apply again after a restart. Operations includes the available server facts.

Daemons use top-level `build_jobs_per_run` and `run_nice` in `daemon.yaml` beside
their credentials. `forge-daemon`, `forge-ctl daemon link` and
`forge-ctl daemon start` accept `--build-jobs-per-run N` and `--run-nice N` to
override the file. This is daemon-local policy, with no transport override and
no daemon protocol change. Remote policy facts are not reported.

A daemon's opt-in shared compiler cache is `workspace.compiler_cache`
(`wrapper`, `max_bytes`, `dir`) in the same `daemon.yaml`; `forge-daemon`
accepts `--compiler-cache-wrapper`, `--compiler-cache-max-bytes` and
`--compiler-cache-dir` to override it (`forge-ctl daemon link` and
`forge-ctl daemon start` read the file only). See "Shared compiler cache" in
`docs/getting-started.md`.

## Task action commands

`forge-ctl task actions <id>` prints current offers and version. `forge-ctl task action <id> <verb> [--version N] [flags]` applies one; without `--version` it reads the current version first. Verbs are `start`, `hold`, `release`, `retry`, `send_back`, `approve`, `restart`, and `cancel`.

Retry flags are `--fresh-session [true|false]`, `--refresh-workspace [true|false]`, `--reset-budget [true|false]`, `--guidance TEXT`, and `--reason TEXT`. Send-back requires nonblank `--guidance TEXT`. Approval uses `--override [true|false]`; when omitted, the server uses the matching offer's value, which can be true (an override), so check `task actions` first. An override requires `--reason TEXT`. Hold, release, restart and cancel accept `--reason TEXT`. One-shot retry (`--reset-budget false`) also requires a reason. Bare boolean flags mean true. Flags belonging to another verb are rejected locally. Offers define any further required reason and allowed values. `task cancel` is removed; use `task action <id> cancel`.

`--output json` prints the offer/version object or the resulting Task. An HTTP 409 `action_unavailable` prints the current available actions (the structured error object in JSON mode) and exits with code **3**; other failures use the normal nonzero error exit. A stale version remains a version conflict. There is no `execution` command group in forge-ctl; individual execution stop is available through `POST /api/v1/executions/{id}/stop` and the web Stop control.

Task action offer tables include required and conditional inputs, accepted boolean
values, the action's preset parameters, and `propagates` (subtask cancellation).
`task action --version` is the Task version, not the CLI version; omit it to fetch
the current version. The help text names each flag's verb. Exit code 3 means
`action_unavailable`: read `task actions` again and select a current offer.
Fixed boolean values and omitted presets are supplied by the server; contradictory
values are refused.

## Operations dead letters

These commands require an administrator login and share the normal `--server`,
`--output json|table`, authentication and HTTP error handling.

```bash
forge-ctl operations dead-letters list [--consumer <name>] [--state open|resolved] [--limit 1..100] [--cursor <opaque>]
forge-ctl operations dead-letters replay <id>
forge-ctl operations dead-letters dismiss <id> [--reason <text>]
```

List defaults to open rows, 50 at a time. JSON returns `items` and `next_cursor`;
table output prints the next cursor when another page exists. It uses lowercase
states and shows `replayable`, `event_created_at`, and `events_since`. Pass the cursor
unchanged with the same filters. Resolved lists order by resolution time. IDs come from this list or Operations status.
Replay accepts only whole-event bare sequence keys; item-level and wake-retry
quarantines return 409 `dead_letter_not_replayable` without counting an attempt.
They remain dismissible. Replay delivers one event to its original consumer through the usual transaction
and idempotency rules, without moving the cursor. Its outcome is `replayed`,
`skipped` or `replay_failed`. Failed replay prints its updated row and exits
nonzero; it remains open for another manual action. Dismiss returns `dismissed`
and retains the optional reason. Unknown IDs return 404; resolved rows or a lost
race return 409. Resolved rows are retained for audit and omitted from open counts.
There is no automatic replay, bulk command or dead-letter retention job.

The server also accepts `--check-run-timeout-seconds N` (positive integer,
1800 seconds by default), overriding `FORGE_SERVER_CHECK_RUN_TIMEOUT_SECONDS`
and `server.check_run_timeout_seconds` in `forge.yaml`. This defines the future
whole-check bundle deadline; no execution reads it in 3.3 stage A. It does not
change today's per-command review timeout or unbounded entry CI.
