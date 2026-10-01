---
created_at: 2026-09-29T00:00:00Z
updated_at: 2026-09-30T12:00:00Z
---

## Why

A linked daemon on a separate machine cannot own a Task's workspace. The
server creates every worktree under its own workspace root, and
`execution.start` sends that path as `workspace_path`
(`task_service/execution/runner.rs`). The daemon's `validate_within_root`
(`forge-client/src/daemon_runtime.rs`) then rejects it, as
`docs/architecture.md#daemon-command-transport` says. Review, CI steps,
hooks, diffs, merge, reset, and cleanup all run against server-local paths,
so running only the CLI remotely would still split the workspace lifecycle
across two machines. The FrameRill investigation
(`inbox/Remote execution and daemon-owned workspace report.md`) showed this:
a "remote" smoke Task ran in `/tmp/forge/worktrees` on the Linux server, and
its commit had to be copied into the Mac checkout by hand.

The code also decides the daemon twice. The executor snapshot resolves a
daemon and stores `resolved_daemon_id` (`task_service/config.rs`), but
execution routing uses only `Agent.daemon_id` (`runner.rs`
`execution_provider_for_agent`). `select_execution_provider` then falls back
to the embedded provider when that field is empty. An unpinned CLI Agent
therefore always runs embedded, while the ledger records the resolved daemon.
Unpinned resolution itself is "first online daemon by `created_at`", which
ignores the repository, the platform, and the workspace.

## What Changes

- Add **repository locations**: a machine-local checkout of a Project
  repository, owned either by the Forge server host or by a specific daemon
  runtime. Locations are registered, verified by their owner, and have a
  status. `Repo.local_path` stops being the only answer to "where is this
  repository".
- Add a persisted **workspace placement** per Task workspace. It binds the
  Agent, the owner (the server host or a daemon runtime), the repository
  location, and an owner-issued opaque workspace handle. It also records a
  generation, a state, and the selection reason. Placement is chosen once,
  at claim admission and before workspace preparation, and it is sticky
  after preparation succeeds. Subtasks that share a root workspace share its
  placement.
- Make placement the single source of truth. Execution routing, the
  executor snapshot, terminals, review, CI steps, hooks, environment setup,
  diffs, plan and artifact reads, merge, reset, recovery, and cleanup
  resolve the owner from the placement. None of them resolve a daemon again
  or read `worktree_path` directly. The duplicate daemon decision is
  removed.
- Add a transport-neutral **workspace backend**. The embedded backend wraps
  today's `WorkspaceManager`, `merge_service`, review runner, and git
  behavior without changing it. The daemon backend issues structured,
  idempotent `workspace.*` operations over the existing daemon command
  stream.
- Extend the daemon protocol (revision 3, capability `workspace.v1`) with
  operation-level RPCs rather than a generic remote shell:
  `repo_location.verify`, `workspace.prepare`, `workspace.describe`,
  `workspace.run`, `workspace.diff`, `workspace.read`, `workspace.merge`,
  `workspace.reset`, and `workspace.cleanup`. Each mutation carries a
  workspace handle, an operation id, the placement generation, and an
  expected base SHA or version. The daemon keeps a local journal so it can
  replay terminal results and cleanup acknowledgements after a reconnect.
  It is the existing terminal store, extended, not a second journal. The
  CLI's worklog and evidence outbox travels inside the terminal report.
  Each daemon advertises per-executor adapter capability facts, and a local
  run policy that decides which `workspace.run` purposes it accepts.
- Admission becomes reserve → prepare → start. The placement is reserved
  first, the owner prepares the workspace, and only then are the Task
  claim, the `Running` Execution, and the lease created. A failed prepare
  leaves no Execution and does not spend the retry budget.
- Failure policy: if a daemon disconnects after preparation, the placement
  becomes `disconnected` and the Task waits. Running executions' leases are
  frozen, not expired, until reconciliation or a `max_disconnect` bound, so
  a CLI that finishes during an outage is not discarded. On reconnect, the
  server reconciles through `workspace.describe` (which lists active and
  journaled executions) before anything resumes, and a periodic sweep
  retries an interrupted reconciliation. Placement and transport failures
  have their own failure causes and never spend the Task retry budget. Cleanup
  stays pending until the owner acknowledges it. Forge never silently
  rebuilds the workspace on another machine.
- First slice scope: **direct-merge** repositories on daemon placements,
  with **CLI Agents for every role that touches the worktree** (coder,
  reviewer, planner). Admission rejects pull-request mode and native
  (server-hosted) Agents on a daemon placement, with a structured reason.
  These are follow-ups, not silent fallbacks.
- **BREAKING**: an unpinned CLI Agent no longer silently runs on the
  embedded provider. It runs on the owner its placement selects, and
  admission fails with a structured reason when no compatible owner exists.
- **BREAKING**: daemon protocol revision 2 → 3. Revision-2 daemons remain
  visible for upgrade diagnostics, but all command RPCs are refused
  with `daemon_upgrade_required`, including execution, verification, filesystem
  browsing, and terminals. Upgrade the server first, then every daemon.
- **BREAKING**: the executor snapshot drops `resolved_daemon_id` in favor of
  `placement_id`. Task and Workspace responses gain a `placement` object.
  `docs/architecture.md` loses the "same absolute path" caveat, and a
  shared-mount daemon on the server host becomes an explicit, verified
  location kind.
- Out of scope: machine groups, label or capability selectors, load
  scoring, draining, per-Task "Run on" overrides, pull-request delivery
  from daemons, native Agents on daemon placements, and cross-machine
  migration. The design leaves room for each of them.

## Impact

- Affected specs: `workspace-placement`, `repository-locations`,
  `daemon-workspace-protocol`
- Affected code:
  - `crates/db/migrations/` (new `V147+` migration: `repo_location`,
    `workspace_placement`, daemon operation journal ack table; backfill for
    existing workspaces)
  - `crates/db/src/{models.rs,repository.rs,sqlite/}` (new repos, row
    mappers)
  - `crates/services/src/task_service/{claim,workspace,config}.rs`,
    `task_service/execution/{runner,recovery,hooks,environment}.rs`
  - `crates/services/src/{merge_service,workspace_cleanup,diff,plan_artifact,terminal_service,operator_status,native_tools,agent_service,recovery}.rs`,
    `lifecycle/`, `workflow/actions/`
  - `crates/review/src/{runner,contract}.rs` (run checks and read the
    worktree through the backend)
  - `crates/services/src/daemon_transport/` (router, remote provider,
    `workspace.*` client)
  - `crates/api-types/src/daemon_transport.rs` (revision 3, new params and
    results)
  - `crates/forge-client/src/{daemon_runtime,daemon_fs}.rs`,
    `crates/forge-daemon/src/commands.rs` (daemon-side workspace backend and
    journal)
  - `crates/api/src/routes/{repos,tasks,daemons}.rs`, `web/src/types/generated/`,
    `forge-ctl` (`repo location` subcommands)
  - `docs/architecture.md`, `docs/api.md`, `docs/getting-started.md`,
    `docs/cli.md`, `CHANGELOG.md`
- Overlap: `refactor-task-hierarchy-policy` and `add-root-lead-agent` both
  touch `claim.rs`, root and subtask workspace sharing, and dispatch. This
  change should be sequenced after them, or rebased onto them. It must not
  land on the task-hierarchy branch.
