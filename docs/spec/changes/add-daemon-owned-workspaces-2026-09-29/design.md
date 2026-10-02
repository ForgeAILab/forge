## Context

Forge splits responsibility today:

| Step | Where it runs now | Code |
| --- | --- | --- |
| Pick daemon (unpinned) | first online daemon by `created_at` | `agent_service::resolve_daemon_for_agent`, `sqlite/daemon.rs` |
| Route execution | `Agent.daemon_id` only; `None` → embedded | `runner.rs::execution_provider_for_agent`, `daemon_transport/router.rs` |
| Create worktree | server `WorkspaceManager` under server root | `task_service/workspace.rs`, `crates/workspace` |
| Run CLI | embedded, or daemon with server path | `ExecutionStartParams.workspace_path` |
| Setup, hooks, CI steps, reviewer prompt | server `sh`/`bash -lc` in worktree | `execution/environment.rs`, `lifecycle/`, `review/runner`, `review/contract.rs` |
| Diff, plan, and progress reads | server fs | `diff.rs`, `plan_artifact.rs`, `operator_status.rs` |
| Merge, PR push | server git on `Repo.local_path` or a managed clone | `merge_service.rs`, `pr_service.rs` |
| Terminal | daemon, when the Agent is pinned | `terminal_service.rs` |
| Cleanup | server | `workspace_cleanup.rs` |

About 25 service modules read `Workspace.worktree_path` directly. That field
is the coupling to remove.

## Goals / Non-Goals

- Goals:
  - One persisted decision (the placement) says who owns a workspace.
    Everything else reads it.
  - A daemon on a separate filesystem can prepare, execute, check, review,
    merge (direct-merge), recover, and clean up a Task end to end.
  - Local server-owned workspace behavior is unchanged. The embedded backend
    is a wrapper, not a rewrite. Remote execution providers also freeze leases
    on disconnect for shared-worktree safety (D8).
  - Remote operations are idempotent, generation-fenced, and survive a
    reconnect.
- Non-Goals (first slice):
  - Machine groups, label or capability selectors, load scoring, draining.
  - Per-Task "Run on" override.
  - Pull-request delivery from a daemon placement.
  - Native (server-hosted) Agents working in a daemon-owned worktree.
  - Cross-machine migration of a prepared workspace.
  - Shared mounts or filesystem sync of any kind.

## Decisions

### D1. Owner = server host or one daemon runtime

A placement's owner is either `server` (the Forge process host, embedded
backend) or `daemon` (one runtime row on one daemon, daemon backend). The
server host gets its own owner kind instead of a synthetic daemon row. That
keeps `--no-embedded-daemon` deployments and existing data unambiguous. The
embedded daemon, when present, is an execution provider for server-owned
placements, as it is today.

- Alternatives considered: model the server as the embedded daemon's row.
  Rejected. The embedded daemon is optional, and backfilling existing
  workspaces would depend on which daemon happens to be registered.

### D2. Repository locations are explicit rows

`repo_location` columns: `id, repo_id, owner_kind (server|daemon),
daemon_id?, runtime_id?, path, kind (primary_checkout|managed_clone|shared_mount),
is_default, status (unverified|ready|unavailable|invalid), last_verified_at,
last_error, version, created_at, updated_at`.

- A `server` location with `kind = primary_checkout` is backfilled from every
  `Repo.local_path`. The managed clone under the server root becomes a
  `server` `managed_clone` location the first time it is created. That is
  today's behavior, now recorded.
- A `daemon` location's `path` must lie inside that runtime's advertised
  `workspace_root`. The owning daemon verifies it (`repo_location.verify`):
  the path exists, is a git work tree, its remote matches or is absent, and
  the default branch resolves. Only a `ready` location can be placed.
- `shared_mount` preserves the documented container case: a daemon whose
  runtime root is the server's worktree root. It is placeable with
  `owner_kind = server` plus that daemon as the execution provider. It is
  allowed only when the daemon has verified a server-written probe file at
  the same path. This replaces the undocumented "same absolute path"
  assumption with a checked one.
- `Repo.local_path` remains for now as the server primary-checkout input.
  All reads switch to locations. Removing the column is a follow-up
  migration.

### D3. Placement row and lifecycle

`workspace_placement` columns: `id, workspace_id (unique), task_id, agent_id,
owner_kind, daemon_id?, runtime_id?, repo_location_id, execution_daemon_id?,
workspace_handle?, generation, state, selected_by (scheduler|pin|inherited|backfill),
selection_reason (JSON), reserved_until?, disconnected_at?, failure_cause?,
version, created_at, updated_at`.

States: `reserved → preparing → ready ⇄ disconnected → cleaning → cleaned`,
plus `failed`. A `reserved` placement may expire and be rescheduled.
`ready` and later states are sticky.

- `workspace_handle` is issued by the owner and is opaque to the server. For
  `server` owners it equals the existing worktree path, so current code keeps
  working behind the backend. The `Workspace.worktree_path` column stays as
  the embedded backend's internal value, and no service outside the backend
  reads it.
- Subtasks that reuse a root workspace (`subtask_reuses_ready_parent_workspace`)
  inherit that workspace's placement with `selected_by = inherited`. A
  subtask whose resolved Agent cannot run on the inherited owner fails
  admission. It does not get a second placement.
- Every mutation increments `version` (`WHERE version = ?`, the usual 409
  path). `generation` increments only when the physical workspace is
  recreated on the same owner, for example after a reset-required recovery.

### D4. Selection at claim admission

Existing workflow role assignment still picks the Agent first. Today
`claim.rs` prepares the worktree (`prepare_workspace_owned`) *before* the
`begin_immediate` transaction and then creates the Execution directly as
`Running`. That order is fine on the server host. For a remote owner, where
preparation is slow and can fail, it would leave a `Running` Execution and a
live lease while nothing runs yet. Admission therefore becomes a
reserve → prepare → start sequence (the order Multica uses: reserve capacity,
claim, prepare, and only then mark the run running):

1. **Reserve** (one `begin_immediate` transaction): select the placement,
   create or reuse the Workspace row, and commit the placement as `reserved`
   with a `reserved_until` deadline. The reservation counts against Agent and
   daemon capacity. No Execution row, lease, or Task status change exists
   yet.
2. **Prepare** (outside any transaction): `backend.prepare(placement, base)`
   with an `operation_id` derived from the placement id and generation, so a
   retried prepare is idempotent. The placement moves `reserved → preparing`
   before the call and `preparing → ready` on success (each a version CAS),
   recording the handle and base SHA.
3. **Start** (the existing claim transaction): the Task claim, the `Running`
   Execution, and the lease are created as today. The transaction also
   checks that the placement is `ready` and still at the version read in
   step 2.

A prepare failure or a `reserved_until` timeout moves the placement to
`failed`, or back to reselectable. It releases the reservation and creates
**no Execution**, so it never spends the Task retry budget. It is reported
with a placement failure cause (D9). The embedded backend runs the same three
steps; for server placements, step 2 is today's `prepare_workspace_owned`, so
the observable behavior is unchanged. A re-claim that already has a `ready`
placement skips steps 1–2 and only runs `describe` as a cheap precondition.

Hard filters (candidate = one ready repo location):

1. The owner is reachable. For a `daemon` owner: connected, protocol ≥ 3
   with `workspace.v1`, visible to the Task owner, runtime `ready`.
2. The Agent's executor is installed, authenticated, and enabled on that
   owner, and the owner's advertised adapter capability facts for that
   executor (D10) cover what the role needs. An Agent pinned with
   `daemon_id` restricts candidates to that daemon.
3. First-slice limits: a `daemon` owner requires `work_mode = direct_merge`,
   and a CLI backend for every worktree role currently assigned (coder,
   reviewer, planner).
4. Agent capacity and daemon capacity (`agent_capacity.rs`), counted as
   described under "Capacity accounting" below.

**Capacity accounting.** Today `agent_capacity.rs` counts only
`execution.status = 'running'`. Daemon capacity joins on `agent.daemon_id`,
so it sees only pinned Agents, and it is checked only when the Agent is
pinned. Two-phase admission and placement routing would break both:
reservations in flight would not count, and unpinned Agents routed to a
daemon would not count against its session cap. The rules become:

- An Agent's occupied slots = its running Executions + its placements in
  `reserved`/`preparing` that have no running Execution yet (placement
  `agent_id`). `max_concurrent_tasks` is checked against that sum.
- A daemon's occupied sessions = running Executions whose placement names
  that daemon as execution provider (`execution_daemon_id`, else `daemon_id`),
  + its `reserved`/`preparing` placements + active chat turns. The session
  cap applies to every candidate daemon, pinned or not.
- Both counts are taken inside the reserve transaction (`begin_immediate`)
  that inserts the placement, and re-checked in the start transaction, so
  two concurrent claims cannot both take the last slot. A placement that is
  `ready`, with no running Execution (between turns, or waiting for review),
  does not hold a slot.
- An expired or failed reservation releases its slot through the same
  state change that ends it.

Preference order: existing placement for this workspace → inherited root
placement → agent pin → the location marked `is_default` → server-owned →
deterministic `(created_at, id)`. `selection_reason` records the winning
rule and the rejected candidates with their filter codes. When nothing
passes, claim returns a structured `placement_unavailable` error that lists
each candidate's rejection. It never falls back.

### D5. One routing decision

`execution_provider_for_agent` and `select_execution_provider` take the
placement, not the Agent. The executor snapshot stores `placement_id` and
drops `resolved_daemon_id`. `ledger.rs`, `terminal_service`, and `recovery.rs`
read the daemon from the placement. `resolve_daemon_for_agent` remains only
for pre-claim availability display (`routes/agents.rs`).

Phase 0 fixes the current divergence (routing ignores `resolved_daemon_id`)
by routing on the snapshot. It is a bug fix that restores intended behavior,
so it can ship before the rest.

### D6. WorkspaceBackend trait (services crate)

```text
prepare(placement, base) -> PreparedWorkspace { handle, base_sha, branch }
describe(placement)      -> WorkspaceState { exists, head_sha, dirty, branch, lock }
run(placement, RunSpec)  -> RunResult { exit_code, stdout/stderr tail, duration }
diff(placement, DiffSpec) -> Diff
read(placement, rel_path, limit) -> bytes
merge(placement, MergeSpec{target_branch, expected_target_sha, handed_off_paths})
                         -> MergeOutcome (existing enum)
reset(placement, ResetSpec) -> PreparedWorkspace
cleanup(placement)       -> CleanupAck
```

- `RunSpec.purpose ∈ {environment_setup, hook, ci_step}`, and the command
  comes from server-side config (review_config, project hooks, project
  environment). The daemon runs it in the handle's worktree with the same
  shell semantics as today (`bash -lc`), time limit, and output cap. It is
  the existing trust model on a different host, not a new shell surface:
  there is no RPC that takes an arbitrary command outside these purposes.
  Running on a different host still widens who can run code on that host
  (see D11), so the daemon decides for itself which purposes it accepts.
- The backend owns **execution outbox harvest**. Today a CLI harness writes
  worklog and evidence into `.forge-outbox/<execution_id>` beside the
  worktree (`executors::execution_outbox_path`), and the runner ingests it
  from the server filesystem after the turn (`runner.rs`, the
  `ingest_execution_outbox` call). On a daemon placement the server cannot
  see that directory. The daemon therefore reads the outbox after the CLI
  exits and embeds the bounded plan candidate, worklog, and evidence entries in the terminal report. The report
  is retained, replayed, and acked as one unit, so evidence and terminal
  status can never arrive separately. The runner ingests entries from the
  report for every placement. The embedded backend fills the same field
  from the local directory, so there is one ingestion path.
- `EmbeddedWorkspaceBackend` delegates to the current code paths
  unchanged. Phase 2 ships only this, and it must be behavior-neutral: the
  existing merge, workspace, and review tests stay green without edits.
- `DaemonWorkspaceBackend` maps each call to one `workspace.*` RPC.
- Plans use the same owner routing boundary for reads, publication, rollback,
  and cleanup. The server sends the start seed in `execution.start.plan_text`;
  the daemon initializes its own outbox and returns `execution.terminal.plan_text`
  with the terminal journal record. The terminal CAS freezes the returned text
  in private execution artifact storage; the existing publication claim authorizes fenced
  owner operations (`publish_plan`, `restore_plan`, `discard_plan`). The daemon
  retains a private prior-plan snapshot until workflow settlement, preserving
  publication and rollback replay. The capability `execution.plan_transport`
  is required at placement for plan-writing roles; absent support yields
  `capability_missing`, while older protocol revisions yield
  `daemon_upgrade_required`. Remote candidates have a 128 KiB byte bound;
  capture failure fails the execution explicitly. Server-owned and shared-mount
  plan storage retains the canonical/outbox layout. Both owners use the same
  implementation-only checklist seed policy; planners never receive a seed.
  Missing/invalid/unchanged required candidates use the existing workflow guard
  rejection. Oversized candidates fail terminally and are acknowledged; plan
  errors never replace failed/cancelled outcomes. Private transported storage is
  redacted, inaccessible through the Execution API and excluded from config
  snapshots and receipt bodies (digest/length only).
  `.forge-plan-staging/` is a Task sibling containing frozen local candidates,
  previous-plan/absent markers on the server, or per-execution
  candidate/previous-plan publication snapshots on the daemon. Settlement,
  abandon cleanup or workspace cleanup removes those execution files.
  Owner errors persist backoff and a visible wait; disconnection uses the same
  durable runtime-offline wait. Plan operations wait behind long workspace
  commands and discard of missing/cleaned state succeeds.

### D7. Daemon protocol revision 3

- `DAEMON_PROTOCOL_REVISION` remains 3, and the handshake retains the
  capability `workspace.v1`. Plan transport adds only the capability
  `execution.plan_transport`; it does not raise the protocol revision. The minimum command revision is 3. Revision-1 and revision-2
  sockets remain visible for upgrade diagnostics but every command RPC is
  refused, including execution, filesystem browsing, verification, and PTY
  terminals. `daemon_upgrade_required` is a human-action refusal on Task
  admission, repository locations, and pinned Agents; admission creates no
  Execution. Task admission requires an otherwise eligible upgrade-only candidate
  and no capacity-only or transient-only alternative; the accepted upgraded handshake
  clears that refusal and wakes dispatch automatically. Reservation writes no Task
  annotation. A socket awaiting its handshake is not ready, not outdated.
  A revision-3 daemon without plan transport remains eligible for reviewer,
  interactive, server-owned shared-mount, filesystem and PTY work. Deterministic
  capability refusals visibly identify the machine, stay parked without repeated
  placement attempts, and wake when eligibility facts change.
  Upgrade the server first, then every daemon from the same release.
- Every mutating request carries `{workspace_handle | placement_id,
  operation_id, generation, expected}`. The daemon records
  `(operation_id → result)` and returns the recorded result for a duplicate
  `operation_id` until acknowledgement. The server must durably store the
  result before `journal.ack`; the daemon then deletes its receipt and prunes
  cleaned handles and their execution IDs. Retired review handles remain fenced
  until their reset or release receipt is acknowledged. A request with a stale `generation` fails with
  `stale_generation`.
- **One daemon journal.** The operation journal extends the existing
  `DaemonTerminalStore` (`forge-client/src/daemon_runtime.rs`) rather than
  adding a second store. Terminal reports, workspace operation results, and
  cleanup acks use one on-disk format, one pending-replay scan, and one ack
  protocol: the existing `execution.terminal.ack`, generalized to
  `journal.ack { entry_id }`.
- Daemon-side safety: a handle maps only to paths the daemon created under
  its own `workspace_root`. `repo_location.verify` rejects paths outside the
  root. `workspace.merge` updates only a `primary_checkout` location the
  daemon verified, and refuses a dirty target (as `merge_service` does
  today). A wrong-owner request is rejected: the daemon checks that
  `placement.daemon_id` is its own id.
- The daemon replays `execution.terminal` and `workspace.cleanup` results
  from the journal on reconnect, until the server acks them.
- The handshake also carries per-executor **adapter capability facts**
  (D10). Capabilities are negotiated as a set, so later additions (for
  example an exact-Execution guidance channel) are new capability names,
  not new protocol revisions.

Deleted worktrees are recreated through their owner from the surviving Task
branch. A damaged worktree on a daemon owner requires a reset; other describe
errors do not trigger recreation.

### D8. Disconnect, reconnect, cleanup

- The daemon monitor marks the owner or remote execution provider offline,
  and every `ready` placement on it becomes `disconnected` with
  `disconnected_at`. This includes server-owned workspaces executed on a
  remote daemon through a shared mount. They do not requeue elsewhere: a
  second execution must not start in a live worktree on that shared mount
  while the first CLI may still be writing.
- A daemon requiring a protocol upgrade is unusable even while its REST heartbeat is online; the monitor treats its placements as disconnected and starts no reconcile worker until a supported handshake arrives.
- **Leases are frozen while disconnected.** Today a remote lease lasts 60s
  (`REMOTE_EXECUTION_LEASE_SECONDS`), and after that
  `execution_events.rs` silently drops any notification from the expired
  lease. Suppose that rule stayed in place and the CLI kept working through
  a longer outage. The daemon's retained terminal report would be rejected
  on reconnect, the commit it describes would be orphaned, and recovery
  could start a second execution in the same worktree while the first CLI
  is still writing to it. Instead:
  - While its placement is `disconnected`, a running execution's lease
    does not expire through heartbeat loss. The lease monitor skips it,
    and notification admission treats the lease as suspended, not expired.
  - A separate bound, `max_disconnect` (config, default 24h), decides when
    the placement gives up waiting. When it elapses, the placement and its
    execution are failed with cause `owner_disconnected_timeout`. Any later
    daemon report for that execution is rejected as a terminal loser, which
    is the existing CAS rule.
  - The execution's `hard_deadline_at` still applies; a frozen lease does
    not extend it.
- On reconnect the server calls `workspace.describe` for each
  `disconnected` placement. `WorkspaceState` also lists the execution ids
  the daemon has active or journaled for that handle. The server drains
  journaled terminal results first, then settles each running execution:
  - one still active on the daemon: resume its lease and keep it;
  - one terminal in the journal: apply the report once;
  - one the daemon does not know: fail it as `owner_lost_execution`.

  Only then does the server compare `head_sha` with the last recorded
  evidence, return the placement to `ready`, and wake the dispatcher.
- Reconciliation is driven by durable state, not by the reconnect event
  alone. A periodic sweep re-runs reconciliation for every placement that
  is `disconnected` with its owner online, and for every `cleaning`
  placement. A reconnect only wakes the sweep early. A half-finished
  reconciliation, such as a server crash between the drain and `ready`,
  is therefore retried. A missing worktree with a
  surviving branch uses the existing recover path on the daemon, and a
  missing branch becomes the existing `reset_required` recovery.
- `workspace_cleanup.rs` schedules cleanup through the backend and moves
  the placement to `cleaned` only on the owner's ack. While the owner is
  offline, cleanup stays `cleaning` and is visible in operator status.
- User actions on a disconnected placement: wait, retry on the same owner,
  or cancel the Task. Migrate is not offered in this change.

### D9. Placement failure causes and the retry budget

Placement and transport failures are infrastructure outcomes, not failed
attempts at the work. They map to structured failure causes that **do not
spend the Task retry budget**, the same as structured executor
unavailability today:

| Cause | Raised by | Effect |
| --- | --- | --- |
| `placement_unavailable` | admission, no candidate passes | claim refused; no Execution |
| `prepare_failed` | `workspace.prepare` error or `reserved_until` timeout | placement `failed` or reselectable; no Execution |
| `owner_disconnected` | daemon monitor | placement `disconnected`; lease frozen (D8) |
| `owner_disconnected_timeout` | `max_disconnect` elapsed | execution failed; recovery offered |
| `owner_lost_execution` | reconcile: daemon does not know the execution | execution failed; `reexecute` offered |
| `stale_generation`, `wrong_owner` | daemon rejection | operation refused; logged as a server bug, surfaced as an attention item |

The cause is stored on the placement (`failure_cause`), and on the
Execution when one exists. The reason-aware recovery classifier the
Multica review recommends can consume these causes later; this change only
guarantees that they exist and do not burn the budget.

### D10. Adapter capability facts per owner

Whether a capability works depends on the CLI version installed on each
machine, so the facts are advertised by each owner, not assumed per Agent.
The embedded provider reports them for the server host, and each daemon
reports them in its handshake and refreshes them when a CLI changes:
`structured_events`, `usage`, `resume`, `cancel_ack`, `terminal_observed`.
Unknown means unsupported.

- The admission hard filter checks the facts the role needs.
- The `resume_session` recovery action is offered only when the placement
  owner is online and reports `resume` for the snapshot's executor. The
  session files live on that machine, so the check is sticky with the
  placement.
- The facts are internal (not a public REST field) in this change.

### D11. Daemon-side run policy

`workspace.run` lets anyone who can edit `review_config`, Project hooks, or
the Project environment on the server run shell commands on the daemon's
machine, for example a developer's Mac. That is a new reach, even though the
command vocabulary is unchanged. The daemon therefore enforces a local
policy in its own config file, which the server cannot change:

- `workspace.run.allow = [ci_step, hook, environment_setup]`: each
  purpose is opt-in. The default is `[ci_step]`.
- The daemon advertises the effective policy in the handshake. Admission
  adds the filter `run_purpose_denied` when the Task's review, hooks, or
  environment config needs a purpose that the owner refuses, so the
  mismatch fails at claim and not halfway through review.
- A refused `workspace.run` returns `purpose_denied`, which is never
  retried.

Documentation separates two questions: what the server may dispatch to a
machine, and what the resulting process can reach there. The daemon is not
a sandbox. The executed process has the daemon user's `HOME`, credentials,
and network.

## Risks / Trade-offs

- **Wide refactor surface.** About 25 modules read `worktree_path`.
  Mitigation: Phase 2 (embedded backend, no behavior change) lands and goes
  green before any remote code exists. A clippy-denied helper or a
  crate-private field blocks new direct reads.
- **Review parity.** The reviewer prompt builder and `review/contract.rs`
  read the worktree on the server. Moving those reads to `backend.read` or
  `diff` must keep the prompt byte-identical for server placements.
  Characterization tests pin it first.
- **Offline owners stall work.** This is intended. Stalls are visible
  (`disconnected` state, attention item) rather than silent, and
  `max_disconnect` bounds them.
- **Frozen leases can hide a dead daemon.** A daemon that never reconnects
  holds its placement until `max_disconnect`. Operator status shows
  `disconnected_at` and the remaining window, and cancel is always
  available.
- **Two-phase admission adds a crash window between reserve and start.**
  Step 1 is durable, step 2 is idempotent by `operation_id`, and the sweep
  expires stale `reserved`/`preparing` rows, so a crash leaves a
  reselectable reservation, never a phantom `Running` Execution.
- **Protocol surface grows.** Nine RPCs instead of a shell. This is more
  code, but each call is auditable and idempotent.
- **Remote merge target.** Direct-merge on a daemon writes the user's real
  checkout on that machine. The same dirty-target and paused-project
  refusals apply there.

## Migration Plan

1. New migration (next free number after `V146`; coordinate with the
   task-hierarchy line, which also adds migrations) creates `repo_location`
   and `workspace_placement`.
2. Backfill in the same migration: one `server`/`primary_checkout` location
   per `repo` with `local_path`, and one `server` placement per
   non-`cleaned` workspace with `selected_by = backfill`, `state` mapped
   from `workspace.status`, `workspace_handle = worktree_path`, and
   `agent_id` taken from the latest execution for that workspace (nullable
   when there is none). Nothing is deleted.
3. Managed clones get their `server`/`managed_clone` location the first time
   `merge_service` or workspace prep touches them. No filesystem scan runs
   in the migration.
4. Rollback: the tables are additive. The breaking behavior (routing,
   snapshot field) is documented under `Unreleased` → `### Breaking`.

## Open Questions

- Should a `daemon` owner be allowed for planning and discovery Tasks
  (read-only worktree) in the first slice, or only for implementation
  Tasks? The default here is to allow them, since they only need `prepare`,
  `read`, and `cleanup`.
- Where does a daemon-owned direct merge record evidence the server can
  show without file access? Proposed: the merge result carries the merged
  SHA, a diffstat, and the conflict paths, and the server stores them on the
  existing merge evidence rows.
- Should `is_default` live on the location or on project settings? The
  location is proposed because it needs no new settings surface.
