## ADDED Requirements

### Requirement: Persisted workspace placement
Every Task workspace SHALL have exactly one persisted placement. The placement records the Agent, the owner (the server host or one daemon runtime), the repository location, an owner-issued opaque workspace handle, a generation, a state, `selected_by`, and a structured selection reason. The placement SHALL be committed as `reserved` at claim admission, in its own transaction, before any workspace preparation runs. The Task claim, the `Running` Execution, and the lease SHALL be created only after preparation succeeds and the placement is `ready`, in a transaction that checks the placement version. Placement updates SHALL use optimistic `version` concurrency.

#### Scenario: Placement is committed before preparation
- **WHEN** a Task is claimed
- **THEN** a placement row exists in state `reserved` before the owner receives any prepare request
- **AND** its `selection_reason` names the winning rule and every rejected candidate with a filter code

#### Scenario: Prepare failure creates no execution
- **WHEN** `workspace.prepare` fails or the reservation passes `reserved_until`
- **THEN** no Execution, lease, or Task status change exists for that attempt
- **AND** the placement records failure cause `prepare_failed`, and the Task retry budget is unchanged

#### Scenario: Concurrent placement update
- **WHEN** two writers update the same placement with the same `version`
- **THEN** one succeeds and the other receives a version conflict (HTTP 409 at the API)

### Requirement: Placement is the only routing authority
Execution dispatch, execution cancel, the executor snapshot, terminals, environment setup, hooks, CI steps, reviewer prompt construction, diffs, plan and artifact reads, merge, reset, recovery, and cleanup SHALL resolve their owner from the Task workspace's placement. No component SHALL re-resolve a daemon from the Agent after admission or read the workspace's server path to reach a daemon-owned workspace. The executor snapshot SHALL record `placement_id` and SHALL NOT record `resolved_daemon_id`.

#### Scenario: Unpinned CLI Agent runs where it was placed
- **WHEN** an unpinned CLI Agent's Task is placed on daemon D
- **THEN** `execution.start` is sent to D
- **AND** the execution ledger records D as the execution daemon

#### Scenario: Cancel follows the placement
- **WHEN** a running execution on a daemon-owned placement is cancelled
- **THEN** `execution.cancel` is sent to the placement's daemon regardless of the Agent's current `daemon_id`

### Requirement: Placement selection
Admission SHALL choose among `ready` repository locations of the Task's repository. A candidate SHALL pass all hard filters: the owner is reachable and visible to the Task owner; a daemon owner has negotiated `workspace.v1`; the Agent's executor is installed, authenticated, and enabled on the owner, and the owner's advertised adapter capability facts for that executor cover the role; the owner's run policy allows every `workspace.run` purpose that the Task's review, hook, and environment configuration needs (filter `run_purpose_denied`); an Agent `daemon_id` pin restricts candidates to that daemon; Agent and daemon capacity are available; and the first-slice limits hold. Among passing candidates the order SHALL be: existing placement for this workspace, inherited root placement, Agent pin, location marked default, server-owned, then `(created_at, id)`. When no candidate passes, admission SHALL fail with `placement_unavailable` listing each rejection and SHALL NOT fall back to another owner.

#### Scenario: Reservations count against capacity
- **WHEN** an Agent with `max_concurrent_tasks = 1` has one placement in `preparing` and a second Task for the same Agent is claimed concurrently
- **THEN** the second claim is refused for capacity, and no second placement or Execution is created

#### Scenario: Unpinned executions count against the daemon cap
- **WHEN** daemon D has a session cap of 2 and two unpinned CLI Agents are running on placements owned by D
- **THEN** a third claim that would place work on D is refused for D with a capacity filter code, and D is never over-subscribed

#### Scenario: Only a daemon location exists
- **WHEN** a repository has a single `ready` location owned by the Mac daemon and the assigned coder and reviewer are CLI Agents installed there
- **THEN** the placement selects the Mac daemon with reason `only_eligible_location`

#### Scenario: No compatible owner
- **WHEN** the only location is on a daemon that lacks the Agent's authenticated executor
- **THEN** claim fails with `placement_unavailable` naming that daemon and filter `executor_unavailable`
- **AND** no execution runs on the embedded provider

#### Scenario: Pinned Agent
- **WHEN** an Agent is pinned to daemon D and the repository has locations on the server and on D
- **THEN** the placement selects D

### Requirement: First-slice daemon placement limits
A daemon-owned placement SHALL require a direct-merge repository, and CLI Agents for every role assigned to the Task that uses the worktree (coder, reviewer, planner). Admission SHALL reject pull-request repositories and native Agents on a daemon placement with the filter codes `work_mode_unsupported` and `native_backend_unsupported`.

#### Scenario: Native reviewer on a daemon location
- **WHEN** the only location is daemon-owned and the Task's reviewer is a native Agent
- **THEN** admission fails with `placement_unavailable` and filter `native_backend_unsupported`

### Requirement: Sticky placement and subtask inheritance
Once preparation succeeds, a placement SHALL NOT move to another owner. A retry, re-review, conflict fix, or recovery for the Task SHALL reuse the existing placement. A subtask that reuses its root's workspace SHALL use the root's placement with `selected_by = inherited`. If the subtask's resolved Agent cannot run on that owner, the subtask SHALL fail admission. A placement in `reserved` MAY expire and be reselected.

#### Scenario: Re-review after fix stays on the owner
- **WHEN** a daemon-owned Task goes back to the coder after a failed review and is claimed again
- **THEN** the same placement, owner, and workspace handle are used

#### Scenario: Subtask inherits root placement
- **WHEN** a subtask is dispatched and reuses its root's ready workspace owned by daemon D
- **THEN** its executions are sent to D and no new workspace is prepared

### Requirement: Disconnect and reconciliation
When a workspace owner daemon or a remote execution provider goes offline or requires a protocol upgrade, its `ready` placements SHALL become `disconnected` (including server-owned workspaces executed on a remote daemon through a shared mount), and their Tasks SHALL NOT be dispatched or recreated elsewhere. While a placement is `disconnected`, the leases of its running executions SHALL NOT expire through heartbeat loss, and notifications for them SHALL NOT be rejected as expired. A configured `max_disconnect` bound SHALL end the wait by failing the placement and its execution with cause `owner_disconnected_timeout`. The execution's hard deadline still applies. On reconnect, the server SHALL call `workspace.describe` for each disconnected placement, drain journaled terminal and cleanup results, and settle every running execution: resume the lease of one the daemon reports active, apply the journaled terminal of one it reports finished, and fail one it does not know with `owner_lost_execution`. It SHALL then compare the reported head with recorded evidence, and only after that return the placement to `ready` and wake dispatch. Reconciliation SHALL be retried by a periodic sweep over `disconnected` placements with an online owner, so an interrupted reconciliation completes. Placement and transport failure causes SHALL NOT spend the Task retry budget. A disconnected placement SHALL be visible in Task responses and operator status, and the user SHALL be able to wait, retry on the same owner, or cancel.

#### Scenario: Reconnect reconciles without duplicates
- **WHEN** daemon D disconnects mid-execution and reconnects after the CLI finished
- **THEN** the journaled terminal result is applied once
- **AND** no second execution or worktree is created

#### Scenario: Outage longer than the heartbeat lease
- **WHEN** daemon D is offline for ten minutes while its CLI keeps running and commits, then reconnects and replays the terminal report
- **THEN** the lease was never expired during the outage
- **AND** the terminal report and its commit are accepted once, and no recovery execution was started during the outage

#### Scenario: Interrupted reconciliation is retried
- **WHEN** the server restarts after draining a disconnected placement's journal but before marking it `ready`
- **THEN** the reconciliation sweep completes it without applying any result twice

#### Scenario: Offline owner does not fall back
- **WHEN** a Task's owner daemon is offline and the Task becomes dispatchable
- **THEN** the Task waits with placement state `disconnected` and an attention item

### Requirement: Owner-acknowledged cleanup
Workspace cleanup SHALL go through the placement's backend. A placement SHALL move to `cleaned` only after the owner acknowledges removal. While the owner is unreachable, cleanup SHALL remain `cleaning` and SHALL be retried on reconnect.

#### Scenario: Cleanup while owner is offline
- **WHEN** a merged Task's cleanup is due and the owner daemon is offline
- **THEN** the placement stays `cleaning`
- **AND** it becomes `cleaned` only after the daemon reconnects and acknowledges

### Requirement: Backend parity for server-owned workspaces
Server-owned placements SHALL retain the same worktree location, merge outcomes, review prompts, CI step semantics, and cleanup timing. A server-owned workspace executed on a remote daemon SHALL freeze its heartbeat lease on disconnect until reconciliation or `workspace.max_disconnect_seconds`, as daemon-owned placements do. This exception prevents a second execution from starting in a live worktree on a shared mount while the original CLI may still be writing. Existing workspaces SHALL be backfilled as server-owned placements without data loss.

#### Scenario: Existing workspace after upgrade
- **WHEN** Forge starts on a database with ready workspaces created before this change
- **THEN** each has a server-owned placement with `selected_by = backfill` and its handle equal to its previous worktree path
- **AND** its next execution runs unchanged

#### Scenario: Remote execution on a server-owned shared mount disconnects
- **WHEN** the remote provider of a server-owned workspace disconnects
- **THEN** its placement becomes `disconnected` and heartbeat leases freeze until reconciliation or `max_disconnect`
- **AND** no second execution starts in the shared worktree while its original writer may still be live
