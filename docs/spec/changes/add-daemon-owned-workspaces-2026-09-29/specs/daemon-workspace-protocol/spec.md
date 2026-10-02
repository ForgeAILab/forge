## ADDED Requirements

### Requirement: Workspace protocol capability
The daemon protocol SHALL remain at revision 3 and advertise `workspace.v1`. Plan transport SHALL be negotiated independently with `execution.plan_transport`. The server SHALL refuse every command RPC from daemons below revision 3, including execution, verification, filesystem browsing, and PTY terminals, with the actionable `daemon_upgrade_required` reason. Their sockets SHALL remain visible for upgrade diagnostics. A socket awaiting its handshake SHALL be reported as not ready.

#### Scenario: Old daemon connects
- **WHEN** a daemon below revision 3 completes the handshake
- **THEN** its socket remains visible and receives `daemon_upgrade_required`
- **AND** otherwise eligible upgrade-only admission, with no transient or capacity alternative, rejects it with filter `daemon_upgrade_required`, without creating an Execution
- **AND** dispatch records the human-action blocker and clears it automatically after that owner's supported handshake

### Requirement: Operation-level workspace RPCs
A `workspace.v1` daemon SHALL implement `repo_location.verify`, `workspace.prepare`, `workspace.describe`, `workspace.run`, `workspace.diff`, `workspace.read`, `workspace.merge`, `workspace.reset`, and `workspace.cleanup`. `workspace.run` SHALL accept only the purposes `environment_setup`, `hook`, and `ci_step`, with commands from server-side Task or Project configuration. It SHALL run them with the same shell semantics, time limit, and output cap as the embedded backend. The protocol SHALL NOT offer a general command-execution method.

#### Scenario: Remote CI step
- **WHEN** a daemon-owned Task reaches review with CI steps configured
- **THEN** each step runs through `workspace.run` with purpose `ci_step` in the daemon's worktree
- **AND** the exit code and bounded output are recorded as they are for server workspaces

#### Scenario: Remote direct merge
- **WHEN** a daemon-owned direct-merge Task passes review
- **THEN** `workspace.merge` merges the task branch into the daemon's verified primary checkout
- **AND** returns the merged SHA or the same conflict, dirty, and target-dirty outcomes as the embedded backend

### Requirement: Idempotent, fenced mutations
Every mutating workspace request SHALL carry the workspace handle or placement id, a unique `operation_id`, the placement `generation`, and an expected base SHA or version. The daemon SHALL journal each operation's result under its workspace root. It SHALL return the recorded result for a repeated `operation_id` until acknowledgement, delete the receipt on ack after the server durably stores it, and SHALL reject a request with a stale generation (`stale_generation`) or for a placement it does not own (`wrong_owner`).

#### Scenario: Retried prepare after timeout
- **WHEN** the server resends `workspace.prepare` with the same `operation_id` after a transport timeout
- **THEN** the daemon returns the original result and creates no second worktree

#### Scenario: Stale generation
- **WHEN** a request carries a generation lower than the daemon's current one for that handle
- **THEN** the daemon rejects it with `stale_generation` and changes nothing

### Requirement: Daemon workspace confinement
A daemon SHALL map workspace handles only to directories it created under its own `workspace_root`. It SHALL reject any request whose resolved path escapes that root. `workspace.merge` SHALL write only to a `primary_checkout` location the daemon has verified, and SHALL refuse a dirty target.

#### Scenario: Handle path escape
- **WHEN** a request references a handle whose mapped path resolves outside the workspace root
- **THEN** the daemon rejects it and performs no filesystem change

### Requirement: Journal replay on reconnect
The daemon SHALL replay unacknowledged `execution.terminal` and `workspace.cleanup` results after reconnecting, until the server acknowledges each one. The server SHALL apply each result at most once. Reports for deleted executions SHALL be acknowledged. Cleaned placements SHALL treat an acknowledged, retired handle as absent.

#### Scenario: Cleanup ack after reconnect
- **WHEN** a daemon removes a worktree while disconnected from the server
- **THEN** it replays the cleanup result on reconnect and the placement becomes `cleaned` once

### Requirement: Daemon-side run policy
A daemon SHALL enforce a local run policy, from its own configuration, that lists which `workspace.run` purposes it accepts; the default SHALL be `ci_step` only. The daemon SHALL advertise the effective policy in the handshake and SHALL refuse a disallowed purpose with `purpose_denied`, which the server SHALL NOT retry. The server SHALL NOT be able to change a daemon's run policy.

#### Scenario: Hook refused by the daemon
- **WHEN** a daemon allows only `ci_step` and a Project hook targets a Task placed on it
- **THEN** placement admission rejects that daemon with filter `run_purpose_denied`
- **AND** no hook command is sent to it

### Requirement: Execution outbox travels with the terminal report
For a daemon-owned placement, the daemon SHALL read the execution's outbox after the CLI exits and embed its bounded plan candidate, worklog and evidence entries in the `execution.terminal` report. The server SHALL ingest outbox entries from the terminal report for every placement, and SHALL NOT read the outbox from its own filesystem for a daemon placement. Content SHALL be retained, replayed, and acknowledged together with the report.
The server SHALL resolve start plan text through the workspace owner with
`task.plan` as fallback, and send `execution.start.plan_text`. Only implementation roles SHALL receive a valid checklist seed; planners SHALL NOT be seeded. The daemon SHALL prepare its plan outbox
locally. A successful terminal CAS SHALL preserve `execution.terminal.plan_text`
in private execution artifact storage and publish those exact bytes through the owner only
after the existing publication claim succeeds. Publication, compare-safe
rollback and cleanup SHALL be owner-aware. Remote plan content SHALL be bounded
to 128 KiB; capture I/O or excess size SHALL produce a clear terminal failure,
without overwriting an already failed/cancelled outcome. Empty, checklist-free or
unchanged-seed content SHALL use the ordinary workflow guard rejection.
Plan-writing roles SHALL require `execution.plan_transport` at placement and SHALL
NOT be silently parked with an owner-unsupported dispatch disposition.

#### Scenario: Remote worklog delivered
- **WHEN** a CLI Agent on daemon D writes worklog and evidence entries to its outbox and exits
- **THEN** the server records those entries from D's terminal report
- **AND** a replayed report after reconnect records them exactly once

#### Scenario: Implementation starts on an existing daemon workspace
- **WHEN** a coder or worker is dispatched to an existing daemon-owned workspace
- **THEN** an Execution is created and `execution.start` contains its owner-read canonical plan or `task.plan` fallback
- **AND** no server directory is created at the daemon's path

#### Scenario: First implementation dispatch
- **WHEN** a coder is first dispatched and its workspace is prepared by a daemon
- **THEN** `execution.start` carries the plan seed and the daemon creates its own private plan outbox
- **AND** no server plan outbox is created for the daemon's path

#### Scenario: Planner completion and reconnect replay
- **WHEN** a daemon planner finishes a revised checklist and its terminal report is replayed after reconnect
- **THEN** the plan content travels with the terminal report and the winning execution publishes it once through its owner
- **AND** normal planning completion or human approval follows without filesystem sync

#### Scenario: Completion, send-back and inherited workspace
- **WHEN** a daemon coder completes and its checklist passes the completion guard
- **THEN** its Task enters review and a subtask reusing that root workspace can dispatch with the same owner plan
- **AND** a guard rejection or abandoned publication restores the prior plan through the owner before re-planning or send-back dispatch

#### Scenario: Plan transport unsupported at placement
- **WHEN** a revision-3 daemon lacks `execution.plan_transport` and a planner is admitted
- **THEN** placement rejects it with `capability_missing` and creates no Execution
- **AND** older protocol revisions receive `daemon_upgrade_required`

#### Scenario: Remote candidate exceeds its bound
- **WHEN** a daemon plan-writing execution produces more than 128 KiB of plan text
- **THEN** its retained terminal report explicitly fails plan capture and does not publish truncated plan content

### Requirement: Single daemon journal and capability facts
Terminal reports, workspace operation results, and cleanup acknowledgements SHALL share one daemon-side durable journal and one acknowledgement protocol (`journal.ack`). The handshake SHALL carry per-executor adapter capability facts (`structured_events`, `usage`, `resume`, `cancel_ack`, `terminal_observed`), where an absent fact means unsupported. The server SHALL offer `resume_session` recovery for a daemon placement only when the owner is online and reports `resume` for the snapshot's executor.

#### Scenario: Resume unsupported on owner
- **WHEN** a daemon-owned execution fails and the owner's CLI does not report `resume`
- **THEN** the recovery actions for that Task omit `resume_session`

#### Scenario: Capability refusal remains visible until eligibility changes
- **WHEN** dispatch finds only machines missing `execution.plan_transport`
- **THEN** the Task records the machine, missing capability and upgrade instruction
- **AND** unchanged scans do not retry placement; eligible capability facts clear the refusal
- **AND** reviewer, interactive, shared-mount, filesystem and PTY use continue at revision 3

#### Scenario: Seed parity and unwritten plan
- **WHEN** an implementation role starts with prose/empty fallback or a planner starts with an old checklist
- **THEN** neither receives a plan seed
- **AND** a returned unchanged implementation seed, missing required plan, empty plan or checklist-free plan uses the bounded guard rejection

#### Scenario: Private terminal storage and replay
- **WHEN** a winning terminal report contains a plan with a Project environment secret
- **THEN** its redacted content is stored privately in the terminal transaction
- **AND** snapshots, retry configuration, Execution API responses and receipt bodies exclude the candidate
- **AND** replay cannot replace the winning artifact; receipt metadata may contain its digest and length

#### Scenario: Owner unavailable during settlement
- **WHEN** publication or restore cannot reach the owner
- **THEN** a durable runtime-offline wait and exponential retry backoff remain visible until success
- **AND** repeated scans do not resend before the retry deadline

#### Scenario: Plan settlement waits for checks
- **WHEN** publish or restore queues behind a long `workspace.run`
- **THEN** it waits for the owner lock without the ordinary command timeout failing the Task
- **AND** discard after workspace cleanup succeeds even when the handle has been retired
