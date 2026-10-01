## ADDED Requirements

### Requirement: Workspace protocol capability
The daemon protocol SHALL advance to revision 3 and add the handshake capability `workspace.v1`. The server SHALL keep accepting revision 2 daemons for execution and filesystem browsing on server-owned placements, SHALL mark them `workspace_incapable`, and SHALL NOT place a workspace on them.

#### Scenario: Old daemon connects
- **WHEN** a revision 2 daemon completes the handshake
- **THEN** it stays connected and reports `workspace_incapable`
- **AND** placement admission rejects it with filter `workspace_protocol_missing`

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
Every mutating workspace request SHALL carry the workspace handle or placement id, a unique `operation_id`, the placement `generation`, and an expected base SHA or version. The daemon SHALL journal each operation's result under its workspace root. It SHALL return the recorded result for a repeated `operation_id`, and SHALL reject a request with a stale generation (`stale_generation`) or for a placement it does not own (`wrong_owner`).

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
The daemon SHALL replay unacknowledged `execution.terminal` and `workspace.cleanup` results after reconnecting, until the server acknowledges each one. The server SHALL apply each result at most once.

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
For a daemon-owned placement, the daemon SHALL read the execution's outbox after the CLI exits and embed its bounded worklog and evidence entries in the `execution.terminal` report. The server SHALL ingest outbox entries from the terminal report for every placement, and SHALL NOT read the outbox from its own filesystem for a daemon placement. Entries SHALL be retained, replayed, and acknowledged together with the report.

#### Scenario: Remote worklog delivered
- **WHEN** a CLI Agent on daemon D writes worklog and evidence entries to its outbox and exits
- **THEN** the server records those entries from D's terminal report
- **AND** a replayed report after reconnect records them exactly once

### Requirement: Single daemon journal and capability facts
Terminal reports, workspace operation results, and cleanup acknowledgements SHALL share one daemon-side durable journal and one acknowledgement protocol (`journal.ack`). The handshake SHALL carry per-executor adapter capability facts (`structured_events`, `usage`, `resume`, `cancel_ack`, `terminal_observed`), where an absent fact means unsupported. The server SHALL offer `resume_session` recovery for a daemon placement only when the owner is online and reports `resume` for the snapshot's executor.

#### Scenario: Resume unsupported on owner
- **WHEN** a daemon-owned execution fails and the owner's CLI does not report `resume`
- **THEN** the recovery actions for that Task omit `resume_session`
