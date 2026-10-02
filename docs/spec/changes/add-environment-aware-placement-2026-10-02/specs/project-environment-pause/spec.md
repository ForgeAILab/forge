---
updated_at: 2026-10-02T10:47:57Z
---

## MODIFIED Requirements

### Requirement: Environment check failure pauses the Project
A failing pre-launch check SHALL terminalize its execution before a provider
call, record its workspace-owner machine not-ready with check results, role,
workspace and bounded output, and leave the Task's state without a blocking
annotation. Admission refusal and launch failure SHALL share one pause
decision in the context of the concrete Task and its worktree roles. When all
ready-location candidates that would otherwise be eligible are rejected only
for applicable `environment_not_ready`, compare-and-set the Project pause
without overwriting a user or repository pause. This SHALL happen before any
initial Task transition or `dispatch_failed` annotation, including a first
failing probe. The detail SHALL name the machine, checks, role, bounded output
and pause/check times; the existing public response shape stays unchanged in
build step A. Step 3 later adds provisioning candidates to eligibility.

Other eligible owners SHALL allow new Tasks to run there. A Task pinned by an
Agent or existing/inherited placement to the failed machine SHALL wait there
when another owner is healthy for other Tasks, including Tasks using another Agent or executor, without pausing the Project. The wait
SHALL have Task-linked Attention of an environment kind naming machine/checks,
no `task.execution_failed` event or `recover_task` recommendation, and count as
parked for Project slots. Readiness success or moving on SHALL clear it. A
single-machine Project pause SHALL be the sole signal with no additional Task
Attention. Single-machine behaviour SHALL match the base except a probe may
run first.

#### Scenario: Disk check fails before a coder launch
- **WHEN** NovelKit runs on one machine and its `disk` check exits 1 with "root free: 7G" before a coder execution for NK-24
- **THEN** the execution is failed with an "environment not ready" message and no provider call is made
- **AND** the Project is paused with `system_pause_reason = "environment_not_ready"` and `environment_pause.checks = ["disk"]`
- **AND** NK-24 stays `in_progress` with no blocking annotation

#### Scenario: Other Tasks stop launching while paused
- **WHEN** the Project is environment-paused
- **THEN** the dispatcher launches no new execution for any of its Tasks
- **AND** executions already running are left to finish

#### Scenario: A user or repository pause is never overwritten
- **WHEN** a check fails in a Project that is already paused by a user or for a repository reason
- **THEN** the existing pause and its reason are left unchanged

#### Scenario: One machine fails while another is ready
- **WHEN** the Project has ready locations on the server and on daemon D, both were `ready`, and the `disk` check fails on D before a launch
- **THEN** D is recorded `not_ready` for the Project and the Project is not paused
- **AND** new Tasks are placed on the server
- **AND** the Task whose workspace is on D waits with an Attention item naming D and `disk`

#### Scenario: Last eligible machine fails
- **WHEN** D is already `not_ready` and a check then fails on the server, the only other machine
- **THEN** the Project is paused with `environment_not_ready` and `environment_pause` names the server

### Requirement: Periodic re-check resumes the Project automatically
One due-row query SHALL drive scheduled re-checks of not-ready machines at
`next_check_at`, with Project env on that machine. Named failures SHALL re-run
recorded checks; unnamed failures SHALL run all configured checks applicable
to the recorded role. Host checks use its repository checkout. In build step A
daemon checks SHALL use only the failure's recorded ready placement through
existing `workspace.run`, never another Task's live workspace or a substitute
host. Later scoped probes can use machine scratch space. Intervals SHALL remain
600 by default, valid range 60–86400. Actual success SHALL mark ready, clear
matching waits, wake dispatch and compare-and-clear an unchanged environment
pause, publishing resumed. Failing results SHALL update facts and scheduling.
Every outcome SHALL advance the schedule; transport, unreachable, version-fence
or unusable-workspace errors SHALL retain facts and never fail another Task's
run. Version/digest fences SHALL discard stale results. A harmless Project-version change SHALL retry compare-and-clear only for the same pause epoch, digest and readiness result version; an intervening user/repository pause SHALL win.

#### Scenario: Disk recovers
- **WHEN** the Project was paused for `disk` and the next scheduled re-check reports "root free: 17G"
- **THEN** the pause is cleared automatically
- **AND** on the next dispatcher tick NK-24, NK-28, NK-35, NK-37, NK-38 and NK-47 re-dispatch in their current states without any manual recovery

#### Scenario: Check still failing
- **WHEN** the re-check fails again
- **THEN** the machine stays `not_ready`, and `next_check_at` moves forward by the configured interval

#### Scenario: Invalid interval refused
- **WHEN** a client PATCHes `recheck_interval_seconds: 5`
- **THEN** the request is refused as invalid

#### Scenario: The re-check runs where the failure was
- **WHEN** daemon D is `not_ready` for `cargo` and the server is `ready`
- **THEN** the scheduled re-check runs `cargo` on D, not on the server
- **AND** when it passes, the Task waiting on D is re-dispatched

#### Scenario: First failing host probe preserves the single-machine pause signal
- **WHEN** the only machine's first dispatch probe fails disk
- **THEN** the Project pauses, the Task retains its queued state with no annotation or Task Attention
- **AND** a passing scheduled re-check resumes and launches it automatically

#### Scenario: Manual resume retries immediately
- **WHEN** the owner resumes after a named or unnamed launch failure with a future next-check time
- **THEN** not-ready rows become unknown in the same transaction
- **AND** the next host admission probes and launches if fixed, or pauses again if still broken

#### Scenario: Unnamed failure is rechecked
- **WHEN** asset staging or run-purpose denial created a not-ready row with no failing check name
- **THEN** the timer runs all applicable checks; command errors keep the row and reschedule, and resume always permits another attempt

#### Scenario: Daemon transport error during re-check
- **WHEN** the recorded daemon workspace is unreachable or its command returns a transport or version-fence error
- **THEN** the row's readiness, checks and output are unchanged, next-check time advances, and no other Task run fails

### Requirement: On-demand environment re-check
The system SHALL expose `POST /api/v1/projects/{id}/environment/recheck`. It runs every configured check immediately on every machine that has a readiness record or a `ready` location for the Project, or on one machine when the request names it, and returns each check's result grouped by machine. A machine on which all checks pass SHALL become `ready`. If the Project is environment-paused and at least one machine becomes `ready`, it resumes the Project. A manual Project resume and existing clear paths intended to retry work SHALL reset not-ready rows to unknown in the same transaction as clearing the pause. A valid digest edit with checks remaining SHALL retire an obsolete environment pause during invalidation so unknown daemon facts can reach launch; removing every check retains the base manual-resume rule. The next host admission SHALL probe and launch or pause again without waiting for the old due time. Daemons SHALL retry at launch until step 3.

#### Scenario: Owner checks now after freeing disk
- **WHEN** the owner frees disk and calls the recheck endpoint
- **THEN** the response lists `disk` as passed for that machine and the Project is resumed

#### Scenario: Re-check one machine
- **WHEN** the owner installs Rust on daemon D and calls the recheck endpoint naming D
- **THEN** only D is checked, the response lists D's results, and D becomes `ready`

## ADDED Requirements

### Requirement: Environment checks declare a scope
Each environment check SHALL have a `scope` of `workspace` or `machine`, default `workspace`. A `machine` check SHALL NOT depend on a checkout: it is run in an empty scratch directory, with the Project `env` and without assets. A `workspace` check is run in a checkout. Existing checks SHALL read as `workspace` with no data change. The Project settings API SHALL validate the value, and the Project settings UI and `forge-ctl` SHALL let the owner set it.

#### Scenario: Toolchain check marked as machine
- **WHEN** the owner sets `scope: machine` on the check `cargo --version`
- **THEN** that check can be run on a machine that has no checkout of the repository

#### Scenario: Existing Project after upgrade
- **WHEN** a Project with three checks is migrated
- **THEN** all three read as `scope: workspace` and launch-time behaviour is unchanged

#### Scenario: Invalid scope refused
- **WHEN** a client PATCHes a check with `scope: "host"`
- **THEN** the request is refused as invalid

### Requirement: Per-machine readiness is visible
The Project response SHALL include `environment_readiness`: for each machine with a record, its name, status, failing checks with the output tail, `scope_covered`, `checked_at` and `next_check_at`. The Project settings page SHALL show this as a table with a "Check now" action per machine, and `forge-ctl` SHALL expose `project env-status <project>` and `project env-recheck <project> [--machine <id>]`. Machine identities SHALL be shown to admins; other users SHALL see the count of ready and not-ready machines.

#### Scenario: Owner sees which machine is unfit
- **WHEN** an admin opens the Project environment settings while daemon D is `not_ready`
- **THEN** the table shows "D · not ready · cargo · command not found · next check in 8m" and the server as ready

### Requirement: Existing environment pauses carry over
The upgrade migration SHALL create a `not_ready` readiness record for each Project with checks that is paused for `environment_not_ready`, for the machine recorded in its pause detail, or inferred from the old `workspace_id` placement, falling back to the server only when neither identifies a machine, with the recorded failing checks and `next_check_at`. An asset-only Project with no checks SHALL retain its pause without a readiness row. Malformed Project settings SHALL leave its readiness unknown and log once rather than fail startup. Existing Project and placement data SHALL be preserved. The Project SHALL stay paused until a re-check passes or the owner resumes it.

#### Scenario: Upgrade while paused
- **WHEN** a database with NovelKit environment-paused for `disk` is migrated
- **THEN** NovelKit is still paused, the server has a `not_ready` record for `disk`, and the scheduled re-check resumes it when `disk` passes
