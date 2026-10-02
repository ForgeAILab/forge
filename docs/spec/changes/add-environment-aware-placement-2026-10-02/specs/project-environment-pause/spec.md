## MODIFIED Requirements

### Requirement: Environment check failure pauses the Project
When a Project environment check fails immediately before an execution launches, the system SHALL fail that execution before any provider call and SHALL record the machine that ran the check as `not_ready` for the Project, with the failing check names, the role that triggered them, and the bounded output tail. It SHALL NOT add a blocking annotation to the Task. The Task SHALL keep its workflow state. The system SHALL pause the Project with `system_pause_reason = "environment_not_ready"` only when no machine remains eligible for the Project's work: every machine that has a `ready` location for its repository, or that could be provisioned, is `not_ready`. When it pauses, it SHALL record the pause detail (the machine, failing check names, the role that triggered them, the bounded output tail, `paused_at`, `last_checked_at`, `next_check_at`) and return it on the Project response as `environment_pause`. A Task whose workspace is already placed on a `not_ready` machine SHALL wait on that machine with a Task-scoped Attention item naming the machine and the checks, and SHALL be re-dispatched when that machine becomes `ready`. A Task with no placement SHALL be placed on another eligible machine.

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
While a machine is `not_ready` for a Project, the dispatcher SHALL re-run that machine's recorded failing checks, with the Project `env`, on that machine (in its repository location's checkout, or in the scratch directory when it has none), once the machine's `next_check_at` is reached. The interval SHALL be `settings.environment.recheck_interval_seconds`, default 600, valid range 60–86400. When every recorded check passes, the system SHALL mark the machine `ready`, wake dispatch, and, if the Project is paused for `environment_not_ready`, clear the pause using the same compare-and-clear guard as the repository pause and publish a Project resumed event. When a check still fails, it SHALL update `last_checked_at`, `next_check_at`, and the output tail. A machine that is unreachable SHALL keep its record and be re-checked when it is reachable.

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

### Requirement: On-demand environment re-check
The system SHALL expose `POST /api/v1/projects/{id}/environment/recheck`. It runs every configured check immediately on every machine that has a readiness record or a `ready` location for the Project, or on one machine when the request names it, and returns each check's result grouped by machine. A machine on which all checks pass SHALL become `ready`. If the Project is environment-paused and at least one machine becomes `ready`, it resumes the Project. A manual Project resume SHALL also clear an environment pause. If the environment is still broken, the next launch marks the machine `not_ready` again.

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
The upgrade migration SHALL create a `not_ready` readiness record for each Project that is paused for `environment_not_ready`, for the machine recorded in its pause detail, or for the server when none is recorded, with the recorded failing checks and `next_check_at`. The Project SHALL stay paused until a re-check passes or the owner resumes it.

#### Scenario: Upgrade while paused
- **WHEN** a database with NovelKit environment-paused for `disk` is migrated
- **THEN** NovelKit is still paused, the server has a `not_ready` record for `disk`, and the scheduled re-check resumes it when `disk` passes
