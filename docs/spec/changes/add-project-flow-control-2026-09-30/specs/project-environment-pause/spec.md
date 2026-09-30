## ADDED Requirements

### Requirement: Environment check failure pauses the Project
When a Project environment check fails immediately before an execution launches, the system SHALL fail that execution before any provider call and SHALL pause the Project with `system_pause_reason = "environment_not_ready"`. It SHALL NOT add a blocking annotation to the Task. The Task SHALL keep its workflow state and SHALL be re-dispatched by the dispatcher once the Project resumes. The system SHALL record the pause detail (failing check names, the role that triggered them, the bounded output tail, `paused_at`, `last_checked_at`, `next_check_at`) and return it on the Project response as `environment_pause`.

#### Scenario: Disk check fails before a coder launch
- **WHEN** NovelKit's `disk` check exits 1 with "root free: 7G" before a coder execution for NK-24
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

### Requirement: Periodic re-check resumes the Project automatically
While a Project is paused for `environment_not_ready`, the dispatcher SHALL re-run the recorded failing checks, with the Project `env`, in the Project's primary checkout, once `next_check_at` is reached. The interval SHALL be `settings.environment.recheck_interval_seconds`, default 600, valid range 60–86400. When every recorded check passes, the system SHALL clear the pause using the same compare-and-clear guard as the repository pause, and SHALL publish a Project resumed event. When a check still fails, it SHALL update `last_checked_at`, `next_check_at`, and the output tail.

#### Scenario: Disk recovers
- **WHEN** the Project was paused for `disk` and the next scheduled re-check reports "root free: 17G"
- **THEN** the pause is cleared automatically
- **AND** on the next dispatcher tick NK-24, NK-28, NK-35, NK-37, NK-38 and NK-47 re-dispatch in their current states without any manual recovery

#### Scenario: Check still failing
- **WHEN** the re-check fails again
- **THEN** the Project stays paused, and `next_check_at` moves forward by the configured interval

#### Scenario: Invalid interval refused
- **WHEN** a client PATCHes `recheck_interval_seconds: 5`
- **THEN** the request is refused as invalid

### Requirement: On-demand environment re-check
The system SHALL expose `POST /api/v1/projects/{id}/environment/recheck`. It runs every configured check immediately and returns each check's result. If the Project is environment-paused and all checks pass, it resumes the Project. A manual Project resume SHALL also clear an environment pause. If the environment is still broken, the next launch re-pauses the Project.

#### Scenario: Owner checks now after freeing disk
- **WHEN** the owner frees disk and calls the recheck endpoint
- **THEN** the response lists `disk` as passed and the Project is resumed

### Requirement: Environment pause is visible
The web UI SHALL show an "Environment paused" label on the Project card and Project header. It SHALL show the failing check names, the output tail, when the next check runs, and a "Check now" action. `forge-ctl` SHALL expose `project env-recheck <project>`.

#### Scenario: Owner opens a paused Project
- **WHEN** the owner opens NovelKit while it is environment-paused
- **THEN** the header shows "Environment paused · disk · root free: 7G · next check in 8m" with a "Check now" button

### Requirement: Legacy environment blocks migrate to the Project pause
The upgrade migration SHALL clear Task blocking annotations of kind `environment_not_ready`, so those Tasks re-enter normal dispatch. If the environment is still broken, the first launch attempt pauses the Project under the new rule.

#### Scenario: Upgrade with stranded Tasks
- **WHEN** a database with six `environment_not_ready`-blocked Tasks is migrated
- **THEN** the six Tasks have no blocking annotation afterwards, and the dispatcher considers them on its next tick
