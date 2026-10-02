## MODIFIED Requirements

### Requirement: Placement selection
Admission SHALL choose among `ready` repository locations of the Task's repository. A candidate SHALL pass all hard filters: the owner is reachable and visible to the Task owner; a daemon owner has negotiated `workspace.v1`; the Agent's executor is installed, authenticated, and enabled on the owner, and the owner's advertised adapter capability facts for that executor cover the role; the owner's run policy allows every `workspace.run` purpose that the Task's review, hook, and environment configuration needs (filter `run_purpose_denied`); the owner's machine is recorded as environment-ready for the Task's Project (filters `environment_not_ready` and `environment_probe_pending`); an Agent `daemon_id` pin restricts candidates to that daemon; Agent and daemon capacity are available; and the first-slice limits hold. Among passing candidates the order SHALL be: existing placement for this workspace, inherited root placement, Agent pin, location marked default, server-owned, then `(created_at, id)`. When no candidate passes, admission SHALL fail with `placement_unavailable` listing each rejection and SHALL NOT fall back to another owner.

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

#### Scenario: The machine that has the code and the toolchain wins
- **WHEN** a repository has ready locations on the server and on daemon D, an unpinned Codex Agent is available on both, and D's readiness for the Project is `not_ready` because its `cargo` check failed
- **THEN** the placement selects the server
- **AND** the selection reason lists D with filter `environment_not_ready` and check `cargo`

#### Scenario: Pinned Agent on an unfit machine
- **WHEN** an Agent is pinned to daemon D and D's readiness for the Project is `not_ready`
- **THEN** claim fails with `placement_unavailable` listing D with `environment_not_ready`
- **AND** no workspace is prepared on D and no other owner is used

## ADDED Requirements

### Requirement: Environment readiness is a placement filter
The system SHALL record, for each Project and each machine (the server host or one daemon runtime), an environment readiness of `ready`, `not_ready`, or `unknown`, together with a digest of the Project's environment `env` and `checks`, the failing check names with a bounded output tail, and the check times. Selection SHALL only read this record. A candidate whose machine is `not_ready` for the current digest SHALL be rejected with `environment_not_ready`. A candidate whose record is missing, `unknown`, or for an older digest SHALL be rejected for that attempt with the retryable filter `environment_probe_pending`, and the system SHALL start one probe for that Project and machine outside the admission transaction. A Project with no environment checks SHALL treat every machine as `ready` and SHALL NOT probe. Automatic dispatch SHALL keep a Task queued while its only refusals are `environment_probe_pending`, without changing the Task version after the first deferral, and SHALL be woken when a probe completes.

#### Scenario: First dispatch probes, then places
- **WHEN** a Task is dispatched for a Project with checks and no readiness record exists for the server
- **THEN** the attempt is deferred with `environment_probe_pending` and a probe starts on the server
- **AND** when the probe passes, the Task is placed on the server without any user action

#### Scenario: Settings edit invalidates readiness
- **WHEN** the owner adds a check to the Project environment
- **THEN** every machine's readiness for the Project is `unknown` until it is probed with the new digest

#### Scenario: Project without checks
- **WHEN** a Project declares no environment checks
- **THEN** placement behaves exactly as before this change and no readiness rows exist for the Project

#### Scenario: Concurrent admissions share one probe
- **WHEN** three Tasks of the same Project are dispatched while the server's readiness is `unknown`
- **THEN** exactly one probe runs on the server

### Requirement: Readiness probe runs the Project's checks on the machine
A probe SHALL run on the machine it is about. On a machine that has a `ready` location for the Project's repository, it SHALL run every configured check with the Project `env` in that location's checkout, without writing to the checkout, and record `scope_covered = full`. On a machine with no location it SHALL run only checks with `scope = machine`, in an empty scratch directory inside the machine's workspace root, and record `scope_covered = machine`. Check timeouts SHALL keep the existing 1–300 second bounds. A probe result SHALL be written with a version check so a newer digest or a newer result wins. A change to the Project environment SHALL start probes on the machines that have a readiness record or a `ready` location, without waiting for a Task.

#### Scenario: Daemon without Rust
- **WHEN** daemon D has a ready location and the Project's `cargo` check exits 127 there
- **THEN** D's readiness is `not_ready` with failing check `cargo` and its output tail

#### Scenario: Stale probe result is discarded
- **WHEN** a probe started for digest A completes after the Project environment changed to digest B
- **THEN** the result is not recorded as the readiness for digest B

### Requirement: Provisioning a verified machine that lacks the code
When no candidate with a `ready` location passes the filters, the system SHALL consider provisioning candidates: daemon runtimes that have no location for the repository, where the repository has a remote URL, the daemon advertises `machine_probe.v1` and `repo_provision.v1`, its run policy allows the probe and provision purposes, the Agent's executor is installed, authenticated, and enabled, and the Project's `settings.placement.provision` is `when_verified` (the default). The system SHALL run the Project's `machine` checks on such a daemon and SHALL provision a managed clone there only when the Project declares at least one `machine` check and all of them pass. A daemon for a Project with no `machine` check SHALL be rejected with `environment_unverified`. After provisioning, the location SHALL be verified and the full checks SHALL run in the clone before the location is eligible. Provisioning SHALL run outside admission with single-flight ownership per repository and runtime; the Task SHALL wait with `environment_probe_pending`. With `settings.placement.provision = never`, no provisioning candidate SHALL be considered.

#### Scenario: Codex exists only on the second machine and it has the toolchain
- **WHEN** the server lacks Codex, daemon D has Codex, no location, and passes the Project's `machine` checks `cargo` and `node`
- **THEN** a managed clone is provisioned on D, verified, and fully checked
- **AND** the Task is then placed on D with the provisioned location

#### Scenario: Second machine lacks the toolchain
- **WHEN** the server lacks Codex, and daemon D has Codex but fails the `cargo` machine check
- **THEN** no clone is created on D
- **AND** claim fails with `placement_unavailable` listing the server with `executor_unavailable` and D with `environment_not_ready` and check `cargo`

#### Scenario: No machine checks declared
- **WHEN** the only machine with the Agent's executor has no location and the Project declares no `machine` check
- **THEN** no clone is created
- **AND** `placement_unavailable` lists that machine with `environment_unverified` and says to declare a machine check or register a location

#### Scenario: A ready location always wins over provisioning
- **WHEN** the server has a ready location and passes all filters, and daemon D could be provisioned
- **THEN** the Task is placed on the server and nothing is cloned to D

#### Scenario: Repository without a remote
- **WHEN** the repository has no remote URL
- **THEN** no daemon is a provisioning candidate for it

### Requirement: Placement refusals for environment are visible
Task responses and the Task workspace placement panel SHALL show, for each rejected machine, the environment filter code and the failing check names. `placement_unavailable` errors SHALL carry the same detail. While a Task waits on `environment_probe_pending`, its dispatch state SHALL say which machine is being checked.

#### Scenario: Owner reads why a Task is not running
- **WHEN** a Task cannot be placed because daemon D failed `cargo` and the server has no Codex
- **THEN** the Task page lists "server: executor unavailable" and "D: environment not ready (cargo)"
