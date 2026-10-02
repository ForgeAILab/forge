---
updated_at: 2026-10-02T13:25:40Z
---

## MODIFIED Requirements

### Requirement: Placement selection
Admission SHALL choose among `ready` repository locations of the Task's repository. A candidate SHALL pass all hard filters: the owner is reachable and visible to the Task owner; a daemon owner has negotiated `workspace.v1`; the Agent's executor is installed, authenticated, and enabled on the owner, and the owner's advertised adapter capability facts for that executor cover the role; the owner's run policy allows every `workspace.run` purpose that the Task's review, hook, and environment configuration needs (filter `run_purpose_denied`); the owner's readiness passes the role-aware environment filter for this Task (filters `environment_not_ready` and `environment_probe_pending`); an Agent `daemon_id` pin restricts candidates to that daemon; Agent and daemon capacity are available; and the first-slice limits hold. Among passing candidates the order SHALL be: existing placement for this workspace, inherited root placement, Agent pin, location marked default, server-owned, then `(created_at, id)`. If a candidate that would outrank the best passing candidate is rejected only for `environment_probe_pending`, admission SHALL defer rather than divert to a lower-preference owner. When no candidate passes, admission SHALL fail with `placement_unavailable` listing each rejection and SHALL NOT fall back to another owner.

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
The system SHALL record per Project and workspace-owner machine (the server
host or daemon runtime) `ready`, `not_ready` or `unknown`, a digest of `env` and
`checks`, per-check results, bounded output and check times. Selection SHALL
only read this context. A current failure SHALL reject only if a named failing
check applies to the role being launched through
`EnvironmentCheck::applies_to`; an unnamed launch failure applies to its
recorded role. Rejections SHALL include applicable failing check names.

In backend build step A only the host SHALL be probed proactively. Missing,
unknown or stale host readiness SHALL yield transient `environment_probe_pending`
only for dispatcher-initiated admission of check-only Projects. Direct/manual
claims SHALL proceed and launch-time preflight SHALL decide. Projects with
assets SHALL use launch preflight instead of primary-checkout probe facts,
which cannot see staged assets; actual launch failures SHALL still reject.
For daemon candidates, only current applicable `not_ready` rejects; missing,
unknown and stale rows SHALL pass identically at reserve and claim. This policy
SHALL live in one function, removed when step 3 introduces `machine.probe`.
Probing through another Task's live daemon workspace SHALL NOT be used.
Projects without checks SHALL NOT probe or create rows. Automatic probe
deferral SHALL leave the Task queued with no repeated version change and
completion SHALL wake dispatch through the in-process dispatcher kick.

#### Scenario: First dispatch probes, then places
- **WHEN** a Task is dispatched for a Project with checks and no readiness record exists for the server
- **THEN** the attempt is deferred with `environment_probe_pending` and a probe starts on the server
- **AND** when the probe passes, the Task is placed on the server without any user action

#### Scenario: Settings edit invalidates readiness
- **WHEN** the owner adds a check to the Project environment
- **THEN** every machine's readiness becomes `unknown`; the host is re-probed with the new digest, and daemon candidates pass until launch-time results (until step 3)

#### Scenario: Project without checks
- **WHEN** a Project declares no environment checks
- **THEN** placement behaves exactly as before this change and no readiness rows exist for the Project

#### Scenario: Concurrent admissions share one probe
- **WHEN** three Tasks of the same Project are dispatched while the server's readiness is `unknown`
- **THEN** exactly one probe runs on the server

### Requirement: Readiness probe runs the Project's checks on the machine
A probe SHALL run on the machine it is about, outside admission with
single-flight ownership and version/digest fences. In build step A only the
host SHALL be probed for Projects without environment assets: run every configured check with Project env in the
repository's server checkout and retain per-check results for role-aware
selection. Commands must be read-only; output SHALL be bounded while collected
and redacted before storage. Completion SHALL immediately wake dispatch. A
digest-changing settings edit SHALL start host probes for existing host rows
or ready host locations without waiting for a Task. If an edit collides with an older probe flight, completion SHALL schedule
the current digest without requiring a queued Task. A passing probe SHALL
compare-and-clear a matching environment pause using the current Project
snapshot after its result; it SHALL preserve intervening user/repository pauses. Step 3 extends probes to
daemons through `machine.probe`, with full checks in a ready checkout or
`scope = machine` checks in scratch space. Timeouts SHALL remain 1–300 seconds.

#### Scenario: Daemon without Rust
- **GIVEN** step 3 daemon probes are available; in build step A this fact is recorded at launch
- **WHEN** daemon D has a ready location and the Project's `cargo` check exits 127 there
- **THEN** D's readiness is `not_ready` with failing check `cargo` and its output tail

#### Scenario: Stale probe result is discarded
- **WHEN** a probe started for digest A completes after the Project environment changed to digest B
- **THEN** the result is not recorded as the readiness for digest B

#### Scenario: Daemon launch facts are the only readiness gate before machine probe
- **WHEN** a daemon candidate has no row, an unknown row, or a stale not-ready row in build step A
- **THEN** both reserve and claim pass its environment filter and no proactive daemon command is sent
- **AND** a current applicable not-ready row rejects at both stages

#### Scenario: Preferred server probe does not divert work
- **WHEN** the server would outrank a passing daemon but is rejected only for environment probe pending
- **THEN** the Task stays queued until the server probe completes

#### Scenario: Failure scoped to another role
- **WHEN** only a reviewer-scoped check fails and the Task launches a coder role, regardless of the assigned reviewer Agent
- **THEN** the candidate passes the environment filter and that failure does not pause this Task's Project


#### Scenario: Direct claim with an unverified host
- **WHEN** a direct or manual claim has checks and no current host readiness record
- **THEN** it succeeds on its first attempt and launch-time preflight runs the checks

#### Scenario: A check reads staged assets
- **WHEN** configured assets are staged into the Task workspace and a check reads them
- **THEN** primary-checkout probes do not gate placement and the check runs after staging at launch

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
