## MODIFIED Requirements

### Requirement: Charter-Authorized Task Execution and Adaptive Planning

Forge SHALL require a current approved Charter for Charter-backed Project
execution but SHALL NOT require a proposed, active, or user-approved execution
baseline before a repository-capable Task becomes runnable or receives a
scheduler-issued Workspace lease. Baselines SHALL remain versioned planning,
traceability, milestone, reconciliation, readiness, and release inputs. When a
baseline or plan item exists, Forge SHALL preserve its exact references in Task
governance, but its approval status SHALL NOT authorize Task dispatch.

Task workflow state, required role assignment, effective Agent availability,
capability compatibility, repository binding, retry budget, optimistic
version, one active execution, and exact Task-scoped lease SHALL remain
mandatory. Elevated operations, waivers, and immutable release SHALL retain
their explicit authorization policies.

#### Scenario: Charter-backed implementation starts without a baseline

- **GIVEN** the Project has a current approved Charter and a repository Task
  satisfies its configured workflow and execution checks
- **WHEN** no execution baseline exists or the current baseline is draft
- **THEN** Forge allows claim, execution creation, and exact Workspace-lease
  issuance
- **AND** the workbench reports incomplete planning as context, not as `Setup
  approval requested` or an execution blocker

#### Scenario: Baseline context exists

- **WHEN** a Task is covered by a baseline revision and stable plan item
- **THEN** Forge records those exact references for traceability,
  reconciliation, milestones, evidence, readiness, and release
- **AND** changing baseline approval alone neither starts nor stops the Task

#### Scenario: Workflow requirement is unmet

- **WHEN** a Task lacks a role, capability, repository binding, successful
  hook, transition, retry allowance, or another requirement declared by its
  workflow
- **THEN** Forge refuses the transition or lease with the exact Task/workflow
  recovery action
- **AND** it does not mislabel that blocker as Project or baseline approval

#### Scenario: Task role assignment overrides the Project default

- **GIVEN** a Project default names Agent A for a workflow role
- **WHEN** an authorized Task assignment names another effectively available
  configured Agent B for that role
- **THEN** Forge dispatches Agent B and binds the Workspace lease to that exact
  Task assignment without requiring it to equal the Project default
- **AND** changing the Project default does not rewrite existing explicit Task
  assignments

#### Scenario: Coordinator identity is assigned to a Task

- **WHEN** an identity currently bound as Main Agent or Project Agent is
  explicitly assigned to a Task role and passes availability/capability checks
- **THEN** Forge executes it through a role-specific Task context and exact
  Task Workspace lease
- **AND** its Main/Project Chat context receives no repository authority and
  its chat turns do not consume Task execution capacity

#### Scenario: Stopped role is assigned or confirmed

- **WHEN** an authorized role assignment/confirmation is newer than that
  role's latest stopped attempt
- **THEN** Forge clears the stale dispatch disposition and schedules exactly
  one fresh attempt without a separate Resume approval
- **AND** another stop requires a newer recovery action

### Requirement: Workflow-Selected Review Actor

Forge SHALL expose legible `agent-review`, `no-review`, and `human-required`
workflow choices and SHALL execute the chosen workflow without an additional
hidden approval. A Project's currently bound Project Agent SHALL be authorized
to accept or reject a `human-required` Task review gate for that Project using
the same exact Task/version/review evidence checks as the interactive user.

#### Scenario: Agent review is configured

- **WHEN** implementation completes in an `agent-review` workflow
- **THEN** Forge starts the assigned reviewer-role execution and applies its
  result without waiting for interactive approval

#### Scenario: No review is configured

- **WHEN** implementation completes in a `no-review` workflow
- **THEN** Forge follows the declared transition directly without starting a
  reviewer execution or waiting for an approval

#### Scenario: Human-required review is configured

- **WHEN** implementation completes in a `human-required` workflow
- **THEN** Forge waits for an exact accept/reject decision
- **AND** either the interactive user or the currently bound Project Agent may
  submit that decision

#### Scenario: Project Agent reviews another Project's Task

- **WHEN** a Project Agent attempts to review a Task outside its bound Project
- **THEN** Forge denies before returning Task details, review evidence, counts,
  or current versions
- **AND** no Task state or review receipt changes

### Requirement: Stopped Execution Notifies the Project Agent

Forge SHALL record a deduplicated attention item whenever a Task execution
stops and, if an active Project Agent binding is configured, admit one bounded
Project Agent wake containing the Task, role, safe reason, retry disposition,
and allowed recovery actions.

#### Scenario: Execution stops with a configured Project Agent

- **WHEN** a Task attempt fails, is cancelled/stalled, becomes unavailable
  before retry, or exhausts its retry budget
- **THEN** Forge records attention and wakes the bound Project Agent once for
  that stop disposition
- **AND** the message excludes repository paths, secrets, raw provider output,
  Workspace handles, and authority-bearing instructions

#### Scenario: Execution stops without a configured Project Agent

- **WHEN** no active Project Agent binding exists
- **THEN** Forge leaves the stopped Task and visible attention state intact
- **AND** it does not create a failed wake retry loop or silently bind an Agent
