---
created_at: 2026-08-26T19:20:36Z
updated_at: 2026-08-27T02:15:00Z
completed_at:
---

## Context

Forge already has three useful authorities: the approved Charter defines the
Project outcome, the Task workflow defines execution/review transitions, and a
scheduler-issued Workspace lease scopes repository access to one Task role and
attempt. The current active-baseline check and coordinator identity exclusion
are additional gates that do not add useful authority once those three checks
pass.

Provider entries, CLI runtimes, and Agents also expose different lifecycle
controls. Selection paths therefore disagree about whether a configured Agent
is actually usable.

## Decisions

### D1 — Keep Charter approval and Project creation approval separate

`ApproveCharter` creates an immutable, exact, principal-bound active receipt.
`CreateProjectFromCharterApproval` consumes that receipt atomically with the
Project, binding, Project Chat, Charter attachment, handoff, first Project
Agent turn, and events. Failure rolls back the creation transaction; replay
returns the original committed result. No Project can be created from a draft,
rejected, stale, or already consumed receipt.

This is intentionally two decisions: agreement with the Charter, then creation
of the Project. The Main Agent may prepare and present both operations but may
not self-approve either one.

### D2 — Charter approval authorizes implementation; baselines describe it

For a Project with a current approved Charter, `TaskService` does not require an
active/user-approved execution baseline before a repository-capable Task can be
claimed or receive a Workspace lease. If a baseline and plan item exist, their
exact references are still attached to Task governance and used for
reconciliation, milestones, evidence, readiness, and release. Missing or draft
baseline data is reported as planning completeness, never as execution
authorization.

Task dispatch still requires all ordinary Task checks: valid workflow
transition, explicit role assignment when the workflow requires one,
effectively available Agent/profile/source, capability compatibility,
repository binding, version/CAS, retry budget, one active execution, and an
exact Task-scoped lease. Charter amendments can mark planning/evidence stale,
but do not invent a second implementation approval.

Elevated operations, waivers, and immutable release remain their own explicit
approval boundaries; this change does not auto-approve them.

### D3 — Project role settings are editable defaults

Project Worker/reviewer settings seed new Tasks when present. They are optional
and never an execution-time allowlist. The Project Agent and authorized user
may update Project defaults, and may assign or replace a role on a specific
Task. Existing explicit Task assignments are not bulk-rewritten when a default
changes.

Any configured Agent that passes the common effective-availability and
capability checks may be assigned, including the identity bound to Main Chat or
Project Chat. A coordinator identity executing a Task receives a separate
role-specific Task context and scheduler lease; its chat context never gains
repository authority.

Main/Project Chat turns are not Task executions and do not occupy Task
execution capacity. If the same identity also runs a Task, only that active
Task attempt counts.

### D4 — Review is a workflow policy

Forge ships three legible review choices:

- `agent-review`: implementation is followed by a reviewer-role execution;
  reviewer completion advances the Task without interactive approval.
- `no-review`: successful implementation skips reviewer execution and advances
  directly according to the workflow.
- `human-required`: successful implementation waits at a review decision. The
  interactive user or the Project's currently bound Project Agent may submit
  `Accept` or `Reject` against the exact Task/version/review attempt.

Custom workflows remain supported. Forge evaluates their declared transitions,
requirements, hooks, and retry budget rather than wrapping another hidden
approval around them. Independent reviewer identity is optional policy, not a
global invariant.

Project-Agent review is a Project-scoped typed action. The server derives the
Project binding from the authenticated/native Chat context and rejects
cross-Project Tasks before returning details. The action creates the same
review evidence and transition receipt as a user action; it cannot bypass CI,
Task version, or workflow-state checks.

### D5 — Stopped execution creates Project attention

When an attempt reaches a stopped terminal disposition (failed, cancelled,
stalled, unavailable before retry, or exhausted retry budget), Forge records a
deduplicated Project attention item and, when an active Project Agent binding is
configured, admits one bounded Project Agent wake. The message contains Task
identity/title, role, safe stop reason, attempt/retry disposition, and allowed
recovery actions; it contains no repository path, secret, raw provider output,
or Workspace lease.

No binding means the Task remains visibly stopped without a failed wake loop.
A role assignment or explicit role confirmation newer than the latest stopped
attempt authorizes exactly one new attempt. Another stop requires a newer
recovery action.

### D6 — One reversible effective-availability policy

Provider entries gain versioned `enabled`; CLI runtime sources gain an
account-owned versioned policy keyed by `(owner_user_id, daemon_id,
executor_type)`; Agents use a reversible enabled/disabled lifecycle presented
consistently in API and UI. Existing records default to enabled.

The resolver returns `available` plus a stable reason from identity state,
selected profile, source enabled state, credential/runtime/daemon health, and
capability compatibility. Genesis selection, bindings, rosters, chat
admission/retry, Project setup, fallback, Task claim/lease/recovery all consume
this decision. Disabled configuration remains inspectable and re-enableable.
Disconnect/archive remain distinct destructive or terminal actions.

Disabling blocks new admissions, retries, fallbacks, renewals, and Task leases.
An already running bounded provider request or Task attempt may finish, but no
follow-up starts. Forge preserves explicit bindings and reports why they are
unavailable; it never silently substitutes another Agent.

### D7 — Reconciliation presents the decision, not the data model

Conflict records remain canonical and versioned, but the default workbench
surface shows: what changed, the concrete effect, and two actions. Agent
replacement copy names the current and proposed Agent. `Accept` applies the
exact proposal; `Reject` keeps current state and records the decision. Revision
IDs, digests, source records, and diff payloads are collapsed under “Technical
details.” Stale actions refresh the current proposal and preserve orientation.

## Migration and compatibility

Add a new numbered, data-preserving migration. Do not edit historical
migrations. Remove or replace SQLite triggers that require an active approved
baseline or forbid coordinator identities from Task leases. Preserve existing
Charter, baseline, Task, execution, lease, provider, profile, binding, and
history rows.

This is a beta breaking behavior change. Update every public handler/type/
generated binding/documented call site together, delete obsolete restrictions,
and record the change under `CHANGELOG.md` `Unreleased / Breaking`.

## Verification strategy

- Migration tests prove old data remains readable and the removed triggers no
  longer reject Charter-backed Tasks or coordinator-assigned Task leases.
- Service tests cover exact Charter receipt consumption, no-baseline Task
  dispatch, optional defaults, coordinator identity assignment, Task-only
  capacity accounting, each review mode, Project-Agent accept/reject,
  stopped-execution attention/wake, availability disable/re-enable, and
  cross-Project denial.
- The canonical API happy path and web tests are updated to the new behavior.
- A real `./test` flow creates a Todo Project from an approved Charter, runs an
  implementation Task without baseline approval, completes each selected
  review mode, stops one attempt to confirm Project Agent notification, and
  disables/re-enables a source/Agent to confirm consistent eligibility.
