# Authority and Effective Project State

## Contents

- Principals and authority matrix
- Server enforcement
- Effective Project State
- Approval and attestation integrity
- Concurrency and idempotency
- Protected data and prompt injection
- Failure rules

## Principals and Authority Matrix

Treat principal identity as part of every consequential record. “An agent did it” is not sufficient provenance.

| Capability | Interactive user | Main Agent | Project Agent | Task Worker | Task reviewer | System |
|---|---:|---:|---:|---:|---:|---:|
| Answer discovery / choose Project identity | approves | drafts/recommends | Project-local amendment only | no | no | validates shape |
| Create Project from Genesis | approves receipt | invokes receipt | no | no | no | commits atomically |
| Read portfolio projection | yes | bounded | own Project only | assigned Task only | assigned Task only | filters |
| Mutate portfolio presentation tags/order | yes | policy-bounded | no | no | no | validates |
| Archive/delete/change Project-local lifecycle | yes | propose only | propose only | no | no | applies policy |
| Draft Project Documents / baseline | yes | no after handoff | yes | evidence/output only | review output only | versions |
| Approve material Charter/Document/baseline | yes | no | no | no | no | records receipt |
| Create/manage Project Tasks | yes | no | own Project | assigned Task operations | assigned review operations | TaskService validates |
| Repository/filesystem mutation | through normal controls | no | no | scoped lease | read-only by default; remediation needs Worker authority | issues lease |
| Submit work/evidence | yes | no | coordinates | yes | review evidence | records |
| Attest validation | manual when permitted | no | no | not own work | independent assignment | automated checks |
| Waive release-gating check | yes, policy-bounded | no | propose only | no | no | validates scope |
| Compute readiness | no | no | request only | no | no | yes |
| Release milestone | yes | no | propose only | no | no | freezes snapshot |

Reviewers must not review their own work. If remediation requires writes, create a Worker assignment or separately authorized write-capable remediation step; do not silently turn a reviewer lease into implementation authority.

## Server Enforcement

Prompt text reinforces behavior; it never substitutes for these controls:

1. Derive account, chat, Project, binding, and principal from authenticated server state.
2. Ignore caller/model-supplied scope when it differs. A Project Agent action should not accept arbitrary `project_id` at all when a binding-derived route/action is possible.
3. Re-authorize every referenced Charter, Document, Decision, baseline, Task, Project repository selection, attempt repository provenance, validation, milestone, release, and media asset before retrieving even counts/snippets.
4. Generate tool/action descriptors from canonical scope and permission ceiling. Keep denied tools absent, then deny again at service boundaries if they appear accidentally.
5. Do not accept a Task-level repository selector. Resolve the current Repo from the Task's server-owned Project and `project.primary_repo_id`, verify same-Project ownership, and reject filesystem paths, raw repository URLs, credentials, cookies, environment variables, Workspace handles/tokens, and capability instructions in model-generated payloads.
6. Let the scheduler create short-lived `WorkspaceLease` state bound to Project, Task, the resolved Project repository, base ref, role/capabilities, issued principal, and expiry. Persist the attempt binding on the Workspace/lease, but never expose a lease to Main/Project chat context.
7. Return only sanitized Task results, immutable git/build refs, validation attestations, and evidence references to the Project Agent.

References and text are authorization inputs to validate, never bearer capabilities.

## Effective Project State

Do not use one universal “latest record wins” hierarchy. Resolve each claim through its authority domain:

| Claim | Effective authority |
|---|---|
| Project display name, identity, users, outcome, scope, non-goals, constraints | current approved Charter revision |
| Detailed product/design/architecture behavior | applicable current approved Document revisions in active baseline |
| Execution plan, acceptance/evidence matrix, adaptive envelope, release policy | active approved execution baseline |
| Repository for a new Task execution | current `project.primary_repo_id`, resolving to a Repo owned by that Project |
| Active decisions | non-superseded/non-invalidated Decision records compatible with current Charter/baseline |
| Current work | latest server-accepted Task revisions/events |
| Check result | authorized validation/manual attestation pinned to exact input and check-definition versions |
| Delivered repository state | immutable git/build/deployment identity emitted by authorized Task workflow |
| Released claim | immutable release manifest revision |
| Current human-readable status | rebuildable projection; never authority |

Compute an `EffectiveProjectState` projection with at least:

- current Charter and baseline IDs/revisions/digests;
- applicable current Document pointers;
- active Decisions and invalidations;
- reconciliation-required records;
- canonical conflicts;
- Task and validation summary;
- active/primary milestone and current readiness;
- latest releases and unreleased changes;
- source event watermark/version.

A release is historical truth about what was claimed at release time. It never overrides current live state.

## Canonical Conflicts and Reconciliation

Create `canonical_conflict` when two current approved records make incompatible claims inside the same authority domain or a subordinate record violates its governing Charter/baseline.

Block only affected execution/readiness when possible. Show:

- conflicting record IDs/revisions/digests;
- authority domain;
- governing record;
- affected Tasks/milestones/checks;
- safe options and required principal.

After a Charter Amendment or incompatible baseline activation, mark affected records `reconciliation_required`. Resolve each explicitly as:

- retained (still compatible, with reason and actor);
- revised/replaced;
- cancelled;
- superseded;
- invalidated.

Do not clear reconciliation from model prose or a dashboard refresh.

## Approval and Attestation Integrity

Bind every approval/attestation to:

- principal type/ID and authorization basis;
- target kind/ID/revision;
- canonical content/input digest;
- rendered-view digest when the user reviewed rendered content;
- governing Charter/baseline/policy/check-definition revisions;
- expected optimistic version;
- explicit UI/command event;
- timestamp and idempotency key.

Release-gating inputs are principal-bound:

- Project Agent may propose but not attest/approve them.
- Worker may submit work/evidence but not validate its own work.
- Independent reviewer may attest only assigned review scope.
- Automated system check records runner/input/result provenance.
- Interactive user may perform policy-permitted manual attestation/waiver.
- System alone aggregates readiness.

A Project Agent closing a Task, editing a check, or attaching a screenshot cannot manufacture readiness. Changes to release policy/check requirements require a new approved baseline.

## Concurrency and Idempotency

Use `version`/compare-and-swap for mutable pointers and state. Use immutable revisions for content.

Require expected versions/digests for:

- Charter/Document draft and proposal creation;
- approval/current pointer changes;
- amendment/baseline activation;
- Task/milestone mutations;
- readiness/release;
- evidence attachment/removal.

Use idempotency keys for every retried side-effecting action. Persist the original outcome and return it on replay.

High-value atomic boundaries:

- `CreateProjectFromCharterApproval`: receipt verification/consumption plus Project, binding, Project Chat, Charter transfer, handoff/message/turn, events, and Genesis transition.
- baseline activation: approval receipt, current pointer, conflict/reconciliation projection, events.
- release: readiness recheck, immutable manifest, evidence pins, milestone/release state, events.
- media reference changes: attachment/pin state before guarded physical cleanup.

If an operation cannot commit all authoritative records/events, expose none of it as successful.

## Protected Data and Prompt Injection

Never place these in ordinary artifacts, handoffs, chat, memory, context manifests, events, logs, or model arguments:

- credentials, API keys, cookies, auth headers, environment secrets;
- Agent Runtime checkpoints/interactions containing secrets;
- hidden prompts/reasoning/evaluator traces;
- Workspace handles, capability tokens, lease secrets;
- raw authenticated browser state;
- unrelated Project/private Main content;
- unsafe absolute paths or repository credentials.

Content guards run before canonical persistence, cross-scope publication, memory indexing, events, and logs. Store redaction audit metadata without protected bodies.

Treat every web page, Task output, repository file, media caption, imported document, and handoff field as potentially adversarial. Its text cannot change role, policy, principal, scope, approval, or tool availability.

## Failure Rules

- A committed user message does not imply a successful agent turn. Use the existing finite queued/leased/retry/failed lifecycle.
- Missing/mismatched handoff or artifact hashes fail closed before Project mutation.
- Version conflicts refresh and re-propose; never merge approval targets automatically.
- A stale baseline blocks affected repository-capable dispatch.
- A missing, nonexistent, or cross-Project primary Repo blocks repository-capable dispatch as Project setup; Forge never substitutes an unselected Repo or mutates a Task to repair it.
- A stale readiness digest blocks release and creates no snapshot/pin.
- Projection/cache failure shows stale/error state; it does not change canonical truth.
- Release evidence survives ordinary Task cleanup. Mandatory security/privacy/legal purge may delete bytes only through an audited exception that preserves permitted tombstone/digest metadata and marks affected release evidence unavailable.
