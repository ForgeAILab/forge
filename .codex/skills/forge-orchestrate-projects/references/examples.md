# End-to-End Examples

Use these examples to calibrate process depth and visible communication. Do not copy names, facts, assumptions, approvals, or IDs into another Project.

## Example 1: Compact Todo Project

### Initial request

> I want a simple todo list. Use Codex through my existing provider connection and prove the agent can create a repository, manage Tasks, and validate the result.

### Main Agent response

The Main Agent should capture the desired outcome and expose the two consequential unknowns, not start implementation:

```text
Current understanding
- You want a small browser-based todo app whose main purpose is an end-to-end Forge agent test.
- Success means a Project Worker creates the repository implementation, Forge tracks the Tasks, an independent check validates the app, and the UI preserves visual proof.

Two decisions
1. Should persistence be local-browser only, or is a server/database part of the test?
2. May Forge create a new local repository, or must it bind an existing repository?

Working assumptions
- Compact mode is appropriate if persistence is browser-local and there are no auth, collaboration, or deployment requirements.
- Provider authentication is an existing account capability; no credentials enter the Charter or handoff.
```

After answers, Main may recommend a name such as **Prooflist**, state explicit non-goals, and propose a compact Charter. It must show the exact frozen rendered view and wait for the explicit approval action. “Looks good” in chat is not the receipt.

### Atomic creation and handoff

On receipt consumption, Forge atomically creates the Project, Project Agent binding, Project Chat, approved Charter attachment, `M1 — Deliver outcome`, handoff message, queued first Project turn, and events. The Project-visible handoff contains the exact approved Charter and no Main transcript, repository path, provider credential, or global portfolio state.

### Project Agent continuation

The Project Agent verifies the handoff, then drafts a Delivery Brief. Before user baseline approval it may create a read-only discovery Task to inspect the Project's current authorized primary Repo, but it may not dispatch a repository-capable implementation Task. The Task itself carries no repository selector.

The proposed compact baseline could contain:

- outcome: create/read/update/complete/delete todos in a responsive browser UI;
- evidence: unit/component tests, production build, independent browser walkthrough, one image and one short video;
- adaptive envelope: split UI, persistence, and validation into separate Tasks; reorder independent Tasks; retry transient failures;
- fixed boundary: no backend, login, sharing, deployment, or provider-auth changes;
- release gates: required tests/build, independent reviewer validation, current proof media.

After explicit baseline approval, Project Agent proposes traceable Tasks. The scheduler—not chat—issues a WorkspaceLease to each Worker/reviewer. The Project Agent receives sanitized diffs, outcomes, checks, and evidence references.

### Status and release

Project Agent reports separately:

```text
Live progress
- 3/4 scoped Tasks reached their expected state.
- Browser proof is waiting on the final validation Task.

Verified release truth
- M1 is not ready.
- Required check `browser-core-flow` has no independent passing result for commit abc123.
- No release has been created.

Next safe action
- Dispatch the independent browser validation Task at commit abc123.
```

When all gates pass, Forge computes a readiness snapshot. The user—not the Project Agent—creates `M1-r1`. That release records Forge state only; it does not deploy the app.

## Example 2: Standard Project

### Initial request

> Help me build a scheduling product for small clinics.

This requires standard mode because patient-related data, permissions, availability rules, notifications, integrations, accessibility, and operations materially affect scope and architecture.

Main should ask no more than two high-impact questions per turn. Early questions should separate the beneficiary and operating boundary, for example:

1. Is this staff-only scheduling or does it include patient self-service?
2. Which country/jurisdiction and existing calendar or health-system integrations are in scope?

Main should distinguish:

- user decisions: intended market and authorized workflows;
- observed facts: supplied existing systems;
- research findings: current integration or regulatory documentation;
- assumptions: volume or notification channel pending evidence;
- hypotheses: product-market claims;
- open decisions: ownership of cancellations or double-book conflicts.

It should not call a hypothesis “approved scope.” The standard Charter must define users, outcomes, non-goals, acceptance boundary, data/security/compliance constraints, risks, and research queue. Project creation still requires an exact approval receipt.

After handoff, Project Agent conducts direct read-only research for stable public primary sources and delegates bounded discovery Tasks when repository inspection, executable experiments, specialist evaluation, or independently reviewable evidence is needed. It drafts only decision-bearing artifacts: likely Product Spec, Design Spec, Architecture Spec, and Execution Plan.

The user-approved execution baseline freezes exact governing revisions, release policy, elevated-action gates, and an adaptive envelope. Implementation Tasks can then evolve inside that envelope. Adding patient messaging, changing the jurisdiction, or weakening the audit boundary requires a Charter amendment; changing the chosen scheduling algorithm inside approved requirements may be an implementation Decision Record.

## Example 3: Material Scope Change

Assume the approved compact todo Charter excludes collaboration. Mid-build, the user asks for shared lists with accounts.

Project Agent must not silently extend the current baseline. It should say:

```text
This changes the approved users, data boundary, security model, and acceptance criteria, so it is a material Charter amendment rather than a Task detail.

Proposed action
- Draft a candidate Charter revision adding accounts and shared-list roles.
- Identify affected Delivery Brief, baseline, Tasks, decisions, validation, and M1.
- Mark those records reconciliation_required if the user approves the amendment.

No current Task has been widened and no new implementation was dispatched.
```

Approval compares the expected current Charter revision plus the candidate content and render digests. A concurrent Charter change makes the proposal stale. After success, affected records remain visibly unresolved until explicit reconciliation produces a compatible new baseline.

## Example 4: Evidence Loss After Release

Assume `M1-r1` pins a validation video. Later the source Task is deleted and the video must eventually be purged for a privacy reason.

- Task deletion removes the Task attachment and Task-scoped URL but does not remove the Project media asset or release pin.
- While the release pin exists, ordinary garbage collection cannot delete the bytes.
- A mandatory audited purge revokes delivery URLs and deletes bytes where required, while retaining the permitted digest/tombstone and audit event.
- The immutable manifest entry becomes `purged`; `M1-r1` becomes `evidence_unavailable`, not silently “ready” or “failed.”
- A later corrected release uses `M1-r2`; it never rewrites `M1-r1`.

## Anti-Examples

Reject or reformulate these behaviors:

- “I created the Project because you said yes in chat.” No exact approval receipt exists.
- “Main archived a struggling Project.” Main has no Project-local lifecycle authority.
- “Project Agent passed its own review.” Release-gating validation must come from an allowed independent principal.
- “The Task contains `/Volumes/Data/repo`, a Repo ID, and a GitHub token.” Task prose carries no repository selector; TaskService derives the Project's current primary Repo and the scheduler supplies a scoped, attempt-pinned lease.
- “The dashboard says 100%, therefore release is ready.” Progress is a projection; readiness is a system snapshot over exact policy-bound inputs.
- “We moved legacy media into a new release directory.” Preserve asset IDs, URLs, storage keys, and bytes in place; add ownership, attachment, and pin metadata.
