---
created_at: 2026-08-26T19:20:36Z
updated_at: 2026-08-27T02:15:00Z
---

## Why

Forge currently layers setup authority on top of Task workflow authority. A
user approves a Charter, separately approves Project creation, then can be
asked to approve a Project execution baseline before an implementation Task is
allowed to start. Execution setup also treats Project role defaults and
Main/Project coordinator identities as hard restrictions. These rules make a
normal Charter-backed Project look blocked even when its Task already has a
valid workflow and an explicitly assigned, configured Agent.

Provider credentials can only be disconnected and Agent identities are exposed
through inconsistent pause/archive controls. Forge needs one reversible
availability policy so disabled sources and Agents are excluded everywhere
without deleting their configuration or history.

## What Changes

- Preserve two distinct user decisions in Product Genesis: approving the exact
  Charter and approving creation of the Project from that receipt. Project
  creation remains impossible without the active exact Charter approval.
- **BREAKING:** stop treating Project Document or execution-baseline approval as
  authorization to implement. Once a Project has an approved Charter,
  repository-capable Tasks may run through their configured Task workflow.
  Baselines remain traceability, reconciliation, milestone, and release inputs;
  they do not gate Task dispatch or Workspace-lease issuance.
- Treat Project Worker/reviewer selections as optional defaults. A Task's
  explicit role assignment is authoritative and may use any configured,
  effectively available Agent, including the identity currently bound as Main
  Agent or Project Agent. The identity receives only the Task role's scoped
  context and lease while executing the Task.
- Do not charge Main/Project Chat turns against Task execution capacity. An
  Agent assigned to a Task consumes Task capacity only while it has a Task
  execution attempt.
- Provide clear workflow review modes: Agent review, no review, and a
  human-required review gate. The bound Project Agent may approve or reject the
  human-required Task review gate, as may the interactive user.
- When a Task execution stops and the Project has a configured Project Agent,
  record attention and wake that Project Agent with a bounded Task summary and
  recovery options. Reassigning or confirming the affected Task role permits
  one fresh attempt without a separate resume approval.
- Add reversible enabled/disabled policy for provider entries, discovered CLI
  runtime sources, and Agent identities. One server-owned availability resolver
  governs selection, binding health, chat admission, retry/fallback, setup,
  and Task dispatch. Disabling never silently rebinds another Agent.
- Replace the dense canonical reconciliation surface with a short conflict
  summary naming the effect (for example, “Replace reviewer Agent A with Agent
  B”) and explicit `Accept` / `Reject` actions; technical provenance stays in
  collapsed details.

## Impact

- Affected specs: `product-genesis`, `project-agent-operations`,
  `agent-source-availability`, `project-agent-workbench`
- Affected code: Task governance and Workspace-lease persistence, execution
  setup and Agent selection, workflow templates/review actions, Project Agent
  wake/attention, provider and Agent APIs, generated TypeScript, Agent Settings
  and Project workbench UI, tests, `DESIGN.md`, `docs/api.md`,
  `docs/architecture.md`, `docs/getting-started.md`, and `CHANGELOG.md`
- The change deliberately supersedes the execution-baseline gate and
  coordinator-exclusion assumptions in older orchestration changes. Task
  workflow, optimistic concurrency, Task-scoped leases, review evidence,
  readiness, and release approval remain authoritative.

## Approval

The user's 2026-08-26 clarification approves this revised change: Project
creation still requires approval; implementation does not require another
approval after Charter approval; any configured enabled Agent may fill a Task
role; review behavior is workflow-configurable; a Project Agent may satisfy a
human-required review; stopped execution notifies the Project Agent; and
providers/Agents can be disabled and re-enabled.
