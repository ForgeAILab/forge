---
created_at: 2026-08-26T19:20:36Z
updated_at: 2026-08-27T02:45:00Z
completed_at:
---

## 1. Authority and persistence

- [x] 1.1 Add a numbered data-preserving migration for reversible provider,
  CLI-runtime, and Agent availability; remove database enforcement of active
  baseline approval and coordinator-identity exclusion for Task leases.
- [x] 1.2 Update DB models, repositories, row mappers, CAS/version handling,
  idempotency, migration fixtures, and stable availability reason codes.
- [x] 1.3 Preserve exact Charter approval then atomic
  `CreateProjectFromCharterApproval`; add regression tests proving no Project
  can be created without the active exact receipt.

## 2. Charter-backed Task execution

- [x] 2.1 Remove baseline approval from repository Task runnable/claim/lease
  checks while retaining Charter ownership, Task workflow, governance
  references when present, repository/capability checks, and scoped leases.
- [x] 2.2 Make Project Worker/reviewer settings optional, editable defaults;
  keep explicit Task roles authoritative and avoid rewriting existing Tasks.
- [x] 2.3 Permit any effectively available configured Agent, including active
  Main/Project binding identities, to fill a Task role through a role-specific
  Task context and lease.
- [x] 2.4 Verify Main/Project Chat turns do not consume Task execution capacity;
  only active Task attempts count.
- [x] 2.5 Preserve the one-attempt retry signal when a stopped role is assigned
  or explicitly reconfirmed, including role-distinct native Task contexts.

## 3. Workflow review and stopped execution

- [x] 3.1 Add/document/test `agent-review`, `no-review`, and
  `human-required` workflow choices with no hidden review approval.
- [x] 3.2 Add a Project-bound typed review action allowing the current Project
  Agent to accept/reject a human-required Task gate with normal version,
  workflow, CI, evidence, and cross-Project checks.
- [x] 3.3 Record deduplicated attention and wake the configured Project Agent
  when a Task execution stops; keep the bounded message free of repository
  authority and protected output.
- [x] 3.4 Add service/integration tests for all review modes, same-agent review,
  Project-Agent review, no binding, wake replay, and stop/recovery behavior.

## 4. Effective availability

- [x] 4.1 Add versioned provider-entry and CLI-runtime availability APIs and a
  clear reversible Agent enable/disable API, events, public types, generated
  TypeScript, and docs.
- [x] 4.2 Apply one availability resolver to Genesis selection, bindings,
  roster projections, chat admission/retry, Project setup, fallback, and Task
  claim/lease/recovery.
- [x] 4.3 Preserve configuration/history, block only new bounded work after
  disable, never silently rebind, and distinguish disable from
  disconnect/archive.
- [x] 4.4 Add enable/disable, stale-version, cross-account, dependent binding,
  in-flight boundary, and fallback-exclusion tests.

## 5. Web and documentation

- [x] 5.1 Update `DESIGN.md` before UI code with optional Project defaults,
  review-mode controls, source/Agent availability states, stopped attention,
  and concise reconciliation decisions.
- [x] 5.2 Add accessible provider/CLI/Agent enable controls with dependent
  warnings and truthful loading/conflict/error/success states.
- [x] 5.3 Simplify Project setup and reconciliation copy to concrete
  `Accept`/`Reject` decisions and hide technical provenance by default.
- [x] 5.4 Update `docs/api.md`, `docs/architecture.md`,
  `docs/getting-started.md`, generated bindings, and `CHANGELOG.md`.

## 6. Verification and handoff

- [x] 6.1 Run formatting, focused Rust tests, canonical happy path, workspace
  tests/clippy, web lint/typecheck/tests/build, and migration/API parity checks.
- [ ] 6.2 Exercise the real Todo flow under `./test`: approve Charter, approve
  Project creation, run without baseline approval, and complete configured
  review behavior.
- [x] 6.3 Stop an execution and verify Project Agent attention; disable and
  re-enable a provider/Agent and verify selection/dispatch consistency.
- [ ] 6.4 Perform browser visual/accessibility QA for setup, reconciliation,
  review, and availability controls at compact and desktop widths.
- [x] 6.5 Reproduce the Todo reviewer mismatch and verify Task assignment wins
  over the Project default and unavailable Agents are still refused.
