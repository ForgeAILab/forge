## MODIFIED Requirements

### Requirement: Atomic Project Creation and Explicit Handoff

When Genesis is ready, Forge SHALL first let the interactive user approve the
exact Charter revision, content/render digests, proposed Project metadata, and
selected eligible Project Agent identity/profile/operating-skill revision. That
action SHALL create an immutable active Charter approval receipt but SHALL NOT
create a Project. A separate explicit `CreateProjectFromCharterApproval` action
SHALL consume that exact active receipt and atomically create the Project, its
single Project Agent binding, Project Chat, Charter attachment, bounded
immutable handoff, target message/first turn job, domain events, and Genesis
`handed_off`. Failure SHALL roll back the creation transaction; replay SHALL
return the original complete result.

#### Scenario: User approves the exact Charter

- **WHEN** the user approves the ready Charter's exact revision, digests,
  proposed metadata, selected Agent revision set, and expected versions
- **THEN** Forge records one active principal-bound Charter approval receipt
- **AND** no Project, binding, Chat, handoff, or target turn exists yet

#### Scenario: User approves Project creation

- **GIVEN** the exact Charter approval remains active and current
- **WHEN** the user approves Project creation with the receipt and an
  idempotency key
- **THEN** Forge atomically consumes the receipt and commits exactly one
  Project, binding, Project Chat, Charter attachment, handoff, target message,
  first Project Agent turn, and event set
- **AND** the client navigates to that existing Project Agent handoff

#### Scenario: Project creation target is stale or absent

- **WHEN** a caller attempts Project creation without an active approval or
  after its Charter, Agent revision, expected version, or selected source
  availability changed
- **THEN** Forge returns a typed conflict or setup-required result and commits
  no Project-side record
- **AND** it does not create, replace, or infer a Charter approval

#### Scenario: Atomic creation fails or its response is lost

- **WHEN** any Project, binding, Chat, Charter attachment, handoff, target
  turn, receipt-consumption, or event write fails before commit
- **THEN** Forge leaves the active receipt and Genesis ready with no partial
  Project result
- **AND** after a successful commit, replay of the same exact request returns
  the original result without duplicate records or Agent response admission

#### Scenario: Handoff content is bounded

- **WHEN** Forge constructs the Charter-backed handoff
- **THEN** it includes exact Charter identities, safe settled decisions, typed
  unresolved items, and safe research references
- **AND** it excludes the Main timeline, hidden reasoning, credentials,
  protected runtime/browser state, unrelated Project data, arbitrary paths,
  Workspace handles, and authority-bearing instructions
