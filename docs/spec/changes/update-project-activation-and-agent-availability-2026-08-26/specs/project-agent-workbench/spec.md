## MODIFIED Requirements

### Requirement: Project Setup and Reconciliation Are Decision-Oriented

The Project workbench SHALL show Charter approval and Project creation as
distinct completed/pending decisions. It SHALL NOT present baseline approval or
missing Project role defaults as implementation authorization. Project Worker
and reviewer settings SHALL be presented as optional defaults that may be
updated globally or overridden on a Task.

A reconciliation conflict SHALL lead with a one-sentence effect, including the
current and proposed Agent when replacement is involved, and SHALL expose
`Accept` and `Reject` as the primary actions. Canonical revisions, digests,
source records, and raw diff metadata SHALL be collapsed under technical
details. Loading, stale, conflict, error, accepted, and rejected states SHALL
remain truthful and keyboard accessible.

#### Scenario: Project has no approved baseline

- **WHEN** the Project has an approved Charter and a runnable Task but no
  approved execution baseline
- **THEN** the workbench does not show `Setup approval requested` or block the
  Task for that reason
- **AND** baseline state remains available as planning/traceability context

#### Scenario: Project role defaults are absent

- **WHEN** no default Worker or reviewer is configured
- **THEN** Project Settings explains that Tasks may be assigned individually
  and does not mark the entire Project unapproved
- **AND** a Task with its required explicit roles may run

#### Scenario: Reconciliation replaces an Agent

- **WHEN** a proposal would replace reviewer Agent A with Agent B
- **THEN** the primary surface says “Replace reviewer Agent A with Agent B” and
  offers `Accept` and `Reject`
- **AND** technical provenance is available without dominating the decision

#### Scenario: Reconciliation action is stale

- **WHEN** the exact proposal version changes before `Accept` or `Reject`
- **THEN** Forge preserves the user's orientation, refreshes the current
  one-sentence effect, and requires a decision on that current proposal
- **AND** it neither silently applies nor discards the replacement

#### Scenario: Review policy is selected

- **WHEN** a user or Project Agent configures Task review behavior
- **THEN** the control clearly distinguishes Agent review, no review, and a
  human-required decision that the Project Agent may also perform
- **AND** it does not describe all three modes as the same generic approval

#### Scenario: Compact viewport

- **WHEN** setup, review, reconciliation, or availability controls render at
  375, 768, or 1280 CSS pixels
- **THEN** decision copy, Agent names, reasons, states, and actions remain
  readable, keyboard-operable, focus-visible, and announced
- **AND** identifiers wrap and actions remain reachable without page-level
  horizontal overflow
