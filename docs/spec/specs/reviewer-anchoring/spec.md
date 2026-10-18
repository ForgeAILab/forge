# reviewer-anchoring Specification

## Purpose
TBD - created by archiving change add-repair-convergence-guards. Update Purpose after archive.
## Requirements
### Requirement: Findings cite requirements, kind and shared cause
A reviewer finding SHALL accept three optional fields:
- `requirement_ids`: ids from the frozen review contract's requirements. Unknown ids are dropped.
- `kind`: `regression`, `missed` or `out_of_scope`. Unknown values read as absent.
- `same_cause_as`: an earlier finding id in the window.

Parsing SHALL stay lenient, as for the rest of the block. The fields SHALL be optional on the stored assessment and omitted when empty.

`RESPONSE_INSTRUCTION` SHALL list the frozen requirement ids and define the three fields. It SHALL also say that, after the first review, a new blocking finding must be a regression, an unmet requirement (cited), or in the Task's changed files.

#### Scenario: Cited finding
- **WHEN** a reviewer reports `{"summary":"Replace All drops unsaved drafts","requirement_ids":["R3"],"kind":"missed"}` and R3 is in the contract
- **THEN** the stored finding keeps `requirement_ids: ["R3"]` and `kind: missed`

#### Scenario: Unknown requirement id
- **WHEN** a finding cites `R99`, which is not in the contract
- **THEN** `R99` is dropped, and the finding is treated as citing no requirement

### Requirement: Later reviews cannot move the goalposts
After the reviewer's first assessment in a window, Forge SHALL treat a new (not continued) blocking finding as `minor` unless one of these holds. A Review that stopped before a reviewer ran (CI failed first, or it was cancelled) does not count as that first assessment. A continued finding that was downgraded stays `minor` until it meets one of these:
- its `kind` is `regression`;
- it cites a requirement id;
- its `where` names a file in the Task's candidate diff. The path is the first repository-relative path in `where`, with any `:line` suffix removed. A `where` without a path does not meet this clause.

An `out_of_scope` finding SHALL never block. It is listed as a follow-up candidate, with ledger status `out_of_scope`.

These rules apply in the derived ledger only. The frozen assessment is never changed.

#### Scenario: CI failed before the first reviewer ran
- **WHEN** review 1 fails CI before any reviewer runs, and review 2 is the reviewer's first assessment with a finding in an untouched file
- **THEN** the finding stays blocking

#### Scenario: Reviewer copies a downgraded id
- **WHEN** review 2's naming nit was downgraded and review 3 copies its id without a citation, regression kind or candidate-diff path
- **THEN** it stays `minor` and does not start a blocking streak

When a reviewer `fail` has no blocking finding left after these rules, the Task SHALL park with `review_needs_owner` and the message `only non-blocking findings remain`. The coder is not dispatched and no review budget is spent.

#### Scenario: New nit in untouched code
- **WHEN** review 2 adds a finding about naming in a file the Task never changed, citing no requirement and not marked regression
- **THEN** the finding shows as `minor` with the note "outside requirements and Task files", and does not count toward the open blocking findings

#### Scenario: Only out-of-scope findings remain
- **WHEN** review 2 resolves every earlier finding and fails only on one `out_of_scope` finding
- **THEN** the Task parks with `only non-blocking findings remain`, and the comment lists the finding as a follow-up candidate

#### Scenario: Missed but cited
- **WHEN** review 2 adds a `missed` finding that cites requirement R2
- **THEN** it stays blocking

### Requirement: Re-broken findings are visible
A finding resolved earlier in the window that is continued again SHALL have ledger status `reopened`. For a Forge-owned check finding, this means the same signature fails again. A `reopened` finding counts as open and starts a new streak. The repair brief and comment SHALL mark it "re-broken: resolved in round N".

#### Scenario: Test fixed then broken again
- **WHEN** `CI-1#a1b2c3d4` failed in review 1, passed in review 2 and fails again in review 3
- **THEN** after review 3 it is `reopened`, and the brief says it was resolved in round 2
