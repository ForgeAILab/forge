## ADDED Requirements

### Requirement: Reviewer result block carries optional findings
The reviewer result block SHALL accept two optional fields next to `result`,
`reason`, `fixable_by` and `repeat`.

`findings` is a list. Each item has:
- `summary` (required, one sentence);
- `severity`: `blocking` (default) or `minor`;
- `where`: file and line, command, or screen;
- `expected`;
- `observed`;
- `id`: only when the item continues a finding from the previous review.

`resolved` is a list of finding ids from the previous review that are now
fixed.

Parsing SHALL stay lenient:
- Items without a non-empty `summary` are dropped.
- Unknown severities read as `blocking`.
- A list that is not an array is ignored.
- At most 50 findings are kept, and text fields are cut at 1,000 characters.
- A malformed `findings` or `resolved` SHALL never make `result` unreadable.

`RESPONSE_INSTRUCTION` and the missing-block correction prompt SHALL define
both fields and give an example.

#### Scenario: Reviewer lists findings
- **WHEN** a reviewer ends with `{"result":"fail","reason":"empty input crashes","findings":[{"summary":"Empty input crashes parse()","where":"src/parse.rs:42","expected":"error message","observed":"panic"}]}`
- **THEN** the stored assessment has `result = fail` and one blocking finding with that summary, location, expected and observed text

#### Scenario: Malformed findings keep the verdict
- **WHEN** a reviewer ends with `{"result":"fail","reason":"x","findings":"see above"}`
- **THEN** the assessment parses with `result = fail`, `reason = x` and no findings

#### Scenario: Older reply without findings
- **WHEN** a reviewer returns only `{"result":"fail","reason":"..."}`
- **THEN** the assessment has empty `findings` and `resolved`, and routing is exactly as before this change

### Requirement: Stored assessments stay byte-identical
`ReviewAssessment.findings` and `ReviewAssessment.resolved` SHALL default to
empty when absent. They SHALL be omitted from serialization when empty. An
assessment frozen before this change SHALL therefore re-serialize to the same
string. Forge-computed state (assigned ids, status, streaks) SHALL NOT be
written into the frozen assessment or its Review copy.

#### Scenario: Pre-change row round-trips
- **WHEN** a `conformance_json` row written by v0.13.12 is read and serialized again
- **THEN** the string is byte-identical and `record_review_conformance` accepts it as unchanged

### Requirement: Forge identifies findings across rounds
Forge SHALL give each reviewer finding without a known id the next free id
`F<n>` in the Task's current review window, in list order.

A reviewer finding that carries an id Forge assigned earlier in the window
continues that finding. An unknown id is treated as a new finding.

Forge-owned failures recorded on a Review SHALL be findings with ids taken
from their source:
- `CI-<step index>#<first 8 hex of sha256(normalised signature)>` per failing
  review CI test, or `CI-<step index>` for a failed step without signatures;
- `CHK-<check id>` for a failed required conformance check;
- `V-<check name>`, `V-start` and `V-ready` for verify.

Their `observed` text is the normalised test signature or bounded output tail. The same source failing
again carries the same id. Merge-queue failures create no Review and are not
part of the ledger; the repair brief covers them.

#### Scenario: Re-reviewer continues and resolves
- **WHEN** review 1 produced F1 and F2, and review 2's block has `findings: [{"id":"F1","summary":"still crashes"}, {"summary":"--json ignored"}]` and `resolved: ["F2"]`
- **THEN** after review 2, F1 is open, F2 is resolved, and the new finding is F3

#### Scenario: CI failure is a finding
- **WHEN** review CI step 2 (`cargo test`) exits 101 without failure signatures
- **THEN** the ledger has an open finding `CI-2` whose `where` is `cargo test` and whose `observed` is the step's output tail

### Requirement: The finding ledger is derived from the window's Reviews
A pure function SHALL derive the ledger from the Task's Reviews in the current
review-budget window, in attempt order. A recovery boundary that resets the
review budget starts a new, empty ledger.

Each entry has:
- `id`, `source` (`reviewer | ci | check | verify | merge`), `severity`;
- `summary`, `where`, `expected`, `observed`;
- `first_attempt`, `last_checked_attempt`;
- `status`: `open | resolved | unconfirmed | flaky | pre_existing | superseded`;
- `streak`.

The rules:
- A finding changes only in a review where its source ran.
- A prior reviewer finding that a reviewer who ran neither continued nor
  resolved becomes `unconfirmed`. It does not count as open.
- A Forge-owned finding is `resolved` when its source passes.

Routing, the repair brief, comments and the findings read SHALL all use this
one function.

#### Scenario: Reviewer forgets a finding
- **WHEN** review 1 produced F1 and F2, and review 2's reviewer lists only F1 and resolves nothing
- **THEN** F2 is `unconfirmed`, F1 is `open` with streak 2, and only F1 counts as open

#### Scenario: Source did not run
- **WHEN** review 1's reviewer produced F1 and review 2 stops at a failing CI step before any reviewer runs
- **THEN** F1 keeps status `open` with streak 1, and `CI-<n>` is open with streak 1

#### Scenario: New window after recovery
- **WHEN** the owner's recovery resets the Task's review budget
- **THEN** the next review starts an empty ledger and numbers its first reviewer finding F1

### Requirement: Findings are readable through the API and CLI
`GET /api/v1/tasks/{id}/findings` SHALL return the current window's ledger as
`{ items, window_started_at, review_attempts }`. `items` is ordered by id and
has at most 200 entries. The route needs the same Task read authority as
`GET /api/v1/tasks/{id}/reviews`.

`forge-ctl task findings <task>` SHALL print the same ledger.

The types SHALL be exported to the generated TypeScript and documented in
`docs/api.md`.

#### Scenario: Read the ledger
- **WHEN** a Project member calls `GET /api/v1/tasks/{id}/findings` after two failed reviews
- **THEN** the response lists every finding in the window with status, source, streak and attempts

#### Scenario: No access
- **WHEN** a user who cannot read the Task calls the route
- **THEN** the response is the same refusal `GET /api/v1/tasks/{id}/reviews` returns for that user

#### Scenario: A fail-fast CI round omits a previous failing binary
- **WHEN** a CI step fails again with an untruncated list that omits a prior test
- **THEN** that test retains its previous status, streak and last checked attempt; it contributes no false progress

#### Scenario: Per-test CI findings replace a step finding
- **WHEN** a failed step formerly represented by CI-0 now reports signatures
- **THEN** CI-0 is superseded, not resolved, and GET findings reports that additive status
