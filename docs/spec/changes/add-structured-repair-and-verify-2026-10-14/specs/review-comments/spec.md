## ADDED Requirements

### Requirement: Review comments are a findings checklist
The Task comment Forge posts for a finished review SHALL contain these parts,
in order:
1. The current headline, for example `Review failed (attempt N): <reason>`.
2. A GitHub-flavoured task list of the ledger after this review. Each finding
   that is new or still open is unchecked and says since which attempt it has
   been open. Each finding resolved in this review is checked; earlier-resolved findings are
   not repeated. Each
   unconfirmed finding is shown without a box, as "not re-checked".
3. When verify ran, one line with its result, the failing check names and the
   evidence count. When it was skipped, the reason.
4. A pointer to the Review tab for the full report.

The reviewer's full Markdown report SHALL NOT be posted in the comment when
the review has findings. When the review has no findings list, the comment
SHALL carry the reason and at most the first 2 KiB of the report.

A review that fails at review CI or verify, before any reviewer runs, SHALL
also get this comment, listing its `CI-*` or `V-*` findings. Today a review CI
failure posts no comment.

#### Scenario: Failed review with findings
- **WHEN** review 2 leaves F1 open since attempt 1, adds F4, resolves F2 and CI-1, and leaves F3 unconfirmed
- **THEN** the comment shows `- [ ] F1 … open since attempt 1`, `- [ ] F4 New: …`, `- [x] F2 … resolved`, `- [x] CI-1 … passing now`, `F3 … not re-checked`, and the pointer to attempt 2 in the Review tab, without the report body

#### Scenario: Earlier resolution is not repeated
- **WHEN** F2 was resolved in review 2 and stays resolved in review 3
- **THEN** review 3's comment omits F2, including when its source ran and passed again

#### Scenario: CI failure gets a comment
- **WHEN** review CI step 2 (`cargo test`) fails and no reviewer runs
- **THEN** one comment reads like `Review failed (attempt N): CI step 2 failed` with `- [ ] CI-2 cargo test …` and the Review-tab pointer

#### Scenario: Reviewer without findings
- **WHEN** a reviewer fails with only a reason and a 20 KiB report
- **THEN** the comment carries the headline, the reason and the first 2 KiB of the report

### Requirement: Repair and park comments name the findings
When Forge dispatches a coder repair round, it SHALL post one system comment
naming:
- the round;
- the budget it uses;
- the open finding ids;
- the ids resolved since the last round.

When Forge parks a Task because of repeated or non-progressing findings, it
SHALL post one system comment naming the finding ids and the rule that
parked it.

Both comments SHALL be idempotent per Review, so a replayed step does not
post them twice.

The no-change gate SHALL also post these Forge comments without creating a new
Review: a send-back comment states that HEAD still matches the failed candidate
and names the `repair_no_change` budget spent and remaining; a park comment
names the open findings, the no-change rule, and owner actions. These are
idempotent per failed Review and comment kind.

#### Scenario: Progress round comment
- **WHEN** Forge grants a progress round after review 2
- **THEN** one comment reads like `Repair round 2 (progress round 1 of 2): F1, F4 open; F2, CI-1 resolved since round 1.`

#### Scenario: Park comment
- **WHEN** F1 is open for the third time in a row
- **THEN** one comment says the Task parked because F1 survived two repair rounds, and names the owner actions

### Requirement: Review tab shows the ledger
The web Review tab SHALL show the finding ledger above the latest attempt. The
ledger has each finding's id, source, severity, status, streak and summary,
with its `where`, `expected` and `observed` text expandable.

Each attempt in the history SHALL show its own findings and its verify
results. Verify evidence SHALL be shown with the existing Task evidence
preview.

#### Scenario: Owner reads a parked Task
- **WHEN** the owner opens a Task parked with `repeated finding: F1`
- **THEN** the Review tab lists F1 as open with streak 3 next to the attempts that raised it, and the owner actions
