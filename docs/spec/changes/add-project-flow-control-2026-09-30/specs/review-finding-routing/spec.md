## ADDED Requirements

### Requirement: Reviewer declares who can fix a failure
The reviewer result block SHALL accept two optional fields next to `result` and `reason`:
- `fixable_by`: `"coder"` or `"owner"`, default `"coder"`. `owner` means the blocking finding needs something the coder cannot provide in this Task: another OS or hardware, an external service or credential, Forge-side metadata, a scope or product decision, or a change to the acceptance criteria.
- `repeat`: a boolean, default `false`. `true` means the blocking finding was already raised by the previous review attempt and is still unaddressed.

Both fields are read leniently, like the existing block, and are stored on the `ReviewAssessment`. The reviewer prompt SHALL define both fields and give examples. Unknown values SHALL be treated as the defaults.

#### Scenario: Reviewer marks an owner-only finding
- **WHEN** the NK-48 reviewer ends with `{"result": "fail", "reason": "Forge linked_documents is empty", "fixable_by": "owner"}`
- **THEN** the stored assessment has `result = fail`, `fixable_by = owner`, `repeat = false`

#### Scenario: Older reply without the fields
- **WHEN** a reviewer returns only `{"result": "fail", "reason": "..."}`
- **THEN** it is treated as `fixable_by = coder`, `repeat = false`, and routed exactly as today

### Requirement: Owner-fixable and repeated failures park for the owner
A `fail` with `fixable_by = owner`, or a `fail` with `repeat = true` whose previous Review attempt also failed, SHALL finish the Review as failed. The Task SHALL be parked with a `review_needs_owner` blocking annotation: its message carries the reason and whether the park came from the owner tag or a repeat. The coder SHALL NOT be dispatched, and the review retry budget SHALL NOT be spent. The recovery actions SHALL be `reexecute` (retry with guidance), `mark_reviewed` (waive with a required reason), `defer_to_follow_up`, `open_interactive` and `cancel_task`.

#### Scenario: NK-48 no longer burns its budget
- **WHEN** NK-48's first review fails with `fixable_by = owner`
- **THEN** NK-48 is parked `review_needs_owner` after one attempt, the coder is not dispatched, and the review budget is unchanged

#### Scenario: Repeat breaker
- **WHEN** attempt 2 fails with `repeat = true` and attempt 1 also failed
- **THEN** the Task parks `review_needs_owner` instead of dispatching the coder a second time

#### Scenario: Repeat flag without a prior failure
- **WHEN** attempt 1 fails with `repeat = true`
- **THEN** the flag is ignored and the coder receives the findings as today

### Requirement: Defer a finding to a follow-up Task
The `defer_to_follow_up` recovery action SHALL require a reason. It SHALL create a new `backlog` Task in the same Project whose description carries the parked finding, linked to the original as a follow-up. It SHALL then mark the current Review passed with a reason naming the follow-up, so the original can proceed to merge. Both effects SHALL commit atomically.

#### Scenario: Cross-OS measurement split off
- **WHEN** the owner applies `defer_to_follow_up` on NK-37 with reason "macOS/Windows runs need a human"
- **THEN** a backlog Task "Follow-up: NK-37 — required cross-platform measurements…" exists, linked to NK-37
- **AND** NK-37's Review is passed with the reason naming that Task, and NK-37 moves on to merging

### Requirement: Owner parks are visible in review
The review tab SHALL show a `review_needs_owner` park with the reason, a "fixable by owner" or "repeated finding" badge, and the owner actions. Parked Tasks SHALL NOT hold an active-task slot.

#### Scenario: Owner opens a parked review
- **WHEN** the owner opens NK-48's review tab
- **THEN** the tab shows "Needs owner — repeated finding: Forge linked_documents is empty" with Retry with guidance, Mark reviewed, Defer to follow-up, and Cancel
