## ADDED Requirements

### Requirement: One repair brief for every repair round
Forge SHALL build the coder's execution request from one repair brief
whenever it sends a Task back to its coder after a failure. This covers a
failed review, a failed review CI step, a failed verify, a merge-time check
failure, a merge conflict handoff, and automatic review recovery.

The brief SHALL contain these sections, in this order:
1. **Task:** title, description and plan.
2. **Round:** the round number, the budget it is charged to, and what remains.
3. **Open findings:** id, source, severity, summary, where, expected,
   observed, and how many rounds the finding has survived.
4. **Resolved and unconfirmed:** what changed since the last round.
5. **Failing check output:** the output tail of every failing check, not only
   the first.
6. **Verify:** verify verdicts and evidence file paths.
7. **Reviewer report:** a bounded slice of the reviewer's Markdown report.
8. **Previous round:** the coder's previous completion summary.
9. **Logs:** reviewer and coder log paths.

The prompt builder IDs and the session-resume policy SHALL stay as they are.

#### Scenario: Review fix after two rounds
- **WHEN** review 2 fails with F1 open (streak 2), F2 resolved and F4 new
- **THEN** the coder's request lists F1 and F4 under open findings, says F1 survived one repair round, and lists F2 as resolved

#### Scenario: CI failure keeps the Task context
- **WHEN** review CI steps 1 and 3 fail
- **THEN** the brief includes the Task description, the plan, and the output tails of both steps

### Requirement: The repair brief has a byte budget
The brief body SHALL be at most 24 KiB. When over budget, Forge SHALL cut
sections in this order until it fits:
1. the reviewer report, down to 2 KiB, then dropped;
2. the previous round summary, dropped;
3. each check tail, down to 1 KiB;
4. the resolved and unconfirmed list, down to ids only.

The Task section and the open findings are never dropped. When the brief is
still over 24 KiB, Forge SHALL next share 4 KiB among the check tails and keep
2 KiB of the verify section. If it is still over, each finding's summary is cut
to 200 characters, `where` to 120, and `expected` and `observed` to 300; past a
line budget the remaining open findings are listed by id only.

#### Scenario: Fifty long findings
- **WHEN** a review fails with 50 findings whose texts are 1,000 characters each
- **THEN** the brief is at most 24 KiB and names every open finding, in full-clipped form or by id

#### Scenario: Large report is trimmed first
- **WHEN** the reviewer report is 40 KiB and the rest of the brief is 6 KiB
- **THEN** the brief is at most 24 KiB, contains every open finding in full, and contains at most 2 KiB of the report

### Requirement: Merge-time check failures reach the coder with detail
The merge-fix request SHALL use the repair brief with a `MERGE-CI` entry in
its open-findings section when checks fail after the rebase in the merge
queue (`FailureKind::CiFailed` from send-back). The entry is read from the
send-back metadata, not from the review ledger. A conflict handoff appears
the same way as `MERGE-CONFLICT`. The `MERGE-CI` entry carries:
- the target branch;
- the failing command;
- its output tail.

The request SHALL NOT tell the coder that manual worktree repair is required.

#### Scenario: Tests fail after rebase
- **WHEN** a Task's merge attempt rebases onto `main` and `cargo test` fails
- **THEN** the coder's merge-fix request contains the `MERGE-CI` finding with `cargo test` and its output tail, and no "manual task-worktree repair" text

### Requirement: Progress rounds after the review budget is spent
A new retry budget kind `review_progress` SHALL exist:
- key `review_progress` in `retry_budgets`;
- default limit 2, and `0` disables it;
- the Review origin window;
- owner actions never spend it.

When the review budget does not allow another round, Forge SHALL grant a
progress round instead of parking only when both of these hold:
- the number of open blocking findings strictly dropped since the previous
  review in the window;
- `review_progress` allows a retry.

A progress round SHALL use the workflow's reject target (`in_progress` by
default, `working` in the autonomous workflow). It SHALL be charged to
`review_progress` with a receipt keyed by the Review. The derived rules are
off for a window once any reviewer `fail` in it had no blocking finding. In
that case today's exhaustion behaviour applies.

Coordination roots with a lead SHALL keep `charge_replan` and get no progress
rounds.

#### Scenario: Progress earns a round
- **WHEN** under the default gate, review 1 opens F1, F2 and F3, and review 2 leaves only F1 open
- **THEN** instead of parking with `review_budget_exhausted`, the coder gets a progress round and `review_progress` shows 1 of 2 spent

#### Scenario: No progress parks as before
- **WHEN** the review budget is spent and review 2 leaves the same number of findings open as review 1
- **THEN** the Task parks with `review_budget_exhausted` as today

#### Scenario: Disabled
- **WHEN** a Task's `retry_budgets.review_progress` is 0
- **THEN** no progress rounds are granted

### Requirement: Repeated findings park from the ledger
Forge SHALL decide repeats from the ledger, not from the reviewer's `repeat`
flag, while the derived rules are on. This requirement amends "Owner-fixable
and repeated failures park for the owner" from
`add-project-flow-control-2026-09-30/specs/review-finding-routing`.
- A reviewer finding (`F*`) open in three consecutive reviewer checks SHALL
  park the Task with `review_needs_owner` and the message
  `repeated finding: <id> <summary>`.
- A reviewer finding open in two consecutive reviewer checks SHALL also park
  the Task that way when the open blocking count did not drop.
- A reviewer finding open in two consecutive reviewer checks SHALL get
  another round when the count dropped.
- Forge-owned findings (`CI-*`, `CHK-*`, `V-*`) count toward the open
  blocking count but SHALL NOT trigger these parks. They keep today's budget
  routing.

No coder is dispatched and no budget is spent on these parks. While the
derived rules are off, the self-reported `repeat` flag SHALL keep its current
behaviour. Owner-fixable findings and `blocked` results keep parking before
these rules, unchanged.

#### Scenario: Carried finding with progress gets one more round
- **WHEN** review 2 still has F1 open but F2 and F3 are resolved
- **THEN** the coder gets another round and the brief marks F1 as having survived one repair round

#### Scenario: Third appearance parks
- **WHEN** review 3 still has F1 open
- **THEN** the Task parks with `review_needs_owner` and the message `repeated finding: F1 ...`, and the coder is not dispatched

#### Scenario: Repeat without progress parks at once
- **WHEN** review 2 has F1 and F2 open again and resolves nothing
- **THEN** the Task parks with `review_needs_owner` after review 2

#### Scenario: A failing CI step is not a repeat park
- **WHEN** review CI step 2 fails in three consecutive reviews
- **THEN** each failure follows the review budget (and `review_progress` only if the open count dropped), and the Task never parks with `review_needs_owner` because of `CI-2`

#### Scenario: Self-reported repeat without findings
- **WHEN** neither review in the window has a findings list and review 2 says `repeat: true`
- **THEN** routing is exactly as `review-finding-routing` specifies today
