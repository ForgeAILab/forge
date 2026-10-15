---
created_at: 2026-10-15T01:40:00Z
updated_at: 2026-10-15T02:22:50Z
---

## Why

Comparing Forge with Munk AI (munk.sh) shows Forge already has most of Munk's
loop: planning, coding, gates, a durable workflow, retry budgets, owner parks
and a merge queue. Two parts of the loop are weak.

**Repair is thin.** When a review fails, the coder gets one round:
- **Little to work with.** It gets the latest reviewer's reason plus its
  Markdown report, cut at 12,000 characters.
- **CI failures:** it sees only the first failing step, and the prompt drops
  the Task description and plan.
- **Checks that fail at merge time:** the coder is told "manual task-worktree
  repair is required" and never sees which check failed (`merge_fix_user`).
- **No history:** the coder never learns which earlier findings it already
  fixed.
- **One round only:** the default review gate allows two failed verdicts, so
  the coder gets one repair round. After that the Task parks for the owner,
  even when the coder fixed most of what the reviewer raised.
- **Repeat parks are blunt:** a repeat parks at once, whatever else the round
  fixed.

**Comments are noisy.** Every review posts its full Markdown report to the Task
comments, up to 32 KiB per attempt. There is no short list of what is still
open, what got fixed, and what is new.

**Nothing checks the running app per Task.** Review CI runs commands, and the
reviewer reads the diff. The coder checks its own work, and milestone
acceptance checks look at integrated behaviour. No independent step starts
the app built from a Task's candidate and exercises the change before the
reviewer is paid to read it. Munk calls this Deploy → Verify.

## What Changes

**1. Structured findings.**
- The reviewer result block gains two optional fields: a `findings` list and a
  `resolved` list of ids.
- Forge numbers findings. A re-reviewer refers back to them by id.
- Forge-owned failures become findings in the same shape. These are CI steps,
  required checks and verify checks. Merge-time checks and merge conflicts
  appear in the repair brief, not in the ledger.
- A **finding ledger**, derived from the Task's Reviews and never stored, says
  what is open, resolved, unconfirmed or new in each round.
- It is readable at `GET /api/v1/tasks/{id}/findings`.
- Older replies without findings behave exactly as today.

**2. One repair brief for every repair round.**
- Every round where Forge sends work back to the coder uses one brief. That
  covers review, CI, verify, merge-time checks and merge conflicts.
- The brief carries:
  - the Task and its plan;
  - the open findings, with how many rounds each has survived;
  - what the last round resolved;
  - the tails of all failing checks;
  - verify evidence;
  - a bounded slice of the reviewer's report.
- It has a fixed byte budget and a drop order.
- Fixes the merge-time check failure that reaches the coder without detail.

**3. More repair rounds, but only while the coder is fixing things.**
- A new `review_progress` retry budget (default 2) grants extra repair rounds
  after the review budget is spent. A round is granted only while the set of
  open blocking findings strictly shrinks.
- A carried reviewer finding (open in two reviews in a row) gets one more
  round only if the same review also shows progress.
- A reviewer finding open in three reviews in a row parks the Task for the
  owner. Forge's own check failures keep today's budget routing.
- A repeat with no progress parks at once, as today.
- This amends the "Owner-fixable and repeated failures park for the owner"
  requirement of
  `add-project-flow-control-2026-09-30/specs/review-finding-routing`. When a
  block has no findings, the self-reported `repeat` flag keeps today's
  behaviour.

**4. A verify check in review.**
- `ReviewConfig.verify` can start the app from the candidate, wait until it is
  ready and run behavioural checks.
- Forge provides:
  - a free loopback port;
  - a request file describing the change;
  - a report path for the verdict and evidence files.
- Verify runs after review CI passes and before the reviewer is dispatched.
  A failing verify never pays for a reviewer, and the reviewer sees the
  verify results and evidence.
- The verdict is `pass`, `fail` or `inconclusive`.
  - A fail becomes `V-*` findings sent through repair.
  - An inconclusive result re-runs once, then parks as `review_blocked` with a
    re-run action. It does not spend review budget.
- Evidence files become Task evidence linked to the Review.
- **Version 1 runs verify on server-owned placements only.** A daemon-placed
  Task records verify as `not_run` with reason `placement_unsupported`, and
  its review continues.

**5. Comments built from findings.**
- A review comment becomes a headline, a findings checklist (new, still open,
  resolved, unconfirmed), a verify line with evidence links, and a pointer to
  the Review tab. The full report moves out of the comment and stays in the
  Review tab.
- Forge posts a short comment when it starts a repair round and when it parks
  a Task over findings. Both name the finding ids.

**Public surfaces** (each change touches the route, `api-types`, generated TS
and `docs/api.md`):
- result block fields `findings`, `resolved`;
- `ReviewAssessment.findings`, `ReviewAssessment.resolved`;
- `ReviewConfig.verify`;
- the Review details key `verify`;
- `GET /api/v1/tasks/{id}/findings` and `forge-ctl task findings`;
- the `review_progress` key in `retry_budgets`;
- the environment variables `FORGE_VERIFY_*`;
- the new `CheckPurpose::ReviewVerify`.

CHANGELOG: `### Added` for findings, verify and the progress budget.
`### Changed` for the following:
- Review comments no longer include the full report. Scripts that parse
  comment text must read `GET /api/v1/tasks/{id}/reviews` instead.
- Tasks that used to park at review-budget exhaustion may now get up to two
  more repair rounds when they are making progress.

Nothing here is a schema break; every new field is optional.

## Impact

- **Affected specs** (new capabilities): `review-findings`, `repair-brief`,
  `review-verify-check`, `review-comments`. This change amends
  `review-finding-routing` from `add-project-flow-control-2026-09-30`.
- **Affected code:**
  - `crates/review/src/contract.rs`: parser, `RESPONSE_INSTRUCTION`,
    `prior_review_section`, `reviewer_context_prompt`, allowed detail keys.
  - `crates/api-types/src/review_conformance.rs` and `core.rs`
    (`ReviewConfig`).
  - `crates/api-types/src/check_spec.rs` (`CheckPurpose`).
  - `crates/db/src/budget.rs`: `Kind::ReviewProgress`,
    `owner_review_failure_message`.
  - `crates/services/src/task_service/execution/cascade.rs`:
    `review_failure_target`, `reviewer_comment`, park paths.
  - `crates/services/src/workflow/dispatch/{loader,coder_prompt}.rs`: the
    repair brief.
  - `crates/services/src/workflow/actions/{review,merge}.rs` and
    `integration_steps/send_back.rs`.
  - `crates/services/src/check_runner/` and `crates/check-executor/`: the
    verify bundle.
  - `crates/services/src/native_tools.rs`: evidence capture.
  - Routes in `crates/api/src/routes/tasks/`.
  - `crates/forge-client`.
  - Web: `ReviewConformancePanel.tsx`, `TaskReviewTab.tsx`,
    `task-comments-panel.tsx`, the Project review settings form.
- **Docs:** `docs/architecture.md` (Retry budgets, Failure classification,
  review routing, checks), `docs/api.md`, `docs/cli.md`, `CHANGELOG.md`.
- **Tests:** see `tasks.md`. The canonical end-to-end case is
  `crates/api/tests/suite/happy_path.rs`.
