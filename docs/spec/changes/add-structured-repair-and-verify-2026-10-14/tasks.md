---
created_at: 2026-10-15T01:40:00Z
updated_at: 2026-10-15T09:11:52Z
completed_at:
---

Each phase lands as its own commit on the implementation branch with its
focused tests green. Run only the tests named in a phase; CI runs the full
suite.

## 1. Findings contract and ledger
- [x] 1.1 `ReviewFinding` and `ReviewAssessment.findings` / `resolved` in `crates/api-types/src/review_conformance.rs`, with `#[serde(default, skip_serializing_if = "Vec::is_empty")]`. Export them to `web/src/types/generated/bindings/`.
- [x] 1.2 Lenient parsing in `crates/review/src/contract.rs` `parse_assessment` / `result_block`:
  - drop items without a summary;
  - default the severity;
  - cap at 50 items and 1,000 characters per text field;
  - a malformed list never fails the block.
- [x] 1.3 Update `RESPONSE_INSTRUCTION` and the correction prompt (`review_correction.rs`). Refresh the snapshot `crates/services/tests/fixtures/server_workspace_reviewer.prompt.snap`.
- [x] 1.4 `prior_review_section` lists the prior ledger's open findings with their ids and asks for `id` / `resolved`. Keep the existing 8 KiB report slice.
- [x] 1.5 Write `finding_ledger(window_reviews)`, a pure function in the `review` crate or `services`. It covers:
  - Forge ids (`F<n>`, `CI-<i>`, `CHK-<id>`, `V-*`). `MERGE-*` stays out of the ledger and is brief-only;
  - the review window boundary;
  - checked versus not-run sources;
  - `unconfirmed`;
  - streaks.
- [x] 1.6 `GET /api/v1/tasks/{id}/findings` with the same authority as `/reviews`, its response type in `api-types`, generated TS, and `docs/api.md`. Add `forge-ctl task findings` and `docs/cli.md`.
- [x] 1.7 Focused tests:
  - parser cases: findings, malformed, legacy, caps;
  - a V145-era `conformance_json` round-trips byte-identical through `record_review_conformance`;
  - ledger unit tests for continue/resolve, forgotten finding (`unconfirmed`), CI-before-reviewer (source not run), and a new window after recovery;
  - a route test for the read and the refusal.

## 2. Comments and Review tab
- [ ] 2.1 `reviewer_comment` (`cascade.rs`) renders the headline, the ledger task list, the verify line and the Review-tab pointer. Without findings it keeps the reason plus the first 2 KiB of the report. Update `reviewer_comment_*` unit tests.
- [ ] 2.2 Repair-round and park system comments, idempotent per Review (`idempotency_key`).
- [ ] 2.3 Web:
  - Ledger table in `TaskReviewTab.tsx`.
  - Per-attempt findings and verify block in `ReviewConformancePanel.tsx`.
  - Comment task lists render through the existing `remarkGfm`. Check this in `task-comments-panel.tsx`.
  - Vitest coverage for the ledger table and a park.
- [ ] 2.4 Focused tests: comment text for failed with findings, failed without findings, and passed with minor findings. Assert that a replayed cascade posts no duplicate comment.

## 3. Repair brief
- [ ] 3.1 Add a `RepairBrief` builder in `crates/services/src/workflow/dispatch/`:
  - sections in the D7 order;
  - a 24 KiB budget with the D7 drop order;
  - sources from the ledger, Review `ci_steps` (all failing steps), `details.verify`, and the previous coder completion summary.
- [ ] 3.2 `review_fix_user`, the CI path, the autonomous worker builder (`BUILDER_ID_WORKER_REVIEW_FIX_V1`) and `render_automatic_review_recovery_prompt` use the brief. The CI path regains the Task description and plan.
- [ ] 3.3 `merge_fix_user` handles merge-time `CiFailed` from `integration_steps/send_back.rs`: a `MERGE-CI` finding with the target, command and tail. The conflict handoff uses a `MERGE-CONFLICT` finding. Dirty-worktree text is unchanged.
- [ ] 3.4 Focused tests in `workflow/dispatch/tests.rs`:
  - update `coder_prompt_review_follow_up_is_self_contained` and `coder_prompt_ci_follow_up_is_focused_on_failing_check`;
  - keep `review_feedback_comes_from_the_reviewer_execution_not_the_reviewed_one` and `merge_fix_prompts_hand_a_committed_conflict_to_the_worker` green;
  - new: a two-round brief lists resolved and open findings;
  - new: the budget trims the report first;
  - new: a merge-time `CiFailed` brief carries the command and tail.

## 4. Round policy
- [ ] 4.1 `Kind::ReviewProgress` (key `review_progress`, default 2, Review window) and `Kind::ReviewVerifyRerun` (volatile, limit 1) in `crates/db/src/budget.rs`. Add a numbered migration only if `task_budget.kind` is constrained.
- [ ] 4.2 Apply the D5 rules in `review_failure_target` and the hook-path duplicate in `workflow/actions/review.rs`:
  - derived repeat (reviewer `F*` findings only) parks with `ReviewNeedsOwner` and the message `repeated finding: <id> <summary>`;
  - the progress round charges `review_progress`;
  - derived rules turn off for a window with a findings-less fail;
  - lead roots keep `charge_replan`.
- [ ] 4.3 Change `owner_review_failure_message` so it ignores the self-reported `repeat` while the derived rules are on. Owner-fixable routing is unchanged.
- [ ] 4.4 `retry_budgets.review_progress` is accepted by the Task/Project editors, `api-types`, TS and `docs/api.md`. Add the `remaining` projection on the Task response.
- [ ] 4.5 Focused tests:
  - keep the five `review_finding_routing_*` tests in `task_service/tests/service_tests/cases/executions.rs` green;
  - new: progress round granted, no progress parks exhausted, carried with progress gets one more round, third appearance parks, repeat without progress parks, a repeated `CI-*` failure never parks as `review_needs_owner`, `review_progress = 0` disables;
  - `system_review_ci_failure_routes_to_coder_and_spends_review_budget` and `dispatcher_failed_review_composes_finding_routing_with_ci_budget` stay green;
  - the autonomous workflow uses `working` as its target.

## 5. Verify check
- [ ] 5.1 `ReviewConfig.verify` types and validation (`api-types` `core.rs`, the Project/Task update routes), generated TS, `docs/api.md`, and the Project review settings form in the web.
- [ ] 5.2 `CheckPurpose::ReviewVerify`. A bundle builder generates the background start step, the readiness step and one step per check, all `Uncacheable`, under `canonical-ci/1` with reuse off and verify's own wall limit.
- [ ] 5.3 Port pick, the `FORGE_VERIFY_*` environment variables, `request.json` (from the Task, its requirements, the diff and the ledger), and the per-check report path.
- [ ] 5.4 Order in `RunCiSteps`: after CI passes, request verify, suspend, and resume. A fail settles the Review failed with `V-*` findings before reviewer dispatch. A daemon placement records `not_run` / `placement_unsupported`.
- [ ] 5.5 Verdict mapping and report parsing. An inconclusive result re-runs once, then parks with `review_blocked` (source verify) and the actions retry, mark_reviewed and cancel. Infrastructure failures use the existing `review_ci_infrastructure` path.
- [ ] 5.6 Evidence capture from report paths, refusing escapes and applying the size and kind limits. Add the allowed detail key `verify`. The reviewer prompt gets a "Verify results" section.
- [ ] 5.7 Focused tests:
  - check-executor: the run tree is stopped after a background start;
  - review hook: verify fails before the reviewer, CI fail skips verify, the report overrides a zero exit, a ready timeout gives `V-ready` with an `app.log` tail, inconclusive twice parks, daemon `not_run`, path escape;
  - `reviewer_receives_and_can_cite_the_recorded_pre_review_ci_result` gains a verify-results sibling.

## 6. Release
- [ ] 6.1 `docs/architecture.md`: the Retry budgets table gets `review_progress` and `review_verify_rerun`. Also update Failure classification, review routing, the checks section (verify) and the Task comments notes.
- [ ] 6.2 `CHANGELOG.md` `Unreleased`:
  - `### Added`: findings, the ledger route and CLI, verify, the progress budget.
  - `### Changed`: review comments drop the full report; extra repair rounds on progress; the merge-time check failure brief.
- [ ] 6.3 `crates/api/tests/suite/happy_path.rs`: a named case where the review fails with findings, the coder repairs, the re-review resolves them, and the Task passes and merges. Extend `forge_happy_path_end_to_end` only if a separate case cannot reach that path.
- [ ] 6.4 Live check on a `./test` data dir with a small web Project:
  - verify catches a broken button and the coder fixes it;
  - a two-round repair shows the ledger in comments;
  - a repeated finding parks with the right message.
