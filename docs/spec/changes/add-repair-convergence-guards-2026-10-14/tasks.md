---
created_at: 2026-10-15T02:15:00Z
updated_at: 2026-10-15T22:34:39Z
completed_at:
---

## Ordering

- **Phases 1–3 do not need the ledger** from `add-structured-repair-and-verify`. They can land first or in parallel. They address the largest waste in the live data, the `world` CI loops.
- **Phases 4–5 need** phases 1 and 3 of that change (the ledger and the repair brief).
- Each phase lands as its own commit with its focused tests green. Run only the named tests; CI runs the full suite.

## 1. Check failure detail
- [x] 1.1 Signature extractors in `crates/check-executor`:
  - cargo test/nextest, vitest/jest, pytest, go test;
  - JUnit XML under `FORGE_CI_REPORT_DIR`;
  - normalisation, a cap of 200 signatures at 300 characters each, `failures_truncated`.
- [x] 1.2 Strip ANSI codes from stored `output_tail` and `stderr_tail`.
- [x] 1.3 Add `CheckCommandOutcome.failures` and `StepResultEntry.failures` (optional, `serde(default)`). Wire them through the durable runner, daemon receipts (an absent field is an empty list), generated TS and `docs/api.md`.
- [x] 1.4 **Pre-ledger half complete.** Until the ledger lands, the CI repair prompt (`ci_failure_reason`) lists signatures for every failing step, not only the first. Once the ledger exists, add `CI-<step>#<hash>` findings in `finding_ledger`.
- [x] 1.5 Focused tests:
  - extractor unit tests per runner, using real captured output from the `world` and NovelKit logs;
  - JUnit parse;
  - ANSI strip;
  - an older-daemon receipt without `failures`.

## 2. CI triage
- [ ] 2.1 Project setting `review_ci_triage` (`full` default, `report`, `off`): validation, api-types, TS, `docs/api.md`, web settings.
- [ ] 2.2 Volatile budget `ReviewCiTriage` (limit 1) in `crates/db/src/budget.rs`.
- [ ] 2.3 Re-run the failing steps once in `RunCiSteps` (`workflow/actions/review.rs`), then classify flaky signatures and steps.
- [ ] 2.4 Base run:
  - same owner, Project env, assets and build environment as the candidate run;
  - `ExactCommit` checkout, running `setup_steps` and then the CI steps up to the last failing one;
  - `canonical-ci/1`, with its own identity so it is reused per base commit;
  - classify `pre_existing`, `introduced` and `pre_existing_unverified`;
  - report `base_unavailable` when there are no `setup_steps` or the Task is on a daemon;
  - report `base_run_invalid` when setup fails, or when the base has no signatures but the candidate has some.
  - Triage never changes the stored `check_result` outcome, and never certifies a pass.
- [ ] 2.5 Routing under `full`: no blocking failures left means the step counts as passed. Record the triage in the Review details (`ci_steps[].triage`). `report` and `off` keep today's routing.
- [ ] 2.6 Queue-head triage against the target tip in `integration_steps/settle.rs` and `send_back.rs`. Under `full`, no send-back for flaky or pre-existing failures.
- [ ] 2.7 `AttentionCategory::{FlakyCheck, BaseCheckFailing}`: dedupe keys, resolution on a passing base run, Mission Control rendering, `docs/api.md`.
- [ ] 2.8 Focused tests:
  - flaky step passes on re-run;
  - consistent failures go to the base;
  - red base under `full` continues to the reviewer;
  - one introduced failure reaches the coder;
  - `report` blocks as today;
  - `off` does no re-run;
  - a shared base run between two Tasks;
  - no `setup_steps` gives `base_unavailable`, and failures block;
  - an install failure gives `base_run_invalid`, and failures block;
  - a triaged pass is not reused at the queue head;
  - the queue head does not send back pre-existing failures.
  - Keep `system_review_ci_failure_routes_to_coder_and_spends_review_budget` green under `off`.

## 3. No-change gate
- [x] 3.1 Persistent budget `RepairNoChange` (Review window, limit 1).
- [x] 3.2 At review entry after a coder repair round: if the latest Review failed and HEAD equals its judged commit with a clean worktree, create no Review. Send back once with the "No change since review N" brief, then park `review_needs_owner` with `no change after repair round`. Owner-started re-reviews are exempt.
- [x] 3.3 Focused tests: unchanged once sends back without a Review row or review budget; unchanged twice parks; an owner re-review of the same commit runs; a changed commit reviews normally.

## 4. Reviewer anchoring
- [ ] 4.1 Finding fields `requirement_ids`, `kind`, `same_cause_as`: lenient parsing, omitted when empty, TS, `docs/api.md`.
- [ ] 4.2 `RESPONSE_INSTRUCTION` and the re-review section list the frozen requirement ids and the later-review rule. Refresh the reviewer prompt snapshot.
- [ ] 4.3 Ledger rules:
  - from review 2, downgrade to `minor` unless the finding is a regression, cites a requirement, or is in a candidate-diff file;
  - `out_of_scope` never blocks;
  - `reopened`.
- [ ] 4.4 Park with `only non-blocking findings remain` when nothing blocking is left after anchoring.
- [ ] 4.5 Focused tests: an untouched-file nit is downgraded; a cited `missed` finding stays blocking; out-of-scope only parks; an unknown requirement id is dropped; a re-broken test is `reopened`.

## 5. Converging repair brief
- [ ] 5.1 The Attempts section (last three rounds) replaces Previous round, with its drop-order position.
- [ ] 5.2 Requirement status line and Pattern section.
- [ ] 5.3 Carried-finding round:
  - fresh execution instead of `ResumeLatestTargetRoleThread`;
  - the root-cause opening line;
  - `repair_switch_model_when_stuck` (Project setting, api-types, TS, docs, web) selects the next fallback candidate for that round only.
- [ ] 5.4 Exclude `flaky`, `pre_existing`, `minor` and `out_of_scope` from the round policy's open count.
- [ ] 5.5 Focused tests:
  - a round-3 brief lists both earlier attempts;
  - the Pattern section appears for a `same_cause_as` family;
  - a carried finding starts a fresh execution;
  - the model switch applies to one round only;
  - pre-existing failures do not stall progress.
  - Keep `auditor_failure_dispatches_follow_up_executor_with_thread_reuse` green for non-carried rounds.

## 6. Release
- [ ] 6.1 `docs/architecture.md`: review CI triage, merge-queue head triage, Retry budgets (`review_ci_triage`, `repair_no_change`), Attention categories, repair brief.
- [ ] 6.2 `CHANGELOG.md` `Unreleased`:
  - `### Changed`: pre-existing and flaky failures no longer block under the default `review_ci_triage: full`; opt out with `off`.
  - `### Added`: everything else.
- [ ] 6.3 A named `crates/api/tests/suite/happy_path.rs` case: on a base with one failing test, a Task's review passes CI triage, the reviewer passes it, and the Task merges.
- [ ] 6.4 Live check on a `./test` Project seeded with one flaky test and one base-failing test:
  - the Task is not sent to the coder for either;
  - both attention items appear;
  - an unchanged repair round is caught.

## 7. Review follow-ups for phases 1 and 3
- [x] 7.1 Select the latest Review before checking its verdict; a later pass suppresses an old failure.
- [x] 7.2 Resume pre-gate entries as `Continue` from their recorded entry progress, without evaluating the gate.
- [x] 7.3 Scope failure glyphs to recognised vitest/jest blocks; capture nextest output and extract stable binary/test identities.
- [x] 7.4 Carry the original entry source and actor through offline same-state retries, and clear the resolved infrastructure blocker on a reminder.
- [x] 7.5 Remove abandoned Forge report directories at the next check, without following symlinks or deleting unrelated entries.
- [x] 7.6 Run the requested focused verification and record results under `/Volumes/Data/tmp/refactor/guards-13b/`.
