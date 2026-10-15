---
created_at: 2026-10-15T02:15:00Z
updated_at: 2026-10-15T02:15:00Z
---

## Why

A repair loop only converges when three things hold:
- the coder knows the **target**;
- each round tries something **new**;
- **progress** can be measured.

Live data from 10.0.0.2 (read-only, 2026-10-14) covers the 25 Tasks whose
review failed twice or more:

| Pattern | Tasks | Outcome |
|---|---|---|
| The same CI step (`pnpm test`, Project `world`) failed every round. The failing tests changed between rounds, and some were flaky (`AgentAuthorityHttpTransportError` timeouts). Nothing was run on the base. The project's own TEST-FIX Tasks for suite nondeterminism suggest the base was flaky too. | 8 | 4 cancelled, 3 still open, 1 done after 8 failures |
| One root cause, one instance fixed per round (NK-28 stale async apply ×3, NK-32 mention index ×2) | 2 | done, slowly |
| "PLAN ONLY" Tasks returned an empty candidate three times | 2 | cancelled. An empty candidate parks since 10/13 (`missing_review_candidate`). |
| The reviewer could not verify (other OS, GUI, toolchain), or only the owner could fix it | ~7 | now mostly `blocked` / `fixable_by: owner` |
| Real progress, a different finding each round | ~4 | done |
| A round undoing the previous round | 0 | — |

What went wrong in the biggest group:
- **The target contradicted the scope.** A review CI step means "this whole
  command exits 0". When the base is already red or flaky, the coder chases
  failures it did not cause.
- **Scope conflict, shown directly.** TEST-FIX3 attempt 6 failed review
  because "four unrelated web-client test files are outside the authorized
  scope". My reading is that the coder edited them to get the suite green.
- **Progress was invisible.** AI-C10R2 went from 28 failing tests to 26, and
  both rounds read only as "`pnpm test` failed". The coder sees a 4 KiB tail
  that often lacks even the summary line, padded with ANSI colour codes.

Published work points the same way:
- OpenHands detects repeated action and error pairs, nudges once, then marks
  the run stuck.
- Reflexion carries 1–3 written reflections between attempts.
- AlphaCodium accepts a fix only when it keeps the tests that already pass.
- MCR-Bench finds that multi-round LLM review re-flags resolved defects
  (32.5% of false positives) and over-reviews (27.8%).

## What Changes

This change builds on `add-structured-repair-and-verify` (the findings ledger,
the repair brief and the round policy).

**1. Per-test check failures.**
- The check executor extracts failure signatures while a step runs. It
  supports cargo test, vitest/jest, pytest and go test, plus a JUnit XML file
  a step declares.
- At most 200 signatures are kept per step, normalised and with ANSI codes
  stripped.
- Each failing test becomes its own `CI-<step>#<hash>` finding, so 28 → 26
  shows as progress.
- A step without signatures stays one `CI-<step>` finding, as today.
- All stored tails are stripped of ANSI codes.

**2. CI triage: a flake re-run and a base comparison.** When a review CI step
or a merge-queue head check fails:
- Forge re-runs the failing steps once on the candidate. Signatures that do
  not fail both times are **flaky**.
- Forge then runs the failing steps once on the base commit: the merge base
  for review, the target tip in the queue. This happens on the same machine,
  in a clean checkout, with the Project's `setup_steps` run first so
  dependencies are installed. That run is cached per commit, so every Task on
  the same base shares it. Signatures that also fail there are
  **pre-existing**.
- With no `setup_steps`, on daemon placements, or when the base run itself
  breaks, nothing is classified, and failures block as today, with a note
  saying why.
- Only consistent, introduced failures reach the coder.
- Flaky and pre-existing failures go to new Project attention items
  `flaky_check` and `base_check_failing`. They do not block the Task when the
  Project's `review_ci_triage` is `full` (the default).
- Without signatures, a step that also fails on the base still blocks as
  today, but the brief and the attention item say so.

**3. No-change gate.**
- A Task that comes back to review with the exact commit its previous failed
  review judged starts no CI, verify or reviewer.
- It goes back to the coder once with "no change since the last review".
- The second time, it parks with `review_needs_owner` (`no change after
  repair round`).
- No review budget is spent.

**4. Reviewer anchoring.** Findings gain three optional fields:
- `requirement_ids`;
- `kind` (`regression | missed | out_of_scope`);
- `same_cause_as`.

From the second review in a window, a new finding stays blocking only if one
of these holds:
- it is a `regression`;
- it cites a requirement of the frozen review contract;
- its `where` is a file in the Task's candidate diff.

Otherwise it is `minor`. `out_of_scope` findings never block; they are listed
as follow-up candidates. A resolved finding that fails again is `reopened`.

**5. Convergence in the repair brief.**
- **Attempts:** the brief gets the last three rounds' files changed, the
  coder's summary, and what each round resolved or left open. All of it is
  Forge-derived.
- **Requirement status:** a line per requirement, derived from the findings
  that cite it.
- **Pattern section:** findings joined by `same_cause_as` get a section that
  asks for a fix at every call site.
- **When a finding is carried:** the round starts a fresh coder session
  instead of resuming, and asks for a root-cause note before any edit. With
  the Project setting `repair_switch_model_when_stuck`, the round uses the
  Agent's next fallback candidate.

**Public surfaces:**
- `StepResultEntry.failures` and `StepResultEntry.triage`;
- the `review_ci_triage` and `repair_switch_model_when_stuck` Project
  settings;
- the finding fields `requirement_ids`, `kind`, `same_cause_as`;
- the ledger statuses `reopened`, `out_of_scope`, `flaky`, `pre_existing`;
- the `AttentionCategory` values `flaky_check` and `base_check_failing`;
- the new step ids in Task comments.

CHANGELOG:
- **`### Changed`:** failures that are pre-existing on the base, or flaky, no
  longer block a Task's review or merge under the default
  `review_ci_triage: full`. Projects that want the old gate set
  `review_ci_triage: off`.
- **`### Added`:** everything else.

## Impact

- **Affected specs** (new capabilities): `check-failure-detail`,
  `ci-triage`, `reviewer-anchoring`, `repair-convergence`. This change amends
  `review-findings` and `repair-brief` from `add-structured-repair-and-verify`.
- **Affected code:**
  - `crates/check-executor` (signature extraction, ANSI strip) and
    `crates/api-types/src/check_spec.rs` (`CheckCommandOutcome.failures`);
  - `crates/api-types/src/repo_review.rs` (`StepResultEntry`);
  - `crates/services/src/workflow/actions/review.rs` (`RunCiSteps`, triage,
    the no-change gate);
  - `crates/services/src/integration_steps/settle.rs` and `send_back.rs`
    (queue head triage);
  - `crates/services/src/check_runner/`;
  - `crates/services/src/attention_service.rs` and
    `crates/api-types/src/mission_control.rs`;
  - the review contract and prompt in `crates/review/src/contract.rs`;
  - the repair brief and dispatch in `workflow/dispatch/` and
    `workflow/actions/dispatch.rs`;
  - `crates/db/src/budget.rs`;
  - the web: Review tab, comments, Project settings, Mission Control.
- **Docs:** `docs/architecture.md` (review CI, merge queue, Retry budgets,
  Attention), `docs/api.md`, `CHANGELOG.md`.
- **Data:** the export the numbers above came from is at
  `/Volumes/Data/tmp/munk-research/live/multi-fail-reviews.json`.
