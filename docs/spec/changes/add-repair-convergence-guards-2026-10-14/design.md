## Context

`add-structured-repair-and-verify` gives Forge three pieces:
- a finding ledger;
- a repair brief;
- a round policy (a normal round, progress rounds, and repeat parks).

That makes repair rounds visible and bounded. This change adds the inputs
those pieces need to converge:
- a target the coder can actually hit;
- a progress signal fine enough to see;
- a memory that steers each round away from what already failed.

Facts this design depends on:
- **The check executor runs exact commits.** It supports
  `CheckoutTarget::ExactCommit { repository, build_area, commit }` with
  `CheckScope::Commit`, which is a clean checkout of a commit.
  `canonical-ci/1` results are reusable when certified and cacheable.
- **The executor sees the full output, but only a tail is kept.** It streams
  every step's output. The durable runner keeps only a 4096-byte tail per
  step (`OUTPUT_TAIL_BYTES`). `steps_json` is capped at 256 KiB.
- **Reviews have no CI retry.** `ReviewCheckRerun` (volatile, limit 2)
  re-runs conformance checks that timed out. It never re-runs a failed review
  CI step.
- **The review contract freezes requirements.** It holds
  `requirements: [{id, source, text, ...}]` (`ReviewGoverningContext`).
- **Dispatch policies.**
  - A repair round resumes the coder's thread
    (`ResumeLatestTargetRoleThread`) when the same Agent runs it.
  - `dispatch_initial_role_execution_with_metadata` starts a fresh one.
  - Fallback chains exist per Agent.
- **Empty candidates already park.** `missing_review_candidate` parks a review
  that has nothing to review (added 10/13). There is no guard against an
  *unchanged* candidate after a repair round.

## Goals / Non-Goals

Goals:
- A check failure the Task did not cause never becomes the coder's job by
  default.
- Progress is counted per failing test, not per command.
- A round with no change costs nothing and stops quickly.
- From the second review on, the reviewer cannot move the goalposts without
  citing a requirement, a regression or the Task's own files.
- Each round sees what earlier rounds tried. A stuck finding gets a different
  approach, not the same session again.

Non-Goals:
- Semantic revert or oscillation detection. The live data shows none.
  `reopened` covers the cheap case.
- Best-of-n parallel candidates or tree search.
- The coder declaring which finding ids it addressed. Forge derives attempts
  itself.
- Automatically creating follow-up Tasks from `out_of_scope` findings. They
  are listed, and the owner acts.
- Test runners beyond cargo test/nextest, vitest/jest, pytest, go test and JUnit XML.

## Decisions

**D1 — Signatures come from the executor, while the step streams.**
- `check-executor` runs line extractors over each step's full output. ANSI
  codes are stripped first.
- What each extractor matches:

  | Runner | Lines matched |
  |---|---|
  | cargo test | `test <path> ... FAILED` |
  | cargo nextest | `FAIL [ <duration>] <binary> <test>` (optional progress counter; also `TIMEOUT`, `SIG*`, and final `TRY n` attempts with `ABRT`/`TMT`); binary and test form the identity |
  | vitest/jest | `FAIL <file> > <suite> > <test>` and `×`/`✗`/`●` lines inside recognised runner blocks; `FAIL <any JS/TS file>` opens a jest block |
  | pytest | `FAILED <file>::<test>` |
  | go test | `--- FAIL: <Test>` |

- Runner headers open vitest/jest blocks and their summaries close them. Glyphs
  outside those blocks are not test failures. Nextest stdout/stderr blocks and
  summary repetitions do not add duplicate libtest identities.
- **JUnit XML.** Forge exports `FORGE_CI_REPORT_DIR` (per run, inside the
  Task root). Any `*.xml` JUnit file a step writes there is parsed for
  failed and errored cases. The step list stays plain strings. At the next check
  command in that worktree, abandoned report directories matching Forge's
  generated names under `.forge-task` are removed without following symlinks
  or removing unrelated entries.
- **Normalisation.** Durations, absolute Task-root prefixes and ANSI codes
  are stripped. Each signature is at most 300 characters. At most 200
  signatures are kept per step, with a `failures_truncated` flag.
- **Storage.** The result is stored on `CheckCommandOutcome.failures` and
  `StepResultEntry.failures`, both optional with `serde(default)`. An owner
  that does not report them (an older daemon) falls back to step-level
  findings.
- **Finding ids.** Each finding is `CI-<step>#<first 8 hex of sha256(signature)>`,
  so the same test failing again carries the same id. A step that fails with
  no signatures stays `CI-<step>`.
- **Display limits.** The brief and comments list at most 20 test findings
  per step, then "+N more"; the ledger keeps all of them.

**D2 — CI triage order: re-run once, then compare with the base.**
1. When review CI fails, Forge re-runs the failing steps once on the
   candidate, in the same workspace (earlier steps' effects are still on
   disk).
2. A signature that does not fail in both runs is `flaky`. A step that passes
   on the re-run is wholly flaky.
3. For the remaining consistent failures, Forge runs a base check on the base
   commit. The base is the merge base with the target branch. The run must be
   comparable to the candidate run:
   - **Where it runs.** On the same owner as the candidate run, with the same
     Project env, assets and per-run build environment.
   - **Checkout.** `ExactCommit` is a fresh `git clone --shared
     --no-checkout` in the build area, so it has no installed dependencies.
   - **Commands.** The effective review config's `setup_steps` (for example
     `pnpm install --frozen-lockfile`), then the CI steps up to the last
     failing one.
   - **No `setup_steps`.** Forge does not guess. The base comparison is
     `base_unavailable`, and the brief and attention say "set `setup_steps`
     to enable base comparison".
   - **Policy.** `canonical-ci/1` on server owners.
   - **Daemon placements.** These run only `legacy-daemon/1`, so they get the
     re-run step only, and the base comparison is `base_unavailable` in
     version 1.
4. That base run has its own identity. It is reused by every Task and attempt
   on the same base commit, setup steps and CI steps. Completed red evidence is
   shared only for base comparison, never as a certified candidate/head pass.
5. **An invalid base run classifies nothing.** The base run is
   `base_run_invalid` when either of these holds:
   - its setup steps fail;
   - it fails with no signatures where the candidate had some.

   Every consistent failure then blocks as under `report`, and a
   `base_check_failing` attention item names the invalid run, so a broken
   base environment never passes failures through.
6. Consistent signatures that also fail on a valid base run are
   `pre_existing`. The rest are `introduced`.
7. **Without signatures:**
   - a step that passes on the re-run is flaky;
   - a step that fails on both the candidate and the base is
     `pre_existing_unverified`. It still blocks, because new failures may hide
     inside it, but the brief and attention say the base fails too.
8. **At the merge-queue head**, the same triage runs against the target tip,
   on that check's owner.

**D3 — Routing after triage, by `review_ci_triage`.** The Project setting
`review_ci_triage` takes three values:

| Value | Re-run and base run | What blocks |
|---|---|---|
| `full` (default) | yes | only `introduced` and `pre_existing_unverified` |
| `report` | yes | everything, as today; the triage is still recorded and reported |
| `off` | no | everything, as today |

- Under `full`, candidate CI collects all configured steps so waiving an early
  failure cannot omit later validation. `report` and `off` retain stop-at-first-failure.
- **Under `full`, with no blocking failures left**, the step counts as passed
  for this review. The Review details keep the triage, and review continues
  to verify and the reviewer. At the queue head, the head is not sent back.
- **Triage never certifies a failure.** The stored `check_result` keeps its
  real outcome (`fail`). Only the Review settlement, or the queue-head
  settle, treats the step as non-blocking. A triaged step therefore never
  becomes a reusable certified pass. The unchanged-commit reuse path cannot
  skip head CI because of it.
- **Budget:** triage uses the volatile `ReviewCiTriage` budget, limit 1 per
  review attempt or queue-head check.
- **Attention.** Flaky and pre-existing failures raise Project attention, deduped:
  - `flaky_check` is keyed by the step command and signature, with a count
    and the Tasks that hit it;
  - `base_check_failing` is keyed by step and base commit, and resolves when a
    later base run of that step passes.

**D4 — No-change gate.**
- When a coder repair round ends and the commit about to enter review equals
  the commit judged by the latest settled Review in the window, and that Review failed
  (with a clean worktree), Forge starts no CI, verify or reviewer, and creates
  no Review row.
- **First time:** it sends the Task back to the coder with a brief headed
  "No change since review N". This is charged to the persistent
  `RepairNoChange` budget (Review window, limit 1).
- **Second time:** it parks with `review_needs_owner` and the message
  `no change after repair round`.
- A later passing Review of the same commit means no reminder is due.
- Owner-initiated re-reviews and explicit owner Retry of a blocked entry are exempt. Only passed/failed Reviews participate in the latest settled Review comparison.
- Entries already past the guard when it ships continue: entry checkpoints
  without a guard decision record `Continue`. A recorded `awaited_consumer_id`
  is no proof (an exit hook's wait keeps it too); a suspended entry hook has
  its own open checkpoint.
- Automatic same-state retries after an offline entry carry the original source state and
  actor in the hooks payload, so reconnect still evaluates a repair entry as a
  repair and retains owner exemptions. An explicit owner Retry is exempt even
  when that original actor was the coder. A reminder clears the resolved entry's
  infrastructure blocker before returning to the coder. No schema migration is
  needed.

**D5 — Reviewer anchoring.** Finding fields:
- `requirement_ids`: ids of the frozen contract's requirements. Unknown ids
  are dropped.
- `kind`: `regression`, `missed` or `out_of_scope`. Absent is allowed.
- `same_cause_as`: an earlier finding id.

From the second review in a window, Forge applies these rules in the ledger,
never in the frozen assessment:
- A new (not continued) blocking finding stays blocking only if one of these
  holds:
  - its kind is `regression`;
  - it cites at least one requirement id;
  - its `where` path is in the Task's candidate diff. The path is the first
    repository-relative path in `where`, with any `:line` suffix removed. If
    `where` has no path, this clause is not met.

  Otherwise it is shown as `minor` with the note "outside requirements and
  Task files".
- `out_of_scope` findings never block. Their status is `out_of_scope`, and
  they are listed as follow-up candidates.

A reviewer `fail` left with no blocking finding after anchoring parks the
Task with `review_needs_owner` and the message
`only non-blocking findings remain`. Forge does not override the verdict
itself. The existing `defer_to_follow_up` and `mark_reviewed` actions let the
owner pass it in one step.

`RESPONSE_INSTRUCTION` gains these instructions:
- list the frozen requirement ids;
- after the first review, raise new blocking findings only for a regression,
  an unmet requirement (cite it) or the Task's changed files;
- mark findings that share a root cause with `same_cause_as`.

**D6 — `reopened`.**
- A finding resolved earlier in the window that is continued again (by id)
  has status `reopened`. For a Forge-owned finding, that means the same
  signature fails again.
- It counts as open, starts a new streak, and is shown in the brief as
  "re-broken: resolved in round N".

**D7 — The repair brief converges.** The brief from
`add-structured-repair-and-verify` changes in three ways:
- **Attempts** replaces the "Previous round" section. For each of the last
  three rounds it gives:
  - the commit range;
  - files changed (at most 20, from `git diff --stat` between the reviewed
    commits);
  - the coder's completion summary, up to 1 KiB;
  - what the round resolved, left open or opened.

  It closes with: "Do not repeat an approach that left a finding open. If
  you keep one, say why it failed before."
- **Requirement status** is one line per frozen requirement:
  - `unmet` if an open blocking finding cites it;
  - `addressed` if only resolved findings cite it;
  - otherwise `no open finding`.
- **Pattern** groups each `same_cause_as` family with its locations and says:
  "These share a cause. Fix it at every call site and list the sites you
  changed."

The drop order adds Attempts after the reviewer report, keeping the latest
round longest. Requirement status and Pattern are never dropped.

**D8 — Change strategy when a finding is carried.** A repair round that
contains a carried reviewer finding (streak 2, the round D5 of the first
change grants on progress) SHALL:
- start a fresh coder execution instead of resuming the thread, with the full
  brief;
- open the brief with "Before editing, write a short root-cause note for each
  carried finding" (recorded as a `decision` worklog entry; not enforced).

With the Project setting `repair_switch_model_when_stuck: true` (default
`false`), that round runs on the coder Agent's next fallback candidate when
one exists.

**D9 — Effect on the round policy.** The open count used for progress and
repeat parks in `add-structured-repair-and-verify` D5 excludes `flaky`,
`pre_existing`, `minor` and `out_of_scope` findings. Under `full`, it also
excludes `pre_existing_unverified` from repeat parks: a base problem is never
a coder repeat. Per-test findings make the count fine-grained.

## Risks / Trade-offs

- **Cost of triage.** Up to two extra runs of the failing steps per failed
  review: one re-run and one base run. The base run is shared per base commit.
  Triage only happens on failure, `report` and `off` limit it, and each extra
  run takes a machine slot like any check.
- **A pre-existing test failing for a new reason** is classed
  `pre_existing`. Accepted: the Task is no worse than the base, and the
  attention item keeps the base failure visible.
- **Signature extraction is heuristic.** Unknown runners fall back to
  step-level findings, which is today's behaviour. JUnit gives an exact path
  for any runner.
- **Anchoring may hide a real defect the reviewer missed in round 1.** It is
  still shown, marked `minor`. The owner sees it in the comment and can
  re-review with guidance. A reviewer that cites the requirement keeps it
  blocking.
- **A fresh session loses the agent's private context.** The Attempts section
  replaces it. This is the trade the owner's 10/6 stuck rule asks for:
  different eyes on a stuck problem.

## Migration Plan

- New optional fields only. There are no schema migrations, unless
  `task_budget.kind` is constrained, in which case it is widened for
  `review_ci_triage` and `repair_no_change`.
- Existing Projects get `review_ci_triage: full`. This is the visible behaviour
  change, listed under CHANGELOG `### Changed` with the `off` opt-out.
- Older daemons keep working without signatures.

## Open Questions

All resolved. The owner accepted the recommendations on 2026-10-14.

- **Q1.** `review_ci_triage` defaults to `full`: failures that are flaky or
  already on the base do not block. The base comparison only works for
  Projects whose review config has `setup_steps` that install dependencies.
  `world` needs `pnpm install --frozen-lockfile` there.
- **Q2.** A reviewer `fail` with nothing blocking left after anchoring parks
  for the owner. Forge does not pass it automatically.
- **Q3.** `repair_switch_model_when_stuck` is opt-in (default `false`).
- **Q4.** Triage also runs at the merge-queue head.
