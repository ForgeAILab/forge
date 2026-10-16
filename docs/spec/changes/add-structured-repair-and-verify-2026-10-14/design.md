## Context

Munk AI runs one durable loop per work item: Plan → Code → Gates → Deploy →
Verify → Delivery. Gates and Verify can fail into a Repair step, and a
max-round policy lives in the workflow. Its open-source verifier, Munk Test,
adds four ideas:
- reviews that emit cases which can be run;
- a judge kept separate from the runner, with the verdicts `passed`, `failed`
  and `inconclusive`;
- `retry_with_context` that carries extra context into the next attempt;
- learned knowledge that a human approves.

Forge against that loop today:

| Munk step | Forge today | This change |
|---|---|---|
| Plan | `planning` | — |
| Code | `in_progress` (autonomous: `working`) | — |
| Gates | `review`: `run_ci_steps` hook, then the reviewer agent | Findings from each gate |
| Deploy | Nothing per Task | `verify.start` + readiness |
| Verify | Nothing per Task; milestone checks are integration-level | `verify.checks` before the reviewer |
| Repair | Re-entry to the reject target with `reason + report`; 1 round by default | Repair brief, finding ledger, progress rounds |
| Delivery | `merging` queue | — (Forge merges; Munk stops at a draft PR) |

Facts this design depends on:

- **Frozen assessments.**
  - `execution_review_assessment.conformance_json` is immutable (V132
    triggers).
  - `record_review_conformance` compares serialized strings.
  - The Review's copy in `step_results_json.conformance` must equal the frozen
    row (integration guard).
- **Coder feedback today.**
  - It is `conformance_feedback` = `"{reason}\n\n{report}"`, cut at 12,000
    characters (`loader.rs`).
  - CI feedback uses the first failing step only (`ci_failure_reason`).
- **Merge-time check failures.** One records `FailureKind::CiFailed` in
  `send_back.rs`. Then `merge_fix_user` falls to the generic "manual repair
  required" branch with no check detail.
- **Budgets.**
  - The default review gate has `max_rejections: 2` (2 failed verdicts, 1
    repair round).
  - The autonomous workflow allows 3 rejections and its reject target is
    `working`.
  - `MergeFix` is 1. `ConflictHandoff` is 5.
- **Durable check runner.**
  - Outcomes are `pass | fail | timed_out | cancelled | infrastructure_failed`.
  - It keeps a 4096-byte tail per step.
  - Under `canonical-ci/1`, a step's service stays up for later steps. Every
    process group is stopped and verified when the run ends.
  - The legacy policies (`legacy-server/1`, `legacy-daemon/1`) never stop a
    step's descendants.
  - Daemon placements run only `legacy-daemon/1`.
- **Ports and evidence.**
  - Nothing in Forge allocates ports.
  - `CheckCommandOutcome` has no asset field.
  - Task evidence (`task_media`) is linked to Tasks, not to Reviews.
- **Review comments today.** `reviewer_comment` (`cascade.rs`) posts the
  headline plus the full report, cut at 32 KiB.

## Goals / Non-Goals

Goals:
- The coder repairs from a complete, bounded brief that says what is open,
  what it already fixed, and what each check printed.
- The coder keeps getting rounds while it makes measurable progress. It stops
  quickly when it does not.
- A Task's candidate can be started and exercised before a reviewer is paid
  for, with its evidence kept.
- Comments read as a checklist per round.
- Old reviewers, old stored rows and Projects without `verify` behave as
  today.

Non-Goals (v1):
- Per-finding `fixable_by` and partial routing (some findings to the coder,
  others to the owner).
- Verify at the merge-queue head (`queue_head_ci`).
- Verify on daemon placements. This needs a daemon policy that stops the run
  tree, which means a protocol bump.
- Check self-repair: Munk's optimize agent, which rewrites a check's hints
  after a flaky verdict.
- Munk Test as a built-in. It works today through a wrapper script; see the
  appendix.
- Learned review knowledge recalled by changed file. That belongs to the
  `add-task-memory-recall` work.

## Decisions

**D1 — Findings live in the reviewer's frozen assessment; their status is
derived.**
- `ReviewAssessment` gains `findings: Vec<ReviewFinding>` and
  `resolved: Vec<String>`.
- Both use `#[serde(default, skip_serializing_if = "Vec::is_empty")]`, so
  every existing row re-serializes byte-for-byte.
- Forge-computed state (ids for new findings, open/resolved status, age)
  never goes into the frozen row.
- A pure function, `db::review_findings::finding_ledger(reviews_in_window)`, computes it from the
  Task's ordered Reviews in the current review-budget window. The same window
  resets the review budget at a recovery boundary.
- Routing, the repair brief, comments and `GET /tasks/{id}/findings` all call
  that one function.
- Alternative considered: a `finding` table. Rejected because it is a second
  source of truth next to immutable rows, and it needs migration and backfill
  for little gain.

**D2 — Forge numbers reviewer findings.**
- The reviewer writes findings without ids.
- On a re-review it may copy a prior id onto a finding that is still open, and
  it lists fixed ids in `resolved`.
- Forge gives a new reviewer finding the next free `F<n>` in the window.
  `n` follows the finding's position in the list.
- Unknown ids, and ids reused for a finding Forge does not know, are treated
  as new findings.
- Forge-owned findings get stable ids from their source:
  - `CI-<step index>#<first 8 hex of sha256(normalised signature)>` for each
    failing review CI test, or `CI-<step index>` when signatures are absent
    (the signature refinement from `add-repair-convergence-guards` D1);
  - `CHK-<check id>` for required conformance checks;
  - `V-<check name>` for verify checks, plus `V-start` and `V-ready`.

  The same source failing again carries the same id.
- Merge failures produce no Review row: a conflict handoff and a merge-time
  `CiFailed` live in send-back metadata and the transition log. They are
  therefore not in the ledger. The repair brief reads them directly and shows
  them as `MERGE-CONFLICT` and `MERGE-CI` entries in its open-findings
  section.

**D3 — Every finding has a source, and only a source that ran can change it.**

A finding is *checked* in review `n` when its source ran in `n`:
- the reviewer, for `F*`;
- that CI step, required check or verify check, for the Forge-owned ids.

When a source did not run, its findings keep their last status unchanged. For
example, the reviewer is never dispatched after review CI fails.

A prior `F*` finding that a reviewer who did run neither keeps nor resolves is
`unconfirmed`:
- It is not counted as open, so a forgetful reviewer cannot manufacture
  repeats.
- The comment and the brief show it as "not re-checked".
- A later reviewer may name its id again, which reopens it with a fresh
  streak.

Forge-owned findings never become `unconfirmed`. When their source runs, it
either passes (`resolved`) or fails (`open`).

**D4 — Severity.**
- `blocking` (the default) or `minor`.
- A `fail` with no blocking finding is still a fail, and the reviewer's
  verdict wins. The ledger then has nothing to measure progress against, so
  the progress rules do not apply.
- A `pass` may carry `minor` findings. They are shown and never routed.

**D5 — Round policy.** Definitions, for a coder-fixable failed review `n` in
the window:
- `open(n)` is every blocking finding open after review `n`. That includes
  findings whose source did not run in `n`.
- `streak(f)` is the number of consecutive checks of `f`'s source that
  reported `f` open.
- `carried(n)` is the set of reviewer findings (`F*`) checked in `n` with
  `streak >= 2`.

Forge-owned findings (`CI-*`, `CHK-*`, `V-*`) count in `open(n)` for
progress, but never trigger the repeat parks in rules 1–2. This matches
today's routing, where Forge's own check failures follow the review budget
and are never owner findings. A check that keeps failing therefore exhausts
the budget, as it does today.

The rules, in order:

1. A reviewer finding with `streak >= 3` survived two repair rounds. The Task
   parks with `review_needs_owner` and the message
   `repeated finding: <id> <summary>`.
2. If `carried(n)` is not empty and `|open(n)| >= |open(n-1)|` (no progress),
   the Task parks the same way. This is today's repeat rule, derived instead
   of self-reported.
3. Otherwise, if the review budget allows a retry, it is a normal repair
   round. The review budget is charged.
4. Otherwise, if `|open(n)| < |open(n-1)|` and the `review_progress` budget
   allows a retry, it is a progress round. The `review_progress` budget is
   charged.
5. Otherwise the review budget is exhausted, as today: automatic recovery,
   then `review_budget_exhausted`.

- **Before this policy:** owner parks (`fixable_by: owner`) and `blocked`
  keep running first, unchanged.
- **When the rules are off:**
  - Rules 1, 2 and 4 are off for the rest of the window once any reviewer
    `fail` in it has no blocking finding (no list, or only `minor` items).
  - With those rules off, the self-reported `repeat` flag keeps today's
    behaviour. With them on, the flag is ignored.
- **Coordination roots:** roots with a lead keep `charge_replan` and are
  outside this policy.

Worked example under the default gate (review budget 2):
- Review 1 finds F1, F2 and F3. Rule 3 gives a repair round; review budget
  1/2 spent.
- Review 2 finds F1 still open; F2 and F3 are resolved. F1 has streak 2, so it
  is carried, but there is progress (1 < 3). The review budget is now 2/2
  spent, so rule 3 cannot apply. Rule 4 gives a progress round
  (`review_progress` 1/2).
- Review 3 finds F1 still open. Its streak is 3, so rule 1 parks the Task with
  `repeated finding: F1 ...`.

Today the same Task parks at review 2 with `review_budget_exhausted`.

**D6 — `review_progress` is a new budget `Kind`.**
- Key `review_progress`, default limit 2, `0` disables it.
- It is resolved the same way as `Review` (Task-wide `retry_budgets`, then
  state config). It is never resolved from gate `max_rejections`.
- It uses the Review origin window and charges with a receipt keyed
  `progress:<review id>`.
- Owner actions never spend it.

**D7 — One repair brief builder.**
- `RepairBrief` is assembled in `workflow/dispatch` and used by the review-fix
  and merge-fix prompt builders and by automatic review recovery. The
  builders keep their IDs; only the user-message body changes.
- Fixed section order:
  1. Task
  2. Round
  3. Open findings
  4. Resolved and unconfirmed since the last round
  5. Failing check output
  6. Verify results and evidence
  7. Reviewer report
  8. Previous round
  9. Logs
- Byte budget: 24 KiB for the brief body. When over budget, sections are cut
  in this order until it fits:
  1. reviewer report, down to 2 KiB, then dropped;
  2. previous round, dropped;
  3. check tails, each down to 1 KiB;
  4. resolved and unconfirmed list, ids only.

  Task and open findings are never dropped. The cap is hard (owner decision,
  2026-10-16): if the brief is still over, check tails share 4 KiB and verify
  keeps 2 KiB; then each finding's summary is cut to 200 characters, `where`
  to 120, `expected` and `observed` to 300, and past a line budget the
  remaining open findings are listed by id. Description and plan each keep
  4 KiB including a truncation note; compacted updates share 1 KiB across
  steps. The final 24 KiB cap includes its notice even for an exceptional
  window whose ids alone exceed the message capacity.

**D8 — Verify runs as its own check request after review CI.**
- New `CheckPurpose::ReviewVerify`, origin `entry`.
- Commands are `Uncacheable`, so verify never makes the CI bundle
  non-reusable, and runtime behaviour is never cached.
- Ordering inside the review entry hook: CI steps pass → verify → reviewer
  dispatch.
- A verify failure settles the Review as failed before any reviewer
  execution, through `review_failure_target` and D5.
- Verify gets its own wall limit (`verify.timeout_seconds`, default 1200,
  maximum 3600). It does not share `ENTRY_CHECK_WALL_SECONDS` with CI.
- The policy is `canonical-ci/1`, the only policy that stops and verifies the
  run tree. Every verify command is `Uncacheable`, so its result is never
  reused. The policy itself is unchanged; any change to it would be a new
  revision.
- A daemon-placed Task gets `verify.status = not_run`,
  `reason = placement_unsupported`, and the review continues to the reviewer.
- A review re-entered after an earlier pass is unchanged. A failure there
  blocks the Task as `merge-fix follow-up failed: ci`
  (`actions/review.rs:537`, `:1339`), and a verify failure on that path
  blocks the same way.

**D9 — Forge drives start and readiness; the user's command drives
judgement.**
- Forge generates the bundle:
  1. A step that starts `verify.start` in the background, with output going to
     `$FORGE_VERIFY_DIR/app.log`.
  2. A readiness step. It polls `$FORGE_VERIFY_URL` + `verify.ready_path` (any
     HTTP status below 500), or runs `verify.ready_command` until it exits 0.
     It gives up after `ready_timeout_seconds`.
  3. One step per `verify.checks[]`.
- Environment variables:

  | Variable | Value |
  |---|---|
  | `FORGE_VERIFY_PORT` | a free loopback port picked when the run starts |
  | `FORGE_VERIFY_URL` | `http://127.0.0.1:$FORGE_VERIFY_PORT` |
  | `FORGE_VERIFY_DIR` | `<task root>/.forge-task/verify/<review attempt>/` |
  | `FORGE_VERIFY_REQUEST` | `$FORGE_VERIFY_DIR/request.json` |
  | `FORGE_VERIFY_REPORT` | `$FORGE_VERIFY_DIR/<check name>.report.json` |

- `request.json` contains:
  - `task_id`, `title`, `description`;
  - the Task's requirement texts;
  - `base_sha`, `commit_sha`, `changed_files`;
  - the open findings from the ledger, so a verifier can target regressions.

**D10 — Verdict mapping.** A verify check's result is:
- **fail** if the command exits non-zero, times out, or its report says
  `fail`/`failed`. The finding is `V-<name>`. Its `observed` is the report's
  summary or findings when present, otherwise the output tail.
- **pass** if it exits 0 and its report is absent or says `pass`/`passed`.
- **inconclusive** if it exits 0 and the report says `inconclusive`.

Start and readiness failures are fails with `V-start` / `V-ready`. Their
`observed` is the tail of `app.log`. The app built from this candidate did not
come up, and the coder owns that.

An inconclusive result re-runs verify once automatically. The new volatile
budget `ReviewVerifyRerun` has limit 1. A second inconclusive parks the Task
with `review_blocked` (source `verify`). The offered actions are `retry`
(re-run verify), `mark_reviewed` (reason required) and `cancel_task`. The
review budget is not charged.

An infrastructure failure of the runner keeps the existing
`review_ci_infrastructure` handling.

**D11 — Evidence.**
- The report's `evidence[]` entries (`path` relative to `FORGE_VERIFY_DIR`,
  `kind`, `caption`) are captured through the `task.evidence` storage path.
  The same limits apply: 25 MiB per file, kinds
  `screenshot | walkthrough_video | log | report | other`.
- Their media ids are written to the Review's `step_results_json.verify`
  object, which is a new allowed detail key:

  ```json
  {"status":"fail","checks":[{"name":"smoke","result":"fail","summary":"...","evidence":["<media id>"]}],"app_log_tail":"..."}
  ```
- The reviewer prompt gets a "Verify results" section: verdicts, summaries,
  and evidence file paths inside the Task root, so a multimodal reviewer can
  open the screenshots.

**D12 — Comments.**
- `reviewer_comment` renders the following:
  - a headline, as today;
  - a GFM task list of the ledger for this round;
  - for verify, one line with the verdict and evidence links;
  - "Full review: Review tab, attempt N".

  The report body is no longer posted.
- When a review has no findings list, the comment keeps the reason and the
  first 2 KiB of the report.
- A review that fails at review CI or verify, before any reviewer runs, also
  gets this comment, listing its `CI-*` / `V-*` findings. This is new: today
  a review CI failure posts no comment.
- Forge adds a system comment when it dispatches a repair round, and one when
  it parks over findings (rules D5.1 and D5.2).
- Example:

  ```md
  Review failed (attempt 2): parser still crashes on empty input
  - [ ] F1 Empty input crashes `parse()` — open since attempt 1
  - [ ] F4 New: `--json` flag ignored — `src/cli.rs:88`
  - [x] F2 Missing test for unicode names — resolved
  - [x] CI-1 `cargo test` — passing now
  - F3 Error message wording — not re-checked
  Verify: fail — V-smoke "empty form submits" (2 screenshots)
  Full review: Review tab, attempt 2
  ```

  ```md
  Repair round 2 (progress round 1 of 2): F1, F4 open; F2, CI-1 resolved since round 1.
  ```

## Risks / Trade-offs

- **Reviewers may be sloppy with ids.** D2 and D3 keep this safe: unknown ids
  become new findings, and omissions become `unconfirmed`, never `open`. The
  cost is that a reviewer who drops an id silently loses a repeat signal. The
  verdict still fails and the budget still applies.
- **Progress rounds cost money.** They are bounded by `review_progress`
  (default 2) and only granted on strict shrink, so a Task can get at most 3
  coder repair rounds under the default gate.
- **Verify is flaky by nature.** It is never cached. An inconclusive result
  re-runs once and then parks. A flaky `fail` reaches the coder, and D5's
  no-progress rule parks it after one repeat.
- **Ports race.** Forge picks a free port at run start, and another process
  can take it before the app binds. A bind failure shows up as `V-start`,
  then the repeat rule parks it. If that happens in practice, the
  implementation may retry the start step once with a new port.
- **Comment change.** Anyone reading full reports from comments must use the
  Review API or the Review tab instead. This goes in the CHANGELOG under
  `Changed`.
- **Coupling to `canonical-ci/1`.** Verify relies on its run-tree stop. A
  future policy revision must keep the stop for `ReviewVerify` bundles.

## Migration Plan

- **No schema migration for findings.** The new fields are optional JSON
  inside existing columns, and old rows re-serialize identically. A test pins
  that a V145-era stored assessment round-trips byte-for-byte.
- **Budget kinds.** If `task_budget.kind` has a CHECK constraint, add a
  numbered migration that widens it for `review_progress` and
  `review_verify_rerun`. Otherwise no migration is needed.
- **`ReviewConfig.verify`** is optional. Projects without it are unchanged.
- **Rollout.** No feature flag. The changes ship together in the next `0.14.x`
  with the CHANGELOG entries listed in the proposal.

## Open Questions

All resolved.

- **Q1 — Daemon support.** Decided by the owner on 2026-10-14: verify on
  daemon placements is a follow-up change. Version 1 records `not_run` /
  `placement_unsupported` there, as D8 says.
- **Q2 — Default `review_progress`.** Decided by the owner on 2026-10-14: 2.
- **Q3 — A repair-round field in the coder's reply.** Resolved by
  `add-repair-convergence-guards-2026-10-14`: Forge derives attempt memory
  itself (D7 there), so the coder does not declare which ids it addressed.

That follow-up change also adds the following on top of this one:
- per-test check findings;
- flaky and base triage for CI;
- a no-change gate;
- reviewer anchoring (`requirement_ids`, `kind`, `same_cause_as`);
- a fresh session or model change for a carried finding.

## Appendix — Munk Test as a verify check

```bash
# scripts/forge-verify-munk.sh (Project-owned; not shipped by Forge)
jq '{app_id:"web", change_summary:.title, changed_files:.changed_files,
     app_target:{app_id:"web", platform:"web",
       web:{base_url:env.FORGE_VERIFY_URL, origin:env.FORGE_VERIFY_URL,
            launch_context:{browser:"chromium", headless:"true"}}},
     provided_cases:[], enable_plan_agent:true}' "$FORGE_VERIFY_REQUEST" > "$FORGE_VERIFY_DIR/munk-request.json"
munk verify change --request-file "$FORGE_VERIFY_DIR/munk-request.json" --config munk.yaml
# convert .munk/runs/<latest>/report.json → $FORGE_VERIFY_REPORT (verdict + screenshots)
```

## Phase 4 implementation decisions (2026-10-16)

- The pure ledger lives in `db::review_findings`, below both transactional
  budget charging and reviewer/services callers, avoiding a dependency cycle.
- `failures_truncated=false` describes extraction completeness, not command
  coverage. Existing failure outcomes cannot prove all test binaries ran, so
  only a passing CI step resolves omitted signatures. A failed step leaves
  omitted findings and their streaks unchanged. Replaced step-level findings
  become `superseded`, never progress credit.
- Task description and plan each keep 4 KiB, including a truncation note.
  Compacted updates share 1 KiB across steps; the 24 KiB cap is unconditional.
  Exceptionally large id-only windows can exceed a message's capacity; the
  final note directs the reader to the complete ledger.
- Merge-CI messages retain 20 signatures (`+N more`) and a 2 KiB output end.
- Review-entry merge-base observations can open/update base Attention, but
  only newer queue-head target-tip passing observations can resolve it.
- Project `retry_budgets.review_progress` supplies the Review state default;
  explicit state values and Task-wide overrides win. The later anchoring,
  reopened, and strategy-switch phases remain unimplemented.

- The hook's merged configuration is an optional trailing field on the typed
  review-routing command. Three-field queued requests decode with no supplied
  configuration; new requests carry the hook configuration when present.
