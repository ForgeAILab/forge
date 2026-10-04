## Context

Evidence is from the NovelKit project database on 10.0.0.2, at 2026-09-30:

- Transitions: `review→merging` 142, `merging→merge_failed` 106 (85 of them
  conflict handoffs), `merge_failed→review` 104, `merging→done` 37. Most
  "stuck in review" time was the conflict loop. v0.13.10's review-authority
  carry already fixes the re-review half: NK-28 went conflict-fix → CI →
  merged with no re-review. What is left is *how many* Tasks are in flight
  against the same hub files, and this change addresses that.
- 41 failed review verdicts. Most are real code defects (NK-28 stale apply,
  NK-35 migrations, and similar), and the loop handled those correctly. The
  rest were findings the coder cannot fix: NK-48 (Forge links, 3/3 attempts),
  NK-24/37/38 (three-OS measurements), NK-5 (live LLM endpoint), NK-1 (host
  packages, some already reported as `blocked`).
- The Tasks blocked by `environment_not_ready` stayed stranded for about
  14h after the disk recovered.

## Goals / Non-Goals

- Goals: self-healing environment pauses, bounded work in flight per Project,
  and no retry budget spent on findings only the owner can resolve.
- Non-Goals: a merge queue or rebase-before-review (a separate change, if the
  limit alone is not enough); remote-daemon environment checks, which stay
  local-only as today; auto-classifying findings without reviewer input.

## Decisions

- **Pause the Project, not the Task, on environment failure.** An environment
  failure is a property of the host, not of one Task, so every Task would fail
  the same way. This reuses `paused_at` + `system_pause_reason` and the
  compare-and-clear guard already proven by `repo_pause_sync`.
  `environment_pause_sync` runs in the same place in `check_once`, before the
  paused-project skip, and owns only the `environment_not_ready` reason.
  - Alternative: keep Task blocks and add an auto-`reexecute` sweep.
    Rejected: N Tasks each discover the same failure, and each block needs its
    own recovery.
- **Where re-checks run.** They run in the primary checkout, with the Project
  `env` and without assets. Checks that depend on worktree assets may pass
  there and fail at launch. That is cheap, because the launch re-pauses. Only
  the recorded failing checks re-run, so role-scoped checks (for example a
  reviewer-only browser check) are covered.
- **Pause detail storage.** A new nullable `project.environment_pause_json`
  column (V202610010410). An in-memory record is not enough: the next check time and
  the output must survive a restart and be visible over the API.
- **Pre-launch failure must not block re-dispatch.** The failed pre-dispatch
  execution is tagged so `latest_stopped_execution_blocks_dispatch` skips it.
  Otherwise the Task would stay stranded after the resume.
- **Slots = `active` + `gate` state kinds, minus parked.** This follows the
  workflow definition rather than hard-coded state names, so custom workflows
  get sensible behavior. `planning` with no planner passes through instantly,
  so it costs nothing.
- **The limit gates admission only.** Refusing recovery of admitted work would
  strand worktrees, and finishing work beats starting it.
- **Parked guard = 2 × limit, fixed.** It is not configurable in this change,
  to keep the settings surface small.
- **Routing is declared by the reviewer.** Forge cannot tell "needs macOS" from
  "wrong null check" without the model. The reviewer already sees prior
  reviews in its context, so it can say `repeat`. Forge still requires the
  previous attempt to have failed before honoring `repeat`, so one flag can
  never park a first attempt.
- **`defer_to_follow_up` is a new recovery action** instead of guidance text,
  so NK-XOS-style human follow-ups become one click and stay linked.

## Risks / Trade-offs

- Reviewers may over-use `fixable_by: owner` to avoid hard calls. Mitigation:
  the prompt defines narrow categories, parks are visible, and the owner can
  `reexecute` with guidance.
- A default limit of 5 reduces throughput on projects that were fine with
  more. This is noted as Breaking; `max_active_tasks: 0` restores the old
  behavior.
- Executions that are already running continue during an environment pause
  and can keep consuming disk. That is acceptable, because each run is bounded
  and this change is about not starting new ones.

## Migration Plan

V202610010410:
- adds `project.environment_pause_json TEXT NULL`;
- clears `error_annotation` / `blocked_json` on Tasks whose annotation `type`
  is `environment_not_ready`.

No change to historical migrations. Settings defaults apply on read, so no
settings rewrite is needed.

## Open Questions

- None blocking. Revisit the fixed 2× guard after one NovelKit run.
