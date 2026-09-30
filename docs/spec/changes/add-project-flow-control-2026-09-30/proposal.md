---
created_at: 2026-09-30T20:30:00Z
updated_at: 2026-09-30T20:30:00Z
---

## Why

NovelKit (10.0.0.2, 9/29–9/30) showed three ways a Project stalls with nobody
told:

1. **Environment failures strand Tasks forever.** A Project environment check
   (`disk`: ≥8G free on `/`) failed at 7G around 05:30Z. Each Task that tried to
   launch was parked `environment_not_ready`. Disk recovered to 17G within hours,
   but nothing re-runs the check, so six Tasks sat blocked for ~14h until a
   manual `reexecute` on each.
2. **Unbounded work in flight.** The dispatcher admits new `todo` work whenever
   an agent has a free *execution* slot. Tasks in review, merging, or conflict
   repair hold no execution, so they don't count. About 10 concurrent tasks
   edited the same hub files, and there were 106 `merge_failed` against 37
   merges. Open worktrees and build dirs filled the 148G disk.
3. **Reviews loop on findings the coder cannot fix.** NK-48 failed review three
   times with the code "met" each time. Every time the blocking finding was
   "Forge `linked_documents` is empty", which the coder has no authority to
   change, and the review budget ran out. NK-24/37/38 (three-OS measurements),
   NK-5 (a live LLM endpoint) and NK-1 (host packages) followed the same
   pattern. Each attempt re-dispatched the coder and burned review budget.

## What Changes

- **Environment pause.** A failing Project environment check pauses the
  *Project* (`system_pause_reason = environment_not_ready`) and does not park
  the Task. Forge re-runs the failed checks every
  `settings.environment.recheck_interval_seconds` (default 600), and on a pass
  resumes the Project automatically. Its Tasks then re-dispatch where they
  were. The Project shows an "Environment paused" label with the failing check,
  its output tail, and the next check time. A "Check now" action re-runs the
  checks immediately.
- **Active task limit.** A per-Project `settings.max_active_tasks` (default
  **5**, `0` = unlimited) caps the Tasks that hold a slot. A Task holds a slot
  in any `active` or `gate` workflow state (default workflow: `planning`,
  `in_progress`, `review`, `merging`, `merge_failed`), unless it is parked
  (blocked or awaiting a human). The limit gates only *new* admission out of
  the initial state: already-admitted Tasks are never refused. Admission also
  stops while parked Tasks ≥ 2 × the limit. Queued Tasks show why they wait.
- **Review finding routing.** The reviewer result block gains two optional
  fields. `fixable_by: "coder" | "owner"` defaults to `coder`. `repeat: true`
  means the blocking finding was already raised on the previous attempt and is
  still unaddressed. A `fail` that is owner-fixable, or a coder-fixable repeat
  of a failed previous attempt, parks the Task for its owner as
  `review_needs_owner`. The coder is not dispatched and no review retry budget
  is spent. Owner actions: retry with guidance, mark reviewed (waive with
  reason), defer the finding to a follow-up Task, open interactive, cancel.
- **BREAKING:** existing Projects get `max_active_tasks = 5` and admit less
  concurrent work than before.
- **BREAKING:** `environment_not_ready` is no longer a Task blocking kind for
  new failures. A migration clears existing Task annotations of that kind, and
  the Project pause takes over.
- New `FailureKind::ReviewNeedsOwner` and `RecoveryAction::DeferToFollowUp`;
  new project fields in API responses; new `POST
  /api/v1/projects/{id}/environment/recheck`.

## Impact

- Affected specs: `project-environment-pause`, `project-active-task-limit`,
  `review-finding-routing`
- Affected code:
  - `crates/services/src/task_service/execution/environment.rs` — pause the Project instead of blocking the Task
  - `crates/services/src/task_dispatcher/` — new `environment_pause_sync` next to `repo_pause_sync`, and slot admission in `initial_scheduling`
  - `crates/services/src/task_service/execution/cascade.rs` — owner/repeat routing next to `block_task_for_review_environment`
  - review result parsing and the reviewer prompt
  - `crates/api-types` (`ProjectEnvironment`, `ProjectSettings`, `ReviewAssessment`, `FailureKind`, `RecoveryAction`, `Project` response)
  - `crates/db` migration V149
  - `crates/api/src/routes/projects.rs`, `docs/api.md`, `docs/architecture.md`, `CHANGELOG.md`
  - web project header/card, task queue reason, review tab
  - `forge-ctl`: `project env-recheck`
