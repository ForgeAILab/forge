---
created_at: 2026-09-30T20:30:00Z
updated_at: 2026-09-30T20:30:00Z
completed_at:
---

## 1. Environment pause
- [x] 1.1 V149 migration: `project.environment_pause_json`; clear `environment_not_ready` Task annotations
- [x] 1.2 `ProjectEnvironment.recheck_interval_seconds` (default 600, 60–86400) and validation in the project PATCH
- [x] 1.3 `prepare_execution_environment`: on failure, fail the execution with a pre-dispatch tag and pause the Project (never overwrite a user or repository pause); drop `block_task_for_environment`
- [x] 1.4 Make `latest_stopped_execution_blocks_dispatch` skip executions tagged as environment pre-dispatch failures
- [x] 1.5 `task_dispatcher/environment_pause_sync.rs`: scheduled re-check in the primary checkout, compare-and-clear resume, detail update, events
- [x] 1.6 `POST /api/v1/projects/{id}/environment/recheck` + `forge-ctl project env-recheck`; a manual resume also clears the environment pause
- [ ] 1.7 `environment_pause` on the Project response (api-types, generated TS, `docs/api.md`)
- [ ] 1.8 Web: Environment paused label on the card and header, with details, next check, and Check now
- [x] 1.9 Focused tests: pause on failure, no Task annotation, re-check pass resumes and Tasks re-dispatch, still failing reschedules, user pause untouched, migration clears legacy blocks

## 2. Active task limit
- [x] 2.1 `ProjectSettings.max_active_tasks` (default 5, 0 = unlimited) and validation
- [x] 2.2 Slot counting helper (active/gate kinds, minus parked, minus coordination roots with running subtasks)
- [x] 2.3 `dispatch_initial_tasks`: gate admission on the slot limit and the parked guard; record `project_at_capacity` / `project_waiting_on_owner` dispositions
- [ ] 2.4 `slots` on the Project response (api-types, TS, docs)
- [ ] 2.5 Web: slot usage in the project header, queue reason on task cards and detail
- [ ] 2.7 Web Project settings page: "Active task limit" (max_active_tasks, 0 = unlimited) and "Environment re-check interval" (recheck_interval_seconds) fields, saved via PATCH with inline validation errors
- [x] 2.6 Focused tests: full project queues, recovery over the limit allowed, parked frees a slot, parked guard, 0 = unlimited

## 3. Review finding routing
- [x] 3.1 `ReviewAssessment.fixable_by` / `repeat` with lenient parsing (defaults when absent or unknown)
- [x] 3.2 Reviewer prompt: define both fields, owner categories, examples
- [x] 3.3 `FailureKind::ReviewNeedsOwner`; cascade routing for an owner-fixable fail or a repeat after a failed attempt, with no coder dispatch and no budget spend
- [x] 3.4 `RecoveryAction::DeferToFollowUp`: atomic follow-up Task creation + linked relation + manual pass with reason
- [ ] 3.5 Web review tab: Needs owner panel, badges, actions
- [x] 3.6 Focused tests: owner park, repeat park, repeat ignored on attempt 1, legacy block routes as today, defer creates follow-up and passes review, budget unchanged

## 4. Release
- [ ] 4.1 `docs/architecture.md` (dispatcher pause/admission, state machine notes), `docs/api.md`, `docs/cli.md`
- [ ] 4.2 CHANGELOG `Unreleased` → `### Breaking` (default limit 5; environment failures pause the Project)
- [ ] 4.3 Relevant `happy_path` case(s) green
- [ ] 4.4 Live check on NovelKit: pause on low disk, auto-resume, slot cap holds at 5, a NK-48-style review parks after one attempt
