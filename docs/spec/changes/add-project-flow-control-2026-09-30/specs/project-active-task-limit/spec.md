## ADDED Requirements

### Requirement: Per-Project active task limit
Each Project SHALL have `settings.max_active_tasks`: an integer, default 5, where `0` means unlimited. A Task SHALL hold a slot when its status is a workflow state of kind `active` or `gate` and it is not parked. In the default workflow those states are `planning`, `in_progress`, `review`, `merging` and `merge_failed`. A Task is parked when it carries a blocking annotation or its current Review is `awaiting_human`. A coordination root whose subtasks run SHALL NOT hold a slot itself; its subtasks do.

#### Scenario: Review and conflict repair take slots
- **WHEN** a Project with limit 5 has 2 Tasks `in_progress`, 2 in `review` and 1 in `merge_failed`, none parked
- **THEN** the Project reports 5 of 5 slots in use

#### Scenario: Parked review frees its slot
- **WHEN** a Task in `review` is parked with `review_needs_owner`
- **THEN** it no longer counts toward the limit

### Requirement: Limit gates only new admission
The dispatcher SHALL admit a Task out of the workflow's initial state only while the number of slot-holding Tasks is below the limit. Admission SHALL follow the existing ready-queue order. Recovery, re-dispatch, un-parking, and transitions of Tasks already past the initial state SHALL never be refused for capacity, even when that temporarily exceeds the limit. The agent's `max_concurrent_tasks` SHALL continue to apply on top of the project limit.

#### Scenario: Full Project queues new work
- **WHEN** 5 of 5 slots are in use and NK-50 is `todo` and ready
- **THEN** NK-50 is not dispatched and stays `todo`

#### Scenario: Unparked Task resumes over the limit
- **WHEN** 5 of 5 slots are in use and the owner re-executes a parked Task
- **THEN** that Task dispatches and the Project reports 6 of 5 in use, and no new `todo` is admitted until usage drops below 5

### Requirement: Parked-task guard
The dispatcher SHALL also stop admitting new Tasks while the number of parked Tasks, as defined above, in slot-eligible states is at least twice the limit. This keeps a pile of parked work from growing worktrees and disk without bound. The guard does not apply when the limit is `0`.

#### Scenario: Too much waiting on the owner
- **WHEN** a Project with limit 5 has 10 parked Tasks and 2 active Tasks
- **THEN** no `todo` Task is admitted, and the Project shows "waiting on you: 10 parked"

### Requirement: Queueing is visible
The Project response SHALL include `slots: {limit, active, parked, queued}`. A ready `todo` Task held back by the limit or by the parked guard SHALL record a dispatch disposition, `project_at_capacity` or `project_waiting_on_owner`, and the UI SHALL show it on the task card and task detail. The limit SHALL be editable in Project settings and through `PATCH /api/v1/projects/{id}`.

#### Scenario: Owner sees why a Task waits
- **WHEN** NK-50 is held back because the Project is full
- **THEN** its card shows "Waiting for a slot (5/5 active)"
