# Queued-action restore literals

`restore-main-cc39cac5.jsonl` contains eight results captured from archived main
`cc39cac5` by calling its production `restore_failed_task_action` under a claimed
Task step and rereading the stored `error_annotation`. The capture harness and
logs are retained in `31g1f2/` alongside the source audit and fixture hashes.

Each record contains the exact saved annotation (`input`), the new failure
`reason`, and the main-written annotation (`raw`). Inputs are the unchanged
`restore-{dispatch-state,placement,plan-wait,dispatch-upgrade}/{first,queue}`
annotations from the archived-main `writer-main-94609411.jsonl` corpus. Both
phases already carry a `blocking_reason`; `queue` also carries `task_step_id`
and `task_step_error`. The new reason differs from the saved reason in every
case, covering replacement as well as preservation of the step overlay.

Expected bytes were captured only from archived main. The branch tests assert
these literals and cannot regenerate them. Existing fixture records remain
unchanged.
