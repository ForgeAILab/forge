-- Public readers use only the typed condition. No Task/history/Attention data is dropped.
-- The migration runner rederives conditions in this transaction before recording this version.
DROP INDEX IF EXISTS idx_task_deferred_dispatch_project;
CREATE INDEX idx_task_condition_retry_project ON task(project_id)
WHERE json_extract(condition_json,'$.evidence.presentation.retry_recorded')=1 AND deleted_at IS NULL;

DROP TRIGGER IF EXISTS task_list_revision_task_update;
CREATE TRIGGER task_list_revision_task_update AFTER UPDATE OF project_id, parent_task_id, assignee_type, assignee_id, title, description, task_type, status, priority, board_position, subtask_order, condition_json, review_passed_at, archived_at, deleted_at, version, created_at, updated_at, task_state_config ON task
WHEN OLD.project_id IS NOT NEW.project_id
    OR OLD.parent_task_id IS NOT NEW.parent_task_id
    OR OLD.assignee_type IS NOT NEW.assignee_type
    OR OLD.assignee_id IS NOT NEW.assignee_id
    OR OLD.title IS NOT NEW.title
    OR OLD.description IS NOT NEW.description
    OR OLD.task_type IS NOT NEW.task_type
    OR OLD.status IS NOT NEW.status
    OR OLD.priority IS NOT NEW.priority
    OR OLD.board_position IS NOT NEW.board_position
    OR OLD.subtask_order IS NOT NEW.subtask_order
    OR OLD.condition_json IS NOT NEW.condition_json
    OR OLD.review_passed_at IS NOT NEW.review_passed_at
    OR OLD.archived_at IS NOT NEW.archived_at
    OR OLD.deleted_at IS NOT NEW.deleted_at
    OR OLD.version IS NOT NEW.version
    OR OLD.created_at IS NOT NEW.created_at
    OR OLD.updated_at IS NOT NEW.updated_at
    OR OLD.task_state_config IS NOT NEW.task_state_config
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (OLD.project_id, NEW.project_id);
END;

-- The bridge annotation was the visible half of an owner park. Its diagnosis
-- (message and blocked_at) moves into the durable park before the bridge is
-- removed: saved beside the park the dispatcher already recorded for this
-- entry, or, when a crash left the annotation committed without its park row
-- (or with one from an earlier entry), as that park. The stored message is
-- "Nothing can continue this Task from `<state>`: <cause>. Owner: ...", so the
-- cause is cut back out of it rather than stored as the whole sentence.
INSERT INTO task_schedule_park(task_id,epoch,reason_json)
SELECT id,status_epoch,json_object(
  'reason', CASE reason
    WHEN 'workflow_invalid' THEN json_object('WorkflowInvalid',json_object('state',status,'cause',
      CASE WHEN message IS NULL THEN 'state has no safe continuation'
           WHEN instr(message,'`: ')>0 AND instr(message,'. Owner: ')>instr(message,'`: ')
             THEN substr(message,instr(message,'`: ')+3,instr(message,'. Owner: ')-instr(message,'`: ')-3)
           ELSE message END))
    ELSE json_object('UnknownCondition',json_object('owner',CASE WHEN instr(COALESCE(message,''),'plan publication cleanup')>0 THEN 'plan publication cleanup' ELSE 'entry hooks' END)) END,
  'owner',CASE reason WHEN 'workflow_invalid' THEN 'ProjectAgent' ELSE 'Workflow' END,
  'recovery',CASE reason WHEN 'workflow_invalid' THEN 'EditWorkflow' ELSE 'ReconcileEntry' END,
  'diagnostic',json(error_annotation)
)
FROM (
  SELECT id,status,status_epoch,error_annotation,
         json_extract(error_annotation,'$.blocking_reason') AS reason,
         json_extract(error_annotation,'$.message') AS message
  FROM task
  WHERE json_valid(error_annotation)
    AND json_extract(error_annotation,'$.type')='workflow_guard_rejected'
    AND json_extract(error_annotation,'$.blocked_by')='system:task_dispatcher'
    AND json_extract(error_annotation,'$.blocking_reason') IN ('workflow_invalid','unknown_condition')
    AND deleted_at IS NULL
)
WHERE true
ON CONFLICT(task_id) DO UPDATE SET
  epoch=excluded.epoch,
  reason_json=CASE
    WHEN task_schedule_park.epoch IS NOT excluded.epoch OR NOT json_valid(task_schedule_park.reason_json)
      THEN excluded.reason_json
    WHEN json_type(task_schedule_park.reason_json,'$.reason.WorkflowInvalid') IS NOT NULL
      OR json_type(task_schedule_park.reason_json,'$.reason.UnknownCondition') IS NOT NULL
      THEN json_set(task_schedule_park.reason_json,'$.diagnostic',json(json_extract(excluded.reason_json,'$.diagnostic')))
    ELSE task_schedule_park.reason_json END;

UPDATE task SET error_annotation=NULL
WHERE json_valid(error_annotation)
  AND json_extract(error_annotation,'$.type')='workflow_guard_rejected'
  AND json_extract(error_annotation,'$.blocked_by')='system:task_dispatcher'
  AND json_extract(error_annotation,'$.blocking_reason') IN ('workflow_invalid','unknown_condition');
