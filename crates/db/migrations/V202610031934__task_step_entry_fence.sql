-- Preserve queued work while replacing version-only fences with entry identity.
ALTER TABLE transition_log ADD COLUMN is_status_entry INTEGER NOT NULL DEFAULT 1 CHECK(is_status_entry IN (0,1));
-- Board reorders historically shared the audit log without entering a state.
UPDATE transition_log SET is_status_entry=0 WHERE from_state=to_state AND trigger_reason='board reorder';
CREATE INDEX transition_log_status_entry ON transition_log(task_id,created_at DESC,id DESC) WHERE is_status_entry=1;
ALTER TABLE task_step ADD COLUMN producing_transition_id TEXT;
ALTER TABLE task_step ADD COLUMN lane TEXT NOT NULL DEFAULT 'fast' CHECK (lane IN ('fast','long'));
UPDATE task_step SET producing_transition_id = COALESCE(
    (SELECT id FROM transition_log WHERE task_id=task_step.task_id AND id=task_step.causation_key),
    (SELECT id FROM transition_log WHERE task_id=task_step.task_id AND is_status_entry=1 AND to_state=task_step.expected_status
        AND created_at<=task_step.created_at ORDER BY created_at DESC,id DESC LIMIT 1)
);
CREATE INDEX task_step_lane_ready ON task_step(lane,available_at,created_at) WHERE status IN ('pending','claimed');
CREATE TABLE task_step_workflow (
    id TEXT PRIMARY KEY,
    definition_json TEXT NOT NULL UNIQUE,
    last_used_at TEXT NOT NULL
);
INSERT INTO task_step_workflow(id,definition_json,last_used_at)
SELECT MIN(id),json_extract(payload_json,'$.workflow'),MAX(created_at) FROM task_step
WHERE json_type(payload_json,'$.workflow')='object' AND json_type(payload_json,'$.authority') IS NOT 'object'
GROUP BY json_extract(payload_json,'$.workflow');
UPDATE task_step SET lane='long' WHERE EXISTS (
    SELECT 1 FROM json_each(payload_json,'$.workflow.states') state,json_tree(state.value,'$.hooks') hook
    WHERE json_extract(state.value,'$.name')=json_extract(task_step.payload_json,'$.to')
      AND hook.key='action' AND hook.value IN ('run_merge','run_ci_steps','run_before_work_hooks','dispatch_role_agent','dispatch_fix_agent','dispatch_executor')
);
UPDATE task_step SET payload_json=json_set(json_remove(payload_json,'$.workflow','$.authority'),
    '$.workflow_ref',json(CASE WHEN json_type(payload_json,'$.authority')='object' THEN '{"kind":"project"}'
    ELSE json_object('kind','snapshot','id',(SELECT id FROM task_step_workflow WHERE definition_json=json_extract(task_step.payload_json,'$.workflow'))) END),
    '$.clear_review_passed_at_on_commit',json(CASE WHEN json_extract(payload_json,'$.authority.clear_review_passed_at_on_commit')=1 THEN 'true' ELSE 'false' END)
) WHERE json_type(payload_json,'$.workflow')='object';
