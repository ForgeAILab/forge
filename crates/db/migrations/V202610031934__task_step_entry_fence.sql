-- Fence queued cascades on a status epoch instead of the Task version.
-- Every status change bumps the epoch, whoever writes it; same-state audit
-- rows (recovery markers, board reorders) never do. The workflow engine bumps
-- it explicitly for a real self-transition (for example planning -> planning).
ALTER TABLE task ADD COLUMN status_epoch INTEGER NOT NULL DEFAULT 0;
CREATE TRIGGER task_status_epoch AFTER UPDATE OF status ON task
WHEN NEW.status IS NOT OLD.status
BEGIN
    UPDATE task SET status_epoch = status_epoch + 1 WHERE id = NEW.id;
END;
-- The epoch a transition entered. Written by the engine CAS and board moves,
-- so producers that enqueue after commit fence on the entry they belong to.
ALTER TABLE transition_log ADD COLUMN status_epoch INTEGER;
-- Steps in flight at upgrade fence on epoch 0: valid until the Task's status
-- next changes.
ALTER TABLE task_step ADD COLUMN expected_epoch INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_step ADD COLUMN lane TEXT NOT NULL DEFAULT 'fast' CHECK (lane IN ('fast','long'));
CREATE INDEX task_step_lane_ready ON task_step(lane,available_at,created_at) WHERE status IN ('pending','claimed');
CREATE INDEX task_step_settled ON task_step(status,completed_at);
CREATE TABLE task_step_workflow (
    id TEXT PRIMARY KEY,
    definition_json TEXT NOT NULL UNIQUE,
    last_used_at TEXT NOT NULL
);
INSERT INTO task_step_workflow(id,definition_json,last_used_at)
SELECT MIN(id),json_extract(payload_json,'$.workflow'),MAX(created_at) FROM task_step
WHERE json_type(payload_json,'$.workflow')='object' AND json_type(payload_json,'$.authority') IS NOT 'object'
GROUP BY json_extract(payload_json,'$.workflow');
-- Only merge and CI review checks hold a long-lane slot.
UPDATE task_step SET lane='long' WHERE EXISTS (
    SELECT 1 FROM json_each(payload_json,'$.workflow.states') state,json_tree(state.value,'$.hooks') hook
    WHERE json_extract(state.value,'$.name')=json_extract(task_step.payload_json,'$.to')
      AND hook.key='action' AND hook.value IN ('run_merge','run_ci_steps')
);
UPDATE task_step SET payload_json=json_set(json_remove(payload_json,'$.workflow','$.authority'),
    '$.workflow_ref',json(CASE WHEN json_type(payload_json,'$.authority')='object' THEN '{"kind":"project"}'
    ELSE json_object('kind','snapshot','id',(SELECT id FROM task_step_workflow WHERE definition_json=json_extract(task_step.payload_json,'$.workflow'))) END),
    '$.clear_review_passed_at_on_commit',json(CASE WHEN json_extract(payload_json,'$.authority.clear_review_passed_at_on_commit')=1 THEN 'true' ELSE 'false' END)
) WHERE json_type(payload_json,'$.workflow')='object';
-- Indexed reference lookup for unreferenced-definition pruning.
ALTER TABLE task_step ADD COLUMN workflow_ref_id TEXT GENERATED ALWAYS AS (CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.workflow_ref.id') END) VIRTUAL;
CREATE INDEX task_step_workflow_ref ON task_step(workflow_ref_id) WHERE workflow_ref_id IS NOT NULL;
