-- Step kind `integration`: the Task-step consumer of the integration queue
-- (plan 3.2 stage D1b). Nothing enqueues one until the queue is activated.
-- SQLite cannot alter a CHECK, so the table is rebuilt; every row, index and
-- trigger is preserved and no dependent row is touched (foreign keys are off
-- for the rebuild, as in V202610051045).
PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;
CREATE TABLE task_step_new (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES task(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('cascade','hooks','command','mutation','integration')),
    payload_json TEXT NOT NULL,
    causation_step_id TEXT REFERENCES task_step_new(id) ON DELETE SET NULL,
    causation_key TEXT NOT NULL,
    chain_id TEXT NOT NULL,
    chain_position INTEGER NOT NULL CHECK (chain_position > 0),
    expected_status TEXT NOT NULL,
    expected_version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','claimed','done','superseded','parked','failed')),
    claimed_by TEXT,
    lease_until TEXT,
    available_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    result_json TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    preempt_requested_at TEXT,
    integration_started_at TEXT,
    expected_epoch INTEGER NOT NULL DEFAULT 0,
    lane TEXT NOT NULL DEFAULT 'fast' CHECK (lane IN ('fast','long')),
    -- 1: the step applies only in its producing status entry and a preempting
    -- Cancel/Hold supersedes it. 0: a queued effect fenced by its own
    -- identity (wakes, clears, dependency blocks); it survives both.
    entry_fenced INTEGER NOT NULL DEFAULT 1 CHECK (entry_fenced IN (0,1)),
    workflow_ref_id TEXT GENERATED ALWAYS AS (CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.workflow_ref.id') END) VIRTUAL,
    UNIQUE(task_id, seq),
    UNIQUE(task_id, causation_key)
);
INSERT INTO task_step_new (id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,attempts,last_error,created_at,updated_at,completed_at,result_json,priority,preempt_requested_at,integration_started_at,expected_epoch,lane,entry_fenced) SELECT id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,attempts,last_error,created_at,updated_at,completed_at,result_json,priority,preempt_requested_at,integration_started_at,expected_epoch,lane,entry_fenced FROM task_step;
DROP TABLE task_step;
ALTER TABLE task_step_new RENAME TO task_step;
CREATE INDEX task_step_next ON task_step(task_id, seq) WHERE status IN ('pending','claimed');
CREATE INDEX task_step_expired ON task_step(lease_until) WHERE lease_until IS NOT NULL;
CREATE INDEX task_step_chain ON task_step(chain_id, chain_position);
CREATE INDEX task_step_task_lease ON task_step(task_id, lease_until) WHERE lease_until IS NOT NULL;
CREATE INDEX task_step_lane_ready ON task_step(lane,available_at,created_at) WHERE status IN ('pending','claimed');
CREATE INDEX task_step_settled ON task_step(status,completed_at);
CREATE INDEX task_step_workflow_ref ON task_step(workflow_ref_id) WHERE workflow_ref_id IS NOT NULL;
CREATE TRIGGER task_schedule_step_insert AFTER INSERT ON task_step WHEN NEW.kind != 'mutation' BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;
CREATE TRIGGER task_schedule_step_delete AFTER DELETE ON task_step WHEN OLD.kind != 'mutation' BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=OLD.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;
CREATE TRIGGER task_schedule_step_update AFTER UPDATE ON task_step WHEN NEW.kind != 'mutation' AND (OLD.status IS NOT NEW.status OR OLD.available_at IS NOT NEW.available_at) BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;
CREATE TRIGGER task_schedule_admission_release AFTER UPDATE ON task_step WHEN OLD.kind='command' AND json_valid(OLD.payload_json) AND json_extract(OLD.payload_json,'$.admission_agent_id') IS NOT NULL AND (OLD.status IS NOT NEW.status OR OLD.available_at IS NOT NEW.available_at OR OLD.lease_until IS NOT NEW.lease_until) BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT task_id,1 FROM task_schedule_wait WHERE agent_id=json_extract(OLD.payload_json,'$.admission_agent_id') OR daemon_id IN (COALESCE((SELECT COALESCE(p.execution_daemon_id,p.daemon_id) FROM workspace_placement p JOIN task t ON p.task_id=COALESCE(t.parent_task_id,t.id) WHERE t.id=OLD.task_id),(SELECT daemon_id FROM agent_current WHERE id=json_extract(OLD.payload_json,'$.admission_agent_id')),''),'*') ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;
COMMIT;
PRAGMA foreign_keys = ON;
