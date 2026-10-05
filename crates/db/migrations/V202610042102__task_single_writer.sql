-- Preserve step history and dependent hook/script checkpoints during the rebuild.
PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;
CREATE TABLE task_step_new (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES task(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('cascade','hooks','command','mutation')),
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
    workflow_ref_id TEXT GENERATED ALWAYS AS (CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.workflow_ref.id') END) VIRTUAL,
    UNIQUE(task_id, seq),
    UNIQUE(task_id, causation_key)
);
INSERT INTO task_step_new (id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,attempts,last_error,created_at,updated_at,completed_at,expected_epoch,lane) SELECT id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,attempts,last_error,created_at,updated_at,completed_at,expected_epoch,lane FROM task_step;
DROP TABLE task_step;
ALTER TABLE task_step_new RENAME TO task_step;
CREATE INDEX task_step_next ON task_step(task_id, seq) WHERE status IN ('pending','claimed');
CREATE INDEX task_step_expired ON task_step(lease_until) WHERE lease_until IS NOT NULL;
CREATE INDEX task_step_chain ON task_step(chain_id, chain_position);
CREATE INDEX task_step_task_lease ON task_step(task_id, lease_until) WHERE lease_until IS NOT NULL;
CREATE INDEX task_step_lane_ready ON task_step(lane,available_at,created_at) WHERE status IN ('pending','claimed');
CREATE INDEX task_step_settled ON task_step(status,completed_at);
CREATE INDEX task_step_workflow_ref ON task_step(workflow_ref_id) WHERE workflow_ref_id IS NOT NULL;
CREATE TABLE task_remote_operation (
    operation_id TEXT PRIMARY KEY,
    step_id TEXT NOT NULL REFERENCES task_step(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE RESTRICT,
    placement_id TEXT NOT NULL REFERENCES workspace_placement(id) ON DELETE RESTRICT,
    daemon_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    expected_epoch INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('running','finished','cancelled')),
    created_at TEXT NOT NULL
);
CREATE INDEX task_remote_operation_step ON task_remote_operation(step_id,state);
CREATE TABLE pending_remote_cancel (
    operation_id TEXT PRIMARY KEY,
    step_id TEXT NOT NULL REFERENCES task_step(id) ON DELETE RESTRICT,
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE RESTRICT,
    placement_id TEXT NOT NULL REFERENCES workspace_placement(id) ON DELETE RESTRICT,
    daemon_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    expected_epoch INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX pending_remote_cancel_owner ON pending_remote_cancel(daemon_id);
CREATE INDEX pending_remote_cancel_workspace ON pending_remote_cancel(workspace_id);

CREATE INDEX pending_remote_cancel_step ON pending_remote_cancel(step_id);
COMMIT;
PRAGMA foreign_keys = ON;
