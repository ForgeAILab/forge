-- Preserve existing cascade history, leases and fences while admitting hook steps.
CREATE TABLE task_step_new (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES task(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('cascade','hooks')),
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
CREATE TABLE task_hook_checkpoint (
    step_id TEXT NOT NULL REFERENCES task_step(id) ON DELETE CASCADE,
    hook_index INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    result_json TEXT,
    effects_json TEXT NOT NULL DEFAULT '{}',
    execution_id TEXT REFERENCES execution(id) ON DELETE SET NULL,
    PRIMARY KEY(step_id,hook_index)
);
CREATE TABLE task_hook_script (
    step_id TEXT NOT NULL,
    hook_index INTEGER NOT NULL,
    script_index INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    result_json TEXT,
    PRIMARY KEY(step_id,hook_index,script_index),
    FOREIGN KEY(step_id,hook_index) REFERENCES task_hook_checkpoint(step_id,hook_index) ON DELETE CASCADE
);
-- Legacy running barriers have no resumable hook row. Preserve their data and
-- expose the established blocked-entry recovery instead of retaining a lock.
UPDATE task SET
    entry_barrier_json=json_set(entry_barrier_json,'$.status','blocked','$.blocking_reason','before_enter was interrupted before durable hook upgrade'),
    error_annotation=COALESCE(error_annotation,json_object('type','before_work_hook_failed','state',status,'message','before_enter was interrupted before durable hook upgrade')),
    version=version+1
WHERE CASE WHEN json_valid(entry_barrier_json) THEN json_extract(entry_barrier_json,'$.status') END='running';
