-- Durable cascade outbox. No existing rows or historical migrations change.
CREATE TABLE task_step (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES task(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind = 'cascade'),
    payload_json TEXT NOT NULL,
    causation_step_id TEXT REFERENCES task_step(id) ON DELETE SET NULL,
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
    UNIQUE(task_id, seq),
    UNIQUE(task_id, causation_key)
);
CREATE INDEX task_step_next ON task_step(task_id, seq) WHERE status IN ('pending','claimed');
CREATE INDEX task_step_expired ON task_step(lease_until) WHERE lease_until IS NOT NULL;
CREATE INDEX task_step_chain ON task_step(chain_id, chain_position);
CREATE INDEX task_step_task_lease ON task_step(task_id, lease_until) WHERE lease_until IS NOT NULL;
