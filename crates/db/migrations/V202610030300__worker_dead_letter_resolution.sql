-- Keep existing quarantine identities and failures; resolution is an audit record.
ALTER TABLE worker_dead_letter ADD COLUMN version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_dead_letter ADD COLUMN resolved_at TEXT;
ALTER TABLE worker_dead_letter ADD COLUMN resolved_by TEXT;
ALTER TABLE worker_dead_letter ADD COLUMN resolution TEXT CHECK (resolution IN ('replayed', 'skipped', 'dismissed'));
ALTER TABLE worker_dead_letter ADD COLUMN resolution_reason TEXT;
CREATE INDEX idx_worker_dead_letter_open_time
    ON worker_dead_letter(dead_lettered_at DESC, id DESC) WHERE resolved_at IS NULL;
CREATE INDEX idx_worker_dead_letter_resolved_time
    ON worker_dead_letter(resolved_at DESC, id DESC) WHERE resolved_at IS NOT NULL;
CREATE TABLE worker_dead_letter_action (
    id TEXT PRIMARY KEY,
    dead_letter_id TEXT NOT NULL REFERENCES worker_dead_letter(id),
    actor_id TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('replayed', 'skipped', 'dismissed', 'replay_failed')),
    reason TEXT,
    version INTEGER NOT NULL,
    UNIQUE (dead_letter_id, version)
);
