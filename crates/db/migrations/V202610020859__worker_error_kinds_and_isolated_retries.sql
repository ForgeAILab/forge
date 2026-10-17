ALTER TABLE worker_health ADD COLUMN runtime_error_kind TEXT CHECK (runtime_error_kind IN ('failure', 'transient', 'terminal'));
ALTER TABLE worker_health ADD COLUMN item_error_kind TEXT CHECK (item_error_kind IN ('failure', 'transient', 'terminal'));
ALTER TABLE worker_health ADD COLUMN tick_error_kind TEXT CHECK (tick_error_kind IN ('failure', 'transient', 'terminal'));
ALTER TABLE worker_health ADD COLUMN after_commit_error_kind TEXT CHECK (after_commit_error_kind IN ('failure', 'transient', 'terminal'));
UPDATE worker_health SET runtime_error_kind = CASE WHEN runtime_error IS NOT NULL THEN 'transient' END,
    item_error_kind = CASE WHEN item_error IS NOT NULL THEN 'failure' END,
    tick_error_kind = CASE WHEN tick_error IS NOT NULL THEN 'failure' END,
    after_commit_error_kind = CASE WHEN after_commit_error IS NOT NULL THEN 'failure' END;
ALTER TABLE worker_health ADD COLUMN last_error_kind TEXT GENERATED ALWAYS AS (CASE last_error_at
    WHEN item_error_at THEN item_error_kind WHEN runtime_error_at THEN runtime_error_kind
    WHEN tick_error_at THEN tick_error_kind WHEN after_commit_error_at THEN after_commit_error_kind END) VIRTUAL;
ALTER TABLE worker_dead_letter ADD COLUMN error_kind TEXT NOT NULL DEFAULT 'failure' CHECK (error_kind IN ('failure', 'transient', 'terminal'));
UPDATE worker_dead_letter SET error_kind = 'terminal' WHERE attempts = 0;
-- Per-row retry failures are operational metadata, independent of the event cursor.
CREATE TABLE worker_item_failure (
    worker_name TEXT NOT NULL, source_key TEXT NOT NULL, attempts INTEGER NOT NULL,
    first_failed_at TEXT NOT NULL, last_error TEXT NOT NULL, error_kind TEXT NOT NULL CHECK (error_kind IN ('failure', 'transient', 'terminal')), retry_not_before TEXT NOT NULL,
    PRIMARY KEY (worker_name, source_key)
);
CREATE INDEX idx_domain_event_causation_type ON domain_event(causation_id, event_type, sequence);
