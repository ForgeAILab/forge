-- Source-neutral health and poison state. Event workers keep their existing
-- event_consumer_cursor; only retired memory delivery bookkeeping is removed.
CREATE TABLE worker_health (
    worker_name TEXT PRIMARY KEY,
    subscription_json TEXT CHECK (subscription_json IS NULL OR json_valid(subscription_json)),
    runtime_error TEXT,
    runtime_error_at TEXT,
    item_error TEXT,
    item_error_at TEXT,
    tick_error TEXT,
    tick_error_at TEXT,
    after_commit_error TEXT,
    after_commit_error_at TEXT,
    last_error_at TEXT GENERATED ALWAYS AS (NULLIF(MAX(
        COALESCE(runtime_error_at, ''), COALESCE(item_error_at, ''),
        COALESCE(tick_error_at, ''), COALESCE(after_commit_error_at, '')), '')) VIRTUAL,
    last_error TEXT GENERATED ALWAYS AS (CASE last_error_at
        WHEN item_error_at THEN item_error
        WHEN runtime_error_at THEN runtime_error
        WHEN tick_error_at THEN tick_error
        WHEN after_commit_error_at THEN after_commit_error END) VIRTUAL,
    restart_count INTEGER NOT NULL DEFAULT 0 CHECK (restart_count >= 0),
    last_success_at TEXT,
    retry_source_key TEXT,
    retry_attempts INTEGER NOT NULL DEFAULT 0 CHECK (retry_attempts >= 0),
    retry_not_before TEXT,
    retry_started_at TEXT,
    deferred_source_key TEXT,
    deferred_reason TEXT,
    deferred_since TEXT,
    defer_not_before TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK ((retry_source_key IS NULL AND retry_attempts = 0
            AND retry_not_before IS NULL AND retry_started_at IS NULL)
        OR (retry_source_key IS NOT NULL AND retry_attempts > 0
            AND retry_started_at IS NOT NULL)),
    CHECK ((deferred_source_key IS NULL AND deferred_reason IS NULL
            AND deferred_since IS NULL AND defer_not_before IS NULL)
        OR (deferred_source_key IS NOT NULL AND deferred_reason IS NOT NULL
            AND deferred_since IS NOT NULL AND defer_not_before IS NOT NULL))
);
CREATE TABLE worker_dead_letter (
    worker_name TEXT NOT NULL,
    source_key TEXT NOT NULL,
    item_type TEXT NOT NULL,
    attempts INTEGER NOT NULL CHECK (attempts >= 0),
    last_error TEXT NOT NULL,
    first_failed_at TEXT NOT NULL,
    last_failed_at TEXT NOT NULL,
    dead_lettered_at TEXT NOT NULL,
    PRIMARY KEY (worker_name, source_key)
);
CREATE INDEX idx_worker_dead_letter_worker_time
    ON worker_dead_letter(worker_name, dead_lettered_at DESC);

-- These are delivery metadata, not projection data or user history. There is
-- no lease/receipt retention job. Preserve all other consumers' rows.
DELETE FROM event_processing_lease WHERE consumer_name = 'scoped-memory-agent-chat-indexer';
DELETE FROM event_projection_receipt WHERE consumer_name = 'scoped-memory-agent-chat-indexer';
