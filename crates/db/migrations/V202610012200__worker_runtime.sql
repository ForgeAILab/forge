-- Shared durable state for single-process cursor workers.  Existing consumer
-- cursors, leases, and projection receipts remain intact so a worker can move
-- onto the runtime without losing its checkpoint or rewriting history.
CREATE TABLE worker_health (
    worker_name          TEXT PRIMARY KEY,
    cursor_sequence      INTEGER NOT NULL DEFAULT 0 CHECK (cursor_sequence >= 0),
    head_sequence        INTEGER NOT NULL DEFAULT 0 CHECK (head_sequence >= 0),
    lag                  INTEGER NOT NULL DEFAULT 0 CHECK (lag >= 0),
    oldest_pending_at    TEXT,
    last_error           TEXT,
    last_error_at        TEXT,
    restart_count        INTEGER NOT NULL DEFAULT 0 CHECK (restart_count >= 0),
    last_success_at      TEXT,
    cursor_updated_at    TEXT NOT NULL,
    retry_sequence       INTEGER,
    retry_attempts       INTEGER NOT NULL DEFAULT 0 CHECK (retry_attempts >= 0),
    retry_not_before     TEXT,
    retry_started_at     TEXT,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL,
    CHECK ((retry_sequence IS NULL AND retry_attempts = 0
            AND retry_not_before IS NULL AND retry_started_at IS NULL)
        OR (retry_sequence IS NOT NULL AND retry_attempts > 0
            AND retry_started_at IS NOT NULL))
);

CREATE TABLE worker_dead_letter (
    worker_name          TEXT NOT NULL,
    event_sequence       INTEGER NOT NULL,
    event_type           TEXT NOT NULL,
    attempts             INTEGER NOT NULL CHECK (attempts > 0),
    last_error           TEXT NOT NULL,
    first_failed_at      TEXT NOT NULL,
    last_failed_at       TEXT NOT NULL,
    dead_lettered_at     TEXT NOT NULL,
    PRIMARY KEY (worker_name, event_sequence)
);

CREATE INDEX idx_worker_dead_letter_worker_time
    ON worker_dead_letter(worker_name, dead_lettered_at DESC);
