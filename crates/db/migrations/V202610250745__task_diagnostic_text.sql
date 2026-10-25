CREATE TABLE task_diagnostic_text (
    id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL REFERENCES task(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    source TEXT NOT NULL,
    owner_json TEXT NOT NULL CHECK (json_valid(owner_json)),
    text TEXT NOT NULL CHECK (length(CAST(text AS BLOB)) <= 1048576),
    byte_length INTEGER NOT NULL CHECK (byte_length >= 0),
    digest TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    occurrence_count INTEGER NOT NULL DEFAULT 1 CHECK (occurrence_count > 0),
    UNIQUE (task_id, kind, digest)
);
CREATE INDEX task_diagnostic_text_task_created ON task_diagnostic_text(task_id, created_at, id);
