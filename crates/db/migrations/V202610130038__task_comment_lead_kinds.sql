-- Two comment kinds a coordination root's lead writes (plan 3.10 stage 4):
-- `handoff`, a note on a subtask that its next implementation run reads
-- first, and `request_input`, the lead's question to the owner on the root.
-- SQLite cannot alter a CHECK, so `task_comment` is rebuilt: every row and
-- every column value is copied, and its three indexes are created again. No
-- other table, view or trigger references `task_comment`; foreign keys are
-- off for the rebuild, as in V202610100820.
PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;
CREATE TABLE task_comment_new (
    id          TEXT PRIMARY KEY,
    task_id     TEXT NOT NULL REFERENCES task(id) ON DELETE CASCADE,
    author_type TEXT NOT NULL CHECK (author_type IN ('user', 'agent', 'system')),
    author_id   TEXT,
    author_name TEXT NOT NULL,
    content     TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    execution_id TEXT REFERENCES execution(id) ON DELETE SET NULL,
    role TEXT,
    worklog_kind TEXT
        CHECK (worklog_kind IS NULL OR worklog_kind IN ('progress', 'decision', 'validation', 'blocker', 'handoff', 'request_input')),
    idempotency_key TEXT
);
INSERT INTO task_comment_new (id, task_id, author_type, author_id, author_name, content, created_at, updated_at, execution_id, role, worklog_kind, idempotency_key)
SELECT id, task_id, author_type, author_id, author_name, content, created_at, updated_at, execution_id, role, worklog_kind, idempotency_key
FROM task_comment;
DROP TABLE task_comment;
ALTER TABLE task_comment_new RENAME TO task_comment;
CREATE INDEX idx_task_comment_task_id ON task_comment(task_id, created_at);
CREATE UNIQUE INDEX idx_task_comment_idempotency
    ON task_comment(task_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_task_comment_execution
    ON task_comment(execution_id, created_at)
    WHERE execution_id IS NOT NULL;
COMMIT;
PRAGMA foreign_keys = ON;
