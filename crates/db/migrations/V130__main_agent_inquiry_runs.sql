-- The Main agent can dispatch an ephemeral, read-only sub-agent turn (an
-- "inquiry") that runs in its own scratch workspace, streams its work to the
-- UI as a visible run record, and returns a bounded findings abstract to the
-- parent turn. There is no repo, no task flow, and no state machine: this is
-- a run log, not a work item, so the only user verb over a row is cancel.
CREATE TABLE agent_inquiry (
    id                 TEXT PRIMARY KEY NOT NULL,
    chat_id            TEXT NOT NULL REFERENCES agent_chat(id) ON DELETE CASCADE,
    turn_job_id        TEXT,
    identity_id        TEXT NOT NULL,
    owner_user_id      TEXT NOT NULL,
    title              TEXT NOT NULL,
    question           TEXT NOT NULL,
    status             TEXT NOT NULL,
    findings           TEXT,
    findings_path      TEXT,
    workspace_path     TEXT,
    error              TEXT,
    input_tokens       INTEGER NOT NULL DEFAULT 0,
    output_tokens      INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens  INTEGER NOT NULL DEFAULT 0,
    cache_write_tokens INTEGER NOT NULL DEFAULT 0,
    duration_ms        INTEGER,
    version            INTEGER NOT NULL DEFAULT 1,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    started_at         TEXT NOT NULL,
    finished_at        TEXT
);

CREATE INDEX idx_agent_inquiry_chat ON agent_inquiry(chat_id, created_at DESC, id DESC);
CREATE INDEX idx_agent_inquiry_status ON agent_inquiry(status);
