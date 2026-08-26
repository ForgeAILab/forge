-- Durable, user-owned Main Chat topic boundary (design D21, live-acceptance
-- finding F18: "the singular Main Chat has no fresh-topic boundary").
--
-- A topic is a context epoch INSIDE the one account Main Chat -- never a
-- second chat, binding, or authority scope.  Rows are immutable once
-- written, matching every other Agent Chat ledger table in this schema
-- (agent_chat_message, agent_chat_instruction_revision, agent_handoff, ...).
--
-- `agent_chat_message` and `agent_chat_turn_job` are deliberately NOT
-- altered by this migration.  A topic instead records the `sequence` of the
-- visible divider message that opens it: "this topic's messages" is
-- `sequence >= starting_message_sequence`, bounded above by the next
-- topic's `starting_message_sequence` when one exists.  That keeps the
-- backfill below from changing a single existing message/turn id or
-- provenance field -- it only inserts new `agent_chat_topic` rows.

CREATE TABLE agent_chat_topic (
    id                          TEXT PRIMARY KEY,
    chat_id                     TEXT NOT NULL REFERENCES agent_chat(id) ON DELETE CASCADE,
    sequence                    INTEGER NOT NULL CHECK (sequence >= 0),
    label                       TEXT NOT NULL CHECK (length(label) BETWEEN 1 AND 200),
    summary                     TEXT CHECK (summary IS NULL OR length(summary) <= 2000),
    -- NULL only for the backfilled initial topic below, which predates any
    -- divider message and simply starts at `starting_message_sequence = 0`.
    starting_message_id         TEXT REFERENCES agent_chat_message(id) ON DELETE SET NULL,
    starting_message_sequence   INTEGER NOT NULL CHECK (starting_message_sequence >= 0),
    principal_type               TEXT NOT NULL CHECK (principal_type IN ('user', 'system')),
    principal_id                TEXT,
    created_at                  TEXT NOT NULL,
    UNIQUE (chat_id, sequence)
);

CREATE INDEX idx_agent_chat_topic_chat
    ON agent_chat_topic(chat_id, sequence ASC);

CREATE TRIGGER agent_chat_topic_immutable_update
BEFORE UPDATE ON agent_chat_topic
BEGIN
    SELECT RAISE(ABORT, 'Main Chat topics are immutable');
END;

CREATE TRIGGER agent_chat_topic_immutable_delete
BEFORE DELETE ON agent_chat_topic
BEGIN
    SELECT RAISE(ABORT, 'Main Chat topics are immutable');
END;

-- Backfill exactly one initial topic for every existing Main Chat, starting
-- at message sequence 0 so every historical message and turn is already
-- covered by it.  No `agent_chat_message`/`agent_chat_turn_job` row is
-- touched by this migration.
INSERT INTO agent_chat_topic (
    id, chat_id, sequence, label, summary, starting_message_id,
    starting_message_sequence, principal_type, principal_id, created_at
)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
        substr('89ab', 1 + (abs(random()) % 4), 1) ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))),
    id,
    0,
    'Original conversation',
    NULL,
    NULL,
    0,
    'system',
    NULL,
    COALESCE(created_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
FROM agent_chat
WHERE kind = 'account_main';
