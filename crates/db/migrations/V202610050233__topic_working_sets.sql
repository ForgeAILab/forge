-- Topic intents and runtime claims preserve every existing transcript/timeline.
ALTER TABLE agent_chat_topic ADD COLUMN runtime_session_id TEXT;
DROP TRIGGER agent_chat_topic_immutable_update;
UPDATE agent_chat_topic SET runtime_session_id = (
    SELECT s.runtime_session_id FROM agent_session s JOIN agent_context_scope c ON c.id = s.context_scope_id
    WHERE c.scope_type = 'agent_chat' AND c.scope_id = agent_chat_topic.chat_id
      AND s.backend_kind = 'native' AND s.status IN ('starting','ready','running','degraded','suspended')
    ORDER BY s.created_at DESC LIMIT 1
) WHERE sequence = (SELECT MAX(t.sequence) FROM agent_chat_topic t WHERE t.chat_id = agent_chat_topic.chat_id);
CREATE TRIGGER agent_chat_topic_immutable_update BEFORE UPDATE ON agent_chat_topic
BEGIN SELECT RAISE(ABORT, 'Agent Chat topics are immutable'); END;

ALTER TABLE agent_lcm_timeline ADD COLUMN claim_owner TEXT;
ALTER TABLE agent_lcm_timeline ADD COLUMN claim_generation INTEGER NOT NULL DEFAULT 0 CHECK (claim_generation >= 0);
CREATE TABLE agent_runtime_lcm_binding (
    runtime_session_id TEXT PRIMARY KEY,
    timeline_id TEXT NOT NULL REFERENCES agent_lcm_timeline(id) ON DELETE CASCADE
);
CREATE TABLE agent_chat_topic_rotation (
    chat_id TEXT PRIMARY KEY REFERENCES agent_chat(id) ON DELETE CASCADE,
    id TEXT NOT NULL UNIQUE,
    successor_session_id TEXT NOT NULL DEFAULT (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || lower(substr(hex(randomblob(2)), 2, 3)) || '-8' || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6)))),
    successor_runtime_id TEXT NOT NULL DEFAULT (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || lower(substr(hex(randomblob(2)), 2, 3)) || '-8' || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6)))),
    rotation_pending INTEGER NOT NULL DEFAULT 1 CHECK (rotation_pending = 1),
    label TEXT NOT NULL CHECK (length(label) BETWEEN 1 AND 200),
    requested_summary TEXT,
    summary_ciphertext BLOB,
    summary_nonce BLOB,
    source_session_id TEXT REFERENCES agent_session(id) ON DELETE SET NULL,
    origin_turn_id TEXT REFERENCES agent_chat_turn_job(id) ON DELETE SET NULL,
    owner_token TEXT,
    lease_until TEXT,
    cause TEXT NOT NULL,
    -- Bounded execution: each failed attempt backs off; the third abandons
    -- the intent so queued turns run on the current topic.
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TEXT,
    last_error_kind TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE agent_topic_read_digest (
    runtime_session_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    digest TEXT NOT NULL,
    call_ref TEXT NOT NULL,
    -- Canonical history index (= LCM entry sequence) of the full result.
    -- A marker is returned only while no LCM node covers this index.
    history_index INTEGER NOT NULL CHECK (history_index >= 0),
    PRIMARY KEY (runtime_session_id, operation)
);
-- Automatic rotation intents are raised only for chats answered by a native
-- responder (account Main binding or Project binding -> selected Profile).
-- CLI chats keep their pre-topic-working-set behaviour.
CREATE TRIGGER genesis_topic_rotation AFTER INSERT ON product_genesis_session

BEGIN
INSERT OR IGNORE INTO agent_chat_topic_rotation (chat_id, id, label, cause, created_at)
SELECT NEW.main_chat_id, lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || lower(substr(hex(randomblob(2)), 2, 3)) || '-8' || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))), 'Product Genesis', 'genesis.start', NEW.created_at WHERE NEW.main_chat_id IS NOT NULL AND EXISTS (SELECT 1 FROM agent_chat c JOIN agent_identity i ON i.id = COALESCE((SELECT b.identity_id FROM account_main_agent_binding b WHERE b.account_id = c.account_id AND b.state = 'active'), (SELECT b.identity_id FROM project_agent_binding b WHERE b.project_id = c.project_id AND b.state = 'active')) JOIN agent_profile p ON p.id = i.selected_profile_id WHERE c.id = NEW.main_chat_id AND p.backend_kind = 'native');
END;
CREATE TRIGGER handoff_topic_rotation AFTER UPDATE OF lifecycle ON product_genesis_session
WHEN NEW.lifecycle = 'handed_off' AND OLD.lifecycle <> 'handed_off'
BEGIN
INSERT OR IGNORE INTO agent_chat_topic_rotation (chat_id, id, label, cause, created_at)
SELECT NEW.main_chat_id, lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || lower(substr(hex(randomblob(2)), 2, 3)) || '-8' || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))), 'After Project handoff', 'project.create', NEW.updated_at WHERE NEW.main_chat_id IS NOT NULL AND EXISTS (SELECT 1 FROM agent_chat c JOIN agent_identity i ON i.id = COALESCE((SELECT b.identity_id FROM account_main_agent_binding b WHERE b.account_id = c.account_id AND b.state = 'active'), (SELECT b.identity_id FROM project_agent_binding b WHERE b.project_id = c.project_id AND b.state = 'active')) JOIN agent_profile p ON p.id = i.selected_profile_id WHERE c.id = NEW.main_chat_id AND p.backend_kind = 'native');
END;
CREATE TRIGGER published_handoff_topic_rotation AFTER INSERT ON agent_handoff

BEGIN
INSERT OR IGNORE INTO agent_chat_topic_rotation (chat_id, id, label, cause, created_at)
SELECT NEW.source_chat_id, lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || lower(substr(hex(randomblob(2)), 2, 3)) || '-8' || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))), 'After handoff', 'handoff', NEW.created_at WHERE NEW.source_chat_id IS NOT NULL AND EXISTS (SELECT 1 FROM agent_chat c JOIN agent_identity i ON i.id = COALESCE((SELECT b.identity_id FROM account_main_agent_binding b WHERE b.account_id = c.account_id AND b.state = 'active'), (SELECT b.identity_id FROM project_agent_binding b WHERE b.project_id = c.project_id AND b.state = 'active')) JOIN agent_profile p ON p.id = i.selected_profile_id WHERE c.id = NEW.source_chat_id AND p.backend_kind = 'native');
END;
CREATE TRIGGER idle_topic_rotation AFTER INSERT ON agent_chat_message
WHEN NEW.author_type = 'user' AND julianday(NEW.created_at) - (SELECT MAX(julianday(created_at)) FROM agent_chat_message WHERE chat_id = NEW.chat_id AND id <> NEW.id AND author_type IN ('user', 'agent')) >= 8.0 / 24.0
BEGIN
INSERT OR IGNORE INTO agent_chat_topic_rotation (chat_id, id, label, cause, created_at)
SELECT NEW.chat_id, lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || lower(substr(hex(randomblob(2)), 2, 3)) || '-8' || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))), 'After idle', 'idle', NEW.created_at WHERE NEW.chat_id IS NOT NULL AND EXISTS (SELECT 1 FROM agent_chat c JOIN agent_identity i ON i.id = COALESCE((SELECT b.identity_id FROM account_main_agent_binding b WHERE b.account_id = c.account_id AND b.state = 'active'), (SELECT b.identity_id FROM project_agent_binding b WHERE b.project_id = c.project_id AND b.state = 'active')) JOIN agent_profile p ON p.id = i.selected_profile_id WHERE c.id = NEW.chat_id AND p.backend_kind = 'native');
END;

CREATE TABLE agent_topic_summary_usage (
    id TEXT PRIMARY KEY,
    runtime_session_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    failed INTEGER NOT NULL,
    purpose TEXT NOT NULL CHECK (purpose = 'topic_summary'),
    -- Outbox for the rotation-summary provider call. Settled once into the
    -- usage ledger, keyed by this row id, at rotation time (or by the
    -- worker's drain after a crash); never through later turns.
    chat_id TEXT,
    settled_at TEXT
);

CREATE TRIGGER topic_rotation_origin AFTER INSERT ON agent_chat_topic_rotation
BEGIN
    UPDATE agent_chat_topic_rotation SET origin_turn_id = (
        SELECT id FROM agent_chat_turn_job WHERE chat_id = NEW.chat_id AND status IN ('leased','awaiting_input')
        ORDER BY created_at DESC LIMIT 1
    ) WHERE chat_id = NEW.chat_id;
END;
