-- A durable, indexed deletion nudge; ciphertext and legacy rows are unchanged.
ALTER TABLE agent_session_journal_pack ADD COLUMN orphaned INTEGER NOT NULL
    DEFAULT 1 CHECK (orphaned IN (0, 1));
UPDATE agent_session_journal_pack SET orphaned =
    NOT EXISTS (SELECT 1 FROM agent_session s WHERE s.id = forge_session_id)
    OR NOT EXISTS (SELECT 1 FROM agent_session_journal_membership m
                   WHERE m.pack_id = agent_session_journal_pack.pack_id);
CREATE INDEX idx_session_journal_pack_orphan
    ON agent_session_journal_pack(pack_id) WHERE orphaned = 1;

CREATE TRIGGER session_journal_pack_membership_insert
AFTER INSERT ON agent_session_journal_membership
BEGIN
    UPDATE agent_session_journal_pack SET orphaned = 0 WHERE pack_id = NEW.pack_id;
END;
CREATE TRIGGER session_journal_pack_membership_delete
AFTER DELETE ON agent_session_journal_membership
BEGIN
    UPDATE agent_session_journal_pack SET orphaned =
        NOT EXISTS (SELECT 1 FROM agent_session_journal_membership WHERE pack_id = OLD.pack_id)
    WHERE pack_id = OLD.pack_id;
END;
CREATE TRIGGER session_journal_pack_membership_move
AFTER UPDATE OF pack_id ON agent_session_journal_membership
WHEN NEW.pack_id <> OLD.pack_id
BEGIN
    UPDATE agent_session_journal_pack SET orphaned =
        NOT EXISTS (SELECT 1 FROM agent_session_journal_membership WHERE pack_id = OLD.pack_id)
    WHERE pack_id = OLD.pack_id;
    UPDATE agent_session_journal_pack SET orphaned = 0 WHERE pack_id = NEW.pack_id;
END;
CREATE TRIGGER session_journal_session_deleted
AFTER DELETE ON agent_session
BEGIN
    UPDATE agent_session_journal_pack SET orphaned = 1 WHERE forge_session_id = OLD.id;
END;

-- Project/Task scopes already have cascading owner FKs. Agent Chat uses a
-- polymorphic scope id, so its delete needs this explicit edge.
CREATE TRIGGER session_journal_chat_deleted
AFTER DELETE ON agent_chat
BEGIN
    DELETE FROM agent_context_scope WHERE scope_type = 'agent_chat' AND scope_id = OLD.id;
END;
-- The user-facing Agent delete archives its identity; retain the identity and
-- transcript, but discard its unusable native/CLI sessions through the same FKs.
CREATE TRIGGER session_journal_agent_archived
AFTER UPDATE OF archived_at ON agent_identity
WHEN OLD.archived_at IS NULL AND NEW.archived_at IS NOT NULL
BEGIN
    DELETE FROM agent_session WHERE identity_id = NEW.id;
END;
