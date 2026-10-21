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

-- Chat-scope deletion and Agent archival are repository logic, in their source
-- writer transactions. Only orphan projection triggers live in this migration.
