-- Physical paging metadata only; every sealed payload and membership survives.
ALTER TABLE agent_session_journal_session ADD COLUMN layout_generation INTEGER NOT NULL DEFAULT 0;
ALTER TABLE agent_session_journal_pin ADD COLUMN pin_order INTEGER;
CREATE INDEX agent_session_journal_pin_order ON agent_session_journal_pin(storage_id,role,runtime_session_id,pin_order);
CREATE TRIGGER journal_pack_insert_layout AFTER INSERT ON agent_session_journal_pack BEGIN
 UPDATE agent_session_journal_session SET layout_generation=layout_generation+1 WHERE runtime_session_id=NEW.runtime_session_id;
END;
CREATE TRIGGER journal_pack_update_layout AFTER UPDATE OF generation ON agent_session_journal_pack BEGIN
 UPDATE agent_session_journal_session SET layout_generation=layout_generation+1 WHERE runtime_session_id=NEW.runtime_session_id;
END;
CREATE TRIGGER journal_pack_delete_layout AFTER DELETE ON agent_session_journal_pack BEGIN
 UPDATE agent_session_journal_session SET layout_generation=layout_generation+1 WHERE runtime_session_id=OLD.runtime_session_id;
END;
