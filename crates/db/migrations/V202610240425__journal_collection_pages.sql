-- Physical paging metadata only; every sealed payload and membership survives.
ALTER TABLE agent_session_journal_session ADD COLUMN layout_generation INTEGER NOT NULL DEFAULT 0;
ALTER TABLE agent_session_journal_pin ADD COLUMN pin_order INTEGER;
CREATE INDEX idx_session_journal_pin_order ON agent_session_journal_pin(storage_id,role,runtime_session_id,pin_order);
CREATE INDEX idx_session_journal_session_runtime ON agent_session_journal_session(runtime_session_id);
