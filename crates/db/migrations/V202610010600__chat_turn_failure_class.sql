-- Old error codes/messages remain untouched; typed evidence is absent on old rows.
ALTER TABLE agent_chat_turn_job ADD COLUMN failure_class_json TEXT;
ALTER TABLE agent_chat_turn_job ADD COLUMN retry_decision TEXT;
ALTER TABLE agent_chat_turn_job ADD COLUMN pre_provider_failure_count INTEGER NOT NULL DEFAULT 0 CHECK (pre_provider_failure_count >= 0);
-- Accounting invocation identity must advance even when retry budget is refunded.
ALTER TABLE agent_chat_turn_job ADD COLUMN invocation_count INTEGER NOT NULL DEFAULT 0 CHECK (invocation_count >= 0);
UPDATE agent_chat_turn_job SET invocation_count = MAX(attempt_count, COALESCE((
    SELECT MAX(attempt_ordinal) + 1 FROM usage_invocation
    WHERE source_id = agent_chat_turn_job.id
), 0));
ALTER TABLE agent_chat_turn_job ADD COLUMN usage_limit_deferral_count INTEGER NOT NULL DEFAULT 0 CHECK (usage_limit_deferral_count >= 0);
ALTER TABLE agent_chat_turn_job ADD COLUMN usage_limit_first_deferred_at TEXT;
