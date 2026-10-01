-- A digest of canonical snapshot state, excluding its save timestamp.
-- Existing encrypted rows are read as-is and converted on the next change.
ALTER TABLE protected_agent_session_state ADD COLUMN snapshot_digest TEXT;
-- Standalone snapshot saves must not relabel an older checkpoint's LCM state.
ALTER TABLE protected_agent_session_state ADD COLUMN checkpoint_lcm_policy_revision TEXT;
