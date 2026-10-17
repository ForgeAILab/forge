-- Passive activation storage for the integration queue (3.2 stage D1a).
-- Additive only: existing rows keep every value and read both columns as NULL.
-- No logic triggers; nothing writes these columns until the queue worker (D2).
ALTER TABLE integration_attempt ADD COLUMN cancel_requested_at TEXT;
ALTER TABLE integration_attempt ADD COLUMN phase_timings_json TEXT
    CHECK(phase_timings_json IS NULL OR (json_valid(phase_timings_json) AND json_type(phase_timings_json) = 'object' AND length(CAST(phase_timings_json AS BLOB)) <= 16384));
-- Cross-queue sweeps. The members index leads with queue_id, so it cannot
-- answer "which attempts anywhere are due / asked to cancel".
CREATE INDEX integration_attempt_timed ON integration_attempt(state,id) WHERE current=1 AND available_at IS NOT NULL;
CREATE INDEX integration_attempt_cancel_requested ON integration_attempt(id) WHERE current=1 AND cancel_requested_at IS NOT NULL;
-- Retention candidates only: a pruned row leaves this index, so each prune
-- call reads what it can still delete instead of every old terminal attempt.
CREATE INDEX integration_attempt_prunable ON integration_attempt(completed_at,id) WHERE completed_at IS NOT NULL AND (effect_receipts_json<>'[]' OR operation_receipts_json<>'[]' OR observations_json<>'[]');
