-- `integration_attempt.predecessor_attempt_id` references `integration_attempt(id)`
-- with ON DELETE SET NULL and had no index: every deleted attempt row made
-- SQLite scan the whole attempt table for rows that name it. Attempt rows are
-- kept for good, so that table only grows; the start pass that deletes the
-- `shadow:` rows of deleted Tasks and a Project deletion paid one full scan
-- per deleted row. Index only: no row is changed.
CREATE INDEX integration_attempt_predecessor ON integration_attempt(predecessor_attempt_id)
    WHERE predecessor_attempt_id IS NOT NULL;
