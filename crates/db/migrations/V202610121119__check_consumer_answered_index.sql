-- A consumer can be created already answered: the integration head's reuse of
-- a certified result is applied by the `settle` step that records it, so it
-- has `applied_at` and never a delivery step. The idle delivery probe reads
-- only this partial index; such a row must not sit in it for the whole
-- retention period. Index only: no row is changed.
DROP INDEX check_consumer_undelivered;
CREATE INDEX check_consumer_undelivered ON check_consumer(created_at, id)
    WHERE result_id IS NOT NULL AND delivery_step_id IS NULL AND cancelled_at IS NULL AND applied_at IS NULL;
