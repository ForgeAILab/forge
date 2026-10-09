-- Durable delivery checkpoint survives Task-step retention. No historical
-- check is certified or scheduled by this migration.
ALTER TABLE check_consumer ADD COLUMN delivery_step_id TEXT;
CREATE INDEX check_consumer_undelivered ON check_consumer(result_id)
    WHERE result_id IS NOT NULL AND delivery_step_id IS NULL;
ALTER TABLE check_consumer ADD COLUMN cancelled_at TEXT;
ALTER TABLE check_run ADD COLUMN dispatch_json TEXT CHECK(dispatch_json IS NULL OR (json_valid(dispatch_json) AND length(CAST(dispatch_json AS BLOB)) <= 131072));
ALTER TABLE check_run ADD COLUMN owner_receipt_json TEXT CHECK(owner_receipt_json IS NULL OR (json_valid(owner_receipt_json) AND length(CAST(owner_receipt_json AS BLOB)) <= 4194304));
ALTER TABLE check_run ADD COLUMN admitted_at TEXT;
ALTER TABLE check_run ADD COLUMN deadline_at TEXT;
ALTER TABLE check_run ADD COLUMN infrastructure_attempt INTEGER NOT NULL DEFAULT 0 CHECK(infrastructure_attempt BETWEEN 0 AND 2);
ALTER TABLE check_run ADD COLUMN acknowledged_at TEXT;
ALTER TABLE check_consumer ADD COLUMN infrastructure_retries INTEGER NOT NULL DEFAULT 0 CHECK(infrastructure_retries BETWEEN 0 AND 2);
ALTER TABLE check_run ADD COLUMN capacity_wait_since TEXT;
