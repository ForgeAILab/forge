-- 3.3 stage A follow-up: run identity, purpose, applied timeout and the
-- no-cleanup pass. Stage A (V202610080851) was never deployed and nothing
-- writes these tables yet, so no check_run, check_result or check_consumer row
-- exists anywhere: the execution digest moves to forge.check-execution/2 with
-- no digest to convert. Every statement below still carries existing rows.

-- The whole-run wall limit a run executes under. It left the digest, so
-- changing the setting neither splits nor invalidates an identity.
ALTER TABLE check_run ADD COLUMN applied_timeout_seconds INTEGER
    CHECK(applied_timeout_seconds IS NULL OR applied_timeout_seconds > 0);

-- Purpose left the digest too: it is a fact about who asked, not what ran.
ALTER TABLE check_consumer ADD COLUMN purpose TEXT
    CHECK(purpose IS NULL OR purpose IN ('entry_ci','review_ci','conformance','before_work','lifecycle','environment_preflight','readiness_probe','environment_helper','agent_selected','queue_head_ci'));

-- A pass whose spec declares no cleanup step is certified with cleanup
-- 'not_performed'. SQLite cannot alter a CHECK, so check_result is rebuilt.
-- Dropping the old table would null check_consumer.result_id (ON DELETE SET
-- NULL), so those links are carried across the rebuild.
CREATE TABLE check_result_rebuilt (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    identity_key TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('pass','fail','timed_out','cancelled','infrastructure_failed')),
    cleanup TEXT NOT NULL CHECK(cleanup IN ('success','failed','uncertain','not_performed')),
    certified INTEGER NOT NULL CHECK(certified IN (0,1)),
    cacheable INTEGER NOT NULL CHECK(cacheable IN (0,1)),
    steps_json TEXT NOT NULL CHECK(json_valid(steps_json) AND length(CAST(steps_json AS BLOB)) <= 262144),
    output_truncated INTEGER NOT NULL CHECK(output_truncated IN (0,1)),
    created_at TEXT NOT NULL,
    FOREIGN KEY(run_id, identity_key) REFERENCES check_run(id, identity_key) ON DELETE CASCADE,
    CHECK(certified = 0 OR (outcome = 'pass' AND cleanup <> 'failed' AND cleanup <> 'uncertain'))
);
INSERT INTO check_result_rebuilt(id,run_id,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at)
    SELECT id,run_id,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at FROM check_result;
CREATE TABLE check_consumer_result_carry AS
    SELECT id, result_id FROM check_consumer WHERE result_id IS NOT NULL;
DROP TABLE check_result;
ALTER TABLE check_result_rebuilt RENAME TO check_result;
UPDATE check_consumer
    SET result_id = (SELECT carry.result_id FROM check_consumer_result_carry carry WHERE carry.id = check_consumer.id)
    WHERE id IN (SELECT id FROM check_consumer_result_carry);
DROP TABLE check_consumer_result_carry;
CREATE INDEX check_result_run ON check_result(run_id, created_at);
CREATE UNIQUE INDEX check_result_reusable_identity ON check_result(identity_key)
    WHERE outcome = 'pass' AND certified = 1 AND cacheable = 1;
