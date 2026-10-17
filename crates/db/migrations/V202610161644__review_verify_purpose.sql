-- Widen the constrained purpose vocabulary; preserve every consumer and delivery checkpoint.
-- SQLite validates cross-table trigger bodies during the rename.
DROP TRIGGER task_slot_wait_age_check_release;
CREATE TABLE check_consumer_rebuilt (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL REFERENCES repo(id) ON DELETE CASCADE,
    task_id TEXT REFERENCES task(id) ON DELETE SET NULL,
    status_epoch INTEGER NOT NULL CHECK(status_epoch >= 0),
    origin TEXT NOT NULL CHECK(origin IN ('entry','manual_review','conformance','before_work','lifecycle','environment','integration')),
    request_key TEXT NOT NULL UNIQUE,
    identity_key TEXT NOT NULL,
    run_id TEXT REFERENCES check_run(id) ON DELETE SET NULL,
    result_id TEXT REFERENCES check_result(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    purpose TEXT CHECK(purpose IS NULL OR purpose IN ('entry_ci','review_ci','review_verify','conformance','before_work','lifecycle','environment_preflight','readiness_probe','environment_helper','agent_selected','queue_head_ci')),
    delivery_step_id TEXT,
    cancelled_at TEXT,
    applied_at TEXT,
    infrastructure_retries INTEGER NOT NULL DEFAULT 0 CHECK(infrastructure_retries BETWEEN 0 AND 2)
);
INSERT INTO check_consumer_rebuilt (id,project_id,repo_id,task_id,status_epoch,origin,request_key,identity_key,run_id,result_id,created_at,purpose,delivery_step_id,cancelled_at,applied_at,infrastructure_retries) SELECT id,project_id,repo_id,task_id,status_epoch,origin,request_key,identity_key,run_id,result_id,created_at,purpose,delivery_step_id,cancelled_at,applied_at,infrastructure_retries FROM check_consumer;
DROP TABLE check_consumer;
ALTER TABLE check_consumer_rebuilt RENAME TO check_consumer;
CREATE INDEX check_consumer_task_epoch ON check_consumer(task_id,status_epoch);
CREATE INDEX check_consumer_run ON check_consumer(run_id);
CREATE INDEX check_consumer_result ON check_consumer(result_id);
CREATE INDEX check_consumer_undelivered ON check_consumer(created_at,id) WHERE result_id IS NOT NULL AND delivery_step_id IS NULL AND cancelled_at IS NULL AND applied_at IS NULL;
CREATE TRIGGER task_schedule_check_consumer_cancel AFTER UPDATE OF cancelled_at ON check_consumer
WHEN OLD.cancelled_at IS NULL AND NEW.cancelled_at IS NOT NULL BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT task_id,1 FROM task_schedule_wait WHERE daemon_id IN (COALESCE((SELECT machine_id FROM check_run WHERE id=NEW.run_id),''),'*') ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;

CREATE TRIGGER task_slot_wait_age_check_release AFTER UPDATE OF state ON check_run
WHEN OLD.admitted_at IS NOT NULL AND OLD.state IN ('running','cancelling','cleaning','uncertain')
 AND NEW.state NOT IN ('running','cancelling','cleaning','uncertain') BEGIN
 UPDATE task_slot_wait_age SET releases=releases+1 WHERE lapsed_at IS NULL
  AND task_id NOT IN (SELECT task_id FROM check_consumer WHERE run_id=OLD.id AND task_id IS NOT NULL);
END;
