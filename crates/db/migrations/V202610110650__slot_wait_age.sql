-- Aging for run-slot waiters. A Task that needs a second slot on its machine
-- (an interactive session plus a role dispatch) is refused by rule 1 of slot
-- fairness at every release while checks keep queuing. Its wait now has an
-- age: when it began and how many capacity releases it was woken for. An
-- aged waiter is admitted ahead of checks queued after it began to wait
-- (`db::machine_capacity`). Additive: one table, one nullable column, six
-- triggers; no existing row is rewritten.
CREATE TABLE task_slot_wait_age (
    task_id TEXT PRIMARY KEY REFERENCES task(id) ON DELETE CASCADE,
    since TEXT NOT NULL,
    releases INTEGER NOT NULL DEFAULT 0,
    -- Set when the Task's wait row stops asking for a run slot. A dispatch
    -- attempt drops the wait row before its start transaction decides; the
    -- age survives that gap for 30 seconds and is otherwise started afresh.
    lapsed_at TEXT
);
INSERT INTO task_slot_wait_age(task_id,since)
SELECT task_id,strftime('%Y-%m-%dT%H:%M:%fZ','now') FROM task_schedule_wait WHERE daemon_id='*';
CREATE TRIGGER task_slot_wait_age_begin_insert AFTER INSERT ON task_schedule_wait
WHEN NEW.daemon_id='*' BEGIN
 INSERT INTO task_slot_wait_age(task_id,since) VALUES (NEW.task_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
 ON CONFLICT(task_id) DO UPDATE SET
  since=CASE WHEN lapsed_at IS NULL OR (julianday('now')-julianday(lapsed_at))*86400.0<30 THEN since ELSE excluded.since END,
  releases=CASE WHEN lapsed_at IS NULL OR (julianday('now')-julianday(lapsed_at))*86400.0<30 THEN releases ELSE 0 END,
  lapsed_at=NULL;
END;
CREATE TRIGGER task_slot_wait_age_begin_update AFTER UPDATE OF daemon_id ON task_schedule_wait
WHEN NEW.daemon_id='*' AND OLD.daemon_id IS NOT '*' BEGIN
 INSERT INTO task_slot_wait_age(task_id,since) VALUES (NEW.task_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
 ON CONFLICT(task_id) DO UPDATE SET
  since=CASE WHEN lapsed_at IS NULL OR (julianday('now')-julianday(lapsed_at))*86400.0<30 THEN since ELSE excluded.since END,
  releases=CASE WHEN lapsed_at IS NULL OR (julianday('now')-julianday(lapsed_at))*86400.0<30 THEN releases ELSE 0 END,
  lapsed_at=NULL;
END;
CREATE TRIGGER task_slot_wait_age_lapse_update AFTER UPDATE OF daemon_id ON task_schedule_wait
WHEN OLD.daemon_id='*' AND NEW.daemon_id IS NOT '*' BEGIN
 UPDATE task_slot_wait_age SET lapsed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE task_id=OLD.task_id AND lapsed_at IS NULL;
END;
CREATE TRIGGER task_slot_wait_age_lapse_delete AFTER DELETE ON task_schedule_wait
WHEN OLD.daemon_id='*' BEGIN
 UPDATE task_slot_wait_age SET lapsed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE task_id=OLD.task_id AND lapsed_at IS NULL;
END;
-- Every capacity release marks its waiters dirty (`external`); that mark is
-- the count. A mark for another reason counts too: the age only errs towards
-- the waiter, and the order it buys is by time, never ahead of older checks.
CREATE TRIGGER task_slot_wait_age_release_insert AFTER INSERT ON task_schedule_dirty
WHEN NEW.external=1 BEGIN
 UPDATE task_slot_wait_age SET releases=releases+1 WHERE task_id=NEW.task_id AND lapsed_at IS NULL;
END;
CREATE TRIGGER task_slot_wait_age_release_update AFTER UPDATE OF generation ON task_schedule_dirty
WHEN NEW.external=1 AND NEW.generation<>OLD.generation BEGIN
 UPDATE task_slot_wait_age SET releases=releases+1 WHERE task_id=NEW.task_id AND lapsed_at IS NULL;
END;
-- The wait is over once the Task starts a run.
CREATE TRIGGER task_slot_wait_age_end AFTER INSERT ON execution
WHEN NEW.status='running' BEGIN
 DELETE FROM task_slot_wait_age WHERE task_id=NEW.task_id;
END;
-- When a queued check first stood back for an aged waiter. It does so for at
-- most 90 seconds in total, so a waiter that cannot use the slot after all
-- never starves the check.
ALTER TABLE check_run ADD COLUMN yielded_since TEXT;
