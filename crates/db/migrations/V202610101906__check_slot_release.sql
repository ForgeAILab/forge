-- A check that stops holding or reserving a machine slot is a release for
-- the Tasks that wait for a run slot. Until now only the end of an
-- execution, a placement, a chat turn or an admission step marked those
-- waiters: a slot freed by a check run that ended, by a queued check that
-- was cancelled or expired, or by a consumer that stopped waiting marked
-- nobody. Additive: three triggers, no table or row is rewritten.
CREATE TRIGGER task_schedule_check_run_release AFTER UPDATE OF state ON check_run
WHEN OLD.state IN ('queued','running','cancelling','cleaning','uncertain')
 AND NEW.state NOT IN ('queued','running','cancelling','cleaning','uncertain') BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT task_id,1 FROM task_schedule_wait WHERE daemon_id IN (COALESCE(NEW.machine_id,''),'*') ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;
CREATE TRIGGER task_schedule_check_run_delete AFTER DELETE ON check_run
WHEN OLD.state IN ('queued','running','cancelling','cleaning','uncertain') BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT task_id,1 FROM task_schedule_wait WHERE daemon_id IN (COALESCE(OLD.machine_id,''),'*') ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;
-- A queued check nobody waits for any more reserves nothing.
CREATE TRIGGER task_schedule_check_consumer_cancel AFTER UPDATE OF cancelled_at ON check_consumer
WHEN OLD.cancelled_at IS NULL AND NEW.cancelled_at IS NOT NULL BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT task_id,1 FROM task_schedule_wait WHERE daemon_id IN (COALESCE((SELECT machine_id FROM check_run WHERE id=NEW.run_id),''),'*') ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;
-- Every wait for a run slot now carries a recheck deadline. A row written by
-- an earlier build has none: it is due at once, and the dispatcher that
-- reads it writes its own.
UPDATE task_schedule_wait SET deadline=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE daemon_id='*' AND deadline IS NULL;
