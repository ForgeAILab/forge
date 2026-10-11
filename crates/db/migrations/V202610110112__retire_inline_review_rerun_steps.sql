-- The inline review re-run was retired (a manual review re-run now re-enters
-- the durable review entry). It reserved its review attempt and its reviewer
-- execution through one queued Task mutation, `ReviewCreateAttemptWithExecution`.
-- That operation no longer exists, and whoever queued such a step was the old
-- server process, which is gone and waits for no reply. A step of that kind
-- still queued at upgrade is closed here as superseded, so the Task's queue
-- does not stall on a payload nothing can apply. The row and its payload are
-- kept; no Review, execution or Task row is touched.
UPDATE task_step
SET status = 'superseded',
    last_error = 'the inline review re-run was retired; re-run the review to run its entry again',
    claimed_by = NULL,
    lease_until = NULL,
    completed_at = strftime('%Y-%m-%dT%H:%M:%SZ','now'),
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
WHERE kind = 'mutation'
  AND status IN ('pending','claimed','suspended','parked')
  AND json_valid(payload_json)
  AND json_type(payload_json, '$.ReviewCreateAttemptWithExecution') IS NOT NULL;
