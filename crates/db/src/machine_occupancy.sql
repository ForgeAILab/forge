WITH embedded AS (SELECT id FROM daemon WHERE machine_id = ?),
base_jobs AS (
    SELECT e.task_id, CASE WHEN e.workspace_id IS NOT NULL
           THEN COALESCE(p.execution_daemon_id, p.daemon_id)
           ELSE COALESCE(CASE WHEN json_valid(e.executor_config_snapshot_json)
                THEN json_extract(e.executor_config_snapshot_json, '$.daemon_id') END, a.daemon_id)
           END AS daemon_id, 1 AS running_executions, 0 AS reservations, 0 AS active_chat_turns
    FROM execution e
    LEFT JOIN workspace_placement p ON p.workspace_id = e.workspace_id
    LEFT JOIN agent_current a ON a.id = e.agent_id
    WHERE e.status = 'running'
    UNION ALL
    SELECT p.task_id, COALESCE(p.execution_daemon_id, p.daemon_id), 0, 1, 0
    FROM workspace_placement p
    WHERE p.state IN ('reserved', 'preparing')
      AND julianday(COALESCE(p.reserved_until, datetime(p.updated_at, '+10 minutes'))) > julianday('now')
      AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.workspace_id = p.workspace_id AND e.status = 'running')
    UNION ALL
    SELECT NULL, a.daemon_id, 0, 0, 1 FROM agent_chat_turn_job j
    JOIN agent_current a ON a.id = j.responder_identity_id
    WHERE j.status IN ('leased', 'running')
), physical_jobs AS (
    SELECT *, CASE WHEN daemon_id IS NULL OR daemon_id IN (SELECT id FROM embedded)
        THEN 'server_host' ELSE daemon_id END AS machine_key FROM base_jobs
), check_jobs AS (
    SELECT r.id, CASE WHEN r.machine_id IS NULL OR r.machine_id IN (SELECT id FROM embedded)
        THEN 'server_host' ELSE r.machine_id END AS machine_key,
        EXISTS(SELECT 1 FROM physical_jobs p JOIN check_consumer c ON c.task_id=p.task_id
            JOIN task t ON t.id=c.task_id AND t.status_epoch=c.status_epoch AND t.deleted_at IS NULL
            WHERE c.run_id=r.id AND c.cancelled_at IS NULL AND (p.running_executions=1 OR p.reservations=1)
            AND p.machine_key=CASE WHEN r.machine_id IS NULL OR r.machine_id IN (SELECT id FROM embedded)
                THEN 'server_host' ELSE r.machine_id END) AS borrowed
    FROM check_run r WHERE r.admitted_at IS NOT NULL AND r.state IN ('running','cancelling','cleaning','uncertain')
), check_queue AS (
    -- Queued checks somebody still waits for, in admission order: review
    -- entry, then integration head, then the rest; oldest first within each.
    -- A consumer whose Task was deleted or left the status it asked from
    -- waits for nothing (the check worker cancels it when it next looks at
    -- the run): its run reserves no slot in the meantime.
    SELECT r.id, r.created_at, CASE WHEN r.machine_id IS NULL OR r.machine_id IN (SELECT id FROM embedded)
        THEN 'server_host' ELSE r.machine_id END AS machine_key,
        (SELECT MIN(CASE c.origin WHEN 'entry' THEN 0 WHEN 'integration' THEN 1 ELSE 2 END)
         FROM check_consumer c WHERE c.run_id=r.id AND c.cancelled_at IS NULL
           AND EXISTS(SELECT 1 FROM task t WHERE t.id=c.task_id AND t.status_epoch=c.status_epoch
                      AND t.deleted_at IS NULL)) AS queue_rank
    FROM check_run r WHERE r.state='queued'
), runs AS (
    SELECT machine_key, running_executions, reservations, active_chat_turns, 0 AS check_runs, 0 AS borrowed_check_runs, 0 AS queued_checks FROM physical_jobs
    UNION ALL SELECT machine_key, 0, 0, 0, CASE WHEN borrowed THEN 0 ELSE 1 END, borrowed, 0 FROM check_jobs
    UNION ALL SELECT machine_key, 0, 0, 0, 0, 0, 1 FROM check_queue WHERE queue_rank IS NOT NULL
), occupancy AS (
    SELECT machine_key, SUM(running_executions) AS running_executions,
           SUM(reservations) AS reservations, SUM(active_chat_turns) AS active_chat_turns,
           SUM(check_runs) AS check_runs, SUM(borrowed_check_runs) AS borrowed_check_runs,
           SUM(queued_checks) AS queued_checks
    FROM runs GROUP BY machine_key
)
