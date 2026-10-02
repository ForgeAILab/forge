WITH embedded AS (SELECT id FROM daemon WHERE machine_id = ?),
runs AS (
    SELECT CASE WHEN e.workspace_id IS NOT NULL
           THEN COALESCE(p.execution_daemon_id, p.daemon_id)
           ELSE COALESCE(CASE WHEN json_valid(e.executor_config_snapshot_json)
                THEN json_extract(e.executor_config_snapshot_json, '$.daemon_id') END, a.daemon_id)
           END AS daemon_id, 1 AS running_executions, 0 AS reservations, 0 AS active_chat_turns
    FROM execution e
    LEFT JOIN workspace_placement p ON p.workspace_id = e.workspace_id
    LEFT JOIN agent_current a ON a.id = e.agent_id
    WHERE e.status = 'running'
    UNION ALL
    SELECT COALESCE(p.execution_daemon_id, p.daemon_id), 0, 1, 0
    FROM workspace_placement p
    WHERE (p.state IN ('reserved', 'preparing') OR (p.state = 'ready' AND p.reserved_until IS NOT NULL))
      AND julianday(COALESCE(p.reserved_until, datetime(p.updated_at, '+10 minutes'))) > julianday('now')
      AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.workspace_id = p.workspace_id AND e.status = 'running')
    UNION ALL
    SELECT a.daemon_id, 0, 0, 1 FROM agent_chat_turn_job j
    JOIN agent_current a ON a.id = j.responder_identity_id
    WHERE j.status IN ('leased', 'running')
), occupancy AS (
    SELECT CASE WHEN daemon_id IS NULL OR daemon_id IN (SELECT id FROM embedded)
           THEN 'server_host' ELSE daemon_id END AS machine_key,
           SUM(running_executions) AS running_executions,
           SUM(reservations) AS reservations, SUM(active_chat_turns) AS active_chat_turns
    FROM runs GROUP BY machine_key
)
