-- Preserve accepted pending recoveries while changing their wire vocabulary.
-- Stored annotation allowlists are left intact and ignored by typed readers.
UPDATE task
SET metadata_json = json_set(metadata_json,
    '$.task_action_migrated_intent', json_extract(metadata_json, '$.queued_recovery'),
    '$.queued_recovery.request', json_object(
        'action', json(CASE json_extract(metadata_json, '$.queued_recovery.request.action')
            WHEN 'reset_to_initial' THEN '{"verb":"restart"}'
            WHEN 'cancel_task' THEN '{"verb":"cancel"}'
            WHEN 'mark_reviewed' THEN '{"verb":"approve","override":true}'
            WHEN 'defer_to_follow_up' THEN '{"verb":"approve","override":false}'
            WHEN 'skip_hook_once' THEN '{"verb":"approve","override":true}'
            WHEN 'reset_retry_window' THEN '{"verb":"retry","reset_budget":true}'
            WHEN 'proceed_once' THEN '{"verb":"retry","reset_budget":false}'
            WHEN 'resume_session' THEN '{"verb":"retry","fresh_session":false}'
            WHEN 'reexecute' THEN '{"verb":"retry","fresh_session":true}'
            WHEN 'update_workspace_and_retry_hook' THEN '{"verb":"retry","refresh_workspace":true}'
            ELSE '{"verb":"retry"}' END),
        'offer', json_object(
            'action', json('{"verb":"retry"}'), 'parameters', json('[]'),
            'authority', json('["owner"]'), 'label', 'Queued Task action',
            'target_execution_id', json_extract(json_extract(metadata_json, '$.queued_recovery.error_annotation'), '$.blocked_execution_id'),
            'reason', CASE json_extract(metadata_json, '$.queued_recovery.request.action')
                WHEN 'reset_to_initial' THEN 'interrupted_restart'
                WHEN 'cancel_task' THEN 'cancellable'
                WHEN 'mark_reviewed' THEN 'failed_review_override'
                WHEN 'defer_to_follow_up' THEN 'review_needs_owner'
                WHEN 'skip_hook_once' THEN 'entry_barrier_override'
                WHEN 'reset_retry_window' THEN 'retry_budget_exhausted'
                WHEN 'proceed_once' THEN 'retry_budget_exhausted'
                WHEN 'resume_process' THEN 'review_owner_retry'
                WHEN 'retry_hook' THEN CASE
                    WHEN entry_barrier_json IS NOT NULL THEN 'entry_barrier_blocked'
                    WHEN status = 'merging' THEN 'merge_gate_retry'
                    WHEN status = 'merge_failed' THEN 'merge_fix_retry'
                    WHEN status = 'review' THEN 'review_failed'
                    ELSE 'role_retry' END
                WHEN 'update_workspace_and_retry_hook' THEN 'entry_barrier_blocked'
                ELSE 'role_retry' END),
        'actor', json('{"User":{"user_id":null,"source":"Api"}}'),
        'agent_id', COALESCE(json_extract(metadata_json, '$.queued_recovery.request.agent_id'), assignee_id,
            (SELECT agent_id FROM execution WHERE task_id = task.id AND agent_id IS NOT NULL ORDER BY created_at DESC, id DESC LIMIT 1))))
WHERE json_valid(metadata_json)
  AND json_type(metadata_json, '$.queued_recovery') = 'object'
  AND json_type(metadata_json, '$.queued_recovery.request.offer') IS NULL
  AND COALESCE(json_extract(metadata_json, '$.queued_recovery.request.action'), '') != 'open_interactive';

-- A queued side-session launch is not a Task condition command. Retain its
-- complete intent as session evidence and restore its former condition.
UPDATE task
SET error_annotation = CASE WHEN EXISTS(SELECT 1 FROM execution WHERE task_id = task.id AND status = 'running') THEN error_annotation ELSE COALESCE(error_annotation, json_extract(metadata_json, '$.queued_recovery.error_annotation')) END,
    blocked_json = CASE WHEN EXISTS(SELECT 1 FROM execution WHERE task_id = task.id AND status = 'running') THEN blocked_json ELSE COALESCE(blocked_json, json_extract(metadata_json, '$.queued_recovery.blocked_json')) END,
    metadata_json = json_remove(json_set(metadata_json, '$.pending_session_launch', json_extract(metadata_json, '$.queued_recovery')), '$.queued_recovery', '$.deferred_dispatch')
WHERE json_valid(metadata_json)
  AND json_extract(metadata_json, '$.queued_recovery.request.action') = 'open_interactive';
