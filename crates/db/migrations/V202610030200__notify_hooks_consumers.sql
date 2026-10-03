-- Cut over at the upgrade boundary, preserving every existing event and cursor.
INSERT INTO event_consumer_cursor (consumer_name, last_sequence, version, updated_at)
SELECT 'project-hooks', COALESCE(MAX(sequence), 0), 1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now') FROM domain_event WHERE 1
ON CONFLICT(consumer_name) DO NOTHING;
INSERT INTO event_consumer_cursor (consumer_name, last_sequence, version, updated_at)
SELECT 'notifications', COALESCE(MAX(sequence), 0), 1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now') FROM domain_event WHERE 1
ON CONFLICT(consumer_name) DO NOTHING;

-- These mutations previously had only live bus hints. Capture immutable
-- delivery requests in their writer transaction, including recovery writers.
CREATE TRIGGER project_hooks_task_created AFTER INSERT ON task

BEGIN
    INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type,
        scope_type, scope_id, correlation_id, causation_depth, payload_json, created_at)
    VALUES (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || substr('89ab', (random() & 3) + 1, 1) || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))), 'project_hook.task_created', 'task', NEW.id, 'system',
        'task', NEW.id, NEW.id, 0, json_object('project_id', NEW.project_id, 'title', NEW.title), NEW.updated_at);
END;

CREATE TRIGGER project_hooks_task_archived AFTER UPDATE OF archived_at ON task
WHEN OLD.archived_at IS NULL AND NEW.archived_at IS NOT NULL
BEGIN
    INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type,
        scope_type, scope_id, correlation_id, causation_depth, payload_json, created_at)
    VALUES (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || substr('89ab', (random() & 3) + 1, 1) || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))), 'project_hook.task_archived', 'task', NEW.id, 'system',
        'task', NEW.id, NEW.id, 0, json_object('project_id', NEW.project_id), NEW.updated_at);
END;

CREATE TRIGGER notifications_blocked_json AFTER UPDATE OF blocked_json ON task
WHEN json_valid(NEW.blocked_json) AND NEW.blocked_json IS NOT NULL AND NEW.blocked_json IS NOT OLD.blocked_json
    AND COALESCE(json_extract(NEW.blocked_json, '$.kind'), '') <> 'manual_stop'
    -- Review CI availability/reset annotations had Attention, not inbox notifications.
    AND NOT (
        CASE WHEN json_valid(NEW.entry_barrier_json) THEN json_extract(NEW.entry_barrier_json, '$.status') ELSE '' END = 'blocked'
        AND CASE WHEN json_valid(NEW.error_annotation) THEN json_extract(NEW.error_annotation, '$.blocking_reason') ELSE '' END
            IN ('review_ci_infrastructure', 'review_ci_infrastructure_exhausted', 'review_ci_unavailable', 'workspace_reset_required')
    )
    -- Restoring a refused queued action updates its saved condition without
    -- publishing an old task.blocked/task.failed outcome.
    AND NOT (
        CASE WHEN json_valid(OLD.metadata_json) THEN json_type(OLD.metadata_json, '$.queued_recovery') IS NOT NULL ELSE 0 END
        AND CASE WHEN json_valid(NEW.metadata_json) THEN json_type(NEW.metadata_json, '$.queued_recovery') IS NULL ELSE 1 END
    )
BEGIN
    INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type,
        scope_type, scope_id, correlation_id, causation_depth, payload_json, created_at)
    VALUES (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || substr('89ab', (random() & 3) + 1, 1) || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))), 'notification.requested', 'task', NEW.id, 'system',
        'task', NEW.id, NEW.id, 0, json_object('project_id', NEW.project_id, 'task_id', NEW.id, 'event_type', 'task.blocked', 'title', NEW.title, 'body', json_extract(NEW.blocked_json, '$.reason')), NEW.updated_at);
END;

CREATE TRIGGER notifications_failed_json AFTER UPDATE OF failed_json ON task
WHEN json_valid(NEW.failed_json) AND NEW.failed_json IS NOT NULL AND NEW.failed_json IS NOT OLD.failed_json
    -- Restoring a refused queued action updates its saved condition without
    -- publishing an old task.blocked/task.failed outcome.
    AND NOT (
        CASE WHEN json_valid(OLD.metadata_json) THEN json_type(OLD.metadata_json, '$.queued_recovery') IS NOT NULL ELSE 0 END
        AND CASE WHEN json_valid(NEW.metadata_json) THEN json_type(NEW.metadata_json, '$.queued_recovery') IS NULL ELSE 1 END
    )
BEGIN
    INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type,
        scope_type, scope_id, correlation_id, causation_depth, payload_json, created_at)
    VALUES (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || substr('89ab', (random() & 3) + 1, 1) || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))), 'notification.requested', 'task', NEW.id, 'system',
        'task', NEW.id, NEW.id, 0, json_object('project_id', NEW.project_id, 'task_id', NEW.id, 'event_type', 'task.failed', 'title', NEW.title, 'body', json_extract(NEW.failed_json, '$.reason')), NEW.updated_at);
END;

CREATE TRIGGER notifications_task_recovered AFTER UPDATE OF error_annotation ON task
WHEN json_valid(NEW.error_annotation) AND NEW.error_annotation IS NOT OLD.error_annotation AND json_extract(NEW.error_annotation, '$.type') = 'recovery_required' AND json_extract(NEW.error_annotation, '$.blocking_reason') IN ('crash_recovery', 'agent_timeout')
    -- Restoring a refused queued action updates its saved condition without
    -- publishing an old task.blocked/task.failed outcome.
    AND NOT (
        CASE WHEN json_valid(OLD.metadata_json) THEN json_type(OLD.metadata_json, '$.queued_recovery') IS NOT NULL ELSE 0 END
        AND CASE WHEN json_valid(NEW.metadata_json) THEN json_type(NEW.metadata_json, '$.queued_recovery') IS NULL ELSE 1 END
    )
BEGIN
    INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type,
        scope_type, scope_id, correlation_id, causation_depth, payload_json, created_at)
    VALUES (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || substr('89ab', (random() & 3) + 1, 1) || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))), 'notification.requested', 'task', NEW.id, 'system',
        'task', NEW.id, NEW.id, 0, json_object('project_id', NEW.project_id, 'task_id', NEW.id, 'event_type', 'task.recovery_required', 'title', NEW.title, 'body', CASE json_extract(NEW.error_annotation, '$.blocking_reason') WHEN 'crash_recovery' THEN 'Needs manual recovery after a server restart' ELSE 'Needs manual recovery after an agent heartbeat timeout' END), NEW.updated_at);
END;

CREATE TRIGGER notifications_merge_failed AFTER UPDATE OF error_annotation ON task
WHEN json_valid(NEW.error_annotation) AND NEW.error_annotation IS NOT OLD.error_annotation AND json_extract(NEW.error_annotation, '$.type') IN ('merge_conflict', 'target_repo_dirty') AND json_type(NEW.error_annotation, '$.detected_at') IS NOT NULL
    -- Restoring a refused queued action updates its saved condition without
    -- publishing an old task.blocked/task.failed outcome.
    AND NOT (
        CASE WHEN json_valid(OLD.metadata_json) THEN json_type(OLD.metadata_json, '$.queued_recovery') IS NOT NULL ELSE 0 END
        AND CASE WHEN json_valid(NEW.metadata_json) THEN json_type(NEW.metadata_json, '$.queued_recovery') IS NULL ELSE 1 END
    )
BEGIN
    INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type,
        scope_type, scope_id, correlation_id, causation_depth, payload_json, created_at)
    VALUES (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || substr('89ab', (random() & 3) + 1, 1) || lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))), 'notification.requested', 'task', NEW.id, 'system',
        'task', NEW.id, NEW.id, 0, json_object('project_id', NEW.project_id, 'task_id', NEW.id, 'event_type', 'merge.failed', 'title', NEW.title, 'body', json_extract(NEW.error_annotation, '$.message')), NEW.updated_at);
END;


-- Only active started runs are inspected by the bounded reconciliation tick.
CREATE INDEX idx_project_hook_run_running_updated
    ON project_hook_run(updated_at, id) WHERE status = 'running' AND completed_at IS NULL;
