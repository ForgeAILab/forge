-- Attention learned to wake a Project Agent when a Task reaches `done` after
-- older consumers may already have checkpointed those transition events.
-- Append one new event per Project that still needs reconciliation so the
-- normal durable projection path can create the incident and admit the wake.
WITH terminal_done AS (
    SELECT
        done.id,
        done.entity_id,
        done.correlation_id,
        done.causation_depth,
        task.project_id,
        ROW_NUMBER() OVER (
            PARTITION BY task.project_id
            ORDER BY done.sequence DESC
        ) AS project_rank
    FROM domain_event AS done
    JOIN task
      ON task.id = done.entity_id
     AND task.project_id IS NOT NULL
     AND task.status = 'done'
    WHERE done.event_type = 'task.transitioned'
      AND CASE
            WHEN json_valid(done.payload_json)
            THEN json_extract(done.payload_json, '$.to_state')
          END = 'done'
      AND NOT EXISTS (
          SELECT 1
          FROM domain_event AS readiness
          WHERE readiness.event_type = 'milestone.readiness.evaluated'
            AND readiness.scope_type = 'project'
            AND readiness.scope_id = task.project_id
            AND readiness.sequence > done.sequence
      )
)
INSERT INTO domain_event (
    id, event_type, entity_type, entity_id, actor_type, actor_id,
    scope_type, scope_id, correlation_id, causation_id, causation_depth,
    dedupe_key, payload_json, created_at
)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
        substr('89ab', 1 + (abs(random()) % 4), 1) ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
        lower(hex(randomblob(6))),
    'task.completed',
    'task',
    terminal_done.entity_id,
    'system',
    'V093__delivery_followup_backfill',
    'task',
    terminal_done.entity_id,
    terminal_done.correlation_id,
    terminal_done.id,
    CASE
        WHEN terminal_done.causation_depth < 16
        THEN terminal_done.causation_depth + 1
        ELSE 16
    END,
    'migration:delivery-followup:v1:' || terminal_done.project_id || ':' || terminal_done.id,
    json_object(
        'backfill_version', 1,
        'source_event_id', terminal_done.id,
        'to_state', 'done'
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
FROM terminal_done
WHERE terminal_done.project_rank = 1;
