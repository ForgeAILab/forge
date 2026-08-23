-- Delivery follow-up turns admitted before the postcondition guard could
-- finish successfully from prose while leaving their Attention incidents
-- open. Append one new event per unreconciled Project so the ordinary durable
-- Attention/wake path re-admits the bound Project Agent after upgrade.
WITH open_projects AS (
    SELECT
        scope_id AS project_id,
        MAX(source_sequence) AS incident_sequence
    FROM attention_projection
    WHERE attention_type = 'delivery_followup'
      AND scope_type = 'project'
      AND status <> 'resolved'
    GROUP BY scope_id
),
latest_delivery AS (
    SELECT
        delivered.id,
        delivered.entity_id AS task_id,
        delivered.correlation_id,
        delivered.causation_depth,
        open_projects.project_id,
        ROW_NUMBER() OVER (
            PARTITION BY open_projects.project_id
            ORDER BY delivered.sequence DESC, delivered.id DESC
        ) AS project_rank
    FROM open_projects
    JOIN task
      ON task.project_id = open_projects.project_id
     AND task.status = 'done'
    JOIN domain_event AS delivered
      ON delivered.entity_id = task.id
     AND delivered.entity_type = 'task'
     AND (
          delivered.event_type = 'task.completed'
          OR (
              delivered.event_type = 'task.transitioned'
              AND CASE
                    WHEN json_valid(delivered.payload_json)
                    THEN json_extract(delivered.payload_json, '$.to_state')
                  END = 'done'
          )
     )
    WHERE NOT EXISTS (
        SELECT 1
        FROM domain_event AS readiness
        WHERE readiness.event_type = 'milestone.readiness.evaluated'
          AND readiness.scope_type = 'project'
          AND readiness.scope_id = open_projects.project_id
          AND readiness.sequence > open_projects.incident_sequence
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
    latest_delivery.task_id,
    'system',
    'V094__delivery_followup_postcondition_recovery',
    'task',
    latest_delivery.task_id,
    latest_delivery.correlation_id,
    latest_delivery.id,
    CASE
        WHEN latest_delivery.causation_depth < 16
        THEN latest_delivery.causation_depth + 1
        ELSE 16
    END,
    'migration:delivery-followup-postcondition:v1:' ||
        latest_delivery.project_id || ':' || latest_delivery.id,
    json_object(
        'backfill_version', 2,
        'reason', 'delivery_followup_postcondition_recovery',
        'source_event_id', latest_delivery.id,
        'to_state', 'done'
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
FROM latest_delivery
WHERE latest_delivery.project_rank = 1;
