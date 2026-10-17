-- Remove lifecycle hook registrations for the retired knowledge plugins.
-- json_remove preserves every surviving value and key order, but SQLite
-- normalizes insignificant JSON whitespace in rows that it changes.
WITH RECURSIVE
json_nodes AS (
    SELECT project.id AS project_id,
           tree.id AS node_id,
           tree.parent AS parent_id,
           tree.key,
           tree.type,
           tree.value,
           tree.fullkey
    FROM project
    JOIN json_tree(
        CASE WHEN json_valid(project.settings) THEN project.settings ELSE '{}' END,
        '$.lifecycle_hooks'
    ) AS tree
    WHERE json_valid(project.settings)
),
target_paths AS (
    SELECT hook.project_id,
           hook.fullkey AS path,
           ROW_NUMBER() OVER (
               PARTITION BY hook.project_id
               ORDER BY event.node_id, CAST(hook.key AS INTEGER) DESC
           ) AS ordinal
    FROM json_nodes AS hook
    JOIN json_nodes AS event
      ON event.project_id = hook.project_id
     AND event.node_id = hook.parent_id
     AND event.type = 'array'
    JOIN json_nodes AS lifecycle_hooks
      ON lifecycle_hooks.project_id = event.project_id
     AND lifecycle_hooks.node_id = event.parent_id
     AND lifecycle_hooks.type = 'object'
     AND lifecycle_hooks.fullkey = '$.lifecycle_hooks'
    WHERE hook.type = 'object'
      AND json_extract(hook.value, '$.type') = 'plugin'
      AND json_extract(hook.value, '$.name') IN (
          'knowledge-inject',
          'knowledge-capture'
      )
),
cleaned(project_id, ordinal, settings) AS (
    SELECT project.id, 0, project.settings
    FROM project
    WHERE EXISTS (
        SELECT 1
        FROM target_paths
        WHERE target_paths.project_id = project.id
    )
    UNION ALL
    SELECT cleaned.project_id,
           target_paths.ordinal,
           json_remove(cleaned.settings, target_paths.path)
    FROM cleaned
    JOIN target_paths
      ON target_paths.project_id = cleaned.project_id
     AND target_paths.ordinal = cleaned.ordinal + 1
)
UPDATE project
SET settings = (
    SELECT cleaned.settings
    FROM cleaned
    WHERE cleaned.project_id = project.id
    ORDER BY cleaned.ordinal DESC
    LIMIT 1
)
WHERE EXISTS (
    SELECT 1
    FROM target_paths
    WHERE target_paths.project_id = project.id
);
