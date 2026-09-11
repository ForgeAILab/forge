-- Representative legacy strict-project data for migration and lifecycle tests.
-- Apply after the repository's migrations have created the current schema.

INSERT INTO project (
    id,
    name,
    settings,
    workflow_definition,
    created_at,
    updated_at
) VALUES (
    'fixture-project-strict',
    'Legacy Strict Fixture',
    '{}',
    '{}',
    '2026-01-01T00:00:00Z',
    '2026-01-01T00:00:00Z'
);

INSERT INTO repo (
    id,
    project_id,
    name,
    remote_url,
    local_path,
    work_mode,
    default_branch,
    created_at,
    updated_at
) VALUES (
    'fixture-repo-strict',
    'fixture-project-strict',
    'forge-fixture',
    'https://example.com/forge-fixture.git',
    NULL,
    'direct_merge',
    'main',
    '2026-01-01T00:00:00Z',
    '2026-01-01T00:00:00Z'
);

UPDATE project
SET primary_repo_id = 'fixture-repo-strict'
WHERE id = 'fixture-project-strict';

INSERT INTO task (
    id,
    project_id,
    title,
    description,
    task_type,
    status,
    is_automation,
    priority,
    task_state_config,
    version,
    created_at,
    updated_at
) VALUES
    ('fixture-task-backlog', 'fixture-project-strict', 'Backlog task', 'Queued legacy work', 'task', 'backlog', 0, 0, '{}', 1, '2026-01-01T00:01:00Z', '2026-01-01T00:01:00Z'),
    ('fixture-task-planning', 'fixture-project-strict', 'Planning task', 'Awaiting plan approval', 'task', 'planning', 0, 1, '{}', 1, '2026-01-01T00:02:00Z', '2026-01-01T00:02:00Z'),
    ('fixture-task-active', 'fixture-project-strict', 'Active task', 'Being implemented', 'task', 'in_progress', 0, 2, '{}', 1, '2026-01-01T00:03:00Z', '2026-01-01T00:03:00Z'),
    ('fixture-task-review', 'fixture-project-strict', 'Review task', 'Awaiting review', 'task', 'review', 0, 2, '{}', 1, '2026-01-01T00:04:00Z', '2026-01-01T00:04:00Z'),
    ('fixture-task-merge-failed', 'fixture-project-strict', 'Merge failed task', 'Needs merge repair', 'task', 'merge_failed', 0, 2, '{}', 1, '2026-01-01T00:05:00Z', '2026-01-01T00:05:00Z'),
    ('fixture-task-done', 'fixture-project-strict', 'Done task', 'Completed legacy work', 'task', 'done', 0, 0, '{}', 1, '2026-01-01T00:06:00Z', '2026-01-01T00:06:00Z'),
    ('fixture-task-cancelled', 'fixture-project-strict', 'Cancelled task', 'Cancelled legacy work', 'task', 'cancelled', 0, 0, '{}', 1, '2026-01-01T00:07:00Z', '2026-01-01T00:07:00Z');
