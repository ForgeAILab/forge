-- @15 teaches the Project Agent about the bounded task.cancel command. The
-- command is versioned, Project-scoped, and limited to non-terminal Tasks, so
-- it closes the healthy-Task control gap without granting a general workflow
-- transition capability.

INSERT INTO operating_skill_revision (
    id, operating_skill_id, skill_key, revision, schema_version, render_version,
    canonical_body, policy_json, policy_digest, content_digest,
    created_by_type, created_at
)
SELECT
    'forge.project.orchestration/v1@15', operating_skill_id, skill_key, 15,
    schema_version, render_version,
    replace(
        replace(
            canonical_body,
            'You may decide a Task workflow''s human-required review only through the typed `task.review` action.',
            'You may decide a Task workflow''s human-required review only through the typed `task.review` action, and cancel a non-terminal Task only through versioned `task.cancel`.'
        ),
        'reassign a role from eligible agents, cancel and replace a wedged Task within the adaptive envelope, including cancelling a verification-shaped Task and settling its checks yourself.',
        'reassign a role from eligible agents, cancel obsolete or wedged work with `task.cancel`, and replace incorrect work through the adaptive envelope, including cancelling a verification-shaped Task and settling its checks yourself.'
    ),
    policy_json, policy_digest,
    '1051cb539ec9d49959999890972fb74dc0a82d7d4aed08fd3bc60703b8f8c977',
    'system', strftime('%Y-%m-%dT%H:%M:%fZ','now')
FROM operating_skill_revision
WHERE id = 'forge.project.orchestration/v1@14';

UPDATE operating_skill
SET current_revision_id = 'forge.project.orchestration/v1@15',
    version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = 'forge.project.orchestration/v1'
  AND current_revision_id IS NOT 'forge.project.orchestration/v1@15';

UPDATE project_agent_binding
SET operating_skill_revision_id = 'forge.project.orchestration/v1@15'
WHERE operating_skill_revision_id = 'forge.project.orchestration/v1@14';
