-- @16 makes Task coordination hierarchy explicit instead of conflating it
-- with execution prerequisites.  A parent_task_id is one-level parentage;
-- dependency edges remain gates only.  Preserve the prior resident contract
-- and derive this revision from @15 so every admitted historical revision
-- remains immutable.

INSERT INTO operating_skill_revision (
    id, operating_skill_id, skill_key, revision, schema_version, render_version,
    canonical_body, policy_json, policy_digest, content_digest,
    created_by_type, created_at
)
SELECT
    'forge.project.orchestration/v1@16', operating_skill_id, skill_key, 16,
    schema_version, render_version,
    replace(
        canonical_body,
        '- Use each milestone''s exact acceptance-check ID and definition revision. Never invent aliases such as `ac-1`, renumber a stable check, or use a description as its identity.',
        '- Use each milestone''s exact acceptance-check ID and definition revision. Never invent aliases such as `ac-1`, renumber a stable check, or use a description as its identity.
- Parentage and dependencies are separate: `parent_task_id` establishes one-level coordination hierarchy: a root with children is non-executing; direct children share the root Workspace and run serially by `subtask_order` while retaining independent assignment, execution, and lifecycle. Dependency edges only gate execution, never hierarchy or Workspace sharing, and a child must not depend on its parent.'
    ),
    policy_json, policy_digest,
    '83dc68545c9a154ca279a7469cca3a7805dcf5165a18a60ae18856180e8e1d29',
    'system', strftime('%Y-%m-%dT%H:%M:%fZ','now')
FROM operating_skill_revision
WHERE id = 'forge.project.orchestration/v1@15';

UPDATE operating_skill
SET current_revision_id = 'forge.project.orchestration/v1@16',
    version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = 'forge.project.orchestration/v1'
  AND current_revision_id IS NOT 'forge.project.orchestration/v1@16';

UPDATE project_agent_binding
SET operating_skill_revision_id = 'forge.project.orchestration/v1@16'
WHERE operating_skill_revision_id = 'forge.project.orchestration/v1@15';
