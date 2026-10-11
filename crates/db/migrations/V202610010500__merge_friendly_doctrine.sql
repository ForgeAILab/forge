-- Add merge-friendly layout and Task ownership doctrine as new immutable
-- revisions. Keep old rows and frozen admissions/session prompts unchanged.

INSERT INTO operating_skill_revision (
    id, operating_skill_id, skill_key, revision, schema_version, render_version,
    canonical_body, policy_json, policy_digest, content_digest,
    created_by_type, created_at
)
SELECT
    'forge.main.project-discovery/v2@6', operating_skill_id, skill_key, 6,
    schema_version, render_version,
    replace(canonical_body, 'SCAFFOLD
', 'PROJECT LAYOUT
Shape the Charter''s architecture constraints as small modules with clear ownership. Avoid a single hub file every feature must edit: central registries, route tables, export lists, feature enums, or a giant shared library. Prefer per-feature files and a thin, mechanical composition point.

SCAFFOLD
'),
    policy_json, policy_digest, 'c8e879bf310398dbc448c022ce38dd6cc82dff76ecf3784a64cb9da7aaebf066',
    'system', strftime('%Y-%m-%dT%H:%M:%fZ','now')
FROM operating_skill_revision
WHERE id = 'forge.main.project-discovery/v2@5';

UPDATE operating_skill
SET current_revision_id = 'forge.main.project-discovery/v2@6',
    version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = 'forge.main.project-discovery/v2'
  AND current_revision_id IS NOT 'forge.main.project-discovery/v2@6';

INSERT INTO operating_skill_revision (
    id, operating_skill_id, skill_key, revision, schema_version, render_version,
    canonical_body, policy_json, policy_digest, content_digest,
    created_by_type, created_at
)
SELECT
    'forge.project.orchestration/v1@17', operating_skill_id, skill_key, 17,
    schema_version, render_version,
    replace(replace(canonical_body, 'never include filesystem paths, credentials',
        'never include host filesystem paths, credentials'), 'STANDING INVARIANTS
', 'STANDING INVARIANTS
- Name owned repository-relative paths in every Task description. Give parallel Tasks disjoint files; order unavoidable shared-file edits with dependencies. Require workers to report out-of-scope edits before changing the Task''s scope.
'),
    policy_json, policy_digest, '16e9e0d2f390ef39309fb28f3266ba7920ef475fcae39ccfaa408366137efd1a',
    'system', strftime('%Y-%m-%dT%H:%M:%fZ','now')
FROM operating_skill_revision
WHERE id = 'forge.project.orchestration/v1@16';

UPDATE operating_skill
SET current_revision_id = 'forge.project.orchestration/v1@17',
    version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = 'forge.project.orchestration/v1'
  AND current_revision_id IS NOT 'forge.project.orchestration/v1@17';

UPDATE project_agent_binding
SET operating_skill_revision_id = 'forge.project.orchestration/v1@17'
WHERE operating_skill_revision_id = 'forge.project.orchestration/v1@16';
