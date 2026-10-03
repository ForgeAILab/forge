-- Advance layout guidance only; retain all historical bodies, session prompts and frozen admissions.

INSERT INTO operating_skill_revision (
    id, operating_skill_id, skill_key, revision, schema_version, render_version,
    canonical_body, policy_json, policy_digest, content_digest, created_by_type, created_at
)
SELECT 'forge.main.project-discovery/v2@7', operating_skill_id, skill_key, 7,
    schema_version, render_version,
    replace(canonical_body, 'Shape the Charter''s architecture constraints as small modules with clear ownership. Avoid a single hub file every feature must edit: central registries, route tables, export lists, feature enums, or a giant shared library. Prefer per-feature files and a thin, mechanical composition point.', 'For Charter architecture constraints: Use small modules with clear ownership so parallel Tasks edit disjoint files. Avoid hub files (central registries, route tables, export/barrel lists, large shared libraries). Prefer per-feature files discovered/registered without shared-list edits; otherwise give one Task ownership of shared edits and make others depend on it. Split along module boundaries; name owned modules/files in each Task.'),
    policy_json, policy_digest, '69bfd668a75912e33c79ce74f10fd90f1a479068b0bab4bfcd027174eed3e24b', 'system', strftime('%Y-%m-%dT%H:%M:%fZ','now')
FROM operating_skill_revision WHERE id = 'forge.main.project-discovery/v2@6';

UPDATE operating_skill
SET current_revision_id = 'forge.main.project-discovery/v2@7', version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = 'forge.main.project-discovery/v2' AND current_revision_id IS NOT 'forge.main.project-discovery/v2@7';

INSERT INTO operating_skill_revision (
    id, operating_skill_id, skill_key, revision, schema_version, render_version,
    canonical_body, policy_json, policy_digest, content_digest, created_by_type, created_at
)
SELECT 'forge.project.orchestration/v1@20', operating_skill_id, skill_key, 20,
    schema_version, render_version,
    replace(canonical_body, 'Name owned repository-relative paths in every Task description. Give parallel Tasks disjoint files; order unavoidable shared-file edits with dependencies.', 'For task.propose: Use small modules with clear ownership so parallel Tasks edit disjoint files. Avoid hub files (central registries, route tables, export/barrel lists, large shared libraries). Prefer per-feature files discovered/registered without shared-list edits; otherwise give one Task ownership of shared edits and make others depend on it. Split along module boundaries; name owned modules/files in each Task.'),
    policy_json, policy_digest, 'fdf2aae9169e70fd2c0c48fad30afc38eb1d90ecb951237ff64385d7f2fc40ca', 'system', strftime('%Y-%m-%dT%H:%M:%fZ','now')
FROM operating_skill_revision WHERE id = 'forge.project.orchestration/v1@19';

UPDATE operating_skill
SET current_revision_id = 'forge.project.orchestration/v1@20', version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = 'forge.project.orchestration/v1' AND current_revision_id IS NOT 'forge.project.orchestration/v1@20';

UPDATE project_agent_binding SET operating_skill_revision_id = 'forge.project.orchestration/v1@20'
WHERE operating_skill_revision_id = 'forge.project.orchestration/v1@19';
