-- Project Agent release-blocker reporting contract. Preserve the former
-- immutable skill revision, derive the new canonical body from it, and carry
-- active Project bindings to the exact server contract used by new turns.
INSERT INTO operating_skill_revision (
    id, operating_skill_id, skill_key, revision, schema_version, render_version,
    canonical_body, policy_json, policy_digest, content_digest,
    created_by_type, created_at
)
SELECT
    'forge.project.orchestration/v1@4',
    operating_skill_id,
    skill_key,
    4,
    schema_version,
    render_version,
    replace(
        canonical_body,
        '- Propose release with a concise summary, exact candidate ReadinessSnapshot ID/digest, exact inputs, known issues, and missing/waived checks. Only the user may approve release; the release transaction recomputes the same digest and atomically creates the release manifest plus release-scoped evidence pins without creating another readiness snapshot.',
        '- Propose release with a concise summary, exact candidate ReadinessSnapshot ID/digest, exact inputs, known issues, and missing/waived checks. Only the user may approve release; the release transaction recomputes the same digest and atomically creates the release manifest plus release-scoped evidence pins without creating another readiness snapshot.
- Never propose or narrate a release from a blocked, failed, or stale readiness result. Report every canonical readiness blocker instead; do not write “Known Issues: None” while any required validation or evidence is missing.'
    ),
    policy_json,
    policy_digest,
    '6d6b422693a3e706cf27e6fbc947309467e6fd5cea39dca67df4a5641298dfe2',
    'system',
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
FROM operating_skill_revision
WHERE id = 'forge.project.orchestration/v1@3';

UPDATE operating_skill
SET current_revision_id = 'forge.project.orchestration/v1@4',
    version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = 'forge.project.orchestration/v1'
  AND current_revision_id IS NOT 'forge.project.orchestration/v1@4';

UPDATE project_agent_binding
SET operating_skill_revision_id = 'forge.project.orchestration/v1@4'
WHERE operating_skill_revision_id = 'forge.project.orchestration/v1@3';
