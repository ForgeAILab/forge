-- Project Agent acceptance/evidence contract discovery. Preserve the former
-- immutable skill revision, derive the new canonical body from it, and carry
-- active Project bindings to the exact server contract used by new turns.
INSERT INTO operating_skill_revision (
    id, operating_skill_id, skill_key, revision, schema_version, render_version,
    canonical_body, policy_json, policy_digest, content_digest,
    created_by_type, created_at
)
SELECT
    'forge.project.orchestration/v1@3',
    operating_skill_id,
    skill_key,
    3,
    schema_version,
    render_version,
    replace(
        replace(
            canonical_body,
            'Bundle the exact governing Charter and content/render digests, applicable Document revisions, stable plan-item identities, milestone selection and primary_milestone_id, release-policy revision, acceptance/evidence matrix, Task capability/risk classes, adaptive envelope, elevated/irreversible operations, known assumptions, exclusions, risks, rollback/recovery, and material diff into one proposed baseline.
Only the interactive user may approve or activate the exact baseline digest.',
            'Bundle the exact governing Charter and content/render digests, applicable Document revisions, stable plan-item identities, milestone selection and primary_milestone_id, release-policy revision, acceptance/evidence matrix, Task capability/risk classes, adaptive envelope, elevated/irreversible operations, known assumptions, exclusions, risks, rollback/recovery, and material diff into one proposed baseline.
Before drafting or proposing, read `project.current_state` and copy each current milestone''s exact acceptance-check ID and definition revision into the acceptance/evidence matrix. Never invent aliases such as `ac-1`, renumber a stable check, or use a description as its identity. The proposed matrix must exactly match the pinned milestone definitions and their required evidence kinds; Forge rejects a mismatch before user approval.
Only the interactive user may approve or activate the exact baseline digest.'
        ),
        '- Define its outcome, included/excluded scope, acceptance checks, linked artifact revisions, Task selection, evidence expectations, and optional human-facing version label.
- Multiple milestones may be active;',
        '- Define its outcome, included/excluded scope, acceptance checks, linked artifact revisions, Task selection, evidence expectations, and optional human-facing version label. Every required acceptance check has one required evidence requirement with the same stable ID. Evidence is mandatory proof, not optional decoration.
- Preserve existing stable check IDs across milestone revisions. Use `manual` only when an authorized user must make a genuinely human observation or judgment; never treat repository test output as a manual attestation. A manual result and its required evidence are separate inputs, and you may request but never record the user''s result.
- Multiple milestones may be active;'
    ),
    policy_json,
    policy_digest,
    'd82c5cb7df0a7b82c715abca911243374e05d07ab777d2fb0a6264aaf351bbfb',
    'system',
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
FROM operating_skill_revision
WHERE id = 'forge.project.orchestration/v1@2';

UPDATE operating_skill
SET current_revision_id = 'forge.project.orchestration/v1@3',
    version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = 'forge.project.orchestration/v1'
  AND current_revision_id IS NOT 'forge.project.orchestration/v1@3';

UPDATE project_agent_binding
SET operating_skill_revision_id = 'forge.project.orchestration/v1@3'
WHERE operating_skill_revision_id = 'forge.project.orchestration/v1@2';
