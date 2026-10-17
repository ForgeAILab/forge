-- Retain session denial candidates; clearing causes are rechecked against live
-- identity/Project state before inclusion in a state card. Session rotation
-- starts with no denial records. Existing user data and frozen admissions remain.
CREATE TABLE chat_session_denied_operation (
    session_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    denied_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (session_id, operation, denied_by)
);

-- New immutable Project doctrine, derived from the previous canonical body.
INSERT INTO operating_skill_revision (
    id, operating_skill_id, skill_key, revision, schema_version, render_version,
    canonical_body, policy_json, policy_digest, content_digest,
    created_by_type, created_at
)
SELECT
    'forge.project.orchestration/v1@18', operating_skill_id, skill_key, 18,
    schema_version, render_version,
    replace(canonical_body, '- On an attention wake: diagnose with your read tools first, then repair what your authority covers — retry or resume a failed execution, correct a Task definition, reassign a role from eligible agents, cancel obsolete or wedged work with `task.cancel`, and replace incorrect work through the adaptive envelope, including cancelling a verification-shaped Task and settling its checks yourself. Escalate to the user only what your authority or the envelope cannot cover.', '- On an attention wake: diagnose with your read tools first, then repair what your authority covers. Recover, resume, or re-execute only when the operation is offered and the previous attempt''s cause has been addressed. A denial marked `retry: none` is final for the turn: repeating the call will be refused again; use offered alternatives and escalate to the user only when your authority cannot cover the blocker. Use other offered operations to correct a Task definition, reassign a role from eligible agents, cancel obsolete or wedged work with `task.cancel`, or replace incorrect work through the adaptive envelope, including cancelling a verification-shaped Task and settling its checks yourself. Escalate to the user only what your authority or the envelope cannot cover.'),
    policy_json, policy_digest, 'ac73e6e27e9e4fe2a41d0c14c9a2e8835f3507596bac057698375922ccbd1c1f',
    'system', strftime('%Y-%m-%dT%H:%M:%fZ','now')
FROM operating_skill_revision
WHERE id = 'forge.project.orchestration/v1@17';

UPDATE operating_skill
SET current_revision_id = 'forge.project.orchestration/v1@18',
    version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = 'forge.project.orchestration/v1'
  AND current_revision_id IS NOT 'forge.project.orchestration/v1@18';

UPDATE project_agent_binding
SET operating_skill_revision_id = 'forge.project.orchestration/v1@18'
WHERE operating_skill_revision_id = 'forge.project.orchestration/v1@17';
