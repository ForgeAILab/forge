-- Early Forge Solo builds activated the selected Project Agent for Charter
-- drafting with a setup-only permission ceiling. Charter approval correctly
-- preserves an already-active binding's ceiling, so that exact Solo default
-- survived adoption and left task.propose unavailable forever.
--
-- Upgrade only the exact old server-authored ceiling on a Solo Project whose
-- binding and Project agree on either side of Charter adoption. This includes
-- an in-flight adoption conversation: new Solo code expects the future-ready
-- ceiling there, while the independent Charter setup gate still prevents those
-- permissions from becoming effective before approval. Caller-authored or
-- otherwise narrowed bindings are deliberately untouched.
UPDATE project_agent_binding
SET permission_ceiling_json =
        '{"permissions":["read_project","read_agent_chat","read_task","read_memory","propose_task","propose_project","propose_message","propose_review","propose_commitment","propose_memory","propose_decision","propose_session"]}',
    version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE state = 'active'
  AND permission_ceiling_json =
        '{"permissions":["read_project","read_agent_chat","read_memory","propose_message","propose_project"]}'
  AND EXISTS (
      SELECT 1
      FROM project
      WHERE project.id = project_agent_binding.project_id
        AND json_type(project.settings, '$.forge_solo') = 'object'
        AND (
            (project.charter_status = 'legacy_unverified'
             AND project.charter_setup_required = 1
             AND project_agent_binding.charter_setup_required = 1)
            OR
            (project.charter_status = 'charter_backed'
             AND project.charter_setup_required = 0
             AND project_agent_binding.charter_setup_required = 0)
        )
  );
