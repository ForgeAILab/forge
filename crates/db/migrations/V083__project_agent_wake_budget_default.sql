-- Project Agent bindings were created with wake_budget = 0, which disables
-- autonomous wake admission entirely: no admitted wake, no self-driving
-- Project Agent. Give current bindings the server default budget and recreate
-- the project-creation trigger so future setup bindings start with it too.
-- User-configured non-zero budgets are preserved.

UPDATE project_agent_binding
SET wake_budget = 10
WHERE wake_budget = 0
  AND state IN ('active', 'agent_setup_required');

DROP TRIGGER IF EXISTS project_agent_chat_after_insert;

CREATE TRIGGER project_agent_chat_after_insert
AFTER INSERT ON project
BEGIN
    INSERT INTO agent_chat (
        id, kind, account_id, project_id, status,
        instruction_revision, message_count, version, created_at, updated_at
    )
    SELECT
        lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
            lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
            substr('89ab', 1 + (abs(random()) % 4), 1) ||
            lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))),
        'project', NULL, NEW.id, 'agent_setup_required', 0, 0, 1,
        NEW.created_at, NEW.updated_at
    WHERE NOT EXISTS (
        SELECT 1 FROM agent_chat
        WHERE kind = 'project' AND project_id = NEW.id
    );

    INSERT INTO project_agent_binding (
        id, project_id, identity_id, profile_id, state,
        autonomy_policy_json, permission_ceiling_json, subscriptions_json,
        wake_budget, version, created_at, updated_at
    )
    SELECT
        lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
            lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
            substr('89ab', 1 + (abs(random()) % 4), 1) ||
            lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))),
        NEW.id, NULL, NULL, 'agent_setup_required', '{}', '{}', '[]', 10, 1,
        NEW.created_at, NEW.updated_at
    WHERE NOT EXISTS (
        SELECT 1 FROM project_agent_binding
        WHERE project_id = NEW.id
          AND state IN ('active', 'agent_setup_required')
    );
END;
