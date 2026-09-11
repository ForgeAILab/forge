-- A Task belongs to a Project, not to a snapshot of whichever repository was
-- primary when the Task happened to be created. Project.primary_repo_id is the
-- only pre-execution repository authority; Workspace and WorkspaceLease retain
-- the exact repository used by an execution attempt.

DROP TRIGGER workspace_lease_scope_guard_insert;
DROP TRIGGER workspace_lease_active_renewal_guard;
DROP INDEX idx_task_repo;

ALTER TABLE task DROP COLUMN repo_id;

CREATE TRIGGER workspace_lease_scope_guard_insert
BEFORE INSERT ON workspace_lease
WHEN NEW.status = 'active'
BEGIN
    SELECT CASE
        WHEN NEW.issuing_principal_type != 'system'
          OR NEW.issuing_principal_id != 'task-service-scheduler'
        THEN RAISE(ABORT, 'Workspace lease may only be issued by the scheduler')
        WHEN NOT EXISTS (
            SELECT 1
            FROM task t
            JOIN project p ON p.id = t.project_id
            JOIN repo r
              ON r.id = p.primary_repo_id
             AND r.project_id = p.id
            WHERE t.id = NEW.task_id AND t.project_id = NEW.project_id
              AND t.version = NEW.task_version
              AND p.primary_repo_id = NEW.repository_binding_id
              AND (
                  (t.assignee_type = NEW.assigned_principal_type
                   AND t.assignee_id = NEW.assigned_principal_id)
                  OR EXISTS (
                      SELECT 1
                      FROM task_role_assignment role_assignment
                      JOIN execution assigned_execution
                        ON assigned_execution.id = NEW.execution_id
                      WHERE role_assignment.task_id = NEW.task_id
                        AND role_assignment.role_name = assigned_execution.role
                        AND role_assignment.assignee_type = NEW.assigned_principal_type
                        AND role_assignment.assignee_id = NEW.assigned_principal_id
                  )
                  OR ((p.charter_status != 'charter_backed'
                       OR p.charter_setup_required != 0)
                      AND t.assignee_type IS NULL AND t.assignee_id IS NULL)
              )
        ) THEN RAISE(ABORT, 'Workspace lease Task is cross-Project or stale')
        WHEN NOT EXISTS (
            SELECT 1
            FROM execution e
            JOIN workspace w ON w.id = e.workspace_id
            WHERE e.id = NEW.execution_id AND e.task_id = NEW.task_id
              AND e.status = 'running'
              AND e.agent_id = NEW.assigned_principal_id
              AND w.repo_id = NEW.repository_binding_id
              AND ((NEW.role = 'reviewer' AND e.role = 'reviewer')
                   OR (NEW.role = 'worker' AND length(trim(e.role)) > 0
                       AND e.role != 'reviewer'))
        ) THEN RAISE(ABORT, 'Workspace lease execution is not Task-scoped')
        WHEN NOT EXISTS (
            SELECT 1
            FROM project p
            LEFT JOIN project_task_governance g
              ON g.task_id = NEW.task_id AND g.project_id = p.id
            WHERE p.id = NEW.project_id
              AND json_array_length(NEW.capabilities_json) = 1
              AND json_extract(NEW.capabilities_json, '$[0]') =
                  COALESCE(g.capability_class,
                    CASE WHEN (SELECT task_type FROM task WHERE id = NEW.task_id)
                              IN ('planning_task', 'discovery')
                         THEN 'repository_read' ELSE 'repository_write' END)
              AND NEW.capability_profile_revision = 'forge.capability-profile/v1'
              AND NEW.capability_profile_digest = CASE json_extract(NEW.capabilities_json, '$[0]')
                  WHEN 'repository_read' THEN 'sha256:6035ec533a0bdb74c461ea9ea2d7147a2e47ba7c8b54c8b732052ceec23e8234'
                  WHEN 'repository_write' THEN 'sha256:eeb061a14ab862e1a7b16989ef637293ba538f46122ff28b30313d330dbae4a8'
                  WHEN 'read_only' THEN 'sha256:08fe2de40d5f9027b803131fcbe5ab3c885c044836d6e20c2e9319951d2e82f3'
                  WHEN 'discovery_read' THEN 'sha256:54502cd9c50b5f43a79e75cd1abdedf5e354393ef1422e6c4932c5716c660c43'
                  WHEN 'planning_read' THEN 'sha256:78316b764f1326273f129407de72a33bbcf8db210d3bdfe7154fa1384a7d366d'
                  ELSE '' END
              AND (
                  p.charter_status != 'charter_backed'
                  OR p.charter_setup_required != 0
                  OR (p.current_charter_revision_id IS NOT NULL
                      AND g.charter_revision_id = p.current_charter_revision_id)
              )
        ) THEN RAISE(ABORT, 'Workspace lease requires the current approved Project Charter')
    END;
END;

CREATE TRIGGER workspace_lease_active_renewal_guard
BEFORE UPDATE ON workspace_lease
WHEN OLD.status = 'active' AND NEW.status = 'active'
BEGIN
    SELECT CASE
        WHEN NEW.expires_at <= OLD.expires_at
          OR NEW.updated_at IS OLD.updated_at
        THEN RAISE(ABORT, 'Workspace lease renewal must extend expiry')
        WHEN EXISTS (
            SELECT 1 FROM project_agent_binding
            WHERE project_id = NEW.project_id
              AND identity_id = NEW.assigned_principal_id
              AND state = 'active'
        ) OR EXISTS (
            SELECT 1 FROM account_main_agent_binding
            WHERE identity_id = NEW.assigned_principal_id
              AND state = 'active'
        ) THEN RAISE(ABORT, 'Orchestration agents cannot receive Workspace leases')
        WHEN NOT EXISTS (
            SELECT 1
            FROM task t
            JOIN project p ON p.id = t.project_id
            JOIN repo r
              ON r.id = p.primary_repo_id
             AND r.project_id = p.id
            JOIN execution e ON e.id = NEW.execution_id
            JOIN workspace w ON w.id = e.workspace_id
            LEFT JOIN project_task_governance g
              ON g.task_id = t.id AND g.project_id = p.id
            WHERE t.id = NEW.task_id
              AND t.project_id = NEW.project_id
              AND t.version = NEW.task_version
              AND p.primary_repo_id = NEW.repository_binding_id
              AND w.repo_id = NEW.repository_binding_id
              AND e.task_id = NEW.task_id
              AND e.status = 'running'
              AND e.agent_id = NEW.assigned_principal_id
              AND ((NEW.role = 'reviewer' AND e.role = 'reviewer')
                   OR (NEW.role = 'worker' AND e.role != 'reviewer'))
              AND (
                  (t.assignee_type = NEW.assigned_principal_type
                   AND t.assignee_id = NEW.assigned_principal_id)
                  OR EXISTS (
                      SELECT 1 FROM task_role_assignment ra
                      WHERE ra.task_id = NEW.task_id
                        AND ra.role_name = e.role
                        AND ra.assignee_type = NEW.assigned_principal_type
                        AND ra.assignee_id = NEW.assigned_principal_id
                  )
                  OR ((p.charter_status != 'charter_backed'
                       OR p.charter_setup_required != 0)
                      AND t.assignee_type IS NULL AND t.assignee_id IS NULL)
              )
              AND json_array_length(NEW.capabilities_json) = 1
              AND json_extract(NEW.capabilities_json, '$[0]') =
                  COALESCE(g.capability_class,
                    CASE WHEN t.task_type IN ('planning_task', 'discovery')
                         THEN 'repository_read' ELSE 'repository_write' END)
              AND (
                  p.charter_status != 'charter_backed'
                  OR p.charter_setup_required != 0
                  OR g.charter_revision_id = p.current_charter_revision_id
              )
        ) THEN RAISE(ABORT, 'Workspace lease renewal authority is stale')
    END;
END;
