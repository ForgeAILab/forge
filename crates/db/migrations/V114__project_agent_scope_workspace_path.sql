-- A Project Agent's workspace had nowhere to be recorded.
--
-- The runtime resolves a session's workspace by joining `workspace` on
-- `task_id`, which only ever finds a Task worktree. A Project Agent Chat holds
-- its own workspace instead, so the binding read back NULL and refused the
-- turn with "native turn workspace does not match the server-issued Task
-- workspace".
--
-- The scope row now carries its own path. It stays NULL for every Task scope,
-- where the `workspace` table remains the authority, and for every denied
-- scope; it is set only for the `project_verify` Agent Chat scope that owns a
-- workspace directly.

ALTER TABLE agent_context_scope ADD COLUMN workspace_path TEXT;

-- A path is meaningful only where the scope owns its workspace.
CREATE TRIGGER agent_context_scope_workspace_path_guard_insert
BEFORE INSERT ON agent_context_scope
WHEN NEW.workspace_path IS NOT NULL AND NEW.workspace_access <> 'project_verify'
BEGIN
    SELECT RAISE(ABORT, 'only a project_verify scope carries its own workspace path');
END;

CREATE TRIGGER agent_context_scope_workspace_path_guard_update
BEFORE UPDATE OF workspace_path, workspace_access ON agent_context_scope
WHEN NEW.workspace_path IS NOT NULL AND NEW.workspace_access <> 'project_verify'
BEGIN
    SELECT RAISE(ABORT, 'only a project_verify scope carries its own workspace path');
END;
