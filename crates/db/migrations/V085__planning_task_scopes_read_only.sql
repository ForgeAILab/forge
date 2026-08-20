-- Planning/discovery Tasks never grant worktree write authority, but the
-- session authorization used to derive workspace access from the role alone,
-- so a coder-role dispatch on a planning record persisted a task_write
-- context scope. The canonical scope row is immutable per
-- (identity, scope_type, scope_id), so those stale rows would now conflict
-- with the corrected task_read authorization forever. Downgrade them once;
-- read access is strictly narrower, so no authority is widened.
UPDATE agent_context_scope
SET workspace_access = 'task_read'
WHERE scope_type = 'task'
  AND workspace_access = 'task_write'
  AND scope_id IN (
      SELECT id FROM task WHERE task_type IN ('planning_task', 'discovery')
  );
