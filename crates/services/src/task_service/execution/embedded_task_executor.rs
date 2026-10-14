/// Roles whose executions and native Task sessions never receive worktree
/// write access. Planners author plans through Task metadata/native tools.
pub(crate) fn is_read_only_task_role(role: &str) -> bool {
    matches!(
        role,
        crate::workflow::default_roles::REVIEWER | crate::workflow::default_roles::PLANNER
    )
}
