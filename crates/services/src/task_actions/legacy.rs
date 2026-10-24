//! Legacy snapshot conversion for condition parity checks.

include!("../task_actions_legacy.rs");
impl From<&super::TaskSnapshot> for TaskSnapshot {
    fn from(s: &super::TaskSnapshot) -> Self {
        Self {
            task: s.task.clone(),
            workflow: s.workflow.clone(),
            executions: s.executions.clone(),
            latest_review: s.latest_review.clone(),
            role_assignments: s.role_assignments.clone(),
            transition_logs: s.transition_logs.clone(),
            budget_spent: s.budget_spent.clone(),
            recovery_budget_limit: s.recovery_budget_limit,
            caller: ActionCaller {
                owner: s.caller.owner,
                assigned_agent: s.caller.assigned_agent,
                project_agent: s.caller.project_agent,
                reviewer: s.caller.reviewer,
            },
            has_agent: s.has_agent,
            dependencies_satisfied: s.dependencies_satisfied,
            owner_supports_resume: s.owner_supports_resume,
            coordination_root: s.coordination_root,
            owner_disconnected: s.owner_disconnected,
            project_paused: s.project_paused,
            wait_cause: s.wait_cause.clone(),
            action_agent_id: s.action_agent_id.clone(),
            planning_approval_ready: s.planning_approval_ready,
            advance_target: s.advance_target.clone(),
            entry_hooks_running: s.entry_hooks_running,
        }
    }
}
