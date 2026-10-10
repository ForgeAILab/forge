use crate::{
    workflow::{
        actions::{
            AutoCascadeOnCompletion, AutoCascadeOnMergeResult, AutoCascadeOnReviewPass,
            AutoCascadeOnUnassignedRole, AutoCascadeOnUnconfiguredReview, CancelPendingSubtasks,
            CarryReviewAuthority, CheckMergeFixBudget, CheckRetryBudget, CleanupWorkspaceNow,
            DependencyGate, DispatchExecutor, DispatchFixAgent, DispatchRoleAgent,
            NotifyRoleHolder, PropagateDoneToSubtasks, RequireCleanWorktree,
            RequireConflictMarkersResolved, RequirePlanChecklistComplete,
            RequireUpstreamRolesCompleted, RunBeforeWorkHooks, RunCiSteps, RunMerge,
            SatisfyDependents, ScheduleWorkspaceCleanup, SubtaskSequenceComplete,
        },
        HookAction,
    },
    ServiceError,
};

pub fn is_known_action(name: &str) -> bool {
    matches!(
        name,
        "run_ci_steps"
            | "run_merge"
            | "run_before_work_hooks"
            | "cleanup_workspace_now"
            | "schedule_workspace_cleanup"
            | "dispatch_role_agent"
            | "carry_review_authority"
            | "satisfy_dependents"
            | "auto_cascade_on_review_pass"
            | "auto_cascade_on_unconfigured_review"
            | "auto_cascade_on_merge_result"
            | "auto_cascade_on_completion"
            | "auto_cascade_on_unassigned_role"
            | "check_merge_fix_budget"
            | "check_retry_budget"
            | "require_clean_worktree"
            | "require_plan_checklist_complete"
            | "require_conflict_markers_resolved"
            | "dependency_gate"
            | "dispatch_executor"
            | "dispatch_fix_agent"
            | "notify_role_holder"
            | "require_upstream_roles_completed"
            | "subtask_sequence_complete"
            | "propagate_done_to_subtasks"
            | "cancel_pending_subtasks"
    )
}

/// Dispatch-style actions are the hooks that start (or hand off) role work.
/// The engine gives their failures special treatment when they gate entry into
/// an `active` state: a failed dispatch means no execution exists, so the task
/// must not be left looking in-flight.
pub fn is_dispatch_action(name: &str) -> bool {
    matches!(
        name,
        "dispatch_role_agent" | "dispatch_fix_agent" | "dispatch_executor"
    )
}

/// Test-only workload that keeps a hooks step running for as long as the
/// test wants: it touches `started` in the gate directory named by the
/// state's `test_hold_gate` and returns once `release` exists there (or the
/// directory is gone).
#[cfg(test)]
pub(crate) struct TestHoldStep;
#[cfg(test)]
#[async_trait::async_trait]
impl HookAction for TestHoldStep {
    async fn execute(&self, ctx: &crate::workflow::HookContext) -> crate::workflow::HookResult {
        let Some(gate) = ctx.state_config["test_hold_gate"]
            .as_str()
            .map(std::path::PathBuf::from)
        else {
            return crate::workflow::HookResult::Skipped {
                reason: "no gate".to_owned(),
            };
        };
        let _ = std::fs::write(gate.join("started"), "");
        while !gate.join("release").exists() && gate.is_dir() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        crate::workflow::HookResult::Ok
    }
}

pub fn resolve_action(name: &str) -> Result<Box<dyn HookAction>, ServiceError> {
    let action: Box<dyn HookAction> = match name {
        "run_ci_steps" => Box::new(RunCiSteps),
        "run_before_work_hooks" => Box::new(RunBeforeWorkHooks),
        "run_merge" => Box::new(RunMerge),
        "cleanup_workspace_now" => Box::new(CleanupWorkspaceNow),
        "schedule_workspace_cleanup" => Box::new(ScheduleWorkspaceCleanup),
        "dispatch_role_agent" => Box::new(DispatchRoleAgent),
        "carry_review_authority" => Box::new(CarryReviewAuthority),
        "satisfy_dependents" => Box::new(SatisfyDependents),
        "auto_cascade_on_review_pass" => Box::new(AutoCascadeOnReviewPass),
        "auto_cascade_on_unconfigured_review" => Box::new(AutoCascadeOnUnconfiguredReview),
        "auto_cascade_on_merge_result" => Box::new(AutoCascadeOnMergeResult),
        "auto_cascade_on_completion" => Box::new(AutoCascadeOnCompletion),
        "auto_cascade_on_unassigned_role" => Box::new(AutoCascadeOnUnassignedRole),
        "check_merge_fix_budget" => Box::new(CheckMergeFixBudget),
        "check_retry_budget" => Box::new(CheckRetryBudget),
        "require_clean_worktree" => Box::new(RequireCleanWorktree),
        "require_plan_checklist_complete" => Box::new(RequirePlanChecklistComplete),
        "require_conflict_markers_resolved" => Box::new(RequireConflictMarkersResolved),
        "dependency_gate" => Box::new(DependencyGate),
        "dispatch_executor" => Box::new(DispatchExecutor),
        "dispatch_fix_agent" => Box::new(DispatchFixAgent),
        "notify_role_holder" => Box::new(NotifyRoleHolder),
        "require_upstream_roles_completed" => Box::new(RequireUpstreamRolesCompleted),
        "subtask_sequence_complete" => Box::new(SubtaskSequenceComplete),
        "propagate_done_to_subtasks" => Box::new(PropagateDoneToSubtasks),
        "cancel_pending_subtasks" => Box::new(CancelPendingSubtasks),
        #[cfg(test)]
        "test_hold_step" => Box::new(TestHoldStep),
        _ => {
            return Err(ServiceError::InvalidOperation {
                message: format!("unknown action: {name}"),
            });
        }
    };

    Ok(action)
}
