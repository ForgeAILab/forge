use async_trait::async_trait;
use db::{
    TaskDependencyRepo, TaskRepo, TaskRoleAssignmentRepo, TransitionLog, TransitionLogRepo,
    WorkspaceRepo,
};

use crate::workflow::{
    default_states, effective_role, engine::WorkflowEngine, HookAction, HookContext, HookResult,
};

use super::common::{block_task, get_role_assignment, task, workspace_id};

pub struct AutoCascadeOnUnassignedRole;

#[async_trait]
impl HookAction for AutoCascadeOnUnassignedRole {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let Some(state) = ctx
            .workflow
            .states
            .iter()
            .find(|state| state.name == ctx.to_state)
        else {
            return HookResult::Failed {
                reason: WorkflowEngine::undefined_state_message(&ctx.to_state, &ctx.workflow),
            };
        };
        let Some(role_name) = effective_role(state) else {
            return HookResult::Skipped {
                reason: "state has no role".to_string(),
            };
        };
        if !state
            .gate_config
            .as_ref()
            .is_some_and(|config| config.optional_when_unassigned())
        {
            return HookResult::Skipped {
                reason: format!("{role_name} role is required"),
            };
        }
        let assignment = match get_role_assignment(ctx, role_name).await {
            Ok(assignment) => assignment,
            Err(reason) => return HookResult::Failed { reason },
        };
        if assignment
            .as_ref()
            .is_some_and(|assignment| assignment.assignee_id.is_some())
        {
            return HookResult::Skipped {
                reason: format!("{role_name} role assigned"),
            };
        }

        let target = ctx
            .workflow
            .outgoing_trigger_targets(&ctx.to_state)
            .filter(|(trigger, _)| !trigger.system_only())
            .find_map(|(_, to)| {
                ctx.workflow
                    .states
                    .iter()
                    .find(|state| state.name == to && state.kind == api_types::StateKind::Active)
                    .map(|state| state.name.clone())
            });

        match target {
            Some(to) => HookResult::Cascade {
                to,
                reason: format!("gate skipped: no {role_name} role assigned"),
            },
            None => HookResult::Skipped {
                reason: format!("no active transition for unassigned {role_name} role"),
            },
        }
    }
}

pub struct CheckRetryBudget;

#[async_trait]
impl HookAction for CheckRetryBudget {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let (max_rejections, count) = if ctx.to_state == default_states::REVIEW {
            let task = match task(ctx).await {
                Ok(task) => task,
                Err(reason) => return HookResult::Failed { reason },
            };
            let budget = match crate::task_service::config::runtime_retry_budget(
                &task,
                crate::task_service::config::RetryBudgetKind::Review,
                Some(&ctx.state_config),
                ctx.gate_config.as_ref(),
            ) {
                Ok(budget) => budget,
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            };
            let entries = match TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id).await {
                Ok(entries) => entries,
                Err(error) => {
                    return HookResult::Skipped {
                        reason: format!("retry budget unavailable: {error}"),
                    };
                }
            };
            let count = review_rejections_since_boundary(&entries);
            (budget, count)
        } else {
            let Some(gate_config) = &ctx.gate_config else {
                return HookResult::Ok;
            };
            let Some(max_rejections) = gate_config.max_rejections else {
                return HookResult::Ok;
            };
            let count = match TransitionLogRepo::count_gate_rejections(
                &*ctx.db,
                &ctx.task_id,
                &ctx.to_state,
            )
            .await
            {
                Ok(count) => count,
                Err(error) => {
                    return HookResult::Skipped {
                        reason: format!("retry budget unavailable: {error}"),
                    };
                }
            };
            (max_rejections, count)
        };

        if count >= i64::from(max_rejections) {
            if ctx.to_state == default_states::REVIEW {
                tracing::debug!(
                    task_id = %ctx.task_id,
                    state = %ctx.to_state,
                    rejections = count,
                    budget = i64::from(max_rejections),
                    "review retry budget exhausted on gate entry; deferring enforcement until review failure"
                );
                return HookResult::Ok;
            }
            let task = match task(ctx).await {
                Ok(task) => task,
                Err(reason) => return HookResult::Failed { reason },
            };
            if task.blocked_json.is_some() {
                return HookResult::Ok;
            }
            let reason = format!(
                "gate rejection budget exhausted: {}/{}",
                count, max_rejections
            );
            tracing::info!(
                task_id = %ctx.task_id,
                state = %ctx.to_state,
                rejections = count,
                budget = i64::from(max_rejections),
                "retry budget exhausted, blocking task"
            );
            if let Err(error) = block_task(
                ctx,
                &task,
                &reason,
                api_types::FailureKind::RetryExhausted,
                None,
            )
            .await
            {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
            return HookResult::Ok;
        }

        tracing::debug!(
            task_id = %ctx.task_id,
            state = %ctx.to_state,
            rejections = count,
            budget = i64::from(max_rejections),
            "retry budget check passed"
        );
        HookResult::Ok
    }
}

pub struct RequirePlanChecklistComplete;

#[async_trait]
impl HookAction for RequirePlanChecklistComplete {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let Some(workspace_id) = workspace_id(ctx).await else {
            return HookResult::Skipped {
                reason: "no workspace".to_string(),
            };
        };
        let workspace = match WorkspaceRepo::get_by_id(&*ctx.db, &workspace_id).await {
            Ok(Some(workspace)) => workspace,
            Ok(None) => {
                return HookResult::Skipped {
                    reason: format!("workspace not found: {workspace_id}"),
                };
            }
            Err(error) => {
                return HookResult::Failed {
                    reason: format!("workspace unavailable: {error}"),
                };
            }
        };

        let artifact = match crate::plan_artifact::read_plan_artifact(
            std::path::Path::new(&workspace.worktree_path),
            None,
        ) {
            Ok(artifact) => artifact,
            Err(crate::plan_artifact::PlanArtifactError::NotFound) => {
                return HookResult::Skipped {
                    reason: "no plan checklist".to_string(),
                };
            }
            Err(error) => {
                return HookResult::Failed {
                    reason: format!("plan checklist unreadable: {error}"),
                };
            }
        };
        let summary = crate::plan_artifact::to_plan_progress_summary(&artifact);
        if summary.total == 0 || summary.remaining == 0 {
            return HookResult::Ok;
        }

        HookResult::Failed {
            reason: format!(
                "Plan checklist incomplete: {} unchecked item(s) remain in ../plan.md. Continue working on the unchecked items, then update completed items to `- [x]` before stopping.",
                summary.remaining
            ),
        }
    }
}

fn review_rejections_since_boundary(entries: &[TransitionLog]) -> i64 {
    let boundary = entries.iter().rposition(|entry| {
        entry.from_state == default_states::REVIEW
            && !entry.rejection
            && (entry.to_state != default_states::REVIEW
                || entry.trigger_name.as_deref() == Some("reset_retry_window"))
    });
    let entries = boundary
        .and_then(|index| entries.get(index + 1..))
        .unwrap_or(entries);
    entries
        .iter()
        .filter(|entry| entry.from_state == default_states::REVIEW && entry.rejection)
        .count() as i64
}

pub struct RequireCleanWorktree;

#[async_trait]
impl HookAction for RequireCleanWorktree {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        if ctx.workspace_id.is_none() {
            return HookResult::Skipped {
                reason: "no workspace".to_string(),
            };
        }
        HookResult::Ok
    }
}

pub struct DependencyGate;

#[async_trait]
impl HookAction for DependencyGate {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        if ctx.triggered_by.is_user() {
            return HookResult::Skipped {
                reason: "user-managed transition bypasses dependency gate".to_string(),
            };
        }

        let Some(to_state) = ctx
            .workflow
            .states
            .iter()
            .find(|state| state.name == ctx.to_state)
        else {
            return HookResult::Failed {
                reason: WorkflowEngine::undefined_state_message(&ctx.to_state, &ctx.workflow),
            };
        };
        if effective_role(to_state).is_none() {
            return HookResult::Skipped {
                reason: "target state does not start role work".to_string(),
            };
        }

        let unsatisfied =
            match TaskDependencyRepo::unsatisfied_dependencies(&*ctx.db, &ctx.task_id).await {
                Ok(deps) => deps,
                Err(error) => {
                    return HookResult::Failed {
                        reason: format!("dependency check failed: {error}"),
                    };
                }
            };
        if unsatisfied.is_empty() {
            return HookResult::Ok;
        }
        let cancellation_state = ctx
            .workflow
            .cancellation_state
            .as_deref()
            .unwrap_or(default_states::CANCELLED);
        let mut cancelled = Vec::new();
        for dependency_id in &unsatisfied {
            match TaskRepo::get_by_id(&*ctx.db, dependency_id, false).await {
                Ok(Some(dependency)) if dependency.status == cancellation_state => {
                    cancelled.push(dependency_id.clone());
                }
                Ok(_) => {}
                Err(error) => {
                    return HookResult::Failed {
                        reason: format!("dependency check failed: {error}"),
                    };
                }
            }
        }
        if !cancelled.is_empty() {
            let current = match task(ctx).await {
                Ok(task) => task,
                Err(reason) => return HookResult::Failed { reason },
            };
            let reason = format!("required dependency cancelled: {}", cancelled.join(", "));
            if current.blocked_json.is_none() {
                if let Err(error) = block_task(
                    ctx,
                    &current,
                    &reason,
                    api_types::FailureKind::WorkflowGuardRejected,
                    Some("dependency_gate"),
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            }
            return HookResult::Failed { reason };
        }
        HookResult::Failed {
            reason: format!(
                "task has {} unsatisfied dependenc{}: {}",
                unsatisfied.len(),
                if unsatisfied.len() == 1 { "y" } else { "ies" },
                unsatisfied.join(", ")
            ),
        }
    }
}

pub struct RequireUpstreamRolesCompleted;

#[async_trait]
impl HookAction for RequireUpstreamRolesCompleted {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        if ctx.workflow.states.is_empty() {
            return HookResult::Skipped {
                reason: "workflow unavailable".to_string(),
            };
        }

        let transition_logs = match TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id).await {
            Ok(logs) => logs,
            Err(error) => {
                return HookResult::Skipped {
                    reason: format!("transition log unavailable: {error}"),
                };
            }
        };

        for gate in ctx.workflow.states.iter().filter(|state| {
            state.kind == api_types::StateKind::Gate
                && ctx
                    .workflow
                    .outgoing_trigger_targets(&state.name)
                    .any(|(_, to)| to == ctx.to_state)
        }) {
            let Some(role) = effective_role(gate) else {
                continue;
            };

            let assignment =
                match TaskRoleAssignmentRepo::get_by_task_and_role(&*ctx.db, &ctx.task_id, role)
                    .await
                {
                    Ok(assignment) => assignment,
                    Err(error) => {
                        return HookResult::Skipped {
                            reason: format!("role assignment unavailable: {error}"),
                        };
                    }
                };

            if assignment.is_some() && !transition_logs.iter().any(|log| log.to_state == gate.name)
            {
                return HookResult::Failed {
                    reason: format!("{} role assigned; complete {} gate first", role, gate.name),
                };
            }
        }

        HookResult::Ok
    }
}
