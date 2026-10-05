use async_trait::async_trait;
use db::{TaskDependencyRepo, TaskRepo, TaskRoleAssignmentRepo, TransitionLogRepo, WorkspaceRepo};

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
                reason: format!("No {role_name} role assigned; optional gate skipped"),

                bridge: api_types::TransitionBridge::new(
                    api_types::TransitionBridgeKind::GateSkipped,
                ),
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
            let budget = match db::budget::task_limit(
                &ctx.db,
                &task,
                db::budget::Kind::Review,
                Some(&ctx.state_config),
                ctx.gate_config.as_ref(),
            )
            .await
            {
                Ok(budget) => budget,
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            };
            let count = match db::budget::spent(
                ctx.db.pool(),
                &ctx.task_id,
                db::budget::Kind::Review.key(),
            )
            .await
            {
                Ok(n) => n,
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            };
            (budget, count)
        } else {
            let Some(gate_config) = &ctx.gate_config else {
                return HookResult::Ok;
            };
            let Some(_) = gate_config.max_rejections else {
                return HookResult::Ok;
            };
            let budget_task = match task(ctx).await {
                Ok(t) => t,
                Err(reason) => return HookResult::Failed { reason },
            };
            let max_rejections = match db::budget::task_limit(
                &ctx.db,
                &budget_task,
                db::budget::Kind::GateRejection,
                Some(&ctx.state_config),
                Some(gate_config),
            )
            .await
            {
                Ok(n) => n,
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            };
            let count = match db::budget::spent(
                ctx.db.pool(),
                &ctx.task_id,
                &db::budget::gate_key(&ctx.to_state),
            )
            .await
            {
                Ok(n) => n,
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            };
            (max_rejections, count)
        };

        if db::budget::gate_entry_exhausted(&ctx.to_state, i64::from(max_rejections), count) {
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
        let task = match db::TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false).await {
            Ok(Some(task)) => task,
            Ok(None) => {
                return HookResult::Failed {
                    reason: format!("task not found: {}", ctx.task_id),
                };
            }
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
        };
        match crate::task_hierarchy::coordination_root_has_subtasks(&ctx.db, &task).await {
            Ok(true) => {
                return HookResult::Skipped {
                    reason: "coordination root is governed by ordered subtask completion"
                        .to_string(),
                };
            }
            Ok(false) => {}
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
        }
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

        let resolved = match super::review::resolve_workspace(ctx, &workspace).await {
            Ok(resolved) => resolved,
            Err(error) => {
                return HookResult::Failed {
                    reason: format!("plan checklist unreadable: {error}"),
                };
            }
        };
        let bytes = match resolved
            .backend
            .read(&resolved.placement, "../plan.md", 1_048_576)
            .await
        {
            Ok(bytes) => bytes,
            Err(crate::workspace_backend::WorkspaceBackendError::Other(error)) if matches!(&*error, crate::ServiceError::InvalidOperation { message } if message == "plan artifact not found") =>
            {
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
        let content = match String::from_utf8(bytes) {
            Ok(content) => content,
            Err(_) => {
                return HookResult::Failed {
                    reason: "plan checklist unreadable: failed to read plan artifact: stream did not contain valid UTF-8".to_owned(),
                };
            }
        };
        let artifact = crate::plan_artifact::parse_plan_markdown(&content);
        let summary = crate::plan_artifact::to_plan_progress_summary(&artifact);
        if summary.total == 0 || summary.remaining == 0 {
            return HookResult::Ok;
        }

        HookResult::Failed {
            reason: format!(
                "Plan checklist incomplete: {} unchecked item(s) remain. Continue working on the unchecked items, then update completed items to `- [x]` using `task.plan` for a native session or `$FORGE_PLAN_PATH` for a CLI harness before stopping.",
                summary.remaining
            ),
        }
    }
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
