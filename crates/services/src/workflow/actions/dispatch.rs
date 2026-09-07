use std::sync::Arc;

use async_trait::async_trait;
use db::ReviewStatus;
use events::{event_timestamp, EventContext, ForgeEvent};

use crate::{
    agent_capacity::has_running_execution_capacity,
    task_service::TaskService,
    workflow::{
        dispatch::{
            build_effective_prompt, dispatch_intent_from_workflow_dispatch,
            effective_prompt_selection, loader::load_agent_dispatch_context,
        },
        effective_role,
        engine::WorkflowEngine,
        HookAction, HookContext, HookResult,
    },
};

use super::common::{
    ensure_review_awaiting_human, ensure_review_record_for_dispatch, execution_guard_roles,
    follow_up_trigger, get_role_assignment, has_running_execution_for_roles, latest_review,
    review_is_ci_only, task,
};

pub struct DispatchRoleAgent;

#[async_trait]
impl HookAction for DispatchRoleAgent {
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

        let current_task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };
        if current_task.blocked_json.is_some() {
            return HookResult::Skipped {
                reason: "task is blocked".to_string(),
            };
        }
        // Note: we used to skip dispatch here for parents-with-subtasks because a
        // separate SubtaskOrchestrator drove the agent. After collapsing to the
        // cascade-handoff model, the parent's coder dispatch is the one that runs
        // each turn (including the review_fix bounce when CI fails). Skipping here
        // would leave the parent stranded in `in_progress` after a review failure
        // — see `finish_current_turn_and_begin_next` for the per-turn handoff.

        let assignment = match get_role_assignment(ctx, role_name).await {
            Ok(assignment) => assignment,
            Err(reason) => return HookResult::Failed { reason },
        };

        match assignment {
            Some(assignment)
                if assignment.assignee_type == Some(db::AssigneeKind::Agent)
                    && assignment.assignee_id.is_some() =>
            {
                if current_task.repo_id.is_none() {
                    return HookResult::Skipped {
                        reason: "task has no associated repo".to_string(),
                    };
                }
                if role_name == crate::workflow::default_roles::REVIEWER {
                    match latest_review(ctx).await {
                        Ok(Some(review))
                            if review.status == ReviewStatus::Failed
                                && Some(review.execution_id.as_str())
                                    == ctx.execution_id.as_deref() =>
                        {
                            return HookResult::Skipped {
                                reason: "review already failed".to_string(),
                            };
                        }
                        Ok(Some(review))
                            if review.status == ReviewStatus::Passed
                                && review_is_ci_only(&review) =>
                        {
                            return HookResult::Skipped {
                                reason: "review already passed CI-only".to_string(),
                            };
                        }
                        Ok(_) => {}
                        Err(reason) => return HookResult::Failed { reason },
                    }
                }
                let agent_id = assignment.assignee_id.expect("checked by match guard");
                let state_dispatch =
                    dispatch_intent_from_workflow_dispatch(state.dispatch.as_ref());
                let trigger_dispatch = ctx
                    .workflow
                    .trigger_definition_between(&ctx.from_state, &ctx.to_state)
                    .and_then(|(_, definition)| {
                        dispatch_intent_from_workflow_dispatch(definition.dispatch.as_ref())
                    });
                let selection = effective_prompt_selection(
                    role_name,
                    trigger_dispatch.as_ref(),
                    state_dispatch.as_ref(),
                );
                let dispatch_ctx = match load_agent_dispatch_context(
                    Arc::clone(&ctx.db),
                    &ctx.task_id,
                    role_name,
                    &ctx.to_state,
                    ctx.state_config.clone(),
                    Some(selection.execution_policy.as_str()),
                    &ctx.workflow,
                )
                .await
                {
                    Ok(dispatch_ctx) => dispatch_ctx,
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                };
                let guard_roles = execution_guard_roles(role_name);
                match has_running_execution_for_roles(ctx, &guard_roles).await {
                    Ok(true) => {
                        return HookResult::Skipped {
                            reason: "execution already running".to_string(),
                        };
                    }
                    Ok(false) => {}
                    Err(reason) => return HookResult::Failed { reason },
                }
                let (prompt, selection) = build_effective_prompt(
                    &dispatch_ctx,
                    trigger_dispatch.as_ref(),
                    state_dispatch.as_ref(),
                );
                let dispatch_metadata = serde_json::json!({
                    "target_role": role_name,
                    "builder_id": selection.builder_id,
                    "execution_policy": selection.execution_policy,
                });

                ctx.event_bus.publish(ForgeEvent {
                    event_type: "task.role_agent_dispatched".to_string(),
                    entity_id: ctx.task_id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskRoleAgentDispatched {
                        task_id: ctx.task_id.clone(),
                        role: role_name.to_string(),
                        agent_id: agent_id.clone(),
                        state: ctx.to_state.clone(),
                        parent_execution_id: dispatch_ctx.continuation_of_execution_id.clone(),
                        prompt_system: prompt.system.clone(),
                        prompt_user: prompt.user.clone(),
                    },
                });

                // Identity guards apply to a follow-up execution exactly as they
                // do to an initial dispatch: it is a Task execution on the same
                // agent, holding the same concurrency slot. Dispatching past a
                // saturated agent minted an execution and a WorkspaceLease that
                // no executor ever picked up, and the heartbeat monitor reaped it
                // as an expired owner lease half a minute later, spending an
                // execution retry on work that never started.
                let agent = match db::AgentRepo::get_by_id(&*ctx.db, &agent_id).await {
                    Ok(Some(agent)) => agent,
                    Ok(None) => {
                        return HookResult::Failed {
                            reason: format!("agent not found: {agent_id}"),
                        };
                    }
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                };
                if agent.paused {
                    return HookResult::Skipped {
                        reason: "agent paused".to_string(),
                    };
                }
                match has_running_execution_capacity(&ctx.db, &agent).await {
                    Ok(true) => {}
                    Ok(false) => {
                        return HookResult::Skipped {
                            reason: "agent at capacity".to_string(),
                        };
                    }
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                }
                if let Some(parent_execution_id) = dispatch_ctx.continuation_of_execution_id.clone()
                {
                    let Some(task_executor) = ctx.task_executor.as_ref().cloned() else {
                        return HookResult::Failed {
                            reason: "task executor is not configured for follow-up dispatch"
                                .to_string(),
                        };
                    };
                    // Follow-up executions continue the workflow lifecycle, so the
                    // service used by the spawned run must keep the hook dependencies.
                    let mut service =
                        TaskService::new(Arc::clone(&ctx.db), Arc::clone(&ctx.event_bus))
                            .with_task_executor(task_executor);
                    if let Some(review_runner) = ctx.review_runner.as_ref().cloned() {
                        service = service.with_review_runner(review_runner);
                    }
                    if let Some(merge_service) = ctx.merge_service.as_ref().cloned() {
                        service = service.with_merge_service(merge_service);
                    }
                    if let Some(cleanup_scheduler) = ctx.cleanup_scheduler.as_ref().cloned() {
                        service = service.with_cleanup_scheduler(cleanup_scheduler);
                    }
                    service = service.with_workspace_root(ctx.workspace_root.clone());
                    if let Some(repo_cache_locks) = ctx.repo_cache_locks.as_ref().cloned() {
                        service = service.with_repo_cache_locks(repo_cache_locks);
                    }
                    if let Some(daemon_connections) = ctx.daemon_connections.as_ref().cloned() {
                        service = service.with_daemon_connections(daemon_connections);
                    }
                    if let Some(workspace_exec_locks) = ctx.workspace_exec_locks.as_ref().cloned() {
                        service = service.with_workspace_exec_locks(workspace_exec_locks);
                    }
                    if let Some(terminal_activity) = ctx.terminal_activity.as_ref().cloned() {
                        service = service.with_terminal_activity_tracker(terminal_activity);
                    }
                    let trigger = follow_up_trigger(ctx);
                    return match service
                        .dispatch_role_follow_up(
                            &ctx.task_id,
                            role_name,
                            parent_execution_id,
                            prompt.user,
                            trigger,
                        )
                        .await
                    {
                        Ok(execution) => {
                            if let Err(reason) =
                                ensure_review_record_for_dispatch(ctx, &execution.id).await
                            {
                                return HookResult::Failed { reason };
                            }
                            HookResult::Ok
                        }
                        Err(error) => HookResult::Failed {
                            reason: error.to_string(),
                        },
                    };
                }

                match db::ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id).await {
                    Ok(Some(project)) if project.paused_at.is_some() => {
                        return HookResult::Skipped {
                            reason: "project paused".to_string(),
                        };
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        return HookResult::Failed {
                            reason: format!("project not found: {}", ctx.project_id),
                        };
                    }
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                };

                let Some(task_executor) = ctx.task_executor.as_ref().cloned() else {
                    return HookResult::Failed {
                        reason: "task executor is not configured for initial role dispatch"
                            .to_string(),
                    };
                };
                let mut service = TaskService::new(Arc::clone(&ctx.db), Arc::clone(&ctx.event_bus))
                    .with_task_executor(task_executor)
                    .with_workspace_root(ctx.workspace_root.clone());
                if let Some(review_runner) = ctx.review_runner.as_ref().cloned() {
                    service = service.with_review_runner(review_runner);
                }
                if let Some(merge_service) = ctx.merge_service.as_ref().cloned() {
                    service = service.with_merge_service(merge_service);
                }
                if let Some(cleanup_scheduler) = ctx.cleanup_scheduler.as_ref().cloned() {
                    service = service.with_cleanup_scheduler(cleanup_scheduler);
                }
                if let Some(repo_cache_locks) = ctx.repo_cache_locks.as_ref().cloned() {
                    service = service.with_repo_cache_locks(repo_cache_locks);
                }
                if let Some(daemon_connections) = ctx.daemon_connections.as_ref().cloned() {
                    service = service.with_daemon_connections(daemon_connections);
                }
                if let Some(workspace_exec_locks) = ctx.workspace_exec_locks.as_ref().cloned() {
                    service = service.with_workspace_exec_locks(workspace_exec_locks);
                }
                if let Some(terminal_activity) = ctx.terminal_activity.as_ref().cloned() {
                    service = service.with_terminal_activity_tracker(terminal_activity);
                }

                match service
                    .dispatch_initial_role_execution_with_metadata(
                        &ctx.task_id,
                        &agent.id,
                        role_name,
                        prompt.user,
                        Some(dispatch_metadata),
                    )
                    .await
                {
                    Ok(execution) => {
                        if let Err(reason) =
                            ensure_review_record_for_dispatch(ctx, &execution.id).await
                        {
                            return HookResult::Failed { reason };
                        }
                        HookResult::Ok
                    }
                    Err(error) => HookResult::Failed {
                        reason: error.to_string(),
                    },
                }
            }
            Some(assignment)
                if assignment.assignee_type == Some(db::AssigneeKind::User)
                    && assignment.assignee_id.is_some() =>
            {
                if role_name == crate::workflow::default_roles::REVIEWER {
                    match latest_review(ctx).await {
                        Ok(Some(review))
                            if review.status == ReviewStatus::Failed
                                && Some(review.execution_id.as_str())
                                    == ctx.execution_id.as_deref() =>
                        {
                            return HookResult::Skipped {
                                reason: "review already failed".to_string(),
                            };
                        }
                        Ok(_) => {}
                        Err(reason) => return HookResult::Failed { reason },
                    }
                    if let Err(reason) = ensure_review_awaiting_human(ctx).await {
                        return HookResult::Failed { reason };
                    }
                }
                let assignee_id = assignment.assignee_id.expect("checked by match guard");
                ctx.event_bus.publish(ForgeEvent {
                    event_type: "task.awaiting_human".to_string(),
                    entity_id: ctx.task_id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskAwaitingHuman {
                        task_id: ctx.task_id.clone(),
                        role: role_name.to_string(),
                        assignee_id,
                        state: ctx.to_state.clone(),
                    },
                });

                HookResult::Ok
            }
            Some(_) => HookResult::Failed {
                reason: format!("invalid {} role assignment", role_name),
            },
            None => HookResult::Skipped {
                reason: format!("no {} role assigned", role_name),
            },
        }
    }
}

pub struct DispatchFixAgent;

#[async_trait]
impl HookAction for DispatchFixAgent {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        DispatchRoleAgent.execute(ctx).await
    }
}

pub struct DispatchExecutor;

#[async_trait]
impl HookAction for DispatchExecutor {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        DispatchRoleAgent.execute(ctx).await
    }
}

pub struct NotifyRoleHolder;

#[async_trait]
impl HookAction for NotifyRoleHolder {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let role_name = ctx
            .workflow
            .states
            .iter()
            .find(|state| state.name == ctx.from_state)
            .and_then(effective_role)
            .or_else(|| {
                ctx.workflow
                    .states
                    .iter()
                    .find(|state| state.kind == api_types::StateKind::Active)
                    .and_then(effective_role)
            });

        let Some(role_name) = role_name else {
            return HookResult::Skipped {
                reason: "no role to notify".to_string(),
            };
        };

        let current_task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };
        if current_task.blocked_json.is_some() {
            return HookResult::Skipped {
                reason: "task is blocked".to_string(),
            };
        }

        let assignment = match get_role_assignment(ctx, role_name).await {
            Ok(assignment) => assignment,
            Err(reason) => return HookResult::Failed { reason },
        };

        let notify_reason = format!("entered {}", ctx.to_state);

        match assignment {
            Some(assignment)
                if assignment.assignee_type == Some(db::AssigneeKind::Agent)
                    && assignment.assignee_id.is_some() =>
            {
                let agent_id = assignment.assignee_id.expect("checked by match guard");
                ctx.event_bus.publish(ForgeEvent {
                    event_type: "task.role_notified".to_string(),
                    entity_id: ctx.task_id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskRoleNotified {
                        task_id: ctx.task_id.clone(),
                        role: role_name.to_string(),
                        notified_agent_id: Some(agent_id),
                        notified_user_handle: None,
                        state: ctx.to_state.clone(),
                        reason: notify_reason,
                    },
                });

                HookResult::Ok
            }
            Some(assignment)
                if assignment.assignee_type == Some(db::AssigneeKind::User)
                    && assignment.assignee_id.is_some() =>
            {
                let user_handle = assignment.assignee_id.expect("checked by match guard");
                ctx.event_bus.publish(ForgeEvent {
                    event_type: "task.role_notified".to_string(),
                    entity_id: ctx.task_id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskRoleNotified {
                        task_id: ctx.task_id.clone(),
                        role: role_name.to_string(),
                        notified_agent_id: None,
                        notified_user_handle: Some(user_handle),
                        state: ctx.to_state.clone(),
                        reason: notify_reason,
                    },
                });

                HookResult::Ok
            }
            Some(_) => HookResult::Failed {
                reason: format!("invalid {} role assignment", role_name),
            },
            None => HookResult::Skipped {
                reason: "role not assigned".to_string(),
            },
        }
    }
}
