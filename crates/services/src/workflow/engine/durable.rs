//! Durable post-commit phase. CAS callers only insert its outbox row.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct HookDefinition {
    pub workflow: WorkflowDefinition,
    pub project_workflow_definition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HookPayload {
    pub from: String,
    pub to: String,
    pub actor: Actor,
    pub reason: String,
    pub transition_log_id: String,
    pub workflow_ref: crate::worker_runtime::queue::WorkflowReference,
    pub authority: Option<i64>,
    pub from_config: serde_json::Value,
    pub to_config: serde_json::Value,
    pub workspace_id: Option<String>,
    pub execution_id: Option<String>,
    pub agent_id: Option<String>,
    pub skip_before_enter: bool,
    pub skip_on_exit: bool,
    pub defer_dispatch_until: Option<String>,
    pub action_dispatch: bool,
    pub pre_results: Vec<api_types::HookResultEntry>,
    pub admission_agent_id: Option<String>,
    pub dispatch_index: Option<i64>,
    pub evidence: Option<String>,
}

pub(crate) struct HookPhaseResult {
    pub follow_up: Option<db::EnqueueTaskStep>,
    pub failure: Option<String>,
}

#[derive(Clone)]
pub(crate) struct HookAttempt {
    pub db: Arc<db::SqliteDb>,
    pub step: db::TaskStep,
    pub index: i64,
    pub interrupted: bool,
}
tokio::task_local! { static HOOK_ATTEMPT: HookAttempt; }
pub(crate) fn current_hook(task_id: &str) -> Option<HookAttempt> {
    HOOK_ATTEMPT
        .try_with(Clone::clone)
        .ok()
        .filter(|attempt| attempt.step.task_id == task_id)
}

pub(crate) async fn hook_effect(task_id: &str, key: &str) -> crate::Result<Option<String>> {
    match current_hook(task_id) {
        Some(a) => Ok(a.db.hook_effect(&a.step, a.index, key).await?),
        None => Ok(None),
    }
}
pub(crate) async fn record_hook_effect(
    task_id: &str,
    key: &str,
    value: &serde_json::Value,
) -> crate::Result<()> {
    if let Some(a) = current_hook(task_id) {
        a.db.record_hook_effect(&a.step, a.index, key, &value.to_string())
            .await?;
    }
    Ok(())
}

pub(crate) fn hooks_lane(workflow: &WorkflowDefinition, from: &str, to: &str) -> &'static str {
    let exit = workflow
        .states
        .iter()
        .find(|s| s.name == from)
        .into_iter()
        .flat_map(|s| &s.hooks.on_exit);
    let enter = workflow
        .states
        .iter()
        .find(|s| s.name == to)
        .into_iter()
        .flat_map(|s| {
            s.hooks
                .before_enter
                .iter()
                .chain(&s.hooks.on_enter)
                .chain(&s.hooks.after_enter)
        });
    if exit
        .chain(enter)
        .any(|h| matches!(h.action.as_str(), "run_merge" | "run_ci_steps"))
    {
        "long"
    } else {
        "fast"
    }
}

impl WorkflowEngine {
    async fn run_durable_hook(
        &self,
        step: &db::TaskStep,
        index: i64,
        hook: &api_types::HookSpec,
        ctx: &HookContext,
    ) -> crate::Result<HookResult> {
        let (recorded, interrupted) = self.db.start_hook(step, index).await?;
        if let Some(recorded) = recorded {
            return serde_json::from_str(&recorded)
                .map_err(|e| ServiceError::invalid_operation(e.to_string()));
        }
        let action = registry::resolve_action(&hook.action)?;
        let attempt = HookAttempt {
            db: self.db.clone(),
            step: step.clone(),
            index,
            interrupted,
        };
        let result = HOOK_ATTEMPT.scope(attempt, action.execute(ctx)).await;
        self.db
            .finish_hook(
                step,
                index,
                &serde_json::to_string(&result)
                    .map_err(|e| ServiceError::invalid_operation(e.to_string()))?,
            )
            .await?;
        Ok(result)
    }

    pub(crate) async fn execute_hook_step(
        &self,
        step: &db::TaskStep,
        payload: &HookPayload,
    ) -> crate::Result<HookPhaseResult> {
        let crate::worker_runtime::queue::WorkflowReference::Snapshot(id) = &payload.workflow_ref
        else {
            return Err(ServiceError::invalid_operation(
                "hook phase requires its committed workflow",
            ));
        };
        let definition: HookDefinition = serde_json::from_str(&self.db.step_workflow(id).await?)
            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
        let workflow = &definition.workflow;
        let from_state = Self::find_state(workflow, &payload.from).ok_or_else(|| {
            ServiceError::invalid_operation(Self::undefined_state_message(&payload.from, workflow))
        })?;
        let to_state = Self::find_state(workflow, &payload.to).ok_or_else(|| {
            ServiceError::invalid_operation(Self::undefined_state_message(&payload.to, workflow))
        })?;
        let mut task = TaskRepo::get_by_id(&*self.db, &step.task_id, false)
            .await?
            .ok_or(db::DbError::NotFound)?;
        if task.status != step.expected_status || !self.db.step_entry_matches(step).await? {
            return Err(db::DbError::VersionConflict.into());
        }
        let task_id = task.id.clone();
        let current_status = payload.from.clone();
        let target_state = payload.to.clone();
        let actor = payload.actor.clone();
        let reason = payload.reason.clone();
        let authority = match (
            payload.authority,
            definition.project_workflow_definition.as_ref(),
        ) {
            (Some(version), Some(workflow)) => Some(WorkflowAuthority {
                project_version: version,
                workflow_definition: workflow.clone(),
                clear_review_passed_at_on_commit: false,
            }),
            _ => None,
        };
        let transition_log_id = payload.transition_log_id.clone();
        let transition_log = TransitionLogRepo::list_by_task(&*self.db, &task_id)
            .await?
            .into_iter()
            .find(|t| t.id == transition_log_id)
            .ok_or(db::DbError::NotFound)?;
        let entry_barrier_started_at = transition_log.created_at.clone();
        let context = |state: &StateDefinition, config: serde_json::Value| HookContext {
            task_id: task.id.clone(),
            project_id: task.project_id.clone(),
            from_state: current_status.clone(),
            to_state: target_state.clone(),
            db: self.db.clone(),
            event_bus: self.event_bus.clone(),
            gate_config: state.gate_config.clone(),
            workflow: Arc::new(workflow.clone()),
            project_version: authority.as_ref().map(|a| a.project_version),
            project_workflow_definition: authority.as_ref().map(|a| a.workflow_definition.clone()),
            triggered_by: actor.clone(),
            review_runner: self.review_runner.clone(),
            merge_service: self.merge_service.clone(),
            cleanup_scheduler: self.cleanup_scheduler.clone(),
            task_service: self.task_service.clone(),
            daemon_connections: self.daemon_connections.clone(),
            workspace_exec_locks: self.workspace_exec_locks.clone(),
            terminal_activity: self.terminal_activity.clone(),
            workspace_root: self.workspace_root.clone(),
            repo_cache_locks: self.repo_cache_locks.clone(),
            workspace_backend_router: self.workspace_backend_router.clone(),
            workspace_id: payload.workspace_id.clone(),
            agent_id: payload.agent_id.clone(),
            execution_id: payload.execution_id.clone(),
            state_config: config,
        };
        let exit_ctx = context(from_state, payload.from_config.clone());
        let enter_ctx = context(to_state, payload.to_config.clone());
        let mut hook_results = payload.pre_results.clone();
        let mut failure = None;
        let mut cascade = None;
        let mut cascade_skip_before_exit = false;
        let mut before_enter_rejection_cascade = false;
        let mut skip_target_enter_hooks = false;
        let review_refresh_bridge = current_status == crate::workflow::default_states::MERGING
            && target_state == crate::workflow::default_states::MERGE_FAILED
            && reason.contains(crate::workflow::REVIEW_REFRESH_MARKER)
            && actor == crate::worker_runtime::queue::cascade_actor();
        let defer_dispatch_until = payload.defer_dispatch_until.clone();
        let action_dispatch = payload.action_dispatch;
        let should_defer_dispatch = (defer_dispatch_until.is_some() || action_dispatch)
            && (to_state.kind != StateKind::Active || action_dispatch)
            && to_state.hooks.on_enter.iter().any(|hook| {
                matches!(
                    hook.action.as_str(),
                    "dispatch_role_agent" | "dispatch_fix_agent" | "dispatch_executor"
                )
            });
        for (index, hook) in from_state
            .hooks
            .on_exit
            .iter()
            .enumerate()
            .filter(|_| !payload.skip_on_exit)
        {
            if !hook_audience_matches(hook.applies_to, &actor) {
                log_hook_skipped_by_audience(
                    &task.id,
                    &current_status,
                    &target_state,
                    "on_exit",
                    hook,
                    &actor,
                );
                continue;
            }

            log_hook_start(
                &task.id,
                &current_status,
                &target_state,
                "on_exit",
                hook,
                &actor,
            );
            let started = Instant::now();
            let result = self
                .run_durable_hook(step, index as i64, hook, &exit_ctx)
                .await?;
            self.refresh_task_after_hook(&mut task, &target_state, Some(step))
                .await?;
            let duration_ms = elapsed_ms(started);
            log_hook_result(
                &task.id,
                &current_status,
                &target_state,
                "on_exit",
                hook,
                &result,
                duration_ms,
            );
            hook_results.push(hook_result_entry(
                &hook.action,
                "on_exit",
                &result,
                duration_ms,
            ));

            match result {
                HookResult::Failed { reason: error } => {
                    failure.get_or_insert_with(|| error.clone());
                    tracing::warn!(
                        action = %hook.action,
                        task_id = %task.id,
                        from_state = %current_status,
                        to_state = %target_state,
                        %error,
                        "workflow effect failed on_exit"
                    );
                    self.event_bus.publish(ForgeEvent {
                        event_type: "transition.effect_failed".to_string(),
                        entity_id: task.id.clone(),
                        timestamp: event_timestamp(),
                        context: EventContext::TransitionEffectFailed {
                            task_id: task.id.clone(),
                            from_state: current_status.clone(),
                            to_state: target_state.clone(),
                            action: hook.action.clone(),
                            error,
                        },
                    });
                }
                HookResult::Cascade {
                    to,
                    reason: cascade_reason,
                } => {
                    cascade = Some((to, cascade_reason));
                    break;
                }
                HookResult::Ok | HookResult::Skipped { .. } => {}
            }
        }

        if !payload.skip_before_enter && cascade.is_none() && !review_refresh_bridge {
            for (index, hook) in to_state.hooks.before_enter.iter().enumerate() {
                if !hook_audience_matches(hook.applies_to, &actor) {
                    log_hook_skipped_by_audience(
                        &task.id,
                        &current_status,
                        &target_state,
                        "before_enter",
                        hook,
                        &actor,
                    );
                    continue;
                }

                log_hook_start(
                    &task.id,
                    &current_status,
                    &target_state,
                    "before_enter",
                    hook,
                    &actor,
                );
                let started = Instant::now();
                let result = self
                    .run_durable_hook(
                        step,
                        (index + from_state.hooks.on_exit.len()) as i64,
                        hook,
                        &enter_ctx,
                    )
                    .await?;
                self.refresh_task_after_hook(&mut task, &target_state, Some(step))
                    .await?;
                let duration_ms = elapsed_ms(started);
                log_hook_result(
                    &task.id,
                    &current_status,
                    &target_state,
                    "before_enter",
                    hook,
                    &result,
                    duration_ms,
                );
                hook_results.push(hook_result_entry(
                    &hook.action,
                    "before_enter",
                    &result,
                    duration_ms,
                ));

                match result {
                    HookResult::Failed { reason: error } => {
                        failure.get_or_insert_with(|| error.clone());
                        tracing::warn!(
                            action = %hook.action,
                            task_id = %task.id,
                            from_state = %current_status,
                            to_state = %target_state,
                            %error,
                            "workflow effect failed before_enter"
                        );
                        self.event_bus.publish(ForgeEvent {
                            event_type: "transition.effect_failed".to_string(),
                            entity_id: task.id.clone(),
                            timestamp: event_timestamp(),
                            context: EventContext::TransitionEffectFailed {
                                task_id: task.id.clone(),
                                from_state: current_status.clone(),
                                to_state: target_state.clone(),
                                action: hook.action.clone(),
                                error: error.clone(),
                            },
                        });

                        if let Some(settled) = self
                            .settle_failed_ci_entry(
                                &task,
                                &hook.action,
                                &actor,
                                &entry_barrier_started_at,
                                authority.as_ref(),
                            )
                            .await?
                        {
                            task = settled.task;
                            cascade = settled.cascade.map(|(target, _)| (target, error.clone()));
                            before_enter_rejection_cascade = cascade.is_some();
                            skip_target_enter_hooks = true;
                            break;
                        }

                        if matches!(hook.on_failure, FailurePolicy::Block) {
                            if target_state == crate::workflow::default_states::REVIEW {
                                if let Some(reject_target) = to_state
                                    .gate_config
                                    .as_ref()
                                    .and_then(|config| config.reject_target.clone())
                                {
                                    let existing_rejections =
                                        crate::task_diagnostics::count_gate_rejections_for_task(
                                            &self.db,
                                            &task_id,
                                            &target_state,
                                        )
                                        .await?;
                                    let max_rejections = to_state
                                        .gate_config
                                        .as_ref()
                                        .and_then(|gc| gc.max_rejections)
                                        .unwrap_or(i32::MAX);

                                    if existing_rejections + 1 >= i64::from(max_rejections) {
                                        let blocked_at = now_rfc3339();
                                        let barrier = serde_json::json!({
                                            "state": target_state.as_str(),
                                            "status": "blocked",
                                            "started_at": entry_barrier_started_at.as_str(),
                                            "updated_at": blocked_at.as_str(),
                                            "blocking_reason": "review retry budget exhausted",
                                        })
                                        .to_string();
                                        task = self
                                            .set_entry_barrier_with_authority(
                                                &task_id,
                                                task.version,
                                                Some(barrier),
                                                &blocked_at,
                                                authority.as_ref(),
                                            )
                                            .await?;
                                        skip_target_enter_hooks = true;
                                    } else {
                                        let clear_updated_at = now_rfc3339();
                                        task = self
                                            .set_entry_barrier_with_authority(
                                                &task_id,
                                                task.version,
                                                None,
                                                &clear_updated_at,
                                                authority.as_ref(),
                                            )
                                            .await?;
                                        before_enter_rejection_cascade = true;
                                        cascade = Some((reject_target, error));
                                    }
                                } else {
                                    let blocked_at = now_rfc3339();
                                    let barrier = serde_json::json!({
                                        "state": target_state.as_str(),
                                        "status": "blocked",
                                        "started_at": entry_barrier_started_at.as_str(),
                                        "updated_at": blocked_at.as_str(),
                                        "blocking_reason": error.as_str(),
                                    })
                                    .to_string();
                                    task = self
                                        .set_entry_barrier_with_authority(
                                            &task_id,
                                            task.version,
                                            Some(barrier),
                                            &blocked_at,
                                            authority.as_ref(),
                                        )
                                        .await?;
                                    skip_target_enter_hooks = true;
                                }
                            } else {
                                let blocked_at = now_rfc3339();
                                let barrier = serde_json::json!({
                                    "state": target_state.as_str(),
                                    "status": "blocked",
                                    "started_at": entry_barrier_started_at.as_str(),
                                    "updated_at": blocked_at.as_str(),
                                    "blocking_reason": error.as_str(),
                                })
                                .to_string();
                                task = self
                                    .set_entry_barrier_with_authority(
                                        &task_id,
                                        task.version,
                                        Some(barrier),
                                        &blocked_at,
                                        authority.as_ref(),
                                    )
                                    .await?;
                                skip_target_enter_hooks = true;
                            }
                            break;
                        }
                    }
                    HookResult::Cascade {
                        to,
                        reason: cascade_reason,
                    } => {
                        cascade = Some((to, cascade_reason));
                        break;
                    }
                    HookResult::Ok | HookResult::Skipped { .. } => {}
                }
            }
        }

        if payload.skip_on_exit && cascade.is_none() && !skip_target_enter_hooks {
            task = self
                .set_entry_barrier_with_authority(
                    &task_id,
                    task.version,
                    None,
                    &now_rfc3339(),
                    authority.as_ref(),
                )
                .await?;
            task = TaskRepo::update(
                &*self.db,
                UpdateTask {
                    id: task.id.clone(),
                    expected_version: task.version,
                    title: None,
                    description: None,
                    priority: None,
                    merge_config: None,
                    plan: None,
                    error_annotation: Some(None),
                    blocked_json: Some(None),
                    failed_json: None,
                    task_state_config: None,
                    parent_task_id: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await?;
        }

        if cascade.is_none() && !skip_target_enter_hooks {
            for (index, hook) in to_state.hooks.on_enter.iter().enumerate() {
                if !hook_audience_matches(hook.applies_to, &actor) {
                    log_hook_skipped_by_audience(
                        &task.id,
                        &current_status,
                        &target_state,
                        "on_enter",
                        hook,
                        &actor,
                    );
                    continue;
                }
                if should_defer_dispatch
                    && matches!(
                        hook.action.as_str(),
                        "dispatch_role_agent" | "dispatch_fix_agent" | "dispatch_executor"
                    )
                {
                    let result = HookResult::Skipped {
                        reason: "dispatch deferred to Task dispatcher".to_owned(),
                    };
                    log_hook_result(
                        &task.id,
                        &current_status,
                        &target_state,
                        "on_enter",
                        hook,
                        &result,
                        0,
                    );
                    hook_results.push(hook_result_entry(&hook.action, "on_enter", &result, 0));
                    // The deferred dispatch is executed later by the task
                    // dispatcher, which skips tasks carrying a
                    // dispatch_failed annotation — the user's drag is an
                    // explicit restart, so drop the stale annotation now.
                    if is_dispatch_failed_annotation(task.error_annotation.as_deref()) {
                        if let Err(error) = clear_dispatch_failure_annotation(
                            &self.db,
                            &task_id,
                            authority.as_ref(),
                        )
                        .await
                        {
                            tracing::warn!(
                                task_id = %task.id,
                                %error,
                                "failed to clear dispatch failure annotation before deferred dispatch"
                            );
                        } else {
                            task = TaskRepo::get_by_id(&*self.db, &task_id, false)
                                .await?
                                .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
                        }
                    }
                    continue;
                }

                log_hook_start(
                    &task.id,
                    &current_status,
                    &target_state,
                    "on_enter",
                    hook,
                    &actor,
                );
                let started = Instant::now();
                let result = self
                    .run_durable_hook(
                        step,
                        (index + from_state.hooks.on_exit.len() + to_state.hooks.before_enter.len())
                            as i64,
                        hook,
                        &enter_ctx,
                    )
                    .await?;
                self.refresh_task_after_hook(&mut task, &target_state, Some(step))
                    .await?;
                let duration_ms = elapsed_ms(started);
                log_hook_result(
                    &task.id,
                    &current_status,
                    &target_state,
                    "on_enter",
                    hook,
                    &result,
                    duration_ms,
                );
                hook_results.push(hook_result_entry(
                    &hook.action,
                    "on_enter",
                    &result,
                    duration_ms,
                ));

                match result {
                    HookResult::Failed { reason: error } => {
                        failure.get_or_insert_with(|| error.clone());
                        tracing::warn!(
                            action = %hook.action,
                            task_id = %task.id,
                            from_state = %current_status,
                            to_state = %target_state,
                            %error,
                            "workflow effect failed on_enter"
                        );
                        self.event_bus.publish(ForgeEvent {
                            event_type: "transition.effect_failed".to_string(),
                            entity_id: task.id.clone(),
                            timestamp: event_timestamp(),
                            context: EventContext::TransitionEffectFailed {
                                task_id: task.id.clone(),
                                from_state: current_status.clone(),
                                to_state: target_state.clone(),
                                action: hook.action.clone(),
                                error: error.clone(),
                            },
                        });

                        // A failed dispatch entering an active state means
                        // no execution is driving the task; left alone it
                        // would sit there looking in-flight forever. Roll
                        // it back to the workflow's initial state and
                        // record the dispatch error on the task.
                        if to_state.kind == StateKind::Active
                            && registry::is_dispatch_action(&hook.action)
                        {
                            let fallback = workflow
                                .states
                                .iter()
                                .find(|state| state.kind == StateKind::Initial)
                                .map(|state| state.name.clone());
                            if let Some(fallback) = fallback {
                                if !has_running_execution(&self.db, &task.id).await? {
                                    if let Err(annotate_error) = annotate_dispatch_failure(
                                        &self.db,
                                        &task.id,
                                        &target_state,
                                        &error,
                                        authority.as_ref(),
                                    )
                                    .await
                                    {
                                        tracing::warn!(
                                            task_id = %task.id,
                                            %annotate_error,
                                            "failed to record dispatch failure annotation"
                                        );
                                    }
                                    tracing::warn!(
                                        task_id = %task.id,
                                        state = %target_state,
                                        fallback = %fallback,
                                        %error,
                                        "dispatch failed entering active state; rolling task back"
                                    );
                                    cascade = Some((
                                        fallback,
                                        format!("dispatch failed entering {target_state}: {error}"),
                                    ));
                                    cascade_skip_before_exit = true;
                                    break;
                                }
                            }
                        }
                    }
                    HookResult::Cascade {
                        to,
                        reason: cascade_reason,
                    } => {
                        cascade = Some((to, cascade_reason));
                        break;
                    }
                    HookResult::Ok => {
                        // A dispatch that succeeds again invalidates any
                        // stale dispatch_failed annotation, un-parking the
                        // task for the task dispatcher.
                        if registry::is_dispatch_action(&hook.action)
                            && is_dispatch_failed_annotation(task.error_annotation.as_deref())
                        {
                            if let Err(error) = clear_dispatch_failure_annotation(
                                &self.db,
                                &task_id,
                                authority.as_ref(),
                            )
                            .await
                            {
                                tracing::warn!(
                                    task_id = %task.id,
                                    %error,
                                    "failed to clear dispatch failure annotation after successful dispatch"
                                );
                            } else {
                                task = TaskRepo::get_by_id(&*self.db, &task_id, false)
                                    .await?
                                    .ok_or_else(|| {
                                        ServiceError::not_found("task", task_id.clone())
                                    })?;
                            }
                        }
                    }
                    HookResult::Skipped { .. } => {}
                }
            }
        }

        if cascade.is_none() && !skip_target_enter_hooks {
            let effective_after_enter_hooks = effective_after_enter_hooks(to_state);
            for (index, hook) in effective_after_enter_hooks.iter().enumerate() {
                if !hook_audience_matches(hook.applies_to, &actor) {
                    log_hook_skipped_by_audience(
                        &task.id,
                        &current_status,
                        &target_state,
                        "after_enter",
                        hook,
                        &actor,
                    );
                    continue;
                }

                log_hook_start(
                    &task.id,
                    &current_status,
                    &target_state,
                    "after_enter",
                    hook,
                    &actor,
                );
                let started = Instant::now();
                let result = self
                    .run_durable_hook(
                        step,
                        (index
                            + from_state.hooks.on_exit.len()
                            + to_state.hooks.before_enter.len()
                            + to_state.hooks.on_enter.len()) as i64,
                        hook,
                        &enter_ctx,
                    )
                    .await?;
                self.refresh_task_after_hook(&mut task, &target_state, Some(step))
                    .await?;
                let duration_ms = elapsed_ms(started);
                log_hook_result(
                    &task.id,
                    &current_status,
                    &target_state,
                    "after_enter",
                    hook,
                    &result,
                    duration_ms,
                );
                hook_results.push(hook_result_entry(
                    &hook.action,
                    "after_enter",
                    &result,
                    duration_ms,
                ));

                match result {
                    HookResult::Failed { reason: error } => {
                        failure.get_or_insert_with(|| error.clone());
                        tracing::warn!(
                            action = %hook.action,
                            task_id = %task.id,
                            from_state = %current_status,
                            to_state = %target_state,
                            %error,
                            "workflow validator failed"
                        );
                        self.event_bus.publish(ForgeEvent {
                            event_type: "transition.effect_failed".to_string(),
                            entity_id: task.id.clone(),
                            timestamp: event_timestamp(),
                            context: EventContext::TransitionEffectFailed {
                                task_id: task.id.clone(),
                                from_state: current_status.clone(),
                                to_state: target_state.clone(),
                                action: hook.action.clone(),
                                error,
                            },
                        });
                    }
                    HookResult::Cascade {
                        to,
                        reason: cascade_reason,
                    } => {
                        cascade = Some((to, cascade_reason));
                        break;
                    }
                    HookResult::Ok | HookResult::Skipped { .. } => {}
                }
            }
        }

        if to_state.kind == StateKind::Terminal && self.cleanup_scheduler.is_some() {
            let delay = match workflow.cleanup_policy_for(&target_state) {
                Some(api_types::CleanupPolicy::Delayed { seconds }) => {
                    std::time::Duration::from_secs(seconds)
                }
                _ => std::time::Duration::ZERO,
            };
            let index = (from_state.hooks.on_exit.len()
                + to_state.hooks.before_enter.len()
                + to_state.hooks.on_enter.len()
                + effective_after_enter_hooks(to_state).len()) as i64;
            let (recorded, _) = self.db.start_hook(step, index).await?;
            if recorded.is_none() {
                let deadline = (chrono::Utc::now()
                    + chrono::Duration::from_std(delay)
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?)
                .to_rfc3339();
                let mut tx = db::begin_immediate(self.db.pool()).await?;
                self.db.fence_hook_in_tx(&mut tx, step).await?;
                sqlx::query("UPDATE workspace SET cleanup_after=COALESCE(cleanup_after,?),updated_at=? WHERE task_id=? AND status!='cleaned'")
                        .bind(deadline).bind(now_rfc3339()).bind(&task.id).execute(&mut *tx).await?;
                sqlx::query("UPDATE task_hook_checkpoint SET result_json=? WHERE step_id=? AND hook_index=?")
                        .bind(serde_json::to_string(&HookResult::Ok).expect("outcome serializes")).bind(&step.id).bind(index).execute(&mut *tx).await?;
                tx.commit().await?;
            }
        }

        if let Ok(payload) = serde_json::to_string(&hook_results) {
            if let Err(error) =
                TransitionLogRepo::update_hook_results(&*self.db, &transition_log.id, &payload)
                    .await
            {
                tracing::warn!(
                    task_id = %task.id,
                    transition_log_id = %transition_log.id,
                    %error,
                    "workflow failed to persist hook results"
                );
            } else {
                let memory_service = crate::MemoryService::new(Arc::clone(&self.db));
                if let Err(error) = memory_service
                    .record_transition_if_failure(&task.project_id, &transition_log, Some(&payload))
                    .await
                {
                    tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
                }
            }
        }

        let mut follow_up = None;
        if let Some((cascade_to, cascade_reason)) = cascade {
            if to_state.kind == StateKind::Gate
                && to_state
                    .gate_config
                    .as_ref()
                    .is_some_and(|gate_config| gate_config.requires_user_approval())
                && !before_enter_rejection_cascade
                && !to_state.gate_config.as_ref().is_some_and(|gate_config| {
                    gate_config.optional_when_unassigned()
                        && cascade_reason.starts_with("gate skipped:")
                })
            {
                tracing::info!(
                    task_id = %task.id,
                    state = %target_state,
                    cascade_to = %cascade_to,
                    cascade_reason = %cascade_reason,
                    "workflow cascade paused because gate requires user approval"
                );
                return Ok(HookPhaseResult {
                    follow_up: None,
                    failure,
                });
            }

            let cascade_rejection = before_enter_rejection_cascade
                || (to_state.kind == StateKind::Gate
                    && !cascade_reason.starts_with("gate skipped:")
                    && !cascade_reason.contains(crate::workflow::REVIEW_REFRESH_MARKER)
                    && !cascade_reason.contains(crate::workflow::CONFLICT_HANDOFF_MARKER)
                    && !Self::is_terminal(workflow, &cascade_to));
            // Fence the entry this transition committed, not a later
            // writer's: its epoch was recorded on its log row.
            let entry_epoch: Option<i64> =
                sqlx::query_scalar("SELECT status_epoch FROM transition_log WHERE id = ?")
                    .bind(&transition_log_id)
                    .fetch_optional(self.db.pool())
                    .await?
                    .flatten();
            let entry_epoch = entry_epoch.ok_or_else(|| {
                ServiceError::invalid_operation("committed transition has no status epoch")
            })?;
            let input = self
                .cascade_step_input(
                    &task,
                    workflow,
                    cascade_to,
                    cascade_reason,
                    cascade_rejection,
                    cascade_skip_before_exit,
                    authority.clone(),
                    Some(step),
                    format!("hooks:{}:cascade", step.id),
                    Some(entry_epoch),
                )
                .await?;
            follow_up = Some(input);
        }

        crate::deferred_dispatch::finish_machine_wait(
            &self.db,
            &mut task,
            step.expected_version - 1,
        )
        .await?;
        Ok(HookPhaseResult { follow_up, failure })
    }
}
