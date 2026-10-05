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
    #[serde(flatten)]
    pub bridge: api_types::TransitionBridge,
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
    /// First hook failure of any policy: the step settles `failed` with it.
    pub failure: Option<String>,
    /// First failure of a `Block`-policy hook. It annotates the Task; any
    /// other `Log`-policy failure is logged and leaves the Task alone.
    pub blocking_failure: Option<String>,
    /// First `run_merge` failure, whatever its policy. A merge that cannot
    /// complete must never leave the Task silently in its merge state, so it
    /// annotates the Task as a merge failure.
    pub merge_failure: Option<String>,
    /// `run_merge` hit a transient error. Its checkpoint stays open and no
    /// later hook ran; the step queue retries the step with back-off.
    pub retry: Option<String>,
}

impl HookPhaseResult {
    fn retry(reason: String) -> Self {
        Self {
            follow_up: None,
            failure: Some(reason.clone()),
            blocking_failure: None,
            merge_failure: Some(reason.clone()),
            retry: Some(reason),
        }
    }
}

/// A hook's settled result, or a transient `run_merge` failure to retry.
pub(crate) enum DurableHook {
    Done(HookResult),
    Retry(String),
}

#[derive(Clone)]
pub(crate) struct HookAttempt {
    pub db: Arc<db::SqliteDb>,
    pub step: db::TaskStep,
    pub index: i64,
    pub interrupted: bool,
    /// Set when the hook's failure came from a transient error.
    pub transient: Arc<std::sync::atomic::AtomicBool>,
}
tokio::task_local! { static HOOK_ATTEMPT: HookAttempt; }
pub(crate) fn current_hook(task_id: &str) -> Option<HookAttempt> {
    HOOK_ATTEMPT
        .try_with(Clone::clone)
        .ok()
        .filter(|attempt| attempt.step.task_id == task_id)
}

/// Classify a hook failure with the step queue's retry rule.
pub(crate) fn note_hook_failure(task_id: &str, error: &ServiceError) {
    if let Some(attempt) = current_hook(task_id) {
        if crate::worker_runtime::queue::retryable(error) {
            attempt
                .transient
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }
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

impl WorkflowExecution<'_> {
    async fn run_durable_hook(
        &self,
        step: &db::TaskStep,
        index: i64,
        hook: &api_types::HookSpec,
        ctx: &HookContext,
    ) -> crate::Result<DurableHook> {
        let (recorded, interrupted) = self.db.start_hook(step, index).await?;
        if let Some(recorded) = recorded {
            return serde_json::from_str(&recorded)
                .map(DurableHook::Done)
                .map_err(|e| ServiceError::invalid_operation(e.to_string()));
        }
        let action = registry::resolve_action(&hook.action)?;
        let transient = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let attempt = HookAttempt {
            db: self.db.clone(),
            step: step.clone(),
            index,
            interrupted,
            transient: transient.clone(),
        };
        let result = HOOK_ATTEMPT.scope(attempt, action.execute(ctx)).await;
        if let HookResult::Failed { reason } = &result {
            if hook.action == "run_merge" && transient.load(std::sync::atomic::Ordering::Relaxed) {
                // Leave the checkpoint open: the retried step resumes this
                // hook from its recorded effects.
                return Ok(DurableHook::Retry(reason.clone()));
            }
        }
        self.db
            .finish_hook(
                step,
                index,
                &serde_json::to_string(&result)
                    .map_err(|e| ServiceError::invalid_operation(e.to_string()))?,
            )
            .await?;
        Ok(DurableHook::Done(result))
    }

    /// Typed failure evidence for failure memory once this transition's
    /// hooks settled: the Task's interruption kind, and whether the
    /// review-verdict hook acted on a failed Review.
    async fn transition_failure_evidence(
        &self,
        task: &db::Task,
        hook_results: &[api_types::HookResultEntry],
    ) -> crate::Result<crate::memory::TransitionFailureEvidence> {
        let review_failed = hook_results
            .iter()
            .any(|entry| entry.action == "auto_cascade_on_review_pass")
            && latest_review(&self.db, &task.id)
                .await?
                .is_some_and(|review| review.status == db::ReviewStatus::Failed);
        Ok(crate::memory::TransitionFailureEvidence {
            failure_kind: crate::memory::TransitionFailureEvidence::annotation_kind(
                task.error_annotation.as_deref(),
            ),
            review_failed,
        })
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
        let from_state = WorkflowEngine::find_state(workflow, &payload.from).ok_or_else(|| {
            ServiceError::invalid_operation(WorkflowEngine::undefined_state_message(
                &payload.from,
                workflow,
            ))
        })?;
        let to_state = WorkflowEngine::find_state(workflow, &payload.to).ok_or_else(|| {
            ServiceError::invalid_operation(WorkflowEngine::undefined_state_message(
                &payload.to,
                workflow,
            ))
        })?;
        let mut task = TaskRepo::get_by_id(&*self.db, &step.task_id, false)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let task_id = task.id.clone();
        let current_status = payload.from.clone();
        let target_state = payload.to.clone();
        let actor = payload.actor.clone();
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
        let bridge = &transition_log.bridge;
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
            review_runner: self.task_service.review_runner.clone(),
            merge_service: self.task_service.merge_service.clone(),
            cleanup_scheduler: self.task_service.cleanup_scheduler.clone(),
            task_service: self.task_service.clone(),
            daemon_connections: self.task_service.daemon_connections.clone(),
            workspace_exec_locks: self.task_service.workspace_exec_locks.clone(),
            terminal_activity: self.task_service.terminal_activity.clone(),
            workspace_root: self.task_service.workspace_root.clone(),
            repo_cache_locks: self.task_service.repo_cache_locks.clone(),
            workspace_backend_router: self.task_service.workspace_backend_router.clone(),
            workspace_id: payload.workspace_id.clone(),
            agent_id: payload.agent_id.clone(),
            execution_id: payload.execution_id.clone(),
            state_config: config,
        };
        let exit_ctx = context(from_state, payload.from_config.clone());
        let enter_ctx = context(to_state, payload.to_config.clone());
        let mut hook_results = payload.pre_results.clone();
        let mut failure = None;
        let mut blocking_failure = None;
        let mut merge_failure = None;
        let mut cascade = None;
        let mut cascade_skip_before_exit = false;
        let mut before_enter_rejection_cascade = false;
        let mut skip_target_enter_hooks = false;
        let review_refresh_bridge = current_status == crate::workflow::default_states::MERGING
            && target_state == crate::workflow::default_states::MERGE_FAILED
            && bridge.is_review_refresh()
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
            let result = match self
                .run_durable_hook(step, index as i64, hook, &exit_ctx)
                .await?
            {
                DurableHook::Done(result) => result,
                DurableHook::Retry(reason) => return Ok(HookPhaseResult::retry(reason)),
            };
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
                    if matches!(hook.on_failure, FailurePolicy::Block) {
                        blocking_failure.get_or_insert_with(|| error.clone());
                    }
                    if hook.action == "run_merge" {
                        merge_failure.get_or_insert_with(|| error.clone());
                    }
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

                    bridge: cascade_bridge,
                } => {
                    cascade = Some((to, cascade_reason, cascade_bridge));
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
                let result = match self
                    .run_durable_hook(
                        step,
                        (index + from_state.hooks.on_exit.len()) as i64,
                        hook,
                        &enter_ctx,
                    )
                    .await?
                {
                    DurableHook::Done(result) => result,
                    DurableHook::Retry(reason) => return Ok(HookPhaseResult::retry(reason)),
                };
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
                        if matches!(hook.on_failure, FailurePolicy::Block) {
                            blocking_failure.get_or_insert_with(|| error.clone());
                        }
                        if hook.action == "run_merge" {
                            merge_failure.get_or_insert_with(|| error.clone());
                        }
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
                            cascade = settled
                                .cascade
                                .map(|(target, _)| (target, error.clone(), Default::default()));
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
                                    let cancelled = latest_review(&self.db, &task.id)
                                        .await?
                                        .is_some_and(|r| r.status == db::ReviewStatus::Cancelled);
                                    if cancelled {
                                        if actor.is_user()
                                            || db::budget::cancelled_review_entry_allows_retry(
                                                &self.db,
                                                &task,
                                                to_state.gate_config.as_ref(),
                                            )
                                            .await?
                                        {
                                            task = self
                                                .set_entry_barrier_with_authority(
                                                    &task_id,
                                                    task.version,
                                                    None,
                                                    &now_rfc3339(),
                                                    authority.as_ref(),
                                                )
                                                .await?;
                                            before_enter_rejection_cascade = true;
                                            cascade =
                                                Some((reject_target, error, Default::default()));
                                        }
                                    } else {
                                        let max_rejections = db::budget::task_limit(
                                            &self.db,
                                            &task,
                                            db::budget::Kind::Review,
                                            Some(&enter_ctx.state_config),
                                            to_state.gate_config.as_ref(),
                                        )
                                        .await?;
                                        let identity = format!("entry:{}:{}", step.id, index);
                                        let (settled, allowed) = db::budget::failed_review_entry(
                                            &self.db,
                                            &task,
                                            i64::from(max_rejections),
                                            &identity,
                                            &entry_barrier_started_at,
                                            actor.is_user(),
                                        )
                                        .await?;
                                        task = settled;
                                        if allowed {
                                            before_enter_rejection_cascade = true;
                                            cascade =
                                                Some((reject_target, error, Default::default()));
                                        }
                                    }
                                    skip_target_enter_hooks = true;
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

                        bridge: cascade_bridge,
                    } => {
                        cascade = Some((to, cascade_reason, cascade_bridge));
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
                let result = match self
                    .run_durable_hook(
                        step,
                        (index + from_state.hooks.on_exit.len() + to_state.hooks.before_enter.len())
                            as i64,
                        hook,
                        &enter_ctx,
                    )
                    .await?
                {
                    DurableHook::Done(result) => result,
                    DurableHook::Retry(reason) => return Ok(HookPhaseResult::retry(reason)),
                };
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
                        if matches!(hook.on_failure, FailurePolicy::Block) {
                            blocking_failure.get_or_insert_with(|| error.clone());
                        }
                        if hook.action == "run_merge" {
                            merge_failure.get_or_insert_with(|| error.clone());
                        }
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
                                        Default::default(),
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

                        bridge: cascade_bridge,
                    } => {
                        cascade = Some((to, cascade_reason, cascade_bridge));
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
                let result = match self
                    .run_durable_hook(
                        step,
                        (index
                            + from_state.hooks.on_exit.len()
                            + to_state.hooks.before_enter.len()
                            + to_state.hooks.on_enter.len()) as i64,
                        hook,
                        &enter_ctx,
                    )
                    .await?
                {
                    DurableHook::Done(result) => result,
                    DurableHook::Retry(reason) => return Ok(HookPhaseResult::retry(reason)),
                };
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
                        if matches!(hook.on_failure, FailurePolicy::Block) {
                            blocking_failure.get_or_insert_with(|| error.clone());
                        }
                        if hook.action == "run_merge" {
                            merge_failure.get_or_insert_with(|| error.clone());
                        }
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

                        bridge: cascade_bridge,
                    } => {
                        cascade = Some((to, cascade_reason, cascade_bridge));
                        break;
                    }
                    HookResult::Ok | HookResult::Skipped { .. } => {}
                }
            }
        }

        if to_state.kind == StateKind::Terminal && self.task_service.cleanup_scheduler.is_some() {
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
                let recorded = match self.transition_failure_evidence(&task, &hook_results).await {
                    Ok(evidence) => {
                        memory_service
                            .record_transition_if_failure(
                                &task.project_id,
                                &transition_log,
                                Some(&payload),
                                evidence,
                            )
                            .await
                    }
                    Err(error) => Err(error),
                };
                if let Err(error) = recorded {
                    tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
                }
            }
        }

        let mut follow_up = None;
        if let Some((cascade_to, cascade_reason, cascade_bridge)) = cascade {
            if to_state.kind == StateKind::Gate
                && to_state
                    .gate_config
                    .as_ref()
                    .is_some_and(|gate_config| gate_config.requires_user_approval())
                && !before_enter_rejection_cascade
                && !to_state.gate_config.as_ref().is_some_and(|gate_config| {
                    gate_config.optional_when_unassigned()
                        && cascade_bridge.bridge_kind
                            == Some(api_types::TransitionBridgeKind::GateSkipped)
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
                    blocking_failure,
                    merge_failure,
                    retry: None,
                });
            }

            let cascade_rejection = before_enter_rejection_cascade
                || (to_state.kind == StateKind::Gate
                    && cascade_bridge.bridge_kind
                        != Some(api_types::TransitionBridgeKind::GateSkipped)
                    && !cascade_bridge.is_review_refresh()
                    && cascade_bridge.bridge_kind
                        != Some(api_types::TransitionBridgeKind::ConflictHandoff)
                    && !WorkflowEngine::is_terminal(workflow, &cascade_to));
            // Fence the entry this transition committed, not a later
            // writer's. The hook row carries that entry's epoch (read inside
            // the status CAS, or by the startup recovery sweep for an entry
            // a pre-upgrade binary logged without one), and the step fence
            // has already proved it is still current.
            let entry_epoch = step.expected_epoch;
            let input = self
                .cascade_step_input(
                    &task,
                    workflow,
                    cascade_to,
                    cascade_reason,
                    cascade_bridge,
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
        Ok(HookPhaseResult {
            follow_up,
            failure,
            blocking_failure,
            merge_failure,
            retry: None,
        })
    }

    /// Enqueue the post-commit hooks of a Task's current entry when that
    /// entry has no hooks row: it was committed by a binary that ran hooks
    /// inline and lost them (crash or shutdown before the durable-hooks
    /// upgrade). The row is keyed `<entry transition_log id>:recovered`, so
    /// repeated startup sweeps never duplicate it. Exit hooks of the prior
    /// state already ran with that commit and are skipped.
    ///
    /// Returns `None` when the current entry cannot be identified.
    pub(crate) async fn enqueue_recovered_entry_hooks(
        &self,
        task: &db::Task,
        status_epoch: i64,
        project: &db::Project,
        workflow: &WorkflowDefinition,
    ) -> crate::Result<Option<String>> {
        let Some(to_state) = WorkflowEngine::find_state(workflow, &task.status) else {
            return Ok(None);
        };
        // An engine or board entry logs its epoch. A pre-upgrade entry has
        // none; it is only the current one while the Task has not changed
        // status since the upgrade (epoch still 0).
        let mut entry: Option<(String, String)> = sqlx::query_as(
            "SELECT id, from_state FROM transition_log WHERE task_id = ? AND to_state = ? AND status_epoch = ? ORDER BY created_at, rowid LIMIT 1",
        )
        .bind(&task.id)
        .bind(&task.status)
        .bind(status_epoch)
        .fetch_optional(self.db.pool())
        .await?;
        if entry.is_none() && status_epoch == 0 {
            entry = sqlx::query_as(
                "SELECT id, from_state FROM transition_log WHERE task_id = ? AND to_state = ? AND status_epoch IS NULL ORDER BY created_at DESC, rowid DESC LIMIT 1",
            )
            .bind(&task.id)
            .bind(&task.status)
            .fetch_optional(self.db.pool())
            .await?;
        }
        let Some((entry_id, from)) = entry else {
            return Ok(None);
        };
        let from_state = WorkflowEngine::find_state(workflow, &from).unwrap_or(to_state);
        let from = from_state.name.clone();
        let bridge = TransitionLogRepo::list_by_task(&*self.db, &task.id)
            .await?
            .into_iter()
            .find(|log| log.id == entry_id)
            .map(|log| log.bridge)
            .unwrap_or_default();
        let actor = Actor::system(api_types::SystemComponent::TaskDispatcher);
        let reason = "recovering post-commit hooks lost before restart".to_owned();
        let input = self
            .cascade_step_input(
                task,
                workflow,
                task.status.clone(),
                reason.clone(),
                bridge.clone(),
                false,
                false,
                Some(WorkflowAuthority {
                    project_version: project.version,
                    workflow_definition: project.workflow_definition.clone(),
                    clear_review_passed_at_on_commit: false,
                }),
                None,
                format!("{entry_id}:recovered"),
                Some(status_epoch),
            )
            .await?;
        let cascade_payload: crate::worker_runtime::queue::CascadePayload =
            serde_json::from_str(&input.payload_json)
                .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
        let frozen = self
            .db
            .store_step_workflow(
                &serde_json::to_string(&HookDefinition {
                    workflow: workflow.clone(),
                    project_workflow_definition: Some(project.workflow_definition.clone()),
                })
                .map_err(|e| ServiceError::invalid_operation(e.to_string()))?,
            )
            .await?;
        let latest_execution = latest_execution_context(&self.db, &task.id).await?;
        let latest_executor = latest_executor_context(&self.db, &task.id).await?;
        let workspace_id = latest_execution
            .as_ref()
            .and_then(|execution| execution.workspace_id.clone())
            .or_else(|| {
                latest_executor
                    .as_ref()
                    .and_then(|execution| execution.workspace_id.clone())
            });
        let execution_id = latest_executor
            .as_ref()
            .or(latest_execution.as_ref())
            .map(|execution| execution.id.clone());
        let dispatch_index = to_state
            .hooks
            .on_enter
            .iter()
            .position(|h| {
                registry::is_dispatch_action(&h.action)
                    && hook_audience_matches(h.applies_to, &actor)
            })
            .map(|i| {
                (from_state.hooks.on_exit.len() + to_state.hooks.before_enter.len() + i) as i64
            });
        let payload = HookPayload {
            from: from.clone(),
            to: task.status.clone(),
            actor,
            reason,
            bridge,
            transition_log_id: entry_id,
            workflow_ref: crate::worker_runtime::queue::WorkflowReference::Snapshot(frozen),
            authority: Some(project.version),
            from_config: merged_state_config(
                from_state,
                Some(project),
                task.task_state_config.as_deref(),
            ),
            to_config: merged_state_config(
                to_state,
                Some(project),
                task.task_state_config.as_deref(),
            ),
            workspace_id,
            execution_id,
            agent_id: latest_execution
                .as_ref()
                .and_then(|execution| execution.agent_id.clone()),
            skip_before_enter: false,
            skip_on_exit: true,
            defer_dispatch_until: None,
            action_dispatch: false,
            pre_results: Vec::new(),
            admission_agent_id: dispatch_index.and(cascade_payload.admission_agent_id),
            dispatch_index,
            evidence: cascade_payload.evidence,
        };
        let step = db::EnqueueTaskStep {
            kind: "hooks".into(),
            // Exit hooks are skipped, so only the entered state sets the lane.
            lane: hooks_lane(workflow, "", &task.status).into(),
            payload_json: serde_json::to_string(&payload)
                .map_err(|e| ServiceError::invalid_operation(e.to_string()))?,
            available_at: now_rfc3339(),
            ..input
        };
        Ok(Some(self.db.enqueue_step(&step).await?))
    }
}
