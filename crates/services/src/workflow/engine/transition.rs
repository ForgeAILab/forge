//! Workflow transition validation, persistence, and hook dispatch.

use super::*;

impl WorkflowExecution<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn transition_inner<'a>(
        &'a self,
        task_id: String,
        target_state: String,
        version: i64,
        workflow: &'a WorkflowDefinition,
        actor: Actor,
        reason: String,
        rejection: bool,
        skip_before_exit: bool,
        defer_dispatch_until: Option<String>,
        board_move: Option<BoardMoveRequest>,
        step: Option<db::TaskStep>,
        authority: Option<WorkflowAuthority>,
        entry_retry: bool,
        bridge: api_types::TransitionBridge,
    ) -> Pin<Box<dyn Future<Output = crate::Result<TransitionResult>> + Send + 'a>> {
        #[cfg(test)]
        self.engine
            .invocations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let span = tracing::info_span!(
            "workflow.transition_inner",
            task_id = %task_id,
            target_state = %target_state,
            version = version,
            actor = %actor,
            reason = %reason,
            rejection = rejection,
            skip_before_exit = skip_before_exit,
            defer_dispatch = defer_dispatch_until.is_some(),
            step_id = ?step.as_ref().map(|step| &step.id),
        );

        Box::pin(async move {
            if !db::task_writer::owns_task(&task_id) {
                let preempt = WorkflowEngine::is_cancellation_target(workflow, &target_state);
                let command=crate::task_service::commands::TaskCommand {
                    operation:"engine_transition".to_owned(),preempt,
                    arguments:serde_json::json!({"task_id":task_id,"target_state":target_state,"version":version,"workflow":workflow,"actor":actor,"reason":reason,"rejection":rejection,"skip_before_exit":skip_before_exit,"defer_dispatch_until":defer_dispatch_until,"board_move":board_move,"authority":authority,"entry_retry":entry_retry,"bridge_kind":bridge.bridge_kind,"bridge_payload":bridge.bridge_payload}),
                };
                if db::task_writer::current_task_step().is_some_and(|step|step.task_id!=task_id) && preempt {
                    self.task_service.enqueue_task_command(&task_id,&command.operation,command.arguments,preempt).await?;
                    let task=TaskRepo::get_by_id(&*self.db,&task_id,false).await?.ok_or(db::DbError::NotFound)?;
                    return Ok(TransitionResult {task,review:None,queued_step_id:None,pending_steps:self.db.pending_steps(&task_id).await?,board_move:None});
                }
                return Arc::new(crate::worker_runtime::queue::TaskStepWorker::new(self.task_service.clone()))
                    .request_command(&task_id,command).await;
            }
            let mut task = TaskRepo::get_by_id(&*self.db, &task_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;

            if step.is_none() && !db::task_writer::owns_task(&task_id) && task.version != version {
                tracing::warn!(
                    task_id = %task.id,
                    expected_version = version,
                    actual_version = task.version,
                    current_state = %task.status,
                    target_state = %target_state,
                    actor = %actor,
                    "workflow transition rejected by version conflict"
                );
                return Err(if board_move.is_some() {
                    db::DbError::TaskVersionConflict {
                        expected: version,
                        actual: task.version,
                    }
                    .into()
                } else {
                    db::DbError::VersionConflict.into()
                });
            }

            let version = task.version;
            let current_status = task.status.to_string();
            tracing::debug!(
                task_id = %task.id,
                from_state = %current_status,
                to_state = %target_state,
                actor = %actor,
                reason = %reason,
                step_id = ?step.as_ref().map(|step| &step.id),
                "workflow transition requested"
            );
            let from_state = WorkflowEngine::find_state(workflow, &current_status).ok_or_else(|| {
                ServiceError::InvalidOperation {
                    message: WorkflowEngine::undefined_state_message(&current_status, workflow),
                }
            })?;
            let to_state = WorkflowEngine::find_state(workflow, &target_state).ok_or_else(|| {
                ServiceError::InvalidOperation {
                    message: WorkflowEngine::undefined_state_message(&target_state, workflow),
                }
            })?;
            let transition = workflow.trigger_between(&current_status, &target_state);
            let trigger_name = transition.map(|trigger| trigger.as_str().to_owned());
            let is_user_actor = actor.is_user();
            let is_agent_cancellation = actor.is_agent()
                && WorkflowEngine::is_cancellation_target(workflow, &target_state)
                && from_state.kind != StateKind::Terminal;
            // A coordination root's re-plan (review rejected, or a subtask
            // parked) enters the planning state over an edge no workflow
            // declares. Only the workflow itself may take it; the source
            // state's exit conditions describe going forward and do not
            // apply to it.
            let replan_entry = transition.is_none()
                && actor.is_system()
                && crate::task_hierarchy::replan_edge(workflow, &bridge, &target_state);
            let none_allowance = transition.is_none()
                && (((current_status == target_state || to_state.kind == StateKind::Initial)
                    && skip_before_exit)
                    || replan_entry
                    || (WorkflowEngine::is_cancellation_target(workflow, &target_state)
                        && from_state.kind != StateKind::Terminal));
            let strict_missing_edge = transition.is_none() && !none_allowance;
            let strict_system_only = matches!(
                transition,
                Some(trigger)
                    if !skip_before_exit
                        && WorkflowEngine::transition_requires_system_actor(trigger, from_state, to_state)
                        && !actor.is_system()
                        && !is_agent_cancellation
            );
            let mut actor = actor;
            let effective_skip_before_exit = if is_user_actor
                && from_state.kind != StateKind::Terminal
                && (strict_missing_edge || strict_system_only)
            {
                actor = actor.into_override();
                false
            } else {
                match transition {
                    Some(_trigger) => {
                        if strict_system_only {
                            tracing::warn!(
                                task_id = %task.id,
                                from_state = %current_status,
                                to_state = %target_state,
                                workflow_trigger = ?transition,
                                actor = %actor,
                                "workflow transition rejected because it is system-only"
                            );
                            return Err(ServiceError::InvalidOperation {
                                message: format!(
                                    "transition {} -> {} is system-only",
                                    current_status, target_state
                                ),
                            });
                        }
                        skip_before_exit
                    }
                    None if skip_before_exit && to_state.kind == StateKind::Initial => true,
                    None if skip_before_exit && current_status == target_state => true,
                    None if replan_entry => true,
                    None if WorkflowEngine::is_cancellation_target(workflow, &target_state)
                        && from_state.kind != StateKind::Terminal =>
                    {
                        true
                    }
                    None => {
                        tracing::warn!(
                            task_id = %task.id,
                            from_state = %current_status,
                            to_state = %target_state,
                            from_kind = ?from_state.kind,
                            to_kind = ?to_state.kind,
                            actor = %actor,
                            reason = %reason,
                            "workflow transition rejected because no transition is defined"
                        );
                        return Err(ServiceError::Db(db::DbError::InvalidTransition));
                    }
                }
            };
            tracing::info!(
                task_id = %task.id,
                from_state = %current_status,
                to_state = %target_state,
                from_kind = ?from_state.kind,
                to_kind = ?to_state.kind,
                workflow_trigger = ?transition,
                actor = %actor,
                reason = %reason,
                rejection = rejection,
                skip_before_exit = effective_skip_before_exit,
                step_id = ?step.as_ref().map(|step| &step.id),
                "workflow transition accepted"
            );

            let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            if let Some(authority) = authority.as_ref() {
                if project.version != authority.project_version
                    || project.workflow_definition != authority.workflow_definition
                {
                    return Err(db::DbError::VersionConflict.into());
                }
            }
            let from_state_config =
                merged_state_config(from_state, Some(&project), task.task_state_config.as_deref());
            let to_state_config =
                merged_state_config(to_state, Some(&project), task.task_state_config.as_deref());
            let workflow_ctx = Arc::new(workflow.clone());
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
                .map(|execution| execution.id.clone())
                .or_else(|| {
                    latest_execution
                        .as_ref()
                        .map(|execution| execution.id.clone())
                });

            let exit_ctx = HookContext {
                task_id: task.id.clone(),
                project_id: task.project_id.clone(),
                from_state: current_status.clone(),
                to_state: target_state.clone(),
                db: Arc::clone(&self.db),
                event_bus: Arc::clone(&self.event_bus),
                gate_config: from_state.gate_config.clone(),
                workflow: Arc::clone(&workflow_ctx),
                project_version: authority
                    .as_ref()
                    .map(|authority| authority.project_version),
                project_workflow_definition: authority
                    .as_ref()
                    .map(|authority| authority.workflow_definition.clone()),
                triggered_by: actor.clone(),
                merge_service: self.task_service.merge_service.clone(),
                cleanup_scheduler: self.task_service.cleanup_scheduler.clone(),
                task_service: self.task_service.clone(),
                daemon_connections: self.task_service.daemon_connections.clone(),
                workspace_exec_locks: self.task_service.workspace_exec_locks.clone(),
                terminal_activity: self.task_service.terminal_activity.clone(),
                workspace_root: self.task_service.workspace_root.clone(),
                repo_cache_locks: self.task_service.repo_cache_locks.clone(),
                workspace_backend_router: Arc::clone(&self.task_service.workspace_backend_router),
                workspace_id: workspace_id.clone(),
                agent_id: latest_execution
                    .as_ref()
                    .and_then(|execution| execution.agent_id.clone()),
                execution_id: execution_id.clone(),
                state_config: from_state_config,
            };
            let enter_ctx = HookContext {
                task_id: task.id.clone(),
                project_id: task.project_id.clone(),
                from_state: current_status.clone(),
                to_state: target_state.clone(),
                db: Arc::clone(&self.db),
                event_bus: Arc::clone(&self.event_bus),
                gate_config: to_state.gate_config.clone(),
                workflow: Arc::clone(&workflow_ctx),
                project_version: authority
                    .as_ref()
                    .map(|authority| authority.project_version),
                project_workflow_definition: authority
                    .as_ref()
                    .map(|authority| authority.workflow_definition.clone()),
                triggered_by: actor.clone(),
                merge_service: self.task_service.merge_service.clone(),
                cleanup_scheduler: self.task_service.cleanup_scheduler.clone(),
                task_service: self.task_service.clone(),
                daemon_connections: self.task_service.daemon_connections.clone(),
                workspace_exec_locks: self.task_service.workspace_exec_locks.clone(),
                terminal_activity: self.task_service.terminal_activity.clone(),
                workspace_root: self.task_service.workspace_root.clone(),
                repo_cache_locks: self.task_service.repo_cache_locks.clone(),
                workspace_backend_router: Arc::clone(&self.task_service.workspace_backend_router),
                workspace_id,
                agent_id: latest_execution
                    .as_ref()
                    .and_then(|execution| execution.agent_id.clone()),
                execution_id,
                state_config: to_state_config,
            };

            let mut hook_results = Vec::new();
            // `merge_failed` is only an edge-compatible bridge for mechanical
            // contention refreshes. It must run its on-enter dispatcher so the
            // task can cascade to review, but it must not run repair-state
            // entry hooks or create a retryable entry barrier along the way.
            let review_refresh_bridge = current_status == crate::workflow::default_states::MERGING
                && target_state == crate::workflow::default_states::MERGE_FAILED
                && bridge.is_review_refresh()
                && matches!(
                    &actor,
                    Actor::System {
                        component: api_types::SystemComponent::Workflow
                    }
                );
            // A review-refresh bridge is bookkeeping for a fresh review, not a
            // rejected merge attempt. Normalize the persisted transition even
            // when a caller supplied `rejection = true`; otherwise the bridge
            // itself spends merge-fix budget before the real conflict is seen.
            let conflict_handoff_bridge = current_status == crate::workflow::default_states::MERGING
                && target_state == crate::workflow::default_states::MERGE_FAILED
                && bridge.bridge_kind == Some(api_types::TransitionBridgeKind::ConflictHandoff)
                && matches!(
                    &actor,
                    Actor::System {
                        component: api_types::SystemComponent::Workflow
                    }
                );
            let rejection = rejection && !review_refresh_bridge && !conflict_handoff_bridge;
            if !effective_skip_before_exit {
                for hook in &from_state.hooks.before_exit {
                    if !hook_audience_matches(hook.applies_to, &actor) {
                        log_hook_skipped_by_audience(
                            &task.id,
                            &current_status,
                            &target_state,
                            "before_exit",
                            hook,
                            &actor,
                        );
                        continue;
                    }
                    let action = registry::resolve_action(&hook.action)?;
                    log_hook_start(
                        &task.id,
                        &current_status,
                        &target_state,
                        "before_exit",
                        hook,
                        &actor,
                    );
            let started = Instant::now();
            let result = crate::workflow::evaluate_hook(action.as_ref(), &exit_ctx).await?;
            self.refresh_task_after_hook(&mut task, &current_status, None)
                .await?;
            let duration_ms = elapsed_ms(started);
                    log_hook_result(
                        &task.id,
                        &current_status,
                        &target_state,
                        "before_exit",
                        hook,
                        &result,
                        duration_ms,
                    );
                    hook_results.push(hook_result_entry(
                        &hook.action,
                        "before_exit",
                        &result,
                        duration_ms,
                    ));

                    if let HookResult::Failed {
                        reason: guard_reason,
                    } = result
                    {
                        if matches!(hook.on_failure, FailurePolicy::Block) {
                            tracing::warn!(
                                task_id = %task.id,
                                from_state = %current_status,
                                to_state = %target_state,
                                guard_name = %hook.action,
                                reason = %guard_reason,
                                "workflow guard rejected transition"
                            );
                            self.event_bus.publish(ForgeEvent {
                                event_type: "transition.guard_rejected".to_string(),
                                entity_id: task.id.clone(),
                                timestamp: event_timestamp(),
                                context: EventContext::TransitionGuardRejected {
                                    task_id: task.id.clone(),
                                    from_state: current_status.clone(),
                                    to_state: target_state.clone(),
                                    guard_name: hook.action.clone(),
                                    reason: guard_reason.clone(),
                                },
                            });

                            return Err(ServiceError::GuardRejection {
                                guard: hook.action.clone(),
                                reason: guard_reason,
                            });
                        }
                    }
                }
            }

            let updated_at = now_rfc3339();
            let entry_barrier_json = entry_retry.then(|| task.entry_barrier_json.clone()).flatten();
            let cleanup_guard = if from_state.kind == StateKind::Terminal
                && to_state.kind != StateKind::Terminal
            {
                if let Some(scheduler) = self.task_service.cleanup_scheduler.as_ref() {
                    Some(scheduler.lock_task(&task).await)
                } else {
                    None
                }
            } else {
                None
            };
            let reopens_visible_work = from_state.kind == StateKind::Terminal
                && to_state.kind != StateKind::Terminal
                && !task.is_automation;
            let transition_log_id = step.as_ref().map(|s| s.id.clone()).unwrap_or_else(new_uuid_v4);
            let workflow_snapshot = crate::workflow::transition_event::transition_workflow_snapshot(
                &task,
                workflow,
                &current_status,
                &target_state,
            )?;
            let mut producing = task.clone();
            producing.status = target_state.clone();
            producing.version = version + 1;
            let input = self.cascade_step_input(&producing, workflow, target_state.clone(), reason.clone(), bridge.clone(), false, false, authority.clone(), step.as_ref(), transition_log_id.clone(), None).await?;
            let frozen = serde_json::to_string(&durable::HookDefinition { workflow: workflow.clone(), project_workflow_definition: authority.as_ref().map(|a| a.workflow_definition.clone()) }).map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
            let cascade_payload: crate::worker_runtime::queue::CascadePayload = serde_json::from_str(&input.payload_json).map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
            let mut dispatch_index = to_state.hooks.on_enter.iter().position(|h| registry::is_dispatch_action(&h.action) && hook_audience_matches(h.applies_to, &actor))
                .map(|i| (from_state.hooks.on_exit.len() + to_state.hooks.before_enter.len() + i) as i64);
            let mut admission_agent_id=dispatch_index.and(cascade_payload.admission_agent_id);
            let role_unassigned = if let Some(role)=crate::workflow::effective_role(to_state) { crate::task_hierarchy::effective_role_assignment(&self.db,&producing,role).await?.is_none_or(|r| r.assignment.assignee_id.is_none()) } else { false };
            // A review re-run must execute its dispatch hook as part of the
            // entry. With no checks, that hook opens the replacement Review;
            // deferring it leaves recovery reconciling the cancelled attempt.
            let action_dispatch = crate::TaskService::task_action_command_active()
                && !entry_retry
                && bridge.review_rerun_requester().is_none();
            let should_defer_dispatch=(defer_dispatch_until.is_some() || action_dispatch) && (to_state.kind!=StateKind::Active || action_dispatch) && dispatch_index.is_some();
            let deferred_marker=should_defer_dispatch.then(||serde_json::json!({"target_state":target_state,"not_before":defer_dispatch_until.clone().unwrap_or_else(now_rfc3339),"reason":if crate::TaskService::task_action_command_active(){"task action dispatch"}else{"board drag dispatch cooldown"}}).to_string());
            // Optional, unassigned planning is an imminent fast role entry:
            // reserve its coder slot while the hook step discovers the skip.
            if admission_agent_id.is_none() && role_unassigned && dispatch_index.is_some() && from_state.hooks.on_exit.is_empty()
                && durable::hooks_lane(workflow,&current_status,&target_state)=="fast"
                && to_state.gate_config.as_ref().is_some_and(|g|g.optional_when_unassigned() && !g.requires_user_approval())
                && to_state.hooks.after_enter.iter().any(|h|h.action=="auto_cascade_on_unassigned_role" && hook_audience_matches(h.applies_to,&actor))
            {
                // The state the skip really goes to; a gate skipped to a terminal
                // state (a step check) starts no role and reserves nothing.
                if let Some(target)=crate::workflow::optional_gate_skip_target(workflow,&target_state).filter(|s|s.kind==StateKind::Active) {
                    if let Some(role)=crate::workflow::effective_role(target) {
                        admission_agent_id=crate::task_hierarchy::effective_role_assignment(&self.db,&producing,role).await?.map(|r|r.assignment).filter(|a|a.assignee_type==Some(db::AssigneeKind::Agent)).and_then(|a|a.assignee_id);
                        if admission_agent_id.is_some() { dispatch_index=None; }
                    }
                }
            }
            // A retry changes the transition to review -> review, but the
            // no-change guard must still see the original repair and actor.
            let entry_origin = if entry_retry {
                self.db.current_entry_hook_payload(&task_id).await?
                    .map(|raw| serde_json::from_str::<durable::HookPayload>(&raw)
                        .map_err(|e| ServiceError::invalid_operation(e.to_string())))
                    .transpose()?
                    .map(|previous| previous.entry_origin.unwrap_or(durable::EntryOrigin {
                        from: previous.from, actor: previous.actor,
                    }))
                    .map(|mut origin| {
                        // The owner taking over a blocked entry also owns its
                        // later automatic reconnect retries. Keep the source
                        // state, while retaining that explicit owner intent.
                        if actor.is_owner() { origin.actor = actor.clone(); }
                        origin
                    })
            } else {
                None
            };
            let hook_payload = durable::HookPayload {
                entry_origin,
                from: current_status.clone(), to: target_state.clone(), actor: actor.clone(), reason: reason.clone(), bridge: bridge.clone(), transition_log_id: transition_log_id.clone(),
                workflow_ref: db::WorkflowReference::Definition(frozen), authority: authority.as_ref().map(|a| a.project_version),
                from_config: exit_ctx.state_config.clone(), to_config: enter_ctx.state_config.clone(), workspace_id: enter_ctx.workspace_id.clone(), execution_id: enter_ctx.execution_id.clone(), agent_id: enter_ctx.agent_id.clone(),
                skip_before_enter: review_refresh_bridge, skip_on_exit: entry_retry, defer_dispatch_until: defer_dispatch_until.clone(), action_dispatch, pre_results: hook_results.clone(), dispatch_index, admission_agent_id, evidence: cascade_payload.evidence,
            };
            let has_hooks = from_state.hooks.on_exit.iter()
                .chain(&to_state.hooks.before_enter).chain(&to_state.hooks.on_enter).chain(effective_after_enter_hooks(to_state).iter())
                .next().is_some() || (to_state.kind == StateKind::Terminal && self.task_service.cleanup_scheduler.is_some());
            let hook_json = serde_json::to_string(&hook_payload).map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
            let initial_hook_step = has_hooks.then(|| db::EnqueueTaskStep {
                kind: "hooks".into(), lane: durable::hooks_lane(workflow, &current_status, &target_state).into(),
                payload_json: hook_json,
                chain_position: step.as_ref().filter(|p| p.chain_id == input.chain_id).map(|p| p.chain_position).unwrap_or(input.chain_position),
                ..input
            });
            let (mut task, transition_log, board_move_outcome) =
                if let Some(move_request) = board_move {
                    let persistence = TaskBoardRepo::compare_and_move_task(
                        &*self.db,
                        CompareAndMoveTask {
                            operation_id: move_request.operation_id,
                            project_id: move_request.project_id,
                            task_id: task_id.clone(),
                            task_version: version,
                            board_revision: move_request.board_revision,
                            target_status: target_state.clone(),
                            target_column_statuses: move_request.target_column_statuses,
                            before_id: move_request.before_id,
                            after_id: move_request.after_id,
                            entry_barrier_json: entry_barrier_json.clone(),
                            post_commit_step: initial_hook_step.clone(),
                            integration_cancel: current_status != target_state
                                && workflow.cancellation_state.as_deref()
                                    == Some(target_state.as_str()),
                            transition_log_id: transition_log_id.clone(),
                            workflow_snapshot: workflow_snapshot.clone(),
                            trigger_name: trigger_name.clone(),
                            triggered_by: actor.clone(),
                            bridge: bridge.clone(),
                            trigger_reason: reason.clone(),
                            rejection,
                            expected_project_version: authority
                                .as_ref()
                                .map(|authority| authority.project_version),
                            expected_workflow_definition: authority
                                .as_ref()
                                .map(|authority| authority.workflow_definition.clone()),
                            updated_at: updated_at.clone(),
                        },
                    )
                    .await?;
                    match persistence {
                        MoveTaskPersistence::Replayed(result) => {
                            let review = latest_review(&self.db, &result.task.id).await?;
                            return Ok(TransitionResult {
                                task: result.task.clone(),
                                review,
                queued_step_id: None,
                pending_steps: self.db.pending_steps(&task_id).await?,
                                board_move: Some(BoardMoveOutcome::Replayed(*result)),
                            });
                        }
                        MoveTaskPersistence::Committed {
                            result,
                            transition_log,
                        } => (
                            result.task.clone(),
                            *transition_log,
                            Some(BoardMoveOutcome::Committed(*result)),
                        ),
                    }
                } else {
                    let mut transaction = db::begin_immediate(self.db.pool()).await?;
        self.db.fence_current_step_in_tx(&mut transaction).await?;
                    let version: i64 = self.db.workflow_transition_task_version_in_tx(&mut transaction, &task_id).await?;
                    if let Some(authority) = authority.as_ref() {
                        let project_authority = self.db.workflow_transition_project_authority_in_tx(&mut transaction, &task.project_id)
                        .await?
                        .ok_or(db::DbError::NotFound)?;
                        let current_project_version: i64 =
                            project_authority.version()?;
                        let current_workflow_definition: String =
                            project_authority.workflow_definition()?;
                        if current_project_version != authority.project_version
                            || current_workflow_definition != authority.workflow_definition
                        {
                            return Err(db::DbError::VersionConflict.into());
                        }
                    }
                    let clear_review_passed_at = authority
                        .as_ref()
                        .is_some_and(|authority| authority.clear_review_passed_at_on_commit);
                    // Superseding the old attempt and durably entering its
                    // replacement are one write boundary. Entry retries keep
                    // the attempt that this entry already opened.
                    if !entry_retry && current_status == target_state
                        && bridge.review_rerun_requester().is_some()
                    {
                        self.db.cancel_review_for_rerun_in_tx(&mut transaction, &task_id, &updated_at).await?;
                    }
                    // A status change bumps status_epoch through its trigger.
                    // A workflow self-transition (planning -> planning on a
                    // gate reject) is also a new entry: bump it here. A step
                    // fences on status plus epoch, never on the version.
                    let update = self.db.write_workflow_transition_state_in_tx(&mut transaction, db::WriteWorkflowTransitionStateInput { target_state: &target_state, updated_at: &updated_at, entry_barrier_json: entry_barrier_json.as_deref(), clear_review_passed_at, task_id: &task_id, has_step: step.is_some(), version, expected_status: step.as_ref().map(|s| s.expected_status.as_str()), expected_epoch: step.as_ref().map(|s| s.expected_epoch) })
                    .await?;

                    if update.rows_affected() != 1 {
                        return Err(db::DbError::VersionConflict.into());
                    }
                    // A Cancel of a Task the integration queue holds ends its
                    // attempt in this transaction when the attempt holds no
                    // slot and no owner effect is outstanding, and otherwise
                    // asks the queue to release it. Once a fast-forward may
                    // have started, the Cancel waits behind the protected
                    // result step instead (nothing is written).
                    if current_status != target_state
                        && workflow.cancellation_state.as_deref() == Some(target_state.as_str())
                        && db::cancel_task_integration_in_tx(&mut transaction, &task_id, &updated_at).await?
                            == db::TaskIntegrationCancel::Protected
                    {
                        let pending_steps: i64 = self.db.workflow_transition_pending_steps_in_tx(&mut transaction, &task_id).await?;
                        return Err(db::DbError::TaskBusy { pending_steps, retry_after_ms: 250 }.into());
                    }
                    if let Some(marker)=&deferred_marker {
                        self.db.set_workflow_deferred_dispatch_in_tx(&mut transaction, marker, &task_id).await?;
                    } else {
                        self.db.clear_workflow_deferred_dispatch_in_tx(&mut transaction, &task_id).await?;
                    }
                    // Carry paused integration through the status hop. Only a
                    // durable successful merge consumes this marker.
                    self.db.carry_paused_integration_state_in_tx(&mut transaction, &target_state, &task_id, &current_status).await?;
                    if reopens_visible_work {
                        ProjectRepo::increment_project_work_epoch(
                            &*self.db,
                            &mut transaction,
                            &task.project_id,
                            1,
                        )
                        .await?;
                    }
                    let event = CreateDomainEvent::task_transition(
                        transition_log_id.clone(),
                        task_id.clone(),
                        task.project_id.clone(),
                        &current_status,
                        &target_state,
                        trigger_name.as_deref(),
                        actor.display(),
                        &reason,
                        rejection,
                        updated_at.clone(),
                        workflow_snapshot,
                    );
                    self.db.insert_workflow_transition_log_in_tx(&mut transaction, db::InsertWorkflowTransitionLogInput { id: &transition_log_id, task_id: &task.id, from_state: &current_status, to_state: &target_state, trigger_name: trigger_name.as_deref(), triggered_by: actor.display(), trigger_reason: &reason, hook_results_json: serde_json::to_string(&hook_results).map_err(|e| ServiceError::invalid_operation(e.to_string()))?, rejection: if rejection { 1_i64 } else { 0_i64 }, created_at: &updated_at, bridge_kind: bridge.bridge_kind.map(api_types::TransitionBridgeKind::as_str), bridge_payload: bridge.bridge_payload.as_ref().map(ToString::to_string) })
                    .await?;
                    if entry_barrier_json.is_none() {
                        db::budget::fresh_entry(&mut transaction, &task.id, &transition_log_id).await?;
                    }
                    // A cascade out of a failed review entry shares that entry's charge.
                    let entry_step = step
                        .as_ref()
                        .filter(|step| step.kind == "cascade")
                        .and_then(|step| step.causation_step_id.as_deref());
                    db::budget::transition(
                        &mut transaction,
                        &task.id,
                        &current_status,
                        actor.is_owner(),
                        &bridge,
                        rejection,
                        &transition_log_id,
                        entry_step,
                    )
                    .await?;
                    // State the entry once its receipt exists: the status CAS,
                    // its SQL-computed metadata writes and the transition row
                    // above are one entry. The step settlement and the hooks
                    // enqueue below restate their own step witness.
                    self.db
                        .state_condition_in_tx(&mut transaction, &task_id, db::ConditionChange::Entry)
                        .await?;
                    if let Some(step) = &step {
                        self.db.finish_step_in_tx(&mut transaction, step, "done", None).await?;
                    }
                    if let Some(input) = &initial_hook_step {
                        self.db.enqueue_step_in_tx(&mut transaction, input).await?;
                    }
                    if task.blocked_json.is_some() {
                        let interruption_snapshot = self.db.get_task_in_tx(&mut transaction,&task_id).await?.ok_or(db::DbError::NotFound)?;
                        let interruption_event = CreateDomainEvent::task_interruption_changed(&interruption_snapshot);
                        DomainEventRepo::append_event_in_tx(&*self.db,&mut transaction,&interruption_event).await?;
                    }
                    DomainEventRepo::append_event_in_tx(&*self.db,&mut transaction,&event).await?;
                    let task = self.db.get_task_in_tx(&mut transaction, &task_id).await?.ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
                    transaction.commit().await?;
                    let transition_log = TransitionLog {
                        id: transition_log_id.clone(),
                        task_id: task.id.clone(),
                        from_state: current_status.clone(),
                        to_state: target_state.clone(),
                        trigger_name: trigger_name.clone(),
                        triggered_by: actor.display(),
                        bridge: bridge.clone(),
                        trigger_reason: reason.clone(),
                        hook_results_json: None,
                        rejection,
                        created_at: updated_at.clone(),
                    };
                    (task, transition_log, None)
                };
            drop(cleanup_guard);

            tracing::info!(
                task_id = %task.id,
                from_state = %current_status,
                to_state = %target_state,
                actor = %actor,
                reason = %reason,
                transition_log_id = %transition_log.id,
                "workflow transition applied"
            );

            if let Some(BoardMoveOutcome::Committed(result)) = &board_move_outcome {
                self.event_bus.publish(ForgeEvent {
                    event_type: TASK_MOVED_EVENT.to_owned(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskMoved(TaskMovedEventPayload {
                        project_id: task.project_id.clone(),
                        operation_id: result.operation_id.clone(),
                        old_status: result.old_status.clone(),
                        new_status: result.task.status.clone(),
                        old_board_position: result.old_board_position,
                        new_board_position: result.task.board_position,
                        task_version: result.task.version,
                        board_revision: result.board_revision,
                        before_id: result.before_id.clone(),
                        after_id: result.after_id.clone(),
                    }),
                });
            } else {
                self.event_bus.publish(ForgeEvent {
                    event_type: "task.status_changed".to_string(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskStatusChanged {
                        project_id: task.project_id.clone(),
                        old_status: current_status.clone(),
                        new_status: task.status.to_string(),
                    },
                });
            }

            let queued_step_id = initial_hook_step.as_ref().map(|i| i.id.clone());
            crate::deferred_dispatch::finish_machine_wait(&self.db, &mut task, version).await?;
            if let Some(persisted) = TaskRepo::get_by_id(&*self.db, &task_id, false).await? {
                if persisted.status == task.status && persisted.version == task.version {
                    task = persisted;
                }
            }
            let pending_steps = self.db.pending_steps(&task_id).await?;
            if let Some(id) = &queued_step_id { self.db.ready_step(id).await?; }
            let review = latest_review(&self.db, &task.id).await?;

            Ok(TransitionResult {
                task,
                review,
                queued_step_id,
                pending_steps,
                board_move: board_move_outcome,
            })
        }
        .instrument(span))
    }
}
