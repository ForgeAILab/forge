use super::*;

impl TaskService {
    pub async fn recover_task(
        &self,
        task_id: impl Into<String>,
        action: api_types::RecoveryAction,
        reason: Option<String>,
        context: Option<String>,
    ) -> Result<Task> {
        let task_id = task_id.into();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if task.failed_json.is_some()
            && !matches!(
                action,
                api_types::RecoveryAction::ResetToInitial | api_types::RecoveryAction::CancelTask
            )
        {
            return Err(ServiceError::invalid_operation(
                "task has failed and cannot be resumed from current context; restart or cancel required",
            ));
        }
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        if workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal) {
            return Err(ServiceError::invalid_operation(format!(
                "cannot recover task {} in terminal status {}",
                task.id, task.status
            )));
        }
        let annotation = self.recovery_annotation(&task);
        if !self_validating_recovery_action(action) {
            let annotation = annotation?;
            self.validate_recovery_action(&annotation, &action)?;
            return match action {
                api_types::RecoveryAction::ResumeSession => {
                    self.recover_resume_session(task, &annotation, reason, context)
                        .await
                }
                api_types::RecoveryAction::Reexecute => {
                    self.recover_reexecute(task, &annotation, reason, context)
                        .await
                }
                api_types::RecoveryAction::ResetToInitial => {
                    self.recover_reset_to_initial(task, &annotation, reason)
                        .await
                }
                api_types::RecoveryAction::CancelTask => {
                    self.recover_cancel_task(task, reason).await
                }
                api_types::RecoveryAction::MarkReviewed => {
                    self.recover_mark_reviewed(task, reason).await
                }
                api_types::RecoveryAction::UpdateWorkspaceAndRetryHook => {
                    self.recover_update_workspace_and_retry_hook(task, &annotation, reason)
                        .await
                }
                api_types::RecoveryAction::SkipHookOnce => {
                    self.recover_skip_hook_once(task, reason).await
                }
                api_types::RecoveryAction::ResetRetryWindow
                | api_types::RecoveryAction::ProceedOnce
                | api_types::RecoveryAction::OpenInteractive
                | api_types::RecoveryAction::RetryHook
                | api_types::RecoveryAction::ResumeProcess => unreachable!(),
            };
        }
        let annotation = annotation.ok();
        match action {
            api_types::RecoveryAction::ResetRetryWindow => {
                self.recover_reset_retry_window(task, reason).await
            }
            api_types::RecoveryAction::ProceedOnce => {
                self.recover_proceed_once(task, reason, context).await
            }
            api_types::RecoveryAction::OpenInteractive => {
                self.recover_open_interactive(task, annotation.as_ref(), reason, context)
                    .await
            }
            api_types::RecoveryAction::MarkReviewed => {
                self.recover_mark_reviewed(task, reason).await
            }
            api_types::RecoveryAction::RetryHook => {
                if let Some(ref annotation) = annotation {
                    self.recover_retry_hook(task, annotation, reason).await
                } else {
                    self.recover_retry_review(task, reason).await
                }
            }
            api_types::RecoveryAction::ResumeProcess => {
                self.recover_resume_process(task, reason, context).await
            }
            api_types::RecoveryAction::ResumeSession
            | api_types::RecoveryAction::Reexecute
            | api_types::RecoveryAction::ResetToInitial
            | api_types::RecoveryAction::CancelTask
            | api_types::RecoveryAction::UpdateWorkspaceAndRetryHook
            | api_types::RecoveryAction::SkipHookOnce => unreachable!(),
        }
    }

    fn parse_blocking_annotation(&self, task: &Task) -> Option<api_types::TaskBlockingAnnotation> {
        let annotation = task.error_annotation.as_deref()?;
        match serde_json::from_str::<api_types::TaskAnnotation>(annotation) {
            Ok(api_types::TaskAnnotation::Blocking(annotation)) => Some(annotation),
            Ok(api_types::TaskAnnotation::Legacy(_)) => None,
            Err(_) => None,
        }
    }

    fn recovery_annotation(&self, task: &Task) -> Result<api_types::TaskBlockingAnnotation> {
        if task
            .blocked_json
            .as_deref()
            .is_some_and(is_retry_exhausted_blocked_metadata)
        {
            if let Some(raw_blocked) = task.blocked_json.as_deref() {
                return metadata_recovery_annotation(
                    raw_blocked,
                    &[
                        api_types::RecoveryAction::RetryHook,
                        api_types::RecoveryAction::ResumeProcess,
                        api_types::RecoveryAction::ResetRetryWindow,
                        api_types::RecoveryAction::OpenInteractive,
                        api_types::RecoveryAction::CancelTask,
                    ],
                );
            }
        }
        if task.status == crate::workflow::default_states::MERGING
            && task
                .blocked_json
                .as_deref()
                .is_some_and(is_recoverable_merge_gate_blocked_metadata)
        {
            if let Some(raw_blocked) = task.blocked_json.as_deref() {
                return metadata_recovery_annotation(
                    raw_blocked,
                    &[
                        api_types::RecoveryAction::RetryHook,
                        api_types::RecoveryAction::OpenInteractive,
                        api_types::RecoveryAction::CancelTask,
                    ],
                );
            }
        }
        if task.status == crate::workflow::default_states::MERGE_FAILED
            && task
                .blocked_json
                .as_deref()
                .is_some_and(is_recoverable_merge_fix_blocked_metadata)
        {
            if let Some(raw_blocked) = task.blocked_json.as_deref() {
                return metadata_recovery_annotation(
                    raw_blocked,
                    &[
                        api_types::RecoveryAction::RetryHook,
                        api_types::RecoveryAction::Reexecute,
                        api_types::RecoveryAction::OpenInteractive,
                        api_types::RecoveryAction::CancelTask,
                    ],
                );
            }
        }
        if let Some(annotation) = self.parse_blocking_annotation(task) {
            return Ok(annotation);
        }
        if let Some(raw_blocked) = task.blocked_json.as_deref() {
            return metadata_recovery_annotation(
                raw_blocked,
                &[
                    api_types::RecoveryAction::ResumeSession,
                    api_types::RecoveryAction::Reexecute,
                    api_types::RecoveryAction::ResetToInitial,
                    api_types::RecoveryAction::CancelTask,
                ],
            );
        }
        if let Some(raw_failed) = task.failed_json.as_deref() {
            return metadata_recovery_annotation(
                raw_failed,
                &[
                    api_types::RecoveryAction::ResetToInitial,
                    api_types::RecoveryAction::CancelTask,
                ],
            );
        }

        Err(ServiceError::invalid_operation(
            "task has no blocking recovery annotation",
        ))
    }

    fn validate_recovery_action(
        &self,
        annotation: &api_types::TaskBlockingAnnotation,
        action: &api_types::RecoveryAction,
    ) -> Result<()> {
        if annotation.recovery_actions.contains(action) {
            Ok(())
        } else {
            Err(ServiceError::invalid_operation(format!(
                "recovery action '{}' is not allowed for this task",
                serde_json::to_string(action).unwrap_or_else(|_| "unknown".to_owned())
            )))
        }
    }

    pub(super) async fn clear_blocking_metadata(&self, task_id: &str) -> Result<Task> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let previous_reason = interruption_reason(task.blocked_json.as_deref());
        let updated = TaskRepo::update(
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
        super::clear_execution_retry_metadata(&self.db, &updated).await?;
        if task.blocked_json.is_some() {
            self.publish(ForgeEvent {
                event_type: "task.unblocked".to_owned(),
                entity_id: updated.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskUnblocked {
                    project_id: updated.project_id.clone(),
                    previous_reason,
                },
            });
        }
        Ok(updated)
    }

    pub async fn unblock_task(&self, task_id: impl Into<String>) -> Result<Task> {
        let task_id = task_id.into();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if task.blocked_json.is_none() {
            return Err(ServiceError::invalid_operation("task is not blocked"));
        }
        let previous_reason = interruption_reason(task.blocked_json.as_deref());
        let updated = TaskRepo::update(
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
        super::clear_execution_retry_metadata(&self.db, &updated).await?;
        self.publish(ForgeEvent {
            event_type: "task.unblocked".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskUnblocked {
                project_id: updated.project_id.clone(),
                previous_reason: previous_reason.clone(),
            },
        });
        tracing::info!(
            task_id = %updated.id,
            status = %updated.status,
            previous_reason = ?previous_reason,
            "task unblocked"
        );
        Ok(updated)
    }

    pub async fn fail_task(
        &self,
        task_id: impl Into<String>,
        reason: impl Into<String>,
        kind: Option<api_types::FailureKind>,
        execution_id: Option<String>,
    ) -> Result<Task> {
        let task_id = task_id.into();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let reason = reason.into();
        let failed_meta = json!({
            "reason": reason,
            "created_at": now_rfc3339(),
            "kind": kind,
            "execution_id": execution_id,
        });
        let updated = TaskRepo::update(
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
                failed_json: Some(Some(failed_meta.to_string())),
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        self.publish(ForgeEvent {
            event_type: "task.failed".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskFailed {
                project_id: updated.project_id.clone(),
                reason: reason.clone(),
                kind,
                execution_id: execution_id.clone(),
            },
        });
        tracing::info!(
            task_id = %updated.id,
            status = %updated.status,
            reason = %reason,
            kind = ?kind,
            execution_id = ?execution_id,
            "task marked as failed"
        );
        Ok(updated)
    }

    pub async fn restart_failed_task(&self, task_id: impl Into<String>) -> Result<Task> {
        let task_id = task_id.into();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if task.failed_json.is_none() {
            return Err(ServiceError::invalid_operation("task has not failed"));
        }
        let previous_reason = interruption_reason(task.failed_json.as_deref());
        let updated = TaskRepo::update(
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
                blocked_json: None,
                failed_json: Some(None),
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        self.publish(ForgeEvent {
            event_type: "task.restarted".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRestarted {
                project_id: updated.project_id.clone(),
                previous_reason: previous_reason.clone(),
                new_execution_id: None,
            },
        });
        tracing::info!(
            task_id = %updated.id,
            status = %updated.status,
            previous_reason = ?previous_reason,
            "failed task restarted"
        );
        Ok(updated)
    }

    async fn recover_resume_session(
        &self,
        task: Task,
        annotation: &api_types::TaskBlockingAnnotation,
        reason: Option<String>,
        context: Option<String>,
    ) -> Result<Task> {
        let blocked_execution_id = annotation.blocked_execution_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("resume_session requires blocked_execution_id")
        })?;
        let blocked_execution = ExecutionRepo::get_by_id(&*self.db, blocked_execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", blocked_execution_id.to_owned()))?;
        let agent_id = blocked_execution
            .agent_id
            .as_ref()
            .cloned()
            .ok_or_else(|| ServiceError::invalid_operation("blocked execution has no assignee"))?;
        let agent_session_id = blocked_execution.agent_session_id.clone().ok_or_else(|| {
            ServiceError::invalid_operation("blocked execution has no resumable session")
        })?;
        let snapshot_json = blocked_execution
            .executor_config_snapshot_json
            .as_deref()
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "blocked execution missing executor config snapshot",
                )
            })?;
        let updated_snapshot =
            executor_snapshot_with_resume_thread(snapshot_json, &agent_session_id)?;
        let updated_snapshot = if let Some(ctx) = context.as_deref() {
            let mut snap: serde_json::Value =
                serde_json::from_str(&updated_snapshot).map_err(|e| {
                    ServiceError::invalid_operation(format!("failed to parse snapshot: {e}"))
                })?;
            if let Some(obj) = snap.as_object_mut() {
                obj.insert(
                    "resume_context".to_owned(),
                    serde_json::Value::String(ctx.to_owned()),
                );
            }
            serde_json::to_string(&snap).map_err(|e| {
                ServiceError::invalid_operation(format!("failed to serialize snapshot: {e}"))
            })?
        } else {
            updated_snapshot
        };
        self.ensure_task_runnable(&task).await?;
        let (workspace, workspace_created_by_attempt) =
            super::super::workspace::prepare_workspace_owned(
                &self.db,
                &self.workspace_root,
                &task,
                &task.id,
                self.repo_cache_locks.clone(),
            )
            .await?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::user(api_types::UserActionSource::Recovery(
                api_types::RecoveryAction::ResumeSession,
            )),
        );
        let updated_task = self
            .recover_task_transition_to_work_state(task, &workflow)
            .await?;
        let updated_task = self.clear_blocking_metadata(&updated_task.id).await?;
        self.ensure_task_runnable(&updated_task).await?;
        let now = now_rfc3339();
        let execution = self
            .create_running_execution(
                CreateExecution {
                    id: new_uuid_v4(),
                    task_id: updated_task.id.clone(),
                    agent_id: Some(agent_id),
                    role: blocked_execution.role.clone(),
                    status: ExecutionStatus::Running,
                    stop_reason: None,
                    stopped_by: None,
                    resume_policy: None,
                    stopped_at: None,
                    parent_execution_id: Some(blocked_execution.id.clone()),
                    agent_session_id: Some(agent_session_id),
                    agent_message_id: None,
                    last_activity_at: None,
                    summary: match (
                        context.as_deref(),
                        blocked_execution
                            .prompt
                            .as_ref()
                            .or(blocked_execution.summary.as_ref()),
                    ) {
                        (Some(ctx), Some(orig)) => Some(format!("[User context: {ctx}]\n\n{orig}")),
                        (Some(ctx), None) => Some(format!("[User context: {ctx}]")),
                        (None, s) => s.cloned(),
                    },
                    logs_path: None,
                    before_sha: workspace.before_sha.clone(),
                    after_sha: None,
                    error: None,
                    executor_config_snapshot_json: Some(updated_snapshot),
                    workspace_id: Some(workspace.id.clone()),
                    created_at: now.clone(),
                    updated_at: now,
                },
                workspace_created_by_attempt,
            )
            .await?;
        self.publish(ForgeEvent {
            event_type: "task.execution_resumed".to_owned(),
            entity_id: updated_task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: updated_task.project_id.clone(),
                reason: reason.unwrap_or_else(|| "resume_session".to_owned()),
            },
        });
        self.spawn_recovery_execution(
            updated_task.id.clone(),
            execution.id.clone(),
            "resume_session",
        )
        .await?;
        Ok(updated_task)
    }

    async fn recover_reexecute(
        &self,
        task: Task,
        annotation: &api_types::TaskBlockingAnnotation,
        reason: Option<String>,
        context: Option<String>,
    ) -> Result<Task> {
        let recovered =
            if let Some(blocked_execution_id) = annotation.blocked_execution_id.as_deref() {
                // Keep the large re-execution future out of the enclosing
                // coordination-tool recovery future.
                let result =
                    Box::pin(self.re_execute_execution_for_recovery(blocked_execution_id, context))
                        .await?;
                self.spawn_recovery_execution(
                    result.task.id.clone(),
                    result.execution.id.clone(),
                    "reexecute",
                )
                .await?;
                result.task
            } else {
                self.recover_reexecute_current_state(task, context).await?
            };
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason: reason.unwrap_or_else(|| "reexecute".to_owned()),
            },
        });
        Ok(recovered)
    }

    async fn recover_reexecute_current_state(
        &self,
        task: Task,
        context: Option<String>,
    ) -> Result<Task> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::user(api_types::UserActionSource::Recovery(
                api_types::RecoveryAction::Reexecute,
            )),
        );
        let state = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .ok_or_else(|| {
                ServiceError::invalid_operation(format!(
                    "cannot re-execute task in unknown state {}",
                    task.status
                ))
            })?;
        if workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal) {
            return Err(ServiceError::invalid_operation(format!(
                "cannot re-execute a task in terminal status {}",
                task.status
            )));
        }
        let role_name = crate::workflow::effective_role(state).ok_or_else(|| {
            ServiceError::invalid_operation(format!("state {} has no executable role", task.status))
        })?;
        let assignment =
            TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role_name)
                .await?
                .ok_or_else(|| {
                    ServiceError::invalid_operation(format!(
                        "task has no assignment for role {role_name}"
                    ))
                })?;
        if assignment.assignee_type != Some(AssigneeKind::Agent) {
            return Err(ServiceError::invalid_operation(format!(
                "role {role_name} is not assigned to an agent"
            )));
        }
        let agent_id = assignment.assignee_id.ok_or_else(|| {
            ServiceError::invalid_operation(format!("role {role_name} has no assigned agent"))
        })?;
        let page = ExecutionRepo::list_by_task_and_role(
            &*self.db,
            &task.id,
            role_name,
            PageRequest {
                cursor: None,
                limit: 20,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await?;
        if page
            .items
            .iter()
            .any(|execution| execution.status == ExecutionStatus::Running)
        {
            return Err(ServiceError::invalid_operation(format!(
                "execution already running for role {role_name}"
            )));
        }

        let state_config = state.config.clone();
        let state_dispatch = dispatch_intent_from_workflow_dispatch(state.dispatch.as_ref());
        let selection = effective_prompt_selection(role_name, None, state_dispatch.as_ref());
        let dispatch_ctx = load_agent_dispatch_context(
            Arc::clone(&self.db),
            &task.id,
            role_name,
            &task.status,
            state_config,
            Some(selection.execution_policy.as_str()),
            &workflow,
        )
        .await?;
        let (prompt, _selection) =
            build_effective_prompt(&dispatch_ctx, None, state_dispatch.as_ref());
        let summary = match context {
            Some(ctx) => format!("[User context: {ctx}]\n\n{}", prompt.user),
            None => prompt.user,
        };
        let agent = AgentRepo::get_by_id(&*self.db, &agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", agent_id.clone()))?;
        self.ensure_task_runnable(&task).await?;
        let (workspace, workspace_created_by_attempt) =
            super::super::workspace::prepare_workspace_owned(
                &self.db,
                &self.workspace_root,
                &task,
                &task.id,
                self.repo_cache_locks.clone(),
            )
            .await?;
        let executor_config_snapshot_json =
            build_executor_config_snapshot(&self.db, &task, &agent, None).await?;
        // Clear recovery state before issuing an exact-version WorkspaceLease.
        let recovered = self.clear_blocking_metadata(&task.id).await?;
        let now = now_rfc3339();
        let execution = self
            .create_running_execution(
                CreateExecution {
                    id: new_uuid_v4(),
                    task_id: recovered.id.clone(),
                    agent_id: Some(agent.id.clone()),
                    role: role_name.to_owned(),
                    status: ExecutionStatus::Running,
                    stop_reason: None,
                    stopped_by: None,
                    resume_policy: None,
                    stopped_at: None,
                    parent_execution_id: None,
                    agent_session_id: None,
                    agent_message_id: None,
                    last_activity_at: None,
                    summary: Some(summary),
                    logs_path: None,
                    before_sha: workspace.before_sha.clone(),
                    after_sha: None,
                    error: None,
                    executor_config_snapshot_json,
                    workspace_id: Some(workspace.id.clone()),
                    created_at: now.clone(),
                    updated_at: now,
                },
                workspace_created_by_attempt,
            )
            .await?;
        self.spawn_recovery_execution(
            recovered.id.clone(),
            execution.id.clone(),
            "reexecute_current_state",
        )
        .await?;
        tracing::info!(
            task_id = %task.id,
            execution_id = %execution.id,
            role = %role_name,
            "recovery re-execute dispatched current state role"
        );
        Ok(recovered)
    }

    async fn spawn_recovery_execution(
        &self,
        _task_id: String,
        execution_id: String,
        _recovery_action: &'static str,
    ) -> Result<()> {
        self.start_execution(execution_id).await?;
        Ok(())
    }

    async fn recover_reset_retry_window(&self, task: Task, reason: Option<String>) -> Result<Task> {
        let reason = optional_recovery_reason(reason, "reset_retry_window");
        let has_exhausted_annotation = self
            .parse_blocking_annotation(&task)
            .as_ref()
            .is_some_and(crate::task_diagnostics::is_retry_budget_exhausted);
        let (gate_state, budget, count) = self.current_gate_retry_budget(&task).await?;
        if count < i64::from(budget) && !has_exhausted_annotation {
            return Err(ServiceError::conflict(format!(
                "retry window for state {gate_state} is not exhausted: {count}/{budget}"
            )));
        }
        let transition_log = TransitionLogRepo::insert_recovery_marker(
            &*self.db,
            &task.id,
            &gate_state,
            "reset_retry_window",
            &api_types::Actor::user(api_types::UserActionSource::Recovery(
                api_types::RecoveryAction::ResetRetryWindow,
            ))
            .display(),
            &reason,
        )
        .await?;
        let updated = self.clear_retry_exhausted_blocking_metadata(&task).await?;
        self.publish_recovery_applied(
            &updated,
            "reset_retry_window",
            Some(&gate_state),
            Some(&transition_log.id),
        );
        if gate_state == crate::workflow::default_states::REVIEW {
            let latest_review = self.latest_review_for_task(&updated.id).await?;
            if latest_review.status == ReviewStatus::Failed {
                return self
                    .recover_resume_process(updated, Some(reason), None)
                    .await;
            }
        }
        if gate_state == crate::workflow::default_states::MERGING {
            return self
                .recover_resume_process(updated, Some(reason), None)
                .await;
        }
        Ok(updated)
    }

    async fn recover_proceed_once(
        &self,
        task: Task,
        reason: Option<String>,
        context: Option<String>,
    ) -> Result<Task> {
        let reason = required_recovery_reason(reason, "proceed_once")?;

        if task.entry_barrier_json.is_some() {
            let transition_log = TransitionLogRepo::insert_recovery_marker(
                &*self.db,
                &task.id,
                &task.status,
                "proceed_once",
                &api_types::Actor::user(api_types::UserActionSource::Recovery(
                    api_types::RecoveryAction::ProceedOnce,
                ))
                .display(),
                &reason,
            )
            .await?;
            let recovered = self.recover_skip_hook_once(task, Some(reason)).await?;
            self.publish_recovery_applied(
                &recovered,
                "proceed_once",
                Some(&recovered.status),
                Some(&transition_log.id),
            );
            return Ok(recovered);
        }

        let has_exhausted_annotation = self
            .parse_blocking_annotation(&task)
            .as_ref()
            .is_some_and(crate::task_diagnostics::is_retry_budget_exhausted);
        let (gate_state, budget, count) = self.current_gate_retry_budget(&task).await?;
        if gate_state != crate::workflow::default_states::REVIEW
            || (count < i64::from(budget) && !has_exhausted_annotation)
        {
            return Err(ServiceError::invalid_operation(format!(
                "proceed_once is not supported for the current exception in state {gate_state}"
            )));
        }

        let transition_reason = match &context {
            Some(guidance) => format!("{reason}\n\nGuidance: {guidance}"),
            None => reason.clone(),
        };
        let transition_log = TransitionLogRepo::insert_recovery_marker(
            &*self.db,
            &task.id,
            &gate_state,
            "proceed_once",
            &api_types::Actor::user(api_types::UserActionSource::Recovery(
                api_types::RecoveryAction::ProceedOnce,
            ))
            .display(),
            &transition_reason,
        )
        .await?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::user(api_types::UserActionSource::Recovery(
                api_types::RecoveryAction::ProceedOnce,
            )),
        );
        let target = workflow
            .states
            .iter()
            .find(|state| state.name == gate_state)
            .and_then(|state| state.gate_config.as_ref())
            .and_then(|gate_config| gate_config.reject_target.clone())
            .unwrap_or_else(|| crate::workflow::default_states::IN_PROGRESS.to_owned());
        let recovered = self
            .transition(
                task.id.clone(),
                target,
                TransitionOptions {
                    version: task.version,
                    reason: Some(transition_reason),
                    triggered_by: api_types::Actor::user(api_types::UserActionSource::Recovery(
                        api_types::RecoveryAction::ProceedOnce,
                    )),
                    rejection: true,
                    defer_dispatch_seconds: None,
                },
            )
            .await?
            .task;
        self.publish_recovery_applied(
            &recovered,
            "proceed_once",
            Some(&gate_state),
            Some(&transition_log.id),
        );
        Ok(recovered)
    }

    async fn recover_open_interactive(
        &self,
        task: Task,
        annotation: Option<&api_types::TaskBlockingAnnotation>,
        reason: Option<String>,
        context: Option<String>,
    ) -> Result<Task> {
        let message = context
            .or(reason.clone())
            .unwrap_or_else(|| "open_interactive".to_owned());
        if let Some(execution) = self
            .interactive_follow_up_execution(&task, annotation)
            .await?
        {
            let result = self
                .follow_up_execution(execution.id, message, execution.agent_id, None)
                .await?;
            self.publish(ForgeEvent {
                event_type: "task.recovery_action".to_owned(),
                entity_id: result.task.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskRecovered {
                    project_id: result.task.project_id.clone(),
                    reason: reason.unwrap_or_else(|| "open_interactive".to_owned()),
                },
            });
            return Ok(result.task);
        }

        let agent_id = self.interactive_launch_agent(&task, annotation).await?;
        let result = self
            .launch_execution(&task.id, agent_id, Some(message), None)
            .await?;
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: result.task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: result.task.project_id.clone(),
                reason: reason.unwrap_or_else(|| "open_interactive".to_owned()),
            },
        });
        Ok(result.task)
    }

    async fn recover_resume_process(
        &self,
        task: Task,
        reason: Option<String>,
        context: Option<String>,
    ) -> Result<Task> {
        let plan = self.resume_process_plan(&task).await?;
        if plan.count >= i64::from(plan.budget) {
            return Err(ServiceError::invalid_operation(format!(
                "resume_process is not supported because retry budget is exhausted for state {}",
                plan.gate_state
            )));
        }

        let has_interruption = task.error_annotation.is_some() || task.blocked_json.is_some();
        let has_failed_review = if task.status == crate::workflow::default_states::REVIEW {
            matches!(
                self.latest_review_for_task(&task.id).await,
                Ok(review) if review.status == ReviewStatus::Failed
            )
        } else {
            false
        };
        if !has_interruption && !has_failed_review {
            return Err(ServiceError::invalid_operation(
                "resume_process requires a recoverable gate exception",
            ));
        }

        let reason = optional_recovery_reason(reason, "resume_process");
        let transition_reason = match &context {
            Some(guidance) => format!("{reason}\n\nGuidance: {guidance}"),
            None => reason.clone(),
        };
        let transition_log = TransitionLogRepo::insert_recovery_marker(
            &*self.db,
            &task.id,
            &plan.gate_state,
            "resume_process",
            &api_types::Actor::user(api_types::UserActionSource::Recovery(
                api_types::RecoveryAction::ResumeProcess,
            ))
            .display(),
            &transition_reason,
        )
        .await?;
        let recovered = self
            .transition_recovery_rejection(
                &task,
                plan.target_state,
                transition_reason,
                &api_types::Actor::user(api_types::UserActionSource::Recovery(
                    api_types::RecoveryAction::ResumeProcess,
                )),
            )
            .await?;
        self.publish_recovery_applied(
            &recovered,
            "resume_process",
            Some(&plan.gate_state),
            Some(&transition_log.id),
        );
        Ok(recovered)
    }

    async fn transition_recovery_rejection(
        &self,
        task: &Task,
        target_state: String,
        reason: String,
        actor: &api_types::Actor,
    ) -> Result<Task> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(task, &project.workflow_definition, actor);
        let uses_system_only_trigger = workflow
            .trigger_between(&task.status, &target_state)
            .is_some_and(|trigger| trigger.system_only());

        if uses_system_only_trigger {
            let engine = WorkflowEngine {
                db: Arc::clone(&self.db),
                event_bus: Arc::clone(&self.event_bus),
                review_runner: self.review_runner.clone(),
                merge_service: self.merge_service.clone(),
                cleanup_scheduler: self.cleanup_scheduler.clone(),
                task_executor: self.task_executor.clone(),
                daemon_connections: self.daemon_connections.clone(),
                workspace_exec_locks: self.workspace_exec_locks.clone(),
                terminal_activity: self.terminal_activity.clone(),
                workspace_root: self.workspace_root.clone(),
                repo_cache_locks: self.repo_cache_locks.clone(),
            };
            return Ok(engine
                .manual_override_transition(
                    &task.id,
                    &target_state,
                    task.version,
                    &workflow,
                    actor.clone(),
                    &reason,
                    true,
                )
                .await?
                .task);
        }

        Ok(self
            .transition(
                task.id.clone(),
                target_state,
                TransitionOptions {
                    version: task.version,
                    reason: Some(reason),
                    triggered_by: actor.clone(),
                    rejection: true,
                    defer_dispatch_seconds: None,
                },
            )
            .await?
            .task)
    }

    async fn resume_process_plan(&self, task: &Task) -> Result<ResumeProcessPlan> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::user(api_types::UserActionSource::Recovery(
                api_types::RecoveryAction::ResumeProcess,
            )),
        );
        let state = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .ok_or_else(|| {
                ServiceError::invalid_operation(WorkflowEngine::undefined_state_message(
                    &task.status,
                    &workflow,
                ))
            })?;
        if state.kind != api_types::StateKind::Gate {
            return Err(ServiceError::invalid_operation(format!(
                "resume_process is only supported in gate states, not {}",
                task.status
            )));
        }
        let target_state = state
            .gate_config
            .as_ref()
            .and_then(|gate_config| gate_config.reject_target.clone())
            .unwrap_or_else(|| crate::workflow::default_states::IN_PROGRESS.to_owned());
        let (gate_state, budget, count) = self.current_gate_retry_budget(task).await?;
        Ok(ResumeProcessPlan {
            gate_state,
            target_state,
            budget,
            count,
        })
    }

    async fn current_gate_retry_budget(&self, task: &Task) -> Result<(String, i32, i64)> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::user(api_types::UserActionSource::Api),
        );
        let state = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .ok_or_else(|| {
                ServiceError::invalid_operation(WorkflowEngine::undefined_state_message(
                    &task.status,
                    &workflow,
                ))
            })?;
        if state.kind != api_types::StateKind::Gate {
            return Err(ServiceError::conflict(format!(
                "state {} is not a retry-budget gate",
                task.status
            )));
        }

        let budget = if task.status == crate::workflow::default_states::REVIEW {
            crate::task_service::config::runtime_retry_budget(
                task,
                crate::task_service::config::RetryBudgetKind::Review,
                Some(&state.config),
                state.gate_config.as_ref(),
            )?
        } else {
            state
                .gate_config
                .as_ref()
                .and_then(|gate_config| gate_config.max_rejections)
                .unwrap_or(i32::MAX)
        };
        let entries = TransitionLogRepo::list_by_task(&*self.db, &task.id).await?;
        let count = gate_rejections_since_recovery_boundary(&entries, &task.status);
        Ok((task.status.clone(), budget, count))
    }

    async fn clear_retry_exhausted_blocking_metadata(&self, task: &Task) -> Result<Task> {
        let clear_error = task
            .error_annotation
            .as_deref()
            .is_some_and(is_retry_exhausted_annotation);
        let clear_blocked = task
            .blocked_json
            .as_deref()
            .is_some_and(is_retry_exhausted_blocked_metadata);
        if !clear_error && !clear_blocked {
            return Ok(task.clone());
        }
        TaskRepo::update(
            &*self.db,
            UpdateTask {
                id: task.id.clone(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: if clear_error { Some(None) } else { None },
                blocked_json: if clear_blocked { Some(None) } else { None },
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn interactive_follow_up_execution(
        &self,
        task: &Task,
        annotation: Option<&api_types::TaskBlockingAnnotation>,
    ) -> Result<Option<Execution>> {
        let Some(role_name) = self.current_effective_role_name(task).await? else {
            return Ok(None);
        };
        if let Some(execution_id) = annotation.and_then(|annotation| {
            annotation
                .blocked_execution_id
                .as_deref()
                .map(str::to_owned)
        }) {
            if let Some(execution) = ExecutionRepo::get_by_id(&*self.db, &execution_id).await? {
                if execution.agent_session_id.is_some()
                    && matches!(
                        execution.status,
                        ExecutionStatus::Completed
                            | ExecutionStatus::Failed
                            | ExecutionStatus::Cancelled
                    )
                    && execution_matches_role(&execution, &role_name)
                {
                    return Ok(Some(execution));
                }
            }
        }

        latest_resumable_interactive_role_execution(&self.db, &task.id, &role_name).await
    }

    async fn interactive_launch_agent(
        &self,
        task: &Task,
        annotation: Option<&api_types::TaskBlockingAnnotation>,
    ) -> Result<String> {
        if let Some(role_name) = self.current_effective_role_name(task).await? {
            if let Some(assignment) =
                TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, &role_name)
                    .await?
            {
                if assignment.assignee_type == Some(AssigneeKind::Agent) {
                    if let Some(agent_id) = assignment.assignee_id {
                        return Ok(agent_id);
                    }
                }
            }
        }

        if let Some(execution_id) = annotation.and_then(|annotation| {
            annotation
                .blocked_execution_id
                .as_deref()
                .map(str::to_owned)
        }) {
            if let Some(execution) = ExecutionRepo::get_by_id(&*self.db, &execution_id).await? {
                if let Some(agent_id) = execution.agent_id {
                    return Ok(agent_id);
                }
            }
        }

        let page = ExecutionRepo::list_by_task(
            &*self.db,
            &task.id,
            PageRequest {
                cursor: None,
                limit: 20,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await?;
        page.items
            .into_iter()
            .find_map(|execution| execution.agent_id)
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "open_interactive requires a blocked execution, assigned agent, or previous execution",
                )
            })
    }

    async fn current_effective_role_name(&self, task: &Task) -> Result<Option<String>> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::user(api_types::UserActionSource::Api),
        );
        Ok(workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(crate::workflow::effective_role)
            .map(str::to_owned))
    }

    fn publish_recovery_applied(
        &self,
        task: &Task,
        action: &str,
        state: Option<&str>,
        transition_log_id: Option<&str>,
    ) {
        self.publish(ForgeEvent {
            event_type: "task.recovery_applied".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::RecoveryApplied {
                project_id: task.project_id.clone(),
                task_id: task.id.clone(),
                action: action.to_owned(),
                state: state.map(str::to_owned),
                transition_log_id: transition_log_id.map(str::to_owned),
            },
        });
    }

    async fn recover_reset_to_initial(
        &self,
        task: Task,
        annotation: &api_types::TaskBlockingAnnotation,
        reason: Option<String>,
    ) -> Result<Task> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::user(api_types::UserActionSource::Recovery(
                api_types::RecoveryAction::ResetToInitial,
            )),
        );
        let initial_state = workflow_initial_state(&workflow)?;
        let assignee_id = if should_clear_assignments_for_reset(annotation) {
            Some(None)
        } else {
            None
        };
        let recovered = TaskRepo::update_status(
            &*self.db,
            UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: initial_state,
                assignee_id,
                error_annotation: Some(None),
                blocked_json: Some(None),
                failed_json: Some(None),
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        self.publish_domain_event_by_dedupe(&format!(
            "task-status-update:{}:{}",
            recovered.id, recovered.version
        ))
        .await;
        super::clear_execution_retry_metadata(&self.db, &recovered).await?;
        if task.blocked_json.is_some() {
            self.publish(ForgeEvent {
                event_type: "task.unblocked".to_owned(),
                entity_id: recovered.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskUnblocked {
                    project_id: recovered.project_id.clone(),
                    previous_reason: interruption_reason(task.blocked_json.as_deref()),
                },
            });
        }
        if task.failed_json.is_some() {
            self.publish(ForgeEvent {
                event_type: "task.restarted".to_owned(),
                entity_id: recovered.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskRestarted {
                    project_id: recovered.project_id.clone(),
                    previous_reason: interruption_reason(task.failed_json.as_deref()),
                    new_execution_id: None,
                },
            });
        }
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason: reason.unwrap_or_else(|| "reset_to_initial".to_owned()),
            },
        });
        Ok(recovered)
    }

    async fn recover_cancel_task(&self, task: Task, reason: Option<String>) -> Result<Task> {
        self.clear_blocking_metadata(&task.id).await?;
        let task = self.cancel_task(task.id).await?;
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: task.project_id.clone(),
                reason: reason.unwrap_or_else(|| "cancel_task".to_owned()),
            },
        });
        Ok(task)
    }

    async fn recover_mark_reviewed(&self, task: Task, reason: Option<String>) -> Result<Task> {
        if task.status != crate::workflow::default_states::REVIEW {
            return Err(ServiceError::invalid_operation(format!(
                "mark_reviewed is only supported from review state, got {}",
                task.status
            )));
        }
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::user(api_types::UserActionSource::Recovery(
                api_types::RecoveryAction::MarkReviewed,
            )),
        );
        let pass_target = workflow
            .auto_transition_target(&task.status)
            .unwrap_or(crate::workflow::default_states::MERGING)
            .to_owned();
        let reason = optional_recovery_reason(reason, "mark_reviewed");
        let latest_review = self.latest_review_for_task(&task.id).await?;
        let finished_at = now_rfc3339();
        let mut details = serde_json::from_str::<Value>(&latest_review.step_results_json)
            .unwrap_or_else(|_| json!({ "ci_steps": [] }));
        details["manual_override"] = json!({
            "action": "mark_reviewed",
            "reason": reason.clone(),
            "at": finished_at,
        });
        let review = ReviewRepo::update_status(
            &*self.db,
            &latest_review.id,
            ReviewStatus::Passed,
            details.to_string(),
            Some(finished_at.clone()),
            &finished_at,
        )
        .await?;
        self.publish_domain_event_by_dedupe(&format!(
            "review-status:{}:{}:{}",
            review.id, review.status, finished_at
        ))
        .await;
        if let Err(error) = self
            .memory_service
            .record_review_result_if_final(&task.project_id, &review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }
        let task = TaskRepo::set_review_passed_at(
            &*self.db,
            &task.id,
            Some(finished_at.clone()),
            &finished_at,
        )
        .await?;
        self.create_system_comment(
            &task.id,
            format!("Review passed manually (attempt {})", review.attempt_number),
        )
        .await?;
        self.publish(ForgeEvent {
            event_type: "review.approved".to_owned(),
            entity_id: review.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReviewApproved {
                task_id: task.id.clone(),
                review_id: review.id.clone(),
            },
        });
        tracing::info!(
            task_id = %task.id,
            reason = %reason,
            "recovery action mark_reviewed logged"
        );
        let transitioned = self
            .transition(task.id.clone(), pass_target, task.version)
            .await?;
        Ok(transitioned.task)
    }

    async fn recover_retry_hook(
        &self,
        task: Task,
        annotation: &api_types::TaskBlockingAnnotation,
        reason: Option<String>,
    ) -> Result<Task> {
        if task.status == crate::workflow::default_states::MERGING
            && crate::task_diagnostics::is_retry_budget_exhausted(annotation)
        {
            return self.recover_reset_retry_window(task, reason).await;
        }
        if task.status == crate::workflow::default_states::MERGING
            && is_recoverable_merge_gate_annotation(annotation)
        {
            if is_human_merge_gate_annotation(annotation) {
                return self.recover_retry_current_state_hooks(task, reason).await;
            }
            return self.recover_resume_process(task, reason, None).await;
        }
        if task.status == crate::workflow::default_states::MERGE_FAILED
            && is_recoverable_merge_fix_annotation(annotation)
        {
            let recovered = self.recover_reexecute_current_state(task, None).await?;
            self.publish(ForgeEvent {
                event_type: "task.recovery_action".to_owned(),
                entity_id: recovered.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskRecovered {
                    project_id: recovered.project_id.clone(),
                    reason: reason.unwrap_or_else(|| "retry_merge_fix".to_owned()),
                },
            });
            return Ok(recovered);
        }
        if task.entry_barrier_json.is_some() {
            let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            let workflow = WorkflowEngine::resolve_workflow_for_task(
                &task,
                &project.workflow_definition,
                &api_types::Actor::user(api_types::UserActionSource::RetryHook),
            );
            let engine = WorkflowEngine {
                db: Arc::clone(&self.db),
                event_bus: Arc::clone(&self.event_bus),
                review_runner: self.review_runner.clone(),
                merge_service: self.merge_service.clone(),
                cleanup_scheduler: self.cleanup_scheduler.clone(),
                task_executor: self.task_executor.clone(),
                daemon_connections: self.daemon_connections.clone(),
                workspace_exec_locks: self.workspace_exec_locks.clone(),
                terminal_activity: self.terminal_activity.clone(),
                workspace_root: self.workspace_root.clone(),
                repo_cache_locks: self.repo_cache_locks.clone(),
            };
            let recovered = engine
                .retry_entry_barrier(
                    &task.id,
                    task.version,
                    &workflow,
                    &api_types::Actor::user(api_types::UserActionSource::RetryHook),
                    reason.as_deref().unwrap_or("retry_hook"),
                )
                .await?
                .task;
            self.publish(ForgeEvent {
                event_type: "task.recovery_action".to_owned(),
                entity_id: recovered.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskRecovered {
                    project_id: recovered.project_id.clone(),
                    reason: reason.unwrap_or_else(|| "retry_hook".to_owned()),
                },
            });
            return Ok(recovered);
        }
        let recovered = self.clear_blocking_metadata(&task.id).await?;
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason: reason.unwrap_or_else(|| "retry_hook".to_owned()),
            },
        });
        Ok(recovered)
    }

    async fn recover_retry_current_state_hooks(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        let reason = optional_recovery_reason(reason, "retry_hook");
        let cleared = self.clear_blocking_metadata(&task.id).await?;
        let project = ProjectRepo::get_by_id(&*self.db, &cleared.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", cleared.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &cleared,
            &project.workflow_definition,
            &api_types::Actor::user(api_types::UserActionSource::RetryHook),
        );
        let engine = WorkflowEngine {
            db: Arc::clone(&self.db),
            event_bus: Arc::clone(&self.event_bus),
            review_runner: self.review_runner.clone(),
            merge_service: self.merge_service.clone(),
            cleanup_scheduler: self.cleanup_scheduler.clone(),
            task_executor: self.task_executor.clone(),
            daemon_connections: self.daemon_connections.clone(),
            workspace_exec_locks: self.workspace_exec_locks.clone(),
            terminal_activity: self.terminal_activity.clone(),
            workspace_root: self.workspace_root.clone(),
            repo_cache_locks: self.repo_cache_locks.clone(),
        };
        let recovered = engine
            .manual_override_transition(
                &cleared.id,
                &cleared.status,
                cleared.version,
                &workflow,
                api_types::Actor::user(api_types::UserActionSource::RetryHook),
                &reason,
                false,
            )
            .await?
            .task;
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason,
            },
        });
        Ok(recovered)
    }

    async fn recover_retry_review(&self, task: Task, reason: Option<String>) -> Result<Task> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::user(api_types::UserActionSource::Recovery(
                api_types::RecoveryAction::RetryHook,
            )),
        );
        let state = workflow
            .states
            .iter()
            .find(|s| s.name == task.status)
            .ok_or_else(|| {
                ServiceError::invalid_operation(WorkflowEngine::undefined_state_message(
                    &task.status,
                    &workflow,
                ))
            })?;
        if state.kind != api_types::StateKind::Gate {
            return Err(ServiceError::invalid_operation(format!(
                "retry_hook without annotation is only supported in gate states, not {}",
                task.status
            )));
        }
        let reject_target = state
            .gate_config
            .as_ref()
            .and_then(|gc| gc.reject_target.clone())
            .unwrap_or_else(|| crate::workflow::default_states::IN_PROGRESS.to_owned());
        let reason_text = reason.clone().unwrap_or_else(|| "retry review".to_owned());
        let bounced = self
            .transition_recovery_rejection(
                &task,
                reject_target,
                format!("retry review: {reason_text}"),
                &api_types::Actor::user(api_types::UserActionSource::Recovery(
                    api_types::RecoveryAction::RetryHook,
                )),
            )
            .await?;
        let fresh = TaskRepo::get_by_id(&*self.db, &bounced.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", bounced.id.clone()))?;
        let recovered = self
            .transition(
                fresh.id.clone(),
                task.status.clone(),
                TransitionOptions {
                    version: fresh.version,
                    reason: Some(format!("retry review: {reason_text}")),
                    triggered_by: api_types::Actor::user(api_types::UserActionSource::Recovery(
                        api_types::RecoveryAction::RetryHook,
                    )),
                    rejection: false,
                    defer_dispatch_seconds: None,
                },
            )
            .await?
            .task;
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason: reason.unwrap_or_else(|| "retry_review".to_owned()),
            },
        });
        Ok(recovered)
    }

    async fn recover_update_workspace_and_retry_hook(
        &self,
        task: Task,
        annotation: &api_types::TaskBlockingAnnotation,
        reason: Option<String>,
    ) -> Result<Task> {
        let workspace = prepare_workspace(
            &self.db,
            &self.workspace_root,
            &task,
            &task.id,
            self.repo_cache_locks.clone(),
        )
        .await?;
        let repo_id = task
            .repo_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("task has no associated repo"))?;
        let repo = RepoRepo::get_by_id(&*self.db, repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", repo_id.to_owned()))?;
        let target_branch = default_target_branch(&repo.default_branch);
        let worktree_path = std::path::Path::new(&workspace.worktree_path);

        if !git::is_worktree_clean(worktree_path).await? {
            let files = git::status_porcelain(worktree_path).await?.join(", ");
            return Err(ServiceError::invalid_operation(format!(
                "cannot update workspace before retrying hook because the worktree is dirty: {files}"
            )));
        }

        match git::rebase(worktree_path, &target_branch).await {
            Ok(()) => {
                tracing::info!(
                    task_id = %task.id,
                    workspace_id = %workspace.id,
                    target_branch = %target_branch,
                    "workspace updated before retrying hook"
                );
            }
            Err(git::GitError::MergeConflict { stderr, .. }) => {
                let _ = git::abort_rebase(worktree_path).await;
                return Err(ServiceError::invalid_operation(format!(
                    "cannot update workspace before retrying hook because rebase onto {target_branch} conflicted: {stderr}"
                )));
            }
            Err(error) => return Err(error.into()),
        }

        self.recover_retry_hook(
            task,
            annotation,
            Some(reason.unwrap_or_else(|| "update_workspace_and_retry_hook".to_owned())),
        )
        .await
    }

    async fn recover_skip_hook_once(&self, task: Task, reason: Option<String>) -> Result<Task> {
        tracing::info!(
            task_id = %task.id,
            reason = %reason.clone().unwrap_or_else(|| "skip_hook_once".to_owned()),
            "recovery action skip_hook_once logged"
        );
        let recovered = if task.entry_barrier_json.is_some() {
            let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            let workflow = WorkflowEngine::resolve_workflow_for_task(
                &task,
                &project.workflow_definition,
                &api_types::Actor::user(api_types::UserActionSource::SkipHookOnce),
            );
            let barrier_state = task
                .entry_barrier_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
                .and_then(|barrier| {
                    barrier
                        .get("state")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| task.status.clone());
            let skip_config = serde_json::json!({
                barrier_state.clone(): {
                    "skip_before_work_hook_once": true
                }
            });
            let updated = TaskRepo::update(
                &*self.db,
                UpdateTask {
                    id: task.id.clone(),
                    expected_version: task.version,
                    title: None,
                    description: None,
                    priority: None,
                    merge_config: None,
                    plan: None,
                    error_annotation: None,
                    blocked_json: None,
                    failed_json: None,
                    task_state_config: Some(Some(skip_config.to_string())),
                    parent_task_id: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await?;
            let engine = WorkflowEngine {
                db: Arc::clone(&self.db),
                event_bus: Arc::clone(&self.event_bus),
                review_runner: self.review_runner.clone(),
                merge_service: self.merge_service.clone(),
                cleanup_scheduler: self.cleanup_scheduler.clone(),
                task_executor: self.task_executor.clone(),
                daemon_connections: self.daemon_connections.clone(),
                workspace_exec_locks: self.workspace_exec_locks.clone(),
                terminal_activity: self.terminal_activity.clone(),
                workspace_root: self.workspace_root.clone(),
                repo_cache_locks: self.repo_cache_locks.clone(),
            };
            let recovered = engine
                .retry_entry_barrier(
                    &updated.id,
                    updated.version,
                    &workflow,
                    &api_types::Actor::user(api_types::UserActionSource::SkipHookOnce),
                    reason.as_deref().unwrap_or("skip_hook_once"),
                )
                .await?
                .task;
            clear_skip_before_work_hook_once_override(&self.db, &recovered, &barrier_state).await?
        } else {
            self.clear_blocking_metadata(&task.id).await?
        };
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason: reason.unwrap_or_else(|| "skip_hook_once".to_owned()),
            },
        });
        Ok(recovered)
    }

    async fn recover_task_transition_to_work_state(
        &self,
        task: Task,
        workflow: &api_types::WorkflowDefinition,
    ) -> Result<Task> {
        let kind = workflow.state_kind(&task.status);
        if matches!(
            kind,
            Some(api_types::StateKind::Initial | api_types::StateKind::Custom)
        ) {
            let target = workflow
                .outgoing_trigger_targets(&task.status)
                .filter(|(trigger, _)| !trigger.system_only())
                .find_map(|(_, target)| {
                    matches!(
                        workflow.state_kind(&target),
                        Some(api_types::StateKind::Active | api_types::StateKind::Gate)
                    )
                    .then_some(target)
                });
            if let Some(target) = target {
                let fresh = TaskRepo::get_by_id(&*self.db, &task.id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
                self.transition(task.id.clone(), target, fresh.version)
                    .await
                    .map(|result| result.task)
            } else {
                Ok(task)
            }
        } else {
            Ok(task)
        }
    }
}

async fn clear_skip_before_work_hook_once_override(
    db: &Arc<SqliteDb>,
    task: &Task,
    state_name: &str,
) -> Result<Task> {
    let Some(raw) = task.task_state_config.as_deref() else {
        return Ok(task.clone());
    };
    let Ok(mut parsed) = serde_json::from_str::<serde_json::Value>(raw) else {
        return Ok(task.clone());
    };
    let Some(root) = parsed.as_object_mut() else {
        return Ok(task.clone());
    };
    let Some(state_value) = root.get_mut(state_name) else {
        return Ok(task.clone());
    };
    let Some(state_object) = state_value.as_object_mut() else {
        return Ok(task.clone());
    };
    if state_object.remove("skip_before_work_hook_once").is_none() {
        return Ok(task.clone());
    }
    if state_object.is_empty() {
        root.remove(state_name);
    }
    let next_config = if root.is_empty() {
        None
    } else {
        Some(parsed.to_string())
    };
    let updated = TaskRepo::update(
        &**db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: Some(next_config),
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await?;
    Ok(updated)
}

fn should_clear_assignments_for_reset(annotation: &api_types::TaskBlockingAnnotation) -> bool {
    // recovery_required covers crash-recovery and agent-timeout interruptions;
    // workspace failures arrive either as a live annotation
    // (workspace_reset_required / workspace_error) or, once fail_task has
    // cleared the annotation, synthesized from failed_json (workspace_failed).
    annotation.annotation_type == api_types::FailureKind::RecoveryRequired
        || annotation.annotation_type.is_workspace_failure()
}

fn workflow_initial_state(workflow: &api_types::WorkflowDefinition) -> Result<String> {
    workflow
        .states
        .iter()
        .find(|state| state.kind == api_types::StateKind::Initial)
        .map(|state| state.name.clone())
        .ok_or_else(|| ServiceError::invalid_operation("workflow has no initial state"))
}

fn metadata_recovery_annotation(
    raw_metadata: &str,
    recovery_actions: &[api_types::RecoveryAction],
) -> Result<api_types::TaskBlockingAnnotation> {
    let metadata: Value = serde_json::from_str(raw_metadata).map_err(|error| {
        ServiceError::invalid_operation(format!("failed to parse interruption metadata: {error}"))
    })?;
    let reason = metadata
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("interrupted")
        .to_owned();
    let execution_id = metadata
        .get("execution_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let annotation_type = metadata
        .get("kind")
        .cloned()
        .and_then(|kind| serde_json::from_value::<api_types::FailureKind>(kind).ok())
        .unwrap_or(api_types::FailureKind::Unknown);
    // Unknown kinds are info-only: legacy rows the migration could not map
    // must not offer actions the service cannot classify.
    let recovery_actions = if annotation_type == api_types::FailureKind::Unknown {
        Vec::new()
    } else {
        recovery_actions.to_vec()
    };
    Ok(api_types::TaskBlockingAnnotation {
        annotation_type,
        blocking_reason: reason.clone(),
        blocked_by: Some(api_types::Actor::system(api_types::SystemComponent::Workflow).display()),
        blocked_at: metadata
            .get("created_at")
            .and_then(Value::as_str)
            .map(str::to_owned),
        blocked_execution_id: execution_id.clone(),
        artifact: execution_id.map(|id| api_types::BlockingArtifact {
            kind: "execution".to_owned(),
            id: Some(id),
            log_path: None,
        }),
        message: Some(reason),
        hook: None,
        recovery_actions,
    })
}

fn default_target_branch(repo_default_branch: &str) -> String {
    let trimmed = repo_default_branch.trim();
    if trimmed.is_empty() {
        "main".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn interruption_reason(raw_metadata: Option<&str>) -> Option<String> {
    raw_metadata
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|metadata| {
            metadata
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

async fn latest_resumable_interactive_role_execution(
    db: &SqliteDb,
    task_id: &str,
    role: &str,
) -> Result<Option<Execution>> {
    if let Some(execution) = latest_resumable_interactive_exact_role(db, task_id, role).await? {
        return Ok(Some(execution));
    }
    if role == crate::workflow::default_roles::CODER {
        return latest_resumable_interactive_exact_role(db, task_id, "executor").await;
    }
    Ok(None)
}

async fn latest_resumable_interactive_exact_role(
    db: &SqliteDb,
    task_id: &str,
    role: &str,
) -> Result<Option<Execution>> {
    let page = ExecutionRepo::list_by_task_and_role(
        db,
        task_id,
        role,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await?;
    Ok(page.items.into_iter().find(|execution| {
        execution.agent_session_id.is_some()
            && matches!(
                execution.status,
                ExecutionStatus::Completed | ExecutionStatus::Failed | ExecutionStatus::Cancelled
            )
    }))
}

fn execution_matches_role(execution: &Execution, role: &str) -> bool {
    execution.role == role
        || (role == crate::workflow::default_roles::CODER && execution.role == "executor")
}

fn self_validating_recovery_action(action: api_types::RecoveryAction) -> bool {
    matches!(
        action,
        api_types::RecoveryAction::ResetRetryWindow
            | api_types::RecoveryAction::ProceedOnce
            | api_types::RecoveryAction::OpenInteractive
            | api_types::RecoveryAction::MarkReviewed
            | api_types::RecoveryAction::RetryHook
            | api_types::RecoveryAction::ResumeProcess
    )
}

fn optional_recovery_reason(reason: Option<String>, action_kind: &str) -> String {
    reason
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| action_kind.to_owned())
}

fn required_recovery_reason(reason: Option<String>, action_kind: &str) -> Result<String> {
    reason
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ServiceError::invalid_operation(format!("{action_kind} requires a recovery reason"))
        })
}

struct ResumeProcessPlan {
    gate_state: String,
    target_state: String,
    budget: i32,
    count: i64,
}

fn gate_rejections_since_recovery_boundary(entries: &[db::TransitionLog], gate_state: &str) -> i64 {
    let boundary = entries.iter().rposition(|entry| {
        entry.from_state == gate_state
            && !entry.rejection
            && (entry.to_state != gate_state
                || entry.trigger_name.as_deref() == Some("reset_retry_window"))
    });
    let entries = boundary
        .and_then(|index| entries.get(index + 1..))
        .unwrap_or(entries);
    entries
        .iter()
        .filter(|entry| entry.from_state == gate_state && entry.rejection)
        .count() as i64
}

fn is_retry_exhausted_annotation(raw_annotation: &str) -> bool {
    match serde_json::from_str::<api_types::TaskAnnotation>(raw_annotation) {
        Ok(api_types::TaskAnnotation::Blocking(ref annotation)) => {
            crate::task_diagnostics::is_retry_budget_exhausted(annotation)
        }
        _ => false,
    }
}

fn blocked_metadata_kind(raw_metadata: &str) -> Option<api_types::FailureKind> {
    let metadata: Value = serde_json::from_str(raw_metadata).ok()?;
    serde_json::from_value(metadata.get("kind")?.clone()).ok()
}

fn is_retry_exhausted_blocked_metadata(raw_metadata: &str) -> bool {
    blocked_metadata_kind(raw_metadata)
        .is_some_and(api_types::FailureKind::is_retry_exhausted_metadata)
}

fn is_recoverable_merge_gate_annotation(annotation: &api_types::TaskBlockingAnnotation) -> bool {
    annotation.annotation_type.is_merge_recoverable()
}

fn is_human_merge_gate_annotation(annotation: &api_types::TaskBlockingAnnotation) -> bool {
    annotation.annotation_type == api_types::FailureKind::TargetRepoDirty
}

fn is_recoverable_merge_fix_annotation(annotation: &api_types::TaskBlockingAnnotation) -> bool {
    annotation.annotation_type.is_merge_recoverable()
}

fn is_recoverable_merge_gate_blocked_metadata(raw_metadata: &str) -> bool {
    blocked_metadata_kind(raw_metadata) == Some(api_types::FailureKind::TargetRepoDirty)
}

fn is_recoverable_merge_fix_blocked_metadata(raw_metadata: &str) -> bool {
    blocked_metadata_kind(raw_metadata).is_some_and(api_types::FailureKind::is_merge_recoverable)
}
