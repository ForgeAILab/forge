use super::*;

const AUTOMATIC_REVIEW_RECOVERY_TRIGGER: &str = "automatic_review_recovery";
const AUTOMATIC_REVIEW_RECOVERY_PROMPT_PREFIX: &str = "[Forge automatic review recovery]";

impl TaskService {
    pub async fn maybe_cascade_executor_completion(&self, execution_id: &str) -> Result<()> {
        let execution = match ExecutionRepo::get_by_id(&*self.db, execution_id).await? {
            Some(execution) => execution,
            None => return Ok(()),
        };
        if execution.role == crate::workflow::default_roles::REVIEWER {
            if execution.status == ExecutionStatus::Running {
                return Ok(());
            }
            return self.maybe_cascade_reviewer_completion(&execution).await;
        }
        if execution.status != ExecutionStatus::Completed {
            return Ok(());
        }

        if execution.role == "interactive" {
            return Ok(());
        }

        let task = match TaskRepo::get_by_id(&*self.db, &execution.task_id, false).await? {
            Some(task) => task,
            None => return Ok(()),
        };
        let project = match ProjectRepo::get_by_id(&*self.db, &task.project_id).await? {
            Some(project) => project,
            None => return Ok(()),
        };
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        let Some(current_state) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
        else {
            return Ok(());
        };
        let Some(effective_role) = crate::workflow::effective_role(current_state) else {
            return Ok(());
        };
        let role_matches = execution.role == effective_role
            || (effective_role == crate::workflow::default_roles::CODER
                && execution.role == "executor");
        if !role_matches {
            return Ok(());
        }
        if workflow.state_kind(&task.status) != Some(api_types::StateKind::Active) {
            return Ok(());
        }
        let Some(target) = workflow
            .auto_transition_target(&task.status)
            .map(str::to_owned)
        else {
            return Ok(());
        };
        if let Some(summary) = execution.summary.as_deref().map(str::trim) {
            if !summary.is_empty() {
                let content = format!("Agent completed execution: {summary}");
                if let Some(agent_id) = execution.agent_id.as_deref() {
                    self.create_agent_comment(&task.id, agent_id, content)
                        .await?;
                } else {
                    self.create_system_comment(&task.id, content).await?;
                }
            }
        }

        let from = task.status.clone();
        match self
            .transition(task.id.clone(), target.clone(), task.version)
            .await
        {
            Ok(_) => {
                if let Err(error) = self.clear_workflow_guard_retry_metadata(&task.id).await {
                    tracing::warn!(
                        task_id = %task.id,
                        %error,
                        "failed to clear workflow guard retry metadata"
                    );
                }
                self.publish(ForgeEvent {
                    event_type: "task.auto_transitioned".to_owned(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskAutoTransitioned {
                        task_id: task.id,
                        from,
                        to: target,
                        reason: "executor_completed".to_owned(),
                    },
                });
                Ok(())
            }
            Err(ServiceError::Db(DbError::VersionConflict)) => {
                tracing::warn!(
                    task_id = %task.id,
                    "executor completion cascade version conflict"
                );
                Ok(())
            }
            Err(ServiceError::GuardRejection { guard, reason }) => {
                self.handle_executor_completion_guard_rejection(
                    &execution,
                    &task,
                    current_state,
                    &guard,
                    &reason,
                )
                .await
            }
            Err(error) => Err(error),
        }
    }

    async fn handle_executor_completion_guard_rejection(
        &self,
        execution: &Execution,
        task: &Task,
        current_state: &api_types::StateDefinition,
        guard: &str,
        reason: &str,
    ) -> Result<()> {
        if guard == "subtask_sequence_complete" && task.parent_task_id.is_none() {
            if let Some(next_turn) = self.subtasks_handoff(task).await? {
                match next_turn {
                    super::subtasks::NextTurn::Prompt { user_prompt } => {
                        if execution.agent_session_id.is_none() {
                            tracing::warn!(
                                task_id = %task.id,
                                execution_id = %execution.id,
                                "subtask handoff cannot resume: missing agent_session_id; blocking task"
                            );
                            return self
                                .annotate_workflow_guard_block(execution, task, guard, reason)
                                .await;
                        }
                        self.resume_execution_for_workflow_guard(execution, task, user_prompt)
                            .await?;
                    }
                    super::subtasks::NextTurn::AllDone => {
                        self.retry_parent_cascade_after_last_subtask(task).await?;
                    }
                }
                return Ok(());
            }
        }

        let budget = crate::task_service::config::runtime_retry_budget(
            task,
            crate::task_service::config::RetryBudgetKind::Execution,
            Some(&current_state.config),
            current_state.gate_config.as_ref(),
        )?;
        let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "invalid task metadata for {}: {error}",
                task.id
            ))
        })?;
        let retry_count = metadata
            .extra
            .get("workflow_guard_retry_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);

        if budget <= 0
            || retry_count >= budget as u64
            || execution.agent_session_id.is_none()
            || execution.agent_id.is_none()
        {
            return self
                .annotate_workflow_guard_block(execution, task, guard, reason)
                .await;
        }

        let attempt = retry_count + 1;
        metadata.extra.insert(
            "workflow_guard_retry_count".to_owned(),
            Value::Number(serde_json::Number::from(attempt)),
        );
        metadata.extra.insert(
            "last_workflow_guard_rejection_at".to_owned(),
            Value::String(now_rfc3339()),
        );
        metadata.extra.insert(
            "last_workflow_guard_name".to_owned(),
            Value::String(guard.to_owned()),
        );
        metadata.extra.insert(
            "last_workflow_guard_reason".to_owned(),
            Value::String(reason.to_owned()),
        );
        TaskRepo::set_metadata_json(&*self.db, &task.id, metadata.to_json(), &now_rfc3339())
            .await?;

        let prompt = render_workflow_guard_follow_up_prompt(guard, reason, attempt, budget as u64);
        self.resume_execution_for_workflow_guard(execution, task, prompt)
            .await?;
        Ok(())
    }

    fn resume_execution_for_workflow_guard<'a>(
        &'a self,
        execution: &'a Execution,
        task: &'a Task,
        prompt: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Execution>> + Send + 'a>> {
        Box::pin(async move {
            let agent_session_id = execution.agent_session_id.clone().ok_or_else(|| {
                ServiceError::invalid_operation(format!(
                    "execution {} missing agent_session_id",
                    execution.id
                ))
            })?;
            let snapshot_json = execution
                .executor_config_snapshot_json
                .as_deref()
                .ok_or_else(|| {
                    ServiceError::invalid_operation(format!(
                        "execution {} missing executor config snapshot",
                        execution.id
                    ))
                })?;
            let updated_snapshot =
                executor_snapshot_with_resume_thread(snapshot_json, &agent_session_id)?;
            let agent_id = execution.agent_id.clone().ok_or_else(|| {
                ServiceError::invalid_operation(format!(
                    "execution {} missing agent_id",
                    execution.id
                ))
            })?;
            let execution_id = new_uuid_v4();
            let now = now_rfc3339();
            let resumed = self
                .create_running_execution(
                    CreateExecution {
                        id: execution_id.clone(),
                        task_id: task.id.clone(),
                        agent_id: Some(agent_id),
                        role: execution.role.clone(),
                        status: ExecutionStatus::Running,
                        stop_reason: None,
                        stopped_by: None,
                        resume_policy: None,
                        stopped_at: None,
                        parent_execution_id: Some(execution.id.clone()),
                        agent_session_id: None,
                        agent_message_id: None,
                        last_activity_at: None,
                        summary: Some(prompt),
                        logs_path: Some(execution_logs_path(
                            &self.workspace_root,
                            &task.project_id,
                            &task.id,
                            &execution_id,
                        )),
                        before_sha: execution.before_sha.clone(),
                        after_sha: None,
                        error: None,
                        executor_config_snapshot_json: Some(updated_snapshot),
                        workspace_id: execution.workspace_id.clone(),
                        created_at: now.clone(),
                        updated_at: now,
                    },
                    false,
                )
                .await?;

            self.publish(ForgeEvent {
                event_type: "follow_up.dispatched".to_owned(),
                entity_id: task.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::FollowUpDispatched {
                    task_id: task.id.clone(),
                    parent_execution_id: execution.id.clone(),
                    execution_id: resumed.id.clone(),
                    trigger: "workflow_guard_rejected".to_owned(),
                },
            });

            self.start_execution(resumed.id.clone()).await?;

            Ok(resumed)
        })
    }

    async fn subtasks_handoff(&self, task: &Task) -> Result<Option<super::subtasks::NextTurn>> {
        let subtasks = db::TaskRepo::list_subtasks_ordered(&*self.db, &task.id).await?;
        if subtasks.is_empty() {
            return Ok(None);
        }

        match super::subtasks::finish_current_turn_and_begin_next(
            &self.db,
            &self.event_bus,
            &self.workspace_root,
            &task.id,
        )
        .await
        {
            Ok(next_turn) => Ok(Some(next_turn)),
            Err(error) => {
                tracing::error!(
                    task_id = %task.id,
                    %error,
                    "subtask handoff failed, falling back to generic handling"
                );
                Ok(None)
            }
        }
    }

    async fn retry_parent_cascade_after_last_subtask(&self, task: &Task) -> Result<()> {
        let task = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        let Some(target) = workflow
            .auto_transition_target(&task.status)
            .map(str::to_owned)
        else {
            return Ok(());
        };

        let from = task.status.clone();
        match self
            .transition(
                task.id.clone(),
                target.clone(),
                TransitionOptions {
                    version: task.version,
                    reason: Some("all subtasks completed".to_owned()),
                    triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow),
                    rejection: false,
                    defer_dispatch_seconds: None,
                },
            )
            .await
        {
            Ok(_) => {
                if let Err(error) = self.clear_workflow_guard_retry_metadata(&task.id).await {
                    tracing::warn!(
                        task_id = %task.id,
                        %error,
                        "failed to clear workflow guard retry metadata"
                    );
                }
                self.publish(ForgeEvent {
                    event_type: "task.auto_transitioned".to_owned(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskAutoTransitioned {
                        task_id: task.id,
                        from,
                        to: target,
                        reason: "all_subtasks_completed".to_owned(),
                    },
                });
                Ok(())
            }
            Err(ServiceError::Db(DbError::VersionConflict)) => {
                tracing::warn!(
                    task_id = %task.id,
                    "last subtask cascade version conflict"
                );
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn annotate_workflow_guard_block(
        &self,
        execution: &Execution,
        task: &Task,
        guard: &str,
        reason: &str,
    ) -> Result<()> {
        let annotation = api_types::TaskBlockingAnnotation {
            annotation_type: api_types::FailureKind::WorkflowGuardRejected,
            blocking_reason: guard.to_owned(),
            blocked_by: Some(
                api_types::Actor::system(api_types::SystemComponent::Workflow).display(),
            ),
            blocked_at: Some(now_rfc3339()),
            blocked_execution_id: Some(execution.id.clone()),
            artifact: Some(api_types::BlockingArtifact {
                kind: "execution".to_owned(),
                id: Some(execution.id.clone()),
                log_path: execution.logs_path.clone(),
            }),
            message: Some(reason.to_owned()),
            hook: None,
            recovery_actions: vec![
                api_types::RecoveryAction::ResumeSession,
                api_types::RecoveryAction::Reexecute,
                api_types::RecoveryAction::CancelTask,
            ],
        };
        let annotation = serde_json::to_string(&annotation).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize workflow-guard annotation: {error}"
            ))
        })?;
        let blocked_meta = json!({
            "reason": reason,
            "created_at": now_rfc3339(),
            "kind": api_types::FailureKind::WorkflowGuardRejected,
            "source": guard,
            "execution_id": execution.id,
        });
        let updated = TaskRepo::update_status(
            &*self.db,
            UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: task.status.clone(),
                assignee_id: None,
                error_annotation: Some(Some(annotation)),
                blocked_json: Some(Some(blocked_meta.to_string())),
                failed_json: Some(None),
                updated_at: now_rfc3339(),
            },
        )
        .await?;

        self.publish_domain_event_by_dedupe(&format!(
            "task-status-update:{}:{}",
            updated.id, updated.version
        ))
        .await;

        self.publish(ForgeEvent {
            event_type: "task.blocked".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskBlocked {
                project_id: updated.project_id,
                reason: reason.to_owned(),
                kind: Some(api_types::FailureKind::WorkflowGuardRejected),
                source: Some(guard.to_owned()),
                execution_id: Some(execution.id.clone()),
            },
        });
        Ok(())
    }

    async fn clear_workflow_guard_retry_metadata(&self, task_id: &str) -> Result<()> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "invalid task metadata for {}: {error}",
                task.id
            ))
        })?;
        let mut changed = false;
        for key in [
            "workflow_guard_retry_count",
            "last_workflow_guard_rejection_at",
            "last_workflow_guard_name",
            "last_workflow_guard_reason",
        ] {
            changed |= metadata.extra.remove(key).is_some();
        }
        if changed {
            TaskRepo::set_metadata_json(&*self.db, &task.id, metadata.to_json(), &now_rfc3339())
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn annotate_executor_failure_block(
        &self,
        execution: &Execution,
    ) -> Result<()> {
        self.annotate_executor_failure_block_with_retry(execution, true)
            .await
    }

    pub(crate) async fn annotate_dispatch_failure_block(
        &self,
        execution: &Execution,
    ) -> Result<()> {
        self.annotate_executor_failure_block_with_retry(execution, false)
            .await
    }

    /// Handle an execution that failed because no executor candidate could
    /// run (`FailureKind::ExecutorUnavailable`). Never consumes the task's
    /// execution retry budget: transient exhaustion (a retry time is known)
    /// schedules a deferred dispatch; permanent unavailability blocks the
    /// task for manual reconfiguration with no automatic redispatch loop.
    pub(crate) async fn annotate_executor_unavailable_block(
        &self,
        execution: &Execution,
        retry_at: Option<String>,
        attempts: Value,
    ) -> Result<()> {
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        if workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal) {
            return Ok(());
        }

        if execution.role != "interactive" {
            if let Some(retry_at) = retry_at.as_deref() {
                let dispatch_at = executor_unavailable_dispatch_time(retry_at, &task.id);
                crate::deferred_dispatch::set(
                    &self.db,
                    &task,
                    &task.status,
                    &dispatch_at,
                    "executor unavailable; retrying when usage recovers",
                )
                .await?;
                ExecutionRepo::update(
                    &*self.db,
                    db::UpdateExecution {
                        id: execution.id.clone(),
                        status: None,
                        stop_reason: None,
                        stopped_by: None,
                        resume_policy: Some(Some(db::ResumePolicy::Auto)),
                        stopped_at: None,
                        agent_session_id: None,
                        agent_message_id: None,
                        last_activity_at: None,
                        summary: None,
                        logs_path: None,
                        before_sha: None,
                        after_sha: None,
                        error: None,
                        executor_config_snapshot_json: None,
                        updated_at: now_rfc3339(),
                    },
                )
                .await?;
                tracing::info!(
                    task_id = %task.id,
                    execution_id = %execution.id,
                    %dispatch_at,
                    "all executor candidates unavailable; deferred dispatch scheduled without consuming retry budget"
                );
                return Ok(());
            }
        }

        let annotation = api_types::TaskBlockingAnnotation {
            annotation_type: api_types::FailureKind::ExecutorUnavailable,
            blocking_reason: "executor_unavailable".to_owned(),
            blocked_by: Some(
                api_types::Actor::system(api_types::SystemComponent::Executor).display(),
            ),
            blocked_at: Some(now_rfc3339()),
            blocked_execution_id: Some(execution.id.clone()),
            artifact: Some(api_types::BlockingArtifact {
                kind: "execution".to_owned(),
                id: Some(execution.id.clone()),
                log_path: execution.logs_path.clone(),
            }),
            message: Some(execution.error.clone().unwrap_or_else(|| {
                "No executor candidate is available (check CLI installs and authentication)"
                    .to_owned()
            })),
            hook: None,
            recovery_actions: vec![
                api_types::RecoveryAction::Reexecute,
                api_types::RecoveryAction::ResetToInitial,
                api_types::RecoveryAction::CancelTask,
            ],
        };
        let annotation = serde_json::to_string(&annotation).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize executor-unavailable annotation: {error}"
            ))
        })?;

        let reason = execution
            .error
            .clone()
            .unwrap_or_else(|| "no executor candidate available".to_owned());
        let blocked_meta = json!({
            "reason": reason,
            "created_at": now_rfc3339(),
            "kind": api_types::FailureKind::ExecutorUnavailable,
            "execution_id": execution.id,
            "details": {
                "retry_at": retry_at,
                "attempts": attempts,
            },
        });

        let updated = TaskRepo::update_status(
            &*self.db,
            UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: task.status.clone(),
                assignee_id: None,
                error_annotation: Some(Some(annotation)),
                blocked_json: Some(Some(blocked_meta.to_string())),
                failed_json: Some(None),
                updated_at: now_rfc3339(),
            },
        )
        .await?;

        self.publish_domain_event_by_dedupe(&format!(
            "task-status-update:{}:{}",
            updated.id, updated.version
        ))
        .await;

        tracing::info!(
            task_id = %task.id,
            execution_id = %execution.id,
            status = %task.status,
            kind = "executor_unavailable",
            "task blocked: no executor candidate available"
        );
        self.publish(ForgeEvent {
            event_type: "task.blocked".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskBlocked {
                project_id: updated.project_id,
                reason,
                kind: Some(api_types::FailureKind::ExecutorUnavailable),
                source: None,
                execution_id: Some(execution.id.clone()),
            },
        });
        Ok(())
    }

    async fn annotate_executor_failure_block_with_retry(
        &self,
        execution: &Execution,
        allow_retry: bool,
    ) -> Result<()> {
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        if workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal) {
            return Ok(());
        }
        let current_state = workflow
            .states
            .iter()
            .find(|state| state.name == task.status);
        if allow_retry
            && self
                .maybe_schedule_execution_retry(
                    execution,
                    &task,
                    current_state.map(|state| &state.config),
                    current_state.and_then(|state| state.gate_config.as_ref()),
                )
                .await?
        {
            return Ok(());
        }

        // Even on executor failure the agent may have committed real work for the
        // current subtask before its process died. Credit that commit so the
        // subtask isn't stuck `in_progress` and the parent doesn't miss progress.
        if task.parent_task_id.is_none() {
            match super::subtasks::credit_in_progress_subtask_commit(
                &self.db,
                &self.event_bus,
                &self.workspace_root,
                &task.id,
            )
            .await
            {
                Ok(super::subtasks::CreditResult::Committed { all_done: true }) => {
                    tracing::info!(
                        task_id = %task.id,
                        execution_id = %execution.id,
                        "executor failed but final subtask was committed; cascading parent to next state"
                    );
                    let task = TaskRepo::get_by_id(&*self.db, &task.id, false)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
                    return self.retry_parent_cascade_after_last_subtask(&task).await;
                }
                Ok(super::subtasks::CreditResult::Committed { all_done: false }) => {
                    tracing::info!(
                        task_id = %task.id,
                        execution_id = %execution.id,
                        "executor failed but a subtask was committed; crediting and falling through to block"
                    );
                    // Fall through to the original block-annotation path. The
                    // user can resume to dispatch the next subtask.
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        task_id = %task.id,
                        execution_id = %execution.id,
                        %error,
                        "failed to credit in-progress subtask commit on executor failure"
                    );
                }
            }
        }

        let mut recovery_actions = vec![
            api_types::RecoveryAction::Reexecute,
            api_types::RecoveryAction::ResetToInitial,
            api_types::RecoveryAction::CancelTask,
        ];
        if execution.agent_session_id.is_some() {
            recovery_actions.insert(0, api_types::RecoveryAction::ResumeSession);
        }
        let annotation = api_types::TaskBlockingAnnotation {
            annotation_type: api_types::FailureKind::ExecutorFailed,
            blocking_reason: "executor_failed".to_owned(),
            blocked_by: Some(
                api_types::Actor::system(api_types::SystemComponent::Executor).display(),
            ),
            blocked_at: Some(now_rfc3339()),
            blocked_execution_id: Some(execution.id.clone()),
            artifact: Some(api_types::BlockingArtifact {
                kind: "execution".to_owned(),
                id: Some(execution.id.clone()),
                log_path: execution.logs_path.clone(),
            }),
            message: Some(
                execution
                    .error
                    .clone()
                    .unwrap_or_else(|| "Execution failed".to_owned()),
            ),
            hook: None,
            recovery_actions,
        };
        let annotation = serde_json::to_string(&annotation).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize executor-failure annotation: {error}"
            ))
        })?;

        let reason = execution
            .error
            .clone()
            .unwrap_or_else(|| "executor failed".to_owned());
        let blocked_meta = json!({
            "reason": reason,
            "created_at": now_rfc3339(),
            "kind": api_types::FailureKind::InternalCommandFailed,
            "execution_id": execution.id,
        });

        let updated = TaskRepo::update_status(
            &*self.db,
            UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: task.status.clone(),
                assignee_id: None,
                error_annotation: Some(Some(annotation)),
                blocked_json: Some(Some(blocked_meta.to_string())),
                failed_json: Some(None),
                updated_at: now_rfc3339(),
            },
        )
        .await?;

        self.publish_domain_event_by_dedupe(&format!(
            "task-status-update:{}:{}",
            updated.id, updated.version
        ))
        .await;

        tracing::info!(
            task_id = %task.id,
            execution_id = %execution.id,
            status = %task.status,
            kind = "internal_command_failed",
            "task blocked after executor failure"
        );
        self.publish(ForgeEvent {
            event_type: "task.blocked".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskBlocked {
                project_id: updated.project_id,
                reason,
                kind: Some(api_types::FailureKind::InternalCommandFailed),
                source: None,
                execution_id: Some(execution.id.clone()),
            },
        });
        Ok(())
    }

    async fn maybe_schedule_execution_retry(
        &self,
        execution: &Execution,
        task: &Task,
        state_config: Option<&Value>,
        gate_config: Option<&api_types::GateConfig>,
    ) -> Result<bool> {
        if execution.role == "interactive" {
            // Interactive runs are user-prompted and do not have a durable dispatcher target yet.
            return Ok(false);
        }

        let budget = crate::task_service::config::runtime_retry_budget(
            task,
            crate::task_service::config::RetryBudgetKind::Execution,
            state_config,
            gate_config,
        )?;
        if budget <= 0 {
            return Ok(false);
        }

        let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "invalid task metadata for {}: {error}",
                task.id
            ))
        })?;
        let retry_count = metadata
            .extra
            .get("execution_retry_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if retry_count >= budget as u64 {
            return Ok(false);
        }

        let attempt = retry_count + 1;
        let delay_seconds =
            (10_u64.saturating_mul(2_u64.saturating_pow(retry_count as u32))).min(300);
        let next_dispatch_at = chrono::Utc::now() + chrono::Duration::seconds(delay_seconds as i64);
        metadata.extra.insert(
            "execution_retry_count".to_owned(),
            Value::Number(serde_json::Number::from(attempt)),
        );
        metadata.extra.insert(
            "last_execution_failure_at".to_owned(),
            Value::String(now_rfc3339()),
        );
        TaskRepo::set_metadata_json(&*self.db, &task.id, metadata.to_json(), &now_rfc3339())
            .await?;
        let task = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        crate::deferred_dispatch::set(
            &self.db,
            &task,
            &task.status,
            &next_dispatch_at.to_rfc3339(),
            &format!("execution retry (attempt {attempt})"),
        )
        .await?;
        ExecutionRepo::update(
            &*self.db,
            db::UpdateExecution {
                id: execution.id.clone(),
                status: None,
                stop_reason: None,
                stopped_by: None,
                resume_policy: Some(Some(db::ResumePolicy::Auto)),
                stopped_at: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;

        tracing::info!(
            task_id = %task.id,
            execution_id = %execution.id,
            attempt,
            delay_seconds,
            next_dispatch_at = %next_dispatch_at.to_rfc3339(),
            "scheduling execution retry"
        );
        self.publish(ForgeEvent {
            event_type: "task.execution_retry".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskExecutionRetry {
                task_id: task.id.clone(),
                execution_id: execution.id.clone(),
                attempt: attempt as u32,
                delay_seconds,
                next_dispatch_at: next_dispatch_at.to_rfc3339(),
            },
        });
        Ok(true)
    }

    async fn maybe_cascade_reviewer_completion(&self, execution: &Execution) -> Result<()> {
        let task = match TaskRepo::get_by_id(&*self.db, &execution.task_id, false).await? {
            Some(task) => task,
            None => return Ok(()),
        };
        if task.status != crate::workflow::default_states::REVIEW {
            return Ok(());
        }

        let review = self
            .ensure_current_review_for_reviewer(&task.id, &execution.id)
            .await?;
        if review.execution_id == execution.id && review.status != ReviewStatus::Running {
            tracing::debug!(
                task_id = %task.id,
                execution_id = %execution.id,
                review_id = %review.id,
                status = %review.status,
                "reviewer completion already processed"
            );
            return Ok(());
        }
        if execution.status != ExecutionStatus::Completed {
            return self
                .fail_review_for_reviewer_execution_exit(&task, execution, review)
                .await;
        }
        let user_approval_required = self.gate_requires_user_approval(&task).await?;
        let final_message = reviewer_final_message(execution).await?;
        let (status, auditor_details) = match ::review::auditor::parse_verdict(&final_message) {
            ::review::auditor::AuditorVerdict::Passed if user_approval_required => {
                (ReviewStatus::AwaitingHuman, json!({ "verdict": "pass" }))
            }
            ::review::auditor::AuditorVerdict::Passed => {
                (ReviewStatus::Passed, json!({ "verdict": "pass" }))
            }
            ::review::auditor::AuditorVerdict::Failed { reason } => (
                ReviewStatus::Failed,
                json!({ "verdict": "fail", "reason": reason }),
            ),
        };
        let comment = reviewer_comment(status.clone(), review.attempt_number, &final_message);

        let finished_at = now_rfc3339();
        let mut review_details = normalize_review_details(&review.step_results_json);
        review_details["auditor"] = auditor_details;
        if status == ReviewStatus::AwaitingHuman {
            review_details["user_approval"] = json!({
                "status": "awaiting_human",
                "reason": "gate requires user approval",
            });
        }
        let updated_review = ReviewRepo::update_status(
            &*self.db,
            &review.id,
            status.clone(),
            review_details.to_string(),
            (status != ReviewStatus::AwaitingHuman).then_some(finished_at.clone()),
            &finished_at,
        )
        .await?;
        self.publish_domain_event_by_dedupe(&format!(
            "review-status:{}:{}:{}",
            updated_review.id, updated_review.status, finished_at
        ))
        .await;
        if let Err(error) = self
            .memory_service
            .record_review_result_if_final(&task.project_id, &updated_review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }

        match status {
            ReviewStatus::Passed => {
                let task = if task.review_passed_at.is_some() {
                    task
                } else {
                    TaskRepo::set_review_passed_at(
                        &*self.db,
                        &task.id,
                        Some(finished_at.clone()),
                        &finished_at,
                    )
                    .await?
                };
                self.publish(ForgeEvent {
                    event_type: "review.passed".to_owned(),
                    entity_id: updated_review.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::ReviewPassed {
                        task_id: task.id.clone(),
                        review_id: updated_review.id.clone(),
                        attempt_number: updated_review.attempt_number,
                    },
                });
                self.publish_reviewer_comment(execution, &task.id, comment)
                    .await?;
                self.cascade_completed_review_task(
                    &task,
                    crate::workflow::default_states::MERGING,
                    "review passed",
                    false,
                )
                .await?;
            }
            ReviewStatus::AwaitingHuman => {
                self.publish_reviewer_comment(execution, &task.id, comment)
                    .await?;
                self.publish(ForgeEvent {
                    event_type: "task.awaiting_human".to_owned(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskAwaitingHuman {
                        task_id: task.id.clone(),
                        role: crate::workflow::default_roles::REVIEWER.to_owned(),
                        assignee_id: "human".to_owned(),
                        state: crate::workflow::default_states::REVIEW.to_owned(),
                    },
                });
            }
            ReviewStatus::Failed => {
                let task =
                    TaskRepo::set_review_passed_at(&*self.db, &task.id, None, &finished_at).await?;
                self.publish(ForgeEvent {
                    event_type: "review.failed".to_owned(),
                    entity_id: updated_review.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::ReviewFailed {
                        task_id: task.id.clone(),
                        review_id: updated_review.id.clone(),
                        attempt_number: updated_review.attempt_number,
                        failed_step_index: 0,
                    },
                });
                self.publish_reviewer_comment(execution, &task.id, comment)
                    .await?;
                let (task, target, reason) = self
                    .review_failure_target(&task, Some(&execution.id))
                    .await?;
                if let Some(target) = target {
                    self.cascade_completed_review_task(&task, &target, &reason, true)
                        .await?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    async fn fail_review_for_reviewer_execution_exit(
        &self,
        task: &Task,
        execution: &Execution,
        review: Review,
    ) -> Result<()> {
        let finished_at = now_rfc3339();
        let reason = execution_failure_reason(execution);
        let mut review_details = normalize_review_details(&review.step_results_json);
        review_details["auditor"] = json!({
            "verdict": "fail",
            "reason": reason,
        });
        review_details["execution"] = json!({
            "id": execution.id,
            "status": execution.status.to_string(),
            "stop_reason": execution.stop_reason.as_ref().map(ToString::to_string),
            "error": execution.error.as_deref(),
        });
        let updated_review = ReviewRepo::update_status(
            &*self.db,
            &review.id,
            ReviewStatus::Failed,
            review_details.to_string(),
            Some(finished_at.clone()),
            &finished_at,
        )
        .await?;
        self.publish_domain_event_by_dedupe(&format!(
            "review-status:{}:{}:{}",
            updated_review.id, updated_review.status, finished_at
        ))
        .await;
        if let Err(error) = self
            .memory_service
            .record_review_result_if_final(&task.project_id, &updated_review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }
        let task = TaskRepo::set_review_passed_at(&*self.db, &task.id, None, &finished_at).await?;

        self.publish(ForgeEvent {
            event_type: "review.failed".to_owned(),
            entity_id: updated_review.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReviewFailed {
                task_id: task.id.clone(),
                review_id: updated_review.id.clone(),
                attempt_number: updated_review.attempt_number,
                failed_step_index: 0,
            },
        });
        self.publish_reviewer_comment(
            execution,
            &task.id,
            format!(
                "Review failed (attempt {}): reviewer execution {}",
                updated_review.attempt_number, reason
            ),
        )
        .await?;

        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        let current_state = workflow
            .states
            .iter()
            .find(|state| state.name == task.status);
        if self
            .maybe_schedule_execution_retry(
                execution,
                &task,
                current_state.map(|state| &state.config),
                current_state.and_then(|state| state.gate_config.as_ref()),
            )
            .await?
        {
            return Ok(());
        }

        Ok(())
    }

    async fn ensure_current_review_for_reviewer(
        &self,
        task_id: &str,
        execution_id: &str,
    ) -> Result<Review> {
        let reviews = ReviewRepo::list_by_task(&*self.db, task_id).await?;
        if let Some(review) = reviews
            .iter()
            .find(|review| review.execution_id == execution_id)
            .cloned()
        {
            return Ok(review);
        }
        let latest = reviews
            .into_iter()
            .max_by_key(|review| review.attempt_number);
        match latest {
            Some(review)
                if matches!(
                    review.status,
                    ReviewStatus::Running | ReviewStatus::AwaitingHuman
                ) =>
            {
                Ok(review)
            }
            _ => {
                let now = now_rfc3339();
                ReviewRepo::create(
                    &*self.db,
                    CreateReview {
                        id: new_uuid_v4(),
                        task_id: task_id.to_owned(),
                        execution_id: execution_id.to_owned(),
                        attempt_number: ReviewRepo::next_attempt_number(&*self.db, task_id).await?,
                        status: ReviewStatus::Running,
                        step_results_json: json!({ "ci_steps": [] }).to_string(),
                        started_at: now.clone(),
                        created_at: now.clone(),
                        updated_at: now,
                    },
                )
                .await
                .map_err(Into::into)
            }
        }
    }

    async fn publish_reviewer_comment(
        &self,
        execution: &Execution,
        task_id: &str,
        content: String,
    ) -> Result<()> {
        if let Some(agent_id) = execution.agent_id.as_deref() {
            self.create_agent_comment(task_id, agent_id, content).await
        } else {
            self.create_system_comment(task_id, content).await
        }
    }

    async fn review_failure_target(
        &self,
        task: &Task,
        execution_id: Option<&str>,
    ) -> Result<(Task, Option<String>, String)> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        let review_state = workflow
            .states
            .iter()
            .find(|state| state.name == crate::workflow::default_states::REVIEW);
        let budget = crate::task_service::config::runtime_retry_budget(
            task,
            crate::task_service::config::RetryBudgetKind::Review,
            review_state.map(|state| &state.config),
            review_state.and_then(|state| state.gate_config.as_ref()),
        )?;
        let entries = TransitionLogRepo::list_by_task(&*self.db, &task.id).await?;
        let existing_count = review_rejections_since_boundary(&entries);
        if existing_count + 1 >= i64::from(budget) {
            let reason = "review retry budget exhausted";
            if let Some((task, recovery_reason)) = self
                .try_dispatch_automatic_review_recovery(
                    &project,
                    task,
                    execution_id,
                    existing_count,
                    budget,
                    reason,
                )
                .await?
            {
                return Ok((task, None, recovery_reason));
            }
            tracing::info!(
                task_id = %task.id,
                rejections = existing_count,
                budget = i64::from(budget),
                "review retry budget exhausted, blocking task"
            );
            let blocked_meta = json!({
                "reason": reason,
                "created_at": now_rfc3339(),
                "kind": api_types::FailureKind::ReviewGateFailed,
                "source": null,
                "execution_id": execution_id,
            });
            let annotation = json!({
                "type": api_types::FailureKind::ReviewBudgetExhausted,
                "blocking_reason": reason,
                "message": reason,
                "detected_at": now_rfc3339(),
                "recovery_actions": [
                    api_types::RecoveryAction::ResetRetryWindow,
                    api_types::RecoveryAction::ProceedOnce,
                    api_types::RecoveryAction::OpenInteractive,
                ],
            });
            let mut current = task.clone();
            let mut updated = None;
            for attempt in 0..3 {
                match TaskRepo::update(
                    &*self.db,
                    UpdateTask {
                        id: current.id.clone(),
                        expected_version: current.version,
                        title: None,
                        description: None,
                        priority: None,
                        merge_config: None,
                        plan: None,
                        error_annotation: Some(Some(annotation.to_string())),
                        blocked_json: Some(Some(blocked_meta.to_string())),
                        failed_json: Some(None),
                        task_state_config: None,
                        parent_task_id: None,
                        updated_at: now_rfc3339(),
                    },
                )
                .await
                {
                    Ok(task) => {
                        updated = Some(task);
                        break;
                    }
                    Err(DbError::VersionConflict) if attempt < 2 => {
                        current = TaskRepo::get_by_id(&*self.db, &task.id, false)
                            .await?
                            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            let task = updated.ok_or(ServiceError::Db(DbError::VersionConflict))?;
            self.publish(ForgeEvent {
                event_type: "task.blocked".to_owned(),
                entity_id: task.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskBlocked {
                    project_id: task.project_id.clone(),
                    reason: reason.to_owned(),
                    kind: Some(api_types::FailureKind::ReviewGateFailed),
                    source: None,
                    execution_id: execution_id.map(str::to_owned),
                },
            });
            Ok((task, None, reason.to_owned()))
        } else {
            let target = crate::workflow::default_states::IN_PROGRESS.to_owned();
            tracing::debug!(
                task_id = %task.id,
                rejections = existing_count,
                budget = i64::from(budget),
                target = %target,
                "review failure within budget, cascading"
            );
            Ok((task.clone(), Some(target), "review failed".to_owned()))
        }
    }

    async fn try_dispatch_automatic_review_recovery(
        &self,
        project: &db::Project,
        task: &Task,
        review_execution_id: Option<&str>,
        existing_rejections: i64,
        budget: i32,
        failure_reason: &str,
    ) -> Result<Option<(Task, String)>> {
        let Some(parent_execution_id) = review_execution_id else {
            return Ok(None);
        };
        let settings = match serde_json::from_str::<ProjectSettings>(&project.settings) {
            Ok(settings) => settings,
            Err(error) => {
                tracing::warn!(
                    task_id = %task.id,
                    project_id = %project.id,
                    %error,
                    "automatic review recovery skipped because project settings are invalid"
                );
                return Ok(None);
            }
        };
        let recovery = settings.automatic_recovery;
        if !recovery.enabled {
            return Ok(None);
        }
        let Some(agent_id) = recovery
            .agent_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
        else {
            tracing::warn!(
                task_id = %task.id,
                project_id = %project.id,
                "automatic review recovery is enabled without a recovery agent"
            );
            return Ok(None);
        };

        let max_attempts = recovery.max_attempts.max(1) as usize;
        let page = ExecutionRepo::list_by_task(
            &*self.db,
            &task.id,
            PageRequest {
                cursor: None,
                limit: 100,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await?;
        let is_automatic_recovery = |execution: &Execution| {
            execution
                .summary
                .as_deref()
                .is_some_and(|summary| summary.starts_with(AUTOMATIC_REVIEW_RECOVERY_PROMPT_PREFIX))
        };
        let recovery_attempts = page
            .items
            .iter()
            .filter(|execution| is_automatic_recovery(execution))
            .count();
        if recovery_attempts >= max_attempts {
            return Ok(None);
        }
        if page.items.iter().any(|execution| {
            execution.status == ExecutionStatus::Running && is_automatic_recovery(execution)
        }) {
            return Ok(Some((
                task.clone(),
                "automatic review recovery already running".to_owned(),
            )));
        }

        let prompt = render_automatic_review_recovery_prompt(
            task,
            failure_reason,
            existing_rejections,
            budget,
            parent_execution_id,
            recovery_attempts + 1,
            max_attempts,
        );
        let execution = match self
            .dispatch_role_follow_up_with_agent(
                &task.id,
                crate::workflow::default_roles::CODER,
                parent_execution_id.to_owned(),
                agent_id.clone(),
                prompt,
                AUTOMATIC_REVIEW_RECOVERY_TRIGGER,
            )
            .await
        {
            Ok(execution) => execution,
            Err(error) => {
                tracing::warn!(
                    task_id = %task.id,
                    project_id = %project.id,
                    agent_id = %agent_id,
                    %error,
                    "automatic review recovery dispatch failed"
                );
                return Ok(None);
            }
        };

        self.create_system_comment(
            &task.id,
            format!(
                "Automatic recovery dispatched before blocking: execution {}",
                execution.id
            ),
        )
        .await?;
        let latest_task = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .unwrap_or_else(|| task.clone());
        Ok(Some((
            latest_task,
            "automatic review recovery dispatched".to_owned(),
        )))
    }

    async fn cascade_completed_review_task(
        &self,
        task: &Task,
        target: &str,
        reason: &str,
        rejection: bool,
    ) -> Result<()> {
        let from = task.status.clone();
        match self
            .transition(
                task.id.clone(),
                target.to_owned(),
                TransitionOptions {
                    version: task.version,
                    reason: Some(reason.to_owned()),
                    triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow),
                    rejection,
                    defer_dispatch_seconds: None,
                },
            )
            .await
        {
            Ok(_) => {
                self.publish(ForgeEvent {
                    event_type: "task.auto_transitioned".to_owned(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskAutoTransitioned {
                        task_id: task.id.clone(),
                        from,
                        to: target.to_owned(),
                        reason: reason.to_owned(),
                    },
                });
                Ok(())
            }
            Err(ServiceError::Db(DbError::VersionConflict)) => {
                tracing::warn!(
                    task_id = %task.id,
                    "review completion cascade version conflict"
                );
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn gate_requires_user_approval(&self, task: &Task) -> Result<bool> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        Ok(workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(|state| state.gate_config.as_ref())
            .is_some_and(|gate_config| gate_config.requires_user_approval()))
    }
}

fn render_workflow_guard_follow_up_prompt(
    guard: &str,
    reason: &str,
    attempt: u64,
    budget: u64,
) -> String {
    format!(
        "Your previous execution completed, but Forge could not move the task to the next workflow state.\n\nWorkflow guard failed: {guard}\n\nFailure:\n{reason}\n\nMake sure you complete all tasks and Fix what is needed for this guard, update any completed checklist items to `- [x]`, you dont need to commit anything if all tasks are complete.\n\nRetry {attempt}/{budget}."
    )
}

fn render_automatic_review_recovery_prompt(
    task: &Task,
    failure_reason: &str,
    existing_rejections: i64,
    budget: i32,
    review_execution_id: &str,
    attempt: usize,
    max_attempts: usize,
) -> String {
    format!(
        "{AUTOMATIC_REVIEW_RECOVERY_PROMPT_PREFIX}\n\n\
         The normal review retry flow is about to block this task, so this is the final automatic recovery attempt.\n\n\
         Task: {title}\n\
         Current status: {status}\n\
         Review failure: {failure_reason}\n\
         Review execution: {review_execution_id}\n\
         Rejections in current window: {rejections}/{budget}\n\
         Automatic recovery attempt: {attempt}/{max_attempts}\n\n\
         Inspect the workspace and the review failure context. Make the smallest useful change that addresses the failing review, then leave the task ready for the normal workflow to review again.",
        title = task.title,
        status = task.status,
        rejections = existing_rejections + 1,
    )
}

fn review_rejections_since_boundary(entries: &[db::TransitionLog]) -> i64 {
    let boundary = entries.iter().rposition(|entry| {
        entry.from_state == crate::workflow::default_states::REVIEW
            && !entry.rejection
            && (entry.to_state != crate::workflow::default_states::REVIEW
                || entry.trigger_name.as_deref() == Some("reset_retry_window"))
    });
    let entries = boundary
        .and_then(|index| entries.get(index + 1..))
        .unwrap_or(entries);
    entries
        .iter()
        .filter(|entry| {
            entry.from_state == crate::workflow::default_states::REVIEW && entry.rejection
        })
        .count() as i64
}

async fn reviewer_final_message(execution: &Execution) -> Result<String> {
    if let Some(logs_path) = execution.logs_path.as_deref() {
        let contents = match tokio::fs::read_to_string(logs_path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => {
                return Err(ServiceError::invalid_operation(format!(
                    "failed to read reviewer logs: {error}"
                )));
            }
        };
        let mut message = String::new();
        let mut stdout_lines = String::new();
        for line in contents.lines() {
            let Ok(entry) = serde_json::from_str::<executors::LogEntry>(line) else {
                continue;
            };
            if entry.kind == executors::LogKind::Assistant {
                append_reviewer_log_text(&entry.payload, &mut message);
            } else if entry.kind == executors::LogKind::SessionInfo
                && entry.payload.get("subtype").and_then(Value::as_str) == Some("success")
            {
                if let Some(result) = entry.payload.get("result").and_then(Value::as_str) {
                    message.push_str(result);
                }
            } else if entry.kind == executors::LogKind::Stdout {
                if let Some(line) = entry.payload.get("line").and_then(Value::as_str) {
                    stdout_lines.push_str(line);
                    stdout_lines.push('\n');
                }
            }
        }
        if !message.trim().is_empty() {
            return Ok(message);
        }
        if !stdout_lines.trim().is_empty() {
            return Ok(stdout_lines);
        }
    }

    Ok(execution.summary.clone().unwrap_or_default())
}

fn append_reviewer_log_text(payload: &Value, message: &mut String) {
    if let Some(text) = payload.get("text").and_then(Value::as_str) {
        message.push_str(text);
    }

    let Some(content) = payload
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
    else {
        return;
    };

    for item in content {
        if item.get("type").and_then(Value::as_str) == Some("text") {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                message.push_str(text);
            }
        }
    }
}

/// Dispatch time for a transient executor-unavailable retry: the structured
/// retry hint plus a small deterministic jitter, floored at ten seconds out
/// so a stale hint cannot hot-loop.
fn executor_unavailable_dispatch_time(retry_at: &str, task_id: &str) -> String {
    let jitter_seconds = i64::from(
        task_id
            .bytes()
            .fold(0u8, |acc, byte| acc.wrapping_add(byte))
            % 30,
    );
    let hinted = chrono::DateTime::parse_from_rfc3339(retry_at)
        .map(|at| at.with_timezone(&chrono::Utc))
        .unwrap_or_else(|_| chrono::Utc::now() + chrono::Duration::minutes(15));
    let floor = chrono::Utc::now() + chrono::Duration::seconds(10);
    (hinted + chrono::Duration::seconds(jitter_seconds))
        .max(floor)
        .to_rfc3339()
}

pub(crate) fn should_block_task_for_failed_execution(execution: &Execution) -> bool {
    matches!(
        execution.role.as_str(),
        "interactive" | "executor" | crate::workflow::default_roles::CODER
    )
}

fn execution_failure_reason(execution: &Execution) -> String {
    execution
        .error
        .as_deref()
        .or(execution.summary.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("ended with status {}", execution.status))
}

fn normalize_review_details(step_results_json: &str) -> Value {
    match serde_json::from_str::<Value>(step_results_json) {
        Ok(Value::Array(ci_steps)) => json!({ "ci_steps": ci_steps }),
        Ok(Value::Object(mut object)) => {
            if !object.contains_key("ci_steps") {
                object.insert("ci_steps".to_owned(), Value::Array(Vec::new()));
            }
            Value::Object(object)
        }
        _ => json!({ "ci_steps": [] }),
    }
}

fn reviewer_comment(status: ReviewStatus, attempt_number: i64, final_message: &str) -> String {
    let fallback = match status {
        ReviewStatus::AwaitingHuman => format!(
            "Review passed automated checks and is awaiting user approval (attempt {attempt_number})"
        ),
        ReviewStatus::Passed => format!("Review passed (attempt {attempt_number})"),
        ReviewStatus::Failed => {
            let reason = match ::review::auditor::parse_verdict(final_message) {
                ::review::auditor::AuditorVerdict::Failed { reason } => reason,
                ::review::auditor::AuditorVerdict::Passed => "review failed".to_owned(),
            };
            format!("Review failed (attempt {attempt_number}): {reason}")
        }
        _ => format!("Review updated (attempt {attempt_number})"),
    };
    let cleaned = strip_review_verdict_marker(final_message).trim().to_owned();
    if cleaned.is_empty() {
        fallback
    } else {
        cleaned
    }
}

fn strip_review_verdict_marker(message: &str) -> String {
    let mut cleaned = message.replace("===REVIEW: PASS===", "");
    while let Some(start) = cleaned.find("===REVIEW: FAIL: ") {
        let Some(relative_end) = cleaned[start + "===REVIEW: FAIL: ".len()..].find("===") else {
            break;
        };
        let end = start + "===REVIEW: FAIL: ".len() + relative_end + "===".len();
        cleaned.replace_range(start..end, "");
    }
    cleaned
}

#[cfg(test)]
mod reviewer_message_tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn reviewer_execution(logs_path: String, summary: &str) -> Execution {
        let now = now_rfc3339();
        Execution {
            id: "execution-reviewer".to_owned(),
            task_id: "task-reviewer".to_owned(),
            agent_id: Some("agent-reviewer".to_owned()),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            execution_version: 1,
            lease_owner: None,
            lease_expires_at: None,
            hard_deadline_at: None,
            last_heartbeat_at: None,
            last_progress_at: None,
            prompt: None,
            summary: Some(summary.to_owned()),
            logs_path: Some(logs_path),
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn reviewer_final_message_reads_claude_assistant_content() {
        let file = NamedTempFile::new().expect("temp log creates");
        let log = json!({
            "schema_version": 1,
            "sequence": 1,
            "timestamp": "2026-04-27T00:00:00Z",
            "execution_id": "execution-reviewer",
            "kind": "assistant",
            "stream": "main",
            "payload": {
                "message": {
                    "content": [
                        {
                            "type": "text",
                            "text": "No issues found.\n===REVIEW: PASS==="
                        }
                    ]
                }
            },
            "truncated": false
        });
        std::fs::write(file.path(), format!("{log}\n")).expect("log writes");

        let execution = reviewer_execution(
            file.path().to_string_lossy().into_owned(),
            "Truncated summary without marker",
        );

        let message = reviewer_final_message(&execution)
            .await
            .expect("message extracts");
        assert!(message.contains("===REVIEW: PASS==="));
    }

    #[tokio::test]
    async fn reviewer_final_message_reads_claude_result_when_assistant_text_missing() {
        let file = NamedTempFile::new().expect("temp log creates");
        let log = json!({
            "schema_version": 1,
            "sequence": 1,
            "timestamp": "2026-04-27T00:00:00Z",
            "execution_id": "execution-reviewer",
            "kind": "session_info",
            "stream": "main",
            "payload": {
                "subtype": "success",
                "result": "Looks good.\n===REVIEW: PASS==="
            },
            "truncated": false
        });
        std::fs::write(file.path(), format!("{log}\n")).expect("log writes");

        let execution = reviewer_execution(
            file.path().to_string_lossy().into_owned(),
            "Truncated summary without marker",
        );

        let message = reviewer_final_message(&execution)
            .await
            .expect("message extracts");
        assert!(message.contains("===REVIEW: PASS==="));
    }

    #[test]
    fn reviewer_comment_uses_clean_final_message() {
        let comment = reviewer_comment(
            ReviewStatus::Passed,
            1,
            "No blocking issues found.\n===REVIEW: PASS===",
        );

        assert_eq!(comment, "No blocking issues found.");
    }

    #[test]
    fn reviewer_comment_falls_back_when_only_marker_exists() {
        let comment = reviewer_comment(ReviewStatus::Passed, 2, "===REVIEW: PASS===");

        assert_eq!(comment, "Review passed (attempt 2)");
    }
}
