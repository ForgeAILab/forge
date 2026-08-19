use super::*;

const EXECUTION_LOG_BATCH_MAX_ENTRIES: usize = 50;
const EXECUTION_LOG_BATCH_MAX_WAIT: Duration = Duration::from_millis(500);

impl TaskService {
    pub async fn start_execution(
        &self,
        execution_id: impl Into<String>,
    ) -> Result<api_types::ExecutionStartResult> {
        let execution_id = execution_id.into();
        validate_required("execution_id", &execution_id)?;
        let execution = ExecutionRepo::get_by_id(&*self.db, &execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", execution_id.clone()))?;
        if execution.status != ExecutionStatus::Running {
            return Err(ServiceError::invalid_operation(
                "only running executions can be started",
            ));
        }
        // Remote and local adapters both require the scheduler-issued lease;
        // checking at this boundary closes the gap between execution-row
        // creation and adapter launch/recovery.
        if let Err(error) = self.verify_execution_workspace_authority(&execution).await {
            // Authority can be revoked or superseded between execution-row
            // creation and this dispatch attempt.  Stop the attempt and
            // revoke any remaining grant before surfacing the denial.
            let failure_message = error.to_string();
            if let Err(mark_error) = self
                .fail_execution_before_dispatch(&execution.id, failure_message)
                .await
            {
                tracing::warn!(
                    execution_id = %execution.id,
                    %mark_error,
                    "failed to terminalize execution after initial WorkspaceLease verification failure"
                );
            }
            return Err(error);
        }

        let result = async {
            let agent = match execution.agent_id.as_deref() {
                Some(agent_id) => Some(
                    AgentRepo::get_by_id(&*self.db, agent_id)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("agent", agent_id.to_owned()))?,
                ),
                None => None,
            };
            let provider = self
                .execution_provider_for_agent(agent.as_ref(), &execution.id)
                .await?;
            let params = self.execution_start_params(&execution).await?;
            provider.start(params).await
        }
        .await;

        match result {
            Ok(result) => Ok(result),
            Err(error) => {
                let failure_message = error.to_string();
                if let Err(mark_error) = self
                    .fail_execution_before_dispatch(&execution.id, failure_message)
                    .await
                {
                    tracing::warn!(
                        execution_id = %execution.id,
                        %mark_error,
                        "failed to mark execution failed after dispatch start error"
                    );
                }
                Err(error)
            }
        }
    }

    pub async fn run_execution(
        &self,
        execution_id: impl Into<String>,
        executor: &dyn TaskExecutor,
    ) -> Result<db::Execution> {
        let execution_id = execution_id.into();
        validate_required("execution_id", &execution_id)?;
        tracing::info!(%execution_id, "execution dispatch starting");
        let execution = ExecutionRepo::get_by_id(&*self.db, &execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", execution_id.clone()))?;
        if execution.status != ExecutionStatus::Running {
            return Err(ServiceError::invalid_operation(
                "only running executions can be executed",
            ));
        }
        if let Some(failed) = self
            .wait_for_agent_active_before_dispatch(&execution)
            .await?
        {
            return Ok(failed);
        }
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let workspace_id = execution
            .workspace_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("execution missing workspace_id"))?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
        self.verify_active_workspace_lease(
            &task,
            &workspace,
            &execution.role,
            execution.agent_id.as_deref(),
            &execution.id,
        )
        .await?;
        let snapshot = execution
            .executor_config_snapshot_json
            .as_deref()
            .ok_or_else(|| {
                ServiceError::invalid_operation("execution missing executor config snapshot")
            })?;
        let mut agent_config = parse_json_value("executor config snapshot", snapshot)?;
        if execution.role == crate::workflow::default_roles::REVIEWER
            || matches!(task.task_type.as_str(), "planning_task" | "discovery")
        {
            executors::mark_worktree_read_only(&mut agent_config);
        }
        if agent_config.get("executor_type").and_then(Value::as_str) == Some("embedded") {
            crate::embedded_task_executor::set_task_role_marker(&mut agent_config, &execution.role);
        }
        // Provider-entry-backed harness agents get their API key injected into
        // the in-memory snapshot only; the stored snapshot never holds it.
        if let Some(credential_env) = self.credential_env.as_ref() {
            credential_env
                .inject_provider_env(&mut agent_config)
                .await?;
        }
        let max_turns = self.resolve_max_turns(&task).await?;
        let logs_path = self
            .resolve_execution_logs_path(&execution, &task, &workspace, &execution_id)
            .await?;
        if let Some(parent) = std::path::Path::new(&logs_path).parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                ServiceError::invalid_operation(format!("failed to create log directory: {error}"))
            })?;
        }

        let launch_activity_at = now_rfc3339();
        ExecutionRepo::update(
            &*self.db,
            db::UpdateExecution {
                id: execution_id.clone(),
                status: None,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: Some(Some(launch_activity_at)),
                summary: None,
                logs_path: Some(Some(logs_path.clone())),
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(ServiceError::from)?;

        if let Some(terminal_activity) = self.terminal_activity.as_ref() {
            if terminal_activity
                .workspace_has_active_terminal(workspace_id)
                .await
            {
                return Err(ServiceError::TerminalActiveExecution {
                    workspace_id: workspace_id.to_owned(),
                });
            }
        }
        let _exec_lock_guard = if let Some(locks) = self.workspace_exec_locks.as_ref() {
            if let Some(guard) = locks.try_acquire(workspace_id) {
                if let Some(terminal_activity) = self.terminal_activity.as_ref() {
                    if terminal_activity
                        .workspace_has_active_terminal(workspace_id)
                        .await
                    {
                        return Err(ServiceError::TerminalActiveExecution {
                            workspace_id: workspace_id.to_owned(),
                        });
                    }
                }
                Some(guard)
            } else {
                if let Some(terminal_activity) = self.terminal_activity.as_ref() {
                    if terminal_activity
                        .workspace_has_active_terminal(workspace_id)
                        .await
                    {
                        return Err(ServiceError::TerminalActiveExecution {
                            workspace_id: workspace_id.to_owned(),
                        });
                    }
                }
                self.event_bus.publish(events::ForgeEvent {
                    event_type: "workspace.execution_waiting".to_owned(),
                    entity_id: workspace_id.to_owned(),
                    timestamp: events::event_timestamp(),
                    context: events::EventContext::WorkspaceExecutionWaiting {
                        workspace_id: workspace_id.to_owned(),
                        task_id: task.id.clone(),
                    },
                });
                Some(locks.acquire(workspace_id).await)
            }
        } else {
            None
        };

        let execution_before_launch = ExecutionRepo::get_by_id(&*self.db, &execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", execution_id.clone()))?;
        if execution_before_launch.status != ExecutionStatus::Running {
            tracing::info!(
                %execution_id,
                status = %execution_before_launch.status,
                "execution dispatch stopped before adapter launch"
            );
            return Ok(execution_before_launch);
        }

        // Workspace lock acquisition and pre-launch preparation can outlive
        // a lease or a baseline supersession.  Re-read the execution/task
        // bindings and acknowledge the lease immediately before handing
        // control to an executor.
        if let Err(error) = self
            .verify_execution_workspace_authority(&execution_before_launch)
            .await
        {
            let failure_message = error.to_string();
            if let Err(mark_error) = self
                .fail_execution_before_dispatch(&execution_before_launch.id, failure_message)
                .await
            {
                tracing::warn!(
                    execution_id = %execution_before_launch.id,
                    %mark_error,
                    "failed to terminalize execution after final WorkspaceLease verification failure"
                );
            }
            return Err(error);
        }

        let description = execution_description(&execution, &task, &agent_config);

        let (log_tx, mut log_rx) = tokio::sync::mpsc::unbounded_channel::<executors::LogEntry>();
        let max_turns_exceeded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let assistant_turn_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let usage_provider = super::usage_provider_from_agent_config(&agent_config);
        let usage_model_fallback = usage_model_fallback(&agent_config);

        // Spawn a task that forwards log entries to the event bus
        let event_bus = self.event_bus.clone();
        let activity_db = Arc::clone(&self.db);
        let cancellation_executor = self.task_executor.clone();
        let sse_execution_id = execution_id.clone();
        let sse_task_id = task.id.clone();
        let log_max_turns = max_turns;
        let log_max_turns_exceeded = Arc::clone(&max_turns_exceeded);
        let log_assistant_turn_count = Arc::clone(&assistant_turn_count);
        tokio::spawn(async move {
            let mut last_db_update: Option<std::time::Instant> = None;
            let mut assistant_turn_count = 0_u32;
            let mut pending_batch: Vec<executors::LogEntry> = Vec::new();
            let mut flush_deadline: Option<tokio::time::Instant> = None;

            let flush_batch = |batch: &mut Vec<executors::LogEntry>| {
                if batch.is_empty() {
                    return;
                }
                let logs = batch
                    .iter()
                    .map(|entry| serde_json::to_value(entry).unwrap_or_default())
                    .collect::<Vec<_>>();
                let first_log = logs.first().cloned().unwrap_or_default();
                let timestamp = batch
                    .last()
                    .map(|entry| entry.timestamp.clone())
                    .unwrap_or_else(events::event_timestamp);
                event_bus.publish(events::ForgeEvent {
                    event_type: "execution.log".to_owned(),
                    entity_id: sse_execution_id.clone(),
                    timestamp,
                    context: events::EventContext::ExecutionLog {
                        task_id: sse_task_id.clone(),
                        log: first_log,
                        logs: Some(logs),
                    },
                });
                batch.clear();
            };

            loop {
                let next_entry = if let Some(deadline) = flush_deadline {
                    tokio::select! {
                        biased;
                        maybe_entry = log_rx.recv() => maybe_entry,
                        _ = tokio::time::sleep_until(deadline) => {
                            flush_batch(&mut pending_batch);
                            flush_deadline = None;
                            continue;
                        }
                    }
                } else {
                    log_rx.recv().await
                };

                let Some(entry) = next_entry else {
                    flush_batch(&mut pending_batch);
                    break;
                };

                if last_db_update
                    .map(|instant| instant.elapsed() >= Duration::from_secs(30))
                    .unwrap_or(true)
                {
                    if let Err(error) = ExecutionRepo::update_last_activity_at(
                        &*activity_db,
                        &sse_execution_id,
                        &entry.timestamp,
                    )
                    .await
                    {
                        tracing::warn!(
                            execution_id = %sse_execution_id,
                            %error,
                            "failed to update execution activity timestamp"
                        );
                    }
                    last_db_update = Some(std::time::Instant::now());
                }
                if entry.kind == executors::LogKind::Assistant {
                    assistant_turn_count = assistant_turn_count.saturating_add(1);
                    log_assistant_turn_count
                        .store(assistant_turn_count, std::sync::atomic::Ordering::SeqCst);
                    if let Some(limit) = log_max_turns {
                        if assistant_turn_count >= limit
                            && !log_max_turns_exceeded
                                .swap(true, std::sync::atomic::Ordering::SeqCst)
                        {
                            tracing::warn!(
                                execution_id = %sse_execution_id,
                                assistant_turn_count,
                                max_turns = limit,
                                "execution exceeded max turns"
                            );
                            if let Some(executor) = cancellation_executor.as_ref() {
                                if let Err(error) = executor.cancel(&sse_execution_id).await {
                                    tracing::warn!(
                                        execution_id = %sse_execution_id,
                                        %error,
                                        "failed to cancel execution after max turns"
                                    );
                                }
                            }
                        }
                    }
                }

                pending_batch.push(entry);
                if flush_deadline.is_none() {
                    flush_deadline =
                        Some(tokio::time::Instant::now() + EXECUTION_LOG_BATCH_MAX_WAIT);
                }
                if pending_batch.len() >= EXECUTION_LOG_BATCH_MAX_ENTRIES {
                    flush_batch(&mut pending_batch);
                    flush_deadline = None;
                }
            }
        });

        let read_only_head = if executors::is_worktree_read_only(&agent_config) {
            Some(git::get_current_sha(std::path::Path::new(&workspace.worktree_path)).await?)
        } else {
            None
        };
        let execution_result = executor
            .execute(ExecutionContext {
                task_id: task.id.clone(),
                execution_id: execution_id.clone(),
                worktree_path: workspace.worktree_path.clone(),
                description,
                agent_config,
                logs_path: logs_path.clone(),
                heartbeat_interval_seconds: 30,
                max_turns,
                log_sender: Some(log_tx),
            })
            .await;
        let mut discarded_read_only_changes = false;
        let restore_result = if let Some(head) = read_only_head.as_deref() {
            let worktree_path = std::path::Path::new(&workspace.worktree_path);
            discarded_read_only_changes =
                !git::is_worktree_clean(worktree_path).await.unwrap_or(true)
                    || git::get_current_sha(worktree_path).await.ok().as_deref() != Some(head);
            git::restore_worktree(worktree_path, head)
                .await
                .map_err(ServiceError::from)
        } else {
            Ok(())
        };
        let mut result = execution_result?;
        restore_result?;
        if let Some(head) = read_only_head {
            result.after_sha = Some(head);
        }
        // A read-only execution that wrote anyway means the work was authored in
        // the wrong task shape (e.g. an implementation task created as
        // `planning_task`). The changes are already discarded by policy — fail
        // the execution loudly instead of reporting a clean completion, so the
        // lost work is visible and the task does not advance on nothing.
        if discarded_read_only_changes && result.status == ExecutionOutcome::Completed {
            result.status = ExecutionOutcome::Failed;
            result.error = Some(format!(
                "read-only execution produced worktree changes, which were discarded: \
                 task_type '{}' (role '{}') never receives write access. If this task \
                 implements code, recreate it as task_type 'task'.",
                task.task_type, execution.role
            ));
        }
        let max_turns_exceeded = max_turns_exceeded.load(std::sync::atomic::Ordering::SeqCst);
        let assistant_turn_count = assistant_turn_count.load(std::sync::atomic::Ordering::SeqCst);
        if max_turns_exceeded {
            result.status = ExecutionOutcome::Failed;
            result.error = Some(match max_turns {
                Some(limit) => format!("max turns exceeded ({assistant_turn_count}/{limit})"),
                None => "max turns exceeded".to_owned(),
            });
        }

        let current_execution = ExecutionRepo::get_by_id(&*self.db, &execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", execution_id.clone()))?;
        if current_execution.status != ExecutionStatus::Running {
            tracing::info!(
                %execution_id,
                status = %current_execution.status,
                "execution dispatch already stopped externally"
            );
            return Ok(current_execution);
        }

        let executor_unavailable =
            result.failure_class == Some(executors::ExecutionFailureClass::ExecutorUnavailable);
        let unavailable_retry_at = result.retry_after.map(|retry_after| {
            let delay = chrono::Duration::from_std(retry_after)
                .unwrap_or_else(|_| chrono::Duration::minutes(15));
            (chrono::Utc::now() + delay).to_rfc3339()
        });
        let route_outcome = crate::task_service::config::RouteOutcome {
            selected: result.resolved_candidate.as_ref().map(|candidate| {
                (
                    candidate.candidate_key.clone(),
                    candidate.executor_type.to_string(),
                    candidate.config.clone(),
                )
            }),
            attempts: result
                .route_attempts
                .iter()
                .map(|attempt| {
                    (
                        attempt.candidate_key.clone(),
                        attempt.outcome.as_str().to_owned(),
                    )
                })
                .collect(),
            unavailable_retry_at: executor_unavailable.then(|| unavailable_retry_at.clone()),
        };
        let snapshot_update = match current_execution.executor_config_snapshot_json.as_deref() {
            Some(snapshot) => crate::task_service::config::apply_route_outcome_to_snapshot(
                snapshot,
                &route_outcome,
            )?,
            None => None,
        };

        let now = now_rfc3339();
        let (status, stop_reason, stopped_by, resume_policy, stopped_at) = match result.status {
            ExecutionOutcome::Completed => (ExecutionStatus::Completed, None, None, None, None),
            ExecutionOutcome::Failed => (
                ExecutionStatus::Failed,
                Some(Some(db::StopReason::ExecutorFailed)),
                Some(Some(
                    api_types::Actor::system(api_types::SystemComponent::Executor).display(),
                )),
                Some(Some(db::ResumePolicy::Manual)),
                Some(Some(now.clone())),
            ),
            ExecutionOutcome::Cancelled => (
                ExecutionStatus::Cancelled,
                Some(Some(db::StopReason::ExecutorCancelled)),
                Some(Some(
                    api_types::Actor::system(api_types::SystemComponent::Executor).display(),
                )),
                Some(Some(db::ResumePolicy::Manual)),
                Some(Some(now.clone())),
            ),
        };
        tracing::info!(
            %execution_id,
            task_id = %task.id,
            status = %status,
            logs_path = %logs_path,
            "execution dispatch completed"
        );

        let updated = ExecutionRepo::update(
            &*self.db,
            db::UpdateExecution {
                id: execution_id,
                status: Some(status),
                stop_reason,
                stopped_by,
                resume_policy,
                stopped_at,
                agent_session_id: Some(result.agent_session_id),
                agent_message_id: None,
                last_activity_at: Some(Some(now.clone())),
                summary: Some(result.summary),
                logs_path: Some(Some(logs_path)),
                before_sha: None,
                after_sha: Some(result.after_sha),
                error: Some(result.error),
                executor_config_snapshot_json: snapshot_update.map(Some),
                updated_at: now_rfc3339(),
            },
        )
        .await?;

        self.revoke_active_workspace_lease_for_execution(&task.id, &updated.id)
            .await;

        if let Some(token_usage) = result.usage {
            let model = token_usage
                .model
                .or_else(|| usage_model_fallback.clone())
                .unwrap_or_else(|| "default".to_owned());
            if let Err(error) = ExecutionUsageRepo::upsert(
                &*self.db,
                db::UpsertExecutionUsage {
                    execution_id: updated.id.clone(),
                    provider: usage_provider,
                    model,
                    input_tokens: token_usage.input_tokens,
                    output_tokens: token_usage.output_tokens,
                    cache_read_tokens: token_usage.cache_read_tokens,
                    cache_write_tokens: token_usage.cache_write_tokens,
                    cost_usd: token_usage.cost_usd,
                },
            )
            .await
            {
                tracing::warn!(
                    execution_id = %updated.id,
                    %error,
                    "failed to record execution token usage"
                );
            }
        }

        super::publish_terminal_execution_event(self, &updated);

        if let Err(error) = self
            .memory_service
            .record_execution_summary_if_present(&task.project_id, &updated)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }

        if updated.status == ExecutionStatus::Completed {
            if let Err(error) = super::clear_execution_retry_metadata(&self.db, &task).await {
                tracing::warn!(
                    task_id = %task.id,
                    execution_id = %updated.id,
                    %error,
                    "failed to clear execution retry metadata"
                );
            }
            if updated.role == crate::workflow::default_roles::PLANNER
                && task.status == crate::workflow::default_states::PLANNING
            {
                if let Err(error) = super::set_planning_awaiting_review_metadata(
                    &self.db,
                    &task,
                    Some(&updated.id),
                    true,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %task.id,
                        execution_id = %updated.id,
                        %error,
                        "failed to mark planning awaiting review"
                    );
                }
            }
        } else if updated.status == ExecutionStatus::Failed && max_turns_exceeded {
            if let Err(error) = self
                .annotate_max_turns_exceeded_block(&updated, max_turns)
                .await
            {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to block task after max turns exceeded"
                );
            }
        } else if updated.status == ExecutionStatus::Failed
            && executor_unavailable
            && should_block_task_for_failed_execution(&updated)
        {
            let attempts = serde_json::Value::Array(
                route_outcome
                    .attempts
                    .iter()
                    .map(|(candidate_key, outcome)| {
                        serde_json::json!({"candidate_key": candidate_key, "outcome": outcome})
                    })
                    .collect(),
            );
            if let Err(error) = self
                .annotate_executor_unavailable_block(&updated, unavailable_retry_at, attempts)
                .await
            {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to handle executor-unavailable execution"
                );
            }
        } else if updated.status == ExecutionStatus::Failed
            && should_block_task_for_failed_execution(&updated)
        {
            if let Err(error) = self.annotate_executor_failure_block(&updated).await {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to block task after executor failure"
                );
            }
        }

        Ok(updated)
    }
}

impl TaskService {
    pub(in crate::task_service) async fn cancel_execution_with_provider(
        &self,
        execution: &Execution,
        reason: &str,
    ) -> Result<()> {
        let agent = match execution.agent_id.as_deref() {
            Some(agent_id) => Some(
                AgentRepo::get_by_id(&*self.db, agent_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("agent", agent_id.to_owned()))?,
            ),
            None => None,
        };
        let provider = self
            .execution_provider_for_agent(agent.as_ref(), &execution.id)
            .await?;
        provider
            .cancel(api_types::ExecutionCancelParams {
                execution_id: execution.id.clone(),
                reason: Some(reason.to_owned()),
            })
            .await?;
        Ok(())
    }

    async fn execution_provider_for_agent(
        &self,
        agent: Option<&Agent>,
        execution_id: &str,
    ) -> Result<Arc<dyn crate::daemon_transport::ExecutionProvider>> {
        let daemon_id = agent.and_then(|agent| agent.daemon_id.as_deref());
        if let Some(registry) = self.daemon_connections.as_ref() {
            return crate::daemon_transport::select_execution_provider(
                daemon_id, &self.db, registry,
            )
            .await
            .inspect_err(|error| {
                if let ServiceError::DaemonUnavailable { daemon_id } = error {
                    tracing::warn!(
                        execution_id = %execution_id,
                        daemon_id = %daemon_id,
                        agent_id = ?agent.map(|agent| agent.id.as_str()),
                        "remote daemon unavailable for execution dispatch"
                    );
                }
            });
        }

        let task_executor = self.task_executor.clone().ok_or_else(|| {
            ServiceError::invalid_operation(
                "task executor is not configured for execution dispatch",
            )
        })?;
        Ok(Arc::new(
            crate::daemon_transport::EmbeddedExecutionProvider::new(
                Arc::new(self.clone()),
                task_executor,
            ),
        ))
    }

    async fn execution_start_params(
        &self,
        execution: &Execution,
    ) -> Result<api_types::ExecutionStartParams> {
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let workspace_id = execution
            .workspace_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("execution missing workspace_id"))?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
        let snapshot = execution
            .executor_config_snapshot_json
            .as_deref()
            .ok_or_else(|| {
                ServiceError::invalid_operation("execution missing executor config snapshot")
            })?;
        let mut executor_config = parse_json_value("executor config snapshot", snapshot)?;
        if execution.role == crate::workflow::default_roles::REVIEWER
            || matches!(task.task_type.as_str(), "planning_task" | "discovery")
        {
            executors::mark_worktree_read_only(&mut executor_config);
        }
        let executor_type = executor_config
            .get("executor_type")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ServiceError::invalid_operation("executor config snapshot missing executor_type")
            })?
            .to_owned();
        let description = execution_description(execution, &task, &executor_config);
        let max_turns = self.resolve_max_turns(&task).await?;

        Ok(api_types::ExecutionStartParams {
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            workspace_path: workspace.worktree_path,
            executor_type,
            executor_config,
            prompt: json!({ "description": description }),
            max_turns,
        })
    }
}

fn execution_description(execution: &Execution, task: &Task, agent_config: &Value) -> String {
    let is_shell_executor =
        agent_config.get("executor_type").and_then(Value::as_str) == Some("shell");
    if is_shell_executor && execution.role == crate::workflow::default_roles::REVIEWER {
        r#"echo "===REVIEW: PASS===""#.to_owned()
    } else {
        execution
            .summary
            .clone()
            .or_else(|| task.description.clone())
            .unwrap_or_else(|| task.title.clone())
    }
}

fn usage_model_fallback(agent_config: &Value) -> Option<String> {
    agent_config
        .get("config")
        .and_then(|config| config.get("model"))
        .and_then(Value::as_str)
        .or_else(|| agent_config.get("model").and_then(Value::as_str))
        .filter(|model| !model.trim().is_empty())
        .map(str::to_owned)
}

fn max_turns_from_value(value: &Value) -> Option<u32> {
    value
        .get("max_turns")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
}

impl TaskService {
    async fn annotate_max_turns_exceeded_block(
        &self,
        execution: &Execution,
        max_turns: Option<u32>,
    ) -> Result<()> {
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let message = match max_turns {
            Some(limit) => format!("Execution stopped after reaching max_turns={limit}"),
            None => "Execution stopped after reaching max_turns".to_owned(),
        };
        let annotation = api_types::TaskBlockingAnnotation {
            annotation_type: api_types::FailureKind::MaxTurnsExceeded,
            blocking_reason: "max_turns_exceeded".to_owned(),
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
            message: Some(message.clone()),
            hook: None,
            recovery_actions: vec![
                api_types::RecoveryAction::ResetToInitial,
                api_types::RecoveryAction::CancelTask,
            ],
        };
        let blocked_meta = json!({
            "reason": message,
            "created_at": now_rfc3339(),
            "kind": "max_turns_exceeded",
            "execution_id": execution.id,
        });
        let updated = TaskRepo::update_status(
            &*self.db,
            db::UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: task.status,
                assignee_id: None,
                error_annotation: Some(Some(serde_json::to_string(&annotation).map_err(
                    |error| {
                        ServiceError::invalid_operation(format!(
                            "failed to serialize max-turns annotation: {error}"
                        ))
                    },
                )?)),
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
            entity_id: updated.id,
            timestamp: event_timestamp(),
            context: EventContext::TaskBlocked {
                project_id: updated.project_id,
                reason: "max_turns_exceeded".to_owned(),
                kind: Some(api_types::FailureKind::MaxTurnsExceeded),
                source: None,
                execution_id: Some(execution.id.clone()),
            },
        });
        Ok(())
    }

    async fn resolve_max_turns(&self, task: &Task) -> Result<Option<u32>> {
        if let Some(value) = task
            .task_state_config
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .and_then(|value| {
                max_turns_from_value(&value)
                    .or_else(|| value.get(&task.status).and_then(max_turns_from_value))
            })
        {
            return Ok(Some(value));
        }

        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Executor),
        );
        if let Some(value) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(|state| max_turns_from_value(&state.config))
        {
            return Ok(Some(value));
        }

        Ok(serde_json::from_str::<Value>(&project.settings)
            .ok()
            .and_then(|value| max_turns_from_value(&value)))
    }

    async fn resolve_execution_logs_path(
        &self,
        execution: &Execution,
        task: &Task,
        workspace: &Workspace,
        execution_id: &str,
    ) -> Result<String> {
        let durable_path = execution_logs_path(
            &self.workspace_root,
            &task.project_id,
            &workspace.task_id,
            execution_id,
        );
        let Some(stored_path) = execution.logs_path.as_deref() else {
            return Ok(durable_path);
        };
        if stored_path == durable_path {
            return Ok(durable_path);
        }

        let stored = std::path::Path::new(stored_path);
        if !stored.exists() {
            return Ok(durable_path);
        }

        let durable = std::path::Path::new(&durable_path);
        if let Some(parent) = durable.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                ServiceError::invalid_operation(format!("failed to create log directory: {error}"))
            })?;
        }
        if !durable.exists() {
            std::fs::rename(stored, durable)
                .or_else(|_| {
                    std::fs::copy(stored, durable)?;
                    std::fs::remove_file(stored)
                })
                .map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "failed to move execution log: {error}"
                    ))
                })?;
        }

        Ok(durable_path)
    }
}

#[cfg(test)]
mod usage_tests {
    use super::*;

    #[test]
    fn usage_provider_and_model_come_from_execution_snapshot() {
        let snapshot = json!({
            "executor_type": "codex",
            "model": "agent-model",
            "config": {
                "model": "gpt-5.5"
            }
        });

        assert_eq!(super::usage_provider_from_agent_config(&snapshot), "openai");
        assert_eq!(usage_model_fallback(&snapshot).as_deref(), Some("gpt-5.5"));
    }

    #[test]
    fn usage_model_falls_back_to_top_level_model() {
        let snapshot = json!({
            "executor_type": "claude_code",
            "model": "claude-haiku-4-5",
            "config": {}
        });

        assert_eq!(
            super::usage_provider_from_agent_config(&snapshot),
            "anthropic"
        );
        assert_eq!(
            usage_model_fallback(&snapshot).as_deref(),
            Some("claude-haiku-4-5")
        );
    }

    #[test]
    fn cursor_usage_provider_maps_to_cursor() {
        let snapshot = json!({
            "executor_type": "cursor",
            "config": {}
        });

        assert_eq!(super::usage_provider_from_agent_config(&snapshot), "cursor");
    }
}
