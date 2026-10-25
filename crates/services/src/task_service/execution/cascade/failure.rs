//! Execution failure blockers and retry scheduling.

use super::*;

impl TaskService {
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
    /// run (`FailureKind::ExecutorUnavailable`). Transient capacity failures
    /// use the bounded execution retry budget; permanent unavailability
    /// blocks for manual reconfiguration.
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

        if task_blocked_by_execution(&task, &execution.id) {
            return Ok(());
        }
        // The current-run fence (see `annotate_executor_failure_block_with_retry`).
        if !super::execution_belongs_to_current_state_entry(&self.db, &task, execution).await? {
            return Ok(());
        }
        let usage_limited = attempts.as_array().is_some_and(|attempts| {
            attempts.iter().any(|attempt| {
                attempt.get("outcome").and_then(Value::as_str) == Some("usage_exhausted")
            })
        });
        let transient = retry_at.is_some() || usage_limited;
        let retry_block_reason = if transient {
            let current_state = workflow
                .states
                .iter()
                .find(|state| state.name == task.status);
            match self
                .maybe_schedule_execution_retry(
                    execution,
                    &task,
                    project.version,
                    current_state.map(|state| &state.config),
                    current_state.and_then(|state| state.gate_config.as_ref()),
                    Some(CapacityRetry {
                        retry_at: retry_at.as_deref(),
                        reason: if usage_limited {
                            "usage limit"
                        } else {
                            "provider capacity unavailable"
                        },
                    }),
                )
                .await?
            {
                ExecutionRetryDisposition::Scheduled => return Ok(()),
                ExecutionRetryDisposition::NotScheduled(reason) => Some(reason),
            }
        } else {
            None
        };

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
            message: Some(
                if let Some(reason) = retry_block_reason.filter(|_| execution.role != "interactive")
                {
                    format!(
                        "Provider capacity {reason}: {}",
                        execution
                            .error
                            .as_deref()
                            .unwrap_or("no executor candidate available")
                    )
                } else {
                    execution.error.clone().unwrap_or_else(|| {
                        "No executor candidate is available (check CLI installs and authentication)"
                            .to_owned()
                    })
                },
            ),
            hook: None,
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

        let Some(updated) = TaskRepo::update_status_for_latest_execution(
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
            super::latest_execution_authority(execution, project.version),
        )
        .await?
        else {
            return Ok(());
        };

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

    /// A failed run is retried or blocks its Task under the Project as it is
    /// now, whichever Project revision dispatched the run. The revision
    /// fence belongs to completions, which advance the workflow; a failure
    /// advances nothing, its retry is a new run admitted under current
    /// authority, and dropping it here left the Task active with no run, no
    /// retry and no park after any Project edit, pause or resume.
    ///
    /// What fences a failure instead is that the run is still the Task's
    /// current one: the Task is in the state, and the entry of that state,
    /// that the run was dispatched for; the state still belongs to the run's
    /// role; and, inside the write itself, the run is the newest of its role
    /// and its Agent still holds the role
    /// (`latest_execution_authority_matches_in_tx`). A late failure of a run
    /// the Task has moved on from, or that a newer run replaced, changes
    /// nothing and spends no budget.
    async fn annotate_executor_failure_block_with_retry(
        &self,
        execution: &Execution,
        allow_retry: bool,
    ) -> Result<()> {
        // Lease contention never spends the execution retry budget; only the
        // dispatch path parks it, once its re-dispatch bound is spent.
        if allow_retry && super::guards::is_workspace_lease_pre_dispatch_failure(execution) {
            return Ok(());
        }
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        if execution.stopped_by.as_deref() == Some("system:dispatch")
            && task
                .condition
                .reasons()
                .any(|reason| matches!(reason, db::ParkReason::Held { .. }))
        {
            return Ok(());
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
            return Ok(());
        }
        let Some(current_state) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
        else {
            return Ok(());
        };
        let current_role = crate::workflow::effective_role(current_state);
        let execution_still_owns_current_role = execution.role == "interactive"
            || current_role == Some(execution.role.as_str())
            || (current_role == Some(crate::workflow::default_roles::CODER)
                && execution.role == "executor");
        if !execution_still_owns_current_role {
            return Ok(());
        }
        if !super::execution_belongs_to_current_state_entry(&self.db, &task, execution).await? {
            return Ok(());
        }
        if task_blocked_by_execution(&task, &execution.id) {
            return Ok(());
        }
        if allow_retry
            && self
                .maybe_schedule_execution_retry(
                    execution,
                    &task,
                    project.version,
                    Some(&current_state.config),
                    current_state.gate_config.as_ref(),
                    None,
                )
                .await?
                .is_scheduled()
        {
            return Ok(());
        }

        self.block_task_after_executor_failure(&task, execution)
            .await
    }

    pub(in crate::task_service) async fn block_task_after_executor_failure(
        &self,
        task: &Task,
        execution: &Execution,
    ) -> Result<()> {
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &task.project_id).await? else {
            return Ok(());
        };
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

        let Some(updated) = TaskRepo::update_status_for_latest_execution(
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
            super::latest_execution_authority(execution, project.version),
        )
        .await?
        else {
            return Ok(());
        };

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

    /// A resume action is useful only when the stopped execution can pass the
    /// same identity/role checks as the WorkspaceLease it will request.  Keep
    /// impossible actions out of the durable annotation instead of inviting a
    /// coordinator to call a recovery route that is guaranteed to fail.
    pub(super) async fn maybe_schedule_execution_retry(
        &self,
        execution: &Execution,
        task: &Task,
        project_version: i64,
        state_config: Option<&Value>,
        gate_config: Option<&api_types::GateConfig>,
        capacity_retry: Option<CapacityRetry<'_>>,
    ) -> Result<ExecutionRetryDisposition> {
        if execution.role == "interactive" {
            // Interactive runs are user-prompted and do not have a durable dispatcher target yet.
            return Ok(ExecutionRetryDisposition::NotScheduled(
                "automatic retry is unavailable for interactive executions",
            ));
        }

        let budget =
            db::budget::limit(task, db::budget::Kind::Execution, state_config, gate_config)?;
        if !db::budget::allows_retry(i64::from(budget), 0) {
            return Ok(ExecutionRetryDisposition::NotScheduled(
                "automatic retries are disabled by the execution retry budget",
            ));
        }

        let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "invalid task metadata for {}: {error}",
                task.id
            ))
        })?;
        let retry_count =
            db::budget::spent(self.db.pool(), &task.id, db::budget::Kind::Execution.key()).await?
                as u64;
        let already_recorded = metadata
            .extra
            .get("last_execution_failure_execution_id")
            .and_then(Value::as_str)
            == Some(execution.id.as_str());
        if already_recorded {
            if !self
                .latest_execution_authority_is_current(task, execution, project_version)
                .await?
            {
                return Ok(ExecutionRetryDisposition::NotScheduled(
                    "automatic retry was skipped because execution authority changed",
                ));
            }
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
            return Ok(ExecutionRetryDisposition::Scheduled);
        }
        if !db::budget::allows_retry(i64::from(budget), retry_count as i64) {
            return Ok(ExecutionRetryDisposition::NotScheduled(
                "retry budget exhausted",
            ));
        }

        let now = now_rfc3339();
        let attempt = retry_count + 1;
        let delay_seconds = (10_u64
            .saturating_mul(2_u64.saturating_pow(attempt.saturating_sub(1) as u32)))
        .min(300);
        let next_dispatch_at = if let Some(capacity) = &capacity_retry {
            executor_unavailable_dispatch_time(capacity.retry_at, &task.id, delay_seconds)
        } else {
            chrono::Utc::now() + chrono::Duration::seconds(delay_seconds as i64)
        };
        let reason = if let Some(capacity) = &capacity_retry {
            format!("{}; capacity retry (attempt {attempt})", capacity.reason)
        } else {
            format!("execution retry (attempt {attempt})")
        };
        let delay_seconds = if capacity_retry.is_some() {
            ((next_dispatch_at - chrono::Utc::now())
                .num_milliseconds()
                .max(0) as u64)
                .div_ceil(1000)
        } else {
            delay_seconds
        };
        if !crate::deferred_dispatch::set_with_mutations_for_latest_execution(
            &self.db,
            task,
            super::latest_execution_authority(execution, project_version),
            &task.status,
            &next_dispatch_at.to_rfc3339(),
            &reason,
            vec![
                db::TaskMetadataMutation::Budget(db::budget::Mutation::Charge {
                    key: db::budget::Kind::Execution.key().into(),
                    limit: i64::from(budget),
                    step: format!("failure:{}", execution.id),
                }),
                db::TaskMetadataMutation::Set {
                    key: "last_execution_failure_at".to_owned(),
                    value: Value::String(now.clone()),
                },
                db::TaskMetadataMutation::Set {
                    key: "last_execution_failure_execution_id".to_owned(),
                    value: Value::String(execution.id.clone()),
                },
            ],
        )
        .await?
        {
            return Ok(ExecutionRetryDisposition::NotScheduled(
                "automatic retry was skipped because execution authority changed",
            ));
        }
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
        Ok(ExecutionRetryDisposition::Scheduled)
    }

    async fn latest_execution_authority_is_current(
        &self,
        task: &Task,
        execution: &Execution,
        project_version: i64,
    ) -> Result<bool> {
        Ok(
            TaskRepo::mutate_metadata_and_bump_version_for_latest_execution(
                &*self.db,
                &task.id,
                task.version,
                super::latest_execution_authority(execution, project_version),
                Vec::new(),
                &now_rfc3339(),
            )
            .await?
            .is_some(),
        )
    }
}
