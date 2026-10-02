use super::*;
use ::review::ReviewWorkspace;
use api_types::{Actor, UserActionSource};

impl TaskService {
    pub(crate) async fn annotate_review_ci_interruption(
        &self,
        task: &Task,
        ctx: &crate::workflow::HookContext,
        reason: &str,
        retry: bool,
        reset: bool,
    ) -> Result<Task> {
        let now = now_rfc3339();
        let mut barrier: Value = task
            .entry_barrier_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_else(|| json!({}));
        let disconnected = db::WorkspacePlacementRepo::get_for_task(&*self.db, &task.id)
            .await?
            .is_some_and(|placement| placement.state == db::PlacementState::Disconnected);
        let attempts = barrier["infrastructure_attempts"].as_u64().unwrap_or(0)
            + u64::from(retry && !disconnected);
        let exhausted = retry && !disconnected && attempts >= 5;
        let retry = retry && !exhausted;
        let kind = if reset {
            "workspace_reset_required"
        } else if retry {
            "review_ci_infrastructure"
        } else if exhausted {
            "review_ci_infrastructure_exhausted"
        } else {
            "review_ci_unavailable"
        };
        barrier["state"] = json!(task.status);
        barrier["status"] = json!("blocked");
        barrier["updated_at"] = json!(now);
        barrier["interrupted_at"] = json!(now);
        barrier["infrastructure_attempts"] = json!(attempts);
        barrier["blocking_reason"] = json!(reason);
        let actions = if reset {
            json!(["reset_to_initial", "cancel_task"])
        } else {
            json!(["retry_hook", "cancel_task"])
        };
        let annotation = json!({"type": if reset { api_types::FailureKind::WorkspaceResetRequired } else { api_types::FailureKind::BeforeWorkHookFailed },
            "blocking_reason": kind, "blocked_at": now, "blocked_by": "system:workflow", "message": reason,
            "recovery_actions": actions});
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        if let Some(version) = ctx.project_version {
            let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project WHERE id = ? AND version = ? AND workflow_definition IS ?)")
                .bind(&task.project_id).bind(version).bind(ctx.project_workflow_definition.as_deref())
                .fetch_one(&mut *tx).await?;
            if !valid {
                return Err(DbError::VersionConflict.into());
            }
        }
        let deferral = retry.then(|| json!({
            "target_state": task.status, "reason": reason,
            "not_before": (Utc::now() + chrono::Duration::seconds(5 * (1_i64 << attempts.saturating_sub(1).min(4)))).to_rfc3339(),
        }).to_string());
        let changed = sqlx::query("UPDATE task SET entry_barrier_json = ?, error_annotation = ?,
            blocked_json = ?, metadata_json = CASE WHEN ? IS NULL THEN
            json_remove(COALESCE(metadata_json, '{}'), '$.deferred_dispatch') ELSE
            json_set(COALESCE(metadata_json, '{}'), '$.deferred_dispatch', json(?)) END,
            updated_at = ?, version = version + 1
            WHERE id = ? AND version = ? AND status = ? AND deleted_at IS NULL")
            .bind(barrier.to_string()).bind(annotation.to_string())
            .bind((!retry).then(|| json!({"kind": if reset { api_types::FailureKind::WorkspaceResetRequired } else { api_types::FailureKind::BeforeWorkHookFailed }, "reason": reason, "created_at": now, "execution_id": null}).to_string()))
            .bind(&deferral).bind(&deferral).bind(&now).bind(&task.id).bind(task.version).bind(&task.status)
            .execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(DbError::VersionConflict.into());
        }
        if !retry {
            crate::placement::admission::record_wait_attention_in_tx(
                &self.db,
                &mut tx,
                task,
                "execution_failed",
                &format!("Review CI [{kind}] could not run: {reason}"),
                &format!("review-ci:{}", task.id),
            )
            .await?;
        }
        tx.commit().await?;
        TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))
    }

    pub(crate) async fn review_workspace_io(
        &self,
        workspace: &Workspace,
    ) -> Result<Arc<dyn ReviewWorkspace>> {
        let workspace = crate::workspace_backend::EmbeddedWorkspaceBackend::resolve_workspace(
            &self.workspace_backend_router,
            &self.db,
            workspace,
            &self.workspace_root,
        )
        .await?;
        Ok(Arc::new(workspace))
    }

    pub async fn rerun_review(&self, task_id: Uuid) -> Result<(Task, Review)> {
        let task_id = task_id.to_string();
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if task.status != "review" {
            return Err(ServiceError::invalid_operation(format!(
                "task {task_id} is in {} state; expected review",
                task.status
            )));
        }
        let (review, outcome) = self
            .run_review_for_task(&task)
            .await?
            .ok_or_else(|| ServiceError::invalid_operation("review runner is not configured"))?;
        let task = self
            .settle_rerun_review_outcome(&task, &review, outcome)
            .await?;
        Ok((task, review))
    }

    pub async fn approve_review(&self, task_id: impl Into<String>) -> Result<(Task, Review)> {
        self.approve_review_as(task_id, Actor::user(UserActionSource::Api))
            .await
    }

    pub async fn approve_review_as(
        &self,
        task_id: impl Into<String>,
        actor: Actor,
    ) -> Result<(Task, Review)> {
        let task_id = task_id.into();
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if task.status != "review" {
            return Err(ServiceError::invalid_operation(format!(
                "task {task_id} is in {} state; expected review",
                task.status
            )));
        }
        let latest_review = self.latest_review_for_task(&task_id).await?;
        if latest_review.status != ReviewStatus::AwaitingHuman {
            return Err(ServiceError::invalid_operation(
                "latest review is not awaiting_human",
            ));
        }
        // Approval must never repair or default malformed persisted review
        // details. Validate the stored row before attempting the transition.
        super::strict_review_details(&latest_review)?;
        let finished_at = now_rfc3339();
        let (review, task) = ReviewRepo::update_status_with_task_authority(
            &*self.db,
            &latest_review.id,
            ReviewStatus::Passed,
            latest_review.step_results_json.clone(),
            Some(finished_at.clone()),
            &finished_at,
            task.version,
            Some(finished_at.clone()),
        )
        .await?;

        if let Err(error) = self
            .memory_service
            .record_review_result_if_final(&task.project_id, &review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }
        self.create_system_comment(
            &task_id,
            format!("Review passed (attempt {})", review.attempt_number),
        )
        .await?;
        self.publish(ForgeEvent {
            event_type: "review.approved".to_owned(),
            entity_id: review.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReviewApproved {
                task_id: task_id.clone(),
                review_id: review.id.clone(),
            },
        });
        let transitioned = self
            .transition(
                task_id,
                "merging".to_owned(),
                TransitionOptions {
                    version: task.version,
                    reason: None,
                    triggered_by: actor,
                    rejection: false,
                    defer_dispatch_seconds: None,
                },
            )
            .await?;
        Ok((transitioned.task, review))
    }

    pub async fn reject_review(
        &self,
        task_id: impl Into<String>,
        reason: Option<String>,
    ) -> Result<(Task, Review)> {
        self.reject_review_as(task_id, reason, Actor::user(UserActionSource::Api))
            .await
    }

    pub async fn reject_review_as(
        &self,
        task_id: impl Into<String>,
        reason: Option<String>,
        actor: Actor,
    ) -> Result<(Task, Review)> {
        let task_id = task_id.into();
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if task.status != "review" {
            return Err(ServiceError::invalid_operation(format!(
                "task {task_id} is in {} state; expected review",
                task.status
            )));
        }
        let latest_review = self.latest_review_for_task(&task_id).await?;
        if latest_review.status != ReviewStatus::AwaitingHuman {
            return Err(ServiceError::invalid_operation(
                "latest review is not awaiting_human",
            ));
        }
        // Rejection also preserves the corruption signal; it must not mutate
        // a malformed persisted review into a valid-looking result.
        super::strict_review_details(&latest_review)?;
        let reason = reason
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "manual review rejected".to_owned());
        let finished_at = now_rfc3339();
        let (review, task) = ReviewRepo::update_status_with_task_authority(
            &*self.db,
            &latest_review.id,
            ReviewStatus::Failed,
            latest_review.step_results_json.clone(),
            Some(finished_at.clone()),
            &finished_at,
            task.version,
            None,
        )
        .await?;

        if let Err(error) = self
            .memory_service
            .record_review_result_if_final(&task.project_id, &review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }
        self.create_system_comment(
            &task_id,
            format!(
                "Review failed (attempt {}): {}",
                review.attempt_number, reason
            ),
        )
        .await?;
        self.publish(ForgeEvent {
            event_type: "review.rejected".to_owned(),
            entity_id: review.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReviewRejected {
                task_id: task_id.clone(),
                review_id: review.id.clone(),
                reason: reason.clone(),
            },
        });

        self.transition(
            task_id.clone(),
            "in_progress".to_owned(),
            TransitionOptions {
                version: task.version,
                reason: Some(reason.clone()),
                triggered_by: actor,
                rejection: true,
                defer_dispatch_seconds: None,
            },
        )
        .await?;
        let remaining_retries = self.remaining_retries(&task_id).await?;
        let follow_up_already_dispatched = ExecutionRepo::has_active_or_completed_child(
            &*self.db,
            &task_id,
            &review.execution_id,
            crate::workflow::default_roles::CODER,
        )
        .await?;
        if remaining_retries > 0 && !follow_up_already_dispatched {
            self.dispatch_follow_up(
                &task_id,
                ::review::ReviewOutcome::AuditorFailed { reason },
                review.execution_id.clone(),
            )
            .await?;
        }
        let latest_task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        Ok((latest_task, review))
    }
    async fn run_review_for_task(
        &self,
        task: &Task,
    ) -> Result<Option<(Review, ::review::ReviewOutcome)>> {
        let Some(review_runner) = &self.review_runner else {
            return Ok(None);
        };
        let execution = self.latest_executor_execution(&task.id).await?;
        execution.workspace_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("executor execution missing workspace_id")
        })?;
        let workspace = prepare_workspace(
            &self.db,
            &self.workspace_root,
            task,
            &task.id,
            self.repo_cache_locks.clone(),
            &self.workspace_backend_router,
        )
        .await?;
        let workspace_io = self.review_workspace_io(&workspace).await?;
        let review_config = review_config_from_json(task.task_state_config.as_deref())?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &Actor::system(api_types::SystemComponent::Workflow),
        );
        let effective_review_role = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(crate::workflow::effective_role);
        let reviewer_assignment = if effective_review_role
            .map(|role| role == crate::workflow::default_roles::REVIEWER)
            .unwrap_or(false)
        {
            TaskRoleAssignmentRepo::get_by_task_and_role(
                &*self.db,
                &task.id,
                crate::workflow::default_roles::REVIEWER,
            )
            .await?
        } else {
            None
        };
        let reviewer_agent_id = reviewer_assignment.and_then(|assignment| {
            (assignment.assignee_type == Some(db::AssigneeKind::Agent))
                .then_some(assignment.assignee_id)
                .flatten()
        });
        // The Task-role assignment is the sole reviewer/auditor authority.
        // A configured auditor id without that materialized assignment is a
        // stale legacy configuration, not permission to synthesize a
        // reviewer principal at rerun time.  Assignment materialization is
        // therefore a prerequisite for the configured-auditor path.
        if review_config.auditor_agent_id.is_some() && reviewer_agent_id.is_none() {
            return Err(ServiceError::conflict(
                "configured auditor requires a reviewer role assignment",
            ));
        }
        let auditor_agent_id = reviewer_agent_id;
        let requires_user_approval = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(|state| state.gate_config.as_ref())
            .is_some_and(|gate| gate.requires_user_approval());
        let logs_path = execution_logs_path(
            &self.workspace_root,
            &task.project_id,
            &task.id,
            &format!("review-{}", execution.id),
        );
        let task_id = Uuid::parse_str(&task.id).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid task id for review: {error}"))
        })?;
        let executor_execution_id = Uuid::parse_str(&execution.id).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid execution id for review: {error}"))
        })?;
        let result = review_runner
            .with_workspace_io(workspace_io)
            .run(ReviewRequest {
                task_id,
                executor_execution_id,
                workspace_path: PathBuf::new(),
                ci_steps: review_config.ci_steps,
                logs_path,
                auditor_agent_id,
                review_prompt: review_config.review_prompt,
                executor_thread_id: execution.agent_session_id,
                requires_user_approval,
            })
            .await;
        match result {
            Ok(result) => Ok(Some(result)),
            Err(::review::ReviewError::Conformance {
                execution_id,
                reason,
            }) => {
                let mut failed = ExecutionRepo::get_by_id(&*self.db, &execution_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("execution", execution_id.clone()))?;
                failed.error = Some(reason.clone());
                // Explicit auditor reruns are synchronous. Leave a durable recovery
                // action on protocol failure instead of bouncing the coder.
                self.block_task_after_executor_failure(task, &failed)
                    .await?;
                Err(ServiceError::invalid_operation(reason))
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn settle_rerun_review_outcome(
        &self,
        task: &Task,
        review: &Review,
        outcome: ::review::ReviewOutcome,
    ) -> Result<Task> {
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        match outcome {
            ::review::ReviewOutcome::Passed | ::review::ReviewOutcome::PassedCiOnly => {
                self.cascade_completed_review_task(
                    &current,
                    crate::workflow::default_states::MERGING,
                    "review rerun passed",
                    false,
                )
                .await?;
            }
            ::review::ReviewOutcome::AwaitingHuman => {}
            ::review::ReviewOutcome::AuditorFailed { .. }
            | ::review::ReviewOutcome::CiFailed { .. }
            | ::review::ReviewOutcome::MergeConflict { .. } => {
                let current = if current.review_passed_at.is_some() {
                    TaskRepo::set_review_passed_at_cas(
                        &*self.db,
                        &current.id,
                        current.version,
                        None,
                        &now_rfc3339(),
                    )
                    .await?
                } else {
                    current
                };
                let (current, target, reason) = self
                    .review_failure_target(&current, Some(&review.execution_id))
                    .await?;
                if let Some(target) = target {
                    self.cascade_completed_review_task(&current, &target, &reason, true)
                        .await?;
                }
            }
        }
        TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))
    }
}
