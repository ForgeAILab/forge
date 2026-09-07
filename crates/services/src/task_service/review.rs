use super::*;
use api_types::{Actor, UserActionSource};

impl TaskService {
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
        let finished_at = now_rfc3339();
        let review = ReviewRepo::update_status(
            &*self.db,
            &latest_review.id,
            ReviewStatus::Passed,
            latest_review.step_results_json.clone(),
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
        TaskRepo::set_review_passed_at(
            &*self.db,
            &task_id,
            Some(finished_at.clone()),
            &finished_at,
        )
        .await?;
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
        let reason = reason
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "manual review rejected".to_owned());
        let finished_at = now_rfc3339();
        let review = ReviewRepo::update_status(
            &*self.db,
            &latest_review.id,
            ReviewStatus::Failed,
            latest_review.step_results_json.clone(),
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
        TaskRepo::set_review_passed_at(&*self.db, &task_id, None, &finished_at).await?;
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
        let follow_up_already_dispatched = ExecutionRepo::list_by_task_and_role(
            &*self.db,
            &task_id,
            crate::workflow::default_roles::CODER,
            PageRequest {
                cursor: None,
                limit: 20,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await?
        .items
        .into_iter()
        .any(|execution| {
            execution.parent_execution_id.as_deref() == Some(review.execution_id.as_str())
                && matches!(
                    execution.status,
                    ExecutionStatus::Running | ExecutionStatus::Completed
                )
        });
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
        let workspace_id = execution.workspace_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("executor execution missing workspace_id")
        })?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
        let review_config = review_config_from_json(task.task_state_config.as_deref())?;
        let reviewer_assignment = TaskRoleAssignmentRepo::get_by_task_and_role(
            &*self.db,
            &task.id,
            crate::workflow::default_roles::REVIEWER,
        )
        .await?;
        let reviewer_agent_id = reviewer_assignment.and_then(|assignment| {
            (assignment.assignee_type == Some(db::AssigneeKind::Agent))
                .then_some(assignment.assignee_id)
                .flatten()
        });
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &Actor::system(api_types::SystemComponent::Workflow),
        );
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
            .run(ReviewRequest {
                task_id,
                executor_execution_id,
                workspace_path: workspace.worktree_path.into(),
                ci_steps: review_config.ci_steps,
                logs_path,
                auditor_agent_id: reviewer_agent_id,
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
                let current =
                    TaskRepo::set_review_passed_at(&*self.db, &current.id, None, &now_rfc3339())
                        .await?;
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
