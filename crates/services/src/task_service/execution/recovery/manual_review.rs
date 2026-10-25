//! Manual review-pass recovery and review follow-up dispatch.

use super::*;

impl TaskService {
    pub(crate) async fn recover_manual_review_pass(
        &self,
        task: Task,
        owner_reason: String,
        finding: Option<&api_types::TaskBlockingAnnotation>,
        action: api_types::TaskAction,
    ) -> Result<Task> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(action.clone()),
            )),
        );
        if !crate::workflow::is_review_state(&workflow, &task.status) {
            return Err(ServiceError::invalid_operation(format!(
                "{action} is only supported from review state, got {}",
                task.status
            )));
        }
        let pass_target = workflow
            .auto_transition_target(&task.status)
            .unwrap_or(crate::workflow::default_states::MERGING)
            .to_owned();
        let latest_review = self.latest_review_for_task(&task.id).await?;
        if latest_review.status != ReviewStatus::Failed {
            return Err(ServiceError::invalid_operation(format!(
                "{action} requires the latest review attempt to be failed"
            )));
        }
        let active_review_execution = ExecutionRepo::list_running_by_task(&*self.db, &task.id)
            .await?
            .into_iter()
            .find(|execution| {
                matches!(
                    execution.role.as_str(),
                    crate::workflow::default_roles::REVIEWER
                        | crate::workflow::default_roles::AUDITOR
                )
            });
        if let Some(execution) = active_review_execution {
            return Err(ServiceError::invalid_operation(format!(
                "cannot pass review manually while {} execution {} is running",
                execution.role, execution.id
            )));
        }
        let finished_at = now_rfc3339();
        let follow_up = finding
            .map(|finding| {
                let backlog = workflow
                    .states
                    .iter()
                    .find(|state| state.kind == api_types::StateKind::Backlog)
                    .ok_or_else(|| {
                        ServiceError::invalid_operation("workflow has no backlog state")
                    })?;
                let message = finding
                    .message
                    .as_deref()
                    .unwrap_or(&finding.blocking_reason);
                let finding_reason = message
                    .strip_prefix("fixable by owner: ")
                    .or_else(|| message.strip_prefix("repeated finding: "))
                    .unwrap_or(message);
                let first_line = finding_reason.lines().next().unwrap_or_default();
                let summary: String = first_line.chars().take(80).collect();
                let summary = if first_line.chars().count() > 80 {
                    format!("{summary}…")
                } else {
                    summary
                };
                Ok::<_, ServiceError>(db::CreateTask {
                    id: new_uuid_v4(),
                    project_id: task.project_id.clone(),
                    parent_task_id: None,
                    assignee_type: None,
                    assignee_id: None,
                    title: format!("Follow-up: {} — {summary}", task.title),
                    description: Some(format!(
                        "Follow-up of Task {}.\n\nParked finding ({}):\n{}\n\nOwner reason:\n{}",
                        task.id, finding.blocking_reason, message, owner_reason
                    )),
                    task_type: "task".to_owned(),
                    status: backlog.name.clone(),
                    is_automation: false,
                    priority: task.priority,
                    subtask_order: None,
                    task_state_config: None,
                    merge_config: None,
                    plan: None,
                    created_at: finished_at.clone(),
                    updated_at: finished_at.clone(),
                })
            })
            .transpose()?;
        let follow_up_governance = if let Some(follow_up) = &follow_up {
            self.prepare_task_governance(&project, &follow_up.task_type, None)
                .await?
        } else {
            None
        };
        let reason = match &follow_up {
            Some(follow_up) => format!(
                "deferred to follow-up {} ({}): {owner_reason}",
                follow_up.id, follow_up.title
            ),
            None => owner_reason,
        };
        let mut details = strict_review_details(&latest_review)?;
        details["manual_override"] = json!({
            "action": action.to_string(),
            "reason": reason.clone(),
            "actor_type": "user",
            "source_review_id": latest_review.id,
            "source_attempt_number": latest_review.attempt_number,
            "at": finished_at,
        });
        let manual_pass = db::CreateManualReviewPass {
            id: new_uuid_v4(),
            source_review_id: latest_review.id.clone(),
            source_review_updated_at: latest_review.updated_at.clone(),
            task_id: task.id.clone(),
            candidate_execution_id: latest_review.execution_id.clone(),
            step_results_json: details.to_string(),
            expected_task_version: task.version,
            expected_task_status: task.status.clone(),
            expected_project_version: project.version,
            expected_workflow_definition: project.workflow_definition.clone(),
            occurred_at: finished_at.clone(),
        };
        let (review, task) = if let Some(follow_up) = follow_up {
            let mut transaction = db::begin_immediate(self.db.pool()).await?;
            let follow_up = self
                .insert_created_task_in_tx(&mut transaction, follow_up, follow_up_governance)
                .await?;
            let linked = self
                .db
                .link_manual_review_follow_up_in_tx(
                    &mut transaction,
                    &task.id,
                    json!({ "follow_up_of": task.id }).to_string(),
                    &follow_up.id,
                    follow_up.version,
                )
                .await?;
            if linked.require_applied()? != 1 {
                return Err(db::DbError::VersionConflict.into());
            }
            let (review, task) = ReviewRepo::create_manual_pass_with_task_authority_in_tx(
                &*self.db,
                &mut transaction,
                manual_pass,
            )
            .await?;
            let cleared = self
                .db
                .clear_manual_review_failure_in_tx(
                    &mut transaction,
                    &finished_at,
                    &task.id,
                    task.version,
                )
                .await?;
            if cleared.require_applied()? != 1 {
                return Err(db::DbError::VersionConflict.into());
            }
            let task = TaskRepo::get_by_id_in_tx(&*self.db, &mut transaction, &task.id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
            transaction.commit().await?;
            self.publish(ForgeEvent {
                event_type: "task.created".to_owned(),
                entity_id: follow_up.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskCreated {
                    project_id: follow_up.project_id,
                    title: follow_up.title,
                },
            });
            self.publish(ForgeEvent {
                event_type: "task.updated".to_owned(),
                entity_id: task.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskUpdated {
                    project_id: task.project_id.clone(),
                },
            });
            (review, task)
        } else {
            ReviewRepo::create_manual_pass_with_task_authority(&*self.db, manual_pass).await?
        };

        if let Err(error) = self
            .memory_service
            .record_review_result_if_final(&task.project_id, &review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }
        self.create_system_comment(
            &task.id,
            format!(
                "Review passed manually (attempt {}): {}",
                review.attempt_number, reason
            ),
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
            action = %action,
            "manual review recovery action logged"
        );
        let transitioned = self
            .transition(
                task.id.clone(),
                pass_target,
                TransitionOptions {
                    bridge: Default::default(),
                    version: task.version,
                    reason: Some(reason),
                    triggered_by: TaskService::task_action_actor(api_types::Actor::user(
                        api_types::UserActionSource::Action(action),
                    )),
                    rejection: false,
                    defer_dispatch_seconds: None,
                },
            )
            .await?;
        Ok(transitioned.task)
    }
}
