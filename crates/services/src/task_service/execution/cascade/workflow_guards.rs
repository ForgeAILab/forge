//! Completion guard retries, rejection prompts, and metadata.

use super::*;

impl TaskService {
    pub(in crate::task_service::execution) async fn handle_executor_completion_guard_rejection(
        &self,
        execution: &Execution,
        task: &Task,
        current_state: &api_types::StateDefinition,
        guard: &str,
        reason: &str,
    ) -> Result<()> {
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &task.project_id).await? else {
            return Ok(());
        };
        if crate::task_service::execution_dispatch_project_version(execution)
            != Some(project.version)
        {
            return Ok(());
        }
        if guard == "subtask_sequence_complete"
            && crate::task_hierarchy::coordination_root_has_subtasks(&self.db, task).await?
        {
            if !self
                .clear_workflow_guard_retry_metadata_for_latest_execution(
                    task,
                    execution,
                    project.version,
                )
                .await?
            {
                return Ok(());
            }
            self.wake_next_ordered_subtask(
                &task.id,
                "coordination root is waiting for its next ordered subtask",
            )
            .await?;
            return Ok(());
        }

        let budget = db::budget::limit(
            task,
            db::budget::Kind::Execution,
            Some(&current_state.config),
            current_state.gate_config.as_ref(),
        )?;
        let retry_count = db::budget::spent(
            self.db.pool(),
            &task.id,
            db::budget::Kind::WorkflowGuard.key(),
        )
        .await? as u64;

        if !db::budget::allows_retry(i64::from(budget), retry_count as i64)
            || execution.agent_session_id.is_none()
            || execution.agent_id.is_none()
        {
            return self
                .annotate_workflow_guard_block(execution, task, guard, reason)
                .await;
        }

        let now = now_rfc3339();
        let Some(updated_task) = TaskRepo::mutate_metadata_and_bump_version_for_latest_execution(
            &*self.db,
            &task.id,
            task.version,
            crate::task_service::execution::latest_execution_authority(execution, project.version),
            vec![
                db::TaskMetadataMutation::Budget(db::budget::Mutation::Charge {
                    key: db::budget::Kind::WorkflowGuard.key().into(),
                    limit: i64::from(budget),
                    step: format!("guard:{}", execution.id),
                }),
                db::TaskMetadataMutation::Set {
                    key: "last_workflow_guard_rejection_at".to_owned(),
                    value: Value::String(now.clone()),
                },
                db::TaskMetadataMutation::Set {
                    key: "last_workflow_guard_name".to_owned(),
                    value: Value::String(guard.to_owned()),
                },
                db::TaskMetadataMutation::Set {
                    key: "last_workflow_guard_reason".to_owned(),
                    value: Value::String(reason.to_owned()),
                },
                db::TaskMetadataMutation::Set {
                    key: "last_workflow_guard_execution_id".to_owned(),
                    value: Value::String(execution.id.clone()),
                },
            ],
            &now,
        )
        .await?
        else {
            return Ok(());
        };
        let attempt = db::budget::spent(
            self.db.pool(),
            &task.id,
            db::budget::Kind::WorkflowGuard.key(),
        )
        .await? as u64;

        let prompt = render_workflow_guard_follow_up_prompt(guard, reason, attempt, budget as u64);
        self.resume_execution_for_workflow_guard(execution, &updated_task, prompt)
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

    async fn annotate_workflow_guard_block(
        &self,
        execution: &Execution,
        task: &Task,
        guard: &str,
        reason: &str,
    ) -> Result<()> {
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &task.project_id).await? else {
            return Ok(());
        };
        if crate::task_service::execution_dispatch_project_version(execution)
            != Some(project.version)
        {
            return Ok(());
        }
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
            crate::task_service::execution::latest_execution_authority(execution, project.version),
        )
        .await?
        else {
            return Ok(());
        };

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

    fn guard_success_mutations(task: &Task, identity: &str) -> Vec<db::TaskMetadataMutation> {
        let mut mutations = vec![db::TaskMetadataMutation::Budget(
            db::budget::Mutation::Reset {
                key: db::budget::Kind::WorkflowGuard.key().into(),
                window: format!("success:{identity}"),
            },
        )];
        for key in [
            "last_workflow_guard_rejection_at",
            "last_workflow_guard_name",
            "last_workflow_guard_reason",
            "last_workflow_guard_execution_id",
        ] {
            mutations.push(db::TaskMetadataMutation::Remove { key: key.into() });
        }
        let _ = task;
        mutations
    }
    pub(in crate::task_service::execution) async fn clear_workflow_guard_retry_metadata(
        &self,
        task: &Task,
    ) -> Result<()> {
        TaskRepo::mutate_metadata(
            &*self.db,
            &task.id,
            None,
            Self::guard_success_mutations(task, &task.updated_at),
            &now_rfc3339(),
        )
        .await?;
        Ok(())
    }
    async fn clear_workflow_guard_retry_metadata_for_latest_execution(
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
                crate::task_service::execution::latest_execution_authority(
                    execution,
                    project_version,
                ),
                Self::guard_success_mutations(task, &execution.id),
                &now_rfc3339(),
            )
            .await?
            .is_some(),
        )
    }
}
