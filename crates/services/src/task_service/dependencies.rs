use super::*;

impl TaskService {
    pub async fn add_task_dependency(&self, task_id: &str, depends_on_id: &str) -> Result<()> {
        validate_required("task_id", task_id)?;
        validate_required("depends_on_id", depends_on_id)?;
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let dependency = TaskRepo::get_by_id(&*self.db, depends_on_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", depends_on_id.to_owned()))?;
        if dependency.project_id != task.project_id {
            return Err(ServiceError::invalid_operation(
                "Task dependencies must belong to the same Project",
            ));
        }
        if self.task_is_cancelled(&dependency).await? {
            return Err(ServiceError::invalid_operation(
                "Task dependencies cannot reference a cancelled Task",
            ));
        }
        TaskDependencyRepo::add_dependency(&*self.db, task_id, depends_on_id, &now_rfc3339())
            .await?;
        Ok(())
    }

    pub async fn remove_task_dependency(&self, task_id: &str, depends_on_id: &str) -> Result<()> {
        validate_required("task_id", task_id)?;
        validate_required("depends_on_id", depends_on_id)?;
        TaskDependencyRepo::remove_dependency(&*self.db, task_id, depends_on_id).await?;
        self.clear_resolved_dependency_block(task_id).await
    }

    pub(super) async fn cancelled_dependency_ids(
        &self,
        task: &Task,
        dependency_ids: &[String],
    ) -> Result<Vec<String>> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &Actor::system(api_types::SystemComponent::Workflow),
        );
        let cancellation_state = workflow
            .cancellation_state
            .as_deref()
            .unwrap_or(default_states::CANCELLED);
        let mut cancelled = Vec::new();
        for dependency_id in dependency_ids {
            if TaskRepo::get_by_id(&*self.db, dependency_id, false)
                .await?
                .is_some_and(|dependency| dependency.status == cancellation_state)
            {
                cancelled.push(dependency_id.clone());
            }
        }
        Ok(cancelled)
    }

    pub(super) async fn block_cancelled_dependencies(
        &self,
        task: &Task,
        cancelled_dependency_ids: &[String],
    ) -> Result<Task> {
        if cancelled_dependency_ids.is_empty() {
            return Ok(task.clone());
        }
        if task.blocked_json.is_some() || task.failed_json.is_some() {
            return Ok(task.clone());
        }

        let reason = format!(
            "required dependenc{} cancelled: {}",
            if cancelled_dependency_ids.len() == 1 {
                "y was"
            } else {
                "ies were"
            },
            cancelled_dependency_ids.join(", ")
        );
        let now = now_rfc3339();
        let annotation = api_types::TaskBlockingAnnotation {
            annotation_type: api_types::FailureKind::WorkflowGuardRejected,
            blocking_reason: "dependency_cancelled".to_owned(),
            blocked_by: Some(Actor::system(api_types::SystemComponent::Workflow).display()),
            blocked_at: Some(now.clone()),
            blocked_execution_id: None,
            artifact: cancelled_dependency_ids.first().map(|dependency_id| {
                api_types::BlockingArtifact {
                    kind: "task_dependency".to_owned(),
                    id: Some(dependency_id.clone()),
                    log_path: None,
                }
            }),
            message: Some(reason.clone()),
            hook: Some(json!({
                "name": "dependency_gate",
                "cancelled_dependency_ids": cancelled_dependency_ids,
            })),
            recovery_actions: vec![api_types::RecoveryAction::CancelTask],
        };
        let annotation = serde_json::to_string(&annotation).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize dependency blocker: {error}"
            ))
        })?;
        let blocked = json!({
            "reason": reason,
            "created_at": now,
            "kind": api_types::FailureKind::WorkflowGuardRejected,
            "source": "dependency_gate",
            "details": { "cancelled_dependency_ids": cancelled_dependency_ids },
        })
        .to_string();

        let mut current = task.clone();
        for attempt in 0..3 {
            match TaskRepo::update(
                &*self.db,
                db::UpdateTask {
                    id: current.id.clone(),
                    expected_version: current.version,
                    title: None,
                    description: None,
                    priority: None,
                    merge_config: None,
                    plan: None,
                    error_annotation: Some(Some(annotation.clone())),
                    blocked_json: Some(Some(blocked.clone())),
                    failed_json: Some(None),
                    task_state_config: None,
                    parent_task_id: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await
            {
                Ok(updated) => {
                    self.publish(ForgeEvent {
                        event_type: "task.blocked".to_owned(),
                        entity_id: updated.id.clone(),
                        timestamp: event_timestamp(),
                        context: EventContext::TaskBlocked {
                            project_id: updated.project_id.clone(),
                            reason: reason.clone(),
                            kind: Some(api_types::FailureKind::WorkflowGuardRejected),
                            source: Some("dependency_gate".to_owned()),
                            execution_id: None,
                        },
                    });
                    return Ok(updated);
                }
                Err(db::DbError::VersionConflict) if attempt < 2 => {
                    current = TaskRepo::get_by_id(&*self.db, &current.id, false)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", current.id.clone()))?;
                    if current.blocked_json.is_some() || current.failed_json.is_some() {
                        return Ok(current);
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(current)
    }

    pub(super) async fn block_dependents_of_cancelled_task(&self, task: &Task) -> Result<()> {
        for dependent_id in TaskDependencyRepo::list_dependents(&*self.db, &task.id).await? {
            let Some(dependent) = TaskRepo::get_by_id(&*self.db, &dependent_id, false).await?
            else {
                continue;
            };
            let project = ProjectRepo::get_by_id(&*self.db, &dependent.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", dependent.project_id.clone()))?;
            let workflow = WorkflowEngine::resolve_workflow_for_task(
                &dependent,
                &project.workflow_definition,
                &Actor::system(api_types::SystemComponent::Workflow),
            );
            if workflow.state_kind(&dependent.status) == Some(api_types::StateKind::Terminal) {
                continue;
            }
            self.block_cancelled_dependencies(&dependent, std::slice::from_ref(&task.id))
                .await?;
        }
        Ok(())
    }

    async fn clear_resolved_dependency_block(&self, task_id: &str) -> Result<()> {
        let Some(task) = TaskRepo::get_by_id(&*self.db, task_id, false).await? else {
            return Ok(());
        };
        let dependency_block = task
            .error_annotation
            .as_deref()
            .and_then(|raw| serde_json::from_str::<api_types::TaskBlockingAnnotation>(raw).ok())
            .is_some_and(|annotation| annotation.blocking_reason == "dependency_cancelled");
        if !dependency_block {
            return Ok(());
        }
        let dependencies = TaskDependencyRepo::list_dependencies(&*self.db, task_id).await?;
        if !self
            .cancelled_dependency_ids(&task, &dependencies)
            .await?
            .is_empty()
        {
            return Ok(());
        }
        TaskRepo::update(
            &*self.db,
            db::UpdateTask {
                id: task.id,
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
        Ok(())
    }

    async fn task_is_cancelled(&self, task: &Task) -> Result<bool> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &Actor::system(api_types::SystemComponent::Workflow),
        );
        Ok(task.status
            == workflow
                .cancellation_state
                .as_deref()
                .unwrap_or(default_states::CANCELLED))
    }
}
