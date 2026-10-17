use super::*;

/// Which way one prerequisite edge is moving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskDependencyAction {
    /// `task_id` waits for `depends_on_task_id`.
    Add,
    /// `task_id` no longer waits for it.
    Remove,
}

impl TaskService {
    pub async fn add_task_dependency(&self, task_id: &str, depends_on_id: &str) -> Result<()> {
        if !db::task_writer::owns_task(task_id) {
            return self
                .request_task_command(
                    task_id,
                    "add_task_dependency",
                    serde_json::json!([task_id, depends_on_id]),
                    false,
                )
                .await;
        }
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
        if task.parent_task_id.as_deref() == Some(depends_on_id) {
            return Err(ServiceError::invalid_operation(
                "a subtask cannot depend on its coordination parent; parentage shares a workspace and the parent completes only after its children",
            ));
        }
        if self.task_is_cancelled(&dependency).await? {
            return Err(ServiceError::invalid_operation(
                "Task dependencies cannot reference a cancelled Task",
            ));
        }
        // Adding a link that already exists is a no-op, not a constraint
        // failure surfaced as an internal error.
        if TaskDependencyRepo::list_dependencies(&*self.db, task_id)
            .await?
            .iter()
            .any(|existing| existing == depends_on_id)
        {
            return Ok(());
        }
        TaskDependencyRepo::add_dependency(&*self.db, task_id, depends_on_id, &now_rfc3339())
            .await?;
        Ok(())
    }

    pub async fn remove_task_dependency(&self, task_id: &str, depends_on_id: &str) -> Result<()> {
        if !db::task_writer::owns_task(task_id) {
            return self
                .request_task_command(
                    task_id,
                    "remove_task_dependency",
                    serde_json::json!([task_id, depends_on_id]),
                    false,
                )
                .await;
        }
        validate_required("task_id", task_id)?;
        validate_required("depends_on_id", depends_on_id)?;
        TaskDependencyRepo::remove_dependency(&*self.db, task_id, depends_on_id).await?;
        self.clear_resolved_dependency_block(task_id).await?;

        let wake_reason = format!("dependency {depends_on_id} removed");
        if let Err(error) = crate::wake_task_dispatch(&self.db, task_id, &wake_reason).await {
            tracing::warn!(
                task_id = %task_id,
                depends_on_id = %depends_on_id,
                %error,
                "dependency removed but waking Task dispatch failed"
            );
        }
        Ok(())
    }

    /// Adds or removes one prerequisite edge on behalf of a Project Agent.
    ///
    /// The Project comes from the server-derived binding, never from the
    /// payload, and both Tasks must belong to it — otherwise an Agent bound to
    /// one Project could rewire another Project's graph by id. Everything
    /// below this is the same path the REST endpoints use, including clearing
    /// a dependency block when the edge that caused it goes away.
    pub async fn perform_project_agent_dependency(
        &self,
        project_id: &str,
        task_id: &str,
        depends_on_task_id: &str,
        action: TaskDependencyAction,
    ) -> Result<Task> {
        validate_required("task_id", task_id)?;
        validate_required("depends_on_task_id", depends_on_task_id)?;
        if task_id == depends_on_task_id {
            return Err(ServiceError::invalid_operation(
                "a Task cannot depend on itself",
            ));
        }
        for id in [task_id, depends_on_task_id] {
            let task = TaskRepo::get_by_id(&*self.db, id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", id.to_owned()))?;
            if task.project_id != project_id {
                return Err(ServiceError::invalid_operation(
                    "both Tasks must belong to the bound Project",
                ));
            }
        }
        match action {
            TaskDependencyAction::Add => {
                self.add_task_dependency(task_id, depends_on_task_id)
                    .await?
            }
            TaskDependencyAction::Remove => {
                self.remove_task_dependency(task_id, depends_on_task_id)
                    .await?
            }
        }
        TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))
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
        if !db::task_writer::owns_task(&task.id) {
            if db::task_writer::current_task_step().is_some() {
                // Identity-fenced (TaskCommand::fence): a dependency block is
                // never lost to a status change or a preempting Hold.
                self.enqueue_task_command(
                    &task.id,
                    "block_cancelled_dependencies",
                    serde_json::json!([task.id, cancelled_dependency_ids]),
                    false,
                )
                .await?;
                return Ok(task.clone());
            }
            return self
                .request_task_command(
                    &task.id,
                    "block_cancelled_dependencies",
                    serde_json::json!([task.id, cancelled_dependency_ids]),
                    false,
                )
                .await;
        }
        let current_task = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let task = &current_task;
        if cancelled_dependency_ids.is_empty() {
            return Ok(task.clone());
        }
        // The blocker takes the place of whatever the Task was waiting on (a
        // hold, a failure park, the saved condition of a queued action) and
        // carries it, so removing the dependency puts it back.
        let (cancelled_dependency_ids, superseded) = match dependency_block(task) {
            Some(block) => {
                let mut ids = block.cancelled_dependency_ids.clone();
                for id in cancelled_dependency_ids {
                    if !ids.contains(id) {
                        ids.push(id.clone());
                    }
                }
                if ids == block.cancelled_dependency_ids {
                    return Ok(task.clone());
                }
                (ids, block.superseded)
            }
            None => (
                cancelled_dependency_ids.to_vec(),
                superseded_condition(task),
            ),
        };
        self.write_dependency_block(task, &cancelled_dependency_ids, superseded)
            .await
    }

    /// Hold a Task that carries a cancelled-dependency blocker: the blocker
    /// stays (it names the dependency and is the Task's first exit) and the
    /// hold takes the place of whatever the blocker had displaced, so
    /// removing the dependency leaves the Task held, with its release on
    /// offer. `None` when the Task carries no such blocker.
    pub(super) async fn hold_under_dependency_block(
        &self,
        task: &Task,
        hold_annotation: String,
        hold_blocked: String,
    ) -> Result<Option<Task>> {
        let Some(block) = dependency_block(task) else {
            return Ok(None);
        };
        let superseded = json!({
            "status": task.status,
            "error_annotation": hold_annotation,
            "blocked_json": hold_blocked,
            "failed_json": Value::Null,
        });
        self.write_dependency_block(task, &block.cancelled_dependency_ids, Some(superseded))
            .await
            .map(Some)
    }

    async fn write_dependency_block(
        &self,
        task: &Task,
        cancelled_dependency_ids: &[String],
        superseded: Option<Value>,
    ) -> Result<Task> {
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
            "details": {
                "cancelled_dependency_ids": cancelled_dependency_ids,
                "superseded": superseded,
            },
        })
        .to_string();

        let current = task.clone();
        {
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
                    Ok(updated)
                }
                Err(error) => Err(error.into()),
            }
        }
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

    /// Success-path mirror of [`Self::block_dependents_of_cancelled_task`]:
    /// when a prerequisite Task reaches a terminal, non-cancelled state (i.e.
    /// it completes normally), every dependent Task's stale dispatch
    /// disposition and deferred-dispatch cooldown are cleared so the next
    /// dispatcher scan reconsiders it instead of skipping it forever on a
    /// disposition keyed to the dependent's own (unchanged) version.
    ///
    /// A failure waking one dependent is logged and does not stop the
    /// others, and never fails the prerequisite's own transition — the
    /// dependency gate re-checks on the next scan regardless, so a missed
    /// wake here is a delay, not data loss.
    pub(crate) async fn wake_dependents_of_completed_task(&self, task: &Task) -> Result<()> {
        for dependent_id in TaskDependencyRepo::list_dependents(&*self.db, &task.id).await? {
            if let Err(error) = self
                .wake_one_dependent_of_completed_task(task, &dependent_id)
                .await
            {
                tracing::warn!(
                    task_id = %dependent_id,
                    dependency_id = %task.id,
                    %error,
                    "failed to wake dependent task after prerequisite completed"
                );
            }
        }
        Ok(())
    }

    async fn wake_one_dependent_of_completed_task(
        &self,
        task: &Task,
        dependent_id: &str,
    ) -> Result<()> {
        let Some(dependent) = TaskRepo::get_by_id(&*self.db, dependent_id, false).await? else {
            return Ok(());
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
            // Already finished or cancelled; nothing to wake.
            return Ok(());
        }
        crate::wake_task_dispatch(
            &self.db,
            &dependent.id,
            &format!("dependency {} reached a terminal success state", task.id),
        )
        .await
    }

    /// Removing a cancelled dependency is the blocker's exit. While another
    /// cancelled dependency remains the blocker names what is left; after the
    /// last one the Task gets back the condition the blocker displaced, or a
    /// clear one the dispatcher picks up.
    async fn clear_resolved_dependency_block(&self, task_id: &str) -> Result<()> {
        let Some(task) = TaskRepo::get_by_id(&*self.db, task_id, false).await? else {
            return Ok(());
        };
        let Some(block) = dependency_block(&task) else {
            return Ok(());
        };
        let dependencies = TaskDependencyRepo::list_dependencies(&*self.db, task_id).await?;
        let remaining = self.cancelled_dependency_ids(&task, &dependencies).await?;
        if !remaining.is_empty() {
            if remaining != block.cancelled_dependency_ids {
                self.write_dependency_block(&task, &remaining, block.superseded)
                    .await?;
            }
            return Ok(());
        }
        // The displaced condition goes back only onto the Task it was taken
        // from: a Task that has since finished, been cancelled or changed
        // state has a newer story, and a stale hold or failure park written
        // over it would name work that no longer exists.
        let still_applies = !self.task_is_terminal(&task).await?
            && block.superseded.as_ref().is_some_and(|superseded| {
                superseded
                    .get("status")
                    .and_then(Value::as_str)
                    .is_none_or(|status| status == task.status)
            });
        let restored = |key: &str| {
            block
                .superseded
                .as_ref()
                .filter(|_| still_applies)
                .and_then(|superseded| superseded.get(key))
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
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
                error_annotation: Some(restored("error_annotation")),
                blocked_json: Some(restored("blocked_json")),
                failed_json: Some(restored("failed_json")),
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(())
    }

    async fn task_is_terminal(&self, task: &Task) -> Result<bool> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &Actor::system(api_types::SystemComponent::Workflow),
        );
        Ok(workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal))
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

/// The typed `dependency_cancelled` blocker a Task carries, if any.
struct DependencyBlock {
    cancelled_dependency_ids: Vec<String>,
    /// The condition columns the blocker displaced.
    superseded: Option<Value>,
}

fn dependency_block(task: &Task) -> Option<DependencyBlock> {
    let annotation = task
        .error_annotation
        .as_deref()
        .and_then(|raw| serde_json::from_str::<api_types::TaskBlockingAnnotation>(raw).ok())
        .filter(|annotation| annotation.blocking_reason == "dependency_cancelled")?;
    let cancelled_dependency_ids = annotation
        .hook
        .as_ref()
        .and_then(|hook| hook.get("cancelled_dependency_ids"))
        .and_then(Value::as_array)
        .map(|ids| {
            ids.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let superseded = task
        .blocked_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .map(|blocked| blocked["details"]["superseded"].clone())
        .filter(Value::is_object);
    Some(DependencyBlock {
        cancelled_dependency_ids,
        superseded,
    })
}

/// What the Task is waiting on right now: its own condition columns, or the
/// condition an accepted, not yet dispatched Task action saved.
fn superseded_condition(task: &Task) -> Option<Value> {
    let columns = |annotation: Option<&str>, blocked: Option<&str>, failed: Option<&str>| {
        (annotation.is_some() || blocked.is_some() || failed.is_some()).then(|| {
            json!({
                "status": task.status,
                "error_annotation": annotation,
                "blocked_json": blocked,
                "failed_json": failed,
            })
        })
    };
    columns(
        task.error_annotation.as_deref(),
        task.blocked_json.as_deref(),
        task.failed_json.as_deref(),
    )
    .or_else(|| {
        let queued = db::TaskMetadata::parse(task.metadata_json.as_deref())
            .ok()?
            .extra
            .get(crate::deferred_dispatch::QUEUED_RECOVERY_KEY)
            .cloned()?;
        let saved = |key: &str| queued.get(key).and_then(Value::as_str);
        columns(
            saved("error_annotation"),
            saved("blocked_json"),
            saved("failed_json"),
        )
    })
}
