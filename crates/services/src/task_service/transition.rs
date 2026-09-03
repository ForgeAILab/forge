use super::*;
use api_types::{Actor, SystemComponent, UserActionSource};
use db::UpdateTask;

impl TaskService {
    pub async fn transition(
        &self,
        task_id: impl Into<String>,
        new_status: TaskStatus,
        options: impl Into<TransitionOptions>,
    ) -> Result<TransitionResult> {
        let task_id = task_id.into();
        let options = options.into();
        let trigger_reason = options.reason.unwrap_or_else(|| "user action".to_owned());
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let previous_status = task.status.clone();
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &options.triggered_by,
        );
        if task.repo_id.is_some()
            && workflow.state_kind(&new_status) == Some(api_types::StateKind::Active)
        {
            // Direct transitions must obey the same admission boundary as
            // claim/launch for every repository-capable task type.  Task
            // labels such as discovery/planning only select a read-only
            // executor profile; they still use the normal lease boundary.
            self.ensure_task_runnable(&task).await?;
        }
        self.ensure_planning_plan_ready_before_leaving(
            &task,
            &new_status,
            &workflow,
            options.rejection,
        )
        .await?;
        self.cancel_active_execution_for_user_transition(
            &task,
            &new_status,
            &workflow,
            &options.triggered_by,
        )
        .await?;
        let was_blocked = task.blocked_json.is_some();
        let blocked_previous_reason = task
            .blocked_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<Value>(json).ok())
            .and_then(|v| v.get("reason").and_then(Value::as_str).map(str::to_owned));
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
        let defer_dispatch_until = options
            .defer_dispatch_seconds
            .map(|seconds| (chrono::Utc::now() + chrono::Duration::seconds(seconds)).to_rfc3339());
        let preserve_deferred_dispatch = defer_dispatch_until.is_some();
        let result = engine
            .transition_with_deferred_dispatch(
                &task_id,
                &new_status,
                options.version,
                &workflow,
                &options.triggered_by,
                &trigger_reason,
                options.rejection,
                defer_dispatch_until,
            )
            .await?;
        // Entry/after-enter hooks can update metadata without changing the
        // Task version (for example, a manual review marker). Reload the
        // committed row so the returned Task snapshot and its version carry
        // the same state used by API readiness calculations.
        let mut task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if was_blocked {
            self.publish(ForgeEvent {
                event_type: "task.unblocked".to_owned(),
                entity_id: task.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskUnblocked {
                    project_id: task.project_id.clone(),
                    previous_reason: blocked_previous_reason.clone(),
                },
            });
            tracing::info!(
                task_id = %task.id,
                from_status = %previous_status,
                to_status = %task.status,
                previous_reason = ?blocked_previous_reason,
                "blocked metadata cleared by transition"
            );
        }
        if should_clear_review_passed_at(
            &workflow,
            &previous_status,
            &task.status,
            options.rejection,
            &options.triggered_by,
        ) {
            task =
                TaskRepo::set_review_passed_at(&*self.db, &task.id, None, &now_rfc3339()).await?;
        }
        if previous_status == crate::workflow::default_states::PLANNING
            && (task.status != crate::workflow::default_states::PLANNING || options.rejection)
        {
            task = super::execution::set_planning_awaiting_review_metadata(
                &self.db, &task, None, false,
            )
            .await?;
        }
        if previous_status == crate::workflow::default_states::REVIEW
            && task.status != crate::workflow::default_states::REVIEW
        {
            task = clear_manual_review_awaiting_metadata(&self.db, &task).await?;
        }
        if should_clear_transient_error_annotation(&task) {
            match TaskRepo::update(
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
                    failed_json: None,
                    task_state_config: None,
                    parent_task_id: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await
            {
                Ok(updated) => task = updated,
                Err(DbError::VersionConflict) => {
                    // An on_enter hook (e.g. dispatch_role_follow_up) may have already
                    // cleared the annotation and incremented the version; re-fetch.
                    task = TaskRepo::get_by_id(&*self.db, &task.id, false)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        if previous_status != task.status {
            if preserve_deferred_dispatch {
                super::execution::clear_execution_retry_metadata_preserving_dispatch(
                    &self.db, &task,
                )
                .await?;
            } else {
                super::execution::clear_execution_retry_metadata(&self.db, &task).await?;
            }
        }

        Ok(TransitionResult {
            task,
            review: result.review,
        })
    }

    pub(super) async fn ensure_planning_plan_ready_before_leaving(
        &self,
        task: &Task,
        new_status: &TaskStatus,
        workflow: &api_types::WorkflowDefinition,
        rejection: bool,
    ) -> Result<()> {
        if task.status != crate::workflow::default_states::PLANNING
            || new_status == crate::workflow::default_states::PLANNING
            || workflow.cancellation_state.as_deref() == Some(new_status.as_str())
            || rejection
        {
            return Ok(());
        }

        let planning_state = workflow
            .states
            .iter()
            .find(|state| state.name == crate::workflow::default_states::PLANNING);
        if planning_state
            .and_then(|state| state.gate_config.as_ref())
            .is_some_and(|gate_config| gate_config.optional_when_unassigned())
        {
            let planner_assignment = TaskRoleAssignmentRepo::get_by_task_and_role(
                &*self.db,
                &task.id,
                crate::workflow::default_roles::PLANNER,
            )
            .await?;
            let planner_assigned = planner_assignment.as_ref().is_some_and(|assignment| {
                assignment.assignee_type.is_some() && assignment.assignee_id.is_some()
            });
            if !planner_assigned {
                return Ok(());
            }
        }

        let Some(workspace) = WorkspaceRepo::get_by_task_id(&*self.db, &task.id).await? else {
            return Err(ServiceError::invalid_operation(
                "planning cannot be approved before a plan artifact exists",
            ));
        };

        let artifact = match crate::plan_artifact::read_plan_artifact(
            std::path::Path::new(&workspace.worktree_path),
            None,
        ) {
            Ok(artifact) => artifact,
            Err(crate::plan_artifact::PlanArtifactError::NotFound) => {
                return Err(ServiceError::invalid_operation(
                    "planning cannot be approved before a plan artifact exists",
                ));
            }
            Err(error) => {
                return Err(ServiceError::invalid_operation(format!(
                    "planning plan artifact is unreadable: {error}"
                )));
            }
        };
        let summary = crate::plan_artifact::to_plan_progress_summary(&artifact);
        if summary.total == 0 {
            return Err(ServiceError::invalid_operation(
                "planning cannot be approved before the plan has checklist items",
            ));
        }

        Ok(())
    }

    pub async fn is_awaiting_human(&self, task_id: impl Into<String>) -> Result<bool> {
        let task_id = task_id.into();
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        self.is_task_awaiting_human(&task).await
    }

    /// Resolve human-readiness from the same Task snapshot whose version will
    /// be returned to the caller.
    pub async fn is_task_awaiting_human(&self, task: &Task) -> Result<bool> {
        if task.blocked_json.is_some() {
            return Ok(true);
        }
        if task.entry_barrier_is_running() {
            // The state's blocking before_enter hooks are still running. The
            // status change is visible but the transition has not settled: the
            // engine clears the barrier (bumping the task version) once they
            // finish, so a gate decision taken now would race that write.
            return Ok(false);
        }
        let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid task metadata: {error}"))
        })?;
        if metadata
            .extra
            .get("awaiting_human")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Ok(true);
        }
        if task.status == crate::workflow::default_states::REVIEW {
            let latest_review = ReviewRepo::list_by_task(&*self.db, &task.id)
                .await?
                .into_iter()
                .max_by_key(|review| review.attempt_number);
            if latest_review
                .as_ref()
                .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
            {
                return Ok(true);
            }
        }
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &Actor::system(SystemComponent::General),
        );
        let Some(state) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
        else {
            return Ok(false);
        };
        if task.status == crate::workflow::default_states::PLANNING {
            let Some(role_name) = state.role.as_deref() else {
                return Ok(false);
            };
            let assignment =
                TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role_name)
                    .await?;
            return Ok(assignment.as_ref().is_some_and(|assignment| {
                assignment.assignee_type == Some(AssigneeKind::User)
                    && assignment.assignee_id.is_some()
            }));
        }
        if state.kind != api_types::StateKind::Gate {
            return Ok(false);
        }
        let transition_log = TransitionLogRepo::list_by_task(&*self.db, &task.id).await?;
        let entered_at = transition_log
            .iter()
            .rev()
            .find(|entry| entry.to_state == task.status)
            .map(|entry| entry.created_at.as_str())
            .unwrap_or(task.created_at.as_str());
        let has_decision_since_entry = transition_log.iter().any(|entry| {
            entry.from_state == task.status
                && entry.created_at.as_str() >= entered_at
                && (entry.trigger_reason.starts_with("gate approved")
                    || entry.trigger_reason.starts_with("gate rejected"))
        });
        if let Some(gate_config) = state
            .gate_config
            .as_ref()
            .filter(|gate_config| gate_config.requires_user_approval())
        {
            if gate_config.optional_when_unassigned() {
                let Some(role_name) = state.role.as_deref() else {
                    return Ok(false);
                };
                let assignment =
                    TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role_name)
                        .await?;
                let assigned = assignment.as_ref().is_some_and(|assignment| {
                    assignment.assignee_type.is_some() && assignment.assignee_id.is_some()
                });
                if !assigned {
                    return Ok(false);
                }
            }
            return Ok(!has_decision_since_entry);
        }

        let Some(role_name) = state.role.as_deref() else {
            return Ok(false);
        };

        let role_assignments = TaskRoleAssignmentRepo::list_by_task(&*self.db, &task.id).await?;
        let Some(assignment) = role_assignments
            .iter()
            .find(|assignment| assignment.role_name == role_name)
        else {
            return Ok(false);
        };
        if assignment.assignee_type != Some(AssigneeKind::User) {
            return Ok(false);
        }

        Ok(!has_decision_since_entry)
    }

    pub async fn executor_attempt_count(&self, task_id: &str) -> Result<i64> {
        validate_required("task_id", task_id)?;
        let executor =
            ExecutionRepo::count_by_task_and_role(&*self.db, task_id, "executor").await?;
        let coder = ExecutionRepo::count_by_task_and_role(&*self.db, task_id, "coder").await?;
        Ok(executor + coder)
    }

    pub async fn remaining_retries(&self, task_id: &str) -> Result<i32> {
        validate_required("task_id", task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;

        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &Actor::system(SystemComponent::General),
        );
        let state = workflow
            .states
            .iter()
            .find(|state| state.name == task.status);
        let max_retries = super::config::runtime_retry_budget(
            &task,
            super::config::RetryBudgetKind::Review,
            state.map(|state| &state.config),
            state.and_then(|state| state.gate_config.as_ref()),
        )?;

        let attempts = self.executor_attempt_count(task_id).await?;
        let remaining = i64::from(max_retries) + 1 - attempts;
        Ok(remaining.clamp(0, i64::from(i32::MAX)) as i32)
    }

    pub async fn cancel_task(&self, task_id: impl Into<String>) -> Result<Task> {
        self.cancel_task_as(task_id, Actor::system(SystemComponent::CancelTask))
            .await
    }

    pub async fn cancel_task_as(&self, task_id: impl Into<String>, actor: Actor) -> Result<Task> {
        let task_id = task_id.into();
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &Actor::system(SystemComponent::General),
        );
        let cancel_target = workflow
            .cancellation_state
            .as_deref()
            .unwrap_or("cancelled")
            .to_owned();
        if task.status == cancel_target {
            return Ok(task);
        }
        self.cancel_running_executions_for_task(
            &task,
            "cancelled by task cancellation",
            actor.clone(),
        )
        .await?;
        let result = self
            .transition(
                task_id,
                cancel_target,
                TransitionOptions {
                    version: task.version,
                    reason: Some("cancel task".to_owned()),
                    triggered_by: actor,
                    rejection: false,
                    defer_dispatch_seconds: None,
                },
            )
            .await?;
        let task = clear_manual_advance_error_annotation(&self.db, &task, result.task).await?;
        Ok(task)
    }

    pub async fn advance_to_next_state(&self, task_id: impl Into<String>) -> Result<Task> {
        let task_id = task_id.into();
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &Actor::user(UserActionSource::ManualAdvance),
        );
        let target = next_workflow_state(&workflow, &task.status)?;

        self.cancel_running_executions_for_manual_advance(&task)
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
        let result = engine
            .manual_override_transition(
                &task_id,
                &target,
                task.version,
                &workflow,
                Actor::user(UserActionSource::ManualAdvance),
                "manual advance",
                false,
            )
            .await?;
        let task = clear_manual_advance_error_annotation(&self.db, &task, result.task).await?;
        Ok(task)
    }

    pub async fn soft_delete(&self, task_id: impl Into<String>) -> Result<Task> {
        let task_id = task_id.into();
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, true)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &Actor::system(SystemComponent::General),
        );
        if matches!(
            workflow.state_kind(&task.status),
            Some(api_types::StateKind::Active | api_types::StateKind::Gate)
        ) {
            return Err(ServiceError::invalid_operation(
                "tasks can only be deleted from inactive states",
            ));
        }

        let deleted = TaskRepo::soft_delete(
            &*self.db,
            SoftDeleteTask {
                id: task_id,
                expected_version: task.version,
                deleted_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await?;

        self.publish(ForgeEvent {
            event_type: "task.deleted".to_owned(),
            entity_id: deleted.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskDeleted {
                project_id: deleted.project_id.clone(),
            },
        });

        Ok(deleted)
    }

    pub async fn archive_task(&self, task_id: impl Into<String>) -> Result<Task> {
        let task_id = task_id.into();
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, true)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let now = now_rfc3339();
        let archived = TaskRepo::archive(
            &*self.db,
            ArchiveTask {
                id: task_id,
                expected_version: task.version,
                archived_at: now.clone(),
                updated_at: now,
            },
        )
        .await?;

        self.publish(ForgeEvent {
            event_type: "task.archived".to_owned(),
            entity_id: archived.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskUpdated {
                project_id: archived.project_id.clone(),
            },
        });

        Ok(archived)
    }
}

impl TaskService {
    pub(super) async fn cancel_active_execution_for_user_transition(
        &self,
        task: &Task,
        target_status: &str,
        workflow: &api_types::WorkflowDefinition,
        actor: &Actor,
    ) -> Result<()> {
        if !actor.is_user() {
            return Ok(());
        }

        if task.status == target_status {
            return Ok(());
        }

        if !workflow
            .states
            .iter()
            .any(|state| state.name.as_str() == target_status)
        {
            return Ok(());
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
        for execution in page
            .items
            .into_iter()
            .filter(|execution| execution.status == ExecutionStatus::Running)
        {
            self.cancel_active_execution(
                &execution,
                "cancelled by user transition",
                db::StopReason::UserCancelled,
                actor,
                db::ResumePolicy::None,
            )
            .await?;
        }
        Ok(())
    }

    async fn cancel_running_executions_for_manual_advance(&self, task: &Task) -> Result<()> {
        self.cancel_running_executions_for_task(
            task,
            "cancelled by manual advance",
            Actor::user(UserActionSource::ManualAdvance),
        )
        .await
    }

    async fn cancel_running_executions_for_task(
        &self,
        task: &Task,
        reason: &str,
        actor: Actor,
    ) -> Result<()> {
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
        for execution in page
            .items
            .into_iter()
            .filter(|execution| execution.status == ExecutionStatus::Running)
        {
            self.cancel_active_execution(
                &execution,
                reason,
                db::StopReason::TaskCancelled,
                &actor,
                db::ResumePolicy::None,
            )
            .await?;
        }
        Ok(())
    }
}

fn next_workflow_state(
    workflow: &api_types::WorkflowDefinition,
    current_status: &str,
) -> Result<String> {
    let current_index = workflow
        .states
        .iter()
        .position(|state| state.name == current_status)
        .ok_or_else(|| {
            ServiceError::invalid_operation(WorkflowEngine::undefined_state_message(
                current_status,
                workflow,
            ))
        })?;
    let cancellation_state = workflow.cancellation_state.as_deref();
    let reject_target = workflow.gate_reject_target(current_status);
    let candidates = workflow
        .outgoing_trigger_targets(current_status)
        .filter(|(_, target)| {
            target != current_status
                && cancellation_state != Some(target.as_str())
                && reject_target != Some(target.as_str())
        })
        .collect::<Vec<_>>();

    candidates
        .iter()
        .filter_map(|(_, target)| {
            workflow
                .states
                .iter()
                .position(|state| state.name == *target)
                .filter(|target_index| *target_index > current_index)
                .map(|target_index| (target.clone(), target_index))
        })
        .min_by_key(|(_, target_index)| *target_index)
        .map(|(target, _)| target)
        .or_else(|| candidates.first().map(|(_, target)| target.clone()))
        .ok_or_else(|| {
            ServiceError::invalid_operation(format!(
                "state '{current_status}' has no next workflow transition"
            ))
        })
}

async fn clear_manual_advance_error_annotation(
    db: &SqliteDb,
    source_task: &Task,
    advanced_task: Task,
) -> Result<Task> {
    if source_task.error_annotation.is_none()
        || source_task.error_annotation != advanced_task.error_annotation
    {
        return Ok(advanced_task);
    }

    TaskRepo::update(
        db,
        UpdateTask {
            id: advanced_task.id.clone(),
            expected_version: advanced_task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .map_err(Into::into)
}

pub(super) async fn clear_manual_review_awaiting_metadata(
    db: &SqliteDb,
    task: &Task,
) -> Result<Task> {
    let current = TaskRepo::get_by_id(db, &task.id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
    let mut metadata = TaskMetadata::parse(current.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    if metadata
        .extra
        .get("awaiting_human_reason")
        .and_then(Value::as_str)
        != Some("manual_review")
    {
        return Ok(current);
    }

    metadata.extra.remove("awaiting_human");
    metadata.extra.remove("awaiting_human_reason");
    TaskRepo::set_metadata_json(db, &task.id, metadata.to_json(), &now_rfc3339()).await?;
    TaskRepo::get_by_id(db, &task.id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))
}

pub(super) fn should_clear_transient_error_annotation(task: &Task) -> bool {
    if task.status.as_str() == default_states::MERGE_FAILED {
        return false;
    }

    task.error_annotation
        .as_deref()
        .is_some_and(is_transient_error_annotation)
}

pub(super) fn should_clear_review_passed_at(
    workflow: &api_types::WorkflowDefinition,
    from: &str,
    to: &str,
    rejection: bool,
    actor: &Actor,
) -> bool {
    let from_kind = workflow.state_kind(from);
    let to_kind = workflow.state_kind(to);

    if matches!(from_kind, Some(api_types::StateKind::Gate)) && rejection {
        return true;
    }
    if matches!(from_kind, Some(api_types::StateKind::Custom))
        && matches!(
            to_kind,
            Some(api_types::StateKind::Initial | api_types::StateKind::Active)
        )
    {
        return true;
    }
    if actor.is_user() {
        let from_is_work = matches!(
            from_kind,
            Some(api_types::StateKind::Active | api_types::StateKind::Gate)
        );
        let to_is_work = matches!(
            to_kind,
            Some(api_types::StateKind::Active | api_types::StateKind::Gate)
        );
        return from_is_work && !to_is_work;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{default_states, default_workflow::default_workflow};

    #[test]
    fn manual_advance_uses_forward_workflow_state() {
        let workflow = default_workflow();

        assert_eq!(
            next_workflow_state(&workflow, default_states::TODO).unwrap(),
            default_states::PLANNING
        );
        assert_eq!(
            next_workflow_state(&workflow, default_states::IN_PROGRESS).unwrap(),
            default_states::REVIEW
        );
        assert_eq!(
            next_workflow_state(&workflow, default_states::REVIEW).unwrap(),
            default_states::MERGING
        );
        assert_eq!(
            next_workflow_state(&workflow, default_states::MERGING).unwrap(),
            default_states::DONE
        );
    }

    #[test]
    fn executor_failure_annotations_are_transient() {
        let annotation = serde_json::json!({
            "type": "executor_failed",
            "blocking_reason": "executor_failed",
            "message": "executor stopped before workflow could continue"
        });

        assert!(is_transient_error_annotation(&annotation.to_string()));

        let target_dirty = serde_json::json!({
            "type": "target_repo_dirty",
            "message": "target repository has uncommitted changes"
        });

        assert!(is_transient_error_annotation(&target_dirty.to_string()));
    }
}
