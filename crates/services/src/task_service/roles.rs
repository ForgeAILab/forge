use super::*;
use events::RoleAssignmentSnapshot;
use sqlx::{Row, Sqlite, Transaction};

impl TaskService {
    pub async fn on_agent_deleted(&self, agent_id: &str) -> Result<()> {
        validate_required("agent_id", agent_id)?;
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let events = self
            .on_agent_deleted_in_tx(&mut transaction, agent_id)
            .await?;
        transaction.commit().await?;
        self.publish_role_sweep_events(events);
        Ok(())
    }

    pub(crate) async fn on_agent_deleted_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        agent_id: &str,
    ) -> Result<Vec<RoleSweepEvent>> {
        validate_required("agent_id", agent_id)?;
        let rows = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at FROM task_role_assignment WHERE assignee_type = 'agent' AND assignee_id = ? ORDER BY task_id, role_name",
        )
        .bind(agent_id)
        .fetch_all(&mut **transaction)
        .await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let previous_assignment = TaskRoleAssignment {
                id: row.try_get("id")?,
                task_id: row.try_get("task_id")?,
                role_name: row.try_get("role_name")?,
                assignee_type: Some(AssigneeKind::Agent),
                assignee_id: row.try_get("assignee_id")?,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            };
            let mut new_assignment = previous_assignment.clone();
            new_assignment.assignee_id = None;
            events.push(RoleSweepEvent {
                task_id: previous_assignment.task_id.clone(),
                role_name: previous_assignment.role_name.clone(),
                previous_assignment,
                new_assignment,
            });
        }

        sqlx::query(
            "UPDATE task_role_assignment SET assignee_id = NULL, updated_at = ? WHERE assignee_type = 'agent' AND assignee_id = ?",
        )
        .bind(now_rfc3339())
        .bind(agent_id)
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            "UPDATE task SET assignee_id = NULL, updated_at = ? WHERE assignee_type = 'agent' AND assignee_id = ?",
        )
        .bind(now_rfc3339())
        .bind(agent_id)
        .execute(&mut **transaction)
        .await?;

        Ok(events)
    }

    pub(crate) fn publish_role_sweep_events(&self, events: Vec<RoleSweepEvent>) {
        for event in events {
            self.publish_role_reassigned(
                &event.task_id,
                &event.role_name,
                Some(&event.previous_assignment),
                Some(&event.new_assignment),
                RoleReassignmentEventFlags::default(),
            );
        }
    }

    pub async fn coder_assignment(&self, task_id: &str) -> Result<Option<TaskRoleAssignment>> {
        validate_required("task_id", task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let Some(role_name) = self.active_work_role(&task).await? else {
            return Ok(None);
        };
        TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, task_id, &role_name)
            .await
            .map_err(Into::into)
    }

    pub async fn reassign_role(
        &self,
        input: CreateTaskRoleAssignment,
        reset_workspace: bool,
        reset_worktree: bool,
    ) -> Result<TaskRoleAssignment> {
        let task = self.validate_reassignable_task(&input.task_id).await?;
        self.enforce_mode_specific_role_guards(&task, &input.role_name, Some(&input))
            .await?;
        if input.assignee_type == Some(AssigneeKind::Agent) {
            if let Some(identity_id) = input.assignee_id.as_deref() {
                crate::ensure_execution_role_principal(
                    &self.db,
                    &task.project_id,
                    &input.role_name,
                    identity_id,
                )
                .await?;
            }
        }
        let previous = TaskRoleAssignmentRepo::get_by_task_and_role(
            &*self.db,
            &input.task_id,
            &input.role_name,
        )
        .await?;
        if same_assignment(previous.as_ref(), Some(&input)) {
            let assignment = TaskRoleAssignmentRepo::assign(&*self.db, input).await?;
            crate::wake_task_dispatch(
                &self.db,
                &assignment.task_id,
                "task role assignment confirmed",
            )
            .await?;
            return Ok(assignment);
        }

        let is_coder_role =
            self.active_work_role(&task).await?.as_deref() == Some(input.role_name.as_str());

        if !is_coder_role {
            let assignment = TaskRoleAssignmentRepo::assign(&*self.db, input).await?;
            self.publish_role_reassigned(
                &assignment.task_id,
                &assignment.role_name,
                previous.as_ref(),
                Some(&assignment),
                RoleReassignmentEventFlags::default(),
            );
            crate::wake_task_dispatch(
                &self.db,
                &assignment.task_id,
                "task role assignment changed",
            )
            .await?;
            return Ok(assignment);
        }

        let active_execution = self
            .active_execution_for_role(&task, &input.role_name)
            .await?;
        let Some(active_execution) = active_execution else {
            let assignment = TaskRoleAssignmentRepo::assign(&*self.db, input).await?;
            TaskRepo::set_review_passed_at(&*self.db, &assignment.task_id, None, &now_rfc3339())
                .await?;
            self.publish_role_reassigned(
                &assignment.task_id,
                &assignment.role_name,
                previous.as_ref(),
                Some(&assignment),
                RoleReassignmentEventFlags::default(),
            );
            crate::wake_task_dispatch(
                &self.db,
                &assignment.task_id,
                "task role assignment changed",
            )
            .await?;
            return Ok(assignment);
        };

        self.cancel_active_execution(
            &active_execution,
            "cancelled by role reassignment",
            db::StopReason::RoleReassigned,
            &api_types::Actor::user(api_types::UserActionSource::RoleReassignment),
            db::ResumePolicy::None,
        )
        .await?;

        let assignment = TaskRoleAssignmentRepo::assign(&*self.db, input).await?;
        TaskRepo::set_review_passed_at(&*self.db, &assignment.task_id, None, &now_rfc3339())
            .await?;

        let workflow = self.workflow_for_task(&task).await?;
        let initial_state = workflow_initial_state(&workflow)?;
        self.workflow_engine()
            .reset_to_initial(
                &task.id,
                &initial_state,
                task.version,
                &workflow,
                &api_types::Actor::user(api_types::UserActionSource::Reassignment),
                "coder reassigned",
            )
            .await?;

        let (effective_reset_workspace, effective_reset_worktree) = self
            .apply_reassignment_reset(&task, &active_execution, reset_workspace, reset_worktree)
            .await?;

        self.publish_role_reassigned(
            &assignment.task_id,
            &assignment.role_name,
            previous.as_ref(),
            Some(&assignment),
            RoleReassignmentEventFlags {
                triggered_cancellation: true,
                reset_workspace: effective_reset_workspace,
                reset_worktree: effective_reset_worktree,
                transitioned_to_todo: true,
            },
        );
        crate::wake_task_dispatch(
            &self.db,
            &assignment.task_id,
            "task role assignment changed",
        )
        .await?;
        Ok(assignment)
    }

    pub async fn remove_role(
        &self,
        task_id: &str,
        role_name: &str,
        reset_workspace: bool,
        reset_worktree: bool,
    ) -> Result<()> {
        let task = self.validate_reassignable_task(task_id).await?;
        self.enforce_mode_specific_role_guards(&task, role_name, None)
            .await?;
        let previous =
            TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, task_id, role_name).await?;
        let Some(previous) = previous else {
            return Ok(());
        };

        let is_coder_role = self.active_work_role(&task).await?.as_deref() == Some(role_name);
        if !is_coder_role {
            TaskRoleAssignmentRepo::remove(&*self.db, task_id, role_name).await?;
            self.publish_role_reassigned(
                task_id,
                role_name,
                Some(&previous),
                None,
                RoleReassignmentEventFlags::default(),
            );
            crate::wake_task_dispatch(&self.db, task_id, "task role assignment removed").await?;
            return Ok(());
        }

        let active_execution = self.active_execution_for_role(&task, role_name).await?;
        let Some(active_execution) = active_execution else {
            TaskRoleAssignmentRepo::remove(&*self.db, task_id, role_name).await?;
            TaskRepo::set_review_passed_at(&*self.db, task_id, None, &now_rfc3339()).await?;
            self.publish_role_reassigned(
                task_id,
                role_name,
                Some(&previous),
                None,
                RoleReassignmentEventFlags::default(),
            );
            crate::wake_task_dispatch(&self.db, task_id, "task role assignment removed").await?;
            return Ok(());
        };

        self.cancel_active_execution(
            &active_execution,
            "cancelled by role reassignment",
            db::StopReason::RoleReassigned,
            &api_types::Actor::user(api_types::UserActionSource::RoleReassignment),
            db::ResumePolicy::None,
        )
        .await?;
        TaskRoleAssignmentRepo::remove(&*self.db, task_id, role_name).await?;
        TaskRepo::set_review_passed_at(&*self.db, task_id, None, &now_rfc3339()).await?;

        let workflow = self.workflow_for_task(&task).await?;
        let initial_state = workflow_initial_state(&workflow)?;
        self.workflow_engine()
            .reset_to_initial(
                &task.id,
                &initial_state,
                task.version,
                &workflow,
                &api_types::Actor::user(api_types::UserActionSource::Reassignment),
                "coder reassigned",
            )
            .await?;

        let (effective_reset_workspace, effective_reset_worktree) = self
            .apply_reassignment_reset(&task, &active_execution, reset_workspace, reset_worktree)
            .await?;

        self.publish_role_reassigned(
            task_id,
            role_name,
            Some(&previous),
            None,
            RoleReassignmentEventFlags {
                triggered_cancellation: true,
                reset_workspace: effective_reset_workspace,
                reset_worktree: effective_reset_worktree,
                transitioned_to_todo: true,
            },
        );
        crate::wake_task_dispatch(&self.db, task_id, "task role assignment removed").await?;
        Ok(())
    }

    async fn validate_reassignable_task(&self, task_id: &str) -> Result<Task> {
        validate_required("task_id", task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
        if workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .is_some_and(|state| state.kind == api_types::StateKind::Terminal)
        {
            return Err(ServiceError::invalid_operation(format!(
                "task {} is in terminal state {}; cannot reassign role",
                task.id, task.status
            )));
        }
        Ok(task)
    }

    async fn workflow_for_task(&self, task: &Task) -> Result<api_types::WorkflowDefinition> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        Ok(WorkflowEngine::resolve_workflow(
            &project.workflow_definition,
        ))
    }

    async fn enforce_mode_specific_role_guards(
        &self,
        task: &Task,
        role_name: &str,
        _incoming: Option<&CreateTaskRoleAssignment>,
    ) -> Result<RoleGuardAction> {
        if role_name != "coder" {
            return Ok(RoleGuardAction::Continue);
        }

        if task.parent_task_id.is_some() {
            return Err(ServiceError::invalid_operation(
                "subtask coder assignment is managed by the root task",
            ));
        }

        if TaskRepo::list_subtasks_ordered(&*self.db, &task.id)
            .await?
            .iter()
            .any(|s| {
                s.status != default_states::TODO
                    && s.status != default_states::DONE
                    && s.status != default_states::CANCELLED
            })
        {
            return Err(ServiceError::task_sequence_already_started(task.id.clone()));
        }

        Ok(RoleGuardAction::Continue)
    }

    async fn active_work_role(&self, task: &Task) -> Result<Option<String>> {
        let workflow = self.workflow_for_task(task).await?;
        Ok(workflow
            .states
            .iter()
            .find(|state| state.name == default_states::IN_PROGRESS)
            .and_then(crate::workflow::effective_role)
            .map(str::to_owned)
            .or_else(|| {
                workflow
                    .states
                    .iter()
                    .find(|state| state.kind == api_types::StateKind::Active)
                    .and_then(crate::workflow::effective_role)
                    .map(str::to_owned)
            }))
    }

    async fn active_execution_for_role(
        &self,
        task: &Task,
        role_name: &str,
    ) -> Result<Option<Execution>> {
        if self.role_for_state(task, &task.status).await?.as_deref() != Some(role_name) {
            return Ok(None);
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
        Ok(page.items.into_iter().find(|execution| {
            execution.status == ExecutionStatus::Running
                && (execution.role == role_name || execution.role == "executor")
        }))
    }

    pub(super) async fn cancel_active_execution(
        &self,
        execution: &Execution,
        reason: &str,
        stop_reason: db::StopReason,
        actor: &api_types::Actor,
        resume_policy: db::ResumePolicy,
    ) -> Result<bool> {
        let reconciliation_reason = match &stop_reason {
            db::StopReason::RoleReassigned => "stopped because task moved".to_owned(),
            _ => reason.to_owned(),
        };
        let preserve_resume_context = resume_policy == db::ResumePolicy::Manual;

        // Terminalization is the authority boundary.  Claiming the terminal
        // state before touching the provider is important: a stale role/task
        // caller must not cancel a newer owner or perform any live/runtime
        // side effect after losing the execution CAS.
        let actor_type = if actor.is_user() {
            "user".to_owned()
        } else if actor.is_agent() {
            "agent".to_owned()
        } else {
            "system".to_owned()
        };
        let mut terminal_candidate = execution.clone();
        let mut outcome = None;
        for _ in 0..3 {
            let terminalized_at = now_rfc3339();
            let attempt = ExecutionRepo::terminalize(
                &*self.db,
                TerminalizeExecution {
                    execution_id: terminal_candidate.id.clone(),
                    expected_version: terminal_candidate.execution_version,
                    lease_owner: terminal_candidate.lease_owner.clone(),
                    status: ExecutionStatus::Cancelled,
                    stop_reason: Some(Some(stop_reason.clone())),
                    stopped_by: Some(Some(actor.display())),
                    stopped_at: Some(Some(terminalized_at.clone())),
                    resume_policy: Some(Some(resume_policy.clone())),
                    agent_session_id: (!preserve_resume_context).then_some(None),
                    agent_message_id: None,
                    last_activity_at: None,
                    last_progress_at: None,
                    summary: None,
                    logs_path: None,
                    before_sha: None,
                    after_sha: None,
                    error: Some(Some(reason.to_owned())),
                    executor_config_snapshot_json: (!preserve_resume_context).then_some(None),
                    updated_at: terminalized_at,
                    actor_type: actor_type.clone(),
                    actor_id: None,
                    correlation_id: Some(terminal_candidate.id.clone()),
                    causation_id: None,
                    causation_depth: 0,
                    lease_disposition: ExecutionLeaseDisposition::Revoke,
                },
            )
            .await
            .map_err(|error| ServiceError::invalid_operation(format!("cancel failed: {error}")))?;
            match attempt {
                ExecutionTerminalOutcome::Concurrent {
                    current: Some(current),
                } if current.status == ExecutionStatus::Running => {
                    // Heartbeats/progress also advance execution_version.  A
                    // cancellation that loses only to that liveness CAS may
                    // retry against the fresh owner/version; terminal rows
                    // are returned as ordinary concurrent losers below.
                    terminal_candidate = current;
                }
                other => {
                    outcome = Some(other);
                    break;
                }
            }
        }
        let outcome = outcome.unwrap_or(ExecutionTerminalOutcome::Concurrent {
            current: Some(terminal_candidate),
        });

        let committed_execution = match outcome {
            ExecutionTerminalOutcome::Committed { execution, .. } => execution,
            ExecutionTerminalOutcome::Concurrent { current } => {
                tracing::debug!(
                    execution_id = %execution.id,
                    current_status = ?current.as_ref().map(|execution| &execution.status),
                    "skipping stale execution cancellation after terminal CAS loss"
                );
                return Ok(false);
            }
        };

        // The provider is only contacted after this caller has won the
        // execution CAS.  A provider-side failure is safe to recover from:
        // durable execution/event/lease truth is already committed.
        if self.daemon_connections.is_some() || self.task_executor.is_some() {
            if let Err(error) = self
                .cancel_execution_with_provider(&committed_execution, reason)
                .await
            {
                tracing::warn!(
                    execution_id = %execution.id,
                    %error,
                    "executor cancellation failed after durable cancellation"
                );
            }
        }
        self.publish(ForgeEvent {
            event_type: "task.execution_cancelled".to_owned(),
            entity_id: committed_execution.task_id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskExecutionCancelled {
                task_id: committed_execution.task_id.clone(),
                execution_id: committed_execution.id.clone(),
                reason: reason.to_owned(),
            },
        });
        self.publish(ForgeEvent {
            event_type: "reconciliation.event".to_owned(),
            entity_id: committed_execution.task_id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReconciliationEvent {
                task_id: Some(committed_execution.task_id.clone()),
                execution_id: Some(committed_execution.id.clone()),
                reason: reconciliation_reason,
            },
        });
        Ok(true)
    }

    async fn apply_reassignment_reset(
        &self,
        task: &Task,
        execution: &Execution,
        reset_workspace: bool,
        reset_worktree: bool,
    ) -> Result<(bool, bool)> {
        if reset_workspace {
            let mut workspace_id = execution.workspace_id.clone();
            if workspace_id.is_none() {
                workspace_id = WorkspaceRepo::get_by_task_id(&*self.db, &task.id)
                    .await?
                    .map(|workspace| workspace.id);
            }
            if let (Some(cleanup_scheduler), Some(workspace_id)) =
                (self.cleanup_scheduler.as_ref(), workspace_id)
            {
                cleanup_scheduler.cleanup_now(workspace_id).await?;
                return Ok((true, false));
            }
            return Ok((false, false));
        }

        if reset_worktree {
            let repo_id = task
                .repo_id
                .as_deref()
                .ok_or_else(|| ServiceError::invalid_operation("task has no associated repo"))?;
            let repo = RepoRepo::get_by_id(&*self.db, repo_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("repo", repo_id.to_owned()))?;
            let repo_url = repo
                .local_path
                .filter(|path| !path.trim().is_empty())
                .unwrap_or(repo.remote_url);
            let repo_name = reassignment_repo_name(&repo_url);
            let workspace_root = self
                .cleanup_scheduler
                .as_ref()
                .map(|scheduler| scheduler.workspace_root().to_path_buf())
                .unwrap_or_else(|| self.workspace_root.clone());
            let manager = WorkspaceManager::new(workspace_root);
            manager
                .reset_worktree(&task.id, &repo_name)
                .await
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
            return Ok((false, true));
        }

        Ok((false, false))
    }

    fn workflow_engine(&self) -> WorkflowEngine {
        WorkflowEngine {
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
        }
    }

    fn publish_role_reassigned(
        &self,
        task_id: &str,
        role_name: &str,
        previous_assignment: Option<&TaskRoleAssignment>,
        new_assignment: Option<&TaskRoleAssignment>,
        flags: RoleReassignmentEventFlags,
    ) {
        self.publish(ForgeEvent {
            event_type: "task.role_reassigned".to_owned(),
            entity_id: task_id.to_owned(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRoleReassigned {
                task_id: task_id.to_owned(),
                role_name: role_name.to_owned(),
                previous_assignment: previous_assignment.map(snapshot),
                new_assignment: new_assignment.map(snapshot),
                triggered_cancellation: flags.triggered_cancellation,
                reset_workspace: flags.reset_workspace,
                reset_worktree: flags.reset_worktree,
                transitioned_to_todo: flags.transitioned_to_todo,
            },
        });
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct RoleReassignmentEventFlags {
    triggered_cancellation: bool,
    reset_workspace: bool,
    reset_worktree: bool,
    transitioned_to_todo: bool,
}

enum RoleGuardAction {
    Continue,
}

pub(crate) struct RoleSweepEvent {
    pub(crate) task_id: String,
    pub(crate) role_name: String,
    pub(crate) previous_assignment: TaskRoleAssignment,
    pub(crate) new_assignment: TaskRoleAssignment,
}

fn same_assignment(
    previous: Option<&TaskRoleAssignment>,
    next: Option<&CreateTaskRoleAssignment>,
) -> bool {
    match (previous, next) {
        (Some(previous), Some(next)) => {
            previous.assignee_type == next.assignee_type && previous.assignee_id == next.assignee_id
        }
        (None, None) => true,
        _ => false,
    }
}

fn snapshot(assignment: &TaskRoleAssignment) -> RoleAssignmentSnapshot {
    RoleAssignmentSnapshot {
        assignee_type: assignment.assignee_type.as_ref().map(ToString::to_string),
        assignee_id: assignment.assignee_id.clone(),
    }
}

fn workflow_initial_state(workflow: &api_types::WorkflowDefinition) -> Result<String> {
    workflow
        .states
        .iter()
        .find(|state| state.kind == api_types::StateKind::Initial)
        .map(|state| state.name.clone())
        .ok_or_else(|| ServiceError::invalid_operation("workflow has no initial state"))
}

fn reassignment_repo_name(repo_url: &str) -> String {
    let trimmed = repo_url.trim_end_matches(['/', '\\']);
    let last_component = trimmed
        .rsplit(['/', '\\'])
        .next()
        .filter(|component| !component.is_empty())
        .unwrap_or("repo");

    last_component
        .strip_suffix(".git")
        .unwrap_or(last_component)
        .to_owned()
}
