use super::*;

use api_types::{Actor, StateKind, TaskAction, UserActionSource, WorkflowTrigger};
use db::{
    Agent, AgentListQuery, AgentRepo, AssigneeKind, ExecutionRepo, PageRequest,
    ProjectAgentBindingRepo, ProjectRepo, ReviewRepo, ReviewStatus, SortBy, SortOrder, TaskRepo,
    TaskRoleAssignmentRepo,
};

/// Whether `task` is parked at a review gate only a user may decide.
///
/// The Project Agent's `task.review` action and the review-ready attention
/// wake both hinge on this: a review run by the workflow's reviewer Agent
/// settles itself, so neither a decision nor a wake is anyone's business.
#[must_use]
pub fn task_review_requires_user_decision(
    task: &Task,
    workflow: &api_types::WorkflowDefinition,
) -> bool {
    workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .is_some_and(|state| {
            state.canonical_phase == Some(api_types::CanonicalPhase::Review)
                && state
                    .gate_config
                    .as_ref()
                    .is_some_and(|gate| gate.requires_user_approval())
        })
}

#[derive(Debug)]
pub struct TaskActionResult {
    pub task: Task,
    pub action: TaskAction,
}

impl TaskService {
    /// Cancel a healthy or stopped non-terminal Task through the Project's
    /// currently bound Project Agent. The Project is host-derived, so a Task
    /// from another Project is indistinguishable from a missing Task here.
    pub async fn perform_project_agent_cancel(
        &self,
        project_id: &str,
        task_id: impl Into<String>,
        reason: String,
        expected_task_version: i64,
        project_agent_identity_id: &str,
    ) -> Result<TaskActionResult> {
        let task_id = task_id.into();
        validate_required("task_id", &task_id)?;
        let reason = reason.trim();
        validate_required("reason", reason)?;
        let binding = ProjectAgentBindingRepo::get_active_project_binding(&*self.db, project_id)
            .await?
            .filter(|binding| {
                binding.state == "active"
                    && binding.identity_id.as_deref() == Some(project_agent_identity_id)
            })
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Task cancellation requires the Project's currently configured Project Agent",
                )
            })?;
        debug_assert_eq!(
            binding.identity_id.as_deref(),
            Some(project_agent_identity_id)
        );
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .filter(|task| task.project_id == project_id)
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        let actor = Actor::agent(project_agent_identity_id);
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);

        // An exact retry after a response loss should observe the requested
        // end state as success even though cancellation incremented version.
        if cancellation_target(&workflow).is_some_and(|target| target == task.status) {
            return Ok(TaskActionResult {
                task,
                action: TaskAction::Cancel,
            });
        }

        self.perform_task_action_as(
            task_id,
            TaskAction::Cancel,
            Some(reason.to_owned()),
            Some(expected_task_version),
            actor,
        )
        .await
    }

    /// Accept or reject a human-required review through the Project's bound
    /// Project Agent. The caller supplies the server-derived Project scope;
    /// this method rejects cross-Project Tasks before exposing their state.
    pub async fn perform_project_agent_review(
        &self,
        project_id: &str,
        task_id: impl Into<String>,
        accept: bool,
        reason: Option<String>,
        expected_task_version: i64,
        project_agent_identity_id: &str,
    ) -> Result<TaskActionResult> {
        let task_id = task_id.into();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .filter(|task| task.project_id == project_id)
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        let binding = ProjectAgentBindingRepo::get_active_project_binding(&*self.db, project_id)
            .await?
            .filter(|binding| {
                binding.state == "active"
                    && binding.identity_id.as_deref() == Some(project_agent_identity_id)
            })
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Task review requires the Project's currently configured Project Agent",
                )
            })?;
        debug_assert_eq!(
            binding.identity_id.as_deref(),
            Some(project_agent_identity_id)
        );
        let actor = Actor::agent(project_agent_identity_id);
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);
        if !task_review_requires_user_decision(&task, &workflow) {
            return Err(ServiceError::invalid_operation(
                "Task is not waiting for a human-required review decision",
            ));
        }
        self.perform_task_action_as(
            task_id,
            if accept {
                TaskAction::Approve
            } else {
                TaskAction::RequestChanges
            },
            reason,
            Some(expected_task_version),
            actor,
        )
        .await
    }

    /// Return intent actions from the resolved workflow capabilities and the
    /// task's current execution/review state. Callers do not need to know the
    /// project's concrete state names.
    pub async fn available_task_actions(
        &self,
        task_id: impl Into<String>,
    ) -> Result<Vec<TaskAction>> {
        let task_id = task_id.into();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let actor = Actor::user(UserActionSource::Api);
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);
        self.available_task_actions_for(&task, &workflow).await
    }

    pub async fn perform_task_action(
        &self,
        task_id: impl Into<String>,
        action: TaskAction,
        reason: Option<String>,
        requested_version: Option<i64>,
    ) -> Result<TaskActionResult> {
        self.perform_task_action_as(
            task_id,
            action,
            reason,
            requested_version,
            Actor::user(UserActionSource::Api),
        )
        .await
    }

    /// Execute a Task action with an already-authorized canonical actor. This
    /// is used by the bound Project Agent's scoped review action; callers must
    /// authorize Project ownership before invoking it.
    pub async fn perform_task_action_as(
        &self,
        task_id: impl Into<String>,
        action: TaskAction,
        reason: Option<String>,
        requested_version: Option<i64>,
        actor: Actor,
    ) -> Result<TaskActionResult> {
        let task_id = task_id.into();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);
        // A stale client version is a conflict for every action, not only the ones whose
        // inner path happens to re-check it.
        if let Some(version) = requested_version {
            if version != task.version {
                return Err(ServiceError::Db(db::DbError::TaskVersionConflict {
                    expected: version,
                    actual: task.version,
                }));
            }
        }

        // Cancelling an already-cancelled task stays an idempotent no-op, matching the
        // pre-facade POST /tasks/{id}/cancel contract. cancellation_target falls back to a
        // terminal "cancelled" state for workflows with no explicit cancellation_state.
        if action == TaskAction::Cancel
            && cancellation_target(&workflow).is_some_and(|cancelled| cancelled == task.status)
        {
            return Ok(TaskActionResult { task, action });
        }

        let available = self.available_task_actions_for(&task, &workflow).await?;
        if !available.contains(&action) {
            return Err(ServiceError::TaskActionUnavailable {
                available_actions: available,
                reason: unavailable_reason(action, &task, &workflow),
            });
        }

        let transition_version = requested_version.unwrap_or(task.version);
        let transition_reason = reason
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("task action: {}", action_name(action)));

        let result = match action {
            TaskAction::Start => {
                let agent_id = self.action_agent_id(&task, &workflow).await?;
                self.claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
                    .await?
                    .task
            }
            TaskAction::Pause => {
                let execution =
                    self.latest_running_execution(&task.id)
                        .await?
                        .ok_or_else(|| ServiceError::TaskActionUnavailable {
                            available_actions: Vec::new(),
                            reason: "task has no running execution to pause".to_owned(),
                        })?;
                self.pause_execution(
                    execution.id,
                    reason
                        .clone()
                        .unwrap_or_else(|| "paused by user".to_owned()),
                )
                .await?;
                self.create_system_comment(
                    &task.id,
                    reason
                        .map(|value| format!("Task paused by user: {value}"))
                        .unwrap_or_else(|| "Task paused by user".to_owned()),
                )
                .await?;
                TaskRepo::get_by_id(&*self.db, &task.id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?
            }
            TaskAction::Resume => {
                self.resume_task_execution(&task, &workflow, reason, transition_version)
                    .await?
            }
            TaskAction::Submit => {
                let target = trigger_target(&workflow, &task.status, WorkflowTrigger::Accept)
                    .expect("submit capability guarantees an Accept target");
                self.transition(
                    task.id.clone(),
                    target,
                    TransitionOptions {
                        version: transition_version,
                        reason: Some(transition_reason),
                        triggered_by: actor,
                        rejection: false,
                        defer_dispatch_seconds: None,
                    },
                )
                .await?
                .task
            }
            TaskAction::RequestChanges => {
                let latest_review = self.latest_review(&task.id).await?;
                if task.status == crate::workflow::default_states::REVIEW
                    && latest_review
                        .as_ref()
                        .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
                    && trigger_target(&workflow, &task.status, WorkflowTrigger::Reject).as_deref()
                        == Some(crate::workflow::default_states::IN_PROGRESS)
                {
                    self.reject_review_as(task.id.clone(), reason, actor)
                        .await?
                        .0
                } else {
                    self.transition_gate_action(
                        &task,
                        &workflow,
                        WorkflowTrigger::Reject,
                        TransitionOptions {
                            version: transition_version,
                            reason: Some(transition_reason),
                            triggered_by: actor,
                            rejection: true,
                            defer_dispatch_seconds: None,
                        },
                    )
                    .await?
                }
            }
            TaskAction::Approve => {
                let latest_review = self.latest_review(&task.id).await?;
                if task.status == crate::workflow::default_states::REVIEW
                    && latest_review
                        .as_ref()
                        .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
                    && trigger_target(&workflow, &task.status, WorkflowTrigger::Accept).as_deref()
                        == Some(crate::workflow::default_states::MERGING)
                {
                    self.approve_review_as(task.id.clone(), actor).await?.0
                } else {
                    self.transition_gate_action(
                        &task,
                        &workflow,
                        WorkflowTrigger::Accept,
                        TransitionOptions {
                            version: transition_version,
                            reason: Some(transition_reason),
                            triggered_by: actor,
                            rejection: false,
                            defer_dispatch_seconds: None,
                        },
                    )
                    .await?
                }
            }
            TaskAction::Cancel => {
                self.cancel_task_at_version_as(
                    task.id.clone(),
                    transition_version,
                    transition_reason,
                    actor,
                )
                .await?
            }
        };

        Ok(TaskActionResult {
            task: result,
            action,
        })
    }

    async fn available_task_actions_for(
        &self,
        task: &Task,
        workflow: &api_types::WorkflowDefinition,
    ) -> Result<Vec<TaskAction>> {
        let state = workflow
            .states
            .iter()
            .find(|state| state.name == task.status);
        let current_role = state.and_then(crate::workflow::effective_role);
        // This is an authority projection, not a history endpoint. The SQL
        // resolver selects the newest representative for each predicate so
        // action availability remains exact without scanning or offset-paging
        // the complete execution history.
        let executions = crate::task_service::action_resolver::list_execution_action_authority(
            &self.db,
            &task.id,
            current_role,
            None,
        )
        .await?;
        let latest_review = self.latest_review(&task.id).await?;
        let is_terminal = state.is_some_and(|state| state.kind == StateKind::Terminal);
        let running = executions
            .iter()
            .any(|execution| execution.status == ExecutionStatus::Running);
        let current_effective_role = state.and_then(crate::workflow::effective_role);
        let execution_matches_current_role = |execution: &Execution| {
            current_effective_role.is_some_and(|role| {
                execution.role == role
                    || (role == crate::workflow::default_roles::CODER
                        && execution.role == "executor")
            })
        };
        let resumable = executions.iter().any(|execution| {
            execution.status != ExecutionStatus::Running
                && execution.agent_session_id.is_some()
                && execution_matches_current_role(execution)
        });
        let has_previous_execution = executions.iter().any(|execution| {
            execution.status != ExecutionStatus::Running
                && execution.agent_id.is_some()
                && execution_matches_current_role(execution)
        });
        let has_agent = self.action_agent_id(task, workflow).await.is_ok();
        // A completed execution from an earlier workflow pass is not evidence
        // that the current active state has work ready to submit. In
        // particular, review remediation re-enters the coder state while the
        // old coder attempt remains completed. Only the newest execution for
        // the Task may satisfy the current role's submit gate.
        let latest_current_role_execution = executions
            .iter()
            .filter(|execution| execution_matches_current_role(execution))
            .max_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.id.cmp(&right.id))
            });
        let latest_state_entry = if state.and_then(crate::workflow::effective_role).is_some() {
            crate::task_service::action_resolver::latest_state_entry_authority(
                &self.db,
                &task.id,
                &task.status,
            )
            .await?
        } else {
            None
        };
        let has_completed_current_role_execution = current_effective_role.is_some_and(|role| {
            latest_current_role_execution.is_some_and(|execution| {
                execution.status == ExecutionStatus::Completed
                    && latest_state_entry
                        .as_ref()
                        .is_none_or(|entry| execution.created_at > entry.created_at)
                    && (execution.role == role
                        || (role == crate::workflow::default_roles::CODER
                            && execution.role == "executor"))
            })
        });
        // A user pause is not a failure, and `resume` is its inverse: it must
        // stay offered, or the advertised `pause` becomes a one-way door whose
        // only exits discard the paused attempt.
        let has_blocking_state = task.failed_json.is_some()
            || task.blocked_json.is_some()
            || task.error_annotation.as_deref().is_some_and(|raw| {
                matches!(
                    serde_json::from_str::<api_types::TaskAnnotation>(raw),
                    Ok(api_types::TaskAnnotation::Blocking(annotation))
                        if annotation.annotation_type != api_types::FailureKind::ManualStop
                )
            });

        let mut actions = Vec::new();
        if can_start(workflow, task)
            && has_agent
            // An unmet dependency is the Task's real reason for sitting in an
            // Initial state. Offering `start` there sends the caller into the
            // claim path, which refuses with a role-assignment conflict that
            // names neither the dependency nor anything the caller can act on.
            && db::TaskDependencyRepo::unsatisfied_dependencies(&*self.db, &task.id)
                .await?
                .is_empty()
        {
            actions.push(TaskAction::Start);
        }
        if running {
            actions.push(TaskAction::Pause);
        }
        if !is_terminal
            && !has_blocking_state
            && (resumable
                || has_previous_execution
                || (state.is_some_and(|state| state.kind == StateKind::Active) && has_agent))
        {
            actions.push(TaskAction::Resume);
        }
        if state.is_some_and(|state| state.kind == StateKind::Active)
            && !running
            && task.failed_json.is_none()
            && task.blocked_json.is_none()
            && task.error_annotation.is_none()
            && has_completed_current_role_execution
            && trigger_target(workflow, &task.status, WorkflowTrigger::Accept).is_some()
        {
            actions.push(TaskAction::Submit);
        }

        let review_waiting = state.is_some_and(|state| state.kind == StateKind::Gate)
            && latest_review
                .as_ref()
                .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman);
        let gate_requires_approval = state
            .and_then(|state| state.gate_config.as_ref())
            .is_some_and(|config| config.requires_user_approval());
        let gate_role_busy = state
            .and_then(crate::workflow::effective_role)
            .is_some_and(|role| {
                executions.iter().any(|execution| {
                    execution.status == ExecutionStatus::Running
                        && (execution.role == role
                            || (role == crate::workflow::default_roles::CODER
                                && execution.role == "executor"))
                })
            });
        let has_reject = trigger_target(workflow, &task.status, WorkflowTrigger::Reject).is_some();
        let has_accept = trigger_target(workflow, &task.status, WorkflowTrigger::Accept).is_some();
        if !gate_role_busy && (review_waiting || (gate_requires_approval && has_accept)) {
            actions.push(TaskAction::Approve);
        }
        if !gate_role_busy
            && (review_waiting
                || (state.is_some_and(|state| state.kind == StateKind::Gate) && has_reject))
        {
            actions.push(TaskAction::RequestChanges);
        }
        if !is_terminal && cancellation_target(workflow).is_some() {
            actions.push(TaskAction::Cancel);
        }
        Ok(actions)
    }

    async fn transition_gate_action(
        &self,
        task: &Task,
        workflow: &api_types::WorkflowDefinition,
        trigger: WorkflowTrigger,
        options: TransitionOptions,
    ) -> Result<Task> {
        let target = trigger_target(workflow, &task.status, trigger).ok_or_else(|| {
            ServiceError::invalid_operation(format!(
                "state '{}' has no {} target",
                task.status,
                action_name_for_trigger(trigger),
            ))
        })?;
        Ok(self
            .transition(task.id.clone(), target, options)
            .await?
            .task)
    }

    async fn resume_task_execution(
        &self,
        task: &Task,
        workflow: &api_types::WorkflowDefinition,
        reason: Option<String>,
        expected_version: i64,
    ) -> Result<Task> {
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        if current.version != expected_version {
            return Err(ServiceError::Db(db::DbError::TaskVersionConflict {
                expected: expected_version,
                actual: current.version,
            }));
        }
        if current.failed_json.is_some() {
            return Err(ServiceError::TaskActionUnavailable {
                available_actions: Vec::new(),
                reason: "failed tasks cannot be resumed; restart or cancel required".to_owned(),
            });
        }
        let context = reason.filter(|value| !value.trim().is_empty());
        let blocking_annotation = task
            .error_annotation
            .as_deref()
            .and_then(|raw| serde_json::from_str::<api_types::TaskAnnotation>(raw).ok())
            .and_then(|annotation| match annotation {
                api_types::TaskAnnotation::Blocking(annotation) => Some(annotation),
                api_types::TaskAnnotation::Legacy(_) => None,
            });
        if let Some(annotation) = blocking_annotation.as_ref() {
            if annotation
                .recovery_actions
                .contains(&api_types::RecoveryAction::ResumeSession)
            {
                match self
                    .recover_task_at_version(
                        task.id.clone(),
                        api_types::RecoveryAction::ResumeSession,
                        Some("resumed by user".to_owned()),
                        context.clone(),
                        expected_version,
                    )
                    .await
                {
                    Ok(task) => return Ok(task),
                    Err(ServiceError::InvalidOperation { message })
                        if message.contains("no resumable session") => {}
                    Err(error) => return Err(error),
                }
            }
        }

        // Resume means "carry on with the work this Task is waiting for". The
        // state's own role decides that: resuming an unrelated older session —
        // a finished coder while the Task sits in `review` — launches an
        // `interactive` execution that satisfies nothing, answers 200, and
        // leaves the Task exactly where it was.
        let expected_role = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(crate::workflow::effective_role);
        let role_matches = |execution: &Execution| {
            expected_role.is_none_or(|role| {
                execution.role == role
                    || (role == crate::workflow::default_roles::CODER
                        && execution.role == "executor")
            })
        };

        let executions = crate::task_service::action_resolver::list_execution_action_authority(
            &self.db,
            &task.id,
            expected_role,
            None,
        )
        .await?;
        if let Some(execution) = executions
            .iter()
            .filter(|execution| {
                execution.status != ExecutionStatus::Running
                    && execution.agent_session_id.is_some()
                    && role_matches(execution)
            })
            .max_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.id.cmp(&right.id))
            })
        {
            let recovery_clear = self
                .prepare_manual_stop_resume_clear(&current, blocking_annotation.as_ref())
                .await?;
            let launched = match self
                .follow_up_execution(
                    execution.id.clone(),
                    context.clone().unwrap_or_else(|| {
                        "Resume work from the latest worker session.".to_owned()
                    }),
                    execution.agent_id.clone(),
                    None,
                )
                .await
            {
                Ok(launched) => launched,
                Err(error) => {
                    self.restore_manual_stop_resume_clear(recovery_clear.as_ref(), &execution.role)
                        .await;
                    return Err(error);
                }
            };
            if let Err(error) = self.start_execution(launched.execution.id.clone()).await {
                self.restore_manual_stop_resume_clear(recovery_clear.as_ref(), &execution.role)
                    .await;
                return Err(error);
            }
            self.clear_resume_retry_metadata(&launched.task).await;
            return Ok(launched.task);
        }

        if let Some(execution) = executions
            .iter()
            .filter(|execution| {
                execution.status != ExecutionStatus::Running
                    && execution.agent_id.is_some()
                    && role_matches(execution)
            })
            .max_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.id.cmp(&right.id))
            })
        {
            let recovery_clear = self
                .prepare_manual_stop_resume_clear(&current, blocking_annotation.as_ref())
                .await?;
            let launched = match self
                .re_execute_execution_with_context(execution.id.clone(), context.clone())
                .await
            {
                Ok(launched) => launched,
                Err(error) => {
                    self.restore_manual_stop_resume_clear(recovery_clear.as_ref(), &execution.role)
                        .await;
                    return Err(error);
                }
            };
            if let Err(error) = self.start_execution(launched.execution.id.clone()).await {
                self.restore_manual_stop_resume_clear(recovery_clear.as_ref(), &execution.role)
                    .await;
                return Err(error);
            }
            self.clear_resume_retry_metadata(&launched.task).await;
            return Ok(launched.task);
        }

        let agent_id = self.action_agent_id(task, workflow).await?;
        let role = expected_role.unwrap_or(crate::workflow::default_roles::WORKER);
        let recovery_clear = self
            .prepare_manual_stop_resume_clear(&current, blocking_annotation.as_ref())
            .await?;
        let _launched = match self
            .dispatch_initial_role_execution(
                &task.id,
                &agent_id,
                role,
                context.unwrap_or_else(|| "Resume task work.".to_owned()),
            )
            .await
        {
            Ok(launched) => launched,
            Err(error) => {
                self.restore_manual_stop_resume_clear(recovery_clear.as_ref(), role)
                    .await;
                return Err(error);
            }
        };
        let resumed_task = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        self.clear_resume_retry_metadata(&resumed_task).await;
        Ok(resumed_task)
    }

    async fn prepare_manual_stop_resume_clear(
        &self,
        task: &Task,
        annotation: Option<&api_types::TaskBlockingAnnotation>,
    ) -> Result<Option<(Task, Task)>> {
        if annotation.is_some_and(|annotation| {
            annotation.annotation_type == api_types::FailureKind::ManualStop
        }) {
            let cleared = self.clear_recovery_metadata_at_version(task).await?;
            Ok(Some((cleared, task.clone())))
        } else {
            Ok(None)
        }
    }

    async fn restore_manual_stop_resume_clear(
        &self,
        recovery_clear: Option<&(Task, Task)>,
        execution_role: &str,
    ) {
        if let Some((cleared, original)) = recovery_clear {
            self.restore_recovery_metadata_after_failed_resume(
                cleared,
                original,
                None,
                execution_role,
            )
            .await;
        }
    }

    async fn clear_resume_retry_metadata(&self, task: &Task) {
        if let Err(error) = super::execution::clear_execution_retry_metadata(&self.db, task).await {
            tracing::warn!(task_id = %task.id, %error, "failed to clear execution retry metadata after resume");
        }
    }

    async fn action_agent_id(
        &self,
        task: &Task,
        workflow: &api_types::WorkflowDefinition,
    ) -> Result<String> {
        if task.assignee_type.as_deref() == Some("agent") {
            if let Some(agent_id) = task.assignee_id.as_deref() {
                return Ok(agent_id.to_owned());
            }
        }

        if let Some(role) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(crate::workflow::effective_role)
        {
            if let Some(assignment) =
                TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role).await?
            {
                if assignment.assignee_type == Some(AssigneeKind::Agent) {
                    if let Some(agent_id) = assignment.assignee_id {
                        return Ok(agent_id);
                    }
                }
            }
        }

        let first_work_role = workflow
            .outgoing_trigger_targets(&task.status)
            .filter_map(|(_, target)| {
                workflow
                    .states
                    .iter()
                    .find(|state| state.name == target)
                    .filter(|state| matches!(state.kind, StateKind::Active | StateKind::Gate))
                    .and_then(crate::workflow::effective_role)
            })
            .next();
        if let Some(role) = first_work_role {
            if let Some(assignment) =
                TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role).await?
            {
                if assignment.assignee_type == Some(AssigneeKind::Agent) {
                    if let Some(agent_id) = assignment.assignee_id {
                        return Ok(agent_id);
                    }
                }
            }
        }

        if let Some(execution) =
            ExecutionRepo::latest_agent_execution_by_task(&*self.db, &task.id).await?
        {
            if let Some(agent_id) = execution.agent_id {
                return Ok(agent_id);
            }
        }

        let agents = AgentRepo::list(
            &*self.db,
            AgentListQuery {
                status: None,
                executor_type: None,
                capabilities: Vec::new(),
                page: PageRequest {
                    cursor: None,
                    limit: 500,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Asc,
                },
            },
        )
        .await?
        .items;
        // The gemini CLI cannot self-authenticate headless: without a bound
        // credential the run exits immediately. Prefer any agent that can
        // actually execute before falling back to credential-less gemini.
        let dispatchable =
            |agent: &&Agent| agent.executor_type != "gemini" || agent.credential_ref.is_some();
        agents
            .iter()
            .find(|agent| agent.is_default && !agent.paused && dispatchable(agent))
            .or_else(|| {
                agents
                    .iter()
                    .find(|agent| !agent.paused && dispatchable(agent))
            })
            .or_else(|| {
                agents
                    .iter()
                    .find(|agent| agent.is_default && !agent.paused)
            })
            .or_else(|| agents.iter().find(|agent| !agent.paused))
            .map(|agent| agent.id.clone())
            .ok_or_else(|| ServiceError::invalid_operation("no available agent to start task"))
    }

    async fn latest_running_execution(&self, task_id: &str) -> Result<Option<Execution>> {
        Ok(ExecutionRepo::list_running_by_task(&*self.db, task_id)
            .await?
            .into_iter()
            .next())
    }

    async fn latest_review(&self, task_id: &str) -> Result<Option<Review>> {
        let task_ids = [task_id];
        Ok(
            ReviewRepo::list_latest_reviews_for_tasks(&*self.db, &task_ids)
                .await?
                .into_iter()
                .next(),
        )
    }
}

fn can_start(workflow: &api_types::WorkflowDefinition, task: &Task) -> bool {
    workflow.state_kind(&task.status) == Some(StateKind::Initial)
        && workflow
            .outgoing_trigger_targets(&task.status)
            .any(|(_, target)| {
                matches!(
                    workflow.state_kind(&target),
                    Some(StateKind::Active | StateKind::Gate)
                )
            })
}

fn trigger_target(
    workflow: &api_types::WorkflowDefinition,
    state: &str,
    trigger: WorkflowTrigger,
) -> Option<String> {
    workflow
        .outgoing_trigger_targets(state)
        .find(|(candidate, _)| *candidate == trigger)
        .map(|(_, target)| target)
}

fn cancellation_target(workflow: &api_types::WorkflowDefinition) -> Option<String> {
    workflow.cancellation_state.clone().or_else(|| {
        workflow
            .states
            .iter()
            .find(|state| {
                state.kind == StateKind::Terminal
                    && state.name == crate::workflow::default_states::CANCELLED
            })
            .map(|state| state.name.clone())
    })
}

fn action_name(action: TaskAction) -> &'static str {
    match action {
        TaskAction::Start => "start",
        TaskAction::Pause => "pause",
        TaskAction::Resume => "resume",
        TaskAction::Submit => "submit",
        TaskAction::RequestChanges => "request_changes",
        TaskAction::Approve => "approve",
        TaskAction::Cancel => "cancel",
    }
}

fn action_name_for_trigger(trigger: WorkflowTrigger) -> &'static str {
    match trigger {
        WorkflowTrigger::Accept => "accept",
        WorkflowTrigger::Reject => "reject",
        _ => "trigger",
    }
}

fn unavailable_reason(
    action: TaskAction,
    task: &Task,
    workflow: &api_types::WorkflowDefinition,
) -> String {
    let state_kind = workflow
        .state_kind(&task.status)
        .map(|kind| format!("{kind:?}"))
        .unwrap_or_else(|| "unknown".to_owned());
    format!(
        "action '{}' is not available while task is in {} state '{}'",
        action_name(action),
        state_kind,
        task.status
    )
}
