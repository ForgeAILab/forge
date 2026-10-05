use super::*;
use crate::workflow::engine::WorkflowAuthority;
use api_types::{Actor, SystemComponent};
use db::UpdateTask;

impl TaskService {
    pub async fn transition(
        &self,
        task_id: impl Into<String>,
        new_status: TaskStatus,
        options: impl Into<TransitionOptions>,
    ) -> Result<TransitionResult> {
        let task_id: String = task_id.into();
        let options: TransitionOptions = options.into();
        if !db::task_writer::owns_task(&task_id) {
            return self
                .request_task_command(
                    &task_id,
                    "transition",
                    serde_json::json!([task_id, new_status, options]),
                    new_status == "cancelled",
                )
                .await;
        }

        self.transition_inner(task_id, new_status, options, None)
            .await
    }

    pub(super) async fn transition_with_plan_publication(
        &self,
        task_id: impl Into<String>,
        new_status: TaskStatus,
        options: impl Into<TransitionOptions>,
        execution_id: &str,
    ) -> Result<TransitionResult> {
        let task_id: String = task_id.into();
        let options: TransitionOptions = options.into();
        if !db::task_writer::owns_task(&task_id) {
            return self
                .request_task_command(
                    &task_id,
                    "transition_with_plan_publication",
                    serde_json::json!([task_id, new_status, options, execution_id]),
                    new_status == "cancelled",
                )
                .await;
        }

        self.transition_inner(task_id, new_status, options, Some(execution_id))
            .await
    }

    async fn transition_inner(
        &self,
        task_id: String,
        new_status: TaskStatus,
        mut options: TransitionOptions,
        plan_publication_execution_id: Option<&str>,
    ) -> Result<TransitionResult> {
        if Self::task_action_command_active() {
            options.defer_dispatch_seconds = Some(0);
        }
        let trigger_reason = options.reason.unwrap_or_else(|| "user action".to_owned());
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        super::execution::ensure_plan_publication_transition_authority(
            &task,
            plan_publication_execution_id,
        )?;
        let previous_status = task.status.clone();
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        if let Some(execution_id) = plan_publication_execution_id {
            if super::execution::plan_publication_project_version(&task, execution_id)?
                .is_some_and(|claim_version| claim_version != project.version)
            {
                return Err(ServiceError::Db(DbError::VersionConflict));
            }
        }
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &options.triggered_by,
        );
        crate::task_hierarchy::ensure_coordination_root_target_ready(
            &self.db,
            &task,
            &workflow,
            &new_status,
        )
        .await?;
        if new_status != task.status
            && task.parent_task_id.is_some()
            && workflow
                .state_kind(&new_status)
                .is_some_and(|kind| kind != api_types::StateKind::Terminal)
        {
            // Keep every transition surface (REST, MCP, board actions, hooks,
            // and recovery) on the same durable sequence cursor. Terminal
            // cancellation remains available for later queued children, but
            // no later sibling may enter work/review before the first
            // incomplete child settles.
            crate::task_hierarchy::ensure_subtask_dispatch_order(&self.db, &task).await?;
        }
        if workflow.state_kind(&new_status) == Some(api_types::StateKind::Active) {
            // Direct transitions must obey the same admission boundary as
            // claim/launch for every repository-capable task type.  Task
            // labels such as discovery/planning only select a read-only
            // executor profile; they still use the normal lease boundary.
            let reviewer_state = workflow
                .states
                .iter()
                .find(|state| state.name == new_status)
                .and_then(crate::workflow::effective_role)
                == Some(crate::workflow::default_roles::REVIEWER);
            if reviewer_state {
                self.ensure_task_reviewable(&task).await?;
            } else {
                self.ensure_task_runnable(&task).await?;
            }
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
        let engine = self.workflow_execution();
        let defer_dispatch_until = options
            .defer_dispatch_seconds
            .map(|seconds| (chrono::Utc::now() + chrono::Duration::seconds(seconds)).to_rfc3339());
        let preserve_deferred_dispatch = defer_dispatch_until.is_some();
        let clear_review_passed_at_on_commit = plan_publication_execution_id.is_some()
            && task.review_passed_at.is_some()
            && workflow.canonical_phase_for_state(&new_status) == api_types::CanonicalPhase::Review;
        let result = engine
            .transition_with_deferred_dispatch_and_authority(
                &task_id,
                &new_status,
                options.version,
                &workflow,
                &options.triggered_by,
                &trigger_reason,
                options.rejection,
                defer_dispatch_until,
                Some(WorkflowAuthority {
                    project_version: project.version,
                    workflow_definition: project.workflow_definition.clone(),
                    clear_review_passed_at_on_commit,
                }),
                options.bridge,
            )
            .await?;
        // The engine returns the requested transition's committed CAS
        // snapshot. A concurrent writer cannot replace it in this response.
        let mut task = result.task;
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
            task = TaskRepo::set_review_passed_at_cas(
                &*self.db,
                &task.id,
                task.version,
                None,
                &now_rfc3339(),
            )
            .await?;
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
        if should_clear_transient_error_annotation(&task, &new_status) {
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
        if previous_status != task.status
            && workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal)
            && workflow.cancellation_state.as_deref() != Some(task.status.as_str())
        {
            // The success-path mirror of the cancellation projection above:
            // a Task that just completed (reached a terminal, non-cancelled
            // state) may be the prerequisite other Tasks are waiting on.
            // Wake them so a stale dependency-gate dispatch disposition
            // (keyed on the *dependent's* own version, which this commit
            // never touches) does not strand them forever. See F6.
            if let Err(error) = self.wake_dependents_of_completed_task(&task).await {
                tracing::warn!(task_id = %task.id, %error, "failed to wake dependents of completed task");
            }
        }
        self.reconcile_terminal_subtask(&task).await;

        if let Some(persisted) = TaskRepo::get_by_id(&*self.db, &task_id, false).await? {
            if persisted.status == task.status && persisted.version == task.version {
                task = persisted;
            }
        }
        let pending_steps = db::TaskStepRepo::pending_steps(&*self.db, &task_id).await?;
        if let Some(id) = &result.queued_step_id {
            db::TaskStepRepo::ready_step(&*self.db, id).await?;
        }
        Ok(TransitionResult {
            task,
            pending_steps,
            review: result.review,
        })
    }

    /// Retry an integration step that was durably deferred by Project pause.
    ///
    /// A passed review remains in `review`; a pause that wins immediately
    /// before the integration authority lock may leave the Task in `merging`.
    /// Resume consumes the same marker in either state without creating a new
    /// reviewer execution or accepting any unreviewed content.
    pub(crate) async fn retry_paused_integration(&self, task: &Task) -> Result<bool> {
        super::execution::ensure_plan_publication_transition_authority(task, None)?;
        let Some(deferred) = crate::deferred_dispatch::paused_integration(task) else {
            return Ok(false);
        };
        if deferred.state != task.status {
            crate::deferred_dispatch::clear_paused_integration(&self.db, &task.id, &deferred)
                .await?;
            return Ok(false);
        }
        // A previous resume's hook step (or any other step) is still queued
        // or running. Re-entering now would supersede it mid-merge; its
        // settlement consumes the marker on success.
        if db::TaskStepRepo::pending_steps(&*self.db, &task.id).await? > 0 {
            return Ok(false);
        }

        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        if project.paused_at.is_some() {
            return Ok(false);
        }
        let actor = Actor::system(SystemComponent::TaskDispatcher);
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(task, &project.workflow_definition, &actor);
        let state = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .ok_or_else(|| {
                ServiceError::invalid_operation(WorkflowEngine::undefined_state_message(
                    &task.status,
                    &workflow,
                ))
            })?;

        // The workflow engine deliberately returns a successful transition even
        // when a log-policy hook reports `HookResult::Failed`; the failure is
        // persisted in the transition log for diagnosis. Keep the pause marker
        // until both the transition and its hook outcomes prove that the
        // integration attempt actually completed. Clearing it before the hook
        // runs strands a Task in `merging` after a transient Git/daemon error.
        let prior_transition_ids = TransitionLogRepo::list_by_task(&*self.db, &task.id)
            .await?
            .into_iter()
            .map(|entry| entry.id)
            .collect::<std::collections::HashSet<_>>();
        let result = if state
            .hooks
            .on_enter
            .iter()
            .any(|hook| hook.action == "run_merge")
        {
            let engine = self.workflow_execution();
            engine
                .manual_override_transition_with_authority(
                    &task.id,
                    &task.status,
                    task.version,
                    &workflow,
                    actor.clone(),
                    "resuming integration after project pause",
                    false,
                    Some(WorkflowAuthority {
                        project_version: project.version,
                        workflow_definition: project.workflow_definition.clone(),
                        clear_review_passed_at_on_commit: false,
                    }),
                )
                .await
                .map(|_| ())
        } else if let Some(target) = workflow.auto_transition_target(&task.status) {
            self.transition(
                task.id.clone(),
                target.to_owned(),
                TransitionOptions {
                    bridge: Default::default(),
                    version: task.version,
                    reason: Some("resuming integration after project pause".to_owned()),
                    triggered_by: actor,
                    rejection: false,
                    defer_dispatch_seconds: None,
                },
            )
            .await
            .map(|_| ())
        } else {
            Err(ServiceError::invalid_operation(format!(
                "state {} has no integration capability to resume",
                task.status
            )))
        };

        if let Err(error) = result {
            // The marker is intentionally left in place for every failed
            // admission/transition, not only the pause/version-conflict cases.
            // A failed hook can be a transient workspace or provider failure,
            // and the next active-recovery scan is the retry boundary.
            if let Err(retain_error) = self.retain_paused_integration_marker(&task.id).await {
                tracing::warn!(
                    task_id = %task.id,
                    %retain_error,
                    "failed to refresh paused-integration marker after retry failure"
                );
            }
            return Err(error);
        }

        if db::TaskStepRepo::pending_steps(&*self.db, &task.id).await? > 0 {
            return Ok(true);
        }

        if paused_integration_transition_failed(&self.db, &task.id, &prior_transition_ids).await? {
            // The transition may have advanced `review -> merging` before its
            // `run_merge` hook failed. Move the marker to the committed state
            // so the next scan retries that exact integration capability.
            self.retain_paused_integration_marker(&task.id).await?;
            return Ok(false);
        }

        // Success is the only point at which the durable pause marker may be
        // consumed. If this metadata update fails, the marker remains and the
        // retry is harmlessly idempotent.
        crate::deferred_dispatch::clear_paused_integration(&self.db, &task.id, &deferred).await?;
        Ok(true)
    }

    async fn retain_paused_integration_marker(&self, task_id: &str) -> Result<()> {
        if let Some(current) = TaskRepo::get_by_id(&*self.db, task_id, false).await? {
            crate::deferred_dispatch::refresh_paused_integration_for_pause(&self.db, &current)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn ensure_planning_plan_ready_before_leaving(
        &self,
        task: &Task,
        new_status: &TaskStatus,
        workflow: &api_types::WorkflowDefinition,
        rejection: bool,
    ) -> Result<()> {
        if crate::task_hierarchy::coordination_root_has_subtasks(&self.db, task).await? {
            return Ok(());
        }
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

        let resolved = crate::workspace_backend::EmbeddedWorkspaceBackend::resolve_workspace(
            &self.workspace_backend_router,
            &self.db,
            &workspace,
            &self.workspace_root,
        )
        .await?;
        let bytes = match resolved
            .backend
            .read(&resolved.placement, "../plan.md", 1_048_576)
            .await
        {
            Ok(bytes) => bytes,
            Err(crate::workspace_backend::WorkspaceBackendError::Other(error)) if matches!(&*error, ServiceError::InvalidOperation { message } if message == "plan artifact not found") =>
            {
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
        let content = String::from_utf8(bytes).map_err(|_| {
            ServiceError::invalid_operation(
                "planning plan artifact is unreadable: failed to read plan artifact: stream did not contain valid UTF-8",
            )
        })?;
        let artifact = crate::plan_artifact::parse_plan_markdown(&content);
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
        if db::TaskStepRepo::entry_hooks_pending(&*self.db, &task.id).await? {
            // The entry's checks (CI, before-work scripts, dispatch) are
            // still queued or running in its hook step. A gate decision
            // taken now would act on the previous Review, not this entry's.
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
            // A *failed* review parked in `review` is also waiting on a
            // person, and nothing said so. The dispatcher cannot re-dispatch
            // a reviewer while the latest review is `Failed`
            // (`reviewer_dispatch_ready` requires a `Running` review), so the
            // Task sits reporting "Waiting for reviewer dispatch" at `info`
            // severity with `awaiting_human: false` while only a human
            // `retry_hook` can move it.
            if latest_review
                .as_ref()
                .is_some_and(|review| review.status == ReviewStatus::Failed)
                && sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM execution WHERE task_id = ? AND status = 'running'",
                )
                .bind(&task.id)
                .fetch_one(self.db.pool())
                .await?
                    == 0
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
        let has_decision_since_entry =
            gate_decision_since_entry(&transition_log, &task.status, entered_at);
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
        // Manual rejection is recorded while the task is being bounced to
        // `in_progress`, but its budget belongs to the review gate that
        // produced the rejection.  Resolving the current active state here
        // would select `in_progress` and count a different/empty budget.
        let review_state = workflow
            .states
            .iter()
            .find(|state| state.name == default_states::REVIEW);
        let max_retries = super::config::runtime_retry_budget(
            &task,
            super::config::RetryBudgetKind::Review,
            review_state.map(|state| &state.config),
            review_state.and_then(|state| state.gate_config.as_ref()),
        )?;

        let used = crate::task_diagnostics::count_gate_rejections_for_task(
            &self.db,
            task_id,
            default_states::REVIEW,
        )
        .await?;
        let remaining = i64::from(max_retries) - used;
        Ok(remaining.clamp(0, i64::from(i32::MAX)) as i32)
    }

    pub async fn cancel_task(&self, task_id: impl Into<String>) -> Result<Task> {
        self.cancel_task_as(task_id, Actor::system(SystemComponent::CancelTask))
            .await
    }

    pub async fn cancel_task_as(&self, task_id: impl Into<String>, actor: Actor) -> Result<Task> {
        self.cancel_task_with_options(task_id.into(), None, "cancel task".to_owned(), actor)
            .await
    }

    pub(crate) async fn cancel_task_at_version_as(
        &self,
        task_id: impl Into<String>,
        expected_version: i64,
        reason: String,
        actor: Actor,
    ) -> Result<Task> {
        self.cancel_task_with_options(task_id.into(), Some(expected_version), reason, actor)
            .await
    }

    pub(crate) async fn cancel_task_with_options(
        &self,
        task_id: String,
        expected_version: Option<i64>,
        reason: String,
        actor: Actor,
    ) -> Result<Task> {
        if !db::task_writer::owns_task(&task_id) {
            return self
                .request_task_command(
                    &task_id,
                    "cancel_task_with_options",
                    serde_json::json!([task_id, expected_version, reason, actor]),
                    true,
                )
                .await;
        }
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        super::execution::ensure_plan_publication_transition_authority(&task, None)?;
        if let Some(expected_version) = expected_version {
            if expected_version != task.version {
                return Err(ServiceError::Db(db::DbError::TaskVersionConflict {
                    expected: expected_version,
                    actual: task.version,
                }));
            }
        }
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
        let child_tasks = crate::task_hierarchy::children_if_root(&self.db, &task).await?;
        if task.status == cancel_target {
            // A prior cancellation may have committed while a child was
            // still starting. Re-check and stop every active child even when
            // the root state is already terminal, before any cleanup hook can
            // use the shared workspace.
            self.cancel_running_executions_for_task(&task, &reason, actor.clone())
                .await?;
            self.repair_cancelled_coordination_children(&task, &workflow, &reason, actor.clone())
                .await?;
            if let Err(error) = self.block_dependents_of_cancelled_task(&task).await {
                tracing::warn!(task_id = %task.id, %error, "failed to re-project cancelled prerequisite onto dependents");
            }
            self.reconcile_terminal_subtask(&task).await;
            crate::placement::admission::resolve_workspace_attention(&self.db, &task.id).await?;
            return TaskRepo::get_by_id(&*self.db, &task.id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", task.id));
        }
        self.cancel_running_executions_for_task(&task, &reason, actor.clone())
            .await?;
        // A coordination root owns the shared worktree, but its children own
        // the running executions. Terminalize those attempts before the root
        // transition enters its workspace-cleanup hooks.
        self.cancel_running_executions_for_tasks(&child_tasks, &reason, actor.clone())
            .await?;
        let result = self
            .transition(
                task_id,
                cancel_target,
                TransitionOptions {
                    bridge: Default::default(),
                    version: expected_version.unwrap_or(task.version),
                    reason: Some(reason.clone()),
                    triggered_by: actor.clone(),
                    rejection: false,
                    defer_dispatch_seconds: None,
                },
            )
            .await?;
        let task = clear_manual_advance_error_annotation(&self.db, &task, result.task).await?;
        crate::placement::admission::resolve_workspace_attention(&self.db, &task.id).await?;
        // Re-read after the root transition hooks and cancel once more. Any
        // child execution that committed between the pre-cancel snapshot and
        // the root transition is now visible, while the transactional parent
        // admission guard prevents a new one from appearing afterward.
        self.repair_cancelled_coordination_children(&task, &workflow, &reason, actor.clone())
            .await?;
        if let Err(error) = self.block_dependents_of_cancelled_task(&task).await {
            // Cancellation already committed.  The dependency gate performs
            // the same durable blocking check on the next attempted dispatch,
            // so report the projection failure without turning a successful
            // cancellation into a misleading request error.
            tracing::warn!(
                task_id = %task.id,
                %error,
                "failed to project cancelled prerequisite onto dependents"
            );
        }
        Ok(task)
    }

    async fn repair_cancelled_coordination_children(
        &self,
        root: &Task,
        workflow: &api_types::WorkflowDefinition,
        reason: &str,
        actor: Actor,
    ) -> Result<()> {
        let children = crate::task_hierarchy::children_if_root(&self.db, root).await?;
        for child in children {
            self.cancel_running_executions_for_task(&child, reason, actor.clone())
                .await?;
            if crate::task_hierarchy::subtask_is_terminal(&child, workflow) {
                continue;
            }
            // The root's cancel has committed under the root lease. Each
            // child's cancel is its own preempting step on the child's queue;
            // the root never waits on a child's lease, so a busy child cannot
            // turn the committed root cancel into an error or skip a sibling.
            // A repeat root Cancel re-enqueues any child still not terminal.
            if let Err(error) = self
                .enqueue_task_command(
                    &child.id,
                    "cancel_task_with_options",
                    serde_json::json!([child.id, Option::<i64>::None, "cancel task", actor]),
                    true,
                )
                .await
            {
                tracing::warn!(root_id = %root.id, child_id = %child.id, %error, "failed to enqueue coordination child cancel");
            }
        }
        Ok(())
    }

    pub(crate) async fn advance_task_condition(
        &self,
        task: &Task,
        workflow: &api_types::WorkflowDefinition,
        target: String,
        reason: String,
        actor: Actor,
    ) -> Result<Task> {
        if !db::task_writer::owns_task(&task.id) {
            return self
                .request_task_command(
                    &task.id,
                    "advance_task_condition",
                    serde_json::json!([task.id, workflow, target, reason, actor]),
                    false,
                )
                .await;
        }
        super::execution::ensure_plan_publication_transition_authority(task, None)?;
        crate::task_hierarchy::ensure_coordination_root_target_ready(
            &self.db, task, workflow, &target,
        )
        .await?;
        if workflow.state_kind(&target) != Some(api_types::StateKind::Terminal) {
            crate::task_hierarchy::ensure_subtask_dispatch_order(&self.db, task).await?;
        }
        self.cancel_running_executions_for_task(task, "cancelled by manual advance", actor.clone())
            .await?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let engine = self.workflow_execution();
        let result = engine
            .manual_override_transition_with_authority(
                &task.id,
                &target,
                task.version,
                workflow,
                actor,
                &reason,
                false,
                Some(WorkflowAuthority {
                    project_version: project.version,
                    workflow_definition: project.workflow_definition.clone(),
                    clear_review_passed_at_on_commit: false,
                }),
            )
            .await?;
        let updated = clear_manual_advance_error_annotation(&self.db, task, result.task).await?;
        if workflow.state_kind(&updated.status) == Some(api_types::StateKind::Terminal)
            && workflow.cancellation_state.as_deref() != Some(updated.status.as_str())
        {
            if let Err(error) = self.wake_dependents_of_completed_task(&updated).await {
                tracing::warn!(task_id = %updated.id, %error, "failed to wake dependents of completed task");
            }
        }
        self.reconcile_terminal_subtask(&updated).await;
        Ok(updated)
    }

    pub async fn soft_delete(&self, task_id: impl Into<String>) -> Result<Task> {
        let task_id = task_id.into();
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, true)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        super::execution::ensure_plan_publication_transition_authority(&task, None)?;
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
        super::execution::ensure_plan_publication_transition_authority(&task, None)?;
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

async fn paused_integration_transition_failed(
    db: &db::SqliteDb,
    task_id: &str,
    prior_transition_ids: &std::collections::HashSet<String>,
) -> Result<bool> {
    let entries = TransitionLogRepo::list_by_task(db, task_id).await?;
    Ok(entries
        .into_iter()
        .filter(|entry| !prior_transition_ids.contains(&entry.id))
        .any(|entry| {
            let Some(raw_results) = entry.hook_results_json.as_deref() else {
                // The engine normally persists `[]` even for a transition
                // without hooks. A missing result payload means the outcome is
                // unknown, so retain the marker rather than falsely consuming
                // a deferred integration.
                return true;
            };
            let Ok(results) = serde_json::from_str::<Vec<api_types::HookResultEntry>>(raw_results)
            else {
                return true;
            };
            results.iter().any(|result| {
                result.outcome == "failed"
                    // `run_merge` uses Skipped for the pause/no-worktree
                    // boundary; neither outcome completed integration.
                    || (result.action == "run_merge" && result.outcome == "skipped")
            })
        }))
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

        let executions = ExecutionRepo::list_running_by_task(&*self.db, &task.id).await?;
        for execution in executions {
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

    async fn cancel_running_executions_for_task(
        &self,
        task: &Task,
        reason: &str,
        actor: Actor,
    ) -> Result<()> {
        let executions = ExecutionRepo::list_running_by_task(&*self.db, &task.id).await?;
        for execution in executions {
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

    async fn cancel_running_executions_for_tasks(
        &self,
        tasks: &[Task],
        reason: &str,
        actor: Actor,
    ) -> Result<()> {
        for task in tasks {
            self.cancel_running_executions_for_task(task, reason, actor.clone())
                .await?;
        }
        Ok(())
    }
}

pub(crate) fn next_workflow_state(
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

    // Compare the annotation, not the version: the Advance's own queued step
    // may already have moved the Task. A replaced annotation is a no-op,
    // never a post-commit 409. The event shares the clear's transaction.
    let mut tx = db::begin_immediate(db.pool()).await?;
    let cleared = db::task_writer::TaskQuery::new(db,&advanced_task.id,"UPDATE task SET error_annotation=NULL,version=version+1,updated_at=? WHERE id=? AND error_annotation IS ? AND deleted_at IS NULL")
        .bind(now_rfc3339()).bind(&advanced_task.id).bind(&source_task.error_annotation)
        .execute_in_tx(&mut tx).await?.require_applied()?;
    let current = db
        .get_task_in_tx(&mut tx, &advanced_task.id)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", advanced_task.id.clone()))?;
    if cleared == 1 {
        let event = db::CreateDomainEvent::task_interruption_changed(&current);
        db::DomainEventRepo::append_event_in_tx(db, &mut tx, &event).await?;
    }
    tx.commit().await?;
    Ok(current)
}

pub(super) async fn clear_manual_review_awaiting_metadata(
    db: &SqliteDb,
    task: &Task,
) -> Result<Task> {
    let current = TaskRepo::get_by_id(db, &task.id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
    let metadata = TaskMetadata::parse(current.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let Some(reason) = metadata
        .extra
        .get("awaiting_human_reason")
        .and_then(Value::as_str)
    else {
        return Ok(current);
    };
    if reason != "manual_review" {
        return Ok(current);
    }

    let Some(expected) = metadata.extra.get("awaiting_human_marker_id").cloned() else {
        // Legacy markers have no identity that can distinguish this clear
        // from a newer manual-review marker with the same reason.
        return Ok(current);
    };
    let mutations = vec![db::TaskMetadataMutation::CompareAndMutate {
        key: "awaiting_human_marker_id".to_owned(),
        expected,
        mutations: vec![
            db::TaskMetadataMutation::Remove {
                key: "awaiting_human".to_owned(),
            },
            db::TaskMetadataMutation::Remove {
                key: "awaiting_human_reason".to_owned(),
            },
            db::TaskMetadataMutation::Remove {
                key: "awaiting_human_marker_id".to_owned(),
            },
        ],
    }];
    TaskRepo::mutate_metadata(db, &task.id, None, mutations, &now_rfc3339())
        .await
        .map_err(Into::into)
}

/// `requested_target` is the status the caller asked for, not necessarily
/// `task.status`: a failed dispatch cascades the Task back to `todo` and
/// records a fresh `dispatch_failed` annotation, which must survive the
/// transition that triggered it.
pub(super) fn should_clear_transient_error_annotation(task: &Task, requested_target: &str) -> bool {
    if task.status.as_str() == default_states::MERGE_FAILED {
        return false;
    }
    // `dispatch_failed` only describes a failed attempt to start work. Once
    // the Task is deliberately placed in `todo`/`backlog` it is no longer
    // attempting to start, so the annotation is stale (a disk-full incident
    // left it on every backlog Task long after the disk was fixed). Real work
    // failures (merge_conflict, executor_failed, review failures) are
    // deliberately not covered here.
    if matches!(
        requested_target,
        default_states::TODO | default_states::BACKLOG
    ) && task.status.as_str() == requested_target
        && crate::workflow::engine::is_dispatch_failed_annotation(task.error_annotation.as_deref())
    {
        return true;
    }
    // A blocked Task has not moved on, and its annotation is the only thing
    // carrying the blocking reason and the recovery actions a client can
    // offer. Clearing it here is what left a Task blocked in `merging` with
    // `blocking_reason: ""` and `recovery_actions: []` — visibly stuck with
    // no documented way out. The annotation is cleared with the block itself
    // (see `clear_retry_exhausted_blocking_metadata`).
    if task.blocked_json.is_some() {
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

    #[test]
    fn blocked_task_keeps_its_transient_annotation() {
        // The annotation is the only carrier of `blocking_reason` and
        // `recovery_actions`. Clearing it while the Task is still blocked left
        // a Task stuck in `merging` advertising no way out at all.
        let now = db::now_rfc3339();
        let mut task = Task {
            id: "task".into(),
            project_id: "project".into(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "contended".into(),
            description: None,
            task_type: "task".into(),
            status: default_states::MERGING.into(),
            is_automation: false,
            priority: 0,
            board_position: 0.0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            metadata_json: None,
            plan: None,
            error_annotation: Some(
                serde_json::json!({"type": "merge_fix_budget_exhausted"}).to_string(),
            ),
            blocked_json: None,
            failed_json: None,
            entry_barrier_json: None,
            review_passed_at: None,
            archived_at: None,
            deleted_at: None,
            version: 1,
            created_at: now.clone(),
            updated_at: now,
        };

        task.blocked_json = None;
        assert!(
            should_clear_transient_error_annotation(&task, default_states::MERGING),
            "an unblocked Task moving on still drops a stale transient annotation"
        );

        task.blocked_json =
            Some(serde_json::json!({"kind": "merge_fix_budget_exhausted"}).to_string());
        assert!(
            !should_clear_transient_error_annotation(&task, default_states::MERGING),
            "a blocked Task must keep the annotation explaining the block"
        );
    }

    #[test]
    fn dispatch_failed_annotation_cleared_only_when_parked_in_todo_or_backlog() {
        let mut task = blocked_free_task(default_states::BACKLOG);
        task.error_annotation = Some(serde_json::json!({"type": "dispatch_failed"}).to_string());

        assert!(should_clear_transient_error_annotation(
            &task,
            default_states::BACKLOG
        ));
        task.status = default_states::TODO.into();
        assert!(should_clear_transient_error_annotation(
            &task,
            default_states::TODO
        ));
        // A failed dispatch cascades back to todo with a fresh annotation
        // while the caller asked for in_progress: it must be kept.
        assert!(!should_clear_transient_error_annotation(
            &task,
            default_states::IN_PROGRESS
        ));
    }

    #[test]
    fn work_failure_annotations_survive_move_to_backlog() {
        for kind in ["workspace_failed", "ci_failed", "review_blocked"] {
            let mut task = blocked_free_task(default_states::BACKLOG);
            task.error_annotation = Some(serde_json::json!({"type": kind}).to_string());
            assert!(
                !should_clear_transient_error_annotation(&task, default_states::BACKLOG),
                "{kind} must not be cleared by a move to backlog"
            );
        }
    }

    fn blocked_free_task(status: &str) -> Task {
        let now = db::now_rfc3339();
        Task {
            id: "task".into(),
            project_id: "project".into(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "t".into(),
            description: None,
            task_type: "task".into(),
            status: status.into(),
            is_automation: false,
            priority: 0,
            board_position: 0.0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            metadata_json: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            entry_barrier_json: None,
            review_passed_at: None,
            archived_at: None,
            deleted_at: None,
            version: 1,
            created_at: now.clone(),
            updated_at: now,
        }
    }
}

#[cfg(test)]
mod audit_tests {
    use super::*;
    #[tokio::test]
    async fn advance_annotation_clear_accepts_both_step_orderings() {
        for step_wins in [false, true] {
            let db = SqliteDb::new(db::create_sqlite_pool("sqlite::memory:").await.unwrap());
            db::run_migrations(db.pool()).await.unwrap();
            let now = now_rfc3339();
            sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','p',?,?)")
                .bind(&now)
                .bind(&now)
                .execute(db.pool())
                .await
                .unwrap();
            sqlx::query("INSERT INTO task(id,project_id,title,status,error_annotation,created_at,updated_at) VALUES('t','p','t','todo','old annotation',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
            let source = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
            sqlx::query("UPDATE task SET status='planning',version=version+1 WHERE id='t'")
                .execute(db.pool())
                .await
                .unwrap();
            let advanced = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
            if step_wins {
                sqlx::query("UPDATE task SET status='in_progress',version=version+1 WHERE id='t'")
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            use db::TaskStepRepo;
            let current = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
            db.enqueue_task_mutation(
                "t",
                db::TaskMutation::TaskSetEntryBarrier {
                    id: "t".into(),
                    expected_version: current.version,
                    entry_barrier_json: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await
            .unwrap();
            let lease = db
                .claim_step(
                    "advance-clear",
                    Some("t"),
                    &db::task_writer::lease_deadline(),
                )
                .await
                .unwrap()
                .unwrap();
            let result = db::task_writer::in_task_step(
                lease.clone(),
                clear_manual_advance_error_annotation(&db, &source, advanced),
            )
            .await
            .unwrap();
            let mut tx = db::begin_immediate(db.pool()).await.unwrap();
            db.finish_step_in_tx(&mut tx, &lease, "done", None)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            db.release_step(&lease.id, "advance-clear").await.unwrap();
            assert!(result.error_annotation.is_none());
            if !step_wins {
                sqlx::query("UPDATE task SET status='in_progress',version=version+1 WHERE id='t'")
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            let final_task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
            assert_eq!(final_task.status, "in_progress");
            assert!(final_task.error_annotation.is_none());
        }
    }
}

fn gate_decision_since_entry(entries: &[db::TransitionLog], state: &str, entered_at: &str) -> bool {
    entries.iter().any(|entry| {
        entry.from_state == state
            && entry.created_at.as_str() >= entered_at
            && matches!(
                entry.bridge.bridge_kind,
                Some(
                    api_types::TransitionBridgeKind::GateApproved
                        | api_types::TransitionBridgeKind::GateRejected
                )
            )
    })
}

#[cfg(test)]
mod typed_gate_tests {
    use super::*;
    #[test]
    fn gate_decision_reader_uses_kind_with_custom_guidance_and_ignores_prefixes() {
        let mut row = db::TransitionLog {
            id: "decision".into(),
            task_id: "task".into(),
            from_state: "review".into(),
            to_state: "review".into(),
            trigger_name: None,
            triggered_by: "user:api".into(),
            bridge: Default::default(),
            trigger_reason: "gate approved; gate rejected".into(),
            hook_results_json: None,
            rejection: false,
            created_at: "2026-10-05".into(),
        };
        assert!(!gate_decision_since_entry(
            &[row.clone()],
            "review",
            "2026-10-05"
        ));
        for kind in [
            api_types::TransitionBridgeKind::GateApproved,
            api_types::TransitionBridgeKind::GateRejected,
        ] {
            row.bridge = api_types::TransitionBridge::new(kind);
            row.trigger_reason = "Owner supplied custom guidance".into();
            assert!(gate_decision_since_entry(
                &[row.clone()],
                "review",
                "2026-10-05"
            ));
            assert!(!gate_decision_since_entry(
                &[row.clone()],
                "review",
                "2026-10-06"
            ));
        }
    }
}
