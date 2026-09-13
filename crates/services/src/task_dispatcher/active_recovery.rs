use std::sync::Arc;

use api_types::{Actor, StateKind, SystemComponent, WorkflowDefinition};
use db::{
    AgentRepo, DbError, ExecutionRepo, ExecutionStatus, PageRequest, Project, ReviewRepo, SortBy,
    SortOrder, Task, TaskRepo, TaskRoleAssignmentRepo,
};

use crate::{
    agent_capacity::has_running_execution_capacity,
    agent_service::{compute_effective_status, EffectiveStatus},
    deferred_dispatch,
    workflow::{
        dispatch::{
            build_effective_prompt, dispatch_intent_from_workflow_dispatch,
            effective_prompt_selection, loader::load_agent_dispatch_context,
        },
        effective_role,
        engine::WorkflowEngine,
    },
    Result, ServiceError,
};

use super::{helpers, TaskDispatcher};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReviewerReconciliation {
    None,
    Reconciled,
    NoExactReviewBinding,
}

impl TaskDispatcher {
    pub(super) async fn recover_active_tasks(
        &self,
        project: &Project,
        workflow: &WorkflowDefinition,
    ) -> Result<u64> {
        let mut active_states: Vec<String> = workflow
            .states
            .iter()
            .filter(|state| matches!(state.kind, StateKind::Active | StateKind::Gate))
            .map(|state| state.name.clone())
            .collect();
        for state in WorkflowEngine::resolve_subtask_workflow()
            .states
            .iter()
            .filter(|state| matches!(state.kind, StateKind::Active | StateKind::Gate))
        {
            if !active_states.contains(&state.name) {
                active_states.push(state.name.clone());
            }
        }
        if active_states.is_empty() {
            return Ok(0);
        }

        let tasks = self.list_tasks(&project.id, active_states).await?;
        let mut dispatched = 0;
        for task in tasks {
            if self.is_stopped() {
                break;
            }
            let task_workflow = WorkflowEngine::resolve_workflow_for_task(
                &task,
                &project.workflow_definition,
                &Actor::system(SystemComponent::TaskDispatcher),
            );
            let recovery_role = task_workflow
                .states
                .iter()
                .find(|state| state.name == task.status)
                .and_then(effective_role);
            if crate::deferred_dispatch::paused_integration(&task).is_some() {
                if helpers::has_blocking_annotation(&task) {
                    continue;
                }
                match self.task_service.retry_paused_integration(&task).await {
                    Ok(true) => dispatched += 1,
                    Ok(false) => {}
                    Err(ServiceError::Db(DbError::VersionConflict)) => {
                        tracing::debug!(
                            task_id = %task.id,
                            from_state = %task.status,
                            "paused integration recovery lost version race"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            task_id = %task.id,
                            from_state = %task.status,
                            %error,
                            "paused integration recovery failed"
                        );
                    }
                }
                continue;
            }
            if crate::workflow::review_refresh_transition_pending(&self.db, &task.id, &task.status)
                .await?
            {
                let Some(target) =
                    crate::workflow::review_refresh_target(&task_workflow, &task.status)
                else {
                    tracing::error!(
                        task_id = %task.id,
                        state = %task.status,
                        "review-refresh task has no reviewer transition"
                    );
                    continue;
                };
                match self
                    .task_service
                    .transition(
                        task.id.clone(),
                        target.clone(),
                        crate::task_service::TransitionOptions {
                            version: task.version,
                            reason: Some(format!(
                                "{} recover interrupted review refresh",
                                crate::workflow::REVIEW_REFRESH_MARKER
                            )),
                            triggered_by: Actor::system(SystemComponent::TaskDispatcher),
                            rejection: false,
                            defer_dispatch_seconds: None,
                        },
                    )
                    .await
                {
                    Ok(_) => dispatched += 1,
                    Err(ServiceError::Db(DbError::VersionConflict)) => {
                        tracing::debug!(task_id = %task.id, "review-refresh recovery lost version race");
                    }
                    Err(error) => {
                        tracing::warn!(
                            task_id = %task.id,
                            from_state = %task.status,
                            to_state = %target,
                            %error,
                            "review-refresh recovery failed"
                        );
                    }
                }
                continue;
            }
            let is_coordination_root =
                crate::task_service::coordination_root_has_subtasks(&self.db, &task).await?;
            let sequence_complete = is_coordination_root
                && crate::task_service::coordination_root_sequence_complete(
                    &self.db, &task, workflow,
                )
                .await?;
            let needs_recovery_advance = sequence_complete
                && workflow.canonical_phase_for_state(&task.status)
                    != api_types::CanonicalPhase::Review
                && workflow.state_kind(&task.status) != Some(StateKind::Terminal);
            if is_coordination_root
                && (crate::task_service::coordination_review_pending(&task)
                    || needs_recovery_advance)
            {
                dispatched += self.advance_coordination_root_once(&task).await?;
                continue;
            }
            if deferred_dispatch::dispatch_disposition_is_current(&task, &task.status) {
                // An unchanged deterministic blocker was already observed for
                // this exact Task version and capability — skip the attempt
                // and its warning entirely (F11). See the matching check in
                // `dispatch_initial_tasks`.
                continue;
            }
            match self
                .recover_active_task(project, &task_workflow, &task)
                .await
            {
                Ok(true) => {
                    deferred_dispatch::clear_dispatch_disposition(&self.db, &task).await?;
                    dispatched += 1;
                }
                Ok(false) => {}
                Err(ServiceError::Db(DbError::VersionConflict)) => {
                    tracing::debug!(
                        task_id = %task.id,
                        from_state = %task.status,
                        target_role = recovery_role.unwrap_or("none"),
                        "task dispatcher recovery lost version race"
                    );
                }
                Err(ref error @ ServiceError::WorkspaceResetRequired { .. }) => {
                    tracing::warn!(task_id = %task.id, %error, "task branch lost, blocking for user reset");
                    if let Err(block_error) =
                        self.block_task_for_workspace_reset(&task, error).await
                    {
                        tracing::warn!(task_id = %task.id, %block_error, "failed to block task for workspace reset");
                    }
                }
                Err(error) if helpers::is_io_or_workspace_error(&error) => {
                    tracing::error!(task_id = %task.id, %error, "task dispatcher recovery blocked task due to workspace error");
                    if let Err(block_error) =
                        self.block_task_on_workspace_error(&task, &error).await
                    {
                        tracing::warn!(task_id = %task.id, %block_error, "failed to block task after workspace error");
                    }
                }
                Err(error) if helpers::is_deterministic_dispatch_refusal(&error) => {
                    deferred_dispatch::record_dispatch_disposition(
                        &self.db,
                        &task,
                        &task.status,
                        &error.to_string(),
                    )
                    .await?;
                    tracing::warn!(
                        task_id = %task.id,
                        from_state = %task.status,
                        target_role = recovery_role.unwrap_or("none"),
                        %error,
                        "task dispatch blocked; parked until Task/governance state changes or an explicit wake"
                    );
                }
                Err(error) => {
                    // Potentially transient: no disposition, so the next scan
                    // retries instead of stalling on a momentary failure.
                    tracing::warn!(
                        task_id = %task.id,
                        from_state = %task.status,
                        target_role = recovery_role.unwrap_or("none"),
                        %error,
                        "task dispatcher recovery failed"
                    );
                }
            }
        }
        Ok(dispatched)
    }

    async fn recover_active_task(
        &self,
        project: &Project,
        workflow: &WorkflowDefinition,
        task: &Task,
    ) -> Result<bool> {
        let Some(state) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
        else {
            return Ok(false);
        };
        if !matches!(state.kind, StateKind::Active | StateKind::Gate) {
            return Ok(false);
        }
        if self.is_stopped() {
            return Ok(false);
        }
        if !state
            .hooks
            .on_enter
            .iter()
            .any(|hook| hook.action == "dispatch_role_agent")
        {
            return Ok(false);
        }
        if helpers::has_blocking_annotation(task) {
            return Ok(false);
        }
        if deferred_dispatch::is_pending(task, chrono::Utc::now()) {
            return Ok(false);
        }
        let Some(role_name) = effective_role(state) else {
            return Ok(false);
        };
        if role_name == crate::workflow::default_roles::REVIEWER {
            self.task_service.ensure_task_reviewable(task).await?;
        } else {
            self.task_service.ensure_task_runnable(task).await?;
        }
        if crate::task_service::coordination_root_has_subtasks(&self.db, task).await?
            && role_name != crate::workflow::default_roles::REVIEWER
        {
            return Ok(false);
        }
        if !crate::task_service::subtask_dispatch_ready(&self.db, task).await? {
            return Ok(false);
        }
        let reviewer_reconciliation = if role_name == crate::workflow::default_roles::REVIEWER {
            self.reconcile_terminal_reviewer_execution(&task.id).await?
        } else {
            ReviewerReconciliation::None
        };
        if reviewer_reconciliation == ReviewerReconciliation::Reconciled {
            // Reconciliation may change the Task version/state, install a
            // blocker, or schedule a deferred retry. Let the next scan work
            // from those committed facts instead of dispatching from this
            // stale Task snapshot.
            return Ok(false);
        }
        if state.kind == StateKind::Gate && helpers::auto_cascades_on_unassigned_role(state) {
            let assignment =
                TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role_name)
                    .await?;
            if helpers::role_assignment_unassigned(assignment.as_ref()) {
                let Some(target) = self.resolve_initial_schedule_target(workflow, task).await?
                else {
                    return Ok(false);
                };
                return self.dispatch_initial_task(task, &target).await;
            }
        }
        if reviewer_reconciliation != ReviewerReconciliation::NoExactReviewBinding
            && helpers::latest_stopped_execution_blocks_dispatch(&self.db, &task.id, role_name)
                .await?
        {
            return Ok(false);
        }
        let Some(assignment) =
            TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role_name).await?
        else {
            return Ok(false);
        };
        if assignment.assignee_type != Some(db::AssigneeKind::Agent) {
            return Ok(false);
        }
        let Some(agent_id) = assignment.assignee_id.as_deref() else {
            return Ok(false);
        };
        if helpers::has_running_execution_for_roles(
            &self.db,
            &task.id,
            &helpers::execution_guard_roles(role_name),
        )
        .await?
        {
            return Ok(false);
        }

        let state_config =
            helpers::merged_state_config(state, project, task.task_state_config.as_deref());
        if task.entry_barrier_json.is_some() {
            return Ok(false);
        }
        if role_name == crate::workflow::default_roles::REVIEWER
            && !helpers::reviewer_dispatch_ready(&self.db, task, &state_config).await?
        {
            return Ok(false);
        }

        let agent = AgentRepo::get_by_id(&*self.db, agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", agent_id.to_owned()))?;
        match compute_effective_status(&self.db, &agent).await? {
            EffectiveStatus::Error
            | EffectiveStatus::Paused
            | EffectiveStatus::DaemonOffline
            | EffectiveStatus::DaemonUnavailable
            | EffectiveStatus::ConnectionDegraded
            | EffectiveStatus::ConnectionUnavailable
            | EffectiveStatus::SourceDisabled
            | EffectiveStatus::Deactivated => return Ok(false),
            EffectiveStatus::Active | EffectiveStatus::Busy => {}
        }
        if !has_running_execution_capacity(&self.db, &agent).await? {
            return Ok(false);
        }
        if deferred_dispatch::pending_until(task).is_some() {
            deferred_dispatch::clear(&self.db, task).await?;
        }

        let state_dispatch = dispatch_intent_from_workflow_dispatch(state.dispatch.as_ref());
        let selection = effective_prompt_selection(role_name, None, state_dispatch.as_ref());
        let dispatch_ctx = load_agent_dispatch_context(
            Arc::clone(&self.db),
            &task.id,
            role_name,
            &state.name,
            state_config,
            Some(selection.execution_policy.as_str()),
            workflow,
        )
        .await?;
        let (prompt, selection) =
            build_effective_prompt(&dispatch_ctx, None, state_dispatch.as_ref());
        let reviewer_snapshot = if role_name == crate::workflow::default_roles::REVIEWER {
            dispatch_ctx
                .prior_reviews
                .iter()
                .max_by_key(|review| (review.attempt_number, review.id.clone()))
                .map(|review| {
                    (
                        review.id.clone(),
                        review.execution_id.clone(),
                        review.attempt_number,
                        review.status.to_string(),
                        review.updated_at.clone(),
                        review.reviewer_execution_id.clone(),
                        review.auditor_execution_id.clone(),
                    )
                })
        } else {
            None
        };
        let dispatch_metadata = serde_json::json!({
            "target_role": role_name,
            "builder_id": selection.builder_id,
            "execution_policy": selection.execution_policy,
        });
        if self.is_stopped() {
            return Ok(false);
        }
        // Everything above was decided from the snapshot this scan listed. A
        // Task that has moved since then — most often because its own
        // execution finished and cascaded into the next state — must not
        // receive a role execution chosen for the state it has left: that
        // launches a second implementation attempt into the state's workspace
        // and blocks the role the Task now actually wants.
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        if current.version != task.version || current.status != task.status {
            tracing::debug!(
                task_id = %task.id,
                from_state = %task.status,
                to_state = %current.status,
                target_role = role_name,
                "task changed while dispatch was being prepared; leaving it to the next scan"
            );
            return Ok(false);
        }
        let mut admission = crate::task_service::execution_admission_for_task(
            &self.db,
            &current,
            &project.workflow_definition,
            role_name,
            Some(&agent),
            project.version,
        )
        .await?;
        if let Some((
            id,
            execution_id,
            attempt_number,
            status,
            updated_at,
            reviewer_execution_id,
            auditor_execution_id,
        )) = reviewer_snapshot
        {
            admission.expected_reviewer_parent_execution_id = Some(execution_id.clone());
            admission.expected_latest_review_candidate_execution_id = Some(execution_id);
            admission.expected_reviewer_id = Some(id);
            admission.expected_reviewer_attempt_number = Some(attempt_number);
            admission.expected_reviewer_status = Some(status);
            admission.expected_reviewer_updated_at = Some(updated_at);
            admission.expected_reviewer_execution_id = reviewer_execution_id;
            admission.expected_auditor_execution_id = auditor_execution_id;
        }
        self.task_service
            .dispatch_initial_role_execution_with_metadata_and_admission(
                &task.id,
                &agent.id,
                role_name,
                prompt.execution_input(None),
                Some(dispatch_metadata),
                admission,
            )
            .await?;
        Ok(true)
    }

    /// Settle a reviewer execution whose terminal event was lost before it
    /// reached the review cascade. A retry that was already scheduled is left
    /// alone so normal deferred dispatch can launch its replacement once due.
    async fn reconcile_terminal_reviewer_execution(
        &self,
        task_id: &str,
    ) -> Result<ReviewerReconciliation> {
        let page = ExecutionRepo::list_by_task_and_role(
            &*self.db,
            task_id,
            crate::workflow::default_roles::REVIEWER,
            PageRequest {
                cursor: None,
                limit: 1,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await?;
        let Some(execution) = page.items.into_iter().next() else {
            return Ok(ReviewerReconciliation::None);
        };
        if execution.status == ExecutionStatus::Running {
            return Ok(ReviewerReconciliation::None);
        }
        let reviews = ReviewRepo::list_by_task(&*self.db, task_id).await?;
        let Some(bound_review) =
            crate::task_service::execution::exact_review_for_execution(&execution, &reviews)
        else {
            return Ok(ReviewerReconciliation::NoExactReviewBinding);
        };
        if crate::task_service::execution::reviewer_execution_lacks_exact_review_binding(
            &execution,
            bound_review,
        ) {
            return Ok(ReviewerReconciliation::NoExactReviewBinding);
        }
        let Some(latest_review) = reviews
            .iter()
            .max_by_key(|review| (review.attempt_number, review.id.clone()))
        else {
            return Ok(ReviewerReconciliation::NoExactReviewBinding);
        };
        if bound_review.id != latest_review.id {
            return Ok(ReviewerReconciliation::NoExactReviewBinding);
        }
        if !helpers::latest_execution_awaits_completion_cascade(
            &self.db,
            task_id,
            crate::workflow::default_roles::REVIEWER,
        )
        .await?
        {
            // Auto-resumable attempts and attempts superseded by an explicit
            // role confirmation belong to the normal dispatch path below.
            return Ok(ReviewerReconciliation::None);
        }

        self.task_service
            .maybe_cascade_executor_completion(&execution.id)
            .await?;
        Ok(ReviewerReconciliation::Reconciled)
    }
}
