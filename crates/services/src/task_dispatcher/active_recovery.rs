use std::sync::Arc;

use api_types::{Actor, StateDefinition, StateKind, SystemComponent, WorkflowDefinition};
use db::{
    AgentRepo, DbError, ExecutionRepo, ExecutionStatus, PageRequest, Project, ReviewRepo, SortBy,
    SortOrder, Task, TaskRepo, TransitionLogRepo,
};

use crate::{
    agent_capacity::has_execution_capacity,
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
    const MERGE_RECOVERY_GRACE: chrono::Duration = chrono::Duration::minutes(2);
    const FAILED_REVIEW_RECOVERY_GRACE: chrono::Duration = chrono::Duration::minutes(2);

    pub(super) async fn recover_active_tasks(
        &self,
        project: &Project,
        workflow: &WorkflowDefinition,
    ) -> Result<u64> {
        crate::placement::admission::sweep_expired_reservations(&self.db, &db::now_rfc3339())
            .await?;
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
        for mut task in tasks {
            if self.is_stopped() {
                break;
            }
            let task_id = task.id.clone();
            let result: Result<()> = async {
                if self.task_service.expire_owner_wait(&task).await? {
                    return Ok(());
                }
                if db::WorkspacePlacementRepo::get_for_task(&*self.db, &task.id).await?
                    .is_some_and(|placement| matches!(placement.state, db::PlacementState::Disconnected | db::PlacementState::Cleaning)) {
                    return Ok(());
                }
                if task.entry_barrier_json.is_some()
                    && task
                        .error_annotation
                        .as_deref()
                        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
                        .is_some_and(|annotation| {
                            annotation["blocking_reason"] == "review_ci_infrastructure"
                        })
                {
                    if db::WorkspacePlacementRepo::get_for_task(&*self.db, &task.id)
                        .await?
                        .is_some_and(|placement| placement.state == db::PlacementState::Disconnected)
                    {
                        return Ok(());
                    }
                    if !deferred_dispatch::is_pending(&task, chrono::Utc::now()) {
                        match self
                            .task_service
                            .recover_task(
                                task.id.clone(),
                                api_types::RecoveryAction::RetryHook,
                                Some("retry review CI infrastructure".into()),
                                None,
                            )
                            .await
                        {
                            Ok(_) => dispatched += 1,
                            Err(error) => {
                                tracing::warn!(task_id = %task.id, %error, "review CI retry remains pending")
                            }
                        }
                    }
                    return Ok(());
                }
                if deferred_dispatch::queued_recovery(&task).is_some() {
                    return Ok(());
                }
                task = match crate::task_service::execution::clear_stale_plan_publication_claim(
                    &self.db, &self.task_service.workspace_backend_router(), &task,
                )
                .await
                {
                    Ok(task) => task,
                    Err(ServiceError::Db(DbError::VersionConflict)) => {
                        tracing::debug!(task_id = %task.id, "stale plan-publication cleanup lost version race");
                        return Ok(());
                    }
                    Err(error) => {
                        tracing::warn!(task_id = %task.id, %error, "stale plan-publication cleanup failed");
                        return Ok(());
                    }
                };
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
                        return Ok(());
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
                    return Ok(());
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
                        return Ok(());
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
                    return Ok(());
                }

                let Some(state) = task_workflow
                    .states
                    .iter()
                    .find(|state| state.name == task.status)
                else {
                    return Ok(());
                };
                if effective_role(state) == Some(crate::workflow::default_roles::REVIEWER) {
                    match self.recover_failed_review(&task).await {
                        Ok(true) => {
                            dispatched += 1;
                            return Ok(());
                        }
                        Ok(false) => {}
                        Err(ServiceError::Db(DbError::VersionConflict)) => {
                            tracing::debug!(task_id = %task.id, "failed review recovery lost version race");
                            return Ok(());
                        }
                        Err(error) => {
                            tracing::warn!(task_id = %task.id, %error, "failed review recovery failed");
                            return Ok(());
                        }
                    }
                }
                if state.kind == StateKind::Gate
                    && state
                        .hooks
                        .on_enter
                        .iter()
                        .any(|hook| hook.action == "run_merge")
                {
                    match self
                        .recover_merge_gate(project, &task_workflow, state, &task)
                        .await
                    {
                        Ok(true) => dispatched += 1,
                        Ok(false) => {}
                        Err(ServiceError::Db(DbError::VersionConflict)) => {
                            tracing::debug!(task_id = %task.id, "merge gate recovery lost version race");
                        }
                        Err(error) => {
                            tracing::warn!(task_id = %task.id, %error, "merge gate recovery failed");
                        }
                    }
                    return Ok(());
                }
                let active_plan_claim =
                    crate::task_service::execution::active_plan_publication_claim_owner(&task)?;
                if helpers::has_blocking_annotation(&task) && active_plan_claim.is_none() {
                    return Ok(());
                }
                if let Some(role_name) = effective_role(state) {
                    let reconciliation_result = if role_name == crate::workflow::default_roles::REVIEWER
                    {
                        self.reconcile_terminal_reviewer_execution(&task.id, project.version)
                            .await
                    } else {
                        self.reconcile_terminal_role_execution(&task, role_name, project.version)
                            .await
                    };
                    let reconciliation = match reconciliation_result {
                        Ok(reconciliation) => reconciliation,
                        Err(ServiceError::Db(DbError::VersionConflict)) => {
                            tracing::debug!(task_id = %task.id, "terminal reconciliation lost version race");
                            return Ok(());
                        }
                        Err(error) => {
                            tracing::warn!(
                                task_id = %task.id,
                                target_role = role_name,
                                %error,
                                "terminal execution reconciliation failed"
                            );
                            return Ok(());
                        }
                    };
                    if reconciliation == ReviewerReconciliation::Reconciled {
                        // Settle a lost terminal cascade before any dispatch-only
                        // eligibility checks can hide it or launch a replacement.
                        return Ok(());
                    }
                    if role_name != crate::workflow::default_roles::REVIEWER
                        && helpers::awaiting_human(&task)
                    {
                        if helpers::awaiting_human_is_authoritative(&task, state, role_name)
                            && crate::task_service::execution::planning_review_matches_current_state_entry(
                                &self.db,
                                &task,
                            )
                            .await?
                        {
                            return Ok(());
                        }
                        task = crate::task_service::execution::clear_stale_planning_review_metadata(
                            &self.db, &task,
                        )
                        .await?;
                    }
                }

                let is_coordination_root =
                    crate::task_hierarchy::coordination_root_has_subtasks(&self.db, &task).await?;
                let sequence_complete = is_coordination_root
                    && crate::task_hierarchy::coordination_root_sequence_complete(
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
                    return Ok(());
                }
                if deferred_dispatch::dispatch_disposition_is_current(&task, &task.status) {
                    // An unchanged deterministic blocker was already observed for
                    // this exact Task version and capability — skip the attempt
                    // and its warning entirely (F11). See the matching check in
                    // `dispatch_initial_tasks`.
                    return Ok(());
                }
                match self
                    .recover_active_task(project, &task_workflow, &task)
                    .await
                {
                    Ok(true) => {
                        self.clear_dispatch_disposition(&task).await?;
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
                    Err(error)
                        if crate::placement::admission_refusal_is_retryable(
                            &self.db, &task.id, &error,
                        )
                        .await? =>
                    {
                        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
                            .await?
                            .ok_or_else(|| ServiceError::not_found("task", &task.id))?;
                        self.task_service
                            .defer_placement_refusal(&current, &error)
                            .await?;
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
                        crate::workflow::engine::annotate_upgrade_dispatch_refusal(
                            &self.db,
                            &task.id,
                            &task.status,
                            &error,
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
                Ok(())
            }.await;
            if let Err(error) = result {
                tracing::warn!(%task_id, %error, "Task scan failed; continuing with the next Task");
            }
        }
        Ok(dispatched)
    }

    pub(super) async fn recover_failed_review(&self, task: &Task) -> Result<bool> {
        if helpers::has_blocking_annotation(task)
            || task.error_annotation.is_some()
            || helpers::awaiting_human(task)
            || (task.entry_barrier_json.is_some() && !task.entry_barrier_is_running())
            || !ExecutionRepo::list_running_by_task(&*self.db, &task.id)
                .await?
                .is_empty()
        {
            return Ok(false);
        }
        let reviews = ReviewRepo::list_by_task(&*self.db, &task.id).await?;
        let Some(review) = reviews
            .into_iter()
            .max_by_key(|review| review.attempt_number)
        else {
            return Ok(false);
        };
        if review.status != db::ReviewStatus::Failed {
            return Ok(false);
        }
        let transitions = TransitionLogRepo::list_by_task(&*self.db, &task.id).await?;
        let Some(entry) = transitions
            .last()
            .filter(|entry| entry.to_state == task.status)
        else {
            return Ok(false);
        };
        // User routing is a deliberate management action. Never reinterpret
        // a parked human review, or a verdict from a previous state entry.
        if entry.triggered_by.starts_with("user:") {
            return Ok(false);
        }
        let (Ok(entered_at), Ok(started_at), Ok(failed_at)) = (
            chrono::DateTime::parse_from_rfc3339(&entry.created_at),
            chrono::DateTime::parse_from_rfc3339(&review.started_at),
            chrono::DateTime::parse_from_rfc3339(&review.updated_at),
        ) else {
            return Ok(false);
        };
        if started_at < entered_at
            || chrono::Utc::now().signed_duration_since(failed_at)
                < Self::FAILED_REVIEW_RECOVERY_GRACE
        {
            return Ok(false);
        }
        if let Some(raw_barrier) = task.entry_barrier_json.as_deref() {
            let barrier: serde_json::Value = serde_json::from_str(raw_barrier)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
            let Some(barrier_started_at) = barrier
                .get("retry_started_at")
                .or_else(|| barrier.get("started_at"))
                .and_then(serde_json::Value::as_str)
                .and_then(|time| chrono::DateTime::parse_from_rfc3339(time).ok())
            else {
                return Ok(false);
            };
            if barrier_started_at > started_at {
                // A newer entry retry has not produced its own verdict yet.
                return Ok(false);
            }
        }
        let Some(_cascade_slot) = self.task_service.claim_completion_cascade(&task.id) else {
            return Ok(false);
        };
        // Claim this exact Task snapshot before routing or installing the
        // exhausted-budget annotation. Concurrent recovery loses this CAS.
        let task = TaskRepo::set_entry_barrier(
            &*self.db,
            &task.id,
            task.version,
            None,
            &db::now_rfc3339(),
        )
        .await?;
        let latest = ReviewRepo::list_by_task(&*self.db, &task.id)
            .await?
            .into_iter()
            .max_by_key(|review| review.attempt_number);
        if latest.is_none_or(|latest| {
            latest.id != review.id
                || latest.status != db::ReviewStatus::Failed
                || latest.updated_at != review.updated_at
        }) || !ExecutionRepo::list_running_by_task(&*self.db, &task.id)
            .await?
            .is_empty()
        {
            return Ok(false);
        }
        let task = if task.review_passed_at.is_some() {
            TaskRepo::set_review_passed_at_cas(
                &*self.db,
                &task.id,
                task.version,
                None,
                &db::now_rfc3339(),
            )
            .await?
        } else {
            task
        };
        tracing::info!(task_id = %task.id, review_id = %review.id, "routing stranded failed review");
        let execution = ExecutionRepo::get_by_id(&*self.db, &review.execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", review.execution_id.clone()))?;
        // Share finding routing with terminal reviewer reconciliation. CI-only
        // failures still spend the normal remediation budget; owner findings park.
        self.task_service
            .reconcile_settled_reviewer_completion(&task, &execution, &review, false)
            .await?;
        Ok(true)
    }

    async fn recover_merge_gate(
        &self,
        project: &Project,
        workflow: &WorkflowDefinition,
        state: &StateDefinition,
        task: &Task,
    ) -> Result<bool> {
        if helpers::has_blocking_annotation(task)
            || helpers::awaiting_human(task)
            || task.entry_barrier_is_running()
            || state
                .gate_config
                .as_ref()
                .is_some_and(|config| config.requires_user_approval())
            || !ExecutionRepo::list_running_by_task(&*self.db, &task.id)
                .await?
                .is_empty()
        {
            return Ok(false);
        }
        if !self.task_service.merge_hook_available(&task.id) {
            return Ok(false);
        }
        let transitions = TransitionLogRepo::list_by_task(&*self.db, &task.id).await?;
        let Some(entry) = transitions
            .iter()
            .rev()
            .find(|entry| entry.to_state == task.status)
        else {
            return Ok(false);
        };
        let Ok(entered_at) = chrono::DateTime::parse_from_rfc3339(&entry.created_at) else {
            return Ok(false);
        };
        if chrono::Utc::now().signed_duration_since(entered_at) < Self::MERGE_RECOVERY_GRACE {
            return Ok(false);
        }
        // A reviewer completion owns this slot through the whole inline
        // cascade, including time between the merge hook and its done hop.
        let Some(_cascade_slot) = self.task_service.claim_completion_cascade(&task.id) else {
            return Ok(false);
        };
        tracing::info!(task_id = %task.id, state = %task.status, "re-driving stale merge gate");
        self.task_service
            .retry_merge_state_entry(task, project, workflow, "recover interrupted merge gate")
            .await?;
        Ok(true)
    }

    /// Reconcile a completed non-reviewer before considering a fresh attempt.
    /// This closes the crash window between terminal settlement and the
    /// workflow cascade, including publication of a frozen plan candidate.
    async fn reconcile_terminal_role_execution(
        &self,
        task: &Task,
        role_name: &str,
        project_version: i64,
    ) -> Result<ReviewerReconciliation> {
        let claimed_execution_id =
            crate::task_service::execution::active_plan_publication_claim_owner(task)?;
        let latest = if let Some(execution_id) = claimed_execution_id.as_deref() {
            ExecutionRepo::get_by_id(&*self.db, execution_id)
                .await?
                .filter(|execution| execution.task_id == task.id)
        } else {
            let mut latest = None;
            for role in helpers::execution_guard_roles(role_name) {
                let page = ExecutionRepo::list_by_task_and_role(
                    &*self.db,
                    &task.id,
                    role,
                    PageRequest {
                        cursor: None,
                        limit: 1,
                        include_total: false,
                        sort_by: SortBy::CreatedAt,
                        sort_order: SortOrder::Desc,
                    },
                )
                .await?;
                if let Some(candidate) = page.items.into_iter().next() {
                    let replace = latest.as_ref().is_none_or(|current: &db::Execution| {
                        (&candidate.created_at, &candidate.id) > (&current.created_at, &current.id)
                    });
                    if replace {
                        latest = Some(candidate);
                    }
                }
            }
            latest
        };
        let Some(execution) = latest else {
            return Ok(ReviewerReconciliation::None);
        };
        if execution.status == ExecutionStatus::Failed && claimed_execution_id.is_none() {
            return self
                .reconcile_failed_role_execution(task, role_name, &execution, project_version)
                .await;
        }
        if execution.status != ExecutionStatus::Completed {
            return Ok(ReviewerReconciliation::None);
        }
        if claimed_execution_id.is_none()
            && helpers::execution_superseded_by_role_assignment(
                &self.db, &task.id, role_name, &execution,
            )
            .await?
        {
            return Ok(ReviewerReconciliation::None);
        }
        if claimed_execution_id.is_none()
            && !crate::task_service::execution::execution_belongs_to_current_state_entry(
                &self.db, task, &execution,
            )
            .await?
        {
            return Ok(ReviewerReconciliation::None);
        }
        let superseded_project_authority =
            crate::task_service::execution_dispatch_project_version(&execution)
                != Some(project_version);
        if claimed_execution_id.is_none()
            && crate::task_service::execution::execution_completion_settled_for_current_state_entry(
                &self.db, task, &execution,
            )
            .await?
        {
            // A same-state settlement is a durable replay receipt, but its
            // outcome belongs to the Project revision that dispatched it. A
            // later workflow edit must be able to launch a replacement under
            // the new revision instead of reporting Reconciled forever.
            return Ok(if superseded_project_authority {
                ReviewerReconciliation::None
            } else {
                ReviewerReconciliation::Reconciled
            });
        }

        self.task_service
            .maybe_cascade_executor_completion(&execution.id)
            .await?;
        Ok(if superseded_project_authority {
            // Cascade ignored this superseded execution (and discarded any
            // private plan authority it owned). Continue to normal replacement
            // dispatch under the current workflow revision.
            ReviewerReconciliation::None
        } else {
            ReviewerReconciliation::Reconciled
        })
    }

    /// Heal a run that died without its failure reaching the Task (for
    /// example the process or database hit a full disk between the execution
    /// row turning `failed` and the executor-failure annotation). The Task is
    /// still in its working state with no running execution and no blocker, so
    /// it offers no recovery action and is never re-dispatched.
    ///
    /// Delegates to the same helper the live failure path uses, so the
    /// annotation shape, retry budget and events are identical. Idempotent:
    /// once the helper writes the blocker (`blocked_json`) the dispatcher's
    /// blocking-annotation gate skips the Task, and a scheduled retry is
    /// recorded via `last_execution_failure_execution_id` / a pending deferred
    /// dispatch, both checked here.
    async fn reconcile_failed_role_execution(
        &self,
        task: &Task,
        role_name: &str,
        execution: &db::Execution,
        project_version: i64,
    ) -> Result<ReviewerReconciliation> {
        if crate::project_environment::is_environment_pre_dispatch_failure(execution)
            || !crate::task_service::execution::should_block_task_for_failed_execution(execution)
            || role_name == crate::workflow::default_roles::REVIEWER
            || helpers::has_blocking_annotation(task)
            || deferred_dispatch::is_pending(task, chrono::Utc::now())
            || crate::task_service::execution_dispatch_project_version(execution)
                != Some(project_version)
        {
            return Ok(ReviewerReconciliation::None);
        }
        // The live path already handled this failure by scheduling a retry.
        let retry_recorded = db::TaskMetadata::parse(task.metadata_json.as_deref())
            .ok()
            .and_then(|metadata| {
                metadata
                    .extra
                    .get("last_execution_failure_execution_id")
                    .and_then(|value| value.as_str().map(str::to_owned))
            })
            .as_deref()
            == Some(execution.id.as_str());
        if retry_recorded {
            return Ok(ReviewerReconciliation::None);
        }
        if helpers::has_running_execution_for_roles(
            &self.db,
            &task.id,
            &helpers::execution_guard_roles(role_name),
        )
        .await?
            || helpers::execution_superseded_by_role_assignment(
                &self.db, &task.id, role_name, execution,
            )
            .await?
            || !crate::task_service::execution::execution_belongs_to_current_state_entry(
                &self.db, task, execution,
            )
            .await?
        {
            return Ok(ReviewerReconciliation::None);
        }
        // Version-CAS'd on the latest-execution authority inside the helper:
        // a concurrently launched execution makes it a no-op.
        self.task_service
            .annotate_executor_failure_block(execution)
            .await?;
        Ok(ReviewerReconciliation::Reconciled)
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
        // The role already finished and the Task is waiting on a human
        // decision (e.g. plan review); relaunching it would loop forever.
        if role_name != crate::workflow::default_roles::REVIEWER
            && helpers::awaiting_human_is_authoritative(task, state, role_name)
            && crate::task_service::execution::planning_review_matches_current_state_entry(
                &self.db, task,
            )
            .await?
        {
            return Ok(false);
        }
        if role_name == crate::workflow::default_roles::REVIEWER {
            self.task_service.ensure_task_reviewable(task).await?;
        } else {
            self.task_service.ensure_task_runnable(task).await?;
        }
        if crate::task_hierarchy::coordination_root_has_subtasks(&self.db, task).await?
            && !crate::task_hierarchy::RootRolePolicy::for_workflow(workflow)
                .allows_execution(&state.name, role_name)
        {
            return Ok(false);
        }
        if !crate::task_hierarchy::subtask_dispatch_ready(&self.db, task).await? {
            return Ok(false);
        }
        let reviewer_reconciliation = if role_name == crate::workflow::default_roles::REVIEWER {
            self.reconcile_terminal_reviewer_execution(&task.id, project.version)
                .await?
        } else {
            self.reconcile_terminal_role_execution(task, role_name, project.version)
                .await?
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
                crate::task_hierarchy::effective_role_assignment(&self.db, task, role_name)
                    .await?
                    .map(|resolved| resolved.assignment);
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
            crate::task_hierarchy::effective_role_assignment(&self.db, task, role_name)
                .await?
                .map(|resolved| resolved.assignment)
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

        let workspace = db::WorkspaceRepo::get_by_task_id(
            &*self.db,
            task.parent_task_id.as_deref().unwrap_or(&task.id),
        )
        .await?;
        if let Some(workspace) = workspace.as_ref() {
            if db::WorkspacePlacementRepo::get_by_workspace_id(&*self.db, &workspace.id)
                .await?
                .is_some_and(|placement| {
                    matches!(
                        placement.state,
                        db::PlacementState::Disconnected
                            | db::PlacementState::Cleaning
                            | db::PlacementState::Reserved
                            | db::PlacementState::Preparing
                    )
                })
            {
                return Ok(false);
            }
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
        if !matches!(
            compute_effective_status(&self.db, &agent, None).await?,
            EffectiveStatus::Active | EffectiveStatus::Busy
        ) {
            return Ok(false);
        }
        if !has_execution_capacity(
            &self.db,
            &agent,
            workspace.as_ref().map(|workspace| workspace.id.as_str()),
        )
        .await?
        {
            return Ok(false);
        }
        if deferred_dispatch::pending_until(task).is_some() {
            deferred_dispatch::clear(&self.db, task).await?;
        }

        let state_dispatch = dispatch_intent_from_workflow_dispatch(state.dispatch.as_ref());
        let selection = effective_prompt_selection(role_name, None, state_dispatch.as_ref());
        // A reviewer execution is bound to its Review attempt inside the
        // admitting transaction, so recovery has to establish that attempt
        // the same way a transition into review does. Without it the bind
        // step fails closed as a bare version conflict, which this scan then
        // logs as a lost race and retries forever: a Task parked in review
        // with no Review row -- a read-only Task whose CI hook skipped, or
        // any Task whose reviewer never launched -- could never be recovered.
        if role_name == crate::workflow::default_roles::REVIEWER {
            self.ensure_review_attempt_for_recovery(project, task, &state.name)
                .await?;
        }
        let dispatch_ctx = load_agent_dispatch_context(
            Arc::clone(&self.db),
            &self.task_service.workspace_backend_router(),
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

    /// Establish the Review attempt a recovered reviewer dispatch binds to.
    ///
    /// Mirrors the transition-time helper: an attempt already Running or
    /// AwaitingHuman for the current candidate is left alone, and a Task with
    /// no implementation candidate gets no attempt, because a Review has no
    /// authority without the execution it reviews.
    async fn ensure_review_attempt_for_recovery(
        &self,
        project: &Project,
        task: &Task,
        to_state: &str,
    ) -> Result<()> {
        if task.review_passed_at.is_some() {
            return Ok(());
        }
        let Some(candidate) =
            crate::task_service::latest_executor_execution_for_task(&self.db, task).await?
        else {
            // Reviewing nothing is not a transient condition: a Task in
            // review with no implementation attempt has nothing to bind a
            // Review to, and retrying every scan only hides that. Refuse
            // deterministically so the Task parks with the actual reason.
            return Err(ServiceError::conflict(format!(
                "task {} is in review with no implementation execution to review",
                task.id
            )));
        };
        let reviews = ReviewRepo::list_by_task(&*self.db, &task.id).await?;
        let current = reviews
            .iter()
            .max_by_key(|review| (review.attempt_number, review.id.clone()));
        if current.is_some_and(|review| {
            review.execution_id == candidate.id
                && matches!(
                    review.status,
                    db::ReviewStatus::Running | db::ReviewStatus::AwaitingHuman
                )
        }) {
            return Ok(());
        }
        let now = db::now_rfc3339();
        ReviewRepo::create_with_task_authority(
            &*self.db,
            db::CreateReview {
                id: db::new_uuid_v4(),
                task_id: task.id.clone(),
                execution_id: candidate.id.clone(),
                attempt_number: 0,
                status: db::ReviewStatus::Running,
                step_results_json: serde_json::json!({ "ci_steps": [] }).to_string(),
                started_at: now.clone(),
                created_at: now.clone(),
                updated_at: now,
            },
            task.version,
            to_state,
            Some(project.version),
            Some(project.workflow_definition.as_str()),
            Some(candidate.id.as_str()),
        )
        .await?;
        Ok(())
    }

    /// Settle a reviewer execution whose terminal event was lost before it
    /// reached the review cascade. A retry that was already scheduled is left
    /// alone so normal deferred dispatch can launch its replacement once due.
    async fn reconcile_terminal_reviewer_execution(
        &self,
        task_id: &str,
        project_version: i64,
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
        if crate::task_service::execution_dispatch_project_version(&execution)
            != Some(project_version)
        {
            // The completion cascade intentionally ignores terminal effects
            // admitted by an older Project revision. That inert success is
            // not reconciliation: let normal recovery launch a replacement
            // reviewer under the current workflow authority.
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
