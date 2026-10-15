use super::*;

const AUTOMATIC_REVIEW_RECOVERY_TRIGGER: &str = "automatic_review_recovery";

tokio::task_local! {
    static AUTOMATIC_REVIEW_RECOVERY_TASK: String;
}

struct CapacityRetry<'a> {
    retry_at: Option<&'a str>,
    reason: &'static str,
}

enum ExecutionRetryDisposition {
    Scheduled,
    NotScheduled(&'static str),
}

impl ExecutionRetryDisposition {
    fn is_scheduled(&self) -> bool {
        matches!(self, Self::Scheduled)
    }
}

impl TaskService {
    /// Settle one terminal execution. The inline completion path and the
    /// dispatcher's reconciliation of terminal executions both call this, and
    /// a reviewer's cascade can run for seconds (the clean-checkout setup and
    /// checks). A second caller for the same Task waits for that cascade, then
    /// retries against current authority. This keeps duplicate delivery
    /// idempotent without dropping a successor role's terminal completion.
    pub async fn maybe_cascade_executor_completion(&self, execution_id: &str) -> Result<()> {
        let _command_task_id = ExecutionRepo::get_by_id(&*self.db, execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", execution_id.to_owned()))?
            .task_id;
        if !db::task_writer::owns_task(&_command_task_id) {
            return self
                .request_task_command(
                    &_command_task_id,
                    "maybe_cascade_executor_completion",
                    serde_json::json!([execution_id]),
                    false,
                )
                .await;
        }

        self.cascade_executor_completion(execution_id).await
    }

    /// A sweep must never wait behind another Task cascade while holding an owner permit.
    pub(crate) async fn try_cascade_executor_completion(&self, execution_id: &str) -> Result<bool> {
        let Some(execution) = ExecutionRepo::get_by_id(&*self.db, execution_id).await? else {
            return Ok(true);
        };
        self.enqueue_task_command(
            &execution.task_id,
            "maybe_cascade_executor_completion",
            serde_json::json!([execution_id]),
            false,
        )
        .await?;
        let worker = self.task_step_worker();
        db::task_writer::TaskStepExecutor::drive_inline(&*worker, &execution.task_id).await?;
        Ok(true)
    }

    async fn cascade_executor_completion(&self, execution_id: &str) -> Result<()> {
        let execution = match ExecutionRepo::get_by_id(&*self.db, execution_id).await? {
            Some(execution) => execution,
            None => return Ok(()),
        };
        let task_snapshot = TaskRepo::get_by_id(&*self.db, &execution.task_id, false).await?;
        if task_snapshot.as_ref().is_some_and(|task| {
            task.error_annotation
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                .is_some_and(|a| a["type"] == "manual_stop")
        }) {
            return Ok(());
        }
        if execution.role == crate::workflow::default_roles::REVIEWER {
            if execution.status == ExecutionStatus::Running {
                return Ok(());
            }
            if let Some(task) = task_snapshot.as_ref() {
                let project_version = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                    .await?
                    .map(|project| project.version);
                if super::super::execution_dispatch_project_version(&execution) != project_version {
                    tracing::info!(
                        task_id = %task.id,
                        execution_id = %execution.id,
                        dispatch_project_version = ?super::super::execution_dispatch_project_version(&execution),
                        current_project_version = ?project_version,
                        "ignoring reviewer terminal effects from a superseded Project revision"
                    );
                    return Ok(());
                }
            }
            return self.maybe_cascade_reviewer_completion(&execution).await;
        }
        if execution.status != ExecutionStatus::Completed {
            self.discard_brokered_execution_plan_stage(&execution)
                .await?;
            return Ok(());
        }

        if execution.role == "interactive" {
            if let Some(task) = task_snapshot.as_ref() {
                self.ingest_terminal_execution_outbox(task, &execution)
                    .await?;
            }
            return Ok(());
        }

        let mut task = match task_snapshot {
            Some(task) => task,
            None => {
                self.discard_brokered_execution_plan_stage(&execution)
                    .await?;
                return Ok(());
            }
        };
        if super::active_plan_publication_claim_owner(&task)?.as_deref()
            == Some(execution.id.as_str())
            && !super::plan_publication_matches_current_state_entry(&self.db, &task, &execution.id)
                .await?
        {
            // The transition authorized by this claim already committed, but
            // cleanup crashed and the Task later re-entered the same named
            // state. The published plan is authoritative; remove only the old
            // claim/private bytes and never roll the canonical file back.
            self.discard_brokered_execution_plan_stage(&execution)
                .await?;
            super::clear_settled_plan_publication(&self.db, &task.id, &execution.id, &task.status)
                .await?;
            return Ok(());
        }
        let project = match ProjectRepo::get_by_id(&*self.db, &task.project_id).await? {
            Some(project) => project,
            None => {
                self.abandon_brokered_plan_authority(&task, &execution)
                    .await?;
                return Ok(());
            }
        };
        let brokered_plan = super::execution_uses_brokered_plan(&execution);
        let dispatch_project_version = if brokered_plan {
            super::brokered_plan_dispatch_project_version(&execution)
        } else {
            super::super::execution_dispatch_project_version(&execution)
        };
        if dispatch_project_version != Some(project.version) {
            // Every workflow-role terminal effect is interpreted under the
            // immutable Project revision that admitted the execution. Missing
            // or malformed authority is intentionally fail-closed; only a
            // brokered plan has private publication state to roll back here.
            if brokered_plan {
                self.abandon_brokered_plan_authority(&task, &execution)
                    .await?;
            }
            tracing::info!(
                task_id = %task.id,
                execution_id = %execution.id,
                ?dispatch_project_version,
                current_project_version = project.version,
                "ignoring terminal effects from a superseded Project revision"
            );
            return Ok(());
        }
        if super::execution_completion_settled_for_current_state_entry(&self.db, &task, &execution)
            .await?
        {
            if super::pending_plan_publication_cleanup_owner(&task)?.as_deref()
                == Some(execution.id.as_str())
            {
                super::cleanup_execution_plan_private_files(
                    &self.db,
                    &self.workspace_backend_router,
                    &task,
                    &execution.id,
                )
                .await?;
                super::clear_plan_publication_cleanup(&self.db, &task, &execution.id).await?;
            }
            return Ok(());
        }
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        let Some(current_state) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
        else {
            self.abandon_brokered_plan_authority(&task, &execution)
                .await?;
            return Ok(());
        };
        let Some(effective_role) = crate::workflow::effective_role(current_state) else {
            self.abandon_brokered_plan_authority(&task, &execution)
                .await?;
            return Ok(());
        };
        let role_matches = execution.role == effective_role
            || (effective_role == crate::workflow::default_roles::CODER
                && execution.role == "executor");
        if !role_matches {
            self.abandon_brokered_plan_authority(&task, &execution)
                .await?;
            return Ok(());
        }
        if !self
            .execution_owns_current_role_attempt(&task, &execution)
            .await?
        {
            self.abandon_brokered_plan_authority(&task, &execution)
                .await?;
            return Ok(());
        }
        // Only a successful terminal CAS that still owns the current role
        // attempt and Project revision can publish agent output. Superseded
        // attempts leave their outbox private and are cleaned up by the
        // brokered-plan path above.
        let outbox_report = self
            .ingest_terminal_execution_outbox(&task, &execution)
            .await?;
        // A step check has its own settlement: a verdict, never a plan (the
        // lead's checklist exemption is for its run on the root only).
        if Self::is_step_check(&task, &execution, current_state) {
            return self
                .settle_step_check(&task, &execution, &workflow, current_state)
                .await;
        }
        // A lead run that asked the owner a question is never the planning
        // gate's Accept: the root waits for the answer and the lead runs
        // again with it (`lead.request_input`).
        if crate::task_service::lead::lead_asked_input(&task, &execution.id) {
            self.abandon_brokered_plan_authority(&task, &execution)
                .await?;
            return Ok(());
        }
        let Some(target) = workflow
            .auto_transition_target(&task.status)
            .map(str::to_owned)
        else {
            self.abandon_brokered_plan_authority(&task, &execution)
                .await?;
            return Ok(());
        };

        let is_planning_gate = task.status == crate::workflow::default_states::PLANNING
            && execution.role == crate::workflow::default_roles::PLANNER
            && current_state.kind == api_types::StateKind::Gate;
        let gate_requires_user_approval = is_planning_gate
            && current_state
                .gate_config
                .as_ref()
                .is_some_and(|gate_config| gate_config.requires_user_approval());

        let planning_marker_matches_execution = if is_planning_gate {
            let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "invalid task metadata for {}: {error}",
                    task.id
                ))
            })?;
            metadata
                .extra
                .get("awaiting_human")
                .and_then(Value::as_bool)
                == Some(true)
                && metadata
                    .extra
                    .get("awaiting_human_reason")
                    .and_then(Value::as_str)
                    == Some("plan_review")
                && metadata
                    .extra
                    .get("planning_execution_id")
                    .and_then(Value::as_str)
                    == Some(execution.id.as_str())
        } else {
            false
        };

        if brokered_plan && !planning_marker_matches_execution {
            // Remote terminal settlement may have committed immediately
            // before a process exit. Recovery freezes that winning outbox
            // above before deciding that its plan candidate is missing.
            if outbox_report.plan_rejected {
                // The agent-writable outbox is the only diagnostic copy of a
                // candidate that failed validation. Keep it intact while the
                // bounded guard path retries or records the terminal blocker.
                return self
                    .handle_executor_completion_guard_rejection(
                        &execution,
                        &task,
                        current_state,
                        "planning_plan_ready",
                        "Forge could not freeze this execution's plan candidate safely",
                    )
                    .await;
            }
        }
        let brokered_workspace = if brokered_plan && !planning_marker_matches_execution {
            match execution.workspace_id.as_deref() {
                Some(workspace_id) => WorkspaceRepo::get_by_id(&*self.db, workspace_id).await?,
                None => {
                    let workspace_task_id = task.parent_task_id.as_deref().unwrap_or(&task.id);
                    WorkspaceRepo::get_by_task_id(&*self.db, workspace_task_id).await?
                }
            }
        } else {
            None
        };
        if brokered_plan && !planning_marker_matches_execution && brokered_workspace.is_none() {
            let result = self
                .handle_executor_completion_guard_rejection(
                    &execution,
                    &task,
                    current_state,
                    "planning_plan_ready",
                    "this completed execution's workspace is no longer available",
                )
                .await;
            if result.is_ok() {
                self.discard_brokered_execution_plan_stage(&execution)
                    .await?;
            }
            return result;
        }
        if brokered_plan && !planning_marker_matches_execution {
            match super::claim_plan_publication(&self.db, &task, &execution, project.version)
                .await?
            {
                super::PlanPublicationClaimOutcome::Claimed(claimed_task) => {
                    task = *claimed_task;
                }
                super::PlanPublicationClaimOutcome::VersionRace => {
                    // Preserve the only frozen bytes. The next dispatcher pass
                    // retries the claim from the newer Task snapshot.
                    return Ok(());
                }
                super::PlanPublicationClaimOutcome::OwnedByOther => {
                    // A durable same-state owner proves this candidate lost.
                    self.discard_brokered_execution_plan_stage(&execution)
                        .await?;
                    return Ok(());
                }
                super::PlanPublicationClaimOutcome::StaleWorkflowAuthority => {
                    self.abandon_brokered_plan_authority(&task, &execution)
                        .await?;
                    return Ok(());
                }
            }
        }

        // A CLI candidate was frozen outside the agent-writable outbox before
        // terminal settlement. Publish only after this completed execution is
        // still the effective role for the current state. Keep the stage until
        // the transition/approval marker commits so a crash can replay this
        // exact snapshot instead of accepting an older canonical plan.
        if brokered_plan && !planning_marker_matches_execution {
            let workspace = brokered_workspace
                .as_ref()
                .expect("brokered workspace checked before plan publication");
            let resolved = self
                .workspace_backend_router
                .resolve(&self.db, workspace)
                .await?;
            let plan = crate::plan_artifact::ExecutionPlan::new(&self.db, &resolved);
            match plan.publish(&execution).await {
                Ok(true) => {
                    task = TaskRepo::get_by_id(&*self.db, &task.id, false)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", &task.id))?;
                }
                Ok(false) => {
                    // A coordination root's lead plans through its subtasks:
                    // the root has no checklist of its own to complete (see
                    // `ensure_planning_plan_ready_before_leaving`), so only a
                    // candidate it wrote and Forge rejected holds it back.
                    // Exactly the lead's run: the planning role, on a root
                    // with subtasks, in the root's planning state. Every
                    // other run keeps its plan contract.
                    let plans_through_subtasks = task.parent_task_id.is_none()
                        && {
                            let policy =
                                crate::task_hierarchy::RootRolePolicy::for_workflow(&workflow);
                            policy.planning_state() == Some(task.status.as_str())
                                && policy.planning_role() == Some(execution.role.as_str())
                        }
                        && crate::task_hierarchy::coordination_root_has_subtasks(&self.db, &task)
                            .await?;
                    let rejected =
                        plan.rejected_candidate(&execution.id)
                            .await
                            .map_err(|error| {
                                error.into_service_error("execution plan candidate is unreadable")
                            })?;
                    let candidate_required = rejected
                        || (!plans_through_subtasks
                            && (execution.role == crate::workflow::default_roles::PLANNER
                                || task.plan.as_deref().is_some_and(|plan| {
                                    !crate::plan_artifact::parse_plan_markdown(plan)
                                        .items
                                        .is_empty()
                                })
                                || crate::plan_artifact::read_plan_text_for_resolved_workspace(
                                    &resolved,
                                )
                                .await
                                .map_err(|error| {
                                    ServiceError::invalid_operation(format!(
                                        "canonical plan artifact is unreadable: {error}"
                                    ))
                                })?
                                .is_some()));
                    if candidate_required {
                        self.discard_brokered_execution_plan_stage(&execution)
                            .await?;
                        task = super::release_plan_publication(&self.db, &task, &execution).await?;
                        let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(
                            |error| {
                                ServiceError::invalid_operation(format!(
                                    "invalid task metadata for {}: {error}",
                                    task.id
                                ))
                            },
                        )?;
                        if metadata
                            .extra
                            .get("awaiting_human_reason")
                            .and_then(Value::as_str)
                            == Some("plan_review")
                        {
                            task = super::set_planning_awaiting_review_metadata(
                                &self.db, &task, None, false,
                            )
                            .await?;
                        }
                        return self
                            .handle_executor_completion_guard_rejection(
                                &execution,
                                &task,
                                current_state,
                                "planning_plan_ready",
                                "this execution did not produce its required checklist plan candidate",
                            )
                            .await;
                    }
                    // Planning may be optional, so a coder can legitimately
                    // run without a plan. If a canonical plan exists, its
                    // private seeded copy is required above.
                }
                Err(error) => {
                    let reason =
                        format!("Forge could not publish this execution's plan candidate: {error}");
                    return Err(ServiceError::invalid_operation(reason));
                }
            }
        }

        // Planning's required artifact is a completion contract, including
        // for workflows that put a human approval boundary after the planner.
        // Route a missing, unreadable, or checklist-free plan through the
        // existing bounded guard-retry machinery; exhaustion leaves one
        // visible blocker instead of an approval marker that can never pass.
        if task.status == crate::workflow::default_states::PLANNING {
            match self
                .ensure_planning_plan_ready_before_leaving(&task, &target, &workflow, false)
                .await
            {
                Ok(()) => {}
                Err(ServiceError::InvalidOperation { message }) => {
                    if brokered_plan && !planning_marker_matches_execution {
                        self.restore_brokered_execution_plan(&task, &execution)
                            .await?;
                        self.discard_brokered_execution_plan_stage(&execution)
                            .await?;
                        task = super::release_plan_publication(&self.db, &task, &execution).await?;
                    }
                    let metadata =
                        TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
                            ServiceError::invalid_operation(format!(
                                "invalid task metadata for {}: {error}",
                                task.id
                            ))
                        })?;
                    if metadata
                        .extra
                        .get("awaiting_human_reason")
                        .and_then(Value::as_str)
                        == Some("plan_review")
                    {
                        task = super::set_planning_awaiting_review_metadata(
                            &self.db, &task, None, false,
                        )
                        .await?;
                    }
                    let result = self
                        .handle_executor_completion_guard_rejection(
                            &execution,
                            &task,
                            current_state,
                            "planning_plan_ready",
                            &message,
                        )
                        .await;
                    return result;
                }
                Err(error) => return Err(error),
            }

            if gate_requires_user_approval {
                if !planning_marker_matches_execution {
                    if brokered_plan {
                        match super::settle_plan_publication_for_review(&self.db, &task, &execution)
                            .await
                        {
                            Ok(settled_task) => task = settled_task,
                            Err(ServiceError::Db(DbError::VersionConflict)) => {
                                self.abandon_brokered_plan_if_project_authority_stale(
                                    &task.id, &execution,
                                )
                                .await?;
                                return Ok(());
                            }
                            Err(error) => return Err(error),
                        }
                    } else {
                        super::set_planning_awaiting_review_metadata(
                            &self.db,
                            &task,
                            Some(&execution.id),
                            true,
                        )
                        .await?;
                    }
                }
                if brokered_plan {
                    self.discard_brokered_execution_plan_stage(&execution)
                        .await?;
                    super::clear_plan_publication_cleanup(&self.db, &task, &execution.id).await?;
                }
                return Ok(());
            }
        }

        // Preserve existing custom-Gate semantics. This automatic Gate
        // completion contract is specific to the built-in planning role.
        let auto_advances = matches!(current_state.kind, api_types::StateKind::Active)
            || (is_planning_gate && !gate_requires_user_approval);
        if !auto_advances {
            if brokered_plan && !planning_marker_matches_execution {
                match super::settle_plan_publication_without_transition(&self.db, &task, &execution)
                    .await
                {
                    Ok(settled_task) => task = settled_task,
                    Err(ServiceError::Db(DbError::VersionConflict)) => {
                        self.abandon_brokered_plan_if_project_authority_stale(&task.id, &execution)
                            .await?;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
            }
            if brokered_plan {
                self.discard_brokered_execution_plan_stage(&execution)
                    .await?;
                super::clear_plan_publication_cleanup(&self.db, &task, &execution.id).await?;
            }
            return Ok(());
        }
        if let Some(summary) = execution.summary.as_deref().map(str::trim) {
            if !summary.is_empty() {
                let content = format!("Agent completed execution: {summary}");
                if let Some(agent_id) = execution.agent_id.as_deref() {
                    self.create_agent_comment(&task.id, agent_id, content)
                        .await?;
                } else {
                    self.create_system_comment(&task.id, content).await?;
                }
            }
        }

        let from = task.status.clone();
        match self
            .transition_with_plan_publication(
                task.id.clone(),
                target.clone(),
                task.version,
                &execution.id,
            )
            .await
        {
            Ok(_) => {
                if brokered_plan {
                    self.discard_brokered_execution_plan_stage(&execution)
                        .await?;
                    if let Err(error) = super::clear_settled_plan_publication(
                        &self.db,
                        &task.id,
                        &execution.id,
                        &from,
                    )
                    .await
                    {
                        tracing::warn!(task_id = %task.id, %error, "failed to clear settled plan publication claim");
                    }
                }
                if let Err(error) = self.clear_workflow_guard_retry_metadata(&task).await {
                    tracing::warn!(
                        task_id = %task.id,
                        %error,
                        "failed to clear workflow guard retry metadata"
                    );
                }
                self.publish(ForgeEvent {
                    event_type: "task.auto_transitioned".to_owned(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskAutoTransitioned {
                        task_id: task.id.clone(),
                        from,
                        to: target,
                        reason: "executor_completed".to_owned(),
                    },
                });
                Ok(())
            }
            Err(ServiceError::Db(DbError::VersionConflict)) => {
                tracing::warn!(
                    task_id = %task.id,
                    "executor completion cascade version conflict"
                );
                let current = TaskRepo::get_by_id(&*self.db, &task.id, false).await?;
                if current
                    .as_ref()
                    .is_none_or(|current| current.status != task.status)
                {
                    self.discard_brokered_execution_plan_stage(&execution)
                        .await?;
                    if let Err(error) = super::clear_settled_plan_publication(
                        &self.db,
                        &task.id,
                        &execution.id,
                        &task.status,
                    )
                    .await
                    {
                        tracing::warn!(
                            task_id = %task.id,
                            %error,
                            "failed to clear superseded plan publication claim"
                        );
                    }
                } else {
                    self.abandon_brokered_plan_if_project_authority_stale(&task.id, &execution)
                        .await?;
                }
                Ok(())
            }
            Err(ServiceError::GuardRejection { guard, reason }) => {
                if brokered_plan {
                    self.restore_brokered_execution_plan(&task, &execution)
                        .await?;
                    self.discard_brokered_execution_plan_stage(&execution)
                        .await?;
                    task = super::release_plan_publication(&self.db, &task, &execution).await?;
                }
                let result = self
                    .handle_executor_completion_guard_rejection(
                        &execution,
                        &task,
                        current_state,
                        &guard,
                        &reason,
                    )
                    .await;
                result
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn discard_brokered_execution_plan_stage(
        &self,
        execution: &Execution,
    ) -> Result<()> {
        if !super::execution_uses_brokered_plan(execution) {
            return Ok(());
        }
        if let Some(task) = TaskRepo::get_by_id(&*self.db, &execution.task_id, true).await? {
            return super::cleanup_execution_plan_private_files(
                &self.db,
                &self.workspace_backend_router,
                &task,
                &execution.id,
            )
            .await;
        }
        let Some(workspace_id) = execution.workspace_id.as_deref() else {
            return Ok(());
        };
        let Some(workspace) = WorkspaceRepo::get_by_id(&*self.db, workspace_id).await? else {
            return Ok(());
        };
        let resolved = self
            .workspace_backend_router
            .resolve(&self.db, &workspace)
            .await?;
        crate::plan_artifact::ExecutionPlan::new(&self.db, &resolved)
            .discard(&execution.id)
            .await
            .map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "failed to remove settled execution plan stage: {error}"
                ))
            })?;
        Ok(())
    }

    async fn abandon_brokered_plan_authority(
        &self,
        task: &Task,
        execution: &Execution,
    ) -> Result<()> {
        if super::execution_uses_brokered_plan(execution)
            && super::active_plan_publication_claim_owner(task)?.as_deref()
                == Some(execution.id.as_str())
        {
            self.restore_brokered_execution_plan(task, execution)
                .await?;
            self.discard_brokered_execution_plan_stage(execution)
                .await?;
            super::release_plan_publication(&self.db, task, execution).await?;
            return Ok(());
        }
        self.discard_brokered_execution_plan_stage(execution)
            .await?;
        if super::pending_plan_publication_cleanup_owner(task)?.as_deref()
            == Some(execution.id.as_str())
        {
            super::clear_plan_publication_cleanup(&self.db, task, &execution.id).await?;
        }
        Ok(())
    }

    pub(crate) async fn abandon_plan_publication_claim(
        &self,
        task: &Task,
        execution_id: &str,
    ) -> Result<()> {
        if super::active_plan_publication_claim_owner(task)?.as_deref() != Some(execution_id) {
            return Ok(());
        }
        let execution = ExecutionRepo::get_by_id(&*self.db, execution_id).await?;
        let workspace = match execution
            .as_ref()
            .and_then(|execution| execution.workspace_id.as_deref())
        {
            Some(workspace_id) => WorkspaceRepo::get_by_id(&*self.db, workspace_id).await?,
            None => {
                let workspace_task_id = task.parent_task_id.as_deref().unwrap_or(&task.id);
                WorkspaceRepo::get_by_task_id(&*self.db, workspace_task_id).await?
            }
        };
        if let Some(workspace) = workspace.as_ref() {
            let resolved = self
                .workspace_backend_router
                .resolve(&self.db, workspace)
                .await?;
            crate::plan_artifact::ExecutionPlan::new(&self.db, &resolved).restore(execution_id).await
            .map_err(|_| {
                ServiceError::invalid_operation(
                    "failed to restore the prior plan while abandoning an invalid publication claim",
                )
            })?;
        }
        super::cleanup_execution_plan_private_files(
            &self.db,
            &self.workspace_backend_router,
            task,
            execution_id,
        )
        .await?;
        super::release_plan_publication_for_execution_id(&self.db, task, execution_id).await?;
        Ok(())
    }

    async fn abandon_brokered_plan_if_project_authority_stale(
        &self,
        task_id: &str,
        execution: &Execution,
    ) -> Result<bool> {
        let Some(task) = TaskRepo::get_by_id(&*self.db, task_id, false).await? else {
            self.discard_brokered_execution_plan_stage(execution)
                .await?;
            return Ok(true);
        };
        let Some(claim_project_version) =
            super::plan_publication_project_version(&task, &execution.id)?
        else {
            return Ok(false);
        };
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id).await?;
        if project
            .as_ref()
            .is_some_and(|project| project.version == claim_project_version)
        {
            return Ok(false);
        }
        self.abandon_brokered_plan_authority(&task, execution)
            .await?;
        Ok(true)
    }

    async fn restore_brokered_execution_plan(
        &self,
        task: &Task,
        execution: &Execution,
    ) -> Result<()> {
        if !super::execution_uses_brokered_plan(execution) {
            return Ok(());
        }
        let workspace = match execution.workspace_id.as_deref() {
            Some(workspace_id) => WorkspaceRepo::get_by_id(&*self.db, workspace_id).await?,
            None => {
                let workspace_task_id = task.parent_task_id.as_deref().unwrap_or(&task.id);
                WorkspaceRepo::get_by_task_id(&*self.db, workspace_task_id).await?
            }
        };
        if let Some(workspace) = workspace {
            let resolved = self
                .workspace_backend_router
                .resolve(&self.db, &workspace)
                .await?;
            crate::plan_artifact::ExecutionPlan::new(&self.db, &resolved)
                .restore(&execution.id)
                .await
                .map_err(|error| {
                    ServiceError::invalid_operation(format!(
                    "failed to restore the prior plan after losing publication authority: {error}"
                ))
                })?;
        }
        Ok(())
    }

    pub(super) async fn handle_executor_completion_guard_rejection(
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
        if super::super::execution_dispatch_project_version(execution) != Some(project.version) {
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
            super::latest_execution_authority(execution, project.version),
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
        if super::super::execution_dispatch_project_version(execution) != Some(project.version) {
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
            super::latest_execution_authority(execution, project.version),
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
    pub(super) async fn clear_workflow_guard_retry_metadata(&self, task: &Task) -> Result<()> {
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
                super::latest_execution_authority(execution, project_version),
                Self::guard_success_mutations(task, &execution.id),
                &now_rfc3339(),
            )
            .await?
            .is_some(),
        )
    }

    pub(crate) async fn annotate_executor_failure_block(
        &self,
        execution: &Execution,
    ) -> Result<()> {
        self.annotate_executor_failure_block_with_retry(execution, true)
            .await
    }

    pub(crate) async fn annotate_dispatch_failure_block(
        &self,
        execution: &Execution,
    ) -> Result<()> {
        self.annotate_executor_failure_block_with_retry(execution, false)
            .await
    }

    /// Handle an execution that failed because no executor candidate could
    /// run (`FailureKind::ExecutorUnavailable`). Transient capacity failures
    /// use the bounded execution retry budget; permanent unavailability
    /// blocks for manual reconfiguration.
    pub(crate) async fn annotate_executor_unavailable_block(
        &self,
        execution: &Execution,
        retry_at: Option<String>,
        attempts: Value,
    ) -> Result<()> {
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        if workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal) {
            return Ok(());
        }

        if task_blocked_by_execution(&task, &execution.id) {
            return Ok(());
        }
        // The current-run fence (see `annotate_executor_failure_block_with_retry`).
        if !super::execution_belongs_to_current_state_entry(&self.db, &task, execution).await? {
            return Ok(());
        }
        let usage_limited = attempts.as_array().is_some_and(|attempts| {
            attempts.iter().any(|attempt| {
                attempt.get("outcome").and_then(Value::as_str) == Some("usage_exhausted")
            })
        });
        let transient = retry_at.is_some() || usage_limited;
        let retry_block_reason = if transient {
            let current_state = workflow
                .states
                .iter()
                .find(|state| state.name == task.status);
            match self
                .maybe_schedule_execution_retry(
                    execution,
                    &task,
                    project.version,
                    current_state.map(|state| &state.config),
                    current_state.and_then(|state| state.gate_config.as_ref()),
                    Some(CapacityRetry {
                        retry_at: retry_at.as_deref(),
                        reason: if usage_limited {
                            "usage limit"
                        } else {
                            "provider capacity unavailable"
                        },
                    }),
                )
                .await?
            {
                ExecutionRetryDisposition::Scheduled => return Ok(()),
                ExecutionRetryDisposition::NotScheduled(reason) => Some(reason),
            }
        } else {
            None
        };

        let annotation = api_types::TaskBlockingAnnotation {
            annotation_type: api_types::FailureKind::ExecutorUnavailable,
            blocking_reason: "executor_unavailable".to_owned(),
            blocked_by: Some(
                api_types::Actor::system(api_types::SystemComponent::Executor).display(),
            ),
            blocked_at: Some(now_rfc3339()),
            blocked_execution_id: Some(execution.id.clone()),
            artifact: Some(api_types::BlockingArtifact {
                kind: "execution".to_owned(),
                id: Some(execution.id.clone()),
                log_path: execution.logs_path.clone(),
            }),
            message: Some(
                if let Some(reason) = retry_block_reason.filter(|_| execution.role != "interactive")
                {
                    format!(
                        "Provider capacity {reason}: {}",
                        execution
                            .error
                            .as_deref()
                            .unwrap_or("no executor candidate available")
                    )
                } else {
                    execution.error.clone().unwrap_or_else(|| {
                        "No executor candidate is available (check CLI installs and authentication)"
                            .to_owned()
                    })
                },
            ),
            hook: None,
        };
        let annotation = serde_json::to_string(&annotation).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize executor-unavailable annotation: {error}"
            ))
        })?;

        let reason = execution
            .error
            .clone()
            .unwrap_or_else(|| "no executor candidate available".to_owned());
        let blocked_meta = json!({
            "reason": reason,
            "created_at": now_rfc3339(),
            "kind": api_types::FailureKind::ExecutorUnavailable,
            "execution_id": execution.id,
            "details": {
                "retry_at": retry_at,
                "attempts": attempts,
            },
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
            super::latest_execution_authority(execution, project.version),
        )
        .await?
        else {
            return Ok(());
        };

        tracing::info!(
            task_id = %task.id,
            execution_id = %execution.id,
            status = %task.status,
            kind = "executor_unavailable",
            "task blocked: no executor candidate available"
        );
        self.publish(ForgeEvent {
            event_type: "task.blocked".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskBlocked {
                project_id: updated.project_id,
                reason,
                kind: Some(api_types::FailureKind::ExecutorUnavailable),
                source: None,
                execution_id: Some(execution.id.clone()),
            },
        });
        Ok(())
    }

    /// A failed run is retried or blocks its Task under the Project as it is
    /// now, whichever Project revision dispatched the run. The revision
    /// fence belongs to completions, which advance the workflow; a failure
    /// advances nothing, its retry is a new run admitted under current
    /// authority, and dropping it here left the Task active with no run, no
    /// retry and no park after any Project edit, pause or resume.
    ///
    /// What fences a failure instead is that the run is still the Task's
    /// current one: the Task is in the state, and the entry of that state,
    /// that the run was dispatched for; the state still belongs to the run's
    /// role; and, inside the write itself, the run is the newest of its role
    /// and its Agent still holds the role
    /// (`latest_execution_authority_matches_in_tx`). A late failure of a run
    /// the Task has moved on from, or that a newer run replaced, changes
    /// nothing and spends no budget.
    async fn annotate_executor_failure_block_with_retry(
        &self,
        execution: &Execution,
        allow_retry: bool,
    ) -> Result<()> {
        // Lease contention never spends the execution retry budget; only the
        // dispatch path parks it, once its re-dispatch bound is spent.
        if allow_retry && super::guards::is_workspace_lease_pre_dispatch_failure(execution) {
            return Ok(());
        }
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        if execution.stopped_by.as_deref() == Some("system:dispatch")
            && task
                .condition
                .reasons()
                .any(|reason| matches!(reason, db::ParkReason::Held { .. }))
        {
            return Ok(());
        }
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        if workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal) {
            return Ok(());
        }
        let Some(current_state) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
        else {
            return Ok(());
        };
        let current_role = crate::workflow::effective_role(current_state);
        let execution_still_owns_current_role = execution.role == "interactive"
            || current_role == Some(execution.role.as_str())
            || (current_role == Some(crate::workflow::default_roles::CODER)
                && execution.role == "executor");
        if !execution_still_owns_current_role {
            return Ok(());
        }
        if !super::execution_belongs_to_current_state_entry(&self.db, &task, execution).await? {
            return Ok(());
        }
        if task_blocked_by_execution(&task, &execution.id) {
            return Ok(());
        }
        if allow_retry
            && self
                .maybe_schedule_execution_retry(
                    execution,
                    &task,
                    project.version,
                    Some(&current_state.config),
                    current_state.gate_config.as_ref(),
                    None,
                )
                .await?
                .is_scheduled()
        {
            return Ok(());
        }

        self.block_task_after_executor_failure(&task, execution)
            .await
    }

    pub(in crate::task_service) async fn block_task_after_executor_failure(
        &self,
        task: &Task,
        execution: &Execution,
    ) -> Result<()> {
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &task.project_id).await? else {
            return Ok(());
        };
        let annotation = api_types::TaskBlockingAnnotation {
            annotation_type: api_types::FailureKind::ExecutorFailed,
            blocking_reason: "executor_failed".to_owned(),
            blocked_by: Some(
                api_types::Actor::system(api_types::SystemComponent::Executor).display(),
            ),
            blocked_at: Some(now_rfc3339()),
            blocked_execution_id: Some(execution.id.clone()),
            artifact: Some(api_types::BlockingArtifact {
                kind: "execution".to_owned(),
                id: Some(execution.id.clone()),
                log_path: execution.logs_path.clone(),
            }),
            message: Some(
                execution
                    .error
                    .clone()
                    .unwrap_or_else(|| "Execution failed".to_owned()),
            ),
            hook: None,
        };
        let annotation = serde_json::to_string(&annotation).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize executor-failure annotation: {error}"
            ))
        })?;

        let reason = execution
            .error
            .clone()
            .unwrap_or_else(|| "executor failed".to_owned());
        let blocked_meta = json!({
            "reason": reason,
            "created_at": now_rfc3339(),
            "kind": api_types::FailureKind::InternalCommandFailed,
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
            super::latest_execution_authority(execution, project.version),
        )
        .await?
        else {
            return Ok(());
        };

        tracing::info!(
            task_id = %task.id,
            execution_id = %execution.id,
            status = %task.status,
            kind = "internal_command_failed",
            "task blocked after executor failure"
        );
        self.publish(ForgeEvent {
            event_type: "task.blocked".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskBlocked {
                project_id: updated.project_id,
                reason,
                kind: Some(api_types::FailureKind::InternalCommandFailed),
                source: None,
                execution_id: Some(execution.id.clone()),
            },
        });
        Ok(())
    }

    /// A resume action is useful only when the stopped execution can pass the
    /// same identity/role checks as the WorkspaceLease it will request.  Keep
    /// impossible actions out of the durable annotation instead of inviting a
    /// coordinator to call a recovery route that is guaranteed to fail.
    async fn maybe_schedule_execution_retry(
        &self,
        execution: &Execution,
        task: &Task,
        project_version: i64,
        state_config: Option<&Value>,
        gate_config: Option<&api_types::GateConfig>,
        capacity_retry: Option<CapacityRetry<'_>>,
    ) -> Result<ExecutionRetryDisposition> {
        if execution.role == "interactive" {
            // Interactive runs are user-prompted and do not have a durable dispatcher target yet.
            return Ok(ExecutionRetryDisposition::NotScheduled(
                "automatic retry is unavailable for interactive executions",
            ));
        }

        let budget =
            db::budget::limit(task, db::budget::Kind::Execution, state_config, gate_config)?;
        if !db::budget::allows_retry(i64::from(budget), 0) {
            return Ok(ExecutionRetryDisposition::NotScheduled(
                "automatic retries are disabled by the execution retry budget",
            ));
        }

        let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "invalid task metadata for {}: {error}",
                task.id
            ))
        })?;
        let retry_count =
            db::budget::spent(self.db.pool(), &task.id, db::budget::Kind::Execution.key()).await?
                as u64;
        let already_recorded = metadata
            .extra
            .get("last_execution_failure_execution_id")
            .and_then(Value::as_str)
            == Some(execution.id.as_str());
        if already_recorded {
            if !self
                .latest_execution_authority_is_current(task, execution, project_version)
                .await?
            {
                return Ok(ExecutionRetryDisposition::NotScheduled(
                    "automatic retry was skipped because execution authority changed",
                ));
            }
            ExecutionRepo::update(
                &*self.db,
                db::UpdateExecution {
                    id: execution.id.clone(),
                    status: None,
                    stop_reason: None,
                    stopped_by: None,
                    resume_policy: Some(Some(db::ResumePolicy::Auto)),
                    stopped_at: None,
                    agent_session_id: None,
                    agent_message_id: None,
                    last_activity_at: None,
                    summary: None,
                    logs_path: None,
                    before_sha: None,
                    after_sha: None,
                    error: None,
                    executor_config_snapshot_json: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await?;
            return Ok(ExecutionRetryDisposition::Scheduled);
        }
        if !db::budget::allows_retry(i64::from(budget), retry_count as i64) {
            return Ok(ExecutionRetryDisposition::NotScheduled(
                "retry budget exhausted",
            ));
        }

        let now = now_rfc3339();
        let attempt = retry_count + 1;
        let delay_seconds = (10_u64
            .saturating_mul(2_u64.saturating_pow(attempt.saturating_sub(1) as u32)))
        .min(300);
        let next_dispatch_at = if let Some(capacity) = &capacity_retry {
            executor_unavailable_dispatch_time(capacity.retry_at, &task.id, delay_seconds)
        } else {
            chrono::Utc::now() + chrono::Duration::seconds(delay_seconds as i64)
        };
        let reason = if let Some(capacity) = &capacity_retry {
            format!("{}; capacity retry (attempt {attempt})", capacity.reason)
        } else {
            format!("execution retry (attempt {attempt})")
        };
        let delay_seconds = if capacity_retry.is_some() {
            ((next_dispatch_at - chrono::Utc::now())
                .num_milliseconds()
                .max(0) as u64)
                .div_ceil(1000)
        } else {
            delay_seconds
        };
        if !crate::deferred_dispatch::set_with_mutations_for_latest_execution(
            &self.db,
            task,
            super::latest_execution_authority(execution, project_version),
            &task.status,
            &next_dispatch_at.to_rfc3339(),
            &reason,
            vec![
                db::TaskMetadataMutation::Budget(db::budget::Mutation::Charge {
                    key: db::budget::Kind::Execution.key().into(),
                    limit: i64::from(budget),
                    step: format!("failure:{}", execution.id),
                }),
                db::TaskMetadataMutation::Set {
                    key: "last_execution_failure_at".to_owned(),
                    value: Value::String(now.clone()),
                },
                db::TaskMetadataMutation::Set {
                    key: "last_execution_failure_execution_id".to_owned(),
                    value: Value::String(execution.id.clone()),
                },
            ],
        )
        .await?
        {
            return Ok(ExecutionRetryDisposition::NotScheduled(
                "automatic retry was skipped because execution authority changed",
            ));
        }
        ExecutionRepo::update(
            &*self.db,
            db::UpdateExecution {
                id: execution.id.clone(),
                status: None,
                stop_reason: None,
                stopped_by: None,
                resume_policy: Some(Some(db::ResumePolicy::Auto)),
                stopped_at: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;

        tracing::info!(
            task_id = %task.id,
            execution_id = %execution.id,
            attempt,
            delay_seconds,
            next_dispatch_at = %next_dispatch_at.to_rfc3339(),
            "scheduling execution retry"
        );
        self.publish(ForgeEvent {
            event_type: "task.execution_retry".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskExecutionRetry {
                task_id: task.id.clone(),
                execution_id: execution.id.clone(),
                attempt: attempt as u32,
                delay_seconds,
                next_dispatch_at: next_dispatch_at.to_rfc3339(),
            },
        });
        Ok(ExecutionRetryDisposition::Scheduled)
    }

    async fn latest_execution_authority_is_current(
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
                super::latest_execution_authority(execution, project_version),
                Vec::new(),
                &now_rfc3339(),
            )
            .await?
            .is_some(),
        )
    }

    async fn maybe_cascade_reviewer_completion(&self, execution: &Execution) -> Result<()> {
        let task = match TaskRepo::get_by_id(&*self.db, &execution.task_id, false).await? {
            Some(task) => task,
            None => return Ok(()),
        };
        if task.status != crate::workflow::default_states::REVIEW {
            return Ok(());
        }

        let reviews = ReviewRepo::list_by_task(&*self.db, &task.id).await?;
        let Some(review) = exact_review_for_execution(execution, &reviews).cloned() else {
            tracing::warn!(
                task_id = %task.id,
                execution_id = %execution.id,
                "ignoring reviewer completion without an explicit Review binding"
            );
            return Ok(());
        };
        let Some(latest_review) = reviews
            .iter()
            .max_by_key(|review| (review.attempt_number, review.id.clone()))
        else {
            return Ok(());
        };
        if latest_review.id != review.id {
            tracing::debug!(
                task_id = %task.id,
                execution_id = %execution.id,
                review_id = %review.id,
                latest_review_id = %latest_review.id,
                "ignoring reviewer completion bound to a superseded review attempt"
            );
            return Ok(());
        }
        if execution.status == ExecutionStatus::Completed {
            self.ingest_terminal_execution_outbox(&task, execution)
                .await?;
        }

        // Reuse the exact Review attempt on duplicate terminal delivery. No
        // synthetic Review is created when the execution has no durable
        // attempt identity, and a bound older attempt cannot settle the
        // current newer Review merely because it shares the candidate parent.
        if review.status != ReviewStatus::Running {
            if review.status == ReviewStatus::Failed
                && (task.blocked_json.is_some()
                    || task.failed_json.is_some()
                    || task.error_annotation.is_some())
            {
                tracing::debug!(
                    task_id = %task.id,
                    execution_id = %execution.id,
                    review_id = %review.id,
                    "failed reviewer completion already produced a durable task disposition"
                );
                return Ok(());
            }
            if matches!(review.status, ReviewStatus::Passed | ReviewStatus::Failed) {
                tracing::warn!(
                    task_id = %task.id,
                    execution_id = %execution.id,
                    review_id = %review.id,
                    status = %review.status,
                    "review outcome was committed without its task cascade; reconciling"
                );
                return self
                    .reconcile_settled_reviewer_completion(&task, execution, &review, true)
                    .await;
            }
            tracing::debug!(
                task_id = %task.id,
                execution_id = %execution.id,
                review_id = %review.id,
                status = %review.status,
                "reviewer completion already processed"
            );
            return Ok(());
        }
        if execution.status != ExecutionStatus::Completed {
            return self
                .fail_review_for_reviewer_execution_exit(&task, execution, review)
                .await;
        }
        let user_approval_required = self.gate_requires_user_approval(&task).await?;
        if review_blocked_by_execution(&task, &execution.id) {
            // Duplicate delivery after the checks already timed out and parked
            // the Task: the owner decides, so do not run them again.
            return Ok(());
        }
        let final_message = reviewer_final_message(execution).await?;
        let workspace_io = match execution.workspace_id.as_deref() {
            Some(_) => {
                let workspace = prepare_workspace(
                    &self.db,
                    &self.workspace_root,
                    &task,
                    &task.id,
                    self.repo_cache_locks.clone(),
                    &self.workspace_backend_router,
                )
                .await?;
                Some(self.review_workspace_io(&workspace).await?)
            }
            None => None,
        };
        let mut check_reruns = db::budget::Invocation::new(db::budget::Kind::ReviewCheckRerun);
        let conformance = loop {
            let conformance = match workspace_io.as_ref() {
                Some(path) => {
                    ::review::contract::evaluate(
                        &self.db,
                        &execution.id,
                        path.as_ref(),
                        &final_message,
                    )
                    .await
                }
                None => Err("review execution has no workspace evidence".to_owned()),
            };
            // A clean-checkout check that outran its limit is Forge's own
            // verification failing, not the reviewer: re-run only the checks,
            // and park the Task for its owner rather than discard the verdict
            // by dispatching another reviewer.
            let Some(reason) = conformance
                .as_ref()
                .ok()
                .filter(|result| result.status == api_types::ConformanceStatus::Unverified)
                .and_then(|result| result.reason.as_deref())
                .filter(|reason| ::review::contract::is_check_timeout(reason))
                .map(str::to_owned)
            else {
                break conformance;
            };
            let attempt = check_reruns.spent() + 1;
            if !check_reruns.consume() {
                return self
                    .block_task_for_review_environment(
                        &task,
                        execution,
                        format!("{reason} ({attempt} attempts)"),
                    )
                    .await;
            }
            tracing::warn!(
                task_id = %task.id,
                execution_id = %execution.id,
                attempt,
                %reason,
                "review checks timed out; re-running the checks only"
            );
        };
        let conformance = match conformance {
            Ok(result) if result.status != api_types::ConformanceStatus::Unverified => result,
            Ok(result) => {
                // An Unverified assessment is retained as diagnostic evidence,
                // but it is still a reviewer execution/protocol failure. Keep
                // it on the bounded reviewer retry/block path so an evidence
                // gap can never route the coder into remediation or grant the
                // current review authority.
                let reason = result
                    .reason
                    .unwrap_or_else(|| "review conformance is unverified".to_owned());
                let mut failed = execution.clone();
                failed.error = Some(reason);
                return self
                    .fail_review_for_reviewer_execution_exit(&task, &failed, review)
                    .await;
            }
            Err(reason) => {
                let mut failed = execution.clone();
                failed.error = Some(reason);
                return self
                    .fail_review_for_reviewer_execution_exit(&task, &failed, review)
                    .await;
            }
        };
        let passed = conformance.status == api_types::ConformanceStatus::Passed;
        let blocked = conformance.status == api_types::ConformanceStatus::Blocked;
        let owner_finding =
            db::budget::owner_review_failure_message(&conformance, &review, &reviews);
        let status = if passed && user_approval_required {
            ReviewStatus::AwaitingHuman
        } else if passed {
            ReviewStatus::Passed
        } else {
            ReviewStatus::Failed
        };
        let auditor_details = json!({
            "verdict": if passed { "pass" } else if blocked { "blocked" } else { "fail" },
            "reason": conformance.reason,
        });

        let finished_at = now_rfc3339();
        let mut review_details = strict_review_details(&review)?;
        review_details["auditor"] = auditor_details;
        review_details["conformance"] = serde_json::to_value(&conformance)
            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
        if status == ReviewStatus::AwaitingHuman {
            review_details["user_approval"] = json!({
                "status": "awaiting_human",
                "reason": "gate requires user approval",
            });
        }
        let task_authority = match &status {
            // The terminal Review and its Task authority projection are one
            // admission decision. Even when an old projection is already
            // present, bind this new terminal attempt to the exact Task
            // version and replace the projection in the same transaction.
            ReviewStatus::Passed => Some(Some(finished_at.clone())),
            ReviewStatus::Failed => Some(None),
            _ => None,
        };
        let (updated_review, task) = if let Some(review_passed_at) = task_authority {
            let (updated_review, task) = ReviewRepo::update_status_with_task_authority(
                &*self.db,
                &review.id,
                status.clone(),
                review_details.to_string(),
                (status != ReviewStatus::AwaitingHuman).then_some(finished_at.clone()),
                &finished_at,
                task.version,
                review_passed_at,
                db::ReviewEventOrigin::Runner,
            )
            .await?;
            (updated_review, task)
        } else {
            let updated_review = ReviewRepo::update_status(
                &*self.db,
                &review.id,
                status.clone(),
                review_details.to_string(),
                (status != ReviewStatus::AwaitingHuman).then_some(finished_at.clone()),
                &finished_at,
            )
            .await?;
            (updated_review, task)
        };

        if let Err(error) = self
            .memory_service
            .record_review_result_if_final(&task.project_id, &updated_review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }

        self.publish_finished_review_comment(&updated_review, Some(execution))
            .await?;

        match status {
            ReviewStatus::Passed => {
                self.publish(ForgeEvent {
                    event_type: "review.passed".to_owned(),
                    entity_id: updated_review.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::ReviewPassed {
                        task_id: task.id.clone(),
                        review_id: updated_review.id.clone(),
                        attempt_number: updated_review.attempt_number,
                    },
                });
                self.cascade_completed_review_task(
                    &task,
                    crate::workflow::default_states::MERGING,
                    "review passed",
                    false,
                )
                .await?;
            }
            ReviewStatus::AwaitingHuman => {
                self.publish(ForgeEvent {
                    event_type: "task.awaiting_human".to_owned(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskAwaitingHuman {
                        task_id: task.id.clone(),
                        role: crate::workflow::default_roles::REVIEWER.to_owned(),
                        assignee_id: "human".to_owned(),
                        state: crate::workflow::default_states::REVIEW.to_owned(),
                    },
                });
            }
            ReviewStatus::Failed => {
                self.publish(ForgeEvent {
                    event_type: "review.failed".to_owned(),
                    entity_id: updated_review.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::ReviewFailed {
                        task_id: task.id.clone(),
                        review_id: updated_review.id.clone(),
                        attempt_number: updated_review.attempt_number,
                        failed_step_index: 0,
                    },
                });
                if blocked {
                    // The coder cannot install a toolchain or grant access,
                    // and another reviewer would hit the same wall: park the
                    // Task for its owner instead of spending either budget.
                    let reason = conformance
                        .reason
                        .clone()
                        .unwrap_or_else(|| "reviewer reported a blocked environment".to_owned());
                    return self
                        .block_task_for_review_environment(&task, execution, reason)
                        .await;
                }
                if let Some(message) = owner_finding {
                    return self
                        .block_task_for_review_finding(
                            &task,
                            execution,
                            api_types::FailureKind::ReviewNeedsOwner,
                            message,
                        )
                        .await;
                }
                let (task, target, reason) = self
                    .review_failure_target(&task, Some(&execution.id))
                    .await?;
                if let Some(target) = target {
                    self.cascade_completed_review_task(&task, &target, &reason, true)
                        .await?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Park a Task whose reviewer reported that its environment, not the
    /// candidate, prevented a verdict. Re-running the review is the recovery
    /// once the owner has fixed the environment (for example by adding
    /// review setup steps).
    async fn block_task_for_review_environment(
        &self,
        task: &Task,
        execution: &Execution,
        reason: String,
    ) -> Result<()> {
        let message = format!("review blocked by its environment: {reason}");
        self.block_task_for_review_finding(
            task,
            execution,
            api_types::FailureKind::ReviewBlocked,
            message,
        )
        .await
    }

    /// Park a failed review without spending the remediation retry budget.
    async fn block_task_for_review_finding(
        &self,
        task: &Task,
        execution: &Execution,
        kind: api_types::FailureKind,
        message: String,
    ) -> Result<()> {
        let annotation = api_types::TaskBlockingAnnotation {
            annotation_type: kind,
            blocking_reason: kind.to_string(),
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
            message: Some(message.clone()),
            hook: None,
        };
        let annotation = serde_json::to_string(&annotation).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize review blocking annotation: {error}"
            ))
        })?;
        let blocked_meta = json!({
            "reason": message,
            "created_at": now_rfc3339(),
            "kind": kind,
            "execution_id": execution.id,
        });
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        let updated = TaskRepo::update_status(
            &*self.db,
            UpdateTaskStatus {
                id: current.id.clone(),
                expected_version: current.version,
                status: current.status.clone(),
                assignee_id: None,
                error_annotation: Some(Some(annotation)),
                blocked_json: Some(Some(blocked_meta.to_string())),
                failed_json: Some(None),
                updated_at: now_rfc3339(),
            },
        )
        .await?;

        let review = self.latest_review_for_task(&task.id).await?;
        self.publish_review_park_comment(&review, &message).await?;

        self.publish(ForgeEvent {
            event_type: "task.blocked".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskBlocked {
                project_id: updated.project_id,
                reason: message,
                kind: Some(kind),
                source: None,
                execution_id: Some(execution.id.clone()),
            },
        });
        Ok(())
    }

    pub(crate) async fn reconcile_settled_reviewer_completion(
        &self,
        task: &Task,
        execution: &Execution,
        review: &Review,
        strict_details: bool,
    ) -> Result<()> {
        if let Err(error) = self
            .publish_finished_review_comment(review, Some(execution))
            .await
        {
            if strict_details {
                return Err(error);
            }
            tracing::warn!(task_id = %task.id, review_id = %review.id, %error,
                "invalid historical review details; comment omitted during plain remediation routing");
        }
        let now = now_rfc3339();
        match review.status {
            ReviewStatus::Passed => {
                let task = if task.review_passed_at.is_some() {
                    task.clone()
                } else {
                    TaskRepo::set_review_passed_at_cas_for_review(
                        &*self.db,
                        &task.id,
                        task.version,
                        Some(review.finished_at.clone().unwrap_or_else(|| now.clone())),
                        &review.updated_at,
                        &now,
                    )
                    .await?
                };
                self.cascade_completed_review_task(
                    &task,
                    crate::workflow::default_states::MERGING,
                    "review passed",
                    false,
                )
                .await
            }
            ReviewStatus::Failed => {
                let task = if task.review_passed_at.is_none() {
                    task.clone()
                } else {
                    TaskRepo::set_review_passed_at_cas(
                        &*self.db,
                        &task.id,
                        task.version,
                        None,
                        &now,
                    )
                    .await?
                };
                let conformance = strict_review_details(review).and_then(|details| {
                    details
                        .get("conformance")
                        .filter(|value| !value.is_null())
                        .map(|value| {
                            serde_json::from_value::<api_types::ReviewConformance>(value.clone())
                                .map_err(|error| ServiceError::invalid_operation(error.to_string()))
                        })
                        .transpose()
                });
                let conformance = match conformance {
                    Ok(conformance) => conformance,
                    Err(error) if !strict_details => {
                        tracing::warn!(task_id = %task.id, review_id = %review.id, %error,
                            "invalid failed review details; recovering with plain remediation routing");
                        None
                    }
                    Err(error) => return Err(error),
                };
                if let Some(conformance) = conformance {
                    if conformance.status == api_types::ConformanceStatus::Blocked {
                        return self
                            .block_task_for_review_environment(
                                &task,
                                execution,
                                conformance.reason.unwrap_or_else(|| {
                                    "reviewer reported a blocked environment".to_owned()
                                }),
                            )
                            .await;
                    }
                    let reviews = ReviewRepo::list_by_task(&*self.db, &task.id).await?;
                    if let Some(message) =
                        db::budget::owner_review_failure_message(&conformance, review, &reviews)
                    {
                        return self
                            .block_task_for_review_finding(
                                &task,
                                execution,
                                api_types::FailureKind::ReviewNeedsOwner,
                                message,
                            )
                            .await;
                    }
                }
                let (task, target, reason) = self
                    .review_failure_target(&task, Some(&execution.id))
                    .await?;
                if let Some(target) = target {
                    self.cascade_completed_review_task(&task, &target, &reason, true)
                        .await?;
                }
                Ok(())
            }
            ReviewStatus::Running | ReviewStatus::AwaitingHuman | ReviewStatus::Cancelled => Ok(()),
        }
    }

    async fn fail_review_for_reviewer_execution_exit(
        &self,
        task: &Task,
        execution: &Execution,
        review: Review,
    ) -> Result<()> {
        let finished_at = now_rfc3339();
        let reason = execution_failure_reason(execution);
        let mut review_details = strict_review_details(&review)?;
        review_details["auditor"] = json!({
            "verdict": "fail",
            "reason": reason,
        });
        if let Some(conformance) =
            db::ReviewConformanceRepo::review_conformance(&*self.db, &execution.id).await?
        {
            review_details["conformance"] = serde_json::to_value(conformance)
                .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
        }
        review_details["execution"] = json!({
            "id": execution.id,
            "status": execution.status.to_string(),
            "stop_reason": execution.stop_reason.as_ref().map(ToString::to_string),
            "error": execution.error.as_deref(),
        });

        // Duplicate terminal delivery for an execution whose retry was
        // already scheduled must be inert.  The review intentionally remains
        // Running while the replacement attempt is pending, so review status
        // alone cannot provide this idempotency guard.
        if review_details
            .pointer("/execution_retry/execution_id")
            .and_then(Value::as_str)
            == Some(execution.id.as_str())
        {
            return Ok(());
        }

        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        let current_state = workflow
            .states
            .iter()
            .find(|state| state.name == task.status);
        if self
            .maybe_schedule_execution_retry(
                execution,
                task,
                project.version,
                current_state.map(|state| &state.config),
                current_state.and_then(|state| state.gate_config.as_ref()),
                None,
            )
            .await?
            .is_scheduled()
        {
            review_details["execution_retry"] = json!({
                "execution_id": execution.id,
                "status": "scheduled",
                "scheduled_at": finished_at.clone(),
            });
            ReviewRepo::update_status(
                &*self.db,
                &review.id,
                ReviewStatus::Running,
                review_details.to_string(),
                None,
                &finished_at,
            )
            .await?;

            self.publish_reviewer_comment(
                execution,
                &task.id,
                format!(
                    "Reviewer execution {} failed; an automatic retry was scheduled",
                    execution.id
                ),
            )
            .await?;
            return Ok(());
        }

        let (updated_review, task) = ReviewRepo::update_status_with_task_authority(
            &*self.db,
            &review.id,
            ReviewStatus::Failed,
            review_details.to_string(),
            Some(finished_at.clone()),
            &finished_at,
            task.version,
            None,
            db::ReviewEventOrigin::Executor,
        )
        .await?;

        if let Err(error) = self
            .memory_service
            .record_review_result_if_final(&task.project_id, &updated_review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }
        self.publish(ForgeEvent {
            event_type: "review.failed".to_owned(),
            entity_id: updated_review.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReviewFailed {
                task_id: task.id.clone(),
                review_id: updated_review.id.clone(),
                attempt_number: updated_review.attempt_number,
                failed_step_index: 0,
            },
        });
        self.publish_reviewer_comment(
            execution,
            &task.id,
            format!(
                "Review failed (attempt {}): reviewer execution {}",
                updated_review.attempt_number, reason
            ),
        )
        .await?;

        self.block_task_after_executor_failure(&task, execution)
            .await
    }

    pub(crate) async fn publish_finished_review_comment(
        &self,
        review: &Review,
        execution: Option<&Execution>,
    ) -> Result<()> {
        if !matches!(
            review.status,
            ReviewStatus::Passed | ReviewStatus::Failed | ReviewStatus::AwaitingHuman
        ) {
            return Ok(());
        }
        let details = strict_review_details(review)?;
        let mut conformance: api_types::ReviewConformance = details
            .get("conformance")
            .filter(|value| !value.is_null())
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
            .unwrap_or_default();
        if conformance.reason.is_none() {
            conformance.reason = details
                .pointer("/auditor/reason")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    details["ci_steps"].as_array()?.iter().find_map(|step| {
                        (step["exit_code"].as_i64()? != 0)
                            .then(|| format!("CI step {} failed", step["index"]))
                    })
                });
        }
        let ledger = self.review_comment_ledger(review).await?;
        let content = reviewer_comment(
            review.status.clone(),
            review.attempt_number,
            &conformance,
            &ledger,
            details.get("verify"),
        );
        self.create_review_comment(
            review,
            "result",
            execution.and_then(|execution| execution.agent_id.as_deref()),
            content,
        )
        .await
    }

    async fn review_comment_ledger(
        &self,
        review: &Review,
    ) -> Result<Vec<api_types::ReviewFindingEntry>> {
        let window = self.db.review_window(&review.task_id).await?;
        let reviews: Vec<_> = window
            .reviews
            .into_iter()
            .filter(|row| row.attempt_number <= review.attempt_number)
            .collect();
        ::review::finding_ledger(&reviews).map_err(ServiceError::invalid_operation)
    }

    pub(crate) async fn publish_review_repair_comment(
        &self,
        review: &Review,
        spent: i64,
        limit: i32,
    ) -> Result<()> {
        let ledger = self.review_comment_ledger(review).await?;
        let content = repair_round_comment(
            review.attempt_number,
            spent,
            i64::from(limit),
            &ledger,
            "review",
        );
        self.create_review_comment(review, "repair", None, content)
            .await
    }

    pub(crate) async fn publish_review_park_comment(
        &self,
        review: &Review,
        rule: &str,
    ) -> Result<()> {
        let ledger = self.review_comment_ledger(review).await?;
        let ids = finding_ids(&ledger, |entry| {
            entry.status == api_types::ReviewFindingStatus::Open
        });
        self.create_review_comment(review, "park", None, format!(
            "Task parked after review attempt {}: {rule}. Open findings: {ids}. Owner actions: re-run review, mark reviewed with a reason, or cancel the Task. See the Review tab.", review.attempt_number
        )).await
    }

    async fn publish_reviewer_comment(
        &self,
        execution: &Execution,
        task_id: &str,
        content: String,
    ) -> Result<()> {
        if let Some(agent_id) = execution.agent_id.as_deref() {
            self.create_agent_comment(task_id, agent_id, content).await
        } else {
            self.create_system_comment(task_id, content).await
        }
    }

    pub(crate) async fn review_failure_target(
        &self,
        task: &Task,
        execution_id: Option<&str>,
    ) -> Result<(Task, Option<String>, String)> {
        if !db::task_writer::owns_task(&task.id) {
            return self
                .request_task_command(
                    &task.id,
                    "review_failure_target",
                    json!([task.id, task.status, execution_id]),
                    false,
                )
                .await;
        }
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        if current.status != task.status {
            return Err(DbError::VersionConflict.into());
        }
        let task = &current;
        let latest_review =
            ReviewRepo::list_latest_reviews_for_tasks(&*self.db, &[task.id.as_str()])
                .await?
                .into_iter()
                .next();
        if let Some(review) = &latest_review {
            db::budget::reconcile_failed_review(&self.db, task, review).await?;
        }
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        let review_state = workflow
            .states
            .iter()
            .find(|state| state.name == task.status);
        let budget = db::budget::limit(
            task,
            db::budget::Kind::Review,
            review_state.map(|state| &state.config),
            review_state.and_then(|state| state.gate_config.as_ref()),
        )?;
        let existing_count =
            db::budget::spent(self.db.pool(), &task.id, db::budget::Kind::Review.key()).await?;
        if !db::budget::allows_retry(i64::from(budget), existing_count) {
            // The verdict that exhausted the budget opens its own automatic
            // recovery episode, here where the limit is resolved.
            if let Some(review) = latest_review
                .as_ref()
                .filter(|review| review.status == ReviewStatus::Failed)
            {
                db::budget::open_recovery_episode(&self.db, &task.id, &review.id).await?;
            }
            let reason = "review retry budget exhausted";
            if let Some((task, recovery_reason)) = self
                .try_dispatch_automatic_review_recovery(
                    &project,
                    task,
                    execution_id,
                    existing_count,
                    budget,
                    reason,
                )
                .await?
            {
                return Ok((task, None, recovery_reason));
            }
            tracing::info!(
                task_id = %task.id,
                rejections = existing_count,
                budget = i64::from(budget),
                "review retry budget exhausted, blocking task"
            );
            let blocked_meta = json!({
                "reason": reason,
                "created_at": now_rfc3339(),
                "kind": api_types::FailureKind::ReviewGateFailed,
                "source": null,
                "execution_id": execution_id,
            });
            let annotation = json!({
                "type": api_types::FailureKind::ReviewBudgetExhausted,
                "blocking_reason": reason,
                "message": reason,
                "detected_at": now_rfc3339(),
            });
            // A stale recovery snapshot must not annotate a successor state
            // or execution. Let the caller retry from current authority.
            let task = TaskRepo::update(
                &*self.db,
                UpdateTask {
                    id: task.id.clone(),
                    expected_version: task.version,
                    title: None,
                    description: None,
                    priority: None,
                    merge_config: None,
                    plan: None,
                    error_annotation: Some(Some(annotation.to_string())),
                    blocked_json: Some(Some(blocked_meta.to_string())),
                    failed_json: Some(None),
                    task_state_config: None,
                    parent_task_id: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await?;
            self.publish(ForgeEvent {
                event_type: "task.blocked".to_owned(),
                entity_id: task.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskBlocked {
                    project_id: task.project_id.clone(),
                    reason: reason.to_owned(),
                    kind: Some(api_types::FailureKind::ReviewGateFailed),
                    source: None,
                    execution_id: execution_id.map(str::to_owned),
                },
            });
            if let Some(review) = &latest_review {
                self.publish_review_park_comment(review, reason).await?;
            }
            Ok((task, None, reason.to_owned()))
        } else {
            // A coordination root with a lead re-plans: its rejection goes to
            // its planning state (`effective_reject_target`). Every other
            // Task keeps the configured target.
            let lead_assigned =
                crate::task_hierarchy::coordination_root_lead(&self.db, task, &workflow)
                    .await?
                    .is_some();
            let configured =
                crate::task_hierarchy::effective_reject_target(&workflow, &task.status, false);
            let target = crate::task_hierarchy::effective_reject_target(
                &workflow,
                &task.status,
                lead_assigned,
            );
            let replans = target != configured;
            // The remediation target is the review gate's `reject_target`,
            // and `in_progress` when the gate names none: that fallback is
            // why a workflow whose gate has no `reject_target` still sends
            // a red entry back to work. A workflow has no remediation
            // target only when that state does not exist in it (a gate that
            // names a state the workflow lacks, or a custom workflow with
            // no `in_progress`). A transition there cannot be made, so the
            // failed entry parks typed in `review` for its owner instead of
            // failing its cascade.
            if !workflow.states.iter().any(|state| state.name == target) {
                let reason = "review failed and the workflow has no remediation state";
                tracing::warn!(
                    task_id = %task.id,
                    target = %target,
                    "review failure has no remediation target, blocking task"
                );
                let blocked_meta = json!({
                    "reason": reason,
                    "created_at": now_rfc3339(),
                    "kind": api_types::FailureKind::ReviewGateFailed,
                    "source": null,
                    "execution_id": execution_id,
                });
                let annotation = json!({
                    "type": api_types::FailureKind::ReviewGateFailed,
                    "blocking_reason": reason,
                    "message": format!(
                        "{reason}: the review gate sends a failed review to `{target}`, which this workflow does not define"
                    ),
                    "detected_at": now_rfc3339(),
                });
                let task = TaskRepo::update(
                    &*self.db,
                    UpdateTask {
                        id: task.id.clone(),
                        expected_version: task.version,
                        title: None,
                        description: None,
                        priority: None,
                        merge_config: None,
                        plan: None,
                        error_annotation: Some(Some(annotation.to_string())),
                        blocked_json: Some(Some(blocked_meta.to_string())),
                        failed_json: Some(None),
                        task_state_config: None,
                        parent_task_id: None,
                        updated_at: now_rfc3339(),
                    },
                )
                .await?;
                self.publish(ForgeEvent {
                    event_type: "task.blocked".to_owned(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskBlocked {
                        project_id: task.project_id.clone(),
                        reason: reason.to_owned(),
                        kind: Some(api_types::FailureKind::ReviewGateFailed),
                        source: None,
                        execution_id: execution_id.map(str::to_owned),
                    },
                });
                return Ok((task, None, reason.to_owned()));
            }
            if replans {
                // One re-plan per rejected review, charged under the receipt
                // the planning entry carries. With none left the root parks
                // in review, typed, for its owner.
                let (receipt, _) = self.review_replan_receipt(task).await?;
                if self.charge_replan(task, &receipt).await?
                    == crate::task_service::replan::ReplanCharge::Exhausted
                {
                    let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
                    let task = self
                        .block_replan_exhausted(&current, "its aggregate review was rejected")
                        .await?;
                    return Ok((
                        task,
                        None,
                        crate::task_service::replan::REPLAN_EXHAUSTED_REASON.to_owned(),
                    ));
                }
            }
            tracing::debug!(
                task_id = %task.id,
                rejections = existing_count,
                budget = i64::from(budget),
                target = %target,
                "review failure within budget, cascading"
            );
            if !replans {
                if let Some(review) = &latest_review {
                    self.publish_review_repair_comment(review, existing_count, budget)
                        .await?;
                }
            }
            Ok((task.clone(), Some(target), "review failed".to_owned()))
        }
    }

    /// The explicit recovery owns its state-entry dispatch. Its scoped future
    /// supplies the configured Agent and recovery prompt after that transition.
    pub(crate) fn automatic_review_recovery_owns_dispatch(task_id: &str) -> bool {
        AUTOMATIC_REVIEW_RECOVERY_TASK
            .try_with(|id| id == task_id)
            .unwrap_or(false)
    }

    pub(in crate::task_service) async fn try_dispatch_automatic_review_recovery(
        &self,
        project: &db::Project,
        task: &Task,
        review_execution_id: Option<&str>,
        existing_rejections: i64,
        budget: i32,
        failure_reason: &str,
    ) -> Result<Option<(Task, String)>> {
        let Some(parent_execution_id) = review_execution_id else {
            return Ok(None);
        };
        let settings = match serde_json::from_str::<ProjectSettings>(&project.settings) {
            Ok(settings) => settings,
            Err(error) => {
                tracing::warn!(
                    task_id = %task.id,
                    project_id = %project.id,
                    %error,
                    "automatic review recovery skipped because project settings are invalid"
                );
                return Ok(None);
            }
        };
        let recovery = settings.automatic_recovery;
        if !recovery.enabled {
            return Ok(None);
        }
        let Some(agent_id) = recovery
            .agent_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
        else {
            tracing::warn!(
                task_id = %task.id,
                project_id = %project.id,
                "automatic review recovery is enabled without a recovery agent"
            );
            return Ok(None);
        };

        let max_attempts = db::budget::recovery_limit(&project.settings) as usize;
        let recovery_attempts = db::budget::spent(
            self.db.pool(),
            &task.id,
            db::budget::Kind::AutomaticReviewRecovery.key(),
        )
        .await? as usize;
        if !db::budget::allows_retry(max_attempts as i64, recovery_attempts as i64) {
            return Ok(None);
        }
        if ExecutionRepo::has_running_by_task_and_purpose(
            &*self.db,
            &task.id,
            api_types::ExecutionPurpose::AutomaticReviewRecovery,
        )
        .await?
        {
            return Ok(Some((
                task.clone(),
                "automatic review recovery already running".to_owned(),
            )));
        }

        if let Some(agent) = AgentRepo::get_by_id(&*self.db, &agent_id).await? {
            if let Some(wait) = self.capacity_wait_for(task, &agent, Some("coder")).await? {
                crate::deferred_dispatch::record_capacity_wait(&self.db, task, wait).await?;
                let waiting = TaskRepo::get_by_id(&*self.db, &task.id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", &task.id))?;
                return Ok(Some((
                    waiting,
                    "automatic review recovery waiting for machine capacity".to_owned(),
                )));
            }
        }

        // Keep the failed window's findings before dispatch starts a recovery entry.
        let recovery_review =
            ReviewRepo::list_latest_reviews_for_tasks(&*self.db, &[task.id.as_str()])
                .await?
                .into_iter()
                .next();
        let recovery_ledger = match &recovery_review {
            Some(review) => self.review_comment_ledger(review).await?,
            None => Vec::new(),
        };

        let prompt = render_automatic_review_recovery_prompt(
            task,
            failure_reason,
            existing_rejections,
            budget,
            parent_execution_id,
            recovery_attempts + 1,
            max_attempts,
        );
        let execution = match AUTOMATIC_REVIEW_RECOVERY_TASK
            .scope(
                task.id.clone(),
                self.dispatch_role_follow_up_with_agent(
                    &task.id,
                    crate::workflow::default_roles::CODER,
                    parent_execution_id.to_owned(),
                    agent_id.clone(),
                    prompt,
                    AUTOMATIC_REVIEW_RECOVERY_TRIGGER,
                ),
            )
            .await
        {
            Ok(execution) => execution,
            Err(error) if crate::placement::is_machine_capacity_refusal(&error) => {
                let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", &task.id))?;
                self.defer_placement_refusal(&current, &error).await?;
                let mut waiting = TaskRepo::get_by_id(&*self.db, &task.id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", &task.id))?;
                crate::deferred_dispatch::refresh_machine_wait(&self.db, &mut waiting).await?;
                return Ok(Some((
                    waiting,
                    "automatic review recovery waiting for machine capacity".to_owned(),
                )));
            }
            Err(error) => {
                tracing::warn!(
                    task_id = %task.id,
                    project_id = %project.id,
                    agent_id = %agent_id,
                    %error,
                    "automatic review recovery dispatch failed"
                );
                return Ok(None);
            }
        };

        if let Some(review) = &recovery_review {
            let comment = repair_round_comment(
                review.attempt_number,
                (recovery_attempts + 1) as i64,
                max_attempts as i64,
                &recovery_ledger,
                "automatic_review_recovery",
            );
            self.create_review_comment(
                review,
                "repair",
                None,
                format!("{comment} Recovery execution: {}.", execution.id),
            )
            .await?;
        }
        let latest_task = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .unwrap_or_else(|| task.clone());
        Ok(Some((
            latest_task,
            "automatic review recovery dispatched".to_owned(),
        )))
    }

    pub(crate) async fn cascade_completed_review_task(
        &self,
        task: &Task,
        target: &str,
        reason: &str,
        rejection: bool,
    ) -> Result<()> {
        if target == crate::workflow::default_states::MERGING {
            let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            if project.paused_at.is_some() {
                crate::deferred_dispatch::defer_integration_for_pause(&self.db, task).await?;
                tracing::info!(
                    task_id = %task.id,
                    project_id = %project.id,
                    from_state = %task.status,
                    to_state = %target,
                    "review passed while project was paused; integration deferred"
                );
                return Ok(());
            }
        }
        let from = task.status.clone();
        // A lead root's rejection enters planning as a re-plan.
        let bridge = if rejection {
            self.review_rejection_bridge(task, target, reason).await?
        } else {
            Default::default()
        };
        match self
            .transition(
                task.id.clone(),
                target.to_owned(),
                TransitionOptions {
                    bridge,
                    version: task.version,
                    reason: Some(reason.to_owned()),
                    triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow),
                    rejection,
                    defer_dispatch_seconds: None,
                },
            )
            .await
        {
            Ok(_) => {
                self.publish(ForgeEvent {
                    event_type: "task.auto_transitioned".to_owned(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskAutoTransitioned {
                        task_id: task.id.clone(),
                        from,
                        to: target.to_owned(),
                        reason: reason.to_owned(),
                    },
                });
                Ok(())
            }
            Err(ServiceError::Db(DbError::VersionConflict)) => {
                tracing::warn!(
                    task_id = %task.id,
                    "review completion cascade version conflict"
                );
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn gate_requires_user_approval(&self, task: &Task) -> Result<bool> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        Ok(workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(|state| state.gate_config.as_ref())
            .is_some_and(|gate_config| gate_config.requires_user_approval()))
    }
}

/// Whether `task` is already parked by a review-environment block that this
/// reviewer execution raised.
fn review_blocked_by_execution(task: &Task, execution_id: &str) -> bool {
    task.blocked_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .is_some_and(|blocked| {
            blocked["kind"] == json!(api_types::FailureKind::ReviewBlocked)
                && blocked["execution_id"].as_str() == Some(execution_id)
        })
}

pub(crate) fn reviewer_execution_lacks_exact_review_binding(
    execution: &Execution,
    review: &Review,
) -> bool {
    !exact_review_binding_matches(execution, review)
}

/// Return the one Review attempt explicitly bound to an execution. Candidate
/// parentage is intentionally excluded: several attempts may inspect the same
/// candidate, so it cannot distinguish an old reviewer from the current one.
pub(crate) fn exact_review_for_execution<'a>(
    execution: &Execution,
    reviews: &'a [Review],
) -> Option<&'a Review> {
    let mut matches = reviews
        .iter()
        .filter(|review| exact_review_binding_matches(execution, review));
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

/// Return whether the exact Review attempt bound to this execution has
/// already settled.  Candidate parentage is intentionally not considered:
/// multiple reviewer attempts may inspect the same candidate, while only the
/// durable reviewer/auditor execution binding identifies the attempt whose
/// session a recovery request would resume.
fn exact_review_binding_matches(execution: &Execution, review: &Review) -> bool {
    if review.reviewer_execution_id.is_some() || review.auditor_execution_id.is_some() {
        review.reviewer_execution_id.as_deref() == Some(execution.id.as_str())
            || review.auditor_execution_id.as_deref() == Some(execution.id.as_str())
    } else {
        review.execution_id == execution.id
    }
}

fn render_workflow_guard_follow_up_prompt(
    guard: &str,
    reason: &str,
    attempt: u64,
    budget: u64,
) -> String {
    let checklist_instruction = if guard == "require_conflict_markers_resolved" {
        "Fix the files named above: remove every conflict marker line and keep the correct merged code, verify nothing else in them is still conflicted, then commit the result. Forge re-checks the committed HEAD before the task can go to review."
    } else if guard == super::step_check::STEP_CHECK_RESULT_GUARD {
        "End your reply with exactly one JSON object on its own: {\"result\": \"pass\", \"reason\": \"one sentence\"} or {\"result\": \"fail\", \"reason\": \"one sentence\"}. Do not write a plan and do not change files."
    } else if guard == "planning_plan_ready" {
        "Write a valid Markdown checklist using `task.plan` for a native session or `$FORGE_PLAN_PATH` for a CLI harness."
    } else {
        "Make sure you complete all tasks and fix what is needed for this guard. Update completed checklist items to `- [x]` using `task.plan` for a native session or `$FORGE_PLAN_PATH` for a CLI harness; you do not need to commit if all implementation work is already complete."
    };
    format!(
        "Your previous execution completed, but Forge could not move the task to the next workflow state.\n\nWorkflow guard failed: {guard}\n\nFailure:\n{reason}\n\n{checklist_instruction}\n\nRetry {attempt}/{budget}."
    )
}

fn task_blocked_by_execution(task: &Task, execution_id: &str) -> bool {
    task.blocked_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|value| {
            value
                .get("execution_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .as_deref()
        == Some(execution_id)
}

fn render_automatic_review_recovery_prompt(
    task: &Task,
    failure_reason: &str,
    existing_rejections: i64,
    budget: i32,
    review_execution_id: &str,
    attempt: usize,
    max_attempts: usize,
) -> String {
    format!(
        "Forge automatic review recovery\n\n\
         The normal review retry flow is about to block this task, so this is the final automatic recovery attempt.\n\n\
         Task: {title}\n\
         Current status: {status}\n\
         Review failure: {failure_reason}\n\
         Review execution: {review_execution_id}\n\
         Rejections in current window: {rejections}/{budget}\n\
         Automatic recovery attempt: {attempt}/{max_attempts}\n\n\
         Inspect the workspace and the review failure context. Make the smallest useful change that addresses the failing review, then leave the task ready for the normal workflow to review again.",
        title = task.title,
        status = task.status,
        rejections = existing_rejections + 1,
    )
}

pub(super) async fn reviewer_final_message(execution: &Execution) -> Result<String> {
    if let Some(logs_path) = execution.logs_path.as_deref() {
        let contents = match tokio::fs::read_to_string(logs_path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => {
                return Err(ServiceError::invalid_operation(format!(
                    "failed to read reviewer logs: {error}"
                )));
            }
        };
        let mut message = String::new();
        let mut stdout_lines = String::new();
        for line in contents.lines() {
            let Ok(entry) = serde_json::from_str::<executors::LogEntry>(line) else {
                continue;
            };
            if entry.kind == executors::LogKind::Assistant {
                let mut candidate = String::new();
                append_reviewer_log_text(&entry.payload, &mut candidate);
                if !candidate.trim().is_empty() {
                    message = candidate;
                }
            } else if entry.kind == executors::LogKind::SessionInfo
                && entry.payload.get("subtype").and_then(Value::as_str) == Some("success")
            {
                if let Some(result) = entry.payload.get("result").and_then(Value::as_str) {
                    message = result.to_owned();
                }
            } else if entry.kind == executors::LogKind::Stdout {
                if let Some(line) = entry.payload.get("line").and_then(Value::as_str) {
                    stdout_lines.push_str(line);
                    stdout_lines.push('\n');
                }
            }
        }
        if !message.trim().is_empty() {
            return Ok(message);
        }
        if !stdout_lines.trim().is_empty() {
            return Ok(stdout_lines);
        }
    }

    Ok(execution.summary.clone().unwrap_or_default())
}

fn append_reviewer_log_text(payload: &Value, message: &mut String) {
    if let Some(text) = payload.get("text").and_then(Value::as_str) {
        message.push_str(text);
    }

    let Some(content) = payload
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
    else {
        return;
    };

    for item in content {
        if item.get("type").and_then(Value::as_str) == Some("text") {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                message.push_str(text);
            }
        }
    }
}

/// Retry hint plus deterministic jitter, bounded by backoff and six hours.
fn executor_unavailable_dispatch_time(
    retry_at: Option<&str>,
    task_id: &str,
    backoff_seconds: u64,
) -> chrono::DateTime<chrono::Utc> {
    let jitter_seconds = i64::from(
        task_id
            .bytes()
            .fold(0u8, |acc, byte| acc.wrapping_add(byte))
            % 30,
    );
    let now = chrono::Utc::now();
    let floor = now + chrono::Duration::seconds(backoff_seconds as i64);
    let hinted = retry_at
        .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
        .map(|at| at.with_timezone(&chrono::Utc))
        .unwrap_or(floor)
        .min(now + chrono::Duration::hours(6));
    (hinted + chrono::Duration::seconds(jitter_seconds))
        .max(floor)
        .min(now + chrono::Duration::hours(6))
}

pub(crate) fn should_block_task_for_failed_execution(execution: &Execution) -> bool {
    if super::guards::is_workspace_lease_pre_dispatch_failure(execution) {
        return false;
    }
    matches!(
        execution.role.as_str(),
        "interactive" | "executor" | crate::workflow::default_roles::CODER
    ) || (execution.role == crate::workflow::default_roles::PLANNER
        // Setup refusals keep dispatch's recovery semantics; only a planner
        // that actually ran joins the ordinary execution-failure retry path.
        && execution.stopped_by.as_deref() != Some("system:dispatch"))
}

fn execution_failure_reason(execution: &Execution) -> String {
    execution
        .error
        .as_deref()
        .or(execution.summary.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("ended with status {}", execution.status))
}

/// Upper bound on the reviewer's Markdown carried into its Task comment.
const REVIEW_COMMENT_REPORT_LIMIT: usize = 2 * 1024;
/// Upper bound on the comment's checklist lines; later findings are named
/// by id only. Comments are read back into agent prompts.
const REVIEW_COMMENT_FINDINGS_LIMIT: usize = 8 * 1024;
const REVIEW_COMMENT_SUMMARY_CHARS: usize = 200;

/// Render the reviewer's Task comment: a `Review <outcome> (attempt N)`
/// headline, the ledger after this attempt, optional verify facts, and the
/// full-report pointer. Only findings-less reviews carry a bounded report.
fn reviewer_comment(
    status: ReviewStatus,
    attempt_number: i64,
    conformance: &api_types::ReviewConformance,
    ledger: &[api_types::ReviewFindingEntry],
    verify: Option<&Value>,
) -> String {
    let reason = conformance
        .reason
        .as_deref()
        .map(one_line)
        .filter(|reason| !reason.is_empty());
    let reason = reason.as_deref();
    let mut out = match status {
        ReviewStatus::AwaitingHuman => format!(
            "Review passed automated checks and is awaiting user approval (attempt {attempt_number})"
        ),
        ReviewStatus::Passed => format!("Review passed (attempt {attempt_number})"),
        ReviewStatus::Failed if conformance.status == api_types::ConformanceStatus::Blocked => {
            format!(
                "Review blocked by its environment (attempt {attempt_number}): {}",
                reason.unwrap_or("no reason given")
            )
        }
        ReviewStatus::Failed => format!(
            "Review failed (attempt {attempt_number}): {}",
            reason.unwrap_or("review conformance failed")
        ),
        _ => format!("Review updated (attempt {attempt_number})"),
    };
    use api_types::{ReviewFindingSource as Source, ReviewFindingStatus as Status};
    // Open findings first, then resolved, then unconfirmed, each in id order,
    // so the size limit drops the least actionable lines.
    let mut entries: Vec<_> = ledger.iter().collect();
    entries.sort_by_key(|entry| match entry.status {
        Status::Open => 0,
        Status::Resolved => 1,
        Status::Unconfirmed => 2,
    });
    let mut listed = 0;
    let mut overflow = Vec::new();
    for entry in entries {
        // A line ending inside finding text would start a forged list item.
        let id = one_line(&entry.id);
        let summary = clip_chars(&one_line(&entry.summary), REVIEW_COMMENT_SUMMARY_CHARS);
        let line = match entry.status {
            Status::Open if entry.first_attempt == attempt_number => format!(
                "- [ ] {id} New: {summary} — open since attempt {}",
                entry.first_attempt
            ),
            Status::Open => format!(
                "- [ ] {id} {summary} — open since attempt {}",
                entry.first_attempt
            ),
            Status::Resolved => format!(
                "- [x] {id} {summary} — {}",
                if entry.source == Source::Reviewer {
                    "resolved"
                } else {
                    "passing now"
                }
            ),
            Status::Unconfirmed => format!("- {id} {summary} — not re-checked"),
        };
        if overflow.is_empty() && listed + line.len() < REVIEW_COMMENT_FINDINGS_LIMIT {
            listed += line.len() + 1;
            out.push('\n');
            out.push_str(&line);
        } else {
            overflow.push(id);
        }
    }
    if !overflow.is_empty() {
        out.push_str(&format!(
            "\n\nAlso in the ledger (see the Review tab): {}",
            overflow.join(", ")
        ));
    }
    let report = conformance
        .assessment
        .as_ref()
        .map(|assessment| assessment.report.trim())
        .unwrap_or_default();
    let has_reviewer_findings = conformance
        .assessment
        .as_ref()
        .is_some_and(|assessment| !assessment.findings.is_empty());
    if !has_reviewer_findings && !report.is_empty() {
        out.push_str("\n\n");
        let excerpt = truncate_on_char_boundary(report, REVIEW_COMMENT_REPORT_LIMIT);
        out.push_str(excerpt);
        // A cut inside a code block would swallow the lines after it.
        if let Some(fence) = open_code_fence(excerpt) {
            out.push('\n');
            out.push_str(fence);
        }
    }
    if let Some(verify) = verify.filter(|value| value.is_object()) {
        let status = verify["status"].as_str().unwrap_or("unknown");
        let failures: Vec<_> = verify["checks"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|check| matches!(check["result"].as_str(), Some("fail" | "failed")))
            .filter_map(|check| check["name"].as_str())
            .collect();
        let evidence: std::collections::BTreeSet<_> = verify["checks"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|check| check["evidence"].as_array().into_iter().flatten())
            .filter_map(Value::as_str)
            .collect();
        out.push_str(&format!("\n\nVerify: {status}"));
        if let Some(reason) = verify["reason"].as_str() {
            out.push_str(&format!(" — {reason}"));
        }
        if !failures.is_empty() {
            out.push_str(&format!(" — failing checks: {}", failures.join(", ")));
        }
        out.push_str(&format!(" ({} evidence)", evidence.len()));
        for id in evidence {
            out.push_str(&format!(" [evidence](/api/v1/media/{id})"));
        }
    }
    out.push_str(&format!(
        "\n\nFull review: Review tab, attempt {attempt_number}"
    ));
    out
}

fn finding_ids(
    ledger: &[api_types::ReviewFindingEntry],
    include: impl Fn(&api_types::ReviewFindingEntry) -> bool,
) -> String {
    let ids: Vec<_> = ledger
        .iter()
        .filter(|entry| include(entry))
        .map(|entry| entry.id.as_str())
        .collect();
    if ids.is_empty() {
        "none".to_owned()
    } else {
        ids.join(", ")
    }
}

fn repair_round_comment(
    attempt: i64,
    spent: i64,
    limit: i64,
    ledger: &[api_types::ReviewFindingEntry],
    budget: &str,
) -> String {
    use api_types::ReviewFindingStatus as Status;
    let open = finding_ids(ledger, |entry| entry.status == Status::Open);
    let resolved = finding_ids(ledger, |entry| {
        entry.status == Status::Resolved && entry.last_checked_attempt == attempt
    });
    format!("Repair round {attempt} ({budget} budget {spent} of {limit}): {open} open; {resolved} resolved since the last round.")
}

fn clip_chars(text: &str, limit: usize) -> String {
    match text.char_indices().nth(limit) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_owned(),
    }
}

/// Text on one Markdown line: CommonMark ends a line at a bare `\r` too.
fn one_line(text: &str) -> String {
    text.split(char::is_control)
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The fence of a code block the Markdown leaves open, if any.
fn open_code_fence(markdown: &str) -> Option<&str> {
    let mut open: Option<&str> = None;
    for line in markdown.lines() {
        let line = line.trim_start();
        let marker = match line.chars().next() {
            Some(ch @ ('`' | '~')) => &line[..line.len() - line.trim_start_matches(ch).len()],
            _ => continue,
        };
        if marker.len() < 3 {
            continue;
        }
        match open {
            None => open = Some(marker),
            Some(fence) if marker.starts_with(fence) && line[marker.len()..].trim().is_empty() => {
                open = None;
            }
            Some(_) => {}
        }
    }
    open
}

fn truncate_on_char_boundary(value: &str, limit: usize) -> &str {
    let mut end = value.len().min(limit);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[cfg(test)]
mod reviewer_message_tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn reviewer_execution(logs_path: String, summary: &str) -> Execution {
        let now = now_rfc3339();
        Execution {
            id: "execution-reviewer".to_owned(),
            task_id: "task-reviewer".to_owned(),
            agent_id: Some("agent-reviewer".to_owned()),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            execution_version: 1,
            lease_owner: None,
            lease_expires_at: None,
            hard_deadline_at: None,
            last_heartbeat_at: None,
            last_progress_at: None,
            prompt: None,
            summary: Some(summary.to_owned()),
            logs_path: Some(logs_path),
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn reviewer_final_message_reads_claude_assistant_content() {
        let file = NamedTempFile::new().expect("temp log creates");
        let log = json!({
            "schema_version": 1,
            "sequence": 1,
            "timestamp": "2026-04-27T00:00:00Z",
            "execution_id": "execution-reviewer",
            "kind": "assistant",
            "stream": "main",
            "payload": {
                "message": {
                    "content": [
                        {
                            "type": "text",
                            "text": "No issues found.\n===REVIEW: PASS==="
                        }
                    ]
                }
            },
            "truncated": false
        });
        std::fs::write(file.path(), format!("{log}\n")).expect("log writes");

        let execution = reviewer_execution(
            file.path().to_string_lossy().into_owned(),
            "Truncated summary without marker",
        );

        let message = reviewer_final_message(&execution)
            .await
            .expect("message extracts");
        assert!(message.contains("===REVIEW: PASS==="));
    }

    #[tokio::test]
    async fn reviewer_final_message_reads_claude_result_when_assistant_text_missing() {
        let file = NamedTempFile::new().expect("temp log creates");
        let log = json!({
            "schema_version": 1,
            "sequence": 1,
            "timestamp": "2026-04-27T00:00:00Z",
            "execution_id": "execution-reviewer",
            "kind": "session_info",
            "stream": "main",
            "payload": {
                "subtype": "success",
                "result": "Looks good.\n===REVIEW: PASS==="
            },
            "truncated": false
        });
        std::fs::write(file.path(), format!("{log}\n")).expect("log writes");

        let execution = reviewer_execution(
            file.path().to_string_lossy().into_owned(),
            "Truncated summary without marker",
        );

        let message = reviewer_final_message(&execution)
            .await
            .expect("message extracts");
        assert!(message.contains("===REVIEW: PASS==="));
    }

    fn conformance(
        assessment: Option<api_types::ReviewAssessment>,
    ) -> api_types::ReviewConformance {
        api_types::ReviewConformance {
            status: api_types::ConformanceStatus::Failed,
            contract: None,
            assessment,
            checks: Vec::new(),
            reason: Some("blocking finding".to_owned()),
        }
    }

    #[test]
    fn reviewer_comment_without_findings_keeps_the_bounded_markdown_report() {
        let assessment = api_types::ReviewAssessment {
            result: api_types::ReviewResult::Fail,
            reason: "api not exported".to_owned(),
            fixable_by: api_types::FixableBy::Coder,
            repeat: false,
            findings: Vec::new(),
            resolved: Vec::new(),
            report: "## R1\n\nExpected the api exported; `src/lib.rs:3` does not.".to_owned(),
        };
        let mut failed = conformance(Some(assessment.clone()));
        failed.reason = Some("api not exported".to_owned());

        assert_eq!(
            reviewer_comment(ReviewStatus::Failed, 2, &failed, &[], None),
            "Review failed (attempt 2): api not exported\n\n\
             ## R1\n\nExpected the api exported; `src/lib.rs:3` does not.\n\nFull review: Review tab, attempt 2"
        );

        let mut blocked = conformance(Some(api_types::ReviewAssessment {
            result: api_types::ReviewResult::Blocked,
            reason: "tsc not found".to_owned(),
            fixable_by: api_types::FixableBy::Coder,
            repeat: false,
            findings: Vec::new(),
            resolved: Vec::new(),
            report: String::new(),
        }));
        blocked.status = api_types::ConformanceStatus::Blocked;
        blocked.reason = Some("tsc not found".to_owned());
        assert_eq!(
            reviewer_comment(ReviewStatus::Failed, 1, &blocked, &[], None),
            "Review blocked by its environment (attempt 1): tsc not found\n\nFull review: Review tab, attempt 1"
        );
    }

    #[test]
    fn reviewer_comment_without_an_assessment_has_the_review_tab_pointer() {
        assert_eq!(
            reviewer_comment(ReviewStatus::Passed, 1, &conformance(None), &[], None),
            "Review passed (attempt 1)\n\nFull review: Review tab, attempt 1"
        );
        assert_eq!(
            reviewer_comment(ReviewStatus::Failed, 1, &conformance(None), &[], None),
            "Review failed (attempt 1): blocking finding\n\nFull review: Review tab, attempt 1"
        );
    }
    fn comment_finding(
        id: &str,
        status: api_types::ReviewFindingStatus,
    ) -> api_types::ReviewFindingEntry {
        api_types::ReviewFindingEntry {
            id: id.into(),
            source: api_types::ReviewFindingSource::Reviewer,
            severity: api_types::ReviewFindingSeverity::Blocking,
            summary: format!("Issue {id}"),
            r#where: None,
            expected: None,
            observed: None,
            first_attempt: 1,
            last_checked_attempt: 2,
            status,
            streak: if status == api_types::ReviewFindingStatus::Open {
                2
            } else {
                0
            },
        }
    }

    #[test]
    fn reviewer_comment_failed_findings_checklist_omits_the_report() {
        use api_types::ReviewFindingStatus as Status;
        let mut ledger = vec![
            comment_finding("F1", Status::Open),
            comment_finding("F2", Status::Resolved),
            comment_finding("F3", Status::Unconfirmed),
            comment_finding("F4", Status::Open),
            comment_finding("CI-1", Status::Resolved),
        ];
        ledger[3].first_attempt = 2;
        ledger[4].source = api_types::ReviewFindingSource::Ci;
        let mut result = conformance(None);
        result.assessment = Some(api_types::ReviewAssessment {
            result: api_types::ReviewResult::Fail,
            reason: "blocked".into(),
            report: "FULL REPORT MUST STAY OUT".into(),
            fixable_by: api_types::FixableBy::Coder,
            repeat: false,
            findings: vec![api_types::ReviewFinding {
                id: Some("F1".into()),
                summary: "Issue F1".into(),
                severity: api_types::ReviewFindingSeverity::Blocking,
                r#where: None,
                expected: None,
                observed: None,
            }],
            resolved: Vec::new(),
        });
        let text = reviewer_comment(ReviewStatus::Failed, 2, &result, &ledger, None);
        assert_eq!(text, "Review failed (attempt 2): blocking finding\n- [ ] F1 Issue F1 — open since attempt 1\n- [ ] F4 New: Issue F4 — open since attempt 2\n- [x] F2 Issue F2 — resolved\n- [x] CI-1 Issue CI-1 — passing now\n- F3 Issue F3 — not re-checked\n\nFull review: Review tab, attempt 2");
        assert!(!text.contains("FULL REPORT"));
        assert!(!text.contains("Verify:"));
    }

    #[test]
    fn reviewer_comment_passed_minor_findings_are_visible() {
        let mut finding = comment_finding("F1", api_types::ReviewFindingStatus::Open);
        finding.severity = api_types::ReviewFindingSeverity::Minor;
        let text = reviewer_comment(
            ReviewStatus::Passed,
            2,
            &conformance(None),
            &[finding],
            None,
        );
        assert_eq!(text, "Review passed (attempt 2)\n- [ ] F1 Issue F1 — open since attempt 1\n\nFull review: Review tab, attempt 2");
    }

    #[test]
    fn reviewer_comment_legacy_report_is_capped_at_two_kib_utf8_bytes() {
        let result = conformance(Some(api_types::ReviewAssessment {
            result: api_types::ReviewResult::Fail,
            reason: "blocked".into(),
            report: "界".repeat(7000),
            fixable_by: api_types::FixableBy::Coder,
            repeat: false,
            findings: Vec::new(),
            resolved: Vec::new(),
        }));
        let text = reviewer_comment(ReviewStatus::Failed, 2, &result, &[], None);
        let report = text.split("\n\n").nth(1).unwrap();
        assert_eq!(report, "界".repeat(2048 / 3));
        assert!(text.ends_with("Full review: Review tab, attempt 2"));
    }

    #[test]
    fn reviewer_comment_verify_only_when_recorded() {
        let verify = json!({"status":"fail","checks":[{"name":"smoke","result":"fail","evidence":["media-1", "media-1"]}]});
        let text = reviewer_comment(
            ReviewStatus::Failed,
            2,
            &conformance(None),
            &[],
            Some(&verify),
        );
        assert!(text.contains(
            "Verify: fail — failing checks: smoke (1 evidence) [evidence](/api/v1/media/media-1)"
        ));
        let skipped = json!({"status":"not_run","reason":"placement_unsupported"});
        let text = reviewer_comment(
            ReviewStatus::Passed,
            2,
            &conformance(None),
            &[],
            Some(&skipped),
        );
        assert!(text.contains("Verify: not_run — placement_unsupported"));
    }

    #[test]
    fn reviewer_comment_ci_failure_lists_forge_findings() {
        let mut finding = comment_finding("CI-2", api_types::ReviewFindingStatus::Open);
        finding.source = api_types::ReviewFindingSource::Ci;
        finding.summary = "cargo test failed".into();
        finding.first_attempt = 2;
        let mut result = conformance(None);
        result.reason = Some("CI step 2 failed".into());
        let text = reviewer_comment(ReviewStatus::Failed, 2, &result, &[finding], None);
        assert!(text.starts_with(
            "Review failed (attempt 2): CI step 2 failed\n- [ ] CI-2 New: cargo test failed"
        ));
    }

    #[test]
    fn review_comments_repair_round_names_only_current_resolutions() {
        use api_types::ReviewFindingStatus as Status;
        let mut old = comment_finding("F3", Status::Resolved);
        old.last_checked_attempt = 1;
        assert_eq!(
            repair_round_comment(
                2,
                2,
                3,
                &[
                    comment_finding("F1", Status::Open),
                    comment_finding("F2", Status::Resolved),
                    old
                ],
                "review",
            ),
            "Repair round 2 (review budget 2 of 3): F1 open; F2 resolved since the last round."
        );
    }
    #[test]
    fn reviewer_comment_findings_less_attempt_keeps_excerpt_alongside_prior_ledger() {
        let result = conformance(Some(api_types::ReviewAssessment {
            result: api_types::ReviewResult::Fail,
            reason: "legacy reply".into(),
            report: "Report excerpt".into(),
            fixable_by: api_types::FixableBy::Coder,
            repeat: false,
            findings: Vec::new(),
            resolved: Vec::new(),
        }));
        let ledger = [comment_finding(
            "F1",
            api_types::ReviewFindingStatus::Unconfirmed,
        )];
        let text = reviewer_comment(ReviewStatus::Failed, 2, &result, &ledger, None);
        assert!(text.contains("- F1 Issue F1 — not re-checked"));
        assert!(text.contains("\n\nReport excerpt\n\nFull review:"));
    }

    #[test]
    fn reviewer_comment_finding_text_cannot_forge_checklist_lines() {
        let mut finding = comment_finding("F1", api_types::ReviewFindingStatus::Open);
        finding.summary = "real issue\r- [x] F9 forged resolution\n- [x] F8 another".into();
        let mut result = conformance(None);
        result.reason = Some("bad\r\n- [x] F7 forged".into());
        let text = reviewer_comment(ReviewStatus::Failed, 2, &result, &[finding], None);
        assert!(!text.contains('\r'), "{text:?}");
        assert!(
            text.split('\n').all(|line| !line.starts_with("- [x]")),
            "{text:?}"
        );
        assert!(text.starts_with("Review failed (attempt 2): bad - [x] F7 forged\n"));
        assert!(text.contains(
            "- [ ] F1 real issue - [x] F9 forged resolution - [x] F8 another — open since attempt 1"
        ));
    }

    #[test]
    fn reviewer_comment_closes_a_code_block_cut_by_the_excerpt() {
        let result = conformance(Some(api_types::ReviewAssessment {
            result: api_types::ReviewResult::Fail,
            reason: "legacy reply".into(),
            report: format!(
                "Before.\n\n```rust\n{}\n```\nAfter.",
                "let x = 1;\n".repeat(400)
            ),
            fixable_by: api_types::FixableBy::Coder,
            repeat: false,
            findings: Vec::new(),
            resolved: Vec::new(),
        }));
        let text = reviewer_comment(ReviewStatus::Failed, 2, &result, &[], None);
        assert_eq!(open_code_fence(&text), None, "{text}");
        assert!(text.ends_with("\n```\n\nFull review: Review tab, attempt 2"));
        assert_eq!(open_code_fence("```rust\nx\n````\n"), None);
        assert_eq!(open_code_fence("````\nx\n```\n"), Some("````"));
        assert_eq!(open_code_fence("~~~\nx\n```\n"), Some("~~~"));
    }

    #[test]
    fn reviewer_comment_stays_bounded_with_two_hundred_findings() {
        use api_types::ReviewFindingStatus as Status;
        let ledger: Vec<_> = (1..=200)
            .map(|n| {
                let mut entry = comment_finding(
                    &format!("F{n}"),
                    if n > 150 {
                        Status::Open
                    } else {
                        Status::Resolved
                    },
                );
                entry.summary = "界".repeat(1000);
                entry
            })
            .collect();
        let text = reviewer_comment(ReviewStatus::Failed, 2, &conformance(None), &ledger, None);
        assert!(text.len() < 12 * 1024, "{} bytes", text.len());
        // Open findings are listed first, and every id is still named.
        assert!(text.contains("\n- [ ] F151 "));
        let words: std::collections::BTreeSet<_> =
            text.split(|ch: char| !ch.is_ascii_alphanumeric()).collect();
        for n in 1..=200 {
            assert!(words.contains(format!("F{n}").as_str()), "F{n} missing");
        }
        assert!(text.contains("Also in the ledger (see the Review tab): "));
        assert!(text.ends_with("Full review: Review tab, attempt 2"));
    }
}
