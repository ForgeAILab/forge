//! Executor completion and brokered-plan publication settlement.

use super::*;

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

    pub(in crate::task_service::execution) async fn discard_brokered_execution_plan_stage(
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
}
