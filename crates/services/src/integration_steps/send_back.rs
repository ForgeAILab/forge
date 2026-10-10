//! `send_back`: an attempt the queue released without a merge goes back to
//! a role, through exactly the writes today's merge hook performs for the
//! same outcome (annotation, comment, `merge.failed`, budget, typed bridge).
use super::{ack_value, attempt_ref, bounded, IntegrationSteps, StepOutcome};
use crate::{
    integration_worker::{IntegrationStepAction, IntegrationStepOutcome, IntegrationStepRequest},
    workflow::{
        actions::{conflict_handoff_result, merge_failure_result, record_integration_failure},
        default_states,
        engine::{WorkflowAuthority, WorkflowEngine},
        HookContext, HookResult,
    },
    Result, ServiceError,
};
use api_types::{
    FailureKind, IntegrationPaths, IntegrationReason, StateKind, TransitionBridge,
    TransitionBridgeKind,
};
use db::{
    ConditionStatement, IntegrationAttempt, IntegrationAttemptState as S, IntegrationFailureKind,
    IntegrationQueueRepo, IntegrationStepAckOutcome, IntegrationStepAckWrite,
    IntegrationStepPosition, TaskRepo, TaskStep, TaskStepRepo,
};
use std::sync::Arc;

impl IntegrationSteps {
    /// The hook context today's merge-failure functions act through: the
    /// `merging` state of the Task's current workflow.
    async fn merging_context(
        &self,
        task: &db::Task,
        project: &db::Project,
        attempt: &IntegrationAttempt,
    ) -> Result<HookContext> {
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &crate::worker_runtime::queue::cascade_actor(),
        );
        let state = workflow
            .states
            .iter()
            .find(|state| state.name == attempt.expected_status)
            .ok_or_else(|| ServiceError::invalid_operation("the admitted state left the workflow"))?
            .clone();
        let execution =
            crate::task_service::latest_executor_execution_for_task(&self.db, task).await?;
        let service = &self.task_service;
        Ok(HookContext {
            task_id: task.id.clone(),
            project_id: task.project_id.clone(),
            from_state: attempt.expected_status.clone(),
            to_state: attempt.expected_status.clone(),
            db: Arc::clone(&self.db),
            event_bus: Arc::clone(&self.engine.event_bus),
            gate_config: state.gate_config.clone(),
            workflow: Arc::new(workflow),
            project_version: Some(project.version),
            project_workflow_definition: Some(project.workflow_definition.clone()),
            triggered_by: crate::worker_runtime::queue::cascade_actor(),
            review_runner: service.review_runner.clone(),
            merge_service: service.merge_service.clone(),
            cleanup_scheduler: service.cleanup_scheduler.clone(),
            task_service: service.clone(),
            daemon_connections: service.daemon_connections.clone(),
            workspace_exec_locks: service.workspace_exec_locks.clone(),
            terminal_activity: service.terminal_activity.clone(),
            workspace_root: service.workspace_root.clone(),
            repo_cache_locks: service.repo_cache_locks.clone(),
            workspace_backend_router: Arc::clone(&service.workspace_backend_router),
            workspace_id: attempt
                .workspace_id
                .clone()
                .or_else(|| execution.as_ref().and_then(|e| e.workspace_id.clone())),
            agent_id: None,
            execution_id: execution.map(|execution| execution.id),
            state_config: crate::workflow::engine::effective_state_config(
                &state,
                Some(project),
                task.task_state_config.as_deref(),
            ),
        })
    }

    pub(crate) async fn send_back(
        &self,
        step: &TaskStep,
        request: &IntegrationStepRequest,
    ) -> Result<StepOutcome> {
        let action = IntegrationStepAction::SendBack;
        let states = [S::Ejected, S::NeedsReview];
        let Some(attempt) = self.db.integration_attempt(&request.attempt_id).await? else {
            return Ok(StepOutcome::Skip("attempt is gone".into()));
        };
        match db::integration_step_position(
            &attempt,
            request.generation,
            request.effect_seq,
            false,
            action.as_str(),
            &states,
        ) {
            IntegrationStepPosition::Answered => {
                return Ok(StepOutcome::Skip("already answered".into()))
            }
            IntegrationStepPosition::Stale => {
                return Ok(StepOutcome::Skip("the attempt moved on".into()))
            }
            IntegrationStepPosition::NotYet => {
                return Ok(StepOutcome::Retry("attempt is not released yet".into()))
            }
            IntegrationStepPosition::Due => {}
        }
        if !self.db.step_entry_matches(step).await? {
            return Ok(StepOutcome::TaskLeft);
        }
        let mut task = TaskRepo::get_by_id(&*self.db, &attempt.task_ref, false)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let queue = match attempt.queue_id.as_deref() {
            Some(id) => self.db.integration_queue(id).await?,
            None => None,
        };
        let target_branch = queue
            .map(|queue| queue.target_branch)
            .unwrap_or_else(|| "the target branch".into());
        let ctx = self.merging_context(&task, &project, &attempt).await?;
        let detail = attempt
            .failure_message
            .clone()
            .unwrap_or_else(|| "integration was sent back".into());
        let clear_review = |task: db::Task| async {
            if task.review_passed_at.is_none() {
                return Ok::<_, ServiceError>(task);
            }
            Ok(TaskRepo::set_review_passed_at_cas(
                &*self.db,
                &task.id,
                task.version,
                None,
                &db::now_rfc3339(),
            )
            .await?)
        };

        // The typed reason first, then today's handoff for that outcome.
        let (reason, result) = if attempt.state == S::NeedsReview {
            // Today's `MergeOutcome::ReviewRequired`.
            clear_review(task).await?;
            (
                IntegrationReason::ReviewRequired {
                    attempt_id: attempt_ref(&attempt.id),
                    authority_reason: bounded(&detail),
                },
                HookResult::Cascade {
                    to: default_states::MERGE_FAILED.to_string(),
                    reason: format!("conformance review required: {detail}"),
                    bridge: TransitionBridge::new(TransitionBridgeKind::ReviewRefresh),
                },
            )
        } else if attempt.failure_kind == Some(IntegrationFailureKind::CandidateCheckFailed) {
            // The rebased commit is red: a merge failure the coder fixes
            // under the merge-fix budget, as any failed integration is.
            let details = format!("checks failed after the rebase onto {target_branch}: {detail}");
            task = record_integration_failure(
                &ctx,
                &task,
                FailureKind::CiFailed,
                &details,
                Some(format!("Merge checks failed: {details}")),
            )
            .await?;
            (
                IntegrationReason::CandidateCheckFailed {
                    attempt_id: attempt_ref(&attempt.id),
                    check: "ci_steps".into(),
                    message: bounded(&detail),
                },
                merge_failure_result(&ctx, &task, details, FailureKind::CiFailed).await,
            )
        } else {
            // A rebase conflict. The owner already committed it with markers
            // (the queue rebases with `handoff_conflicts`): today's conflict
            // handoff to the Worker, or manual repair when no path is known.
            let paths: Vec<String> = attempt
                .conflict_paths_json
                .as_ref()
                .and_then(|paths| serde_json::from_value(paths.clone()).ok())
                .unwrap_or_default();
            task = clear_review(task).await?;
            if let Some(workspace) = db::WorkspaceRepo::get_by_task_id(&*self.db, &task.id).await? {
                if let Ok(resolved) =
                    crate::workspace_backend::EmbeddedWorkspaceBackend::resolve_workspace(
                        &ctx.workspace_backend_router,
                        &ctx.db,
                        &workspace,
                        &ctx.workspace_root,
                    )
                    .await
                {
                    resolved.record_head_best_effort(&ctx.db).await;
                }
            }
            let coordination_root =
                crate::task_hierarchy::coordination_root_has_subtasks(&self.db, &task).await?;
            let result = if !coordination_root && !paths.is_empty() {
                conflict_handoff_result(&ctx, &task, &target_branch, &paths).await
            } else {
                merge_failure_result(
                    &ctx,
                    &task,
                    format!("rebase onto {target_branch} conflicted: {detail}"),
                    FailureKind::MergeConflict,
                )
                .await
            };
            (
                IntegrationReason::Repair {
                    attempt_id: attempt_ref(&attempt.id),
                    predecessor_attempt_id: attempt
                        .predecessor_attempt_id
                        .as_deref()
                        .map(attempt_ref),
                    conflict_paths: (!paths.is_empty())
                        .then(|| IntegrationPaths::bounded(paths.iter().cloned())),
                    repair_paths: IntegrationPaths::bounded(paths.iter().cloned()),
                },
                result,
            )
        };
        let cascade = match result {
            HookResult::Cascade { to, reason, bridge } => Some((to, reason, bridge)),
            // Blocked for the owner (budget spent, manual repair): the Task
            // stays where it is with today's block and annotation.
            HookResult::Ok | HookResult::Skipped { .. } => None,
            HookResult::Failed { reason } => return Err(ServiceError::invalid_operation(reason)),
        };
        let task = TaskRepo::get_by_id(&*self.db, &attempt.task_ref, false)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let follow_up = match cascade {
            Some((to, reason, bridge)) => {
                let workflow = ctx.workflow.as_ref();
                // The rejection rule of a hook cascade out of a gate.
                let gate = workflow.state_kind(&attempt.expected_status) == Some(StateKind::Gate);
                let rejection = gate
                    && bridge.bridge_kind != Some(TransitionBridgeKind::GateSkipped)
                    && !bridge.is_review_refresh()
                    && bridge.bridge_kind != Some(TransitionBridgeKind::ConflictHandoff)
                    && workflow.state_kind(&to) != Some(StateKind::Terminal);
                Some(
                    self.engine
                        .bind(&self.task_service)
                        .cascade_step_input(
                            &task,
                            workflow,
                            to,
                            reason,
                            bridge,
                            rejection,
                            false,
                            Some(WorkflowAuthority {
                                project_version: project.version,
                                workflow_definition: project.workflow_definition.clone(),
                                clear_review_passed_at_on_commit: false,
                            }),
                            Some(step),
                            format!("{}:cascade", step.causation_key),
                            Some(attempt.expected_epoch),
                        )
                        .await?,
                )
            }
            None => None,
        };

        let mut tx = db::begin_immediate(self.db.pool()).await?;
        if !Self::entry_live_in_tx(&mut tx, &attempt).await? {
            return Ok(StepOutcome::TaskLeft);
        }
        let stated = self
            .state_in_tx(
                &mut tx,
                &task.id,
                &ConditionStatement::Integration { reason },
            )
            .await?;
        if stated && follow_up.is_some() {
            self.state_in_tx(
                &mut tx,
                &task.id,
                &ConditionStatement::IntegrationHandedOff {
                    attempt_id: attempt_ref(&attempt.id),
                },
            )
            .await?;
        }
        let written = db::acknowledge_integration_step_in_tx(
            &mut tx,
            &IntegrationStepAckWrite {
                attempt_id: attempt.id.clone(),
                generation: request.generation,
                effect_seq: request.effect_seq,
                bind_generation: false,
                states: states.to_vec(),
                ack: ack_value(request, action, IntegrationStepOutcome::Done, None, None),
                permit: None,
                acknowledged_at: db::now_rfc3339(),
            },
        )
        .await?;
        if !matches!(
            written,
            IntegrationStepAckOutcome::Written(_)
                | IntegrationStepAckOutcome::AlreadyAcknowledged(_)
        ) {
            return Ok(StepOutcome::Retry(
                "attempt changed while sending back".into(),
            ));
        }
        self.db
            .finish_step_in_tx(&mut tx, step, "done", None)
            .await?;
        if let Some(follow_up) = &follow_up {
            self.db.enqueue_step_in_tx(&mut tx, follow_up).await?;
        }
        tx.commit().await?;
        self.task_service.dispatch_wake.notify_one();
        Ok(StepOutcome::Settled)
    }
}
