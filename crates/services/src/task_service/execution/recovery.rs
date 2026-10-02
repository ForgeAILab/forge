use super::*;
use db::TaskMetadataMutation;

tokio::task_local! {
    pub(crate) static REPLAYING_RECOVERY: (String, String);
}

impl TaskService {
    /// Read historical persisted commands at the storage boundary. No old
    /// action name is accepted by a command endpoint or transport schema.
    async fn normalize_stored_task_action(&self, task: &Task) -> Result<Task> {
        if crate::deferred_dispatch::queued_recovery(task).is_some() {
            return Ok(task.clone());
        }
        let metadata = db::TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let Some(raw) = metadata
            .extra
            .get(crate::deferred_dispatch::QUEUED_RECOVERY_KEY)
        else {
            return Ok(task.clone());
        };
        if raw
            .get("request")
            .and_then(|request| request.get("offer"))
            .is_some()
        {
            return Ok(task.clone());
        }
        let id = raw
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ServiceError::invalid_operation("stored queued intent has no id"))?;
        let request = &raw["request"];
        let guidance = request
            .get("context")
            .or_else(|| request.get("resume_reason"))
            .or_else(|| request.get("reason"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let name = request
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("resume_session");
        let actor = Actor::user(UserActionSource::Api);
        let mut snapshot = self.task_action_snapshot(&task.id, &actor).await?;
        snapshot.task.error_annotation = raw
            .get("error_annotation")
            .and_then(Value::as_str)
            .map(str::to_owned);
        snapshot.task.blocked_json = raw
            .get("blocked_json")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut projection = metadata.clone();
        projection
            .extra
            .remove(crate::deferred_dispatch::QUEUED_RECOVERY_KEY);
        snapshot.task.metadata_json = Some(
            serde_json::to_string(&projection)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        );
        let action = match name {
            "reset_to_initial" => Some(api_types::TaskAction::Restart),
            "cancel_task" => Some(api_types::TaskAction::Cancel),
            "mark_reviewed" | "skip_hook_once" => Some(api_types::TaskAction::Approve {
                override_checks: true,
            }),
            "defer_to_follow_up" => Some(api_types::TaskAction::Approve {
                override_checks: false,
            }),
            "resume_process" if snapshot.task.status == "review" => {
                Some(api_types::TaskAction::SendBack {
                    guidance: guidance
                        .clone()
                        .unwrap_or_else(|| "Return for revisions.".to_owned()),
                })
            }
            "resume_session"
            | "reexecute"
            | "retry_hook"
            | "resume_process"
            | "update_workspace_and_retry_hook"
            | "reset_retry_window"
            | "proceed_once" => Some(api_types::TaskAction::Retry {
                fresh_session: match name {
                    "resume_session" => Some(false),
                    "reexecute" => Some(true),
                    _ => None,
                },
                refresh_workspace: (name == "update_workspace_and_retry_hook").then_some(true),
                reset_budget: match name {
                    "reset_retry_window" => Some(true),
                    "proceed_once" => Some(false),
                    _ => None,
                },
                guidance: guidance.clone(),
            }),
            _ => None,
        };
        let offers = crate::available_actions(&snapshot);
        let selected = action.as_ref().and_then(|action| {
            offers
                .into_iter()
                .find(|offer| offer.action.verb() == action.verb())
        });
        // Keep the original bytes as evidence even when the state/owner has
        // changed and the old pending command can no longer be represented.
        let saved = TaskRepo::mutate_metadata_and_bump_version(
            &*self.db,
            &task.id,
            task.version,
            vec![TaskMetadataMutation::Set {
                key: "task_action_migrated_intent".to_owned(),
                value: raw.clone(),
            }],
            &now_rfc3339(),
        )
        .await?;
        let same_state =
            raw.get("target_state").and_then(Value::as_str) == Some(task.status.as_str());
        if !same_state || selected.is_none() {
            return TaskRepo::restore_queued_recovery(
                &*self.db,
                db::RestoreQueuedRecovery {
                    task_id: saved.id,
                    expected_version: saved.version,
                    queued_recovery_id: id.to_owned(),
                    error_annotation: if same_state {
                        task.error_annotation
                            .clone()
                            .or(snapshot.task.error_annotation)
                    } else {
                        task.error_annotation.clone()
                    },
                    blocked_json: if same_state {
                        task.blocked_json.clone().or(snapshot.task.blocked_json)
                    } else {
                        task.blocked_json.clone()
                    },
                    updated_at: now_rfc3339(),
                },
            )
            .await
            .map_err(Into::into);
        }
        snapshot.task.version = saved.version;
        let offer = selected.expect("selected above");
        let mut action = action.expect("selected action");
        // Missing legacy defaults now come from the offer. Parameters absent
        // from the state are not carried into a different apply plan.
        if let api_types::TaskAction::Retry {
            fresh_session,
            refresh_workspace,
            reset_budget,
            guidance,
        } = &mut action
        {
            if let api_types::TaskAction::Retry {
                fresh_session: fresh,
                refresh_workspace: refresh,
                reset_budget: reset,
                ..
            } = &offer.action
            {
                if !offer
                    .parameters
                    .iter()
                    .any(|parameter| parameter.name == "fresh_session")
                {
                    *fresh_session = *fresh;
                }
                if !offer
                    .parameters
                    .iter()
                    .any(|parameter| parameter.name == "refresh_workspace")
                {
                    *refresh_workspace = *refresh;
                }
                if !offer
                    .parameters
                    .iter()
                    .any(|parameter| parameter.name == "reset_budget")
                {
                    *reset_budget = *reset;
                }
                if !offer
                    .parameters
                    .iter()
                    .any(|parameter| parameter.name == "guidance")
                {
                    *guidance = None;
                }
            }
        }
        self.queue_task_action(
            &snapshot,
            offer,
            action.clone(),
            Actor::user(UserActionSource::Action(action)),
        )
        .await
    }

    /// Dispatcher-only consumption of an accepted Task action. This never
    /// calls a command handler or restores the old condition to authorize it.
    pub(crate) async fn dispatch_queued_recovery(&self, task: &Task) -> Result<bool> {
        let task = self.normalize_stored_task_action(task).await?;
        let Some(queued) = crate::deferred_dispatch::queued_recovery(&task) else {
            return Ok(false);
        };
        if queued.target_state != task.status
            || task.archived_at.is_some()
            || task.failed_json.is_some()
        {
            TaskRepo::mutate_metadata_and_bump_version(
                &*self.db,
                &task.id,
                task.version,
                vec![
                    TaskMetadataMutation::RemoveIf {
                        key: crate::deferred_dispatch::QUEUED_RECOVERY_KEY.to_owned(),
                        expected: serde_json::to_value(&queued)
                            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                    },
                    TaskMetadataMutation::Remove {
                        key: "deferred_dispatch".to_owned(),
                    },
                ],
                &now_rfc3339(),
            )
            .await?;
            return Ok(false);
        }
        if ExecutionRepo::list_running_by_task(&*self.db, &task.id)
            .await?
            .iter()
            .any(|execution| execution.role != "interactive")
        {
            return Ok(false);
        }
        if let Some(agent_id) = queued.request.agent_id.as_deref() {
            let agent = AgentRepo::get_by_id(&*self.db, agent_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("agent", agent_id.to_owned()))?;
            if !crate::agent_capacity::has_execution_capacity(&self.db, &agent, None).await? {
                return Ok(false);
            }
        }
        let claimed = TaskRepo::mutate_metadata_and_bump_version(
            &*self.db,
            &task.id,
            task.version,
            Vec::new(),
            &now_rfc3339(),
        )
        .await?;
        let result = REPLAYING_RECOVERY
            .scope(
                (claimed.id.clone(), queued.id.clone()),
                crate::task_service::actions::TASK_ACTION_ACTOR.scope(
                    queued.request.actor.clone(),
                    Box::pin(self.apply_queued_task_action(claimed, &queued)),
                ),
            )
            .await;
        match result {
            Err(error)
                if crate::placement::admission_refusal_is_retryable(&self.db, &task.id, &error)
                    .await? =>
            {
                return Ok(false)
            }
            Err(error) => return Err(error),
            Ok(_) => {}
        }
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        if crate::deferred_dispatch::queued_recovery(&current)
            .is_some_and(|value| value.id == queued.id)
        {
            TaskRepo::mutate_metadata_and_bump_version(
                &*self.db,
                &current.id,
                current.version,
                vec![
                    TaskMetadataMutation::RemoveIf {
                        key: crate::deferred_dispatch::QUEUED_RECOVERY_KEY.to_owned(),
                        expected: serde_json::to_value(&queued)
                            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                    },
                    TaskMetadataMutation::Remove {
                        key: "deferred_dispatch".to_owned(),
                    },
                ],
                &now_rfc3339(),
            )
            .await?;
        }
        Ok(true)
    }

    async fn apply_queued_task_action(
        &self,
        task: Task,
        queued: &crate::deferred_dispatch::QueuedRecovery,
    ) -> Result<Task> {
        let request = &queued.request;
        let guidance = match &request.action {
            api_types::TaskAction::Retry { guidance, .. } => guidance.clone(),
            api_types::TaskAction::SendBack { guidance } => Some(guidance.clone()),
            _ => None,
        };
        let annotation = queued
            .error_annotation
            .as_deref()
            .and_then(|raw| serde_json::from_str::<api_types::TaskBlockingAnnotation>(raw).ok());
        let role_launch = matches!(
            request.offer.reason.as_str(),
            "ready_to_start" | "manually_held" | "role_retry" | "merge_fix_retry"
        );
        let apply = async {
            match request.offer.reason.as_str() {
                "hard_failure" | "interrupted_restart" => {
                    let annotation =
                        annotation.unwrap_or_else(|| api_types::TaskBlockingAnnotation {
                            annotation_type: api_types::FailureKind::ExecutorFailed,
                            blocking_reason: "restart".to_owned(),
                            blocked_by: None,
                            blocked_at: None,
                            blocked_execution_id: None,
                            artifact: None,
                            message: None,
                            hook: None,
                        });
                    self.restart_task_condition(task, &annotation, Some("restart".to_owned()))
                        .await
                }
                "gate_waiting_for_decision"
                | "human_review_decision"
                | "human_review_send_back"
                | "work_ready_to_submit"
                | "gate_can_reject" => {
                    let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                        .await?
                        .ok_or_else(|| {
                            ServiceError::not_found("project", task.project_id.clone())
                        })?;
                    let workflow = WorkflowEngine::resolve_workflow_for_task(
                        &task,
                        &project.workflow_definition,
                        &request.actor,
                    );
                    let (trigger, guidance) = match &request.action {
                        api_types::TaskAction::SendBack { guidance } => {
                            (api_types::WorkflowTrigger::Reject, Some(guidance.clone()))
                        }
                        _ => (api_types::WorkflowTrigger::Accept, None),
                    };
                    self.apply_gate_decision(
                        &task,
                        &workflow,
                        trigger,
                        guidance,
                        request.actor.clone(),
                        matches!(
                            request.offer.reason.as_str(),
                            "human_review_decision" | "human_review_send_back"
                        ),
                    )
                    .await
                }
                "ready_to_start" => {
                    let agent = request.agent_id.clone().ok_or_else(|| {
                        ServiceError::invalid_operation("accepted start lost its agent")
                    })?;
                    let claimed = self
                        .claim_task(task.id, Assignee::Agent(agent), None)
                        .await?;
                    self.start_execution(claimed.execution.id.clone()).await?;
                    Ok(claimed.task)
                }
                "manually_held" | "role_retry" | "merge_fix_retry" => {
                    let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                        .await?
                        .ok_or_else(|| {
                            ServiceError::not_found("project", task.project_id.clone())
                        })?;
                    let workflow = WorkflowEngine::resolve_workflow_for_task(
                        &task,
                        &project.workflow_definition,
                        &request.actor,
                    );
                    let role = workflow
                        .states
                        .iter()
                        .find(|state| state.name == task.status)
                        .and_then(crate::workflow::effective_role)
                        .ok_or_else(|| {
                            ServiceError::invalid_operation("accepted retry lost its workflow role")
                        })?;
                    let fresh = match &request.action {
                        api_types::TaskAction::Retry { fresh_session, .. } => (*fresh_session)
                            .or(match request.offer.action {
                                api_types::TaskAction::Retry { fresh_session, .. } => fresh_session,
                                _ => None,
                            })
                            .unwrap_or(request.offer.target_execution_id.is_none()),
                        _ => false,
                    };
                    if !fresh {
                        if let Some(execution_id) = request.offer.target_execution_id.as_deref() {
                            let execution = ExecutionRepo::get_by_id(&*self.db, execution_id)
                                .await?
                                .ok_or_else(|| {
                                    ServiceError::not_found("execution", execution_id.to_owned())
                                })?;
                            let logical_thread = workflow.states.iter().find(|state| state.name == task.status)
                            .and_then(|state| state.dispatch.as_ref()).is_some_and(|dispatch| dispatch.execution_policy == Some(api_types::WorkflowExecutionPolicy::ResumeLatestTargetRoleThread));
                            if logical_thread && execution.agent_session_id.is_none() {
                                self.dispatch_role_follow_up(
                                    &task.id,
                                    role,
                                    execution.id,
                                    guidance.unwrap_or_else(|| "Continue Task work.".to_owned()),
                                    "task_action",
                                )
                                .await?;
                                return Ok(TaskRepo::get_by_id(&*self.db, &task.id, false)
                                    .await?
                                    .expect("Task remains present"));
                            }
                            let result = self
                                .follow_up_execution(
                                    execution.id,
                                    guidance.unwrap_or_else(|| "Continue Task work.".to_owned()),
                                    execution.agent_id,
                                    None,
                                )
                                .await?;
                            self.start_execution(result.execution.id.clone()).await?;
                            return Ok(result.task);
                        }
                    }
                    if role == crate::workflow::default_roles::REVIEWER {
                        self.ensure_review_attempt_for_recovery(&task, &project)
                            .await?;
                    }
                    let agent_id = request.agent_id.as_deref().ok_or_else(|| {
                        ServiceError::invalid_operation("accepted retry lost its agent")
                    })?;
                    self.dispatch_initial_role_execution(
                        &task.id,
                        agent_id,
                        role,
                        guidance.unwrap_or_else(|| "Retry Task work.".to_owned()),
                    )
                    .await?;
                    Ok(TaskRepo::get_by_id(&*self.db, &task.id, false)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?)
                }
                "entry_barrier_blocked" | "state_hooks_failed" => {
                    if matches!(
                        &request.action,
                        api_types::TaskAction::Retry {
                            refresh_workspace: Some(true),
                            ..
                        }
                    ) {
                        self.refresh_workspace_and_retry_entry(task, guidance).await
                    } else if task.entry_barrier_json.is_some() {
                        self.retry_entry_checks(task, guidance).await
                    } else {
                        self.recover_retry_current_state_hooks(task, guidance).await
                    }
                }
                "entry_barrier_override" | "state_hooks_override" => {
                    self.override_entry_checks_once(task, Some("approve override".to_owned()), None)
                        .await
                }
                "retry_budget_exhausted" => {
                    let mut evidence = task;
                    evidence.error_annotation = queued.error_annotation.clone();
                    evidence.blocked_json = queued.blocked_json.clone();
                    if matches!(
                        &request.action,
                        api_types::TaskAction::Retry {
                            reset_budget: Some(false),
                            ..
                        }
                    ) {
                        self.permit_one_task_retry(
                            evidence,
                            Some("approve one retry".to_owned()),
                            guidance,
                        )
                        .await
                    } else {
                        self.reset_task_retry_budget(evidence, guidance).await
                    }
                }
                "retry_budget_one_attempt" => {
                    self.permit_one_task_retry(task, Some("approve one retry".to_owned()), None)
                        .await
                }
                "review_checks_retry" => {
                    self.apply_review_check_retry(
                        &task,
                        request.offer.clone(),
                        request.action.clone(),
                        request.actor.clone(),
                    )
                    .await
                }
                "review_failed" => self.retry_review_entry(task, guidance).await,
                "review_owner_retry" => self.retry_review_entry(task, guidance).await,
                "merge_retry_remediation" => {
                    let mut evidence = task;
                    evidence.error_annotation = queued.error_annotation.clone();
                    evidence.blocked_json = queued.blocked_json.clone();
                    self.continue_task_process(evidence, guidance.clone(), guidance)
                        .await
                }
                "manual_merge_repair" => {
                    self.recover_manual_merge_repair_for_review(task, guidance)
                        .await
                }
                "merge_gate_retry" => self.recover_retry_current_state_hooks(task, guidance).await,
                "pull_request_merge_wait" => {
                    self.recover_pull_request_merge_wait(task, guidance).await
                }
                "review_needs_owner" | "reviewer_failed_manual_pass" | "failed_review_override" => {
                    let finding = (request.offer.reason == "review_needs_owner"
                        && matches!(
                            request.action,
                            api_types::TaskAction::Approve {
                                override_checks: false
                            }
                        ))
                    .then_some(annotation.as_ref())
                    .flatten();
                    self.recover_manual_review_pass(
                        task,
                        "owner approved review".to_owned(),
                        finding,
                        request.action.clone(),
                    )
                    .await
                }
                _ => Err(ServiceError::invalid_operation(
                    "accepted Task action has no apply plan",
                )),
            }
        };
        let updated = if role_launch {
            apply.await?
        } else {
            crate::task_service::actions::TASK_ACTION_COMMAND
                .scope((), apply)
                .await?
        };
        if role_launch {
            return Ok(updated);
        }
        if crate::deferred_dispatch::queued_recovery(&updated)
            .is_some_and(|intent| intent.id != queued.id)
        {
            return Ok(updated);
        }
        self.queue_deferred_action_role(
            updated,
            request.offer.clone(),
            request.action.clone(),
            request.actor.clone(),
        )
        .await
    }

    fn parse_blocking_annotation(&self, task: &Task) -> Option<api_types::TaskBlockingAnnotation> {
        let annotation = task.error_annotation.as_deref()?;
        match serde_json::from_str::<api_types::TaskAnnotation>(annotation) {
            Ok(api_types::TaskAnnotation::Blocking(annotation)) => Some(annotation),
            Ok(api_types::TaskAnnotation::Legacy(_)) => None,
            Err(_) => None,
        }
    }

    /// Clear the interruption projection using the exact Task snapshot that
    /// authorized the recovery attempt.  The version predicate is important:
    /// a caller that loaded an older annotation must never clear a newer one
    /// that won a concurrent update.
    pub(crate) async fn clear_recovery_metadata_at_version(&self, task: &Task) -> Result<Task> {
        let previous_reason = interruption_reason(task.blocked_json.as_deref());
        let updated = TaskRepo::update(
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
                blocked_json: Some(None),
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        if task.blocked_json.is_some() {
            self.publish(ForgeEvent {
                event_type: "task.unblocked".to_owned(),
                entity_id: updated.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskUnblocked {
                    project_id: updated.project_id.clone(),
                    previous_reason,
                },
            });
        }
        Ok(updated)
    }

    pub(crate) async fn clear_blocking_metadata(&self, task_id: &str) -> Result<Task> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let updated = self.clear_recovery_metadata_at_version(&task).await?;
        super::clear_execution_retry_metadata(&self.db, &updated).await?;
        Ok(updated)
    }

    pub(crate) async fn restore_recovery_metadata_after_failed_resume(
        &self,
        cleared_task: &Task,
        original_task: &Task,
        workspace_id: Option<&str>,
        execution_role: &str,
    ) {
        // Restore only if the clear is still the latest Task write and no
        // replacement execution was admitted in the clear-to-launch gap. The
        // DB boundary checks both facts under one write transaction; in
        // particular, an execution insert does not bump Task.version.
        let result = TaskRepo::update_recovery_metadata_if_no_running_execution(
            &*self.db,
            &cleared_task.id,
            cleared_task.version,
            original_task.error_annotation.clone(),
            original_task.blocked_json.clone(),
            original_task.failed_json.clone(),
            &now_rfc3339(),
            workspace_id,
            overlapping_execution_roles(execution_role),
            Vec::new(),
        )
        .await;
        if let Err(error) = result {
            tracing::warn!(
                task_id = %cleared_task.id,
                %error,
                "failed to restore recovery metadata after resume failure"
            );
        }
    }

    pub async fn fail_task(
        &self,
        task_id: impl Into<String>,
        reason: impl Into<String>,
        kind: Option<api_types::FailureKind>,
        execution_id: Option<String>,
    ) -> Result<Task> {
        let task_id = task_id.into();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let reason = reason.into();
        let failed_meta = json!({
            "reason": reason,
            "created_at": now_rfc3339(),
            "kind": kind,
            "execution_id": execution_id,
        });
        let updated = TaskRepo::update(
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
                blocked_json: Some(None),
                failed_json: Some(Some(failed_meta.to_string())),
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        self.publish(ForgeEvent {
            event_type: "task.failed".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskFailed {
                project_id: updated.project_id.clone(),
                reason: reason.clone(),
                kind,
                execution_id: execution_id.clone(),
            },
        });
        tracing::info!(
            task_id = %updated.id,
            status = %updated.status,
            reason = %reason,
            kind = ?kind,
            execution_id = ?execution_id,
            "task marked as failed"
        );
        Ok(updated)
    }

    /// Reuse the Task's live Review attempt, or open one for the current
    /// implementation candidate. A reviewer execution can only bind an
    /// attempt that is still `Running`, so a settled attempt has to be
    /// replaced before recovery can launch. An attempt awaiting human
    /// approval is deliberately left alone: opening a new one would discard
    /// the gate, so recovery says so instead of failing as a conflict.
    pub(super) async fn ensure_review_attempt_for_recovery(
        &self,
        task: &Task,
        project: &db::Project,
    ) -> Result<()> {
        let latest = ReviewRepo::list_by_task(&*self.db, &task.id)
            .await?
            .into_iter()
            .max_by_key(|review| (review.attempt_number, review.id.clone()));
        match latest.as_ref().map(|review| &review.status) {
            Some(ReviewStatus::Running) => return Ok(()),
            Some(ReviewStatus::AwaitingHuman) => {
                return Err(ServiceError::invalid_operation(
                    "the current review attempt is awaiting human approval; approve or reject it \
                     instead of re-executing the reviewer",
                ));
            }
            _ => {}
        }
        let candidate = super::super::latest_executor_execution_for_task(&self.db, task)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "review recovery requires a current implementation candidate",
                )
            })?;
        // A reviewer cannot start without the pre-review check results, and
        // nothing re-runs them here. The attempt being replaced ran them
        // against this same candidate, so carry those results across -- and
        // only those, never its verdict, which belongs to the attempt that
        // reached it.
        let carried_ci_steps = latest
            .as_ref()
            .filter(|review| review.execution_id == candidate.id)
            .and_then(|review| {
                serde_json::from_str::<serde_json::Value>(&review.step_results_json).ok()
            })
            .and_then(|details| details.get("ci_steps").cloned())
            .unwrap_or_else(|| serde_json::json!([]));
        let now = now_rfc3339();
        ReviewRepo::create_with_task_authority(
            &*self.db,
            db::CreateReview {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                execution_id: candidate.id.clone(),
                attempt_number: 0,
                status: ReviewStatus::Running,
                step_results_json: serde_json::json!({ "ci_steps": carried_ci_steps }).to_string(),
                started_at: now.clone(),
                created_at: now.clone(),
                updated_at: now,
            },
            task.version,
            &task.status,
            Some(project.version),
            Some(&project.workflow_definition),
            Some(&candidate.id),
        )
        .await
        .map_err(|error| match error {
            db::DbError::VersionConflict => ServiceError::conflict(format!(
                "could not open a review attempt for candidate {} at task {} version {}: the \
                 Task, Project workflow, or implementation candidate moved",
                candidate.id, task.id, task.version
            )),
            other => other.into(),
        })?;
        Ok(())
    }

    pub(crate) async fn reset_task_retry_budget(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        let reason = optional_recovery_reason(reason, "retry");
        let (gate_state, _budget, _count) = self.current_gate_retry_budget(&task).await?;
        let resume_after_reset = if task.status == gate_state {
            let failed_review = if gate_state == crate::workflow::default_states::REVIEW {
                ReviewRepo::list_latest_reviews_for_tasks(&*self.db, &[task.id.as_str()])
                    .await?
                    .first()
                    .is_some_and(|review| review.status == ReviewStatus::Failed)
            } else {
                false
            };
            if gate_state == crate::workflow::default_states::MERGING || failed_review {
                let mut plan = self.resume_process_plan(&task).await?;
                // The recovery marker below becomes the new counting boundary.
                // Validate the route before mutating anything, then evaluate it
                // as a fresh retry window after the reset commits.
                plan.count = 0;
                Some(plan)
            } else {
                None
            }
        } else {
            None
        };
        let mut transition_log = recovery_marker(
            &task.id,
            &gate_state,
            "retry",
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(api_types::TaskAction::retry()),
            )),
            &reason,
        );
        transition_log.hook_results_json = Some(
            serde_json::to_string(&vec![api_types::HookResultEntry {
                action: "retry".to_owned(),
                phase: "action".to_owned(),
                outcome: "reset_budget".to_owned(),
                duration_ms: None,
                error: None,
            }])
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        );
        let updated = self
            .clear_retry_exhausted_blocking_metadata_with_marker(&task, transition_log.clone())
            .await?;
        self.publish_recovery_applied(
            &updated,
            "retry",
            Some(&gate_state),
            Some(&transition_log.id),
        );
        // `resume_process` exists to move a Task out of the gate it is parked
        // in. When the gate has already rejected the Task into its reject
        // target there is nothing left to move: clearing the exhausted-budget
        // block is the whole recovery, and the version bump from that clear
        // invalidates any dispatch disposition so the next scan reconsiders it.
        if let Some(plan) = resume_after_reset {
            return self
                .continue_task_process_with_plan(updated, Some(reason), None, plan, false)
                .await;
        }
        Ok(updated)
    }

    pub(crate) async fn permit_one_task_retry(
        &self,
        task: Task,
        reason: Option<String>,
        context: Option<String>,
    ) -> Result<Task> {
        let reason = required_recovery_reason(reason, "retry")?;

        if task.entry_barrier_json.is_some() {
            let transition_log = recovery_marker(
                &task.id,
                &task.status,
                "retry",
                &TaskService::task_action_actor(api_types::Actor::user(
                    api_types::UserActionSource::Action(api_types::TaskAction::Retry {
                        fresh_session: None,
                        refresh_workspace: None,
                        reset_budget: Some(false),
                        guidance: None,
                    }),
                )),
                &reason,
            );
            let recovered = self
                .override_entry_checks_once(task, Some(reason), Some(transition_log.clone()))
                .await?;
            self.publish_recovery_applied(
                &recovered,
                "retry",
                Some(&recovered.status),
                Some(&transition_log.id),
            );
            return Ok(recovered);
        }

        let has_exhausted_annotation = self
            .parse_blocking_annotation(&task)
            .as_ref()
            .is_some_and(crate::task_diagnostics::is_retry_budget_exhausted);
        let (gate_state, budget, count) = self.current_gate_retry_budget(&task).await?;
        if gate_state != crate::workflow::default_states::REVIEW
            || (count < i64::from(budget) && !has_exhausted_annotation)
        {
            return Err(ServiceError::invalid_operation(format!(
                "retry without resetting the budget is not supported for the current exception in state {}",
                task.status
            )));
        }
        let transition_reason = match &context {
            Some(guidance) => format!("{reason}\n\nGuidance: {guidance}"),
            None => reason.clone(),
        };
        let mut transition_log = recovery_marker(
            &task.id,
            &gate_state,
            "retry",
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(api_types::TaskAction::Retry {
                    fresh_session: None,
                    refresh_workspace: None,
                    reset_budget: Some(false),
                    guidance: None,
                }),
            )),
            &transition_reason,
        );
        // The gate may already have rejected the Task into its active target
        // by the time a user chooses this recovery. In that case there is no
        // state transition left to perform: preserve the exhausted retry
        // count, clear only its blocker, and wake the current active attempt.
        // If that attempt fails review, the unchanged count blocks it again,
        // which is the promised one-shot behavior.
        if task.status != gate_state {
            let updated = self
                .clear_retry_exhausted_blocking_metadata_with_marker(&task, transition_log.clone())
                .await?;
            crate::wake_task_dispatch(
                &self.db,
                &updated.id,
                "one retry cleared an exhausted gate blocker",
            )
            .await?;
            let recovered = TaskRepo::get_by_id(&*self.db, &updated.id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", updated.id.clone()))?;
            self.publish_recovery_applied(
                &recovered,
                "retry",
                Some(&gate_state),
                Some(&transition_log.id),
            );
            return Ok(recovered);
        }

        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(api_types::TaskAction::Retry {
                    fresh_session: None,
                    refresh_workspace: None,
                    reset_budget: Some(false),
                    guidance: None,
                }),
            )),
        );
        let target = workflow
            .states
            .iter()
            .find(|state| state.name == gate_state)
            .and_then(|state| state.gate_config.as_ref())
            .and_then(|gate_config| gate_config.reject_target.clone())
            .unwrap_or_else(|| crate::workflow::default_states::IN_PROGRESS.to_owned());
        let recovered = self
            .transition(
                task.id.clone(),
                target,
                TransitionOptions {
                    version: task.version,
                    reason: Some(transition_reason),
                    triggered_by: TaskService::task_action_actor(api_types::Actor::user(
                        api_types::UserActionSource::Action(api_types::TaskAction::Approve {
                            override_checks: true,
                        }),
                    )),
                    rejection: true,
                    defer_dispatch_seconds: None,
                },
            )
            .await?
            .task;
        // The Task CAS above is the authority boundary. Persist the marker
        // only after it wins, so a stale ProceedOnce request cannot create a
        // retry-window boundary for a transition that never committed.
        transition_log.created_at = db::now_rfc3339();
        TransitionLogRepo::insert(&*self.db, transition_log.clone()).await?;
        self.publish_recovery_applied(
            &recovered,
            "retry",
            Some(&gate_state),
            Some(&transition_log.id),
        );
        Ok(recovered)
    }

    pub(crate) async fn continue_task_process(
        &self,
        task: Task,
        reason: Option<String>,
        context: Option<String>,
    ) -> Result<Task> {
        let plan = self.resume_process_plan(&task).await?;
        self.continue_task_process_with_plan(task, reason, context, plan, true)
            .await
    }

    async fn continue_task_process_with_plan(
        &self,
        task: Task,
        reason: Option<String>,
        context: Option<String>,
        plan: ResumeProcessPlan,
        require_interruption: bool,
    ) -> Result<Task> {
        if plan.count >= i64::from(plan.budget) {
            return Err(ServiceError::invalid_operation(format!(
                "resume_process is not supported because retry budget is exhausted for state {}",
                plan.gate_state
            )));
        }

        let has_interruption = task.error_annotation.is_some() || task.blocked_json.is_some();
        let has_failed_review = if task.status == crate::workflow::default_states::REVIEW {
            matches!(
                self.latest_review_for_task(&task.id).await,
                Ok(review) if review.status == ReviewStatus::Failed
            )
        } else {
            false
        };
        if require_interruption && !has_interruption && !has_failed_review {
            return Err(ServiceError::invalid_operation(
                "resume_process requires a recoverable gate exception",
            ));
        }

        let reason = optional_recovery_reason(reason, "retry");
        let transition_reason = match &context {
            Some(guidance) => format!("{reason}\n\nGuidance: {guidance}"),
            None => reason.clone(),
        };
        let mut transition_log = recovery_marker(
            &task.id,
            &plan.gate_state,
            "retry",
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(api_types::TaskAction::retry()),
            )),
            &transition_reason,
        );
        let recovered = self
            .transition_recovery_rejection(
                &task,
                plan.target_state,
                transition_reason,
                &TaskService::task_action_actor(api_types::Actor::user(
                    api_types::UserActionSource::Action(api_types::TaskAction::retry()),
                )),
            )
            .await?;
        // The workflow transition owns the Task version CAS. Persist this
        // retry-window boundary only after that CAS commits; a stale recovery
        // request must not reset the window for a transition it never won.
        // If this post-CAS insert fails, the conservative outcome is a
        // committed transition with no new boundary (the retry count is not
        // silently reset); surface the database error to the caller.
        transition_log.created_at = db::now_rfc3339();
        TransitionLogRepo::insert(&*self.db, transition_log.clone()).await?;
        self.publish_recovery_applied(
            &recovered,
            "retry",
            Some(&plan.gate_state),
            Some(&transition_log.id),
        );
        Ok(recovered)
    }

    async fn transition_recovery_rejection(
        &self,
        task: &Task,
        target_state: String,
        reason: String,
        actor: &api_types::Actor,
    ) -> Result<Task> {
        super::ensure_plan_publication_transition_authority(task, None)?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(task, &project.workflow_definition, actor);
        let uses_system_only_trigger = workflow
            .trigger_between(&task.status, &target_state)
            .is_some_and(|trigger| trigger.system_only());

        if uses_system_only_trigger && !actor.is_system() {
            let engine = WorkflowEngine {
                db: Arc::clone(&self.db),
                event_bus: Arc::clone(&self.event_bus),
                review_runner: self.review_runner.clone(),
                merge_service: self.merge_service.clone(),
                cleanup_scheduler: self.cleanup_scheduler.clone(),
                task_service: self.clone(),
                daemon_connections: self.daemon_connections.clone(),
                workspace_exec_locks: self.workspace_exec_locks.clone(),
                terminal_activity: self.terminal_activity.clone(),
                workspace_root: self.workspace_root.clone(),
                repo_cache_locks: self.repo_cache_locks.clone(),
                workspace_backend_router: Arc::clone(&self.workspace_backend_router),
            };
            let recovered = engine
                .manual_override_transition_with_authority(
                    &task.id,
                    &target_state,
                    task.version,
                    &workflow,
                    actor.clone(),
                    &reason,
                    true,
                    Some(crate::workflow::engine::WorkflowAuthority {
                        project_version: project.version,
                        workflow_definition: project.workflow_definition.clone(),
                        clear_review_passed_at_on_commit: false,
                    }),
                )
                .await?
                .task;
            self.reconcile_terminal_subtask(&recovered).await;
            return Ok(recovered);
        }

        Ok(self
            .transition(
                task.id.clone(),
                target_state,
                TransitionOptions {
                    version: task.version,
                    reason: Some(reason),
                    triggered_by: actor.clone(),
                    rejection: true,
                    defer_dispatch_seconds: None,
                },
            )
            .await?
            .task)
    }

    async fn resume_process_plan(&self, task: &Task) -> Result<ResumeProcessPlan> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(api_types::TaskAction::retry()),
            )),
        );
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
        if state.kind != api_types::StateKind::Gate {
            return Err(ServiceError::invalid_operation(format!(
                "resume_process is only supported in gate states, not {}",
                task.status
            )));
        }
        let target_state = state
            .gate_config
            .as_ref()
            .and_then(|gate_config| gate_config.reject_target.clone())
            .unwrap_or_else(|| crate::workflow::default_states::IN_PROGRESS.to_owned());
        let (gate_state, budget, count) = self.current_gate_retry_budget(task).await?;
        Ok(ResumeProcessPlan {
            gate_state,
            target_state,
            budget,
            count,
        })
    }

    pub(crate) async fn current_gate_retry_budget(
        &self,
        task: &Task,
    ) -> Result<(String, i32, i64)> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Api,
            )),
        );
        let current = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .ok_or_else(|| {
                ServiceError::invalid_operation(WorkflowEngine::undefined_state_message(
                    &task.status,
                    &workflow,
                ))
            })?;
        // A Task sitting *in* a gate is the easy case. A Task the gate has
        // already rejected is the common one: `review` rejects into
        // `in_progress` and `merging` into `merge_failed`, so by the time the
        // retry-budget blocker is visible the Task has usually left the gate.
        // A retry may reset the budget or permit one attempt from the reject
        // target; resolve the originating gate for that accepted apply plan
        // through its `reject_target` as well.
        let state = if current.kind == api_types::StateKind::Gate {
            current
        } else {
            let mut origins = workflow.states.iter().filter(|state| {
                state.kind == api_types::StateKind::Gate
                    && state
                        .gate_config
                        .as_ref()
                        .and_then(|gate_config| gate_config.reject_target.as_deref())
                        == Some(task.status.as_str())
            });
            let Some(origin) = origins.next() else {
                return Err(ServiceError::conflict(format!(
                    "state {} is not a retry-budget gate and no gate rejects into it",
                    task.status
                )));
            };
            if let Some(also) = origins.next() {
                return Err(ServiceError::conflict(format!(
                    "state {} is the reject target of more than one gate ({} and {}); recover from the gate itself",
                    task.status, origin.name, also.name
                )));
            }
            origin
        };

        let budget = if state.name == crate::workflow::default_states::REVIEW {
            crate::task_service::config::runtime_retry_budget(
                task,
                crate::task_service::config::RetryBudgetKind::Review,
                Some(&state.config),
                state.gate_config.as_ref(),
            )?
        } else {
            state
                .gate_config
                .as_ref()
                .and_then(|gate_config| gate_config.max_rejections)
                .unwrap_or(i32::MAX)
        };
        let entries = TransitionLogRepo::list_by_task(&*self.db, &task.id).await?;
        let count =
            crate::task_diagnostics::count_gate_rejections_since_boundary(&entries, &state.name);
        Ok((state.name.clone(), budget, count))
    }

    pub(crate) async fn clear_retry_exhausted_blocking_metadata_with_marker(
        &self,
        task: &Task,
        marker: db::CreateTransitionLog,
    ) -> Result<Task> {
        let clear_error = task
            .error_annotation
            .as_deref()
            .is_some_and(is_retry_exhausted_annotation);
        let clear_blocked = task
            .blocked_json
            .as_deref()
            .is_some_and(is_retry_exhausted_blocked_metadata);
        TaskRepo::update_with_recovery_marker(
            &*self.db,
            UpdateTask {
                id: task.id.clone(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: clear_error.then_some(None),
                blocked_json: clear_blocked.then_some(None),
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
            marker,
        )
        .await
        .map_err(Into::into)
    }

    fn publish_recovery_applied(
        &self,
        task: &Task,
        action: &str,
        state: Option<&str>,
        transition_log_id: Option<&str>,
    ) {
        self.publish(ForgeEvent {
            event_type: "task.recovery_applied".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::RecoveryApplied {
                project_id: task.project_id.clone(),
                task_id: task.id.clone(),
                action: action.to_owned(),
                state: state.map(str::to_owned),
                transition_log_id: transition_log_id.map(str::to_owned),
            },
        });
    }

    pub(crate) async fn restart_task_condition(
        &self,
        task: Task,
        annotation: &api_types::TaskBlockingAnnotation,
        reason: Option<String>,
    ) -> Result<Task> {
        let reason = optional_recovery_reason(reason, "restart");
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(api_types::TaskAction::Restart),
            )),
        );
        let initial_state = workflow_initial_state(&workflow)?;
        self.reset_daemon_owner_workspace(&task).await?;
        let assignee_id = should_clear_assignments_for_reset(annotation).then_some(None);
        let retry_gate_state = match self.current_gate_retry_budget(&task).await {
            Ok((gate_state, _, _)) => Some(gate_state),
            // A reset from a normal (non-gate) state does not need a retry
            // boundary.  Do not turn unrelated workflow/project/database
            // failures into that case: without this narrow match a status
            // reset could commit while silently losing its marker.
            Err(ServiceError::Conflict(message))
                if message.contains(" is not a retry-budget gate and no gate rejects into it") =>
            {
                None
            }
            Err(error) => return Err(error),
        };
        let actor = TaskService::task_action_actor(api_types::Actor::user(
            api_types::UserActionSource::Action(api_types::TaskAction::Restart),
        ));
        let engine = WorkflowEngine {
            db: Arc::clone(&self.db),
            event_bus: Arc::clone(&self.event_bus),
            review_runner: self.review_runner.clone(),
            merge_service: self.merge_service.clone(),
            cleanup_scheduler: self.cleanup_scheduler.clone(),
            task_service: self.clone(),
            daemon_connections: self.daemon_connections.clone(),
            workspace_exec_locks: self.workspace_exec_locks.clone(),
            terminal_activity: self.terminal_activity.clone(),
            workspace_root: self.workspace_root.clone(),
            repo_cache_locks: self.repo_cache_locks.clone(),
            workspace_backend_router: Arc::clone(&self.workspace_backend_router),
        };
        let mut recovered = engine
            .restart_with_authority(
                &task.id,
                &initial_state,
                task.version,
                &workflow,
                &actor,
                &reason,
                crate::workflow::engine::WorkflowAuthority {
                    project_version: project.version,
                    workflow_definition: project.workflow_definition.clone(),
                    clear_review_passed_at_on_commit: true,
                },
            )
            .await?;
        if assignee_id.is_some() {
            // Assignment cleanup follows the engine's state transition and has its own CAS.
            recovered = TaskRepo::update_status(
                &*self.db,
                UpdateTaskStatus {
                    id: recovered.id.clone(),
                    expected_version: recovered.version,
                    status: recovered.status.clone(),
                    assignee_id,
                    error_annotation: Some(None),
                    blocked_json: Some(None),
                    failed_json: Some(None),
                    updated_at: now_rfc3339(),
                },
            )
            .await?;
        }
        let retry_boundary_id = if let Some(gate_state) = retry_gate_state.as_deref() {
            let marker = recovery_marker(&task.id, gate_state, "restart", &actor, &reason);
            let id = marker.id.clone();
            TransitionLogRepo::insert(&*self.db, marker).await?;
            Some(id)
        } else {
            None
        };
        crate::placement::admission::resolve_workspace_attention(&self.db, &recovered.id).await?;
        if let Some(boundary_id) = retry_boundary_id.as_deref() {
            self.publish_recovery_applied(
                &recovered,
                "restart",
                retry_gate_state.as_deref(),
                Some(boundary_id),
            );
        }

        super::clear_execution_retry_metadata(&self.db, &recovered).await?;
        if task.blocked_json.is_some() {
            self.publish(ForgeEvent {
                event_type: "task.unblocked".to_owned(),
                entity_id: recovered.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskUnblocked {
                    project_id: recovered.project_id.clone(),
                    previous_reason: interruption_reason(task.blocked_json.as_deref()),
                },
            });
        }
        if task.failed_json.is_some() {
            self.publish(ForgeEvent {
                event_type: "task.restarted".to_owned(),
                entity_id: recovered.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskRestarted {
                    project_id: recovered.project_id.clone(),
                    previous_reason: interruption_reason(task.failed_json.as_deref()),
                    new_execution_id: None,
                },
            });
        }
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason,
            },
        });
        Ok(recovered)
    }

    pub(crate) async fn reset_daemon_owner_workspace(&self, task: &Task) -> Result<()> {
        let Some(placement) = db::WorkspacePlacementRepo::get_for_task(&*self.db, &task.id).await?
        else {
            return Ok(());
        };
        if placement.owner_kind != db::PlacementOwnerKind::Daemon {
            return Ok(());
        }
        if placement.task_id != task.id {
            return Err(ServiceError::invalid_operation(
                "reset the root's shared workspace instead",
            ));
        }
        let workspace = WorkspaceRepo::get_by_id(&*self.db, &placement.workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", &placement.workspace_id))?;
        let resolved = self
            .workspace_backend_router
            .resolve(&self.db, &workspace)
            .await?;
        super::super::workspace::reset_daemon_workspace(&self.db, &workspace, &resolved).await?;
        Ok(())
    }

    pub(crate) async fn recover_manual_review_pass(
        &self,
        task: Task,
        owner_reason: String,
        finding: Option<&api_types::TaskBlockingAnnotation>,
        action: api_types::TaskAction,
    ) -> Result<Task> {
        if task.status != crate::workflow::default_states::REVIEW {
            return Err(ServiceError::invalid_operation(format!(
                "{action} is only supported from review state, got {}",
                task.status
            )));
        }
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(action.clone()),
            )),
        );
        let pass_target = workflow
            .auto_transition_target(&task.status)
            .unwrap_or(crate::workflow::default_states::MERGING)
            .to_owned();
        let latest_review = self.latest_review_for_task(&task.id).await?;
        if latest_review.status != ReviewStatus::Failed {
            return Err(ServiceError::invalid_operation(format!(
                "{action} requires the latest review attempt to be failed"
            )));
        }
        let active_review_execution = ExecutionRepo::list_running_by_task(&*self.db, &task.id)
            .await?
            .into_iter()
            .find(|execution| {
                matches!(
                    execution.role.as_str(),
                    crate::workflow::default_roles::REVIEWER
                        | crate::workflow::default_roles::AUDITOR
                )
            });
        if let Some(execution) = active_review_execution {
            return Err(ServiceError::invalid_operation(format!(
                "cannot pass review manually while {} execution {} is running",
                execution.role, execution.id
            )));
        }
        let finished_at = now_rfc3339();
        let follow_up = finding
            .map(|finding| {
                let backlog = workflow
                    .states
                    .iter()
                    .find(|state| state.kind == api_types::StateKind::Backlog)
                    .ok_or_else(|| {
                        ServiceError::invalid_operation("workflow has no backlog state")
                    })?;
                let message = finding
                    .message
                    .as_deref()
                    .unwrap_or(&finding.blocking_reason);
                let finding_reason = message
                    .strip_prefix("fixable by owner: ")
                    .or_else(|| message.strip_prefix("repeated finding: "))
                    .unwrap_or(message);
                let first_line = finding_reason.lines().next().unwrap_or_default();
                let summary: String = first_line.chars().take(80).collect();
                let summary = if first_line.chars().count() > 80 {
                    format!("{summary}…")
                } else {
                    summary
                };
                Ok::<_, ServiceError>(db::CreateTask {
                    id: new_uuid_v4(),
                    project_id: task.project_id.clone(),
                    parent_task_id: None,
                    assignee_type: None,
                    assignee_id: None,
                    title: format!("Follow-up: {} — {summary}", task.title),
                    description: Some(format!(
                        "Follow-up of Task {}.\n\nParked finding ({}):\n{}\n\nOwner reason:\n{}",
                        task.id, finding.blocking_reason, message, owner_reason
                    )),
                    task_type: "task".to_owned(),
                    status: backlog.name.clone(),
                    is_automation: false,
                    priority: task.priority,
                    subtask_order: None,
                    task_state_config: None,
                    merge_config: None,
                    plan: None,
                    created_at: finished_at.clone(),
                    updated_at: finished_at.clone(),
                })
            })
            .transpose()?;
        let follow_up_governance = if let Some(follow_up) = &follow_up {
            self.prepare_task_governance(&project, &follow_up.task_type, None)
                .await?
        } else {
            None
        };
        let reason = match &follow_up {
            Some(follow_up) => format!(
                "deferred to follow-up {} ({}): {owner_reason}",
                follow_up.id, follow_up.title
            ),
            None => owner_reason,
        };
        let mut details = strict_review_details(&latest_review)?;
        details["manual_override"] = json!({
            "action": action.to_string(),
            "reason": reason.clone(),
            "actor_type": "user",
            "source_review_id": latest_review.id,
            "source_attempt_number": latest_review.attempt_number,
            "at": finished_at,
        });
        let manual_pass = db::CreateManualReviewPass {
            id: new_uuid_v4(),
            source_review_id: latest_review.id.clone(),
            source_review_updated_at: latest_review.updated_at.clone(),
            task_id: task.id.clone(),
            candidate_execution_id: latest_review.execution_id.clone(),
            step_results_json: details.to_string(),
            expected_task_version: task.version,
            expected_task_status: task.status.clone(),
            expected_project_version: project.version,
            expected_workflow_definition: project.workflow_definition.clone(),
            occurred_at: finished_at.clone(),
        };
        let (review, task) = if let Some(follow_up) = follow_up {
            let mut transaction = db::begin_immediate(self.db.pool()).await?;
            let follow_up = self
                .insert_created_task_in_tx(&mut transaction, follow_up, follow_up_governance)
                .await?;
            let linked = sqlx::query(
                "UPDATE task SET metadata_json = ?, version = version + 1
                 WHERE id = ? AND version = ?",
            )
            .bind(json!({ "follow_up_of": task.id }).to_string())
            .bind(&follow_up.id)
            .bind(follow_up.version)
            .execute(&mut *transaction)
            .await?;
            if linked.rows_affected() != 1 {
                return Err(db::DbError::VersionConflict.into());
            }
            let (review, task) = ReviewRepo::create_manual_pass_with_task_authority_in_tx(
                &*self.db,
                &mut transaction,
                manual_pass,
            )
            .await?;
            let cleared = sqlx::query(
                "UPDATE task SET error_annotation = NULL, blocked_json = NULL,
                 updated_at = ?, version = version + 1 WHERE id = ? AND version = ?",
            )
            .bind(&finished_at)
            .bind(&task.id)
            .bind(task.version)
            .execute(&mut *transaction)
            .await?;
            if cleared.rows_affected() != 1 {
                return Err(db::DbError::VersionConflict.into());
            }
            let task = TaskRepo::get_by_id_in_tx(&*self.db, &mut transaction, &task.id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
            transaction.commit().await?;
            self.publish(ForgeEvent {
                event_type: "task.created".to_owned(),
                entity_id: follow_up.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskCreated {
                    project_id: follow_up.project_id,
                    title: follow_up.title,
                },
            });
            self.publish(ForgeEvent {
                event_type: "task.updated".to_owned(),
                entity_id: task.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskUpdated {
                    project_id: task.project_id.clone(),
                },
            });
            (review, task)
        } else {
            ReviewRepo::create_manual_pass_with_task_authority(&*self.db, manual_pass).await?
        };

        if let Err(error) = self
            .memory_service
            .record_review_result_if_final(&task.project_id, &review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }
        self.create_system_comment(
            &task.id,
            format!(
                "Review passed manually (attempt {}): {}",
                review.attempt_number, reason
            ),
        )
        .await?;
        self.publish(ForgeEvent {
            event_type: "review.approved".to_owned(),
            entity_id: review.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReviewApproved {
                task_id: task.id.clone(),
                review_id: review.id.clone(),
            },
        });
        tracing::info!(
            task_id = %task.id,
            reason = %reason,
            action = %action,
            "manual review recovery action logged"
        );
        let transitioned = self
            .transition(
                task.id.clone(),
                pass_target,
                TransitionOptions {
                    version: task.version,
                    reason: Some(reason),
                    triggered_by: TaskService::task_action_actor(api_types::Actor::user(
                        api_types::UserActionSource::Action(action),
                    )),
                    rejection: false,
                    defer_dispatch_seconds: None,
                },
            )
            .await?;
        Ok(transitioned.task)
    }

    pub(crate) async fn retry_entry_checks(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let actor = if self
            .parse_blocking_annotation(&task)
            .is_some_and(|annotation| annotation.blocking_reason == "review_ci_infrastructure")
        {
            api_types::Actor::system(api_types::SystemComponent::TaskDispatcher)
        } else {
            TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(api_types::TaskAction::retry()),
            ))
        };
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);
        let engine = WorkflowEngine {
            db: Arc::clone(&self.db),
            event_bus: Arc::clone(&self.event_bus),
            review_runner: self.review_runner.clone(),
            merge_service: self.merge_service.clone(),
            cleanup_scheduler: self.cleanup_scheduler.clone(),
            task_service: self.clone(),
            daemon_connections: self.daemon_connections.clone(),
            workspace_exec_locks: self.workspace_exec_locks.clone(),
            terminal_activity: self.terminal_activity.clone(),
            workspace_root: self.workspace_root.clone(),
            repo_cache_locks: self.repo_cache_locks.clone(),
            workspace_backend_router: Arc::clone(&self.workspace_backend_router),
        };
        let recovered = engine
            .retry_entry_barrier_with_authority(
                &task.id,
                task.version,
                &workflow,
                &actor,
                reason.as_deref().unwrap_or("retry"),
                crate::workflow::engine::WorkflowAuthority {
                    project_version: project.version,
                    workflow_definition: project.workflow_definition.clone(),
                    clear_review_passed_at_on_commit: false,
                },
            )
            .await?
            .task;
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason: reason.unwrap_or_else(|| "retry".to_owned()),
            },
        });
        if recovered
            .entry_barrier_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .is_none_or(|barrier| barrier["status"] != "blocked")
        {
            crate::placement::admission::resolve_review_ci_attention(&self.db, &recovered.id)
                .await?;
        }
        Ok(recovered)
    }

    pub(crate) async fn recover_retry_current_state_hooks(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        let reason = optional_recovery_reason(reason, "retry");
        super::ensure_plan_publication_transition_authority(&task, None)?;
        let cleared = self.clear_blocking_metadata(&task.id).await?;
        let project = ProjectRepo::get_by_id(&*self.db, &cleared.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", cleared.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &cleared,
            &project.workflow_definition,
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(api_types::TaskAction::retry()),
            )),
        );
        let engine = WorkflowEngine {
            db: Arc::clone(&self.db),
            event_bus: Arc::clone(&self.event_bus),
            review_runner: self.review_runner.clone(),
            merge_service: self.merge_service.clone(),
            cleanup_scheduler: self.cleanup_scheduler.clone(),
            task_service: self.clone(),
            daemon_connections: self.daemon_connections.clone(),
            workspace_exec_locks: self.workspace_exec_locks.clone(),
            terminal_activity: self.terminal_activity.clone(),
            workspace_root: self.workspace_root.clone(),
            repo_cache_locks: self.repo_cache_locks.clone(),
            workspace_backend_router: Arc::clone(&self.workspace_backend_router),
        };
        let recovered = engine
            .manual_override_transition_with_authority(
                &cleared.id,
                &cleared.status,
                cleared.version,
                &workflow,
                TaskService::task_action_actor(api_types::Actor::user(
                    api_types::UserActionSource::Action(api_types::TaskAction::retry()),
                )),
                &reason,
                false,
                Some(crate::workflow::engine::WorkflowAuthority {
                    project_version: project.version,
                    workflow_definition: project.workflow_definition.clone(),
                    clear_review_passed_at_on_commit: false,
                }),
            )
            .await?
            .task;
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason,
            },
        });
        Ok(recovered)
    }

    pub(crate) async fn recover_pull_request_merge_wait(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        let cleared = TaskRepo::mutate_metadata_and_bump_version(
            &*self.db,
            &task.id,
            task.version,
            vec![TaskMetadataMutation::CompareAndMutate {
                key: "awaiting_human_reason".to_owned(),
                expected: Value::String("pull_request_merge".to_owned()),
                mutations: vec![
                    TaskMetadataMutation::Remove {
                        key: "awaiting_human".to_owned(),
                    },
                    TaskMetadataMutation::Remove {
                        key: "awaiting_human_reason".to_owned(),
                    },
                    TaskMetadataMutation::Remove {
                        key: "awaiting_human_marker_id".to_owned(),
                    },
                ],
            }],
            &now_rfc3339(),
        )
        .await?;
        self.recover_retry_current_state_hooks(cleared, reason)
            .await
    }

    pub(crate) async fn recover_manual_merge_repair_for_review(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        let recovery_reason = optional_recovery_reason(reason, "manual merge repair completed");
        // Validate the complete route before mutating the Task. The transition
        // clears blocked_json atomically with the status change, and the normal
        // transition cleanup clears the transient annotation and stale review
        // authority. A custom/invalid workflow therefore keeps the original
        // manual-repair blocker instead of losing its only recovery evidence.
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let actor = api_types::Actor::system(api_types::SystemComponent::Workflow);
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);
        let reject_target = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(|state| state.gate_config.as_ref())
            .and_then(|gate| gate.reject_target.clone())
            .ok_or_else(|| {
                ServiceError::invalid_operation(format!(
                    "manual merge repair cannot find a reject route from {}",
                    task.status
                ))
            })?;
        let recovered = self
            .transition_recovery_rejection(
                &task,
                reject_target,
                format!(
                    "{} {}; fresh review required",
                    crate::workflow::REVIEW_REFRESH_MARKER,
                    recovery_reason
                ),
                &actor,
            )
            .await?;
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason: recovery_reason,
            },
        });
        Ok(recovered)
    }

    pub(crate) async fn retry_review_entry(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        self.recover_retry_current_state_hooks(task, reason).await
    }

    pub(crate) async fn refresh_workspace_and_retry_entry(
        &self,
        task: Task,
        reason: Option<String>,
    ) -> Result<Task> {
        let workspace = super::super::workspace::prepare_workspace(
            &self.db,
            &self.workspace_root,
            &task,
            &task.id,
            self.repo_cache_locks.clone(),
            &self.workspace_backend_router,
        )
        .await?;
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
        let target_branch = default_target_branch(&repo.default_branch);
        let resolved = self
            .workspace_backend_router
            .resolve(&self.db, &workspace)
            .await?;
        let status = resolved
            .git_query(api_types::WorkspaceGitQuery::StatusPorcelain, false)
            .await?
            .ok_or_else(|| ServiceError::invalid_operation("owner returned no workspace status"))?;
        if !status.trim().is_empty() {
            let files = status.lines().collect::<Vec<_>>().join(", ");
            return Err(ServiceError::invalid_operation(format!(
                "cannot update workspace before retrying hook because the worktree is dirty: {files}"
            )));
        }

        match resolved.rebase_target(&target_branch, false).await? {
            api_types::WorkspaceOwnerOperationOutcome::Rebased => {
                resolved.record_head_best_effort(&self.db).await;
                tracing::info!(
                    task_id = %task.id,
                    workspace_id = %workspace.id,
                    target_branch = %target_branch,
                    "workspace updated before retrying hook"
                );
            }
            api_types::WorkspaceOwnerOperationOutcome::Conflict { details, .. }
            | api_types::WorkspaceOwnerOperationOutcome::UnsupportedConflict { details } => {
                return Err(ServiceError::invalid_operation(format!(
                    "cannot update workspace before retrying hook because rebase onto {target_branch} conflicted: {details}"
                )));
            }
            api_types::WorkspaceOwnerOperationOutcome::Dirty { files } => {
                return Err(ServiceError::invalid_operation(format!(
                    "cannot update workspace before retrying hook because the worktree is dirty: {}",
                    files.join(", ")
                )));
            }
            _ => {
                return Err(ServiceError::invalid_operation(
                    "owner returned an invalid rebase result",
                ))
            }
        }

        if task.entry_barrier_json.is_some() {
            self.retry_entry_checks(task, reason).await
        } else {
            self.recover_retry_current_state_hooks(task, reason).await
        }
    }

    pub(crate) async fn override_entry_checks_once(
        &self,
        task: Task,
        reason: Option<String>,
        recovery_marker: Option<db::CreateTransitionLog>,
    ) -> Result<Task> {
        tracing::info!(
            task_id = %task.id,
            reason = %reason.clone().unwrap_or_else(|| "approve".to_owned()),
            "entry-check override recorded"
        );
        let recovered = if task.entry_barrier_json.is_some() {
            let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            let workflow = WorkflowEngine::resolve_workflow_for_task(
                &task,
                &project.workflow_definition,
                &TaskService::task_action_actor(api_types::Actor::user(
                    api_types::UserActionSource::Action(api_types::TaskAction::Approve {
                        override_checks: true,
                    }),
                )),
            );
            let barrier_state = task
                .entry_barrier_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
                .and_then(|barrier| {
                    barrier
                        .get("state")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| task.status.clone());
            let skip_config = serde_json::json!({
                barrier_state.clone(): {
                    "skip_before_work_hook_once": true
                }
            });
            let update = UpdateTask {
                id: task.id.clone(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: None,
                blocked_json: None,
                failed_json: None,
                task_state_config: Some(Some(skip_config.to_string())),
                parent_task_id: None,
                updated_at: now_rfc3339(),
            };
            let updated = if let Some(marker) = recovery_marker.as_ref() {
                TaskRepo::update_with_workflow_authority_and_recovery_marker(
                    &*self.db,
                    update,
                    marker.clone(),
                    project.version,
                    project.workflow_definition.clone(),
                )
                .await?
            } else {
                TaskRepo::update_with_workflow_authority(
                    &*self.db,
                    update,
                    project.version,
                    project.workflow_definition.clone(),
                )
                .await?
            };
            let engine = WorkflowEngine {
                db: Arc::clone(&self.db),
                event_bus: Arc::clone(&self.event_bus),
                review_runner: self.review_runner.clone(),
                merge_service: self.merge_service.clone(),
                cleanup_scheduler: self.cleanup_scheduler.clone(),
                task_service: self.clone(),
                daemon_connections: self.daemon_connections.clone(),
                workspace_exec_locks: self.workspace_exec_locks.clone(),
                terminal_activity: self.terminal_activity.clone(),
                workspace_root: self.workspace_root.clone(),
                repo_cache_locks: self.repo_cache_locks.clone(),
                workspace_backend_router: Arc::clone(&self.workspace_backend_router),
            };
            let recovered = engine
                .retry_entry_barrier_with_authority(
                    &updated.id,
                    updated.version,
                    &workflow,
                    &TaskService::task_action_actor(api_types::Actor::user(
                        api_types::UserActionSource::Action(api_types::TaskAction::Approve {
                            override_checks: true,
                        }),
                    )),
                    reason.as_deref().unwrap_or("approve"),
                    crate::workflow::engine::WorkflowAuthority {
                        project_version: project.version,
                        workflow_definition: project.workflow_definition.clone(),
                        clear_review_passed_at_on_commit: false,
                    },
                )
                .await?
                .task;
            clear_skip_before_work_hook_once_override(&self.db, &recovered, &barrier_state).await?
        } else {
            let mut config = task
                .task_state_config
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
                .unwrap_or_else(|| json!({}));
            config[&task.status]["skip_before_work_hook_once"] = json!(true);
            let updated = TaskRepo::update(
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
                    blocked_json: Some(None),
                    failed_json: None,
                    task_state_config: Some(Some(config.to_string())),
                    parent_task_id: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await?;
            let recovered = self
                .recover_retry_current_state_hooks(updated, reason.clone())
                .await?;
            clear_skip_before_work_hook_once_override(&self.db, &recovered, &task.status).await?
        };
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: recovered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: recovered.project_id.clone(),
                reason: reason.unwrap_or_else(|| "approve".to_owned()),
            },
        });
        Ok(recovered)
    }
}

async fn clear_skip_before_work_hook_once_override(
    db: &Arc<SqliteDb>,
    task: &Task,
    state_name: &str,
) -> Result<Task> {
    let Some(raw) = task.task_state_config.as_deref() else {
        return Ok(task.clone());
    };
    let Ok(mut parsed) = serde_json::from_str::<serde_json::Value>(raw) else {
        return Ok(task.clone());
    };
    let Some(root) = parsed.as_object_mut() else {
        return Ok(task.clone());
    };
    let Some(state_value) = root.get_mut(state_name) else {
        return Ok(task.clone());
    };
    let Some(state_object) = state_value.as_object_mut() else {
        return Ok(task.clone());
    };
    if state_object.remove("skip_before_work_hook_once").is_none() {
        return Ok(task.clone());
    }
    if state_object.is_empty() {
        root.remove(state_name);
    }
    let next_config = if root.is_empty() {
        None
    } else {
        Some(parsed.to_string())
    };
    let updated = TaskRepo::update(
        &**db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: Some(next_config),
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await?;
    Ok(updated)
}

fn should_clear_assignments_for_reset(annotation: &api_types::TaskBlockingAnnotation) -> bool {
    // recovery_required covers crash-recovery and agent-timeout interruptions;
    // workspace failures arrive either as a live annotation
    // (workspace_reset_required / workspace_error) or, once fail_task has
    // cleared the annotation, synthesized from failed_json (workspace_failed).
    annotation.annotation_type == api_types::FailureKind::RecoveryRequired
        || annotation.annotation_type.is_workspace_failure()
}

fn workflow_initial_state(workflow: &api_types::WorkflowDefinition) -> Result<String> {
    workflow
        .states
        .iter()
        .find(|state| state.kind == api_types::StateKind::Initial)
        .map(|state| state.name.clone())
        .ok_or_else(|| ServiceError::invalid_operation("workflow has no initial state"))
}

fn default_target_branch(repo_default_branch: &str) -> String {
    let trimmed = repo_default_branch.trim();
    if trimmed.is_empty() {
        "main".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn interruption_reason(raw_metadata: Option<&str>) -> Option<String> {
    raw_metadata
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|metadata| {
            metadata
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

fn optional_recovery_reason(reason: Option<String>, action_kind: &str) -> String {
    reason
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| action_kind.to_owned())
}

fn required_recovery_reason(reason: Option<String>, action_kind: &str) -> Result<String> {
    reason
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ServiceError::invalid_operation(format!("{action_kind} requires a recovery reason"))
        })
}

fn recovery_marker(
    task_id: &str,
    state: &str,
    action: &str,
    actor: &api_types::Actor,
    reason: &str,
) -> db::CreateTransitionLog {
    db::CreateTransitionLog {
        id: db::new_uuid_v4(),
        task_id: task_id.to_owned(),
        from_state: state.to_owned(),
        to_state: state.to_owned(),
        trigger_name: Some(action.to_owned()),
        triggered_by: actor.display(),
        trigger_reason: reason.to_owned(),
        hook_results_json: None,
        rejection: false,
        created_at: db::now_rfc3339(),
    }
}

fn overlapping_execution_roles(role: &str) -> Vec<String> {
    // Restoration must never put an old blocker over a live replacement in
    // another workflow role. The DB helper interprets an empty list as the
    // fail-closed all-role wildcard.
    let _ = role;
    Vec::new()
}

struct ResumeProcessPlan {
    gate_state: String,
    target_state: String,
    budget: i32,
    count: i64,
}

fn is_retry_exhausted_annotation(raw_annotation: &str) -> bool {
    match serde_json::from_str::<api_types::TaskAnnotation>(raw_annotation) {
        Ok(api_types::TaskAnnotation::Blocking(ref annotation)) => {
            crate::task_diagnostics::is_retry_budget_exhausted(annotation)
        }
        _ => false,
    }
}

fn blocked_metadata_kind(raw_metadata: &str) -> Option<api_types::FailureKind> {
    let metadata: Value = serde_json::from_str(raw_metadata).ok()?;
    serde_json::from_value(metadata.get("kind")?.clone()).ok()
}

fn is_retry_exhausted_blocked_metadata(raw_metadata: &str) -> bool {
    blocked_metadata_kind(raw_metadata)
        .is_some_and(api_types::FailureKind::is_retry_exhausted_metadata)
}
