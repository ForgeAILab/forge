//! Queued task action admission, refusal, and application.

use super::*;

impl TaskService {
    /// Dispatcher-only consumption of an accepted Task action. This never
    /// calls a command handler or restores the old condition to authorize it.
    pub(crate) async fn dispatch_queued_recovery(&self, task: &Task) -> Result<bool> {
        if db::TaskStepRepo::entry_hooks_pending(&*self.db, &task.id).await? {
            return Ok(false);
        }
        if self.db.task_has_pending_remote_cancel(&task.id).await? {
            return Ok(false);
        }
        if !db::task_writer::owns_task(&task.id) {
            let queued = TaskMetadata::parse(task.metadata_json.as_deref())
                .map_err(|e| ServiceError::invalid_operation(e.to_string()))?
                .extra
                .contains_key(crate::deferred_dispatch::QUEUED_RECOVERY_KEY);
            if !queued {
                return Ok(false);
            }
            return self
                .request_task_command(
                    &task.id,
                    "dispatch_queued_recovery",
                    serde_json::json!([task.id]),
                    false,
                )
                .await;
        }
        let result = Box::pin(self.dispatch_queued_task_action_inner(task)).await;
        if let Err(error) = &result {
            if self.settle_queued_task_action_refusal(task, error).await? {
                return Ok(false);
            }
        }
        result
    }

    /// Record on an accepted action that the hierarchy refused its replay.
    /// The Task's version does not move: a wait must not invalidate itself.
    async fn mark_queued_action_awaits_hierarchy(&self, task: &Task) -> Result<()> {
        let Some(expected) = TaskMetadata::parse(task.metadata_json.as_deref())
            .ok()
            .and_then(|metadata| {
                metadata
                    .extra
                    .get(crate::deferred_dispatch::QUEUED_RECOVERY_KEY)
                    .cloned()
            })
            .filter(|raw| raw["awaits_hierarchy"] != true)
        else {
            return Ok(());
        };
        let mut value = expected.clone();
        value["awaits_hierarchy"] = json!(true);
        TaskRepo::mutate_metadata_with_change(
            &*self.db,
            &task.id,
            None,
            vec![TaskMetadataMutation::SetIf {
                key: crate::deferred_dispatch::QUEUED_RECOVERY_KEY.to_owned(),
                expected,
                value,
            }],
            &now_rfc3339(),
        )
        .await?;
        Ok(())
    }

    /// One failure boundary for replay: wait with its intent, or restore it.
    pub(crate) async fn settle_queued_task_action_refusal(
        &self,
        task: &Task,
        error: &ServiceError,
    ) -> Result<bool> {
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        let claim_race = matches!(
            error,
            ServiceError::Db(
                db::DbError::VersionConflict | db::DbError::TaskVersionConflict { .. }
            )
        ) && (current.version != task.version
            || crate::deferred_dispatch::queued_recovery(&current)
                .is_some_and(|intent| intent.launches_role()));
        // The root and child have separate writers. A root re-plan can win
        // after this action's eligibility read; its admission refusal is a
        // wait, even if the root has already finished when we settle it. The
        // intent records the refusal, so the scheduler parks the subtask on
        // its parent instead of replaying an action the hierarchy refuses.
        if crate::task_hierarchy::is_subtask_wait_refusal(error) {
            self.mark_queued_action_awaits_hierarchy(&current).await?;
            return Ok(true);
        }
        let waiting = claim_race
            || matches!(
                error,
                ServiceError::AgentPaused { .. } | ServiceError::ProjectPaused { .. }
            )
            || crate::placement::admission_refusal_is_retryable(&self.db, &task.id, error).await?;
        if waiting {
            if !claim_race {
                self.defer_placement_refusal(&current, error).await?;
            }
            return Ok(true);
        }
        let dependency_gate = match error {
            ServiceError::DependencyGate => true,
            ServiceError::GuardRejection { guard, .. } => guard == "dependency_gate",
            _ => false,
        };
        if dependency_gate && db::task_writer::owns_task(&current.id) {
            let unsatisfied =
                TaskDependencyRepo::unsatisfied_dependencies(&*self.db, &current.id).await?;
            if !unsatisfied.is_empty()
                && self
                    .cancelled_dependency_ids(&current, &unsatisfied)
                    .await?
                    .is_empty()
            {
                // An unfinished dependency is a wait, not a refusal: the
                // action stays accepted behind the same dependency wait an
                // unstarted Task shows, and runs when the dependency
                // settles (`wake_dependents_of_completed_task`) or its link
                // is removed. Restoring the old park here turned the wait
                // into an untyped `recovery_required: "dependency gate"`
                // that offered the refused action again.
                let dependency_ids = unsatisfied.clone();
                // A cycle is not a wait: name it and its repair.
                let message = if self.db.dependency_wait_is_cycle(&current.id).await? {
                    dependency_cycle_reason(&dependency_ids)
                } else {
                    format!(
                        "waiting for unfinished dependenc{}: {}",
                        if dependency_ids.len() == 1 {
                            "y"
                        } else {
                            "ies"
                        },
                        dependency_ids.join(", ")
                    )
                };
                crate::deferred_dispatch::record_dispatch_disposition_naming(
                    &self.db,
                    &current,
                    crate::deferred_dispatch::DEPENDENCY_WAIT_CAPABILITY,
                    &message,
                    &dependency_ids,
                )
                .await?;
                return Ok(true);
            }
        }
        let original = db::TaskMetadata::parse(task.metadata_json.as_deref())
            .ok()
            .and_then(|metadata| {
                metadata
                    .extra
                    .get(crate::deferred_dispatch::QUEUED_RECOVERY_KEY)
                    .cloned()
            });
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        let raw = db::TaskMetadata::parse(current.metadata_json.as_deref())
            .ok()
            .and_then(|metadata| {
                metadata
                    .extra
                    .get(crate::deferred_dispatch::QUEUED_RECOVERY_KEY)
                    .cloned()
            });
        if let (Some(original), Some(raw)) = (original, raw) {
            if original.get("id") == raw.get("id") {
                // A cancelled dependency is its own typed park, with its own
                // exit: it carries the condition this action had cleared, in
                // place of an untyped refusal that offers the action again.
                if dependency_gate && db::task_writer::owns_task(&current.id) {
                    let unsatisfied =
                        TaskDependencyRepo::unsatisfied_dependencies(&*self.db, &current.id)
                            .await?;
                    let cancelled = self
                        .cancelled_dependency_ids(&current, &unsatisfied)
                        .await?;
                    self.block_cancelled_dependencies(&current, &cancelled)
                        .await?;
                }
                self.restore_failed_task_action(&current, &raw, error)
                    .await?;
            }
        }
        Ok(false)
    }

    async fn dispatch_queued_task_action_inner(&self, task: &Task) -> Result<bool> {
        let raw = db::TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
            .extra
            .get(crate::deferred_dispatch::QUEUED_RECOVERY_KEY)
            .cloned();
        let task = match Box::pin(self.normalize_stored_task_action(task)).await {
            Ok(value) => value,
            Err(error) => {
                if let Some(raw) = raw.as_ref() {
                    self.restore_failed_task_action(task, raw, &error).await?;
                }
                return Err(error);
            }
        };
        let Some(queued) = crate::deferred_dispatch::queued_recovery(&task) else {
            if let Some(raw) = raw.as_ref() {
                self.restore_failed_task_action(
                    &task,
                    raw,
                    &ServiceError::invalid_operation("malformed queued Task action"),
                )
                .await?;
            }
            return Ok(false);
        };
        if queued.continuation && queued.target_state != task.status {
            // The entry this launch continued has ended by its own hooks. The
            // Task is where the workflow sent it, in the condition the
            // workflow gave it: the intent goes, and nothing is failed.
            let raw = raw.as_ref().expect("queued marker came from metadata");
            let task = TaskRepo::get_by_id(&*self.db, &task.id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
            if crate::deferred_dispatch::queued_recovery(&task)
                .is_none_or(|current| current.id != queued.id)
            {
                return Ok(false);
            }
            TaskRepo::restore_queued_recovery(
                &*self.db,
                db::RestoreQueuedRecovery {
                    task_id: task.id.clone(),
                    expected_version: task.version,
                    queued_recovery_id: raw
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    error_annotation: task.error_annotation.clone(),
                    blocked_json: task.blocked_json.clone(),
                    failed_json: task.failed_json.clone(),
                    updated_at: now_rfc3339(),
                },
            )
            .await?;
            return Ok(false);
        }
        if queued.target_state != task.status
            || task.archived_at.is_some()
            || task.condition.read().hard_failure
        {
            self.restore_failed_task_action(
                &task,
                raw.as_ref().expect("queued marker came from metadata"),
                &ServiceError::invalid_operation("stale queued Task action"),
            )
            .await?;
            return Err(ServiceError::invalid_operation("stale queued Task action"));
        }
        if let Some(running) = ExecutionRepo::list_running_by_task(&*self.db, &task.id)
            .await?
            .first()
        {
            let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
            if crate::deferred_dispatch::queued_recovery(&current)
                .is_none_or(|intent| intent.id != queued.id)
            {
                return Ok(false);
            }
            return Err(ServiceError::ExecutionAlreadyRunning {
                scope: task.id.clone(),
                execution_id: running.id.clone(),
            });
        }
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &queued.request.actor,
        );
        if queued.launches_role() {
            let role = crate::workflow::action_role(&workflow, &task.status).ok_or_else(|| {
                ServiceError::invalid_operation("queued action lost its workflow role")
            })?;
            if queued
                .request
                .role_name
                .as_deref()
                .is_some_and(|saved| saved != role)
            {
                return Err(ServiceError::invalid_operation(
                    "queued action's workflow role changed",
                ));
            }
            let assignment =
                crate::task_hierarchy::effective_role_assignment(&self.db, &task, role)
                    .await?
                    .map(|resolved| resolved.assignment);
            if queued
                .request
                .assignment_id
                .as_deref()
                .is_some_and(|id| assignment.as_ref().is_none_or(|row| row.id != id))
                || assignment.as_ref().is_some_and(|row| {
                    row.assignee_type != Some(AssigneeKind::Agent)
                        || row.assignee_id != queued.request.agent_id
                })
            {
                return Err(ServiceError::invalid_operation(
                    "queued action's role ownership changed",
                ));
            }
            let needs_session = matches!(
                queued.request.action,
                api_types::TaskAction::Release { .. }
                    | api_types::TaskAction::Retry {
                        fresh_session: Some(false),
                        ..
                    }
            );
            if needs_session {
                if let Some(id) = queued.request.offer.target_execution_id.as_deref() {
                    let execution = ExecutionRepo::get_by_id(&*self.db, id)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("execution", id.to_owned()))?;
                    let logical = workflow.states.iter().find(|state| state.name == task.status).and_then(|state| state.dispatch.as_ref())
                        .is_some_and(|dispatch| dispatch.execution_policy == Some(api_types::WorkflowExecutionPolicy::ResumeLatestTargetRoleThread));
                    if execution.task_id != task.id
                        || execution.agent_id != queued.request.agent_id
                        || (!logical && execution.agent_session_id.is_none())
                    {
                        return Err(ServiceError::invalid_operation(
                            "queued action's pinned session is no longer resumable",
                        ));
                    }
                }
            }
        }
        // An action that needs the hierarchy's admission is subject to the
        // same wait as an ordinary dispatch. Do not claim it or restore its
        // saved park while the root plans, is held, or an earlier sibling
        // runs: `next_step` parks it on the parent until then.
        if queued.needs_subtask_admission()
            && !crate::task_hierarchy::subtask_dispatch_ready(&self.db, &task).await?
        {
            return Ok(false);
        }
        if let Some(agent_id) = queued.request.agent_id.as_deref() {
            let agent = AgentRepo::get_by_id(&*self.db, agent_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("agent", agent_id.to_owned()))?;
            if agent.paused
                || crate::agent_service::compute_effective_status(
                    &self.db,
                    &agent,
                    self.daemon_connections.as_deref(),
                )
                .await?
                    == crate::agent_service::EffectiveStatus::DaemonOffline
            {
                return Ok(false);
            }
            if project.paused_at.is_some() {
                return Ok(false);
            }
            if crate::placement::machine_precheck::wait_before_dispatch(
                &self.db,
                self,
                &task,
                &agent,
                queued.request.role_name.as_deref(),
            )
            .await?
                || !crate::agent_capacity::has_execution_capacity(&self.db, &agent).await?
            {
                return Ok(false);
            }
        }
        // Admission/engine writes fence the exact Task revision and consume
        // the queue ID. Waiting must not advance Task.version each pass.
        if project.paused_at.is_some() {
            return Ok(false);
        }
        if workflow.state_kind(&task.status) == Some(api_types::StateKind::Initial) {
            let slots =
                crate::task_dispatcher::slots::load_project_slots(&self.db, &project).await?;
            if slots.limit != 0 && slots.active >= slots.limit {
                return Ok(false);
            }
        }
        let deferred = db::TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
            .extra
            .get("deferred_dispatch")
            .cloned();
        if crate::deferred_dispatch::is_pending(&task, chrono::Utc::now()) {
            return Ok(false);
        }
        let claim_value = json!({
            "not_before": (chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc3339(),
            "reason": "recovery replay in flight",
            "target_state": queued.target_state,
        });
        let mutation = match deferred.as_ref() {
            Some(old) => TaskMetadataMutation::SetIf {
                key: "deferred_dispatch".to_owned(),
                expected: old.clone(),
                value: claim_value.clone(),
            },
            None => TaskMetadataMutation::SetIfAbsent {
                key: "deferred_dispatch".to_owned(),
                value: claim_value.clone(),
            },
        };
        let (claimed, changed) = TaskRepo::mutate_metadata_with_change(
            &*self.db,
            &task.id,
            Some(task.version),
            vec![mutation],
            &now_rfc3339(),
        )
        .await?;
        if !changed {
            return Ok(false);
        }
        let result = REPLAYING_RECOVERY
            .scope(
                (claimed.id.clone(), queued.id.clone()),
                crate::task_service::actions::TASK_ACTION_ACTOR.scope(
                    queued.request.actor.clone(),
                    Box::pin(self.apply_queued_task_action(claimed, &queued)),
                ),
            )
            .await;
        // Release only this replay's temporary claim, preserving an owner or
        // environment wait installed by admission itself.
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", &task.id))?;
        let release = match deferred.as_ref() {
            Some(old) => TaskMetadataMutation::SetIf {
                key: "deferred_dispatch".to_owned(),
                expected: claim_value.clone(),
                value: old.clone(),
            },
            None => TaskMetadataMutation::RemoveIf {
                key: "deferred_dispatch".to_owned(),
                expected: claim_value.clone(),
            },
        };
        TaskRepo::mutate_metadata_with_change(
            &*self.db,
            &task.id,
            Some(current.version),
            vec![release],
            &now_rfc3339(),
        )
        .await?;
        result?;

        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        if crate::deferred_dispatch::queued_recovery(&current)
            .is_some_and(|value| value.id == queued.id)
        {
            if crate::deferred_dispatch::is_pending(&current, chrono::Utc::now())
                || crate::deferred_dispatch::current_dispatch_disposition(&current).is_some_and(
                    |disposition| {
                        matches!(
                            disposition.capability.as_str(),
                            "machine_capacity" | "project_capacity"
                        )
                    },
                )
            {
                return Ok(false);
            }
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
        let reason = match &request.action {
            api_types::TaskAction::Approve { reason, .. }
            | api_types::TaskAction::Retry { reason, .. }
            | api_types::TaskAction::Cancel { reason }
            | api_types::TaskAction::Hold { reason }
            | api_types::TaskAction::Release { reason }
            | api_types::TaskAction::Restart { reason } => reason.clone(),
            _ => None,
        };
        let guidance = match &request.action {
            api_types::TaskAction::Retry { guidance, .. } => guidance.clone(),
            api_types::TaskAction::SendBack { guidance } => Some(guidance.clone()),
            api_types::TaskAction::Release { reason } => reason.clone(),
            _ => None,
        };
        let annotation = queued
            .error_annotation
            .as_deref()
            .and_then(|raw| serde_json::from_str::<api_types::TaskBlockingAnnotation>(raw).ok());
        let role_launch = queued.launches_role();
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
                    self.restart_task_condition(task, &annotation, reason.clone())
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
                "ready_to_start" | "initial_retry" => {
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
                    // An accepted lead recovery is a new subtask window, even
                    // when admission queued it as a role retry. Never validate
                    // under the gate's spent window. The queue ID fences the
                    // reset receipt; redelivery before admission sees zero
                    // spending and cannot open another window.
                    // The offer names the lead for every caller, so its
                    // authority list does not say who accepted it: only the
                    // root's lead itself opens the window. The owner's or the
                    // Project Agent's ordinary Retry keeps the spent window.
                    let accepted_by_lead = match &request.actor {
                        api_types::Actor::Agent { agent_id, .. }
                            if task.parent_task_id.is_some()
                                && task.status == crate::workflow::default_states::LEAD_CHECK =>
                        {
                            crate::task_hierarchy::lead_verbs(&self.db, &task)
                                .await?
                                .is_some_and(|(lead, _)| lead == *agent_id)
                        }
                        _ => false,
                    };
                    let task = if accepted_by_lead
                        && matches!(&request.action, api_types::TaskAction::Retry { .. })
                        && request
                            .offer
                            .authority
                            .contains(&api_types::ActionAuthority::Lead)
                        && db::budget::spent(
                            self.db.pool(),
                            &task.id,
                            &db::budget::gate_key(&task.status),
                        )
                        .await?
                            > 0
                    {
                        let mut marker = recovery_marker(
                            &task.id,
                            &task.status,
                            "retry",
                            &request.actor,
                            "lead recovery opened a fresh step-check window",
                        );
                        marker.id = queued.id.clone();
                        marker.bridge = api_types::TransitionBridge::recovery("retry", true);
                        self.clear_retry_exhausted_blocking_metadata_with_marker(&task, marker)
                            .await?
                    } else {
                        task
                    };
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
                        let prompt = self
                            .task_action_role_prompt(&task, &workflow, role, &request.action)
                            .await?;
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
                                    prompt.clone(),
                                    "task_action",
                                )
                                .await?;
                                return Ok(TaskRepo::get_by_id(&*self.db, &task.id, false)
                                    .await?
                                    .expect("Task remains present"));
                            }
                            let result = self
                                .follow_up_execution(execution.id, prompt, execution.agent_id, None)
                                .await?;
                            self.start_execution(result.execution.id.clone()).await?;
                            return Ok(result.task);
                        }
                    }
                    let parent = ExecutionRepo::list_by_task(
                        &*self.db,
                        &task.id,
                        db::PageRequest {
                            cursor: None,
                            limit: 100,
                            include_total: false,
                            sort_by: db::SortBy::CreatedAt,
                            sort_order: db::SortOrder::Desc,
                        },
                    )
                    .await?
                    .items
                    .into_iter()
                    .find(|execution| {
                        execution.status != ExecutionStatus::Running
                            && execution.role == role
                            && execution.agent_id == request.agent_id
                    });
                    if let Some(parent) = parent {
                        let result = if guidance.is_some() {
                            Box::pin(
                                self.re_execute_execution_with_context(parent.id, guidance.clone()),
                            )
                            .await?
                        } else {
                            Box::pin(self.re_execute_execution(parent.id)).await?
                        };
                        self.start_execution(result.execution.id.clone()).await?;
                        return Ok(result.task);
                    }
                    let agent_id = request.agent_id.as_deref().ok_or_else(|| {
                        ServiceError::invalid_operation("accepted retry lost its agent")
                    })?;
                    Box::pin(self.dispatch_recovery_role(
                        &task,
                        &project,
                        &workflow,
                        agent_id,
                        role,
                        &request.action,
                    ))
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
                        self.refresh_workspace_and_retry_entry(task, reason.clone())
                            .await
                    } else if task.condition.read().entry_recorded {
                        self.retry_entry_checks(task, reason.clone()).await
                    } else {
                        self.recover_retry_current_state_hooks(task, reason.clone())
                            .await
                    }
                }
                "entry_barrier_override" | "state_hooks_override" => {
                    self.override_entry_checks_once(
                        task,
                        Some(reason.clone().unwrap_or_default()),
                        None,
                    )
                    .await
                }
                "retry_budget_exhausted" => {
                    let mut evidence = task;
                    evidence.error_annotation = queued.error_annotation.clone();
                    evidence.blocked_json = queued.blocked_json.clone();
                    evidence.condition =
                        db::compose_condition(&db::ConditionInput::from(&evidence));
                    if matches!(
                        &request.action,
                        api_types::TaskAction::Retry {
                            reset_budget: Some(false),
                            ..
                        }
                    ) {
                        self.permit_one_task_retry(evidence, reason.clone(), guidance)
                            .await
                    } else {
                        self.reset_task_retry_budget(evidence, reason.clone()).await
                    }
                }
                "review_checks_retry" => {
                    self.apply_review_check_retry(&task, request.actor.clone())
                        .await
                }
                "review_failed" => self.retry_review_entry(task, guidance).await,
                "review_owner_retry" => self.retry_review_entry(task, guidance).await,
                "merge_retry_remediation" => {
                    let mut evidence = task;
                    evidence.error_annotation = queued.error_annotation.clone();
                    evidence.blocked_json = queued.blocked_json.clone();
                    evidence.condition =
                        db::compose_condition(&db::ConditionInput::from(&evidence));
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
                                override_checks: Some(false),
                                ..
                            }
                        ))
                    .then_some(annotation.as_ref())
                    .flatten();
                    self.recover_manual_review_pass(
                        task,
                        reason.clone().unwrap_or_default(),
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
            Box::pin(apply).await?
        } else {
            crate::task_service::actions::TASK_ACTION_COMMAND
                .scope((), Box::pin(apply))
                .await?
        };
        if role_launch {
            if matches!(
                request.action,
                api_types::TaskAction::Retry { .. } | api_types::TaskAction::Release { .. }
            ) {
                self.publish(ForgeEvent {
                    event_type: "task.recovery_action".to_owned(),
                    entity_id: updated.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskRecovered {
                        project_id: updated.project_id.clone(),
                        reason: reason.unwrap_or_else(|| request.action.verb().to_owned()),
                    },
                });
            }
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
}
