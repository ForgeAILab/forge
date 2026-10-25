//! Process resumption plans and recovery rejection handling.

use super::*;

impl TaskService {
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

    pub(super) async fn continue_task_process_with_plan(
        &self,
        task: Task,
        reason: Option<String>,
        context: Option<String>,
        plan: ResumeProcessPlan,
        require_interruption: bool,
    ) -> Result<Task> {
        if !db::budget::allows_retry(i64::from(plan.budget), plan.count) {
            return Err(ServiceError::invalid_operation(format!(
                "resume_process is not supported because retry budget is exhausted for state {}",
                plan.gate_state
            )));
        }

        let has_interruption =
            task.condition.read().diagnostic_present || task.condition.read().interruption_present;
        let has_failed_review = if crate::workflow::is_review_state(
            &self.workflow_for_task(&task).await?,
            &task.status,
        ) {
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
                // The same-state marker below is the recovery record; this
                // gate->target move is an ordinary rejection, as in history.
                Default::default(),
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

    pub(crate) async fn transition_recovery_rejection(
        &self,
        task: &Task,
        target_state: String,
        reason: String,
        actor: &api_types::Actor,
        bridge: api_types::TransitionBridge,
    ) -> Result<Task> {
        crate::task_service::execution::ensure_plan_publication_transition_authority(task, None)?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(task, &project.workflow_definition, actor);
        let uses_system_only_trigger = workflow
            .trigger_between(&task.status, &target_state)
            .is_some_and(|trigger| trigger.system_only());

        if uses_system_only_trigger && !actor.is_system() {
            let engine = self.workflow_execution();
            let recovered = engine
                .transition_inner(
                    task.id.clone(),
                    target_state,
                    task.version,
                    &workflow,
                    actor.clone(),
                    reason,
                    true,
                    true,
                    None,
                    None,
                    None,
                    Some(crate::workflow::engine::WorkflowAuthority {
                        project_version: project.version,
                        workflow_definition: project.workflow_definition.clone(),
                        clear_review_passed_at_on_commit: false,
                    }),
                    false,
                    bridge,
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
                    bridge,
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

    pub(super) async fn resume_process_plan(&self, task: &Task) -> Result<ResumeProcessPlan> {
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

        let kind = if db::budget::is_review_state(state) {
            db::budget::Kind::Review
        } else {
            db::budget::Kind::GateRejection
        };
        let mut origin = task.clone();
        origin.status = state.name.clone();
        let budget = db::budget::limit(
            &origin,
            kind,
            Some(&state.config),
            state.gate_config.as_ref(),
        )?;
        let count = db::budget::spent(
            self.db.pool(),
            &task.id,
            &db::budget::gate_budget_key(state),
        )
        .await?;
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

    pub(crate) fn publish_recovery_applied(
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
}
