//! Recovery review attempts and Task retry budget mutations.

use super::*;

impl TaskService {
    /// Reuse the Task's live Review attempt, or open one for the current
    /// implementation candidate. A reviewer execution can only bind an
    /// attempt that is still `Running`, so a settled attempt has to be
    /// replaced before recovery can launch. An attempt awaiting human
    /// approval is deliberately left alone: opening a new one would discard
    /// the gate, so recovery says so instead of failing as a conflict.
    pub(in crate::task_service::execution) async fn ensure_review_attempt_for_recovery(
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
        let candidate = crate::task_service::latest_executor_execution_for_task(&self.db, task)
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
        // A coordination root has no implementation run to go back to: its
        // retry is review-only. The aggregate review runs again in place,
        // under the fresh budget, instead of moving the root to a working
        // state where no role may run for it.
        let review_only = task.parent_task_id.is_none()
            && task.status == gate_state
            && crate::workflow::is_review_state(&self.workflow_for_task(&task).await?, &gate_state)
            && crate::task_hierarchy::coordination_root_has_subtasks(&self.db, &task).await?;
        let resume_after_reset = if review_only {
            None
        } else if task.status == gate_state {
            let failed_review = if crate::workflow::is_review_state(
                &self.workflow_for_task(&task).await?,
                &gate_state,
            ) {
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
        transition_log.bridge = api_types::TransitionBridge::recovery("retry", true);
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
        if review_only {
            let id = uuid::Uuid::parse_str(&updated.id)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
            let requested_by = TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Api,
            ));
            return Box::pin(self.rerun_review(id, requested_by)).await;
        }
        if updated.status == crate::workflow::default_states::LEAD_CHECK
            && updated.parent_task_id.is_some()
        {
            // Validate the existing rework in a new entry, under the reset
            // gate budget. Its role continuation waits for any root re-plan.
            return self
                .recover_retry_current_state_hooks(updated, Some(reason))
                .await;
        }
        Ok(updated)
    }

    /// Clear a coordination root's exhausted review budget without running
    /// anything: the first half of sending the root back for a corrective
    /// subtask. The marker is the new counting boundary, so the aggregate
    /// review that follows the corrective work starts a fresh budget.
    pub(crate) async fn reset_root_review_budget_for_reopen(
        &self,
        task: &Task,
        reason: &str,
    ) -> Result<Task> {
        let (gate_state, _budget, _count) = self.current_gate_retry_budget(task).await?;
        let mut transition_log = recovery_marker(
            &task.id,
            &gate_state,
            "retry",
            &TaskService::task_action_actor(api_types::Actor::user(
                api_types::UserActionSource::Action(api_types::TaskAction::retry()),
            )),
            reason,
        );
        transition_log.bridge = api_types::TransitionBridge::recovery("retry", true);
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
        self.clear_retry_exhausted_blocking_metadata_with_marker(task, transition_log)
            .await
    }

    pub(crate) async fn permit_one_task_retry(
        &self,
        task: Task,
        reason: Option<String>,
        context: Option<String>,
    ) -> Result<Task> {
        let reason = required_recovery_reason(reason, "retry")?;

        if task.condition.read().entry_recorded {
            let transition_log = recovery_marker(
                &task.id,
                &task.status,
                "retry",
                &TaskService::task_action_actor(api_types::Actor::user(
                    api_types::UserActionSource::Action(api_types::TaskAction::Retry {
                        reason: None,
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
        if !crate::workflow::is_review_state(&self.workflow_for_task(&task).await?, &gate_state)
            || (db::budget::allows_retry(i64::from(budget), count) && !has_exhausted_annotation)
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
                    reason: None,
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
                    reason: None,
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
                    bridge: Default::default(),
                    version: task.version,
                    reason: Some(transition_reason),
                    triggered_by: TaskService::task_action_actor(api_types::Actor::user(
                        api_types::UserActionSource::Action(api_types::TaskAction::Approve {
                            reason: None,
                            override_checks: Some(true),
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
}
