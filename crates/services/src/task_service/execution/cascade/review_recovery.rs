//! Review failure routing and automatic recovery dispatch.

use super::*;
use db::task_condition::channels;

impl TaskService {
    pub(crate) async fn review_failure_target(
        &self,
        task: &Task,
        execution_id: Option<&str>,
    ) -> Result<(Task, Option<String>, String)> {
        self.review_failure_target_with_config(task, execution_id, None)
            .await
    }

    pub(crate) async fn review_failure_target_with_config(
        &self,
        task: &Task,
        execution_id: Option<&str>,
        state_config: Option<&Value>,
    ) -> Result<(Task, Option<String>, String)> {
        if !db::task_writer::owns_task(&task.id) {
            return self
                .request_task_command(
                    &task.id,
                    "review_failure_target",
                    json!([task.id, task.status, execution_id, state_config]),
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
        let lead_assigned =
            crate::task_hierarchy::coordination_root_lead(&self.db, task, &workflow)
                .await?
                .is_some();
        let policy = match latest_review.as_ref() {
            Some(review) if review.status == ReviewStatus::Failed && !lead_assigned => {
                let window = self.db.review_window(&task.id).await?;
                db::review_findings::round_policy(&window.reviews, review).unwrap_or_else(|error| {
                    tracing::warn!(task_id = %task.id, %error, "unreadable historical ledger; using ordinary review remediation");
                    db::review_findings::RoundPolicy::default()
                })
            }
            _ => db::review_findings::RoundPolicy::default(),
        };
        if let Some(review) = latest_review.as_ref() {
            let details: Value =
                serde_json::from_str(&review.step_results_json).unwrap_or_default();
            if let Ok(conformance) = serde_json::from_value::<api_types::ReviewConformance>(
                details["conformance"].clone(),
            ) {
                let window = self.db.review_window(&task.id).await?;
                if let Some(message) = db::budget::owner_review_failure_message(
                    &conformance,
                    review,
                    &ReviewRepo::list_by_task(&*self.db, &task.id).await?,
                    &window.reviews,
                    !lead_assigned,
                ) {
                    let id = review
                        .reviewer_execution_id
                        .as_deref()
                        .or(execution_id)
                        .unwrap_or(&review.execution_id);
                    let execution = ExecutionRepo::get_by_id(&*self.db, id)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("execution", id))?;
                    self.block_task_for_review_finding(
                        task,
                        &execution,
                        api_types::FailureKind::ReviewNeedsOwner,
                        message.clone(),
                    )
                    .await?;
                    let updated = TaskRepo::get_by_id(&*self.db, &task.id, false)
                        .await?
                        .ok_or(DbError::NotFound)?;
                    return Ok((updated, None, message));
                }
            }
        }
        let effective_config = state_config.cloned().or_else(|| {
            review_state.map(|state| db::budget::state_config(state, &project.settings))
        });
        let budget = db::budget::limit(
            task,
            db::budget::Kind::Review,
            effective_config.as_ref(),
            review_state.and_then(|state| state.gate_config.as_ref()),
        )?;
        let existing_count =
            db::budget::spent(self.db.pool(), &task.id, db::budget::Kind::Review.key()).await?;
        let mut progress_granted = false;
        if !db::budget::allows_retry(i64::from(budget), existing_count) && policy.progress {
            if let Some(review) = &latest_review {
                let progress_limit = db::budget::limit(
                    task,
                    db::budget::Kind::ReviewProgress,
                    effective_config.as_ref(),
                    None,
                )?;
                progress_granted = db::budget::grant_review_progress(
                    &self.db,
                    task,
                    review,
                    i64::from(progress_limit),
                )
                .await?;
            }
        }
        if !db::budget::allows_retry(i64::from(budget), existing_count) && !progress_granted {
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
            let blocked_meta = serde_json::to_string(&channels::Interruption {
                reason: Some(reason.into()),
                created_at: Some(Some(now_rfc3339())),
                kind: Some(Some(api_types::FailureKind::ReviewGateFailed.into())),
                source: Some(None),
                execution_id: Some(execution_id.map(str::to_owned)),
                ..Default::default()
            })
            .expect("condition channel serializes");
            let annotation = serde_json::to_string(&channels::ErrorAnnotation::new(
                channels::AnnotationShape::Budget(channels::BudgetAnnotation {
                    kind: api_types::FailureKind::ReviewBudgetExhausted,
                    blocking_reason: reason.into(),
                    message: reason.into(),
                    detected_at: now_rfc3339(),
                }),
            ))
            .expect("condition channel serializes");
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
                    error_annotation: Some(Some(annotation)),
                    blocked_json: Some(Some(blocked_meta)),
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
                let blocked_meta = serde_json::to_string(&channels::Interruption {
                    reason: Some(reason.into()),
                    created_at: Some(Some(now_rfc3339())),
                    kind: Some(Some(api_types::FailureKind::ReviewGateFailed.into())),
                    source: Some(None),
                    execution_id: Some(execution_id.map(str::to_owned)),
                    ..Default::default()
                })
                .expect("condition channel serializes");
                let annotation = channels::ErrorAnnotation::new(channels::AnnotationShape::Budget(
                    channels::BudgetAnnotation {
                        kind: api_types::FailureKind::ReviewGateFailed,
                        blocking_reason: reason.into(),
                        message: format!("{reason}: the review gate sends a failed review to `{target}`, which this workflow does not define"),
                        detected_at: now_rfc3339(),
                    },
                ));
                let annotation =
                    serde_json::to_string(&annotation).expect("condition channel serializes");
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
                        error_annotation: Some(Some(annotation)),
                        blocked_json: Some(Some(blocked_meta)),
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
                    if progress_granted {
                        self.publish_review_progress_comment(
                            review,
                            task,
                            effective_config.as_ref(),
                        )
                        .await?;
                    } else {
                        self.publish_review_repair_comment(review, existing_count, budget)
                            .await?;
                    }
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

        let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &crate::worker_runtime::queue::cascade_actor(),
        );
        // The brief is best effort: like a refused dispatch, an unreadable
        // context leaves the normal review-failure routing in charge.
        let mut context = match crate::workflow::dispatch::loader::load_agent_dispatch_context(
            crate::workflow::dispatch::loader::DispatchContextParams {
                db: Arc::clone(&self.db),
                router: &self.workspace_backend_router,
                task_id: &task.id,
                role: crate::workflow::default_roles::CODER,
                state_name: &task.status,
                state_config: json!({}),
                execution_policy: Some(
                    crate::workflow::dispatch::EXECUTION_POLICY_RESUME_LATEST_TARGET_ROLE_THREAD,
                ),
                workflow: &workflow,
            },
        )
        .await
        {
            Ok(context) => context,
            Err(error) => {
                tracing::warn!(
                    task_id = %task.id,
                    project_id = %project.id,
                    %error,
                    "automatic review recovery skipped because its repair brief could not load"
                );
                return Ok(None);
            }
        };
        context.repair_budget = Some(crate::workflow::dispatch::repair_brief::RepairBudget {
            name: "review".into(),
            spent: existing_rejections,
            limit: i64::from(budget),
        });
        let prompt = render_automatic_review_recovery_prompt(
            &context,
            failure_reason,
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

#[cfg(test)]
mod writer_parity_tests;
