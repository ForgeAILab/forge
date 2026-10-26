//! Reviewer completion, check failures, and terminal reconciliation.

use super::*;
use db::task_condition::channels;

impl TaskService {
    pub(super) async fn maybe_cascade_reviewer_completion(
        &self,
        execution: &Execution,
    ) -> Result<()> {
        let task = match TaskRepo::get_by_id(&*self.db, &execution.task_id, false).await? {
            Some(task) => task,
            None => return Ok(()),
        };
        if !crate::workflow::is_review_state(&self.workflow_for_task(&task).await?, &task.status) {
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
        let owner_finding = db::budget::owner_review_failure_message(
            &conformance,
            &review,
            &reviews,
            &self.db.review_window(&task.id).await?.reviews,
            self.derived_review_rules_apply(&task).await?,
        );
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
                        state: task.status.clone(),
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
    pub(super) async fn block_task_for_review_finding(
        &self,
        task: &Task,
        execution: &Execution,
        kind: api_types::FailureKind,
        message: String,
    ) -> Result<()> {
        let annotation = channels::ErrorAnnotation::new(channels::AnnotationShape::Blocking(
            channels::BlockingAnnotation {
                kind,
                blocking_reason: kind.to_string(),
                blocked_by: Some(
                    api_types::Actor::system(api_types::SystemComponent::Workflow).display(),
                ),
                blocked_at: Some(now_rfc3339()),
                blocked_execution_id: Some(execution.id.clone()),
                artifact: Some(channels::AnnotationArtifact {
                    kind: "execution".to_owned(),
                    id: Some(execution.id.clone()),
                    log_path: execution.logs_path.clone(),
                }),
                message: Some(message.clone()),
                hook: None,
            },
        ));
        let annotation = serde_json::to_string(&annotation).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize review blocking annotation: {error}"
            ))
        })?;
        let blocked_meta = serde_json::to_string(&channels::Interruption {
            reason: Some(message.clone()),
            created_at: Some(Some(now_rfc3339())),
            kind: Some(Some(kind.into())),
            execution_id: Some(Some(execution.id.clone())),
            ..Default::default()
        })
        .expect("condition channel serializes");
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
                    if let Some(message) = db::budget::owner_review_failure_message(
                        &conformance,
                        review,
                        &reviews,
                        &self.db.review_window(&task.id).await?.reviews,
                        self.derived_review_rules_apply(&task).await?,
                    ) {
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
        // This is the Review's result comment: a replay that reconciles the
        // settled Review must find it under the same key.
        self.create_review_comment(
            &updated_review,
            "result",
            execution.agent_id.as_deref(),
            format!(
                "Review failed (attempt {}): reviewer execution {}",
                updated_review.attempt_number, reason
            ),
        )
        .await?;

        self.block_task_after_executor_failure(&task, execution)
            .await
    }
}

#[cfg(test)]
mod lane_condition_tests;
