use super::*;
use ::review::ReviewWorkspace;
use api_types::Actor;
/// What a stopped execution of the retired inline review re-run says while
/// its Task is on hold. The release looks for it and runs the review entry.
pub(crate) const RETIRED_REVIEW_RERUN_HELD: &str =
    "this execution belonged to the inline review re-run, \
     which this Forge version no longer has; it was stopped at upgrade. The Task is on hold: \
     its review entry runs again when it is released";

/// Until v0.14 a manual review re-run ran inline: Forge created a reviewer
/// execution that no role ran, leased it to itself (`review-owner:<uuid>`,
/// logs in `review-<candidate>.jsonl`) and ran CI under it. That pipeline is
/// gone. A server upgraded while one was in flight still finds its execution
/// `running`; this names it, so recovery can settle it instead of treating it
/// as a reviewer that died.
pub(crate) fn retired_review_rerun(execution: &Execution) -> bool {
    execution
        .lease_owner
        .as_deref()
        .is_some_and(|owner| owner.starts_with("review-owner:"))
        || (execution.role == crate::workflow::default_roles::REVIEWER
            && execution.executor_config_snapshot_json.is_none()
            && execution
                .logs_path
                .as_deref()
                .and_then(|path| std::path::Path::new(path).file_name())
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("review-")))
}

impl TaskService {
    /// Settle an execution of the retired inline review re-run that recovery
    /// just stopped: say on the execution why it ended, then run the Task's
    /// review entry again through the durable path. The re-entry closes the
    /// review attempt the old pipeline left open.
    ///
    /// The old pipeline ran an auditor child beside its reviewer execution;
    /// the Task is settled once, when the last of them has stopped. A Task
    /// whose review entry is already running needs nothing more. When the
    /// entry cannot run at all (the Task has no candidate), the Task is
    /// blocked on the stopped execution with the owner's recovery actions.
    pub(crate) async fn settle_retired_review_rerun(&self, execution: &Execution) -> Result<()> {
        const REASON: &str = "this execution belonged to the inline review re-run, \
             which this Forge version no longer has; it was stopped at upgrade";
        self.db
            .record_retired_review_rerun_error(REASON, now_rfc3339(), &execution.id)
            .await?;
        let Some(task) = TaskRepo::get_by_id(&*self.db, &execution.task_id, false).await? else {
            return Ok(());
        };
        if task.status != "review" {
            return Ok(());
        }
        if ExecutionRepo::list_running_by_task(&*self.db, &task.id)
            .await?
            .iter()
            .any(retired_review_rerun)
        {
            return Ok(());
        }
        // A held Task waits for its owner: the stopped execution says why it
        // ended, and the hold (not a failure) is what the Task shows. The
        // review is not re-entered behind the hold. The execution remembers
        // it: the release settles it again, and then the entry runs
        // (`resume_review_entry_after_release`).
        if task
            .condition
            .reasons()
            .any(|reason| matches!(reason, db::ParkReason::Held { .. }))
        {
            self.db
                .record_retired_review_rerun_error(
                    RETIRED_REVIEW_RERUN_HELD,
                    now_rfc3339(),
                    &execution.id,
                )
                .await?;
            return Ok(());
        }
        let id = Uuid::parse_str(&task.id)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let requested_by = Actor::system(api_types::SystemComponent::CrashRecovery);
        match Box::pin(self.rerun_review(id, requested_by)).await {
            Ok(_) | Err(ServiceError::TaskBusy { .. }) => Ok(()),
            Err(error) => {
                tracing::warn!(
                    task_id = %task.id,
                    execution_id = %execution.id,
                    %error,
                    "the review entry could not be re-run after the retired inline re-run was stopped"
                );
                let mut stopped = execution.clone();
                stopped.error = Some(format!(
                    "{REASON}, and the review entry could not be run again: {error}"
                ));
                let task = TaskRepo::get_by_id(&*self.db, &task.id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
                self.block_task_after_executor_failure(&task, &stopped)
                    .await
            }
        }
    }

    /// Start-time half of [`Self::settle_retired_review_rerun`]: a server
    /// restarted quickly still holds the old pipeline's unexpired leases, so
    /// its executions are stopped here instead of waiting for them to expire.
    pub(crate) async fn stop_retired_review_reruns(&self, task_id: &str) -> Result<()> {
        let stopped = crate::recovery::stop_retired_review_reruns(&self.db, task_id).await?;
        for execution_id in &stopped {
            if let Some(execution) = ExecutionRepo::get_by_id(&*self.db, execution_id).await? {
                self.settle_retired_review_rerun(&execution).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn annotate_review_ci_interruption(
        &self,
        task: &Task,
        ctx: &crate::workflow::HookContext,
        reason: &str,
        retry: bool,
        reset: bool,
    ) -> Result<Task> {
        let now = now_rfc3339();
        let mut barrier: Value = task
            .entry_barrier_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_else(|| json!({}));
        let disconnected = db::WorkspacePlacementRepo::get_for_task(&*self.db, &task.id)
            .await?
            .is_some_and(|placement| placement.state == db::PlacementState::Disconnected);
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        if let Some(version) = ctx.project_version {
            let valid: bool = self
                .db
                .review_ci_project_authority_valid_in_tx(
                    &mut tx,
                    &task.project_id,
                    version,
                    ctx.project_workflow_definition.as_deref(),
                )
                .await?;
            if !valid {
                return Err(DbError::VersionConflict.into());
            }
        }
        // The episode window opened when this review entry began (or when a
        // connected CI run last succeeded); entry retries keep it.
        let attempts = if retry && !disconnected {
            let identity = crate::workflow::engine::durable::current_hook(&task.id)
                .map(|h| format!("{}:{}", h.step.id, h.index))
                .unwrap_or_else(|| format!("ci:{}", task.version));
            db::budget::charge(
                &mut tx,
                &task.id,
                db::budget::Kind::ReviewCiInfrastructure.key(),
                i64::from(db::budget::Kind::ReviewCiInfrastructure.default_limit()),
                &identity,
            )
            .await?
            .spent
        } else {
            self.db
                .review_ci_infrastructure_budget_spent_in_tx(&mut tx, &task.id)
                .await?
        };
        let exhausted = retry
            && !disconnected
            && !db::budget::allows_retry(
                i64::from(db::budget::Kind::ReviewCiInfrastructure.default_limit()),
                attempts,
            );
        let retry = retry && !exhausted;
        let kind = if reset {
            "workspace_reset_required"
        } else if retry {
            "review_ci_infrastructure"
        } else if exhausted {
            "review_ci_infrastructure_exhausted"
        } else {
            "review_ci_unavailable"
        };
        barrier["state"] = json!(task.status);
        barrier["status"] = json!("blocked");
        barrier["updated_at"] = json!(now);
        barrier["interrupted_at"] = json!(now);
        barrier["blocking_reason"] = json!(reason);
        let annotation = json!({"type":if reset {api_types::FailureKind::WorkspaceResetRequired} else {api_types::FailureKind::BeforeWorkHookFailed},"blocking_reason":kind,"blocked_at":now,"blocked_by":"system:workflow","message":reason});
        let deferral = retry.then(|| json!({
            "target_state": task.status, "reason": reason,
            "not_before": (Utc::now() + chrono::Duration::seconds(5 * (1_i64 << attempts.saturating_sub(1).clamp(0,4)))).to_rfc3339(),
        }).to_string());
        let changed = db::task_writer::TaskQuery::new(&self.db,&task.id,"UPDATE task SET entry_barrier_json = ?, error_annotation = ?,
            blocked_json = ?, metadata_json = CASE WHEN ? IS NULL THEN
            json_remove(COALESCE(metadata_json, '{}'), '$.deferred_dispatch') ELSE
            json_set(COALESCE(metadata_json, '{}'), '$.deferred_dispatch', json(?)) END,
            updated_at = ?, version = version + 1
            WHERE id = ? AND version = ? AND status = ? AND deleted_at IS NULL")
            .bind(barrier.to_string()).bind(annotation.to_string())
            .bind((!retry).then(|| json!({"kind": if reset { api_types::FailureKind::WorkspaceResetRequired } else { api_types::FailureKind::BeforeWorkHookFailed }, "reason": reason, "created_at": now, "execution_id": null}).to_string()))
            .bind(&deferral).bind(&deferral).bind(&now).bind(&task.id).bind(task.version).bind(&task.status)
            .execute_in_tx(&mut tx).await?;
        // Runs in the Task's review hook step; a queued write would make the
        // attention and returned Task below untruthful.
        if changed.require_applied()? != 1 {
            return Err(DbError::VersionConflict.into());
        }
        if !retry {
            crate::placement::admission::record_wait_attention_in_tx(
                &self.db,
                &mut tx,
                task,
                "execution_failed",
                &format!("Review CI [{kind}] could not run: {reason}"),
                &format!("review-ci:{}", task.id),
            )
            .await?;
        }
        tx.commit().await?;
        TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))
    }

    pub(crate) async fn review_workspace_io(
        &self,
        workspace: &Workspace,
    ) -> Result<Arc<dyn ReviewWorkspace>> {
        let workspace = crate::workspace_backend::EmbeddedWorkspaceBackend::resolve_workspace(
            &self.workspace_backend_router,
            &self.db,
            workspace,
            &self.workspace_root,
        )
        .await?;
        Ok(Arc::new(workspace))
    }

    /// Re-run the review of a Task that sits in `review`: the Task re-enters
    /// its current state, so the ordinary durable review entry runs again for
    /// the current candidate (before-work hooks, entry CI through the check
    /// runner, then reviewer dispatch). Nothing runs inline: the returned Task
    /// is the re-entered one and its hooks step is queued behind this step.
    ///
    /// The entry runs under the workflow's own authority, exactly as an
    /// automatic entry does: failed CI goes to remediation, a Task without a
    /// reviewer passes on its checks, and a gate that requires a person still
    /// waits for one. A same-state entry is never a mechanical merge bridge,
    /// so a carried approval cannot stand in for the review that was asked for.
    ///
    /// `requested_by` is who asked. It does not route the entry; it is
    /// recorded on the entry, and an entry the owner asked for spends no
    /// review retry budget whatever it finds (red checks, a failing reviewer,
    /// the bounce to remediation). Any other requester's entry spends as an
    /// automatic one does.
    pub async fn rerun_review(&self, task_id: Uuid, requested_by: Actor) -> Result<Task> {
        let task_id = task_id.to_string();
        if !db::task_writer::owns_task(&task_id) {
            return self
                .request_task_command(
                    &task_id,
                    "rerun_review",
                    serde_json::json!([task_id, requested_by]),
                    false,
                )
                .await;
        }
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if task.status != "review" {
            return Err(ServiceError::invalid_operation(format!(
                "task {task_id} is in {} state; expected review",
                task.status
            )));
        }
        // A held Task is its owner's decision to stop. Nothing re-enters its
        // review while the hold stands, whoever asks: recovery, a queued
        // command, a budget reset. (The owner is not offered the re-run on a
        // held Task.) After the release the request runs as usual.
        if task
            .condition
            .reasons()
            .any(|reason| matches!(reason, db::ParkReason::Held { .. }))
        {
            return Err(ServiceError::conflict(format!(
                "task {task_id} is on hold; its review runs again once it is released"
            )));
        }
        // A review needs something to review.
        self.latest_executor_execution(&task.id).await?;
        // A Task configured before v0.14 may still name an auditor. The
        // review it expects is the reviewer's: without an Agent in that role
        // the re-run would pass on its checks alone, so it is refused, as the
        // inline re-run refused it.
        if super::update::names_retired_auditor(task.task_state_config.as_deref())
            && !crate::task_hierarchy::effective_role_assignment(
                &self.db,
                &task,
                crate::workflow::default_roles::REVIEWER,
            )
            .await?
            .map(|resolved| resolved.assignment)
            .is_some_and(|assignment| {
                assignment.assignee_type == Some(db::AssigneeKind::Agent)
                    && assignment.assignee_id.is_some()
            })
        {
            return Err(ServiceError::conflict(format!(
                "task {task_id} names an auditor and has no reviewer Agent: {}",
                super::update::RETIRED_AUDITOR_CONFIG
            )));
        }
        // One review pipeline per Task. An entry that is still running (its
        // hooks step is queued, claimed or suspended on a check) or a
        // reviewer that is still working is not replaced by a second one.
        let reviewing = ExecutionRepo::list_running_by_task(&*self.db, &task_id)
            .await?
            .iter()
            .filter(|execution| matches!(execution.role.as_str(), "reviewer" | "auditor"))
            .count() as i64;
        if reviewing > 0 || db::TaskStepRepo::entry_hooks_pending(&*self.db, &task_id).await? {
            return Err(ServiceError::TaskBusy {
                pending_steps: db::TaskStepRepo::pending_steps(&*self.db, &task_id)
                    .await?
                    .max(reviewing)
                    .max(1),
                retry_after_ms: 250,
            });
        }
        // The attempt this re-run replaces is closed first, so the Task never
        // has two open attempts: neither a human gate nor a reviewer can
        // settle an attempt the owner asked to run again.
        let superseded = ReviewRepo::list_by_task(&*self.db, &task_id)
            .await?
            .into_iter()
            .max_by_key(|review| (review.attempt_number, review.id.clone()))
            .filter(|review| {
                matches!(
                    review.status,
                    ReviewStatus::Running | ReviewStatus::AwaitingHuman
                )
            });
        if let Some(review) = superseded {
            let now = now_rfc3339();
            let mut details: Value = serde_json::from_str(&review.step_results_json)
                .unwrap_or_else(|_| json!({ "ci_steps": [] }));
            if !details.is_object() {
                details = json!({ "ci_steps": details });
            }
            details["execution_retry"] = json!({
                "status": "superseded_by_rerun",
                "reason": "a review re-run was requested",
                "cancelled_at": now,
            });
            ReviewRepo::cancel_if_unchanged(
                &*self.db,
                &review.id,
                review.status.clone(),
                &review.updated_at,
                details.to_string(),
                &now,
                &now,
            )
            .await?;
        }
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let actor = Actor::system(api_types::SystemComponent::Workflow);
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);
        let reason = "review re-run requested";
        let entered = self
            .workflow_execution()
            .review_rerun_entry_with_authority(
                &task,
                &workflow,
                actor,
                reason,
                crate::workflow::engine::WorkflowAuthority {
                    project_version: project.version,
                    workflow_definition: project.workflow_definition.clone(),
                    clear_review_passed_at_on_commit: false,
                },
                api_types::TransitionBridge::review_rerun(&requested_by),
            )
            .await?
            .task;
        self.publish(ForgeEvent {
            event_type: "task.recovery_action".to_owned(),
            entity_id: entered.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRecovered {
                project_id: entered.project_id.clone(),
                reason: reason.to_owned(),
            },
        });
        Ok(entered)
    }

    pub async fn approve_review_as(
        &self,
        task_id: impl Into<String>,
        actor: Actor,
    ) -> Result<(Task, Review)> {
        let task_id = task_id.into();
        if !db::task_writer::owns_task(&task_id) {
            return self
                .request_task_command(
                    &task_id,
                    "approve_review_as",
                    serde_json::json!([task_id, actor]),
                    false,
                )
                .await;
        }
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if task.status != "review" {
            return Err(ServiceError::invalid_operation(format!(
                "task {task_id} is in {} state; expected review",
                task.status
            )));
        }
        let latest_review = self.latest_review_for_task(&task_id).await?;
        if latest_review.status != ReviewStatus::AwaitingHuman {
            return Err(ServiceError::invalid_operation(
                "latest review is not awaiting_human",
            ));
        }
        // Approval must never repair or default malformed persisted review
        // details. Validate the stored row before attempting the transition.
        super::strict_review_details(&latest_review)?;
        let finished_at = now_rfc3339();
        let (review, task) = ReviewRepo::update_status_with_task_authority(
            &*self.db,
            &latest_review.id,
            ReviewStatus::Passed,
            latest_review.step_results_json.clone(),
            Some(finished_at.clone()),
            &finished_at,
            task.version,
            Some(finished_at.clone()),
            db::ReviewEventOrigin::for_human(&actor),
        )
        .await?;

        if let Err(error) = self
            .memory_service
            .record_review_result_if_final(&task.project_id, &review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }
        self.create_system_comment(
            &task_id,
            format!("Review passed (attempt {})", review.attempt_number),
        )
        .await?;
        self.publish(ForgeEvent {
            event_type: "review.approved".to_owned(),
            entity_id: review.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReviewApproved {
                task_id: task_id.clone(),
                review_id: review.id.clone(),
            },
        });
        let transitioned = self
            .transition(
                task_id,
                "merging".to_owned(),
                TransitionOptions {
                    bridge: Default::default(),
                    version: task.version,
                    reason: None,
                    triggered_by: actor,
                    rejection: false,
                    defer_dispatch_seconds: None,
                },
            )
            .await?;
        Ok((transitioned.task, review))
    }

    pub async fn reject_review_as(
        &self,
        task_id: impl Into<String>,
        reason: Option<String>,
        actor: Actor,
    ) -> Result<(Task, Review)> {
        let task_id = task_id.into();
        if !db::task_writer::owns_task(&task_id) {
            return self
                .request_task_command(
                    &task_id,
                    "reject_review_as",
                    serde_json::json!([task_id, reason, actor]),
                    false,
                )
                .await;
        }
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if task.status != "review" {
            return Err(ServiceError::invalid_operation(format!(
                "task {task_id} is in {} state; expected review",
                task.status
            )));
        }
        let latest_review = self.latest_review_for_task(&task_id).await?;
        if latest_review.status != ReviewStatus::AwaitingHuman {
            return Err(ServiceError::invalid_operation(
                "latest review is not awaiting_human",
            ));
        }
        // Rejection also preserves the corruption signal; it must not mutate
        // a malformed persisted review into a valid-looking result.
        super::strict_review_details(&latest_review)?;
        let reason = reason
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "manual review rejected".to_owned());
        let finished_at = now_rfc3339();
        let (review, task) = ReviewRepo::update_status_with_task_authority(
            &*self.db,
            &latest_review.id,
            ReviewStatus::Failed,
            latest_review.step_results_json.clone(),
            Some(finished_at.clone()),
            &finished_at,
            task.version,
            None,
            db::ReviewEventOrigin::for_human(&actor),
        )
        .await?;

        if let Err(error) = self
            .memory_service
            .record_review_result_if_final(&task.project_id, &review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }
        self.create_system_comment(
            &task_id,
            format!(
                "Review failed (attempt {}): {}",
                review.attempt_number, reason
            ),
        )
        .await?;
        self.publish(ForgeEvent {
            event_type: "review.rejected".to_owned(),
            entity_id: review.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReviewRejected {
                task_id: task_id.clone(),
                review_id: review.id.clone(),
                reason: reason.clone(),
            },
        });

        self.transition(
            task_id.clone(),
            "in_progress".to_owned(),
            TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                reason: Some(reason.clone()),
                triggered_by: actor,
                rejection: true,
                defer_dispatch_seconds: None,
            },
        )
        .await?;
        let remaining_retries = self.remaining_retries(&task_id).await?;
        let follow_up_already_dispatched = ExecutionRepo::has_active_or_completed_child(
            &*self.db,
            &task_id,
            &review.execution_id,
            crate::workflow::default_roles::CODER,
        )
        .await?;
        if db::budget::allows_retry(i64::from(remaining_retries), 0)
            && !follow_up_already_dispatched
            && !Self::task_action_command_active()
        {
            self.dispatch_follow_up(
                &task_id,
                ::review::ReviewOutcome::AuditorFailed { reason },
                review.execution_id.clone(),
            )
            .await?;
        }
        let latest_task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        Ok((latest_task, review))
    }
}
