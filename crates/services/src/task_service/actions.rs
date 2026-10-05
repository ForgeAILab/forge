use super::*;
use crate::task_actions::{available_actions, TaskSnapshot};
use api_types::{Offer, TaskAction, WorkflowTrigger};

tokio::task_local! { pub(crate) static TASK_ACTION_COMMAND: (); pub(crate) static TASK_ACTION_ACTOR: Actor; }

/// Whether `task` is parked at a review gate only a user may decide.
///
/// The Project Agent's review decision offer and the review-ready attention
/// wake both hinge on this: a review run by the workflow's reviewer Agent
/// settles itself, so neither a decision nor a wake is anyone's business.
#[must_use]
pub fn task_review_requires_user_decision(
    task: &Task,
    workflow: &api_types::WorkflowDefinition,
) -> bool {
    workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .is_some_and(|state| {
            state.canonical_phase == Some(api_types::CanonicalPhase::Review)
                && state
                    .gate_config
                    .as_ref()
                    .is_some_and(|gate| gate.requires_user_approval())
        })
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TaskActionResult {
    pub task: Task,
    pub action: TaskAction,
}

impl TaskService {
    pub(crate) fn task_action_actor(fallback: Actor) -> Actor {
        TASK_ACTION_ACTOR.try_with(Clone::clone).unwrap_or(fallback)
    }
    pub(crate) fn task_action_command_active() -> bool {
        TASK_ACTION_COMMAND.try_with(|_| true).unwrap_or(false)
    }
    pub async fn task_action_snapshot(&self, task_id: &str, actor: &Actor) -> Result<TaskSnapshot> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        self.task_action_snapshot_for_task(task, actor).await
    }

    /// Derive offers for a known committed Task without replacing it with a
    /// newer row while an asynchronous cascade is advancing in the background.
    pub async fn task_action_snapshot_for_task(
        &self,
        task: Task,
        actor: &Actor,
    ) -> Result<TaskSnapshot> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, actor);
        crate::task_actions::load_snapshot(
            &self.db,
            task,
            workflow,
            actor,
            self.daemon_connections.as_deref(),
        )
        .await
    }

    pub async fn task_action_offers(
        &self,
        task_id: &str,
        actor: &Actor,
    ) -> Result<api_types::TaskActionsResponse> {
        let snapshot = self.task_action_snapshot(task_id, actor).await?;
        Ok(api_types::TaskActionsResponse {
            available_actions: available_actions(&snapshot),
            version: snapshot.task.version,
        })
    }

    pub async fn perform_task_action(
        &self,
        task_id: impl Into<String>,
        action: TaskAction,
        version: i64,
    ) -> Result<TaskActionResult> {
        self.perform_task_action_as(task_id, action, version, Actor::user(UserActionSource::Api))
            .await
    }

    pub async fn perform_task_action_as(
        &self,
        task_id: impl Into<String>,
        action: TaskAction,
        version: i64,
        actor: Actor,
    ) -> Result<TaskActionResult> {
        let task_id: String = task_id.into();
        if !db::task_writer::owns_task(&task_id) {
            return self
                .request_task_command(
                    &task_id,
                    "perform_task_action_as",
                    serde_json::json!([task_id, action, version, actor]),
                    matches!(
                        &action,
                        api_types::TaskAction::Cancel { .. } | api_types::TaskAction::Hold { .. }
                    ),
                )
                .await;
        }

        let snapshot = self.task_action_snapshot(&task_id, &actor).await?;
        let preempting_hooks = db::task_writer::current_task_step()
            .and_then(|step| serde_json::from_str::<Value>(&step.payload_json).ok())
            .is_some_and(|payload| payload["preempting_hooks"] == true);
        if matches!(&action, TaskAction::Cancel { .. } | TaskAction::Hold { .. }) {
            TaskRepo::mutate_metadata(
                &*self.db,
                &task_id,
                None,
                vec![db::TaskMetadataMutation::Remove {
                    key: crate::deferred_dispatch::QUEUED_RECOVERY_KEY.to_owned(),
                }],
                &now_rfc3339(),
            )
            .await?;
            let integrated: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND integration_started_at IS NOT NULL AND status='done')").bind(&task_id).fetch_one(self.db.pool()).await?;
            if snapshot.workflow.state_kind(&snapshot.task.status)
                == Some(api_types::StateKind::Terminal)
                && snapshot.workflow.cancellation_state.as_deref()
                    != Some(snapshot.task.status.as_str())
                && (preempting_hooks || integrated)
            {
                // The protected merge landed before this queued command ran.
                // Record that the request had no effect on the done Task.
                self.create_system_comment(
                    &task_id,
                    format!(
                        "{} had no effect: the merge landed first and the Task is {}.",
                        if matches!(&action, TaskAction::Cancel { .. }) {
                            "Cancel"
                        } else {
                            "Hold"
                        },
                        snapshot.task.status
                    ),
                )
                .await?;
                return Ok(TaskActionResult {
                    task: snapshot.task,
                    action,
                });
            }
        }
        if let TaskAction::Hold { reason } = &action {
            if actor.is_user() && preempting_hooks {
                for execution in snapshot.executions.iter().filter(|execution| {
                    execution.status == ExecutionStatus::Running && execution.role != "interactive"
                }) {
                    self.pause_execution(
                        execution.id.clone(),
                        reason.clone().unwrap_or_else(|| "held by owner".to_owned()),
                    )
                    .await?;
                }
                let fresh = self.task_action_snapshot(&task_id, &actor).await?;
                let held = self.hold_waiting_task(&fresh, reason.as_deref()).await?;
                let held = TaskRepo::set_entry_barrier(
                    &*self.db,
                    &held.id,
                    held.version,
                    None,
                    &now_rfc3339(),
                )
                .await?;
                return Ok(TaskActionResult { task: held, action });
            }
        }
        if snapshot.task.version != version {
            return Err(DbError::TaskVersionConflict {
                expected: version,
                actual: snapshot.task.version,
            }
            .into());
        }
        let offers = available_actions(&snapshot);
        let selected = offers.iter().find(|offer| {
            offer.action.verb() == action.verb()
                && action_parameters_allowed(offer, &action, actor.is_user())
        });
        let offer = selected
            .cloned()
            .ok_or_else(|| ServiceError::TaskActionUnavailable {
                available_actions: offers,
                reason: format!("action '{}' is unavailable", action.verb()),
                wait_cause: matches!(
                    action,
                    TaskAction::Start | TaskAction::Retry { .. } | TaskAction::Release { .. }
                )
                .then(|| snapshot.wait_cause.clone())
                .flatten(),
            })?;
        let action = apply_offered_parameters(&offer, action);
        let fencing_machines = if matches!(
            &action,
            TaskAction::Restart { .. } | TaskAction::Retry { .. } | TaskAction::Release { .. }
        ) {
            self.db
                .task_pending_remote_cancel_machines(&task_id)
                .await?
        } else {
            Vec::new()
        };
        if !fencing_machines.is_empty() {
            let queued = self
                .queue_task_action(&snapshot, offer, action.clone(), actor)
                .await?;
            // Name the machine so the owner knows which one to bring back.
            let machines = fencing_machines.join(", ");
            let message = format!(
                "Waiting for machine {machines} to confirm its remote work has stopped. Reconnect that machine to finish cleanup."
            );
            let parked=TaskRepo::update(&*self.db,db::UpdateTask {
                id:queued.id.clone(),expected_version:queued.version,title:None,description:None,priority:None,merge_config:None,plan:None,
                error_annotation:Some(Some(json!({"type":api_types::FailureKind::WorkspaceResetRequired,"blocking_reason":"pending_remote_cancel","blocked_by":format!("machine:{machines}"),"message":message}).to_string())),
                blocked_json:None,failed_json:None,task_state_config:None,parent_task_id:None,updated_at:now_rfc3339(),
            }).await?;
            return Ok(TaskActionResult {
                task: parked,
                action,
            });
        }
        if let TaskAction::SendBack { guidance } = &action {
            validate_required("guidance", guidance)?;
        }
        let reason = match &action {
            TaskAction::Approve { reason, .. }
            | TaskAction::Cancel { reason }
            | TaskAction::Retry { reason, .. }
            | TaskAction::Hold { reason }
            | TaskAction::Release { reason }
            | TaskAction::Restart { reason } => reason.as_deref(),
            _ => None,
        };
        if offer
            .parameters
            .iter()
            .any(|parameter| parameter.name == "reason" && parameter.required)
            || matches!(
                &action,
                TaskAction::Approve {
                    override_checks: Some(true),
                    ..
                } | TaskAction::Retry {
                    reset_budget: Some(false),
                    ..
                }
            )
        {
            validate_required("reason", reason.unwrap_or_default())?;
        } else if let Some(reason) = reason {
            validate_required("reason", reason)?;
        }
        if snapshot.task.status == "review"
            && matches!(
                action,
                TaskAction::Approve { .. } | TaskAction::SendBack { .. }
            )
        {
            if let Some(review) = snapshot.latest_review.as_ref() {
                super::strict_review_details(review)?;
            }
        }
        let actor = match actor {
            Actor::User { user_id, .. } => Actor::User {
                user_id,
                source: UserActionSource::Action(action.clone()),
            },
            actor => actor,
        };
        let task = snapshot.task.clone();
        // A completion that settled while the Task was held was a no-op
        // (a held Task does not advance). Release settles it instead of
        // re-running the role, so the completion is never lost.
        let held_completion =
            if matches!(&action, TaskAction::Release { .. }) && offer.reason == "manually_held" {
                self.held_completion_pending(&task, offer.target_execution_id.as_deref())
                    .await?
            } else {
                None
            };
        let updated = TASK_ACTION_ACTOR
            .scope(
                actor.clone(),
                Box::pin(async {
                    Ok::<Task, ServiceError>(match &action {
                        TaskAction::Retry { reason, .. } if offer.reason == "placement_retry" => {
                            self.retry_recorded_placement(&snapshot, reason.as_deref())
                                .await?
                        }
                        TaskAction::Retry { .. } if offer.reason == "owner_reconcile" => {
                            self.reconcile_task_action_owner(task, action.clone())
                                .await?
                        }
                        TaskAction::Restart { reason } => {
                            let annotation = snapshot.annotation().unwrap_or_else(|| {
                                api_types::TaskBlockingAnnotation {
                                    annotation_type: snapshot
                                        .condition()
                                        .unwrap_or(api_types::FailureKind::ExecutorFailed),
                                    blocking_reason: "restart".to_owned(),
                                    blocked_by: None,
                                    blocked_at: None,
                                    blocked_execution_id: None,
                                    artifact: None,
                                    message: None,
                                    hook: None,
                                }
                            });
                            TASK_ACTION_COMMAND
                                .scope(
                                    (),
                                    Box::pin(self.restart_task_condition(
                                        task,
                                        &annotation,
                                        reason.clone(),
                                    )),
                                )
                                .await?
                        }
                        TaskAction::Hold { reason } if offer.reason == "dispatch_wait" => {
                            self.hold_waiting_task(&snapshot, reason.as_deref()).await?
                        }
                        TaskAction::Release { reason } if offer.reason == "held_waiting" => {
                            self.release_to_dispatch_queue(&snapshot, reason.as_deref())
                                .await?
                        }
                        TaskAction::Release { reason } if held_completion.is_some() => {
                            let execution_id = held_completion.as_deref().expect("held completion");
                            self.release_to_dispatch_queue(&snapshot, reason.as_deref())
                                .await?;
                            Box::pin(self.maybe_cascade_executor_completion(execution_id)).await?;
                            TaskRepo::get_by_id(&*self.db, &task.id, false)
                                .await?
                                .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?
                        }
                        TaskAction::Hold { reason } => {
                            TaskRepo::mutate_metadata_and_bump_version(
                                &*self.db,
                                &task.id,
                                task.version,
                                Vec::new(),
                                &now_rfc3339(),
                            )
                            .await?;
                            for execution in snapshot.executions.iter().filter(|execution| {
                                execution.status == ExecutionStatus::Running
                                    && execution.role != "interactive"
                            }) {
                                self.pause_execution(
                                    execution.id.clone(),
                                    reason.clone().unwrap_or_else(|| "held by owner".to_owned()),
                                )
                                .await?;
                            }
                            if offer.reason == "execution_running" {
                                self.create_system_comment(
                                    &task.id,
                                    reason
                                        .as_ref()
                                        .map(|reason| format!("Task paused by user: {reason}"))
                                        .unwrap_or_else(|| "Task paused by user".to_owned()),
                                )
                                .await?;
                            }
                            TaskRepo::get_by_id(&*self.db, &task.id, false)
                                .await?
                                .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?
                        }
                        TaskAction::SendBack { guidance } => {
                            let task = TASK_ACTION_COMMAND
                                .scope(
                                    (),
                                    Box::pin(self.apply_gate_decision(
                                        &task,
                                        &snapshot.workflow,
                                        WorkflowTrigger::Reject,
                                        Some(guidance.clone()),
                                        actor.clone(),
                                        offer.reason == "human_review_send_back",
                                    )),
                                )
                                .await?;
                            self.queue_deferred_action_role(
                                task,
                                offer,
                                action.clone(),
                                actor.clone(),
                            )
                            .await?
                        }
                        TaskAction::Approve {
                            reason,
                            override_checks: Some(true),
                        } if matches!(
                            offer.reason.as_str(),
                            "state_advance_override"
                                | "gate_waiting_for_decision"
                                | "human_review_decision"
                                | "work_ready_to_submit"
                        ) =>
                        {
                            let target = snapshot
                                .advance_target
                                .clone()
                                .expect("offer pins a next-state plan");
                            let task = TASK_ACTION_COMMAND
                                .scope(
                                    (),
                                    Box::pin(self.advance_task_condition(
                                        &task,
                                        &snapshot.workflow,
                                        target,
                                        reason.clone().expect("validated required reason"),
                                        actor.clone(),
                                    )),
                                )
                                .await?;
                            self.queue_deferred_action_role(
                                task,
                                offer,
                                action.clone(),
                                actor.clone(),
                            )
                            .await?
                        }
                        TaskAction::Approve { reason, .. }
                            if matches!(
                                offer.reason.as_str(),
                                "review_needs_owner"
                                    | "reviewer_failed_manual_pass"
                                    | "failed_review_override"
                            ) =>
                        {
                            let annotation = snapshot.annotation();
                            let finding = (offer.reason == "review_needs_owner"
                                && matches!(
                                    action,
                                    TaskAction::Approve {
                                        override_checks: Some(false),
                                        ..
                                    }
                                ))
                            .then_some(annotation.as_ref())
                            .flatten();
                            let task = TASK_ACTION_COMMAND
                                .scope(
                                    (),
                                    Box::pin(self.recover_manual_review_pass(
                                        task,
                                        reason.clone().unwrap_or_default(),
                                        finding,
                                        action.clone(),
                                    )),
                                )
                                .await?;
                            self.queue_deferred_action_role(
                                task,
                                offer,
                                action.clone(),
                                actor.clone(),
                            )
                            .await?
                        }
                        TaskAction::Approve {
                            reason,
                            override_checks: Some(false),
                        } if matches!(
                            offer.reason.as_str(),
                            "gate_waiting_for_decision"
                                | "human_review_decision"
                                | "work_ready_to_submit"
                        ) =>
                        {
                            let task = TASK_ACTION_COMMAND
                                .scope(
                                    (),
                                    Box::pin(self.apply_gate_decision(
                                        &task,
                                        &snapshot.workflow,
                                        WorkflowTrigger::Accept,
                                        reason.clone(),
                                        actor.clone(),
                                        offer.reason == "human_review_decision",
                                    )),
                                )
                                .await?;
                            self.queue_deferred_action_role(
                                task,
                                offer,
                                action.clone(),
                                actor.clone(),
                            )
                            .await?
                        }
                        TaskAction::Retry {
                            guidance, reason, ..
                        } if offer.reason == "retry_budget_exhausted" => {
                            let reset_budget = !matches!(
                                action,
                                TaskAction::Retry {
                                    reset_budget: Some(false),
                                    ..
                                }
                            );
                            let updated = if reset_budget {
                                TASK_ACTION_COMMAND
                                    .scope(
                                        (),
                                        Box::pin(
                                            self.reset_task_retry_budget(task, reason.clone()),
                                        ),
                                    )
                                    .await?
                            } else {
                                TASK_ACTION_COMMAND
                                    .scope(
                                        (),
                                        Box::pin(self.permit_one_task_retry(
                                            task,
                                            reason.clone(),
                                            guidance.clone(),
                                        )),
                                    )
                                    .await?
                            };
                            if reset_budget
                                && snapshot.task.status == "review"
                                && snapshot.latest_review.as_ref().is_some_and(|review| {
                                    review.status == ReviewStatus::AwaitingHuman
                                })
                            {
                                return Ok(updated);
                            }
                            let staged = self
                                .queue_deferred_action_role(
                                    updated,
                                    offer.clone(),
                                    action.clone(),
                                    actor.clone(),
                                )
                                .await?;
                            if crate::deferred_dispatch::queued_recovery(&staged).is_some() {
                                staged
                            } else {
                                let snapshot =
                                    self.task_action_snapshot(&staged.id, &actor).await?;
                                if !snapshot.has_agent
                                    || snapshot
                                        .workflow
                                        .states
                                        .iter()
                                        .find(|state| state.name == snapshot.task.status)
                                        .and_then(crate::workflow::effective_role)
                                        .is_none()
                                {
                                    return Ok(staged);
                                }
                                let mut selection = snapshot.clone();
                                selection.caller = crate::ActionCaller::owner();
                                // This action's own entry hooks are queued;
                                // they fence readiness, not the continuation.
                                selection.entry_hooks_running = false;
                                let mut work = offer;
                                work.reason = "role_retry".to_owned();
                                work.target_execution_id = crate::available_actions(&selection)
                                    .into_iter()
                                    .find(|offer| offer.action.verb() == "retry")
                                    .and_then(|offer| offer.target_execution_id);
                                self.queue_task_action(
                                    &snapshot,
                                    work,
                                    action.clone(),
                                    actor.clone(),
                                )
                                .await?
                            }
                        }
                        TaskAction::Retry {
                            guidance,
                            reason,
                            reset_budget,
                            ..
                        } if offer.reason == "execution_retry_exhausted" => {
                            let cleared = if *reset_budget == Some(false) {
                                TASK_ACTION_COMMAND
                                    .scope(
                                        (),
                                        Box::pin(self.permit_one_task_retry(
                                            task,
                                            reason.clone(),
                                            guidance.clone(),
                                        )),
                                    )
                                    .await?
                            } else {
                                let mutations =
                                    super::execution::execution_retry_clear_mutations(&task, true)?;
                                TaskRepo::mutate_metadata_and_bump_version(
                                    &*self.db,
                                    &task.id,
                                    task.version,
                                    mutations,
                                    &now_rfc3339(),
                                )
                                .await?
                            };
                            let mut snapshot = snapshot;
                            snapshot.task = cleared;
                            let mut work = offer;
                            work.reason = "role_retry".to_owned();
                            self.queue_task_action(&snapshot, work, action.clone(), actor.clone())
                                .await?
                        }
                        TaskAction::Cancel { reason } => {
                            self.cancel_task_at_version_as(
                                task.id.clone(),
                                version,
                                reason
                                    .clone()
                                    .unwrap_or_else(|| "task action: cancel".to_owned()),
                                actor.clone(),
                            )
                            .await?
                        }
                        _ => {
                            self.queue_task_action(&snapshot, offer, action.clone(), actor.clone())
                                .await?
                        }
                    })
                }),
            )
            .await?;
        // The Task version/metadata write commits before the in-process wake.
        self.dispatch_wake.notify_one();
        Ok(TaskActionResult {
            task: updated,
            action,
        })
    }

    fn workflow_execution_roles(snapshot: &TaskSnapshot) -> Vec<String> {
        let mut roles = snapshot
            .workflow
            .states
            .iter()
            .filter_map(crate::workflow::effective_role)
            .filter(|role| *role != "interactive")
            .map(str::to_owned)
            .collect::<Vec<_>>();
        for role in [
            "coder", "planner", "reviewer", "auditor", "worker", "assignee", "executor",
        ] {
            if !roles.iter().any(|candidate| candidate == role) {
                roles.push(role.to_owned());
            }
        }
        roles
    }

    async fn hold_waiting_task(
        &self,
        snapshot: &TaskSnapshot,
        reason: Option<&str>,
    ) -> Result<Task> {
        let task = &snapshot.task;
        let reason = reason.unwrap_or("held by owner");
        let now = now_rfc3339();
        let held = TaskRepo::update_recovery_metadata_if_no_running_execution(
            &*self.db,
            &task.id,
            task.version,
            Some(
                json!({"type":"manual_stop", "blocking_reason":reason, "blocked_by":"user",
                "blocked_at":now, "blocked_execution_id":null})
                .to_string(),
            ),
            Some(json!({"kind":"manual_stop", "reason":reason, "created_at":now}).to_string()),
            None,
            &now,
            None,
            Self::workflow_execution_roles(snapshot),
            [
                "queued_recovery",
                "deferred_dispatch",
                "dispatch_disposition",
                "environment_wait",
                "owner_wait",
            ]
            .into_iter()
            .map(|key| db::TaskMetadataMutation::Remove {
                key: key.to_owned(),
            })
            .collect(),
        )
        .await?;
        self.create_system_comment(&task.id, format!("Task paused by user: {reason}"))
            .await?;
        Ok(held)
    }

    /// Clear an owner's hold on a Task that no Agent can take yet. The Task
    /// goes back to waiting for dispatch; nothing is launched here.
    /// The released role attempt already completed but its completion has
    /// not settled for the current status entry (the Hold made it a no-op).
    async fn held_completion_pending(
        &self,
        task: &Task,
        execution_id: Option<&str>,
    ) -> Result<Option<String>> {
        let Some(execution_id) = execution_id else {
            return Ok(None);
        };
        let Some(execution) = ExecutionRepo::get_by_id(&*self.db, execution_id).await? else {
            return Ok(None);
        };
        if execution.task_id != task.id
            || execution.status != ExecutionStatus::Completed
            || super::execution::execution_completion_settled_for_current_state_entry(
                &self.db, task, &execution,
            )
            .await?
        {
            return Ok(None);
        }
        Ok(Some(execution.id))
    }

    async fn release_to_dispatch_queue(
        &self,
        snapshot: &TaskSnapshot,
        reason: Option<&str>,
    ) -> Result<Task> {
        let task = &snapshot.task;
        let released = TaskRepo::update_recovery_metadata_if_no_running_execution(
            &*self.db,
            &task.id,
            task.version,
            None,
            None,
            None,
            &now_rfc3339(),
            None,
            Self::workflow_execution_roles(snapshot),
            Vec::new(),
        )
        .await?;
        self.create_system_comment(
            &task.id,
            reason
                .map(|reason| format!("Task released by user: {reason}"))
                .unwrap_or_else(|| "Task released by user".to_owned()),
        )
        .await?;
        Ok(released)
    }

    async fn retry_recorded_placement(
        &self,
        snapshot: &TaskSnapshot,
        reason: Option<&str>,
    ) -> Result<Task> {
        let task = &snapshot.task;
        let metadata = db::TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let mut mutations = Vec::new();
        for key in ["placement_refusal", "dispatch_disposition"] {
            if let Some(value) = metadata.extra.get(key) {
                mutations.push(db::TaskMetadataMutation::RemoveIf {
                    key: key.to_owned(),
                    expected: value.clone(),
                });
            }
        }
        let retried = TaskRepo::update_recovery_metadata_if_no_running_execution(
            &*self.db,
            &task.id,
            task.version,
            None,
            task.blocked_json.clone(),
            task.failed_json.clone(),
            &now_rfc3339(),
            None,
            Self::workflow_execution_roles(snapshot),
            mutations,
        )
        .await?;
        self.create_system_comment(
            &task.id,
            reason
                .map(|reason| format!("Placement retry requested: {reason}"))
                .unwrap_or_else(|| "Placement retry requested".to_owned()),
        )
        .await?;
        self.publish_recovery_applied(&retried, "retry", Some(&retried.status), None);
        Ok(retried)
    }

    pub(crate) async fn apply_gate_decision(
        &self,
        task: &Task,
        workflow: &api_types::WorkflowDefinition,
        trigger: WorkflowTrigger,
        guidance: Option<String>,
        actor: Actor,
        human_review: bool,
    ) -> Result<Task> {
        if human_review {
            return match trigger {
                WorkflowTrigger::Accept => {
                    Ok(self.approve_review_as(task.id.clone(), actor).await?.0)
                }
                _ => Ok(self
                    .reject_review_as(task.id.clone(), guidance, actor)
                    .await?
                    .0),
            };
        }
        let target = workflow
            .outgoing_trigger_targets(&task.status)
            .find(|(candidate, _)| *candidate == trigger)
            .map(|(_, target)| target)
            .expect("offer guarantees a workflow target");
        Ok(self
            .transition(
                task.id.clone(),
                target,
                TransitionOptions {
                    version: task.version,
                    reason: Some(match trigger {
                        WorkflowTrigger::Accept => guidance
                            .clone()
                            .unwrap_or_else(|| "gate approved".to_owned()),
                        _ => guidance.unwrap_or_default(),
                    }),
                    triggered_by: actor,
                    rejection: trigger == WorkflowTrigger::Reject,
                    defer_dispatch_seconds: None,
                },
            )
            .await?
            .task)
    }

    pub(crate) async fn apply_review_check_retry(
        &self,
        task: &Task,
        offer: Offer,
        action: TaskAction,
        actor: Actor,
    ) -> Result<Task> {
        let id = Uuid::parse_str(&task.id)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let (updated, _) = TASK_ACTION_COMMAND
            .scope((), Box::pin(self.rerun_review(id)))
            .await?;
        self.queue_deferred_action_role(updated, offer, action, actor)
            .await
    }

    pub(crate) async fn queue_deferred_action_role(
        &self,
        task: Task,
        mut offer: Offer,
        action: TaskAction,
        actor: Actor,
    ) -> Result<Task> {
        if crate::deferred_dispatch::pending_until(&task).is_none() {
            return Ok(task);
        }
        let snapshot = self.task_action_snapshot(&task.id, &actor).await?;
        let role = snapshot
            .workflow
            .states
            .iter()
            .find(|state| state.name == snapshot.task.status)
            .and_then(crate::workflow::effective_role);
        let agent_owned = role.is_some_and(|role| {
            snapshot.role_assignments.iter().any(|assignment| {
                assignment.role_name == role
                    && assignment.assignee_type == Some(AssigneeKind::Agent)
                    && assignment.assignee_id.is_some()
            })
        });
        if !agent_owned || !snapshot.has_agent {
            return Ok(snapshot.task);
        }
        let mut selection = snapshot.clone();
        selection.caller = crate::ActionCaller::owner();
        // The accepted intent and this action's own queued entry hooks are
        // scheduling fences, not conditions on the next role. Derive its
        // continuation target from the same pure resolver.
        selection.entry_hooks_running = false;
        if let Some(raw) = selection.task.metadata_json.as_deref() {
            if let Ok(mut metadata) = serde_json::from_str::<Value>(raw) {
                if let Some(object) = metadata.as_object_mut() {
                    object.remove(crate::deferred_dispatch::QUEUED_RECOVERY_KEY);
                }
                selection.task.metadata_json = Some(metadata.to_string());
            }
        }
        offer.target_execution_id = crate::available_actions(&selection)
            .into_iter()
            .find(|offer| offer.action.verb() == "retry")
            .and_then(|offer| offer.target_execution_id);
        offer.reason = "role_retry".to_owned();
        self.queue_task_action(&snapshot, offer, action, actor)
            .await
    }

    pub(crate) async fn queue_task_action(
        &self,
        snapshot: &TaskSnapshot,
        offer: Offer,
        action: TaskAction,
        actor: Actor,
    ) -> Result<Task> {
        let task = &snapshot.task;
        // A replay fallback retains its accepted authority and intent, even
        // when admission loses the race after an advisory capacity read.
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", &task.id))?;
        if Self::is_replaying_recovery(&task.id)
            && crate::deferred_dispatch::queued_recovery(&current)
                .is_some_and(|existing| existing.target_state == current.status)
        {
            return Ok(current);
        }
        let agent_id = if matches!(
            offer.reason.as_str(),
            "ready_to_start"
                | "initial_retry"
                | "role_retry"
                | "manually_held"
                | "merge_fix_retry"
                | "review_failed"
                | "entry_barrier_blocked"
        ) {
            Some(snapshot.action_agent_id.clone().ok_or_else(|| {
                ServiceError::invalid_operation("accepted role action lost its Agent")
            })?)
        } else {
            None
        };
        let saved_annotation = task.error_annotation.clone().or_else(|| {
            task.failed_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                .and_then(|failure| {
                    let kind = failure
                        .get("kind")
                        .cloned()
                        .and_then(|kind| {
                            serde_json::from_value::<api_types::FailureKind>(kind).ok()
                        })
                        .unwrap_or(api_types::FailureKind::ExecutorFailed);
                    serde_json::to_string(&api_types::TaskBlockingAnnotation {
                        annotation_type: kind,
                        blocking_reason: failure
                            .get("reason")
                            .and_then(Value::as_str)
                            .unwrap_or("failed Task")
                            .to_owned(),
                        blocked_by: None,
                        blocked_at: None,
                        blocked_execution_id: None,
                        artifact: None,
                        message: None,
                        hook: None,
                    })
                    .ok()
                })
        });
        let role_name = snapshot
            .workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(crate::workflow::effective_role)
            .map(str::to_owned)
            .or_else(|| {
                snapshot
                    .workflow
                    .outgoing_trigger_targets(&task.status)
                    .find_map(|(_, target)| {
                        snapshot
                            .workflow
                            .states
                            .iter()
                            .find(|state| state.name == target)
                            .and_then(crate::workflow::effective_role)
                            .map(str::to_owned)
                    })
            });
        let assignment_id = role_name.as_deref().and_then(|role| {
            snapshot
                .role_assignments
                .iter()
                .find(|assignment| assignment.role_name == role)
                .map(|assignment| assignment.id.clone())
        });
        let queued = crate::deferred_dispatch::QueuedRecovery {
            id: new_uuid_v4(),
            request: crate::deferred_dispatch::QueuedTaskAction {
                action,
                offer,
                actor,
                agent_id,
                role_name,
                assignment_id,
            },
            target_state: task.status.clone(),
            error_annotation: saved_annotation,
            blocked_json: task.blocked_json.clone(),
            failed_json: task.failed_json.clone(),
        };
        if let Some(existing) = crate::deferred_dispatch::queued_recovery(&current) {
            if existing.target_state == queued.target_state
                && serde_json::to_value(&existing.request)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
                    == serde_json::to_value(&queued.request)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
            {
                return Ok(current);
            }
        }
        let machine_wait = if let Some(agent_id) = queued.request.agent_id.as_deref() {
            let agent = db::AgentRepo::get_by_id(&*self.db, agent_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("agent", agent_id))?;
            self.machine_capacity_blocked(task, &agent, queued.request.role_name.as_deref())
                .await?
        } else {
            false
        };
        let now = now_rfc3339();
        let repository_roles = Self::workflow_execution_roles(snapshot);
        let updated = TaskRepo::update_recovery_metadata_if_no_running_execution(&*self.db, &task.id, task.version, None, None, None, &now, None, repository_roles, vec![
            db::TaskMetadataMutation::Set { key: crate::deferred_dispatch::QUEUED_RECOVERY_KEY.to_owned(), value: serde_json::to_value(&queued).map_err(|error| ServiceError::invalid_operation(error.to_string()))? },
            db::TaskMetadataMutation::Set { key: "deferred_dispatch".to_owned(), value: json!({ "not_before": now, "reason": "task action queued", "target_state": task.status }) },
        ]).await?;
        let updated = if machine_wait {
            crate::deferred_dispatch::record_dispatch_disposition(
                &self.db,
                &updated,
                "machine_capacity",
                "machine_capacity: waiting for a machine run slot",
            )
            .await?;
            TaskRepo::get_by_id(&*self.db, &updated.id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", &updated.id))?
        } else {
            updated
        };
        if task.blocked_json.is_some() {
            self.publish(ForgeEvent {
                event_type: "task.unblocked".to_owned(),
                entity_id: task.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskUnblocked {
                    project_id: task.project_id.clone(),
                    previous_reason: task
                        .blocked_json
                        .as_deref()
                        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                        .and_then(|value| {
                            value
                                .get("reason")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                        }),
                },
            });
        }
        self.publish(ForgeEvent {
            event_type: "task.action".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::RecoveryApplied {
                task_id: task.id.clone(),
                project_id: task.project_id.clone(),
                action: queued.request.action.verb().to_owned(),
                state: Some(task.status.clone()),
                transition_log_id: None,
            },
        });
        self.dispatch_wake.notify_one();
        Ok(updated)
    }

    pub async fn perform_project_agent_cancel(
        &self,
        project_id: &str,
        task_id: impl Into<String>,
        reason: String,
        expected_task_version: i64,
        identity: &str,
    ) -> Result<TaskActionResult> {
        let id = task_id.into();
        self.ensure_task_in_project(&id, project_id).await?;
        self.perform_task_action_as(
            id,
            TaskAction::Cancel {
                reason: Some(reason),
            },
            expected_task_version,
            Actor::agent(identity),
        )
        .await
    }

    pub async fn perform_project_agent_review(
        &self,
        project_id: &str,
        task_id: impl Into<String>,
        accept: bool,
        reason: Option<String>,
        version: i64,
        identity: &str,
    ) -> Result<TaskActionResult> {
        let id = task_id.into();
        self.ensure_task_in_project(&id, project_id).await?;
        self.perform_task_action_as(
            id,
            if accept {
                TaskAction::Approve {
                    reason,
                    override_checks: Some(false),
                }
            } else {
                TaskAction::SendBack {
                    guidance: reason.unwrap_or_default(),
                }
            },
            version,
            Actor::agent(identity),
        )
        .await
    }

    async fn ensure_task_in_project(&self, id: &str, project_id: &str) -> Result<()> {
        TaskRepo::get_by_id(&*self.db, id, false)
            .await?
            .filter(|task| task.project_id == project_id)
            .ok_or_else(|| ServiceError::not_found("task", id.to_owned()))?;
        Ok(())
    }
}

fn action_parameters_allowed(offer: &Offer, action: &TaskAction, owner: bool) -> bool {
    let supports = |name: &str| {
        offer
            .parameters
            .iter()
            .any(|parameter| parameter.name == name)
    };
    let allows_bool = |name: &str, value: bool| {
        offer
            .parameters
            .iter()
            .find(|parameter| parameter.name == name)
            .and_then(|parameter| parameter.boolean_values.as_ref())
            .is_some_and(|values| values.contains(&value))
    };
    match action {
        TaskAction::Approve {
            override_checks, ..
        } => {
            (override_checks != &Some(true) || owner)
                && (matches!(&offer.action, TaskAction::Approve { override_checks: offered, .. } if offered == override_checks)
                    || override_checks.is_none()
                    || override_checks.is_some_and(|value| allows_bool("override", value)))
        }
        TaskAction::Retry {
            reason: _,
            fresh_session,
            refresh_workspace,
            reset_budget,
            guidance,
        } => {
            let TaskAction::Retry {
                fresh_session: offered_fresh,
                refresh_workspace: offered_refresh,
                reset_budget: offered_reset,
                ..
            } = &offer.action
            else {
                return false;
            };
            let allowed = |name: &str, supplied: Option<bool>, preset: Option<bool>| {
                supplied.is_none_or(|value| {
                    if supports(name) {
                        allows_bool(name, value)
                    } else {
                        preset == Some(value)
                    }
                })
            };
            allowed("fresh_session", *fresh_session, *offered_fresh)
                && allowed("refresh_workspace", *refresh_workspace, *offered_refresh)
                && allowed("reset_budget", *reset_budget, *offered_reset)
                && (guidance.is_none() || supports("guidance"))
        }
        TaskAction::SendBack { .. } => true,
        _ => true,
    }
}

fn apply_offered_parameters(offer: &Offer, mut action: TaskAction) -> TaskAction {
    if let (
        TaskAction::Approve {
            override_checks, ..
        },
        TaskAction::Approve {
            override_checks: preset,
            ..
        },
    ) = (&mut action, &offer.action)
    {
        if override_checks.is_none() {
            *override_checks = offer
                .parameters
                .iter()
                .find(|parameter| parameter.name == "override")
                .and_then(|parameter| parameter.boolean_values.as_ref())
                .filter(|values| values.len() == 1)
                .map(|values| values[0])
                .or(*preset);
        }
    }
    if let (
        TaskAction::Retry {
            fresh_session,
            refresh_workspace,
            reset_budget,
            ..
        },
        TaskAction::Retry {
            fresh_session: offered_fresh,
            refresh_workspace: offered_refresh,
            reset_budget: offered_reset,
            ..
        },
    ) = (&mut action, &offer.action)
    {
        for (name, requested, preset) in [
            ("fresh_session", fresh_session, offered_fresh),
            ("refresh_workspace", refresh_workspace, offered_refresh),
            ("reset_budget", reset_budget, offered_reset),
        ] {
            if requested.is_none() {
                *requested = offer
                    .parameters
                    .iter()
                    .find(|parameter| parameter.name == name)
                    .and_then(|parameter| parameter.boolean_values.as_ref())
                    .filter(|values| values.len() == 1)
                    .map(|values| values[0])
                    .or(*preset);
            }
        }
    }
    action
}

// Fixture drivers preserve the tests' synchronous lifecycle assertions while
// exercising the actual command commit and then the dispatcher separately.
#[cfg(any(test, feature = "test-support"))]
impl TaskService {
    pub async fn test_apply_intent(
        &self,
        task_id: impl Into<String>,
        action: TaskAction,
        reason: Option<String>,
        version: Option<i64>,
    ) -> Result<TaskActionResult> {
        let id = task_id.into();
        let task = TaskRepo::get_by_id(&*self.db, &id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", id.clone()))?;
        let action = match action {
            TaskAction::SendBack { guidance } if guidance.is_empty() => TaskAction::SendBack {
                guidance: reason.unwrap_or_default(),
            },
            TaskAction::Approve {
                override_checks,
                reason: current,
            } => TaskAction::Approve {
                override_checks,
                reason: reason.or(current),
            },
            TaskAction::Cancel { reason: current } => TaskAction::Cancel {
                reason: reason.or(current),
            },
            TaskAction::Hold { reason: current } => TaskAction::Hold {
                reason: reason.or(current),
            },
            TaskAction::Release { reason: current } => TaskAction::Release {
                reason: reason.or(current),
            },
            TaskAction::Restart { reason: current } => TaskAction::Restart {
                reason: reason.or(current),
            },
            TaskAction::Retry {
                fresh_session,
                refresh_workspace,
                reset_budget,
                guidance,
                reason: current,
            } => TaskAction::Retry {
                fresh_session,
                refresh_workspace,
                reset_budget,
                guidance,
                reason: reason.or(current),
            },
            value => value,
        };
        let result = self
            .perform_task_action(id.clone(), action, version.unwrap_or(task.version))
            .await?;
        let task = TaskRepo::get_by_id(&*self.db, &id, false)
            .await?
            .expect("Task remains present");
        self.dispatch_queued_recovery(&task).await?;
        Ok(TaskActionResult {
            task: TaskRepo::get_by_id(&*self.db, &id, false)
                .await?
                .expect("Task remains present"),
            action: result.action,
        })
    }
    pub async fn test_apply_action(
        &self,
        task_id: impl Into<String>,
        action: TaskAction,
        reason: Option<String>,
        context: Option<String>,
    ) -> Result<Task> {
        let action = match action {
            TaskAction::Retry {
                reason,
                fresh_session,
                refresh_workspace,
                reset_budget,
                guidance,
            } => TaskAction::Retry {
                reason,
                fresh_session,
                refresh_workspace,
                reset_budget,
                guidance: context.or(guidance),
            },
            value => value,
        };
        Ok(self
            .test_apply_intent(task_id, action, reason, None)
            .await?
            .task)
    }
    pub async fn test_apply_action_at_version(
        &self,
        task_id: impl Into<String>,
        action: TaskAction,
        reason: Option<String>,
        context: Option<String>,
        version: i64,
    ) -> Result<Task> {
        let action = match action {
            TaskAction::Retry {
                reason,
                fresh_session,
                refresh_workspace,
                reset_budget,
                guidance,
            } => TaskAction::Retry {
                reason,
                fresh_session,
                refresh_workspace,
                reset_budget,
                guidance: context.or(guidance),
            },
            value => value,
        };
        Ok(self
            .test_apply_intent(task_id, action, reason, Some(version))
            .await?
            .task)
    }
    pub async fn test_dispatch_task_action(&self, task_id: &str) -> Result<bool> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        self.dispatch_queued_recovery(&task).await
    }
    pub async fn test_launch_side_session(
        &self,
        task_id: impl Into<String>,
        reason: Option<String>,
        context: Option<String>,
    ) -> Result<Task> {
        let id = task_id.into();
        let snapshot = self
            .task_action_snapshot(&id, &Actor::user(UserActionSource::Test))
            .await?;
        let role = snapshot
            .workflow
            .states
            .iter()
            .find(|state| state.name == snapshot.task.status)
            .and_then(crate::workflow::effective_role);
        let blocked_id = snapshot
            .annotation()
            .and_then(|annotation| annotation.blocked_execution_id);
        let target = crate::task_service::action_resolver::select_open_interactive_target(
            &snapshot.executions,
            role,
            blocked_id.as_deref(),
        );
        let message = context
            .or(reason)
            .unwrap_or_else(|| "Continue side session.".to_owned());
        let launched = if let Some(execution) = target {
            self.follow_up_interactive_execution(
                execution.id.clone(),
                message,
                execution.agent_id.clone(),
                None,
            )
            .await?
        } else {
            self.launch_execution(
                &id,
                snapshot.action_agent_id.clone().ok_or_else(|| {
                    ServiceError::invalid_operation("no available agent for side session")
                })?,
                Some(message),
                None,
            )
            .await?
        };
        self.start_execution(launched.execution.id.clone()).await?;
        Ok(launched.task)
    }
    pub async fn test_action_values(&self, task_id: impl Into<String>) -> Result<Vec<TaskAction>> {
        Ok(self
            .task_action_offers(&task_id.into(), &Actor::user(UserActionSource::Test))
            .await?
            .available_actions
            .into_iter()
            .map(|offer| offer.action)
            .collect())
    }
}

#[cfg(test)]
mod parameter_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn fixed_and_preset_parameters_default_and_reject_contradictions() {
        for (preset, parameters, supplied, normalized, contrary) in [
            (
                json!({"verb":"retry","refresh_workspace":true}),
                json!([]),
                json!({"verb":"retry"}),
                json!({"verb":"retry","refresh_workspace":true}),
                json!({"verb":"retry","refresh_workspace":false}),
            ),
            (
                json!({"verb":"retry","reset_budget":true}),
                json!([{"name":"reset_budget","required":false,"boolean_values":[true]}]),
                json!({"verb":"retry"}),
                json!({"verb":"retry","reset_budget":true}),
                json!({"verb":"retry","reset_budget":false}),
            ),
            (
                json!({"verb":"approve","override":true}),
                json!([{"name":"override","required":false,"boolean_values":[true]}]),
                json!({"verb":"approve"}),
                json!({"verb":"approve","override":true}),
                json!({"verb":"approve","override":false}),
            ),
        ] {
            let offer: Offer = serde_json::from_value(json!({"action":preset,"parameters":parameters,"authority":["owner"],"reason":"test","label":"Test","propagates":false,"target_execution_id":null})).unwrap();
            let supplied = serde_json::from_value::<TaskAction>(supplied).unwrap();
            assert!(action_parameters_allowed(&offer, &supplied, true));
            assert_eq!(
                serde_json::to_value(apply_offered_parameters(&offer, supplied)).unwrap(),
                normalized
            );
            assert!(!action_parameters_allowed(
                &offer,
                &serde_json::from_value(contrary).unwrap(),
                true
            ));
        }
    }
}
