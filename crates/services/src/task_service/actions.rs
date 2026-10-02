use super::*;
use crate::task_actions::{available_actions, TaskSnapshot};
use api_types::{Offer, TaskAction, WorkflowTrigger};

tokio::task_local! { pub(crate) static TASK_ACTION_COMMAND: (); pub(crate) static TASK_ACTION_ACTOR: Actor; }

/// Whether `task` is parked at a review gate only a user may decide.
///
/// The Project Agent's `task.review` action and the review-ready attention
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

#[derive(Debug)]
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
        let task_id = task_id.into();
        let snapshot = self.task_action_snapshot(&task_id, &actor).await?;
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
            })?;
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
                    override_checks: true,
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
        let updated = TASK_ACTION_ACTOR
            .scope(
                actor.clone(),
                Box::pin(async {
                    Ok::<Task, ServiceError>(match &action {
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
                            override_checks: true,
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
                                        override_checks: false,
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
                            override_checks: false,
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
                        _ => format!("gate rejected: {}", guidance.unwrap_or_default()),
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
        // The accepted intent is a scheduling fence, not a condition on the
        // next role. Derive its continuation target from the same pure resolver.
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
        let now = now_rfc3339();
        let mut repository_roles = snapshot
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
            if !repository_roles.iter().any(|candidate| candidate == role) {
                repository_roles.push(role.to_owned());
            }
        }
        let updated = TaskRepo::update_recovery_metadata_if_no_running_execution(&*self.db, &task.id, task.version, None, None, None, &now, None, repository_roles, vec![
            db::TaskMetadataMutation::Set { key: crate::deferred_dispatch::QUEUED_RECOVERY_KEY.to_owned(), value: serde_json::to_value(&queued).map_err(|error| ServiceError::invalid_operation(error.to_string()))? },
            db::TaskMetadataMutation::Set { key: "deferred_dispatch".to_owned(), value: json!({ "not_before": now, "reason": "task action queued", "target_state": task.status }) },
        ]).await?;
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
                    override_checks: false,
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
            (!*override_checks || owner)
                && (matches!(&offer.action, TaskAction::Approve { override_checks: offered, .. } if offered == override_checks)
                    || allows_bool("override", *override_checks))
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
            (fresh_session.is_none() || fresh_session == offered_fresh || supports("fresh_session"))
                && (refresh_workspace.is_none()
                    || refresh_workspace == offered_refresh
                    || supports("refresh_workspace"))
                && (reset_budget.is_none()
                    || reset_budget == offered_reset
                    || reset_budget.is_some_and(|value| allows_bool("reset_budget", value)))
                && (guidance.is_none() || supports("guidance"))
        }
        TaskAction::SendBack { .. } => true,
        _ => true,
    }
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
