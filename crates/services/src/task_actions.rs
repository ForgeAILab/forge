//! Task condition offers are derived from values, never from stored action lists.
use api_types::{
    ActionAuthority, Actor, FailureKind, Offer, StateKind, TaskAction, TaskAnnotation,
    WorkflowDefinition, WorkflowTrigger,
};
use db::{
    Execution, ExecutionStatus, Review, ReviewStatus, Task, TaskRoleAssignment, TransitionLog,
};

#[derive(Debug, Clone, Default)]
pub struct ActionCaller {
    pub owner: bool,
    pub assigned_agent: bool,
    pub project_agent: bool,
    pub reviewer: bool,
}

impl ActionCaller {
    pub fn owner() -> Self {
        Self {
            owner: true,
            ..Self::default()
        }
    }
    fn permits(&self, authorities: &[ActionAuthority]) -> bool {
        authorities.iter().any(|authority| match authority {
            ActionAuthority::Owner => self.owner,
            ActionAuthority::AssignedAgent => self.assigned_agent,
            ActionAuthority::ProjectAgent => self.project_agent,
            ActionAuthority::Reviewer => self.reviewer,
        })
    }
}

/// A value projection of rows already loaded by a command or query adapter.
/// Capacity deliberately is not an eligibility input: accepted work queues.
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub task: Task,
    pub workflow: WorkflowDefinition,
    pub executions: Vec<Execution>,
    pub latest_review: Option<Review>,
    pub role_assignments: Vec<TaskRoleAssignment>,
    pub transition_logs: Vec<TransitionLog>,
    pub budget_spent: std::collections::HashMap<String, i64>,
    pub recovery_budget_limit: i64,
    pub caller: ActionCaller,
    pub has_agent: bool,
    pub dependencies_satisfied: bool,
    pub owner_supports_resume: bool,
    pub coordination_root: bool,
    pub owner_disconnected: bool,
    pub project_paused: bool,
    pub wait_cause: Option<api_types::DeniedBy>,
    pub action_agent_id: Option<String>,
    pub planning_approval_ready: bool,
    pub advance_target: Option<String>,
    /// The current entry's post-commit hook step is pending or claimed.
    pub entry_hooks_running: bool,
}

impl TaskSnapshot {
    pub fn condition(&self) -> Option<FailureKind> {
        task_condition(&self.task)
    }

    pub fn annotation(&self) -> Option<api_types::TaskBlockingAnnotation> {
        match self
            .task
            .error_annotation
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
        {
            Some(TaskAnnotation::Blocking(annotation)) => Some(annotation),
            _ => None,
        }
    }
}

pub fn task_condition(task: &Task) -> Option<FailureKind> {
    if let Some(raw) = task.failed_json.as_deref() {
        return Some(
            serde_json::from_str::<serde_json::Value>(raw)
                .ok()
                .and_then(|value| value.get("kind").cloned())
                .and_then(|kind| serde_json::from_value(kind).ok())
                .unwrap_or(FailureKind::ExecutorFailed),
        );
    }
    let blocked_kind = task
        .blocked_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(|value| value.get("kind").cloned())
        .and_then(|value| serde_json::from_value::<FailureKind>(value).ok());
    if blocked_kind.is_some_and(|kind| {
        kind.is_retry_exhausted_metadata() || kind.is_budget_exhausted_annotation()
    }) {
        return blocked_kind;
    }
    if let Some(TaskAnnotation::Blocking(annotation)) = task
        .error_annotation
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
    {
        if annotation.annotation_type != FailureKind::Unknown {
            return Some(annotation.annotation_type);
        }
    }
    task.blocked_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(|value| value.get("kind").cloned())
        .and_then(|value| serde_json::from_value(value).ok())
        .or_else(|| task.error_annotation.as_ref().map(|_| FailureKind::Unknown))
}

/// The sole Task action advertiser. Every offer selects an apply path with a
/// stable reason and defaults; handlers never re-derive state eligibility.
pub fn available_actions(snapshot: &TaskSnapshot) -> Vec<Offer> {
    use ActionAuthority::{Owner, ProjectAgent};
    let task = &snapshot.task;
    let workflow = &snapshot.workflow;
    let Some(state) = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
    else {
        return Vec::new();
    };
    let mut offers = Vec::new();
    let running = snapshot.executions.iter().any(|execution| {
        execution.status == ExecutionStatus::Running && execution.role != "interactive"
    });
    let role = crate::workflow::effective_role(state);
    let current_executions: Vec<_> = snapshot
        .executions
        .iter()
        .filter(|execution| {
            role.is_some_and(|role| {
                role == execution.role || (role == "coder" && execution.role == "executor")
            })
        })
        .collect();
    let latest = current_executions
        .iter()
        .max_by_key(|execution| (&execution.created_at, &execution.id));
    let logical_thread = state.dispatch.as_ref().is_some_and(|dispatch| {
        dispatch.execution_policy
            == Some(api_types::WorkflowExecutionPolicy::ResumeLatestTargetRoleThread)
    });
    let eligible_resume = |execution: &&Execution| {
        let terminal_review = snapshot.latest_review.as_ref().is_some_and(|review| {
            matches!(
                review.status,
                ReviewStatus::Passed | ReviewStatus::Failed | ReviewStatus::Cancelled
            ) && (review.reviewer_execution_id.as_deref() == Some(execution.id.as_str())
                || review.auditor_execution_id.as_deref() == Some(execution.id.as_str())
                || (matches!(execution.role.as_str(), "reviewer" | "auditor")
                    && review.execution_id == execution.id))
        });
        let assigned = snapshot
            .role_assignments
            .iter()
            .find(|assignment| role == Some(assignment.role_name.as_str()))
            .and_then(|assignment| assignment.assignee_id.as_deref())
            .or(snapshot.task.assignee_id.as_deref());
        snapshot.owner_supports_resume
            && (execution.agent_session_id.is_some() || logical_thread)
            && execution.status != ExecutionStatus::Running
            && !terminal_review
            && execution.agent_id.as_deref() == assigned
    };
    let resumable = current_executions
        .iter()
        .copied()
        .any(|execution| eligible_resume(&execution));
    let condition = snapshot.condition();
    let barrier = task
        .entry_barrier_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
    let barrier_blocked = barrier
        .as_ref()
        .is_some_and(|barrier| barrier["status"] == "blocked");
    let metadata = task
        .metadata_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .unwrap_or_default();
    let held = condition == Some(FailureKind::ManualStop);
    let queued = metadata
        .get(crate::deferred_dispatch::QUEUED_RECOVERY_KEY)
        .is_some();
    let implementation_candidate = snapshot.executions.iter().any(|execution| {
        execution.status == ExecutionStatus::Completed
            && (execution.task_id == task.id || snapshot.coordination_root)
            && matches!(execution.role.as_str(), "executor" | "coder" | "worker")
    });
    let dependency_cancelled = condition == Some(FailureKind::DependencyCancelled)
        || (condition == Some(FailureKind::WorkflowGuardRejected)
            && snapshot
                .annotation()
                .is_some_and(|annotation| annotation.blocking_reason == "dependency_cancelled"));
    let manual_review_candidate = snapshot.latest_review.as_ref().is_some_and(|review| {
        review.status == ReviewStatus::Failed
            && snapshot.executions.iter().any(|execution| {
                execution.id == review.execution_id
                    && (execution.task_id == task.id || snapshot.coordination_root)
                    && execution.status == ExecutionStatus::Completed
                    && !matches!(
                        execution.role.as_str(),
                        "interactive" | "reviewer" | "auditor"
                    )
            })
    });
    let failed_review = snapshot
        .latest_review
        .as_ref()
        .is_some_and(|review| review.status == ReviewStatus::Failed);
    // While the entry's hook step runs, the latest Review may belong to an
    // earlier entry; nothing waits for a human until its checks settle.
    let entry_hooks_running = snapshot.entry_hooks_running;
    let awaiting_human = !entry_hooks_running
        && (snapshot
            .latest_review
            .as_ref()
            .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
            || (metadata["awaiting_human"] == true
                && (task.status != "review" || snapshot.latest_review.is_none()))
            || (task.status == "review"
                && snapshot.latest_review.is_none()
                && !running
                && state.gate_config.as_ref().is_some_and(|gate| {
                    gate.requires_user_approval()
                        && (!gate.optional_when_unassigned()
                            || role.is_some_and(|role| {
                                snapshot.role_assignments.iter().any(|assignment| {
                                    assignment.role_name == role && assignment.assignee_id.is_some()
                                })
                            }))
                })));
    let target = |trigger| {
        workflow
            .outgoing_trigger_targets(&task.status)
            .find(|(candidate, _)| *candidate == trigger)
            .map(|(_, target)| target)
    };
    let retry_gate = workflow.states.iter().find(|gate| {
        gate.kind == StateKind::Gate
            && (gate.name == task.status
                || gate
                    .gate_config
                    .as_ref()
                    .and_then(|config| config.reject_target.as_deref())
                    == Some(task.status.as_str()))
            && (gate.name == "review"
                || gate
                    .gate_config
                    .as_ref()
                    .and_then(|config| config.max_rejections)
                    .is_some())
    });
    let gate_budget = |gate: &api_types::StateDefinition| {
        let kind = if gate.name == crate::workflow::default_states::REVIEW {
            db::budget::Kind::Review
        } else {
            db::budget::Kind::GateRejection
        };
        let mut origin = task.clone();
        origin.status = gate.name.clone();
        db::budget::limit(&origin, kind, Some(&gate.config), gate.gate_config.as_ref()).ok()
    };
    let exhausted = condition.is_some_and(|kind| {
        kind.is_budget_exhausted_annotation() || kind.is_retry_exhausted_metadata()
    }) || retry_gate.is_some_and(|gate| {
        gate_budget(gate).is_some_and(|budget| {
            db::budget::gate_entry_exhausted(
                &gate.name,
                i64::from(budget),
                *snapshot
                    .budget_spent
                    .get(&db::budget::gate_key(&gate.name))
                    .unwrap_or(&0),
            )
        })
    });
    let blocked_execution_id = snapshot
        .annotation()
        .and_then(|annotation| annotation.blocked_execution_id);
    let pinned_resume = current_executions
        .iter()
        .copied()
        .filter(eligible_resume)
        .max_by_key(|execution| {
            (
                blocked_execution_id.as_deref() == Some(execution.id.as_str()),
                &execution.created_at,
                &execution.id,
            )
        });
    let one_shot_allowed = snapshot.caller.owner
        && snapshot.wait_cause.is_none()
        && retry_gate.is_some_and(|gate| {
            gate.name == "review"
                && (gate_budget(gate).is_some_and(|budget| {
                    !db::budget::allows_retry(
                        i64::from(budget),
                        *snapshot
                            .budget_spent
                            .get(&db::budget::gate_key(&gate.name))
                            .unwrap_or(&0),
                    )
                }) || snapshot.annotation().is_some_and(|annotation| {
                    crate::task_diagnostics::is_retry_budget_exhausted(&annotation)
                }))
        });
    let mut offer = |action: TaskAction,
                     parameters: &[&str],
                     authority: &[ActionAuthority],
                     reason: &str,
                     label: &str| {
        if snapshot.caller.permits(authority)
            && (snapshot.wait_cause.is_none()
                || action.verb() != "retry"
                || matches!(
                    reason,
                    "retry_budget_exhausted" | "execution_retry_exhausted"
                ))
            && (task.status != "review"
                || action.verb() != "retry"
                || implementation_candidate
                || matches!(
                    reason,
                    "entry_barrier_blocked"
                        | "state_hooks_failed"
                        | "placement_retry"
                        | "owner_reconcile"
                        | "retry_budget_exhausted"
                        | "execution_retry_exhausted"
                ))
        {
            offers.push(Offer {
                action: action.clone(),
                parameters: parameters
                    .iter()
                    .copied()
                    .chain(
                        matches!(
                            action,
                            TaskAction::Approve { .. }
                                | TaskAction::Cancel { .. }
                                | TaskAction::Retry { .. }
                                | TaskAction::Hold { .. }
                                | TaskAction::Release { .. }
                                | TaskAction::Restart { .. }
                        )
                        .then_some("reason"),
                    )
                    .collect::<Vec<_>>()
                    .iter()
                    .map(|name| api_types::ActionParameter {
                        name: (*name).to_owned(),
                        required: (*name == "guidance"
                            && matches!(action, TaskAction::SendBack { .. }))
                            || (*name == "reason"
                                && (matches!(
                                    action,
                                    TaskAction::Approve {
                                        override_checks: Some(true),
                                        ..
                                    }
                                ) || reason == "review_needs_owner"
                                    || (snapshot.caller.project_agent
                                        && matches!(
                                            action,
                                            TaskAction::Cancel { .. }
                                                | TaskAction::Retry { .. }
                                                | TaskAction::Restart { .. }
                                        )))),
                        required_when: (*name == "reason"
                            && reason == "retry_budget_exhausted"
                            && one_shot_allowed
                            && !awaiting_human)
                            .then(|| api_types::ActionParameterRequirement {
                                parameter: "reset_budget".to_owned(),
                                value: false,
                            }),
                        boolean_values: match *name {
                            "guidance" | "reason" => None,
                            "reset_budget" => Some(
                                if reason == "retry_budget_exhausted"
                                    && one_shot_allowed
                                    && !awaiting_human
                                {
                                    vec![false, true]
                                } else {
                                    vec![true]
                                },
                            ),
                            "override" if reason != "review_needs_owner" => Some(vec![true]),
                            _ => Some(vec![false, true]),
                        },
                    })
                    .collect(),
                authority: authority.to_vec(),
                reason: reason.to_owned(),
                label: label.to_owned(),
                propagates: matches!(action, TaskAction::Cancel { .. })
                    && snapshot.coordination_root,
                target_execution_id: if matches!(
                    action,
                    TaskAction::Retry { .. } | TaskAction::Release { .. }
                ) && resumable
                {
                    pinned_resume.map(|execution| execution.id.clone())
                } else {
                    None
                },
            });
        }
    };
    if state.kind == StateKind::Terminal || task.archived_at.is_some() {
        return offers;
    }
    if workflow.cancellation_state.is_some()
        || workflow
            .states
            .iter()
            .any(|state| state.name == "cancelled" && state.kind == StateKind::Terminal)
    {
        offer(
            TaskAction::Cancel { reason: None },
            &[],
            &[Owner, ProjectAgent],
            "cancellable",
            "Cancel Task",
        );
    }
    // A running Task is held through its execution (below); a dispatch-wait
    // Hold only applies to work that is waiting to be dispatched.
    let running_live = running && !snapshot.owner_disconnected;
    if !held
        && !running_live
        && (queued
            || (snapshot.wait_cause.is_some() && condition.is_none() && task.failed_json.is_none()))
    {
        offer(
            TaskAction::Hold { reason: None },
            &[],
            &[Owner],
            "dispatch_wait",
            "Hold Task",
        );
    }
    if dependency_cancelled {
        return offers;
    }
    if task.failed_json.is_some() {
        if workflow
            .states
            .iter()
            .any(|state| state.kind == StateKind::Initial)
        {
            offer(
                TaskAction::Restart { reason: None },
                &[],
                &[Owner, ProjectAgent],
                "hard_failure",
                "Restart Task",
            );
        }
        return offers;
    }
    if running_live {
        if !held {
            offer(
                TaskAction::Hold { reason: None },
                &[],
                &[Owner],
                "execution_running",
                "Hold Task",
            );
        }
        append_owner_advance(snapshot, &mut offers);
        return offers;
    }
    if snapshot.owner_disconnected && condition != Some(FailureKind::WorkspaceResetRequired) {
        offer(
            TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            &["guidance"],
            &[Owner, ProjectAgent],
            "owner_reconcile",
            "Retry on Workspace Owner",
        );
        return offers;
    }
    let decision_ready = condition.is_none()
        && (awaiting_human
            || (state.kind == StateKind::Active
                && latest.is_some_and(|execution| execution.status == ExecutionStatus::Completed))
            || (task.status == "review"
                && (manual_review_candidate || target(WorkflowTrigger::Accept).is_some()))
            || (task.status == "planning" && snapshot.planning_approval_ready));
    let placement_wait = snapshot.wait_cause.is_some() && !decision_ready
        || condition.is_none()
            && ((snapshot.project_paused && !decision_ready)
                || metadata.get("environment_wait").is_some()
                || metadata.get("deferred_dispatch").is_some_and(|deferred| {
                    deferred["kind"]
                        .as_str()
                        .is_some_and(|kind| kind.starts_with("environment_"))
                })
                || crate::deferred_dispatch::current_dispatch_disposition(task).is_some_and(
                    |disposition| {
                        matches!(
                            disposition.capability.as_str(),
                            "machine_capacity" | "project_capacity"
                        )
                    },
                ));
    if placement_wait {
        if !queued && snapshot.wait_cause.is_none() {
            offer(
                TaskAction::Hold { reason: None },
                &[],
                &[Owner],
                "dispatch_wait",
                "Hold Task",
            );
        }
        if condition.is_none() {
            return offers;
        }
    }
    if entry_hooks_running || queued {
        return offers;
    }
    if matches!(
        condition,
        Some(FailureKind::DispatchFailed | FailureKind::WorkflowLoop | FailureKind::CascadeFailed)
    ) && metadata["placement_refusal"]["annotation"]
        == task
            .error_annotation
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .unwrap_or_default()
        && metadata.get("placement_refusal").is_some()
    {
        offer(
            TaskAction::retry(),
            &[],
            &[Owner],
            "placement_retry",
            "Retry Placement",
        );
        if workflow
            .states
            .iter()
            .any(|state| state.kind == StateKind::Initial)
        {
            offer(
                TaskAction::Restart { reason: None },
                &[],
                &[Owner, ProjectAgent],
                "interrupted_restart",
                "Restart Task",
            );
        }
        append_owner_advance(snapshot, &mut offers);
        return offers;
    }
    if held {
        if (snapshot.has_agent && !snapshot.project_paused) && role.is_some() {
            offer(
                TaskAction::Release { reason: None },
                &[],
                &[Owner],
                "manually_held",
                "Release Task",
            );
            offer(
                TaskAction::Retry {
                    reason: None,
                    fresh_session: Some(!resumable),
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
                if resumable {
                    &["fresh_session", "guidance"]
                } else {
                    &["guidance"]
                },
                &[Owner, ProjectAgent],
                "role_retry",
                "Retry Held Task",
            );
        } else {
            // No Agent can take the role right now: releasing returns the Task
            // to the dispatch queue instead of launching it.
            offer(
                TaskAction::Release { reason: None },
                &[],
                &[Owner],
                "held_waiting",
                "Release Task",
            );
        }
        offer(
            TaskAction::Restart { reason: None },
            &[],
            &[Owner, ProjectAgent],
            "interrupted_restart",
            "Restart Task",
        );
        append_owner_advance(snapshot, &mut offers);
        return offers;
    }
    let retry = |fresh_session, refresh_workspace, reset_budget| TaskAction::Retry {
        reason: None,
        fresh_session,
        refresh_workspace,
        reset_budget,
        guidance: None,
    };
    if state.kind != StateKind::Backlog
        && (barrier_blocked
            || matches!(
                condition,
                Some(FailureKind::BeforeWorkHookFailed | FailureKind::BeforeWorkHookTimeout)
            ))
    {
        offer(
            retry(None, Some(false), None),
            &["refresh_workspace", "guidance"],
            &[Owner],
            if barrier_blocked {
                "entry_barrier_blocked"
            } else {
                "state_hooks_failed"
            },
            "Retry Entry Checks",
        );
        offer(
            TaskAction::Approve {
                reason: None,
                override_checks: Some(true),
            },
            &["override"],
            &[Owner],
            if barrier_blocked {
                "entry_barrier_override"
            } else {
                "state_hooks_override"
            },
            "Skip Entry Checks Once",
        );
        if restart_condition(condition) {
            offer(
                TaskAction::Restart { reason: None },
                &[],
                &[Owner, ProjectAgent],
                "interrupted_restart",
                "Restart Task",
            );
        }
        return offers;
    }
    if exhausted {
        if retry_gate.is_none()
            && !(condition == Some(FailureKind::RetryExhausted)
                && role.is_some()
                && (snapshot.has_agent && !snapshot.project_paused))
        {
            if restart_condition(condition)
                && workflow
                    .states
                    .iter()
                    .any(|state| state.kind == StateKind::Initial)
            {
                offer(
                    TaskAction::Restart { reason: None },
                    &[],
                    &[Owner, ProjectAgent],
                    "interrupted_restart",
                    "Restart Task",
                );
            }
            append_owner_advance(snapshot, &mut offers);
            return offers;
        }
        offer(
            retry(None, None, Some(true)),
            &["reset_budget", "guidance"],
            &[Owner, ProjectAgent],
            if condition == Some(FailureKind::RetryExhausted)
                && role.is_some()
                && (snapshot.has_agent && !snapshot.project_paused)
            {
                "execution_retry_exhausted"
            } else {
                "retry_budget_exhausted"
            },
            "Reset Budget and Retry",
        );
        if restart_condition(condition) {
            offer(
                TaskAction::Restart { reason: None },
                &[],
                &[Owner, ProjectAgent],
                "interrupted_restart",
                "Restart Task",
            );
        }
        if task.status == "review" && failed_review && manual_review_candidate {
            offer(
                TaskAction::Approve {
                    reason: None,
                    override_checks: Some(true),
                },
                &["override"],
                &[Owner],
                "failed_review_override",
                "Override Failed Review",
            );
        }
        if !(task.status == "review"
            && state
                .gate_config
                .as_ref()
                .is_some_and(|gate| gate.requires_user_approval())
            && !failed_review
            && condition.is_none()
            && awaiting_human)
        {
            append_owner_advance(snapshot, &mut offers);
            return offers;
        }
    }
    if metadata["awaiting_human_reason"] == "pull_request_merge" {
        offer(
            retry(None, None, None),
            &[],
            &[Owner],
            "pull_request_merge_wait",
            "Retry Merge",
        );
        append_owner_advance(snapshot, &mut offers);
        return offers;
    }
    let review_wait = state.kind == StateKind::Gate
        && awaiting_human
        && (task.status != "planning" || snapshot.planning_approval_ready);
    let owner_gate = state
        .gate_config
        .as_ref()
        .is_some_and(|gate| gate.requires_user_approval());
    if task.status == "review"
        && matches!(
            condition,
            Some(FailureKind::ReviewNeedsOwner | FailureKind::ReviewBlocked)
        )
    {
        if manual_review_candidate && condition == Some(FailureKind::ReviewNeedsOwner) {
            offer(
                TaskAction::Approve {
                    reason: None,
                    override_checks: Some(false),
                },
                &["override"],
                &[Owner],
                "review_needs_owner",
                "Defer Finding to Follow-up",
            );
        } else if manual_review_candidate {
            offer(
                TaskAction::Approve {
                    reason: None,
                    override_checks: Some(true),
                },
                &["override"],
                &[Owner],
                "reviewer_failed_manual_pass",
                "Pass Review Manually",
            );
        }
        if role.is_some() && (snapshot.has_agent && !snapshot.project_paused) {
            offer(
                retry(Some(!resumable), None, None),
                if resumable {
                    &["fresh_session", "guidance"]
                } else {
                    &["guidance"]
                },
                &[Owner, ProjectAgent],
                "role_retry",
                "Retry Review With Guidance",
            );
        } else {
            offer(
                retry(None, None, None),
                &["guidance"],
                &[Owner],
                "review_owner_retry",
                "Retry With Guidance",
            );
        }
    } else if (task.status != "review" || (awaiting_human && condition.is_none()))
        && (review_wait
            || ((task.status == "review"
                || (task.status == "planning" && snapshot.planning_approval_ready))
                && !failed_review
                && target(WorkflowTrigger::Accept).is_some())
            || (state.kind == StateKind::Gate
                && owner_gate
                && (task.status != "planning" || snapshot.planning_approval_ready)
                && target(WorkflowTrigger::Accept).is_some()))
    {
        let authorities = if task.status == "review" && owner_gate {
            vec![Owner, ProjectAgent]
        } else {
            vec![Owner]
        };
        offer(
            TaskAction::Approve {
                reason: None,
                override_checks: Some(false),
            },
            &[],
            &authorities,
            if task.status == "review"
                && snapshot
                    .latest_review
                    .as_ref()
                    .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
                && target(WorkflowTrigger::Accept).as_deref() == Some("merging")
            {
                "human_review_decision"
            } else {
                "gate_waiting_for_decision"
            },
            state
                .gate_config
                .as_ref()
                .and_then(|gate| gate.approve_label.as_deref())
                .unwrap_or("Approve"),
        );
        if one_shot_allowed && !exhausted {
            offer(
                retry(None, None, Some(true)),
                &["reset_budget"],
                &[Owner],
                "retry_budget_exhausted",
                "Reset Budget and Retry",
            );
        }
        if task.status == "review"
            && !exhausted
            && !one_shot_allowed
            && implementation_candidate
            && snapshot
                .latest_review
                .as_ref()
                .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
        {
            offer(
                retry(None, None, None),
                &[],
                &[Owner],
                "review_checks_retry",
                "Recheck Review",
            );
        }
    } else if task.status == "review"
        && state.kind == StateKind::Gate
        && failed_review
        && implementation_candidate
    {
        if condition.is_some() && role.is_some() && (snapshot.has_agent && !snapshot.project_paused)
        {
            offer(
                retry(Some(!resumable), None, None),
                if resumable {
                    &["fresh_session", "guidance"]
                } else {
                    &["guidance"]
                },
                &[Owner, ProjectAgent],
                "role_retry",
                "Retry Review",
            );
        } else {
            offer(
                retry(None, None, None),
                &["guidance"],
                &[Owner],
                "review_failed",
                "Retry Review",
            );
        }
        if task.status == "review" && manual_review_candidate {
            offer(
                TaskAction::Approve {
                    reason: None,
                    override_checks: Some(true),
                },
                &["override"],
                &[Owner],
                "failed_review_override",
                "Override Failed Review",
            );
        }
    } else if condition.is_some() {
        let annotation = snapshot.annotation();
        let reason = if task.status == "merging"
            && annotation.as_ref().is_some_and(|annotation| {
                matches!(
                    annotation.blocked_by.as_deref(),
                    Some("coordination_root" | "manual_workspace_repair")
                )
            }) {
            "manual_merge_repair"
        } else if task.status == "merging"
            && (condition == Some(FailureKind::TargetRepoDirty)
                || !condition.is_some_and(FailureKind::is_merge_recoverable))
        {
            "merge_gate_retry"
        } else if task.status == "merging"
            && condition.is_some_and(FailureKind::is_merge_recoverable)
        {
            "merge_retry_remediation"
        } else if task.status == "merge_failed" {
            "merge_fix_retry"
        } else {
            if role.is_some() {
                "role_retry"
            } else {
                "initial_retry"
            }
        };
        if state.kind != StateKind::Backlog
            && !matches!(
                condition,
                Some(
                    FailureKind::BeforeWorkHookFailed
                        | FailureKind::BeforeWorkHookTimeout
                        | FailureKind::MaxTurnsExceeded
                        | FailureKind::WorkspaceResetRequired
                        | FailureKind::DependencyCancelled
                )
            )
            && (!snapshot.coordination_root || state.kind == StateKind::Gate)
            && ((role.is_some() && (snapshot.has_agent && !snapshot.project_paused))
                || (reason != "role_retry" && reason != "initial_retry")
                || (state.kind == StateKind::Initial
                    && (snapshot.has_agent && !snapshot.project_paused)
                    && task.status != "backlog"))
        {
            let authorities =
                if matches!(reason, "role_retry" | "merge_fix_retry" | "initial_retry") {
                    vec![Owner, ProjectAgent]
                } else {
                    vec![Owner]
                };
            offer(
                retry(Some(!resumable), None, None),
                if resumable {
                    &["fresh_session", "guidance"]
                } else {
                    &["guidance"]
                },
                &authorities,
                reason,
                "Retry Task",
            );
        }
        if restart_condition(condition)
            && workflow
                .states
                .iter()
                .any(|state| state.kind == StateKind::Initial)
        {
            offer(
                TaskAction::Restart { reason: None },
                &[],
                &[Owner, ProjectAgent],
                "interrupted_restart",
                "Restart Task",
            );
        }
    } else if task.status == "merging" {
        offer(
            retry(None, None, None),
            &[],
            &[Owner],
            "merge_gate_retry",
            "Retry Merge",
        );
    } else if state.kind == StateKind::Initial
        && !snapshot.coordination_root
        && (snapshot.has_agent && !snapshot.project_paused)
        && snapshot.dependencies_satisfied
        && workflow
            .outgoing_trigger_targets(&task.status)
            .any(|(_, target)| {
                matches!(
                    workflow.state_kind(&target),
                    Some(StateKind::Active | StateKind::Gate)
                )
            })
    {
        offer(
            TaskAction::Start,
            &[],
            &[Owner],
            "ready_to_start",
            "Start Task",
        );
    } else if state.kind == StateKind::Active && !snapshot.coordination_root {
        if latest.is_some_and(|execution| {
            execution.status == ExecutionStatus::Completed
                && snapshot
                    .transition_logs
                    .iter()
                    .rfind(|entry| entry.to_state == task.status)
                    .is_none_or(|entry| execution.created_at > entry.created_at)
        }) && target(WorkflowTrigger::Accept).is_some()
        {
            offer(
                TaskAction::Approve {
                    reason: None,
                    override_checks: Some(false),
                },
                &[],
                &[Owner],
                "work_ready_to_submit",
                "Submit Work",
            );
        }
        if snapshot.has_agent && !snapshot.project_paused {
            offer(
                retry(Some(!resumable), None, None),
                if resumable {
                    &["fresh_session", "guidance"]
                } else {
                    &["guidance"]
                },
                &[Owner],
                "role_retry",
                "Retry Task",
            );
        }
    }
    if task.status == "planning"
        && snapshot.has_agent
        && !snapshot.project_paused
        && role.is_some()
        && !offers.iter().any(|offer| offer.action.verb() == "retry")
        && snapshot.caller.owner
    {
        let mut parameters = vec![
            api_types::ActionParameter {
                name: "guidance".to_owned(),
                required: false,
                required_when: None,
                boolean_values: None,
            },
            api_types::ActionParameter {
                name: "reason".to_owned(),
                required: false,
                required_when: None,
                boolean_values: None,
            },
        ];
        if resumable {
            parameters.push(api_types::ActionParameter {
                name: "fresh_session".to_owned(),
                required: false,
                required_when: None,
                boolean_values: Some(vec![false, true]),
            });
        }
        offers.push(Offer {
            action: retry(Some(!resumable), None, None),
            parameters,
            authority: vec![Owner],
            reason: "role_retry".to_owned(),
            label: "Retry Planning".to_owned(),
            target_execution_id: pinned_resume.map(|execution| execution.id.clone()),
            propagates: false,
        });
    }
    if task.status == "review"
        && implementation_candidate
        && condition.is_none()
        && !offers.iter().any(|offer| offer.action.verb() == "retry")
    {
        let mut parameters = vec![api_types::ActionParameter {
            name: "reason".to_owned(),
            required: false,
            required_when: None,
            boolean_values: None,
        }];
        parameters.push(api_types::ActionParameter {
            name: "guidance".to_owned(),
            required: false,
            required_when: None,
            boolean_values: None,
        });
        if snapshot.caller.owner {
            offers.push(Offer {
                action: TaskAction::retry(),
                parameters,
                authority: vec![Owner],
                reason: "review_checks_retry".to_owned(),
                label: "Re-run Review".to_owned(),
                target_execution_id: None,
                propagates: false,
            });
        }
    }
    let mut offer = |action: TaskAction,
                     parameters: &[&str],
                     authority: &[ActionAuthority],
                     reason: &str,
                     label: &str| {
        if snapshot.caller.permits(authority) {
            offers.push(Offer {
                action,
                parameters: parameters
                    .iter()
                    .map(|name| api_types::ActionParameter {
                        name: (*name).to_owned(),
                        required: *name == "guidance",
                        required_when: None,
                        boolean_values: None,
                    })
                    .collect(),
                authority: authority.to_vec(),
                reason: reason.to_owned(),
                label: label.to_owned(),
                target_execution_id: None,
                propagates: false,
            });
        }
    };
    if target(WorkflowTrigger::Reject).is_some() && state.kind == StateKind::Gate {
        let authorities = if owner_gate && task.status == "review" {
            vec![Owner, ProjectAgent]
        } else {
            vec![Owner]
        };
        offer(
            TaskAction::SendBack {
                guidance: String::new(),
            },
            &["guidance"],
            &authorities,
            if task.status == "review"
                && snapshot
                    .latest_review
                    .as_ref()
                    .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
                && target(WorkflowTrigger::Reject).as_deref() == Some("in_progress")
            {
                "human_review_send_back"
            } else {
                "gate_can_reject"
            },
            state
                .gate_config
                .as_ref()
                .and_then(|gate| gate.reject_label.as_deref())
                .unwrap_or("Send Back"),
        );
    }
    append_owner_advance(snapshot, &mut offers);
    offers
}

fn append_owner_advance(snapshot: &TaskSnapshot, offers: &mut Vec<Offer>) {
    if !snapshot.caller.owner || snapshot.advance_target.is_none() {
        return;
    }
    if let Some(approval) = offers
        .iter_mut()
        .find(|offer| offer.action.verb() == "approve")
    {
        if matches!(
            approval.action,
            TaskAction::Approve {
                override_checks: Some(false),
                ..
            }
        ) && approval.reason != "review_needs_owner"
        {
            approval.parameters.push(api_types::ActionParameter {
                name: "override".to_owned(),
                required: false,
                boolean_values: Some(vec![false, true]),
                required_when: None,
            });
            if let Some(reason) = approval
                .parameters
                .iter_mut()
                .find(|parameter| parameter.name == "reason")
            {
                reason.required_when = Some(api_types::ActionParameterRequirement {
                    parameter: "override".to_owned(),
                    value: true,
                });
            }
        }
        return;
    }
    offers.push(Offer {
        action: TaskAction::Approve {
            override_checks: Some(true),
            reason: None,
        },
        parameters: vec![
            api_types::ActionParameter {
                name: "override".to_owned(),
                required: false,
                boolean_values: Some(vec![true]),
                required_when: None,
            },
            api_types::ActionParameter {
                name: "reason".to_owned(),
                required: true,
                boolean_values: None,
                required_when: None,
            },
        ],
        authority: vec![ActionAuthority::Owner],
        reason: "state_advance_override".to_owned(),
        label: "Advance Task".to_owned(),
        target_execution_id: None,
        propagates: false,
    });
}

fn restart_condition(condition: Option<FailureKind>) -> bool {
    matches!(
        condition,
        Some(
            FailureKind::DispatchFailed
                | FailureKind::WorkflowLoop
                | FailureKind::CascadeFailed
                | FailureKind::RecoveryRequired
                | FailureKind::ExecutorFailed
                | FailureKind::ExecutorUnavailable
                | FailureKind::WorkspaceFailed
                | FailureKind::WorkspaceError
                | FailureKind::WorkspaceResetRequired
                | FailureKind::ManualStop
                | FailureKind::Unknown
                | FailureKind::RetryExhausted
                | FailureKind::MaxTurnsExceeded
        )
    )
}

pub fn caller_for(
    actor: &Actor,
    project_agent: Option<&str>,
    assignments: &[TaskRoleAssignment],
) -> ActionCaller {
    match actor {
        Actor::User { .. } => ActionCaller::owner(),
        Actor::Agent { agent_id, .. } => ActionCaller {
            owner: false,
            project_agent: project_agent == Some(agent_id.as_str()),
            assigned_agent: assignments
                .iter()
                .any(|assignment| assignment.assignee_id.as_ref() == Some(agent_id)),
            reviewer: assignments.iter().any(|assignment| {
                assignment.role_name == "reviewer"
                    && assignment.assignee_id.as_ref() == Some(agent_id)
            }),
        },
        Actor::System { .. } => ActionCaller::default(),
    }
}

/// I/O is confined to snapshot construction. Query surfaces and commands share
/// this bounded row projection and the pure function above.
pub async fn load_snapshot(
    db: &db::SqliteDb,
    task: Task,
    workflow: WorkflowDefinition,
    actor: &Actor,
    connections: Option<&crate::daemon_transport::DaemonConnectionRegistry>,
) -> crate::Result<TaskSnapshot> {
    use db::{
        ExecutionRepo, ProjectAgentBindingRepo, ReviewRepo, TaskDependencyRepo,
        TaskRoleAssignmentRepo, TransitionLogRepo,
    };
    let mut assignments = TaskRoleAssignmentRepo::list_by_task(db, &task.id).await?;
    if let Some(role) = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .and_then(crate::workflow::effective_role)
    {
        if let Some(resolved) =
            crate::task_hierarchy::effective_role_assignment(db, &task, role).await?
        {
            if !assignments
                .iter()
                .any(|assignment| assignment.role_name == role)
            {
                assignments.push(resolved.assignment);
            }
        }
    }
    let project_agent = ProjectAgentBindingRepo::get_active_project_binding(db, &task.project_id)
        .await?
        .filter(|binding| binding.state == "active")
        .and_then(|binding| binding.identity_id);
    let caller = caller_for(actor, project_agent.as_deref(), &assignments);
    let role = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .and_then(crate::workflow::effective_role);
    let blocked_id = task
        .error_annotation
        .as_deref()
        .and_then(|raw| serde_json::from_str::<TaskAnnotation>(raw).ok())
        .and_then(|annotation| match annotation {
            TaskAnnotation::Blocking(annotation) => annotation.blocked_execution_id,
            _ => None,
        });
    let executions = crate::task_service::action_resolver::list_execution_action_authority(
        db,
        &task.id,
        role,
        blocked_id.as_deref(),
    )
    .await?;
    let ids = [task.id.as_str()];
    let latest_review = ReviewRepo::list_latest_reviews_for_tasks(db, &ids)
        .await?
        .into_iter()
        .next();
    let mut executions = executions;
    if let Some(review) = latest_review.as_ref() {
        if !executions
            .iter()
            .any(|execution| execution.id == review.execution_id)
        {
            if let Some(candidate) = ExecutionRepo::get_by_id(db, &review.execution_id).await? {
                let belongs = candidate.task_id == task.id
                    || db::TaskRepo::get_by_id(db, &candidate.task_id, false)
                        .await?
                        .is_some_and(|source| {
                            source.parent_task_id.as_deref() == Some(task.id.as_str())
                        });
                if belongs {
                    executions.push(candidate);
                }
            }
        }
    }
    if task.status == "review" {
        if let Some(candidate) =
            crate::task_service::latest_executor_execution_for_task(db, &task).await?
        {
            if !executions
                .iter()
                .any(|execution| execution.id == candidate.id)
            {
                executions.push(candidate);
            }
        }
    }
    let transition_logs = TransitionLogRepo::list_by_task(db, &task.id).await?;
    let budget_spent = db::budget::load(db.pool(), &task.id).await?;
    let dependencies_satisfied = TaskDependencyRepo::unsatisfied_dependencies(db, &task.id)
        .await?
        .is_empty();
    let selected_role = role.or_else(|| {
        workflow
            .outgoing_trigger_targets(&task.status)
            .filter_map(|(_, target)| {
                workflow
                    .states
                    .iter()
                    .find(|state| state.name == target)
                    .filter(|state| matches!(state.kind, StateKind::Active | StateKind::Gate))
                    .and_then(crate::workflow::effective_role)
            })
            .next()
    });
    let action_agent_id = select_action_agent(db, &task, selected_role, connections).await?;
    let has_agent = action_agent_id.is_some();
    let project = db::ProjectRepo::get_by_id(db, &task.project_id).await?;
    let recovery_budget_limit = project
        .as_ref()
        .map(|p| db::budget::recovery_limit(&p.settings))
        .unwrap_or(0);
    let workflow = project
        .as_ref()
        .map(|p| db::budget::with_project_defaults(&workflow, &p.settings))
        .unwrap_or_else(|| workflow.clone());
    let project_paused = project
        .as_ref()
        .is_some_and(|project| project.paused_at.is_some());
    let wait_cause = if project_paused {
        Some(db::project_pause_denial(
            project
                .as_ref()
                .and_then(|project| project.system_pause_reason.as_deref()),
        ))
    } else {
        let pinned_id = assignments
            .iter()
            .find(|assignment| Some(assignment.role_name.as_str()) == selected_role)
            .and_then(|assignment| assignment.assignee_id.as_deref())
            .or(task.assignee_id.as_deref());
        let previous_agent = if pinned_id.is_none() {
            db::ExecutionRepo::latest_agent_execution_by_task(db, &task.id)
                .await?
                .and_then(|execution| execution.agent_id)
        } else {
            None
        };
        let pinned_id = pinned_id.or(previous_agent.as_deref());
        if let Some(id) = pinned_id {
            match db::AgentRepo::get_by_id(db, id).await? {
                Some(agent)
                    if crate::agent_service::compute_effective_status(db, &agent, connections)
                        .await?
                        == crate::agent_service::EffectiveStatus::Paused =>
                {
                    Some(api_types::DeniedBy::TargetAgentPaused)
                }
                _ => None,
            }
        } else {
            None
        }
    };
    let running = ExecutionRepo::list_running_by_task(db, &task.id).await?;
    for execution in running {
        if !executions.iter().any(|row| row.id == execution.id) {
            executions.push(execution);
        }
    }
    let placement = db::WorkspacePlacementRepo::get_for_task(db, &task.id).await?;
    let owner_supports_resume = owner_supports_resume(
        db,
        &task,
        &executions,
        role,
        &workflow,
        placement.as_ref(),
        connections,
    )
    .await?;
    let coordination_root =
        crate::task_hierarchy::coordination_root_has_subtasks(db, &task).await?;
    let owner_disconnected = placement.as_ref().is_some_and(|placement| {
        placement.state == db::PlacementState::Disconnected
            || (placement.state == db::PlacementState::Failed
                && placement.failure_cause
                    == Some(db::PlacementFailureCause::OwnerDisconnectedTimeout))
    });
    let planning_approval_ready = if task.status != "planning" {
        true
    } else {
        let metadata = db::TaskMetadata::parse(task.metadata_json.as_deref()).ok();
        let published = metadata.as_ref().is_some_and(|metadata| {
            metadata.extra.get("awaiting_human_reason") == Some(&serde_json::json!("plan_review"))
        });
        let optional_unassigned = workflow
            .states
            .iter()
            .find(|state| state.name == "planning")
            .and_then(|state| state.gate_config.as_ref())
            .is_some_and(|gate| gate.optional_when_unassigned())
            && !assignments.iter().any(|assignment| {
                assignment.role_name == "planner" && assignment.assignee_id.is_some()
            });
        if published || optional_unassigned {
            true
        } else if let Some(workspace) = db::WorkspaceRepo::get_by_task_id(db, &task.id).await? {
            let remote = db::WorkspacePlacementRepo::get_by_workspace_id(db, &workspace.id)
                .await?
                .is_some_and(|placement| placement.owner_kind == db::PlacementOwnerKind::Daemon);
            let path = (!remote)
                .then(|| {
                    std::path::Path::new(workspace.embedded_worktree_path_for_backend())
                        .parent()
                        .map(|parent| parent.join("plan.md"))
                })
                .flatten();
            match path {
                Some(path) => tokio::fs::read_to_string(path)
                    .await
                    .ok()
                    .is_some_and(|content| {
                        crate::plan_artifact::to_plan_progress_summary(
                            &crate::plan_artifact::parse_plan_markdown(&content),
                        )
                        .total
                            > 0
                    }),
                None => false,
            }
        } else {
            workflow
                .states
                .iter()
                .find(|state| state.name == "planning")
                .and_then(|state| state.gate_config.as_ref())
                .is_some_and(|gate| gate.optional_when_unassigned())
                && !assignments.iter().any(|assignment| {
                    assignment.role_name == "planner" && assignment.assignee_id.is_some()
                })
        }
    };
    let entry_hooks_running = db::TaskStepRepo::entry_hooks_pending(db, &task.id).await?;
    let advance_target =
        if crate::task_service::execution::ensure_plan_publication_transition_authority(&task, None)
            .is_ok()
        {
            match crate::task_service::next_workflow_state(&workflow, &task.status) {
                Ok(target)
                    if crate::task_hierarchy::ensure_coordination_root_target_ready(
                        db, &task, &workflow, &target,
                    )
                    .await
                    .is_ok()
                        && (workflow.state_kind(&target) == Some(StateKind::Terminal)
                            || crate::task_hierarchy::ensure_subtask_dispatch_order(db, &task)
                                .await
                                .is_ok()) =>
                {
                    Some(target)
                }
                _ => None,
            }
        } else {
            None
        };
    Ok(TaskSnapshot {
        task,
        workflow,
        executions,
        latest_review,
        role_assignments: assignments,
        transition_logs,
        budget_spent,
        recovery_budget_limit,
        caller,
        has_agent,
        dependencies_satisfied,
        owner_supports_resume,
        coordination_root,
        owner_disconnected,
        project_paused,
        wait_cause,
        action_agent_id,
        planning_approval_ready,
        advance_target,
        entry_hooks_running,
    })
}
async fn select_action_agent(
    db: &db::SqliteDb,
    task: &Task,
    role: Option<&str>,
    connections: Option<&crate::daemon_transport::DaemonConnectionRegistry>,
) -> crate::Result<Option<String>> {
    use db::{AgentRepo, ExecutionRepo};
    let pinned = if let Some(role) = role {
        crate::task_hierarchy::effective_role_assignment(db, task, role)
            .await?
            .map(|resolved| resolved.assignment)
    } else {
        None
    };
    let pinned_id = pinned
        .as_ref()
        .filter(|assignment| assignment.assignee_type == Some(db::AssigneeKind::Agent))
        .and_then(|assignment| assignment.assignee_id.clone())
        .or_else(|| {
            (task.assignee_type.as_deref() == Some("agent"))
                .then(|| task.assignee_id.clone())
                .flatten()
        });
    if pinned
        .as_ref()
        .is_some_and(|assignment| assignment.assignee_type == Some(db::AssigneeKind::User))
    {
        return Ok(None);
    }
    let pinned_id = match pinned_id {
        Some(id) => Some(id),
        None => ExecutionRepo::latest_agent_execution_by_task(db, &task.id)
            .await?
            .and_then(|execution| execution.agent_id),
    };
    if let Some(id) = pinned_id {
        let Some(agent) = AgentRepo::get_by_id(db, &id).await? else {
            return Ok(None);
        };
        return Ok(action_agent_available(db, &agent, connections)
            .await?
            .then_some(id));
    }
    let agents = AgentRepo::list(
        db,
        db::AgentListQuery {
            status: None,
            executor_type: None,
            capabilities: Vec::new(),
            page: db::PageRequest {
                cursor: None,
                limit: 500,
                include_total: false,
                sort_by: db::SortBy::CreatedAt,
                sort_order: db::SortOrder::Asc,
            },
        },
    )
    .await?
    .items;
    for require_default in [true, false] {
        for agent in &agents {
            if (!require_default || agent.is_default)
                && (agent.executor_type != "gemini" || agent.credential_ref.is_some())
                && action_agent_available(db, agent, connections).await?
            {
                return Ok(Some(agent.id.clone()));
            }
        }
    }
    Ok(None)
}

async fn action_agent_available(
    db: &db::SqliteDb,
    agent: &db::Agent,
    connections: Option<&crate::daemon_transport::DaemonConnectionRegistry>,
) -> crate::Result<bool> {
    Ok(agent.status != db::AgentStatus::Offline
        && matches!(
            crate::agent_service::compute_effective_status(db, agent, connections).await?,
            crate::agent_service::EffectiveStatus::Active
                | crate::agent_service::EffectiveStatus::Busy
        ))
}

async fn owner_supports_resume(
    db: &db::SqliteDb,
    task: &Task,
    executions: &[Execution],
    role: Option<&str>,
    workflow: &WorkflowDefinition,
    placement: Option<&db::WorkspacePlacement>,
    connections: Option<&crate::daemon_transport::DaemonConnectionRegistry>,
) -> crate::Result<bool> {
    let logical_thread = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .and_then(|state| state.dispatch.as_ref())
        .is_some_and(|dispatch| {
            dispatch.execution_policy
                == Some(api_types::WorkflowExecutionPolicy::ResumeLatestTargetRoleThread)
        });
    let Some(placement) = placement else {
        return Ok(true);
    };
    let Some(execution_id) = task
        .error_annotation
        .as_deref()
        .and_then(|raw| serde_json::from_str::<api_types::TaskBlockingAnnotation>(raw).ok())
        .and_then(|annotation| annotation.blocked_execution_id)
        .or_else(|| {
            if logical_thread {
                executions
                    .iter()
                    .filter(|execution| {
                        role == Some(execution.role.as_str())
                            && execution.status != ExecutionStatus::Running
                    })
                    .max_by_key(|execution| (&execution.created_at, &execution.id))
                    .map(|execution| execution.id.clone())
            } else {
                crate::task_service::action_resolver::latest_resumable_execution_for_role(
                    executions, role,
                )
                .map(|execution| execution.id.clone())
            }
        })
    else {
        return Ok(false);
    };
    let Some(execution) = executions
        .iter()
        .find(|execution| execution.id == execution_id)
    else {
        return Ok(false);
    };
    if (!logical_thread && execution.agent_session_id.is_none()) || execution.task_id != task.id {
        return Ok(false);
    }
    let executor = execution
        .executor_config_snapshot_json
        .as_deref()
        .and_then(|snapshot| serde_json::from_str::<serde_json::Value>(snapshot).ok())
        .and_then(|snapshot| {
            snapshot
                .get("executor_type")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
    let Some(executor) = executor else {
        return Ok(false);
    };
    let logical_resume = placement.owner_kind == db::PlacementOwnerKind::Server
        && workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(|state| state.dispatch.as_ref())
            .is_some_and(|dispatch| {
                dispatch.execution_policy
                    == Some(api_types::WorkflowExecutionPolicy::ResumeLatestTargetRoleThread)
            });
    let daemon_workspace = placement.owner_kind == db::PlacementOwnerKind::Daemon;
    let daemon_id = match placement.owner_kind {
        db::PlacementOwnerKind::Daemon => placement.daemon_id.as_deref(),
        db::PlacementOwnerKind::Server => placement.execution_daemon_id.as_deref(),
    };
    let Some(daemon_id) = daemon_id else {
        return Ok(logical_resume
            || (!daemon_workspace
                && crate::daemon_transport::EmbeddedExecutionProvider::adapter_capabilities(
                    &executor,
                )
                .resume));
    };
    let Some(daemon) = db::DaemonRepo::get_by_id(db, daemon_id).await? else {
        return Ok(false);
    };
    if daemon.status != db::DaemonStatus::Online {
        return Ok(false);
    }
    if logical_resume {
        return Ok(true);
    }
    let connection = connections.and_then(|registry| registry.get(daemon_id));
    if !daemon_workspace
        && connection
            .as_ref()
            .is_none_or(|connection| connection.is_stale())
        && crate::embedded_daemon::is_embedded_daemon_machine(&daemon.machine_id)
    {
        return Ok(
            crate::daemon_transport::EmbeddedExecutionProvider::adapter_capabilities(&executor)
                .resume,
        );
    }
    Ok(connection
        .and_then(|connection| connection.snapshot())
        .filter(|facts| !daemon_workspace || !facts.workspace_incapable)
        .and_then(|facts| {
            facts
                .handshake
                .executor_capabilities
                .get(&executor)
                .cloned()
        })
        .is_some_and(|capabilities| capabilities.resume))
}
#[cfg(test)]
pub(crate) fn action_test_evidence_dir() -> std::path::PathBuf {
    std::env::var_os("FORGE_TASK_ACTION_EVIDENCE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("forge-task-actions"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub(super) fn snapshot(state: &str, kind: Option<FailureKind>) -> TaskSnapshot {
        let mut task: Task = serde_json::from_value(json!({
            "id":"task", "project_id":"project", "parent_task_id":null, "assignee_type":"agent", "assignee_id":"agent", "title":"Fixture", "description":null, "task_type":"task", "status":state, "is_automation":false, "priority":0, "board_position":0.0, "subtask_order":null, "task_state_config":null, "merge_config":null, "metadata_json":null, "plan":null, "error_annotation":null, "blocked_json":null, "failed_json":null, "entry_barrier_json":null, "review_passed_at":null, "archived_at":null, "deleted_at":null, "version":1, "created_at":"2026-10-02T00:00:00Z", "updated_at":"2026-10-02T00:00:00Z"
        })).expect("Task fixture");
        task.error_annotation = kind.map(|kind| json!({ "type":kind, "blocking_reason":"fixture", "blocked_by":null, "blocked_at":null, "blocked_execution_id":null, "artifact":null, "message":"fixture", "hook":null }).to_string());
        TaskSnapshot {
            task,
            workflow: crate::workflow::default_workflow::default_workflow(),
            executions: Vec::new(),
            latest_review: None,
            role_assignments: Vec::new(),
            transition_logs: Vec::new(),
            budget_spent: Default::default(),
            recovery_budget_limit: 0,
            caller: ActionCaller::owner(),
            has_agent: true,
            dependencies_satisfied: true,
            owner_supports_resume: true,
            coordination_root: false,
            owner_disconnected: false,
            project_paused: false,
            wait_cause: None,
            action_agent_id: None,
            planning_approval_ready: true,
            advance_target: None,
            entry_hooks_running: false,
        }
    }

    #[test]
    fn review_condition_wins_over_generic_decision_for_owner_and_project_agent() {
        for caller in [
            ActionCaller::owner(),
            ActionCaller {
                project_agent: true,
                ..Default::default()
            },
        ] {
            let mut snapshot = snapshot("review", Some(FailureKind::RecoveryRequired));
            snapshot.caller = caller;
            snapshot.executions.push(super::condition_matrix::exec(
                "candidate",
                "coder",
                ExecutionStatus::Completed,
                false,
            ));
            snapshot.task.metadata_json = Some(json!({"awaiting_human":true}).to_string());
            let offers = available_actions(&snapshot);
            assert!(!offers.iter().any(|offer| matches!(
                offer.reason.as_str(),
                "gate_waiting_for_decision" | "human_review_decision"
            )));
            assert!(offers.iter().any(|offer| offer.action.verb() == "retry"));
            assert!(offers.iter().any(|offer| offer.action.verb() == "restart"));
        }
    }

    #[test]
    fn full_human_review_window_offers_only_budget_reset_retry() {
        let mut snapshot = snapshot("review", None);
        let gate = snapshot
            .workflow
            .states
            .iter_mut()
            .find(|state| state.name == "review")
            .unwrap()
            .gate_config
            .as_mut()
            .unwrap();
        gate.requires_user_approval = Some(true);
        gate.max_rejections = Some(1);
        snapshot.transition_logs = vec![super::condition_matrix::rejection(
            0,
            "review",
            "in_progress",
        )];
        snapshot.budget_spent.insert("review".into(), 1);
        snapshot.latest_review = Some(super::condition_matrix::review(ReviewStatus::AwaitingHuman));
        snapshot.executions = vec![super::condition_matrix::exec(
            "candidate",
            "coder",
            ExecutionStatus::Completed,
            false,
        )];
        let offers = available_actions(&snapshot);
        let retries = offers
            .iter()
            .filter(|offer| offer.action.verb() == "retry")
            .collect::<Vec<_>>();
        assert_eq!(retries.len(), 1);
        assert_eq!(retries[0].reason, "retry_budget_exhausted");
        assert_eq!(
            retries[0]
                .parameters
                .iter()
                .find(|parameter| parameter.name == "reset_budget")
                .unwrap()
                .boolean_values,
            Some(vec![true])
        );
        assert!(retries[0]
            .parameters
            .iter()
            .all(|parameter| parameter.required_when.is_none()));
    }

    #[test]
    fn implicit_human_review_gate_is_decidable_but_running_or_passed_review_is_not() {
        let mut snapshot = snapshot("review", None);
        snapshot
            .workflow
            .states
            .iter_mut()
            .find(|state| state.name == "review")
            .unwrap()
            .gate_config
            .as_mut()
            .unwrap()
            .requires_user_approval = Some(true);
        assert!(available_actions(&snapshot)
            .iter()
            .any(|offer| offer.reason == "gate_waiting_for_decision"));
        snapshot.task.metadata_json = Some(json!({"awaiting_human":true}).to_string());
        for status in [ReviewStatus::Running, ReviewStatus::Passed] {
            snapshot.latest_review = Some(super::condition_matrix::review(status));
            assert!(!available_actions(&snapshot).iter().any(|offer| matches!(
                offer.reason.as_str(),
                "human_review_decision" | "gate_waiting_for_decision"
            )));
        }
    }

    #[test]
    fn dependency_cancelled_writer_shape_offers_only_cancel() {
        let mut snapshot = snapshot("in_progress", Some(FailureKind::WorkflowGuardRejected));
        snapshot.task.error_annotation = Some(
            json!({"type":"workflow_guard_rejected", "blocking_reason":"dependency_cancelled"})
                .to_string(),
        );
        assert_eq!(
            available_actions(&snapshot)
                .iter()
                .map(|offer| offer.action.verb())
                .collect::<Vec<_>>(),
            vec!["cancel"]
        );
    }

    #[test]
    fn queued_retry_with_paused_agent_and_new_dependency_blocker_still_offers_hold() {
        let mut snapshot = snapshot("in_progress", Some(FailureKind::WorkflowGuardRejected));
        snapshot.has_agent = false;
        snapshot.wait_cause = Some(api_types::DeniedBy::TargetAgentPaused);
        snapshot.task.error_annotation = Some(
            json!({"type":"workflow_guard_rejected", "blocking_reason":"dependency_cancelled"})
                .to_string(),
        );
        snapshot.task.metadata_json = Some(
            json!({crate::deferred_dispatch::QUEUED_RECOVERY_KEY:{"action":{"verb":"retry"}}})
                .to_string(),
        );
        let offers = available_actions(&snapshot);
        assert!(offers
            .iter()
            .any(|offer| offer.reason == "dispatch_wait" && offer.action.verb() == "hold"));
        assert!(offers.iter().any(|offer| offer.action.verb() == "cancel"));
    }

    #[test]
    fn paused_review_keeps_budget_reset_but_withholds_hold_and_one_shot_retry() {
        let mut snapshot = snapshot("review", Some(FailureKind::ReviewBudgetExhausted));
        snapshot.project_paused = true;
        snapshot.wait_cause = Some(api_types::DeniedBy::ProjectPaused(
            "environment_not_ready".to_owned(),
        ));
        let offers = available_actions(&snapshot);
        // A Hold would overwrite the exhausted-budget condition; nothing is
        // waiting to be dispatched.
        assert!(!offers.iter().any(|offer| offer.action.verb() == "hold"));
        let reset = offers
            .iter()
            .find(|offer| offer.reason == "retry_budget_exhausted")
            .unwrap();
        assert_eq!(
            reset
                .parameters
                .iter()
                .find(|parameter| parameter.name == "reset_budget")
                .unwrap()
                .boolean_values,
            Some(vec![true])
        );
    }

    #[test]
    fn review_retry_needs_a_completed_implementation_candidate() {
        let mut snapshot = snapshot("review", None);
        assert!(!available_actions(&snapshot)
            .iter()
            .any(|offer| offer.action.verb() == "retry"));
        for role in ["planner", "reviewer", "auditor", "interactive"] {
            snapshot.executions = vec![super::condition_matrix::exec(
                "not-candidate",
                role,
                ExecutionStatus::Completed,
                false,
            )];
            assert!(
                !available_actions(&snapshot)
                    .iter()
                    .any(|offer| offer.action.verb() == "retry"),
                "{role}"
            );
        }
        snapshot.executions = vec![super::condition_matrix::exec(
            "candidate",
            "coder",
            ExecutionStatus::Completed,
            false,
        )];
        assert!(available_actions(&snapshot)
            .iter()
            .any(|offer| offer.reason == "review_checks_retry"));
    }

    #[test]
    fn general_condition_restart_matches_reset_kinds() {
        for kind in [
            FailureKind::WorkflowGuardRejected,
            FailureKind::CiFailed,
            FailureKind::InternalCommandFailed,
        ] {
            assert!(
                !available_actions(&snapshot("in_progress", Some(kind)))
                    .iter()
                    .any(|offer| offer.action.verb() == "restart"),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn every_state_condition_has_a_closed_deterministic_offer_set() {
        let states = [
            "backlog",
            "todo",
            "planning",
            "in_progress",
            "review",
            "merging",
            "merge_failed",
            "done",
            "cancelled",
        ];
        let conditions = [
            None,
            Some(FailureKind::ExecutorFailed),
            Some(FailureKind::RetryExhausted),
            Some(FailureKind::ReviewBudgetExhausted),
            Some(FailureKind::MergeFixBudgetExhausted),
            Some(FailureKind::MergeConflict),
            Some(FailureKind::TargetRepoDirty),
            Some(FailureKind::DirtyWorktree),
            Some(FailureKind::BeforeWorkHookFailed),
            Some(FailureKind::BeforeWorkHookTimeout),
            Some(FailureKind::WorkspaceResetRequired),
            Some(FailureKind::WorkspaceFailed),
            Some(FailureKind::WorkspaceError),
            Some(FailureKind::WorkflowGuardRejected),
            Some(FailureKind::ReviewBlocked),
            Some(FailureKind::ReviewNeedsOwner),
            Some(FailureKind::MaxTurnsExceeded),
            Some(FailureKind::ManualStop),
            Some(FailureKind::RecoveryRequired),
            Some(FailureKind::ExecutorUnavailable),
            Some(FailureKind::Unknown),
        ];
        for state in states {
            for condition in conditions {
                let snapshot = snapshot(state, condition);
                let offers = available_actions(&snapshot);
                assert_eq!(offers, available_actions(&snapshot));
                let mut verbs = std::collections::HashSet::new();
                for offer in offers {
                    assert!(
                        verbs.insert(offer.action.verb()),
                        "duplicate verb in {state}/{condition:?}"
                    );
                    assert!(snapshot.caller.permits(&offer.authority));
                    assert!(!offer.reason.is_empty());
                    assert!(!offer.label.is_empty());
                    assert!(matches!(
                        offer.action.verb(),
                        "start"
                            | "hold"
                            | "release"
                            | "retry"
                            | "send_back"
                            | "approve"
                            | "restart"
                            | "cancel"
                    ));
                }
            }
        }
    }

    #[test]
    fn old_stored_lists_are_ignored_including_non_enum_strings() {
        let mut original = snapshot("in_progress", Some(FailureKind::ExecutorFailed));
        let expected = available_actions(&original);
        let mut annotation: serde_json::Value =
            serde_json::from_str(original.task.error_annotation.as_deref().unwrap()).unwrap();
        annotation["recovery_actions"] = json!([
            "return_to_implementation",
            "retry_pr_publication",
            "resume_session"
        ]);
        let parsed: api_types::TaskBlockingAnnotation = serde_json::from_value(annotation.clone())
            .expect("legacy lists never affect deserialization");
        assert!(serde_json::to_value(parsed)
            .unwrap()
            .get("recovery_actions")
            .is_none());
        original.task.error_annotation = Some(annotation.to_string());
        assert_eq!(expected, available_actions(&original));
    }

    #[test]
    fn authority_filters_every_verb_before_advertising() {
        for state in ["todo", "in_progress", "review", "merge_failed"] {
            for kind in [
                None,
                Some(FailureKind::ExecutorFailed),
                Some(FailureKind::ReviewNeedsOwner),
                Some(FailureKind::RetryExhausted),
                Some(FailureKind::ManualStop),
            ] {
                let mut snapshot = snapshot(state, kind);
                snapshot.caller = ActionCaller::default();
                assert!(available_actions(&snapshot).is_empty());
                snapshot.caller = ActionCaller {
                    project_agent: true,
                    ..ActionCaller::default()
                };
                for offer in available_actions(&snapshot) {
                    assert!(!matches!(
                        offer.action,
                        TaskAction::Approve {
                            reason: None,
                            override_checks: Some(true)
                        }
                    ));
                }
            }
        }
    }

    #[test]
    fn entry_barrier_and_flow_parks_offer_one_apply_reason_per_verb() {
        let mut snapshot = snapshot("review", Some(FailureKind::BeforeWorkHookFailed));
        snapshot.task.entry_barrier_json =
            Some(json!({"status":"blocked","state":"review"}).to_string());
        let offers = available_actions(&snapshot);
        assert!(offers
            .iter()
            .any(|offer| offer.reason == "entry_barrier_blocked"));
        assert!(offers
            .iter()
            .any(|offer| offer.reason == "entry_barrier_override"));
        snapshot.task.entry_barrier_json = None;
        snapshot.entry_hooks_running = true;
        assert_eq!(
            available_actions(&snapshot).len(),
            1,
            "only cancellation while entry checks run"
        );
        snapshot.entry_hooks_running = false;
        snapshot.task.error_annotation =
            Some(json!({"type":"review_needs_owner","blocking_reason":"finding"}).to_string());
        snapshot.executions.push(super::condition_matrix::exec(
            "candidate",
            "coder",
            ExecutionStatus::Completed,
            false,
        ));
        snapshot.latest_review = Some(super::condition_matrix::review(ReviewStatus::Failed));
        let offers = available_actions(&snapshot);
        assert_eq!(
            offers
                .iter()
                .filter(|offer| offer.action.verb() == "approve")
                .count(),
            1
        );
        assert!(offers
            .iter()
            .any(|offer| offer.reason == "review_needs_owner"));
    }
}

#[cfg(test)]
mod condition_matrix {
    use super::*;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};
    use std::fmt::Write as _;

    fn task(state: &str) -> Task {
        serde_json::from_value(json!({
            "id":"task", "project_id":"project", "parent_task_id":null, "assignee_type":"agent", "assignee_id":"agent", "title":"Fixture", "description":null, "task_type":"task", "status":state, "is_automation":false, "priority":0, "board_position":0.0, "subtask_order":null, "task_state_config":null, "merge_config":null, "metadata_json":null, "plan":null, "error_annotation":null, "blocked_json":null, "failed_json":null, "entry_barrier_json":null, "review_passed_at":null, "archived_at":null, "deleted_at":null, "version":1, "created_at":"2026-10-02T00:00:00Z", "updated_at":"2026-10-02T00:00:00Z"
        })).expect("Task fixture")
    }

    pub(super) fn exec(id: &str, role: &str, status: ExecutionStatus, session: bool) -> Execution {
        Execution {
            id: id.to_owned(),
            task_id: "task".to_owned(),
            agent_id: Some("agent".to_owned()),
            role: role.to_owned(),
            status,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: session.then(|| "session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            prompt: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            execution_version: 1,
            lease_owner: None,
            lease_expires_at: None,
            hard_deadline_at: None,
            last_heartbeat_at: None,
            last_progress_at: None,
            created_at: "2026-10-02T00:10:00Z".to_owned(),
            updated_at: "2026-10-02T00:10:00Z".to_owned(),
        }
    }

    pub(super) fn review(status: ReviewStatus) -> Review {
        Review {
            id: "review".to_owned(),
            task_id: "task".to_owned(),
            execution_id: "candidate".to_owned(),
            reviewer_execution_id: None,
            auditor_execution_id: None,
            attempt_number: 1,
            status,
            step_results_json: "[]".to_owned(),
            started_at: "2026-10-02T00:05:00Z".to_owned(),
            finished_at: None,
            created_at: "2026-10-02T00:05:00Z".to_owned(),
            updated_at: "2026-10-02T00:05:00Z".to_owned(),
        }
    }

    pub(super) fn rejection(index: usize, from: &str, to: &str) -> TransitionLog {
        serde_json::from_value(json!({
            "id": format!("log-{index}"), "task_id":"task", "from_state":from, "to_state":to,
            "trigger_name":"reject", "triggered_by":"user", "trigger_reason":"fixture",
            "hook_results_json":null, "rejection":true,
            "created_at": format!("2026-10-02T00:0{index}:00Z")
        }))
        .expect("log fixture")
    }

    #[test]
    fn condition_pure_dead_end_matrix() {
        let kinds = [
            FailureKind::MergeConflict,
            FailureKind::TargetRepoDirty,
            FailureKind::DirtyWorktree,
            FailureKind::CiFailed,
            FailureKind::ReviewGateFailed,
            FailureKind::ReviewBudgetExhausted,
            FailureKind::ReviewBlocked,
            FailureKind::ReviewNeedsOwner,
            FailureKind::EnvironmentNotReady,
            FailureKind::RetryExhausted,
            FailureKind::MergeFixBudgetExhausted,
            FailureKind::WorkflowGuardRejected,
            FailureKind::InternalCommandFailed,
            FailureKind::ExecutorFailed,
            FailureKind::WorkspaceFailed,
            FailureKind::WorkspaceResetRequired,
            FailureKind::WorkspaceError,
            FailureKind::BeforeWorkHookTimeout,
            FailureKind::BeforeWorkHookFailed,
            FailureKind::MaxTurnsExceeded,
            FailureKind::ManualStop,
            FailureKind::RecoveryRequired,
            FailureKind::ExecutorUnavailable,
            FailureKind::Unknown,
        ];
        let mut conditions: Vec<String> = vec!["none".to_owned()];
        conditions.extend(kinds.iter().map(|kind| format!("ann:{kind}")));
        for extra in [
            "blocked:retry_exhausted",
            "blocked:review_gate_failed",
            "blocked:merge_fix_budget_exhausted",
            "blocked:executor_failed",
            "blocked:manual_stop",
            "failed_json",
            "legacy_annotation",
            "barrier_blocked",
            "queued",
            "awaiting_human_meta",
            "pr_merge_wait",
        ] {
            conditions.push(extra.to_owned());
        }
        let reviews = [
            "none",
            "running",
            "awaiting",
            "failed",
            "passed",
            "cancelled",
        ];
        let execs = [
            "none",
            "done_session",
            "done_nosession",
            "failed_session",
            "running",
        ];
        let flags = [
            "default",
            "coordination_root",
            "no_agent",
            "deps_unsatisfied",
        ];
        let workflows = [
            ("std", crate::workflow::default_workflow::default_workflow()),
            (
                "auto",
                crate::workflow::default_autonomous_workflow::default_autonomous_workflow(),
            ),
        ];
        let mut full = String::new();
        // key -> (dead combos, total combos, example dims)
        let mut dead: BTreeMap<String, (usize, usize, BTreeSet<String>)> = BTreeMap::new();
        let mut summary: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (wf_name, workflow) in &workflows {
            for state in &workflow.states {
                let role = crate::workflow::effective_role(state).unwrap_or("coder");
                let gate = workflow.states.iter().find(|gate| {
                    gate.kind == StateKind::Gate
                        && (gate.name == state.name
                            || gate
                                .gate_config
                                .as_ref()
                                .and_then(|config| config.reject_target.as_deref())
                                == Some(state.name.as_str()))
                        && gate
                            .gate_config
                            .as_ref()
                            .and_then(|config| config.max_rejections)
                            .is_some()
                });
                for condition in &conditions {
                    for flag in flags {
                        for review_name in reviews {
                            for exec_name in execs {
                                for exhausted in [false, true] {
                                    if exhausted && gate.is_none() {
                                        continue;
                                    }
                                    let mut task = task(&state.name);
                                    let mut metadata = json!({});
                                    if let Some(kind) = condition.strip_prefix("ann:") {
                                        task.error_annotation = Some(json!({"type":kind,"blocking_reason":"fixture","recovery_actions":["retry_hook","return_to_implementation"]}).to_string());
                                    } else if let Some(kind) = condition.strip_prefix("blocked:") {
                                        task.blocked_json = Some(json!({"kind":kind,"reason":"fixture","created_at":"2026-10-02T00:00:00Z"}).to_string());
                                    } else {
                                        match condition.as_str() {
                                            "failed_json" => task.failed_json = Some(json!({"kind":"executor_failed","reason":"fixture"}).to_string()),
                                            "legacy_annotation" => task.error_annotation = Some(json!({"message":"old free-form annotation"}).to_string()),
                                            "barrier_blocked" => task.entry_barrier_json = Some(json!({"status":"blocked","state":state.name}).to_string()),
                                            "queued" => metadata["queued_recovery"] = json!({"id":"q"}),
                                            "awaiting_human_meta" => metadata["awaiting_human"] = json!(true),
                                            "pr_merge_wait" => { metadata["awaiting_human"] = json!(true); metadata["awaiting_human_reason"] = json!("pull_request_merge"); }
                                            _ => {}
                                        }
                                    }
                                    if metadata
                                        .as_object()
                                        .is_some_and(|object| !object.is_empty())
                                    {
                                        task.metadata_json = Some(metadata.to_string());
                                    }
                                    let mut executions = vec![];
                                    match exec_name {
                                        "done_session" => executions.push(exec(
                                            "e1",
                                            role,
                                            ExecutionStatus::Completed,
                                            true,
                                        )),
                                        "done_nosession" => executions.push(exec(
                                            "e1",
                                            role,
                                            ExecutionStatus::Completed,
                                            false,
                                        )),
                                        "failed_session" => executions.push(exec(
                                            "e1",
                                            role,
                                            ExecutionStatus::Failed,
                                            true,
                                        )),
                                        "running" => executions.push(exec(
                                            "e1",
                                            role,
                                            ExecutionStatus::Running,
                                            true,
                                        )),
                                        _ => {}
                                    }
                                    let latest_review = match review_name {
                                        "running" => Some(review(ReviewStatus::Running)),
                                        "awaiting" => Some(review(ReviewStatus::AwaitingHuman)),
                                        "failed" => Some(review(ReviewStatus::Failed)),
                                        "passed" => Some(review(ReviewStatus::Passed)),
                                        "cancelled" => Some(review(ReviewStatus::Cancelled)),
                                        _ => None,
                                    };
                                    let mut transition_logs = vec![];
                                    if exhausted {
                                        let gate = gate.unwrap();
                                        let config = gate.gate_config.as_ref().unwrap();
                                        for index in 0..config.max_rejections.unwrap() as usize {
                                            transition_logs.push(rejection(
                                                index,
                                                &gate.name,
                                                config
                                                    .reject_target
                                                    .as_deref()
                                                    .unwrap_or("in_progress"),
                                            ));
                                        }
                                    }
                                    let snapshot = TaskSnapshot {
                                        task,
                                        workflow: workflow.clone(),
                                        executions,
                                        latest_review,
                                        role_assignments: Vec::new(),
                                        transition_logs,
                                        budget_spent: gate
                                            .filter(|_| exhausted)
                                            .map(|g| {
                                                std::collections::HashMap::from([(
                                                    db::budget::gate_key(&g.name),
                                                    i64::from(
                                                        g.gate_config
                                                            .as_ref()
                                                            .unwrap()
                                                            .max_rejections
                                                            .unwrap(),
                                                    ),
                                                )])
                                            })
                                            .unwrap_or_default(),
                                        recovery_budget_limit: 0,
                                        caller: ActionCaller::owner(),
                                        has_agent: flag != "no_agent",
                                        dependencies_satisfied: flag != "deps_unsatisfied",
                                        owner_supports_resume: true,
                                        coordination_root: flag == "coordination_root",
                                        owner_disconnected: false,
                                        project_paused: false,
                                        wait_cause: None,
                                        action_agent_id: None,
                                        planning_approval_ready: true,
                                        advance_target: None,
                                        entry_hooks_running: false,
                                    };
                                    let offers = available_actions(&snapshot);
                                    let verbs: Vec<String> = offers
                                        .iter()
                                        .map(|offer| {
                                            format!("{}[{}]", offer.action.verb(), offer.reason)
                                        })
                                        .collect();
                                    let _ = writeln!(full, "{wf_name}\t{}\t{:?}\t{condition}\t{flag}\trev={review_name}\texec={exec_name}\trej_max={exhausted}\t{}", state.name, state.kind, verbs.join(","));
                                    let terminal = state.kind == StateKind::Terminal;
                                    let in_flight = exec_name == "running";
                                    let only_cancel =
                                        offers.iter().all(|offer| offer.action.verb() == "cancel");
                                    let key = format!(
                                        "{wf_name}|{}|{:?}|{condition}|{flag}",
                                        state.name, state.kind
                                    );
                                    let entry = dead.entry(key).or_default();
                                    if !in_flight && !terminal {
                                        entry.1 += 1;
                                        if only_cancel {
                                            entry.0 += 1;
                                            entry.2.insert(format!("rev={review_name},exec={exec_name},rej_max={exhausted}"));
                                        }
                                    }
                                    if flag == "default" && !terminal {
                                        summary.entry(format!("{wf_name}|{}|{condition}|rev={review_name}|exec={exec_name}|rej_max={exhausted}", state.name)).or_default().extend(verbs.iter().cloned());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        std::fs::create_dir_all(action_test_evidence_dir()).unwrap();
        std::fs::write(
            action_test_evidence_dir().join("pure-matrix-full.tsv"),
            full,
        )
        .unwrap();
        let mut out = String::new();
        for (key, (count, total, dims)) in &dead {
            if *count == 0 {
                continue;
            }
            if count == total {
                let _ = writeln!(out, "DEAD-ALL\t{key}\t{count}/{total}");
            } else {
                let revs: BTreeSet<_> = dims
                    .iter()
                    .map(|dim| dim.split(',').next().unwrap().to_owned())
                    .collect();
                let execs: BTreeSet<_> = dims
                    .iter()
                    .map(|dim| dim.split(',').nth(1).unwrap().to_owned())
                    .collect();
                let rejs: BTreeSet<_> = dims
                    .iter()
                    .map(|dim| dim.split(',').nth(2).unwrap().to_owned())
                    .collect();
                let _ = writeln!(
                    out,
                    "DEAD-SOME\t{key}\t{count}/{total}\t{revs:?}\t{execs:?}\t{rejs:?}"
                );
            }
        }
        std::fs::write(action_test_evidence_dir().join("pure-matrix-dead.tsv"), out).unwrap();
    }
}
