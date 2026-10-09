// Task condition offers are derived from values, never from stored action lists.
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
    // Mirrors `task_actions`: a held coordination root's release launches
    // no role outside its aggregate review.
    let root_role_refused = snapshot.coordination_root
        && role.is_some_and(|role| {
            !crate::task_hierarchy::RootRolePolicy::for_workflow(workflow)
                .allows_execution(&task.status, role)
        });
    if held {
        if (snapshot.has_agent && !snapshot.project_paused) && role.is_some() && !root_role_refused
        {
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
        // A parent Task whose aggregate review cannot pass needs corrective
        // work, and a parent in review accepts no new subtask. Sending it
        // back reopens it, held, so a corrective subtask can be added; its
        // release runs that subtask and the aggregate review after it.
        if task.status == "review"
            && snapshot.coordination_root
            && target(WorkflowTrigger::Reject).is_some()
        {
            offer(
                TaskAction::SendBack {
                    guidance: String::new(),
                },
                &["guidance"],
                &[Owner],
                "root_review_reopen",
                "Reopen for a Corrective Subtask",
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

