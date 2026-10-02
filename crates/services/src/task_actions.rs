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
    pub caller: ActionCaller,
    pub has_agent: bool,
    pub dependencies_satisfied: bool,
    pub owner_supports_resume: bool,
    pub coordination_root: bool,
}

impl TaskSnapshot {
    pub fn condition(&self) -> Option<FailureKind> {
        let task = &self.task;
        if task.failed_json.is_some() {
            return Some(FailureKind::ExecutorFailed);
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

/// The sole Task action advertiser. Every offer selects an apply path with a
/// stable reason and defaults; handlers never re-derive state eligibility.
pub fn available_actions(snapshot: &TaskSnapshot) -> Vec<Offer> {
    use ActionAuthority::{Owner, ProjectAgent, Reviewer};
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
    let interactive = snapshot
        .executions
        .iter()
        .filter(|execution| {
            execution.status == ExecutionStatus::Running && execution.role == "interactive"
        })
        .max_by_key(|execution| (&execution.created_at, &execution.id));
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
    let barrier_running = barrier
        .as_ref()
        .is_some_and(|barrier| barrier["status"] == "running");
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
    let failed_review = snapshot
        .latest_review
        .as_ref()
        .is_some_and(|review| review.status == ReviewStatus::Failed);
    let awaiting_human = !barrier_running
        && (snapshot
            .latest_review
            .as_ref()
            .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
            || metadata["awaiting_human"] == true);
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
            && gate
                .gate_config
                .as_ref()
                .and_then(|config| config.max_rejections)
                .is_some()
    });
    let exhausted = condition.is_some_and(|kind| {
        kind.is_budget_exhausted_annotation() || kind.is_retry_exhausted_metadata()
    }) || retry_gate.is_some_and(|gate| {
        crate::task_diagnostics::count_gate_rejections_since_boundary(
            &snapshot.transition_logs,
            &gate.name,
        ) >= i64::from(
            gate.gate_config
                .as_ref()
                .and_then(|config| config.max_rejections)
                .unwrap_or_default(),
        )
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
    let mut offer = |action: TaskAction,
                     parameters: &[&str],
                     authority: &[ActionAuthority],
                     reason: &str,
                     label: &str| {
        if snapshot.caller.permits(authority) {
            offers.push(Offer {
                action: action.clone(),
                parameters: parameters
                    .iter()
                    .map(|name| api_types::ActionParameter {
                        name: (*name).to_owned(),
                        required: *name == "guidance"
                            && matches!(action, TaskAction::SendBack { .. }),
                        boolean_values: match *name {
                            "guidance" => None,
                            "reset_budget" => Some(
                                if snapshot.caller.owner
                                    && retry_gate.is_some_and(|gate| gate.name == "review")
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
                target_execution_id: if matches!(
                    action,
                    TaskAction::Retry { .. } | TaskAction::Release
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
        if let Some(execution) = interactive {
            offer(
                TaskAction::Hold,
                &[],
                &[Owner],
                "interactive_session_running",
                "Stop Current Session",
            );
            if let Some(hold) = offers.last_mut() {
                hold.target_execution_id = Some(execution.id.clone());
            }
        }
        return offers;
    }
    if workflow.cancellation_state.is_some()
        || workflow
            .states
            .iter()
            .any(|state| state.name == "cancelled" && state.kind == StateKind::Terminal)
    {
        offer(
            TaskAction::Cancel,
            &[],
            &[Owner, ProjectAgent],
            "cancellable",
            "Cancel Task",
        );
    }
    if task.failed_json.is_some() {
        if workflow
            .states
            .iter()
            .any(|state| state.kind == StateKind::Initial)
        {
            offer(
                TaskAction::Restart,
                &[],
                &[Owner, ProjectAgent],
                "hard_failure",
                "Restart Task",
            );
        }
        return offers;
    }
    if running || interactive.is_some() {
        offer(
            TaskAction::Hold,
            &[],
            &[Owner],
            if running {
                "execution_running"
            } else {
                "interactive_session_running"
            },
            if running {
                "Hold Task"
            } else {
                "Stop Current Session"
            },
        );
        if !running {
            if let Some(hold) = offers
                .iter_mut()
                .find(|offer| offer.action == TaskAction::Hold)
            {
                hold.target_execution_id = interactive.map(|execution| execution.id.clone());
            }
        }
        return offers;
    }
    if barrier_running || queued {
        return offers;
    }
    if held {
        if snapshot.has_agent {
            offer(
                TaskAction::Release,
                &[],
                &[Owner],
                "manually_held",
                "Release Task",
            );
            offer(
                TaskAction::Retry {
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
        }
        return offers;
    }
    let retry = |fresh_session, refresh_workspace, reset_budget| TaskAction::Retry {
        fresh_session,
        refresh_workspace,
        reset_budget,
        guidance: None,
    };
    if barrier_blocked
        || matches!(
            condition,
            Some(FailureKind::BeforeWorkHookFailed | FailureKind::BeforeWorkHookTimeout)
        )
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
                override_checks: true,
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
        offer(
            TaskAction::Restart,
            &[],
            &[Owner, ProjectAgent],
            "interrupted_restart",
            "Restart Task",
        );
        return offers;
    }
    if exhausted {
        if retry_gate.is_none()
            && !(condition == Some(FailureKind::RetryExhausted)
                && role.is_some()
                && snapshot.has_agent)
        {
            if workflow
                .states
                .iter()
                .any(|state| state.kind == StateKind::Initial)
            {
                offer(
                    TaskAction::Restart,
                    &[],
                    &[Owner, ProjectAgent],
                    "interrupted_restart",
                    "Restart Task",
                );
            }
            return offers;
        }
        offer(
            retry(None, None, Some(true)),
            &["reset_budget", "guidance"],
            &[Owner, ProjectAgent],
            if condition == Some(FailureKind::RetryExhausted) {
                "execution_retry_exhausted"
            } else {
                "retry_budget_exhausted"
            },
            "Reset Budget and Retry",
        );
        offer(
            TaskAction::Restart,
            &[],
            &[Owner, ProjectAgent],
            "interrupted_restart",
            "Restart Task",
        );
        if task.status == "review" && failed_review {
            offer(
                TaskAction::Approve {
                    override_checks: true,
                },
                &["override"],
                &[Owner],
                "failed_review_override",
                "Override Failed Review",
            );
        }
        return offers;
    }
    if metadata["awaiting_human_reason"] == "pull_request_merge" {
        offer(
            retry(None, None, None),
            &[],
            &[Owner],
            "pull_request_merge_wait",
            "Retry Merge",
        );
        return offers;
    }
    let review_wait = state.kind == StateKind::Gate && awaiting_human;
    let owner_gate = state
        .gate_config
        .as_ref()
        .is_some_and(|gate| gate.requires_user_approval());
    if condition == Some(FailureKind::ReviewNeedsOwner)
        || condition == Some(FailureKind::ReviewBlocked)
    {
        if condition == Some(FailureKind::ReviewNeedsOwner) {
            offer(
                TaskAction::Approve {
                    override_checks: false,
                },
                &["override"],
                &[Owner],
                "review_needs_owner",
                "Defer Finding to Follow-up",
            );
        } else {
            offer(
                TaskAction::Approve {
                    override_checks: true,
                },
                &["override"],
                &[Owner],
                "reviewer_failed_manual_pass",
                "Pass Review Manually",
            );
        }
        if role.is_some() && snapshot.has_agent {
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
    } else if review_wait
        || (state.kind == StateKind::Gate
            && owner_gate
            && target(WorkflowTrigger::Accept).is_some())
    {
        let authorities = if task.status == "review" && owner_gate {
            vec![Owner, ProjectAgent]
        } else {
            vec![Owner]
        };
        offer(
            TaskAction::Approve {
                override_checks: false,
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
        if task.status == "review"
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
    } else if state.kind == StateKind::Gate && failed_review {
        if condition.is_some() && role.is_some() && snapshot.has_agent {
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
        if task.status == "review" {
            offer(
                TaskAction::Approve {
                    override_checks: true,
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
            "role_retry"
        };
        if condition != Some(FailureKind::Unknown)
            && (!snapshot.coordination_root || state.kind == StateKind::Gate)
            && (snapshot.has_agent || role.is_none())
        {
            let authorities = if reason == "role_retry" {
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
        if condition != Some(FailureKind::Unknown)
            && workflow
                .states
                .iter()
                .any(|state| state.kind == StateKind::Initial)
        {
            offer(
                TaskAction::Restart,
                &[],
                &[Owner, ProjectAgent],
                "interrupted_restart",
                "Restart Task",
            );
        }
    } else if state.kind == StateKind::Initial
        && !snapshot.coordination_root
        && snapshot.has_agent
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
    } else if state.kind == StateKind::Active && !snapshot.coordination_root && snapshot.has_agent {
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
                    override_checks: false,
                },
                &[],
                &[Owner],
                "work_ready_to_submit",
                "Submit Work",
            );
        }
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
    if target(WorkflowTrigger::Reject).is_some() && state.kind == StateKind::Gate {
        let authorities = if owner_gate && task.status == "review" {
            vec![Owner, ProjectAgent]
        } else {
            vec![Owner, Reviewer]
        };
        offer(
            TaskAction::SendBack {
                guidance: "Return for revisions.".to_owned(),
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
    offers
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
        AgentRepo, ExecutionRepo, ProjectAgentBindingRepo, ReviewRepo, TaskDependencyRepo,
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
    let transition_logs = TransitionLogRepo::list_by_task(db, &task.id).await?;
    let dependencies_satisfied = TaskDependencyRepo::unsatisfied_dependencies(db, &task.id)
        .await?
        .is_empty();
    let mut has_agent = assignments.iter().any(|assignment| {
        assignment.assignee_type == Some(db::AssigneeKind::Agent)
            && assignment.assignee_id.is_some()
    }) || task.assignee_type.as_deref() == Some("agent")
        || executions
            .iter()
            .any(|execution| execution.agent_id.is_some());
    if !has_agent {
        has_agent = !AgentRepo::list(
            db,
            db::AgentListQuery {
                status: None,
                executor_type: None,
                capabilities: Vec::new(),
                page: db::PageRequest {
                    cursor: None,
                    limit: 1,
                    include_total: false,
                    sort_by: db::SortBy::CreatedAt,
                    sort_order: db::SortOrder::Asc,
                },
            },
        )
        .await?
        .items
        .is_empty();
    }
    let running = ExecutionRepo::list_running_by_task(db, &task.id).await?;
    let mut executions = executions;
    for execution in running {
        if !executions.iter().any(|row| row.id == execution.id) {
            executions.push(execution);
        }
    }
    let owner_supports_resume =
        owner_supports_resume(db, &task, &executions, role, &workflow, connections).await?;
    let coordination_root =
        crate::task_hierarchy::coordination_root_has_subtasks(db, &task).await?;
    Ok(TaskSnapshot {
        task,
        workflow,
        executions,
        latest_review,
        role_assignments: assignments,
        transition_logs,
        caller,
        has_agent,
        dependencies_satisfied,
        owner_supports_resume,
        coordination_root,
    })
}
async fn owner_supports_resume(
    db: &db::SqliteDb,
    task: &Task,
    executions: &[Execution],
    role: Option<&str>,
    workflow: &WorkflowDefinition,
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
    let Some(placement) = db::WorkspacePlacementRepo::get_for_task(db, &task.id).await? else {
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
    let Some(execution) = db::ExecutionRepo::get_by_id(db, &execution_id).await? else {
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
            caller: ActionCaller::owner(),
            has_agent: true,
            dependencies_satisfied: true,
            owner_supports_resume: true,
            coordination_root: false,
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
                            override_checks: true
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
        snapshot.task.entry_barrier_json =
            Some(json!({"status":"running","state":"review"}).to_string());
        assert_eq!(
            available_actions(&snapshot).len(),
            1,
            "only cancellation while entry checks run"
        );
        snapshot.task.entry_barrier_json = None;
        snapshot.task.error_annotation =
            Some(json!({"type":"review_needs_owner","blocking_reason":"finding"}).to_string());
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
