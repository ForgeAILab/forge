//! Task condition offers are derived from values, never from stored action lists.
use api_types::{
    ActionAuthority, Actor, FailureKind, Offer, StateKind, TaskAction, WorkflowDefinition,
    WorkflowTrigger,
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
    /// The caller is the lead of this subtask's coordination root and
    /// `lead_verbs` is not empty. It then sees the offers the Project Agent
    /// sees on this subtask, limited to those verbs.
    pub lead: bool,
    /// What the lead of this subtask's root may do to it now, whoever asks
    /// (`task_hierarchy::lead_verbs`): recover it while it is parked on
    /// something the lead decides, cancel it while it has not started. The
    /// offers of these verbs name the lead in their audience. Empty for
    /// every Task that is not a subtask of a root with a lead.
    pub lead_verbs: Vec<&'static str>,
}

/// The recovery actions a lead may apply to a subtask of its root that is
/// parked on something the lead decides.
pub const LEAD_RECOVERY_VERBS: [&str; 3] = ["retry", "restart", "cancel"];
/// What a lead may do to a subtask that has not started.
pub const LEAD_UNSTARTED_VERBS: [&str; 1] = ["cancel"];

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
            ActionAuthority::ProjectAgent => self.project_agent || self.lead,
            ActionAuthority::Reviewer => self.reviewer,
            ActionAuthority::Lead => self.lead,
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
        self.task.condition.read().diagnostic
    }
}

pub fn task_condition(task: &Task) -> Option<FailureKind> {
    task.condition.read().failure_kind
}

#[cfg(test)]
pub(crate) fn legacy_task_condition(task: &Task) -> Option<FailureKind> {
    use api_types::TaskAnnotation;
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
/// Whether the action system offers `retry` (reason `integration_retry`) for
/// the Task's merge attempt `attempt_id`: the park step has told the Task
/// that this very attempt stopped (`deferred`). `available_actions` builds
/// the offer from this function and the merge-queue summary its `retry` hint,
/// so the hint never names an action that would be refused.
pub fn integration_retry_offered(task: &Task, attempt_id: &str) -> bool {
    task.archived_at.is_none()
        && matches!(
            &task.condition,
            db::TaskCondition::Parked {
                primary: db::ParkReason::Integration {
                    reason: reason @ api_types::IntegrationReason::Deferred { .. },
                },
                ..
            } if reason.attempt_id().as_str() == attempt_id
        )
}

pub fn available_actions(snapshot: &TaskSnapshot) -> Vec<Offer> {
    let mut offers = derive_offers(snapshot);
    lead_audience(&snapshot.caller, &mut offers);
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
            lead: false,
            lead_verbs: Vec::new(),
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
        // A subtask's step-check role is its root's lead and never an own
        // row: the snapshot shows what the resolver says, nothing else.
        if crate::task_hierarchy::step_check_root(&task, role).is_some() {
            assignments.retain(|assignment| assignment.role_name != role);
        }
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
    let mut caller = caller_for(actor, project_agent.as_deref(), &assignments);
    // What the lead of this subtask's root may do to it now: those offers
    // name the lead, and the lead itself is offered them.
    if let Some((lead, verbs)) = crate::task_hierarchy::lead_verbs(db, &task).await? {
        caller.lead = !verbs.is_empty()
            && matches!(actor, Actor::Agent { agent_id, .. } if *agent_id == lead);
        caller.lead_verbs = verbs;
    }
    let role = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .and_then(crate::workflow::effective_role);
    let blocked_id = task
        .condition
        .read()
        .diagnostic
        .and_then(|a| a.blocked_execution_id);
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
    if crate::workflow::is_review_state(&workflow, &task.status) {
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
    let selected_role = crate::workflow::action_role(&workflow, &task.status);
    let action_agent_id = select_action_agent(db, &task, selected_role, connections).await?;
    let retry_needs_assignment = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .is_some_and(|state| state.kind == StateKind::Active)
        || (task.status == "merging"
            && task_condition(&task).is_some_and(FailureKind::is_merge_recoverable));
    let assigned_agent = if retry_needs_assignment {
        match selected_role {
            Some(role) => crate::task_hierarchy::effective_role_assignment(db, &task, role)
                .await?
                .is_some_and(|resolved| {
                    resolved.assignment.assignee_type == Some(db::AssigneeKind::Agent)
                        && resolved.assignment.assignee_id.is_some()
                }),
            None => false,
        }
    } else {
        true
    };
    let has_agent = action_agent_id.is_some() && assigned_agent;
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
        let published = task.condition.reasons().any(|r| {
            matches!(
                r,
                db::ParkReason::HumanDecision {
                    boundary: db::HumanBoundary::PlanReview,
                    ..
                }
            )
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
        .condition
        .read()
        .diagnostic
        .and_then(|a| a.blocked_execution_id)
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
    let dir = std::env::var_os("FORGE_TASK_ACTION_EVIDENCE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("forge-task-actions"));
    // Every writer may be the first in its process: the unit tests run in
    // two CI shards, so no test can rely on another having made the directory.
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

mod offers;
use offers::{derive_offers, lead_audience};

#[cfg(test)]
mod condition_matrix;
#[cfg(test)]
mod condition_reader_parity;
#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod legacy;
/// Task fixtures shared with the crates that test against a [`TaskSnapshot`].
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
#[cfg(test)]
pub(crate) mod tests;
