//! Advisory reads only; the reserve/start transactions decide admission.
use super::{
    load_selection_context, select_placement, ConnectionHandshake, ExecutorFacts,
    PlacementCandidate, PlacementFilterCode, SelectionLoadInput, SelectionOutcome, ServerFacts,
    WorktreeAgent,
};
use crate::{
    daemon_transport::DaemonConnectionRegistry, workflow::engine::WorkflowEngine, Result,
    ServiceError,
};
use api_types::{Actor, ProjectSettings, WorkspaceRunPurpose};
use db::{
    Agent, AgentConnectionHealthRepo, AgentRepo, PlacementState, ProjectRepo, RepoLocation,
    RepoLocationKind, RepoLocationOwnerKind, RepoLocationStatus, RepoRepo, Task,
    TaskRoleAssignmentRepo, WorkspacePlacementRepo,
};

pub(crate) async fn snapshot(
    db: &db::SqliteDb,
) -> Result<Vec<db::machine_capacity::MachineCapacityRow>> {
    let mut tx = db.pool().begin().await?;
    Ok(db::machine_capacity::list_machine_capacity(
        &mut tx,
        &db.server_run_cap.embedded_machine_id(),
        db.server_run_cap.effective(),
    )
    .await?)
}

/// False includes unknown facts and candidates reserve could verify. Never
/// claim capacity is the reason when another outcome might be possible.
pub(crate) async fn task_blocked(
    db: &db::SqliteDb,
    task: &Task,
    agent: &Agent,
    connections: Option<&DaemonConnectionRegistry>,
    adapters: Option<&executors::AdapterRegistry>,
) -> Result<bool> {
    if agent.paused || agent.status == db::AgentStatus::Error {
        return Ok(false);
    }
    let capacities = snapshot(db).await?;
    if capacities.iter().all(|row| row.capacity.has_capacity()) {
        return Ok(false);
    }
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", &task.project_id))?;
    let Some(repo_id) = project.primary_repo_id.as_deref() else {
        return Ok(false);
    };
    let repo = RepoRepo::get_by_id(db, repo_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("repository", repo_id))?;
    let binding = WorkspacePlacementRepo::get_for_task(db, &task.id).await?;
    if binding
        .as_ref()
        .is_some_and(|p| !matches!(p.state, PlacementState::Ready | PlacementState::Cleaned))
    {
        return Ok(false);
    }
    let locations: Vec<(String, Option<String>, String)> =
        sqlx::query_as("SELECT id, daemon_id, status FROM repo_location WHERE repo_id = ?")
            .bind(repo_id)
            .fetch_all(db.pool())
            .await?;
    // Any possibly usable free or unverified location delegates to reserve.
    // In particular, an unverified managed clone must not be hidden behind a
    // full ready daemon location.
    for (id, daemon, status) in &locations {
        if binding
            .as_ref()
            .is_some_and(|p| p.state != PlacementState::Cleaned && &p.repo_location_id != id)
        {
            continue;
        }
        let route = binding
            .as_ref()
            .and_then(|p| p.execution_daemon_id.as_ref().or(p.daemon_id.as_ref()))
            .or(daemon.as_ref());
        let row = capacities
            .iter()
            .find(|r| r.daemon_id.as_ref() == route)
            .or_else(|| capacities.iter().find(|r| r.daemon_id.is_none()));
        if status != "ready" || row.is_none_or(|r| r.capacity.has_capacity()) {
            return Ok(false);
        }
    }
    if locations.is_empty()
        && capacities
            .iter()
            .any(|r| r.daemon_id.is_none() && r.capacity.has_capacity())
    {
        return Ok(false);
    }

    let settings: ProjectSettings = serde_json::from_str(&project.settings)
        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
    let workflow = WorkflowEngine::resolve_workflow_for_task(
        task,
        &project.workflow_definition,
        &Actor::system(api_types::SystemComponent::General),
    );
    let source = serde_json::json!({"workflow": workflow, "project_settings": settings,
        "task_scope": {"task_type": task.task_type, "config": task.task_state_config.as_deref().map(serde_json::from_str::<serde_json::Value>).transpose().map_err(|e| ServiceError::invalid_operation(e.to_string()))?}});
    let review = serde_json::from_value(
        api_types::effective_review_config(&source).map_err(ServiceError::invalid_operation)?,
    )
    .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
    let claiming = worktree_agent("coder", agent.clone());
    let mut assignments = TaskRoleAssignmentRepo::list_by_task(db, &task.id).await?;
    if let Some(parent) = task.parent_task_id.as_deref() {
        for assignment in TaskRoleAssignmentRepo::list_by_task(db, parent).await? {
            if assignment.role_name != "coder"
                && !assignments
                    .iter()
                    .any(|a| a.role_name == assignment.role_name)
            {
                assignments.push(assignment);
            }
        }
    }
    assignments.retain(|a| a.role_name != "coder");
    if let Some(coder) = crate::task_hierarchy::effective_coder_assignment(db, task).await? {
        assignments.push(coder.assignment);
    }
    let mut others = Vec::new();
    for assignment in assignments {
        if !matches!(
            assignment.role_name.as_str(),
            "coder" | "executor" | "reviewer" | "planner" | "auditor"
        ) || assignment.assignee_type != Some(db::AssigneeKind::Agent)
        {
            continue;
        }
        if let Some(id) = assignment
            .assignee_id
            .as_deref()
            .filter(|id| *id != agent.id)
        {
            let Some(assigned) = AgentRepo::get_by_id(db, id).await? else {
                return Ok(false);
            };
            others.push(worktree_agent(&assignment.role_name, assigned));
        }
    }
    let embedded: Option<String> =
        sqlx::query_scalar("SELECT id FROM daemon WHERE machine_id = ? AND status <> 'offline'")
            .bind(db.server_run_cap.embedded_machine_id())
            .fetch_optional(db.pool())
            .await?;
    let default_adapters;
    let adapters = match adapters {
        Some(adapters) => adapters,
        None => {
            default_adapters = cli_adapters::default_registry();
            &default_adapters
        }
    };
    let mut server = ServerFacts {
        execution_daemon_id: embedded,
        ..Default::default()
    };
    for role in std::iter::once(&claiming).chain(others.iter()) {
        let agent = &role.agent;
        let available = if agent.backend_kind == "native" {
            AgentConnectionHealthRepo::get_connection_health(db, &agent.profile_id)
                .await?
                .is_some_and(|h| h.status == "healthy")
        } else {
            agent
                .executor_type
                .parse::<executors::ExecutorKind>()
                .ok()
                .and_then(|kind| adapters.get(&kind))
                .is_some_and(|a| {
                    matches!(
                        a.check_availability().status,
                        executors::AvailabilityStatus::Authenticated
                    )
                })
        };
        server.executors.insert(
            agent.id.clone(),
            ExecutorFacts {
                installed: available,
                authenticated: available,
                enabled: !agent.paused && agent.status != db::AgentStatus::Error,
                capabilities:
                    crate::daemon_transport::EmbeddedExecutionProvider::adapter_capabilities(
                        &agent.executor_type,
                    ),
            },
        );
    }
    let default_connections = DaemonConnectionRegistry::default();
    let connections = connections.unwrap_or(&default_connections);
    let handshakes = connections
        .connection_snapshots()
        .into_iter()
        .map(|(id, facts)| {
            (
                id,
                ConnectionHandshake {
                    connection_id: facts.connection_id,
                    handshake: facts.handshake,
                },
            )
        })
        .collect();
    let mut tx = db.pool().begin().await?;
    let mut context = load_selection_context(
        db,
        &mut tx,
        connections,
        SelectionLoadInput {
            task,
            repo: &repo,
            claiming_agent: &claiming,
            worktree_agents: &others,
            task_owner_id: project.owner_id.as_deref(),
            workspace_id: binding.as_ref().map(|p| p.workspace_id.as_str()),
            inherited_root_workspace_id: None,
            review_config: &review,
            project_settings: &settings,
            server: &server,
            handshakes: &handshakes,
        },
    )
    .await?;
    if locations.is_empty() && binding.is_none() && repo.local_path.is_some() {
        context.candidates.push(PlacementCandidate {
            location: RepoLocation {
                id: String::new(),
                repo_id: repo.id.clone(),
                owner_kind: RepoLocationOwnerKind::Server,
                daemon_id: None,
                runtime_id: None,
                path: repo.local_path.clone().unwrap_or_default(),
                kind: RepoLocationKind::PrimaryCheckout,
                is_default: true,
                status: RepoLocationStatus::Ready,
                last_verified_at: None,
                last_error: None,
                version: 1,
                created_at: String::new(),
                updated_at: String::new(),
            },
            execution_daemon_id: server.execution_daemon_id.clone(),
            embedded_execution: true,
            connected: true,
            negotiated_revision: Some(api_types::DAEMON_MIN_PROTOCOL_REVISION),
            workspace_v1: true,
            runtime_ready: true,
            visible: true,
            executors: server.executors,
            allowed_run_purposes: vec![
                WorkspaceRunPurpose::EnvironmentSetup,
                WorkspaceRunPurpose::Hook,
                WorkspaceRunPurpose::CiStep,
            ],
            machine_capacity: capacities
                .iter()
                .find(|r| r.daemon_id.is_none())
                .map(|r| r.capacity),
        });
    }
    Ok(
        matches!(select_placement(&context), SelectionOutcome::Unavailable(refusal)
        if !refusal.rejected_candidates.is_empty() && refusal.rejected_candidates.iter().all(|r| r.filter_codes == [PlacementFilterCode::MachineCapacity])),
    )
}

fn worktree_agent(role: &str, agent: Agent) -> WorktreeAgent {
    WorktreeAgent {
        role: role.to_owned(),
        agent,
        required_capabilities: api_types::ExecutorAdapterCapabilityFacts {
            cancel_ack: true,
            terminal_observed: true,
            ..Default::default()
        },
    }
}

/// Re-admitting a parked Task needs both machine and Project room. This uses
/// fresh counts for each candidate, including dispatches earlier in this tick.
pub(crate) async fn wait_before_dispatch(
    db: &db::SqliteDb,
    service: &crate::TaskService,
    task: &Task,
    agent: &Agent,
) -> Result<bool> {
    if service.machine_capacity_blocked(task, agent).await? {
        crate::deferred_dispatch::record_dispatch_disposition(
            db,
            task,
            "machine_capacity",
            "machine_capacity: waiting for a machine run slot",
        )
        .await?;
        return Ok(true);
    }
    retire_wait(db, task).await
}

/// Clearing an obsolete machine reason cannot promote a parked Task past its
/// Project limit. Another skip may leave a Project wait, never a machine claim.
pub(crate) async fn retire_wait(db: &db::SqliteDb, task: &Task) -> Result<bool> {
    if crate::deferred_dispatch::current_dispatch_disposition(task).is_some_and(|d| {
        matches!(
            d.capability.as_str(),
            "machine_capacity" | "project_capacity"
        )
    }) {
        let project = ProjectRepo::get_by_id(db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", &task.project_id))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &Actor::system(api_types::SystemComponent::General),
        );
        if matches!(
            workflow.state_kind(&task.status),
            Some(api_types::StateKind::Active | api_types::StateKind::Gate)
        ) {
            let slots = crate::task_dispatcher::slots::load_project_slots(db, &project).await?;
            if slots.limit > 0 && slots.active >= slots.limit {
                crate::deferred_dispatch::record_dispatch_disposition(
                    db,
                    task,
                    "project_capacity",
                    "project_at_capacity: waiting for a Project slot",
                )
                .await?;
                return Ok(true);
            }
        }
        crate::deferred_dispatch::clear_capacity_wait(db, task).await?;
    }
    Ok(false)
}
