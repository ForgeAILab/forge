//! Shared, read-only inputs for real admission and environment deferral.
use super::{
    load_selection_context, SelectionContext, SelectionLoadInput, ServerFacts, WorktreeAgent,
};
use crate::{workflow::engine::WorkflowEngine, Result, ServiceError};
use api_types::{Actor, ProjectSettings, ReviewConfig};
use db::{
    Agent, AgentRepo, AssigneeKind, RepoLocationKind, RepoLocationOwnerKind, RepoLocationStatus,
    SqliteDb, Task, TaskRoleAssignmentRepo,
};
use executors::ExecutorKind;
use serde_json::{json, Value};
use std::path::Path;

pub(crate) struct PreparedSelection {
    pub project: db::Project,
    pub repo: db::Repo,
    pub settings: ProjectSettings,
    pub review_config: ReviewConfig,
    pub claiming_agent: Option<WorktreeAgent>,
    pub worktree_agents: Vec<WorktreeAgent>,
    pub server_facts: ServerFacts,
}
fn parse_json_value(name: &str, raw: &str) -> Result<Value> {
    serde_json::from_str(raw)
        .map_err(|error| ServiceError::invalid_operation(format!("invalid {name}: {error}")))
}
pub(crate) async fn prepare_selection(
    db: &SqliteDb,
    task: &Task,
    agent: Option<&Agent>,
    role: &str,
    adapter_registry: Option<&executors::AdapterRegistry>,
) -> Result<PreparedSelection> {
    let authority = crate::task_service::resolve_task_repository_authority(db, task).await?;
    let settings: ProjectSettings =
        serde_json::from_str(&authority.project.settings).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid project settings: {error}"))
        })?;
    let workflow = WorkflowEngine::resolve_workflow_for_task(
        task,
        &authority.project.workflow_definition,
        &Actor::system(api_types::SystemComponent::General),
    );
    let review_source = json!({
        "workflow": workflow,
        "project_settings": parse_json_value("project settings", &authority.project.settings)?,
        "task_scope": { "task_type": task.task_type,
            "config": task.task_state_config.as_deref().map(|raw| parse_json_value("task state config", raw)).transpose()? },
    });
    let review_config: api_types::ReviewConfig = serde_json::from_value(
        api_types::effective_review_config(&review_source)
            .map_err(ServiceError::invalid_operation)?,
    )
    .map_err(|error| ServiceError::invalid_operation(format!("invalid review config: {error}")))?;
    let claiming_agent = agent.map(|agent| worktree_agent(role, agent.clone()));
    let mut worktree_agents = Vec::new();
    let mut assignments = TaskRoleAssignmentRepo::list_by_task(db, &task.id).await?;
    // Root roles also constrain the owner of their shared workspace, even
    // though only coder is an inherited child execution assignment.
    if let Some(root_id) = task.parent_task_id.as_deref() {
        for assignment in TaskRoleAssignmentRepo::list_by_task(db, root_id).await? {
            if assignment.role_name != "coder"
                && !assignments
                    .iter()
                    .any(|existing| existing.role_name == assignment.role_name)
            {
                assignments.push(assignment);
            }
        }
    }
    // An explicit empty child coder row overrides the root default.
    assignments.retain(|assignment| assignment.role_name != "coder");
    if let Some(coder) = crate::task_hierarchy::effective_coder_assignment(db, task).await? {
        assignments.push(coder.assignment);
    }
    for assignment in assignments {
        if !matches!(
            assignment.role_name.as_str(),
            "coder" | "executor" | "reviewer" | "planner" | "auditor"
        ) || assignment.assignee_type != Some(AssigneeKind::Agent)
        {
            continue;
        }
        if let Some(id) = assignment.assignee_id.as_deref() {
            if assignment.role_name == role && agent.is_some_and(|agent| agent.id == id) {
                continue;
            }
            let assigned_agent = AgentRepo::get_by_id(db, id)
                .await?
                .ok_or_else(|| ServiceError::not_found("agent", id.to_owned()))?;
            worktree_agents.push(worktree_agent(&assignment.role_name, assigned_agent));
        }
    }
    let server_facts = server_executor_facts(
        db,
        claiming_agent.iter().chain(worktree_agents.iter()),
        adapter_registry,
    )
    .await?;

    Ok(PreparedSelection {
        project: authority.project,
        repo: authority.repo,
        settings,
        review_config,
        claiming_agent,
        worktree_agents,
        server_facts,
    })
}
impl PreparedSelection {
    pub(crate) async fn load_context(
        &self,
        db: &SqliteDb,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        registry: &crate::daemon_transport::DaemonConnectionRegistry,
        task: &Task,
        workspace_id: Option<&str>,
        fallback_path: Option<&Path>,
    ) -> Result<Option<SelectionContext>> {
        let Some(claiming) = self.claiming_agent.as_ref() else {
            return Ok(None);
        };
        let fallback_server_location =
            fallback_path
                .filter(|path| path.is_dir())
                .map(|path| db::RepoLocation {
                    id: self.repo.id.clone(),
                    repo_id: self.repo.id.clone(),
                    owner_kind: RepoLocationOwnerKind::Server,
                    daemon_id: None,
                    runtime_id: None,
                    path: path.to_string_lossy().into_owned(),
                    kind: if self.repo.local_path.is_some() {
                        RepoLocationKind::PrimaryCheckout
                    } else {
                        RepoLocationKind::ManagedClone
                    },
                    is_default: true,
                    status: RepoLocationStatus::Ready,
                    last_verified_at: None,
                    last_error: None,
                    version: 1,
                    created_at: self.repo.created_at.clone(),
                    updated_at: self.repo.updated_at.clone(),
                });
        Ok(Some(
            load_selection_context(
                db,
                tx,
                registry,
                SelectionLoadInput {
                    task,
                    repo: &self.repo,
                    claiming_agent: claiming,
                    worktree_agents: &self.worktree_agents,
                    task_owner_id: self.project.owner_id.as_deref(),
                    workspace_id: workspace_id.filter(|_| task.parent_task_id.is_none()),
                    inherited_root_workspace_id: workspace_id
                        .filter(|_| task.parent_task_id.is_some()),
                    review_config: &self.review_config,
                    project_settings: &self.settings,
                    server: &self.server_facts,
                    handshakes: &connection_handshakes(registry),
                    fallback_server_location,
                },
            )
            .await?,
        ))
    }
}
pub(crate) fn worktree_agent(role: &str, agent: Agent) -> crate::placement::WorktreeAgent {
    crate::placement::WorktreeAgent {
        role: role.to_owned(),
        agent,
        required_capabilities: api_types::ExecutorAdapterCapabilityFacts {
            cancel_ack: true,
            terminal_observed: true,
            ..Default::default()
        },
    }
}

pub(crate) fn connection_handshakes(
    registry: &crate::daemon_transport::DaemonConnectionRegistry,
) -> std::collections::BTreeMap<String, crate::placement::ConnectionHandshake> {
    registry
        .connection_snapshots()
        .into_iter()
        .map(|(id, facts)| {
            (
                id,
                crate::placement::ConnectionHandshake {
                    connection_id: facts.connection_id,
                    handshake: facts.handshake,
                },
            )
        })
        .collect()
}

pub(crate) async fn server_executor_facts<'a>(
    db: &SqliteDb,
    agents: impl Iterator<Item = &'a crate::placement::WorktreeAgent>,
    registry: Option<&executors::AdapterRegistry>,
) -> Result<crate::placement::ServerFacts> {
    let default_registry;
    let registry = match registry {
        Some(registry) => registry,
        None => {
            default_registry = cli_adapters::default_registry();
            &default_registry
        }
    };
    let mut server = crate::placement::ServerFacts {
        execution_daemon_id: sqlx::query_scalar::<_, String>(
            "SELECT id FROM daemon WHERE machine_id = ? AND status <> 'offline'",
        )
        .bind(crate::embedded_daemon::embedded_machine_id())
        .fetch_optional(db.pool())
        .await?,
        ..Default::default()
    };
    for role in agents {
        let agent = &role.agent;
        let available = if agent.backend_kind == "native" {
            db::AgentConnectionHealthRepo::get_connection_health(db, &agent.profile_id)
                .await?
                .is_some_and(|health| health.status == "healthy")
        } else {
            agent
                .executor_type
                .parse::<ExecutorKind>()
                .ok()
                .and_then(|kind| registry.get(&kind))
                .is_some_and(|adapter| {
                    matches!(
                        adapter.check_availability().status,
                        executors::AvailabilityStatus::Authenticated
                    )
                })
        };
        server.executors.insert(
            agent.id.clone(),
            crate::placement::ExecutorFacts {
                installed: available,
                authenticated: available,
                enabled: !agent.paused,
                capabilities:
                    crate::daemon_transport::EmbeddedExecutionProvider::adapter_capabilities(
                        &agent.executor_type,
                    ),
            },
        );
    }
    Ok(server)
}
