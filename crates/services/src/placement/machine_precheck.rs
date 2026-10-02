//! Advisory, read-only capacity checks. Reservation/start transactions remain authoritative.
use crate::{daemon_transport::DaemonConnectionRegistry, Result, ServiceError};
use db::{Agent, AgentRepo, PlacementState, ProjectRepo, RepoRepo, Task, WorkspacePlacementRepo};
use sqlx::Row;

pub(crate) async fn snapshot(
    db: &db::SqliteDb,
) -> Result<Vec<db::machine_capacity::MachineCapacityRow>> {
    let mut tx = db.pool().begin().await?;
    Ok(db::machine_capacity::list_machine_capacity(
        &mut tx,
        &config::embedded_machine_id(),
        db.server_run_cap.effective(),
    )
    .await?)
}

pub(crate) async fn any_full(db: &db::SqliteDb) -> Result<bool> {
    Ok(snapshot(db)
        .await?
        .iter()
        .any(|row| !row.capacity.has_capacity()))
}

/// Consult recorded placement/checkout routing and current connection facts,
/// never prepare, verify a clone, sweep reservations, or acquire a writer lock.
pub(crate) async fn task_blocked(
    db: &db::SqliteDb,
    task: &Task,
    agent: &Agent,
    connections: Option<&DaemonConnectionRegistry>,
) -> Result<bool> {
    let capacities = snapshot(db).await?;
    if capacities.iter().all(|row| row.capacity.has_capacity()) {
        return Ok(false);
    }
    if !crate::agent_capacity::has_execution_capacity(db, agent).await? {
        return Ok(false);
    }
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
    let Some(repo_id) = project.primary_repo_id.as_deref() else {
        return Ok(false);
    };
    let repo = RepoRepo::get_by_id(db, repo_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("repository", repo_id))?;
    let mut agents = vec![agent.clone()];
    for role in ["coder", "reviewer", "planner"] {
        if let Some(resolved) =
            crate::task_hierarchy::effective_role_assignment(db, task, role).await?
        {
            if resolved.assignment.assignee_type == Some(db::AssigneeKind::Agent) {
                if let Some(id) = resolved.assignment.assignee_id.as_deref() {
                    if !agents.iter().any(|agent| agent.id == id) {
                        agents.push(
                            AgentRepo::get_by_id(db, id)
                                .await?
                                .ok_or_else(|| ServiceError::not_found("agent", id))?,
                        );
                    }
                }
            }
        }
    }
    if agents
        .iter()
        .any(|agent| agent.paused || agent.status == db::AgentStatus::Error)
    {
        return Ok(false);
    }
    let embedded: Option<String> =
        sqlx::query_scalar("SELECT id FROM daemon WHERE machine_id = ? AND status <> 'offline'")
            .bind(config::embedded_machine_id())
            .fetch_optional(db.pool())
            .await?;
    let binding = WorkspacePlacementRepo::get_for_task(db, &task.id)
        .await?
        .filter(|p| !matches!(p.state, PlacementState::Failed | PlacementState::Cleaned));
    let mut locations: Vec<(String, Option<String>, String, Option<String>)> = sqlx::query_as(
        "SELECT id, daemon_id, owner_kind, runtime_id FROM repo_location WHERE repo_id = ? AND status = 'ready'")
        .bind(&repo.id).fetch_all(db.pool()).await?;
    // The reserve path creates a server checkout location when none exists.
    if locations.is_empty() && binding.is_none() {
        locations.push((String::new(), embedded.clone(), "server".to_owned(), None));
    }
    let handshakes = connections
        .map(DaemonConnectionRegistry::connection_snapshots)
        .unwrap_or_default();
    let mut eligible = false;
    for (location_id, location_daemon, owner, runtime_id) in locations {
        if binding
            .as_ref()
            .is_some_and(|p| p.repo_location_id != location_id)
        {
            continue;
        }
        let provider = binding
            .as_ref()
            .and_then(|p| p.execution_daemon_id.clone().or(p.daemon_id.clone()))
            .or(location_daemon)
            .or_else(|| (owner == "server").then(|| embedded.clone()).flatten());
        if agents.iter().any(|agent| {
            agent
                .daemon_id
                .as_ref()
                .is_some_and(|pin| Some(pin) != provider.as_ref())
        }) {
            continue;
        }
        let remote = provider
            .as_ref()
            .is_some_and(|id| Some(id) != embedded.as_ref());
        if owner == "daemon" && agents.iter().any(|agent| agent.backend_kind != "cli") {
            continue;
        }
        if remote {
            let id = provider.as_deref().expect("remote provider");
            let Some(connection) = connections.and_then(|registry| registry.get(id)) else {
                continue;
            };
            let Some(facts) = handshakes.get(id) else {
                continue;
            };
            if connection.is_stale()
                || !connection.protocol_allows_dispatch()
                || !facts
                    .handshake
                    .capabilities
                    .iter()
                    .any(|value| value == "workspace.v1")
            {
                continue;
            }
            let ready: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime WHERE id = ? AND daemon_id = ? AND status = 'ready')")
                .bind(runtime_id.as_deref()).bind(id).fetch_one(db.pool()).await?;
            if !ready
                || agents.iter().any(|agent| {
                    !facts
                        .handshake
                        .executor_capabilities
                        .get(&agent.executor_type)
                        .is_some_and(|caps| caps.cancel_ack && caps.terminal_observed)
                })
            {
                continue;
            }
            let row = sqlx::query(
                "SELECT owner_id, visibility, detected_clis_json FROM daemon WHERE id = ?",
            )
            .bind(id)
            .fetch_one(db.pool())
            .await?;
            let owner_id: Option<String> = row.try_get("owner_id")?;
            if row.try_get::<String, _>("visibility")? != "global"
                && owner_id.is_some()
                && owner_id != project.owner_id
            {
                continue;
            }
            let clis: serde_json::Value =
                serde_json::from_str(&row.try_get::<String, _>("detected_clis_json")?)
                    .unwrap_or_default();
            if agents.iter().any(|agent| {
                !clis.as_array().is_some_and(|clis| {
                    clis.iter().any(|cli| {
                        cli["kind"].as_str() == Some(&agent.executor_type)
                            && cli["availability"] == "authenticated"
                    })
                })
            }) {
                continue;
            }
        }
        eligible = true;
        let machine = if remote { provider.as_deref() } else { None };
        if capacities
            .iter()
            .find(|row| row.daemon_id.as_deref() == machine)
            .is_some_and(|row| row.capacity.has_capacity())
        {
            return Ok(false);
        }
    }
    Ok(eligible)
}
