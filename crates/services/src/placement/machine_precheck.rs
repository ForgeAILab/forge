//! Advisory reads only; the reserve/start transactions decide admission.
use super::{select_placement, PlacementFilterCode, SelectionOutcome};
use crate::{
    daemon_transport::DaemonConnectionRegistry, workflow::engine::WorkflowEngine, Result,
    ServiceError,
};
use api_types::Actor;
use db::{Agent, PlacementState, ProjectRepo, Task, WorkspacePlacementRepo};
use std::path::Path;

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

/// `None` includes unknown verification/probe facts and non-capacity refusals.
/// Current environment failures exclude a machine from the usable set.
pub(crate) async fn task_blocked(
    db: &db::SqliteDb,
    task: &Task,
    agent: &Agent,
    connections: Option<&DaemonConnectionRegistry>,
    adapters: Option<&executors::AdapterRegistry>,
    workspace_root: &Path,
    role: Option<&str>,
) -> Result<Option<super::CapacityWait>> {
    if agent.paused || agent.status == db::AgentStatus::Error {
        return Ok(None);
    }
    if snapshot(db)
        .await?
        .iter()
        .all(|row| row.capacity.has_capacity())
        && db::machine_disk::list_machine_disks(db)
            .await?
            .iter()
            .all(|row| row.disk.pressure.is_none())
    {
        return Ok(None);
    }
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", &task.project_id))?;
    if project.primary_repo_id.is_none() {
        return Ok(None);
    }
    let workflow = WorkflowEngine::resolve_workflow_for_task(
        task,
        &project.workflow_definition,
        &Actor::system(api_types::SystemComponent::General),
    );
    let role = role
        .or_else(|| {
            workflow
                .states
                .iter()
                .find(|s| s.name == task.status)
                .and_then(crate::workflow::effective_role)
        })
        .unwrap_or("coder");
    let prepared = super::context::prepare_selection(db, task, Some(agent), role, adapters).await?;
    let binding = WorkspacePlacementRepo::get_for_task(db, &task.id).await?;
    if binding
        .as_ref()
        .is_some_and(|p| !matches!(p.state, PlacementState::Ready | PlacementState::Cleaned))
    {
        return Ok(None);
    }
    let locations: Vec<(String, String)> =
        sqlx::query_as("SELECT id, status FROM repo_location WHERE repo_id = ?")
            .bind(&prepared.repo.id)
            .fetch_all(db.pool())
            .await?;
    for (id, status) in &locations {
        if binding
            .as_ref()
            .is_some_and(|p| p.state != PlacementState::Cleaned && &p.repo_location_id != id)
        {
            continue;
        }
        if status != "ready" {
            return Ok(None);
        } // Reserve may verify it first.
    }
    let empty = DaemonConnectionRegistry::without_handlers();
    let connections = connections.unwrap_or(&empty);
    let path = prepared
        .repo
        .local_path
        .as_ref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| workspace_root.join(".repos").join(&prepared.repo.id));
    let mut tx = db.pool().begin().await?;
    let Some(context) = prepared
        .load_context(
            db,
            &mut tx,
            connections,
            task,
            binding.as_ref().map(|p| p.workspace_id.as_str()),
            Some(&path),
        )
        .await?
    else {
        return Ok(None);
    };
    Ok(match select_placement(&context) {
        SelectionOutcome::Unavailable(refusal) if capacity_only_wait(&refusal) => {
            Some(super::CapacityWait::of(&refusal))
        }
        _ => None,
    })
}

/// A known environment-failed machine is not an alternative to a full,
/// environment-ready machine. A pending probe remains unknown and wins no
/// capacity-only conclusion. Mixed failures alone never establish a wait.
pub(crate) fn capacity_only_wait(refusal: &super::PlacementUnavailable) -> bool {
    use PlacementFilterCode::*;
    refusal
        .rejected_candidates
        .iter()
        .any(|r| super::capacity_codes_only(&r.filter_codes))
        && refusal.rejected_candidates.iter().all(|r| {
            super::capacity_codes_only(&r.filter_codes)
                || (r.filter_codes.contains(&EnvironmentNotReady)
                    && r.filter_codes
                        .iter()
                        .all(|c| matches!(c, MachineCapacity | DiskPressure | EnvironmentNotReady)))
        })
}

/// Re-admitting a parked Task needs both machine and Project room. This uses
/// fresh counts for each candidate, including dispatches earlier in this tick.
pub(crate) async fn wait_before_dispatch(
    db: &db::SqliteDb,
    service: &crate::TaskService,
    task: &Task,
    agent: &Agent,
    role: Option<&str>,
) -> Result<bool> {
    if let Some(wait) = service.capacity_wait_for(task, agent, role).await? {
        crate::deferred_dispatch::record_capacity_wait(db, task, wait).await?;
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
