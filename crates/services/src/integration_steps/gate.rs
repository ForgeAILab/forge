//! The Task gate of the integration queue: the one rule for "this Task is
//! still in its merge state, and may not be integrated now".
//!
//! Read by the queue worker before every effect (`IntegrationFactsPort::
//! task_gate`), by the `settle` step in the transaction that would write the
//! permit, and by the reconciler before it admits a Task. It is level
//! triggered: nothing records "gated" anywhere, the Task's own columns are the
//! fact, so the gate lifts in the write of the owner command that clears them.
use crate::{integration_worker::GateCause, Result};
use db::SqliteDb;

/// The gate the Task's own row states: a Hold, a block, a failure, a blocked
/// entry or a wait for a person.
pub(crate) fn task_row_gate(task: &db::Task) -> Option<GateCause> {
    let json = |raw: &Option<String>| {
        raw.as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
    };
    let manual_stop = |value: &serde_json::Value| {
        value["kind"] == "manual_stop" || value["type"] == "manual_stop"
    };
    let annotation = json(&task.error_annotation);
    if matches!(
        &task.condition,
        db::TaskCondition::Parked {
            primary: db::ParkReason::Held { .. },
            ..
        }
    ) || annotation.as_ref().is_some_and(manual_stop)
        || json(&task.blocked_json).as_ref().is_some_and(manual_stop)
    {
        return Some(GateCause::Held);
    }
    if task.blocked_json.is_some() {
        return Some(GateCause::Blocked);
    }
    if task.failed_json.is_some() {
        return Some(GateCause::Failed);
    }
    if json(&task.entry_barrier_json).is_some_and(|barrier| barrier["status"] == "blocked") {
        return Some(GateCause::EntryBlocked);
    }
    if json(&task.metadata_json).is_some_and(|metadata| metadata["awaiting_human"] == true) {
        return Some(GateCause::AwaitingHuman);
    }
    None
}

/// The whole gate: the Task's row, then its Project, then the Agent it is
/// assigned to. `None`: the Task is gone or nothing stops it.
pub(crate) async fn task_gate(db: &SqliteDb, task_id: &str) -> Result<Option<GateCause>> {
    let Some(task) = db::TaskRepo::get_by_id(db, task_id, false).await? else {
        return Ok(None);
    };
    if let Some(cause) = task_row_gate(&task) {
        return Ok(Some(cause));
    }
    let project_paused: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM project WHERE id=? AND paused_at IS NOT NULL)",
    )
    .bind(&task.project_id)
    .fetch_one(db.pool())
    .await
    .map_err(db::DbError::from)?;
    if project_paused {
        return Ok(Some(GateCause::ProjectPaused));
    }
    if let (Some("agent"), Some(agent_id)) =
        (task.assignee_type.as_deref(), task.assignee_id.as_deref())
    {
        if db::AgentRepo::get_by_id(db, agent_id)
            .await?
            .is_some_and(|agent| agent.paused)
        {
            return Ok(Some(GateCause::AgentPaused));
        }
    }
    Ok(None)
}
