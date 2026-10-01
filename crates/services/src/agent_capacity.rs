use db::{Agent, WorkspacePlacementRepo};
use serde_json::Value;

use crate::Result;

pub(crate) async fn count_running_executions(db: &db::SqliteDb, agent_id: &str) -> Result<i64> {
    // Main and Project Agent chat turns are coordination work, not Task
    // executions. They intentionally do not consume the identity's Task
    // concurrency quota; the same identity may still be assigned a Task and
    // receives a separate Task-scoped context/lease for that attempt.
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM execution WHERE agent_id = ? AND status = 'running'",
    )
    .bind(agent_id)
    .fetch_one(db.pool())
    .await?)
}

pub(crate) async fn count_occupied_agent_slots(db: &db::SqliteDb, agent_id: &str) -> Result<i64> {
    let mut transaction = db.pool().begin().await?;
    Ok(
        crate::placement::capacity::count_agent_capacity(&mut transaction, agent_id)
            .await?
            .occupied_slots(),
    )
}

pub(crate) async fn has_execution_capacity(
    db: &db::SqliteDb,
    agent: &Agent,
    workspace_id: Option<&str>,
) -> Result<bool> {
    let mut transaction = db.pool().begin().await?;
    if !crate::placement::capacity::count_agent_capacity(&mut transaction, &agent.id)
        .await?
        .has_capacity(agent.max_concurrent_tasks)
    {
        return Ok(false);
    }
    let placement = match workspace_id {
        Some(id) => {
            WorkspacePlacementRepo::get_by_workspace_id_in_tx(db, &mut transaction, id).await?
        }
        None => None,
    };
    let daemon_id = if workspace_id.is_some() {
        placement.as_ref().and_then(|placement| {
            placement
                .execution_daemon_id
                .as_deref()
                .or(placement.daemon_id.as_deref())
        })
    } else {
        agent.daemon_id.as_deref()
    };
    let Some(daemon_id) = daemon_id else {
        return Ok(true);
    };
    let labels = sqlx::query_scalar::<_, String>("SELECT labels_json FROM daemon WHERE id = ?")
        .bind(daemon_id)
        .fetch_optional(&mut *transaction)
        .await?;
    let max_sessions = labels.as_deref().and_then(daemon_session_cap_from_labels);
    Ok(
        crate::placement::capacity::count_daemon_capacity(
            &mut transaction,
            daemon_id,
            max_sessions,
        )
        .await?
        .has_capacity(),
    )
}

pub(crate) fn daemon_session_cap_from_labels(labels_json: &str) -> Option<i64> {
    let labels = serde_json::from_str::<Value>(labels_json).ok()?;
    [
        "max_concurrent_sessions",
        "max_sessions",
        "active_session_cap",
        "max_concurrent_tasks",
    ]
    .into_iter()
    .find_map(|key| {
        labels
            .get(key)
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
    })
}
