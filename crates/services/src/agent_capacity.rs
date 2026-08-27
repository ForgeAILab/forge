use db::{Agent, DaemonRepo};
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

pub(crate) async fn has_running_execution_capacity(
    db: &db::SqliteDb,
    agent: &Agent,
) -> Result<bool> {
    let running_count = count_running_executions(db, &agent.id).await?;
    if running_count >= agent.max_concurrent_tasks {
        return Ok(false);
    }

    let Some(daemon_id) = agent.daemon_id.as_deref() else {
        return Ok(true);
    };
    let Some(daemon) = DaemonRepo::get_by_id(db, daemon_id).await? else {
        return Ok(true);
    };
    let Some(max_sessions) = daemon_session_cap_from_labels(&daemon.labels_json) else {
        return Ok(true);
    };
    let running_count = count_running_executions_for_daemon(db, daemon_id).await?;
    Ok(running_count < max_sessions)
}

pub(crate) async fn count_running_executions_for_daemon(
    db: &db::SqliteDb,
    daemon_id: &str,
) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)
         FROM execution
         JOIN agent_current AS agent ON agent.id = execution.agent_id
         WHERE agent.daemon_id = ? AND execution.status = 'running'",
    )
    .bind(daemon_id)
    .fetch_one(db.pool())
    .await?)
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
