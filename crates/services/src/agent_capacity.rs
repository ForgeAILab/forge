use db::Agent;

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

/// Agent availability uses only the identity quota. Machine saturation is a
/// placement refusal, not an unhealthy or unconfigured Agent/Project.
pub(crate) async fn has_execution_capacity(db: &db::SqliteDb, agent: &Agent) -> Result<bool> {
    let mut transaction = db.pool().begin().await?;
    Ok(
        crate::placement::capacity::count_agent_capacity(&mut transaction, &agent.id)
            .await?
            .has_capacity(agent.max_concurrent_tasks),
    )
}
