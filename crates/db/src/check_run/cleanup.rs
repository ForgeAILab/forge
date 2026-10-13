//! Check liveness for Task workspace reclamation.
use crate::SqliteDb;

impl SqliteDb {
    pub async fn task_has_live_cleanup_check(&self, task_id: &str) -> crate::Result<i64> {
        let result = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (
                SELECT 1 FROM check_consumer c JOIN check_run r ON r.id = c.run_id
                WHERE c.task_id = ?
                  AND r.state IN ('running', 'cancelling', 'cleaning', 'uncertain')
             )",
        )
        .bind(task_id)
        .fetch_one(self.pool())
        .await?;
        Ok(result)
    }
}
