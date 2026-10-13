//! Worker health reads for the operator status projection.
use crate::SqliteDb;

impl SqliteDb {
    pub async fn task_step_worker_error(
        &self,
    ) -> crate::Result<Option<(Option<String>, Option<String>, i64)>> {
        let result = sqlx::query_as("SELECT last_error,last_error_at,restart_count FROM worker_health WHERE worker_name='task_steps'")
            .fetch_optional(self.pool()).await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn periodic_worker_error(
        &self,
        worker_name: &str,
    ) -> crate::Result<Option<(Option<String>, Option<String>, i64)>> {
        Ok(sqlx::query_as("SELECT last_error, last_error_at, restart_count FROM worker_health WHERE worker_name = ?")
            .bind(worker_name)
            .fetch_optional(self.pool()).await?)
    }
}
