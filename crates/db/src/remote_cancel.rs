use crate::{
    begin_immediate, now_rfc3339, DbError, Result, SqliteDb, TaskStep, WorkspacePlacement,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteTaskOperation {
    pub operation_id: String,
    pub step_id: String,
    pub workspace_id: String,
    pub placement_id: String,
    pub daemon_id: String,
    pub runtime_id: String,
    pub generation: i64,
    pub expected_epoch: i64,
    pub created_at: String,
}

fn row_operation(row: sqlx::sqlite::SqliteRow) -> RemoteTaskOperation {
    RemoteTaskOperation {
        operation_id: row.get("operation_id"),
        step_id: row.get("step_id"),
        workspace_id: row.get("workspace_id"),
        placement_id: row.get("placement_id"),
        daemon_id: row.get("daemon_id"),
        runtime_id: row.get("runtime_id"),
        generation: row.get("generation"),
        expected_epoch: row.get("expected_epoch"),
        created_at: row.get("created_at"),
    }
}

impl SqliteDb {
    pub async fn register_remote_task_operation(
        &self,
        step: &TaskStep,
        placement: &WorkspacePlacement,
        operation_id: &str,
    ) -> Result<()> {
        let mut tx = begin_immediate(self.pool()).await?;
        self.fence_hook_in_tx(&mut tx, step).await?;
        let removed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM daemon WHERE id=? AND removed_at IS NOT NULL)",
        )
        .bind(placement.daemon_id.as_deref())
        .fetch_one(&mut *tx)
        .await?;
        if removed {
            return Err(DbError::NotFound);
        }
        sqlx::query("INSERT INTO task_remote_operation(operation_id,step_id,workspace_id,placement_id,daemon_id,runtime_id,generation,expected_epoch,state,created_at) VALUES(?,?,?,?,?,?,?,?,'running',?) ON CONFLICT(operation_id) DO NOTHING")
            .bind(operation_id).bind(&step.id).bind(&placement.workspace_id).bind(&placement.id)
            .bind(placement.daemon_id.as_deref().ok_or(DbError::NotFound)?)
            .bind(placement.runtime_id.as_deref().ok_or(DbError::NotFound)?)
            .bind(placement.generation).bind(step.expected_epoch).bind(now_rfc3339()).execute(&mut *tx).await?;
        tx.commit().await?;
        self.domain_event_notify().notify_waiters();
        Ok(())
    }

    /// A late orphan result cannot turn an unconfirmed cancellation into a
    /// finished operation or clear its workspace exclusion.
    pub async fn finish_remote_task_operation(
        &self,
        step: &TaskStep,
        operation_id: &str,
    ) -> Result<()> {
        let mut tx = begin_immediate(self.pool()).await?;
        self.fence_hook_in_tx(&mut tx, step).await?;
        sqlx::query("UPDATE task_remote_operation SET state='finished' WHERE operation_id=? AND step_id=? AND expected_epoch=? AND state='running'")
            .bind(operation_id).bind(&step.id).bind(step.expected_epoch).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn running_remote_task_operations(
        &self,
        step_id: &str,
    ) -> Result<Vec<RemoteTaskOperation>> {
        Ok(
            sqlx::query("SELECT * FROM task_remote_operation WHERE step_id=? AND state='running'")
                .bind(step_id)
                .fetch_all(self.pool())
                .await?
                .into_iter()
                .map(row_operation)
                .collect(),
        )
    }
    pub async fn running_remote_operations_for_task(
        &self,
        task_id: &str,
    ) -> Result<Vec<RemoteTaskOperation>> {
        Ok(sqlx::query("SELECT r.* FROM task_remote_operation r JOIN task_step s ON s.id=r.step_id WHERE s.task_id=? AND r.state='running'")
            .bind(task_id).fetch_all(self.pool()).await?.into_iter().map(row_operation).collect())
    }
    pub async fn task_has_pending_remote_cancel(&self, task_id: &str) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_remote_cancel r JOIN workspace w ON w.id=r.workspace_id WHERE w.task_id=? OR EXISTS(SELECT 1 FROM execution e WHERE e.task_id=? AND e.workspace_id=w.id))")
            .bind(task_id).bind(task_id).fetch_one(self.pool()).await?)
    }

    /// Machines whose unconfirmed cancellation still fences this Task's
    /// workspace: the hostname, or the daemon id if the registration is gone.
    pub async fn task_pending_remote_cancel_machines(&self, task_id: &str) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT DISTINCT COALESCE(d.hostname,r.daemon_id) FROM pending_remote_cancel r JOIN workspace w ON w.id=r.workspace_id LEFT JOIN daemon d ON d.id=r.daemon_id WHERE w.task_id=? OR EXISTS(SELECT 1 FROM execution e WHERE e.task_id=? AND e.workspace_id=w.id) ORDER BY 1")
            .bind(task_id).bind(task_id).fetch_all(self.pool()).await?)
    }

    pub async fn mark_pending_remote_cancel(&self, operation: &RemoteTaskOperation) -> Result<()> {
        // Orphan cleanup intentionally has no registration FK. Only explicit
        // revocation may suppress a marker for still-running remote work.
        let mut tx = begin_immediate(self.pool()).await?;
        sqlx::query("INSERT INTO pending_remote_cancel(operation_id,step_id,workspace_id,placement_id,daemon_id,runtime_id,generation,expected_epoch,created_at) SELECT operation_id,step_id,workspace_id,placement_id,daemon_id,runtime_id,generation,expected_epoch,created_at FROM task_remote_operation WHERE operation_id=? AND step_id=? AND state='running' AND NOT EXISTS(SELECT 1 FROM daemon d WHERE d.id=task_remote_operation.daemon_id AND d.removed_at IS NOT NULL) ON CONFLICT DO NOTHING")
            .bind(&operation.operation_id).bind(&operation.step_id).execute(&mut *tx).await?;
        let tasks: Vec<String> = sqlx::query_scalar("SELECT id FROM task WHERE id=(SELECT task_id FROM task_step WHERE id=?) OR id=(SELECT task_id FROM workspace WHERE id=?)")
            .bind(&operation.step_id)
            .bind(&operation.workspace_id)
            .fetch_all(&mut *tx)
            .await?;
        for task in tasks {
            crate::task_condition::sync_condition(&mut tx, &task).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn acknowledge_remote_cancel(&self, operation: &RemoteTaskOperation) -> Result<()> {
        let mut tx = begin_immediate(self.pool()).await?;
        sqlx::query("DELETE FROM pending_remote_cancel WHERE operation_id=? AND step_id=? AND placement_id=? AND generation=? AND expected_epoch=?")
            .bind(&operation.operation_id).bind(&operation.step_id).bind(&operation.placement_id)
            .bind(operation.generation).bind(operation.expected_epoch).execute(&mut *tx).await?;
        sqlx::query(
            "UPDATE task_remote_operation SET state='cancelled' WHERE operation_id=? AND step_id=?",
        )
        .bind(&operation.operation_id)
        .bind(&operation.step_id)
        .execute(&mut *tx)
        .await?;
        let tasks:Vec<String>=sqlx::query_scalar("SELECT id FROM task WHERE deleted_at IS NULL AND (id=(SELECT task_id FROM workspace WHERE id=?) OR id IN (SELECT task_id FROM execution WHERE workspace_id=?) OR id=(SELECT task_id FROM task_step WHERE id=?))")
            .bind(&operation.workspace_id).bind(&operation.workspace_id).bind(&operation.step_id).fetch_all(&mut *tx).await?;
        for task_id in tasks {
            crate::task_condition::sync_condition(&mut tx, &task_id).await?;
            if !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pending_remote_cancel WHERE workspace_id=?)",
            )
            .bind(&operation.workspace_id)
            .fetch_one(&mut *tx)
            .await?
            {
                // Clears only its own marker: identity-fenced, never dropped.
                self.enqueue_fenced_task_mutation_in_tx(&mut tx,&task_id,crate::TaskMutation::Sql {
                    task_id:task_id.clone(),
                    query:"UPDATE task SET error_annotation=CASE WHEN json_valid(error_annotation) AND json_extract(error_annotation,'$.blocking_reason')='pending_remote_cancel' THEN NULL ELSE error_annotation END, metadata_json=CASE WHEN json_valid(metadata_json) THEN json_remove(metadata_json,'$.dispatch_disposition','$.deferred_dispatch') ELSE metadata_json END,version=version+1,updated_at=? WHERE id=? AND deleted_at IS NULL".to_owned(),
                    arguments:vec![serde_json::json!(now_rfc3339()),serde_json::json!(task_id)],
                },crate::task_writer::EffectFence::Identity).await?;
            }
        }
        tx.commit().await?;
        self.domain_event_notify().notify_waiters();
        Ok(())
    }

    pub async fn workspace_remote_cancels(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> Result<Vec<RemoteTaskOperation>> {
        Ok(sqlx::query("SELECT r.* FROM pending_remote_cancel r LEFT JOIN task_step s ON s.id=r.step_id WHERE r.workspace_id=? OR s.task_id=? ORDER BY r.created_at,r.operation_id")
            .bind(workspace_id).bind(task_id).fetch_all(self.pool()).await?.into_iter().map(row_operation).collect())
    }
    pub async fn pending_remote_cancels(
        &self,
        daemon_id: Option<&str>,
        workspace_id: Option<&str>,
    ) -> Result<Vec<RemoteTaskOperation>> {
        Ok(sqlx::query("SELECT * FROM pending_remote_cancel WHERE (? IS NULL OR daemon_id=?) AND (? IS NULL OR workspace_id=?) ORDER BY created_at,operation_id")
            .bind(daemon_id).bind(daemon_id).bind(workspace_id).bind(workspace_id)
            .fetch_all(self.pool()).await?.into_iter().map(row_operation).collect())
    }
}
