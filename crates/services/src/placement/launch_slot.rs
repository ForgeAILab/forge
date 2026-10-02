//! Cancellation cleanup for a launch's durable reservation.
use std::sync::Arc;

pub(crate) struct LaunchSlot {
    db: Arc<db::SqliteDb>,
    placement_id: String,
    deadline: Option<String>,
}
impl LaunchSlot {
    pub(crate) fn new(db: Arc<db::SqliteDb>, placement: &db::WorkspacePlacement) -> Self {
        Self {
            db,
            placement_id: placement.id.clone(),
            deadline: placement.reserved_until.clone(),
        }
    }
}
impl Drop for LaunchSlot {
    fn drop(&mut self) {
        let Some(deadline) = self.deadline.clone() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let db = Arc::clone(&self.db);
        let id = self.placement_id.clone();
        // The deadline is the launch token. A later launch has a new deadline;
        // successful INSERT clears this one in its own transaction.
        runtime.spawn(async move {
            if let Err(error) = sqlx::query("UPDATE workspace_placement SET reserved_until = NULL,
                state = CASE WHEN state IN ('reserved', 'preparing') THEN CASE WHEN workspace_handle IS NULL THEN 'failed' ELSE 'ready' END ELSE state END,
                failure_cause = CASE WHEN state IN ('reserved', 'preparing') AND workspace_handle IS NULL THEN 'prepare_failed' ELSE failure_cause END,
                version = version + 1, updated_at = ?
                WHERE id = ? AND reserved_until = ?
                  AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.workspace_id = workspace_placement.workspace_id AND e.status = 'running')")
                .bind(db::now_rfc3339()).bind(id).bind(deadline).execute(db.pool()).await {
                tracing::warn!(%error, "could not release abandoned launch slot; its deadline still bounds it");
            }
        });
    }
}
