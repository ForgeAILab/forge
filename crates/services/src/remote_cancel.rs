//! Remote cleanup is eventual; the durable marker excludes the workspace.
use crate::{
    daemon_transport::{workspace_client::DaemonWorkspaceClient, DaemonConnectionRegistry},
    Result,
};
use std::{sync::Arc, time::Duration};

pub(crate) async fn cancel_operations(
    db: &db::SqliteDb,
    registry: Option<Arc<DaemonConnectionRegistry>>,
    operations: &[db::RemoteTaskOperation],
) -> Result<bool> {
    for operation in operations {
        db.mark_pending_remote_cancel(operation).await?;
    }
    if let Some(registry) = registry {
        let client = DaemonWorkspaceClient::new(registry);
        let attempt = async {
            for operation in operations {
                if client
                    .cancel_workspace_operation(&operation.daemon_id, &operation.operation_id)
                    .await
                    .is_ok()
                {
                    db.acknowledge_remote_cancel(operation).await?;
                }
            }
            Ok::<(), db::DbError>(())
        };
        if let Ok(result) = tokio::time::timeout(Duration::from_secs(10), attempt).await {
            result?
        }
    }
    let pending = db.pending_remote_cancels(None, None).await?;
    Ok(operations.iter().any(|operation| {
        pending
            .iter()
            .any(|p| p.operation_id == operation.operation_id)
    }))
}

pub(crate) async fn reconcile(
    db: &db::SqliteDb,
    registry: Arc<DaemonConnectionRegistry>,
    daemon_id: &str,
) -> Result<()> {
    let operations = db.pending_remote_cancels(Some(daemon_id), None).await?;
    let _ = cancel_operations(db, Some(registry), &operations).await?;
    Ok(())
}
