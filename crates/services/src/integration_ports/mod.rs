//! Production ports of the integration queue worker (plan 3.2 stage D2a).
//!
//! The worker (`integration_worker`) holds no Git, Task or transport
//! capability of its own; these are the real things behind its seams:
//!
//! | worker port | here | over |
//! |---|---|---|
//! | `IntegrationOwnerPort` | [`RoutingIntegrationOwner`] | `ServerIntegrationOwner`, or `DaemonWorkspaceClient::integration_effect` for a daemon-owned target |
//! | `ObjectTransferPort` | [`OwnerObjectTransfer`] | the owners' `imported_objects` / `export_objects` / `import_objects` / `release_objects` |
//! | `IntegrationFactsPort` | [`WorkspaceHeadFacts`] | storage and the workspace placement's Git reads |
//! | `IntegrationStepPort` | `integration_steps::TaskStepIntegrationPort` | the Task-step queue |
//!
//! [`build_integration_worker`] assembles them. Nothing calls it from the
//! runtime yet: the queue stays passive until the cutover (stage D2b).
mod facts;
mod owner;
#[cfg(test)]
mod tests;
mod transfer;

pub use facts::WorkspaceHeadFacts;
pub use owner::{DaemonFences, RoutingIntegrationOwner};
pub use transfer::OwnerObjectTransfer;

use crate::{
    daemon_transport::{workspace_client::DaemonWorkspaceClient, DaemonConnectionRegistry},
    integration_owner::ServerIntegrationOwner,
    integration_steps::TaskStepIntegrationPort,
    integration_worker::{
        IntegrationLocationPort, IntegrationQueueWorker, IntegrationWorkerConfig, SystemClock,
    },
    repo_location::RepoLocationService,
    workspace_backend::WorkspaceBackendRouter,
    Result,
};
use async_trait::async_trait;
use db::SqliteDb;
use std::sync::Arc;

/// The integration queue worker over its production ports, not started.
///
/// The caller starts it (`IntegrationQueueWorker::start`) after owner
/// reconciliation and the importer, and hands the same `config` to
/// `IntegrationSteps::with_timers` so both sides of the Task-step handshake
/// use one set of timers. The worker's first act is the transfer sweep
/// (`ObjectTransferPort::sweep_at_start`): leftovers of a crashed transfer in
/// every server-owned checkout, and imported refs of attempts off the slot.
///
/// The returned worker is also the `IntegrationEnqueuePort` admission calls
/// after its commit, and the `IntegrationSnapshotPort` of the queue reads.
pub fn build_integration_worker(
    db: Arc<SqliteDb>,
    router: Arc<WorkspaceBackendRouter>,
    daemons: Arc<DaemonConnectionRegistry>,
    locations: Arc<RepoLocationService>,
    config: IntegrationWorkerConfig,
) -> Arc<IntegrationQueueWorker> {
    let server = Arc::new(ServerIntegrationOwner::new(Arc::clone(&db)));
    let client = DaemonWorkspaceClient::new(daemons).with_receipts(Arc::clone(&db));
    let fences = Arc::new(DaemonFences::new(Arc::clone(&db), client.clone()));
    let owner = Arc::new(RoutingIntegrationOwner::new(
        Arc::clone(&server),
        client.clone(),
        Arc::clone(&fences),
    ));
    let transfer = Arc::new(OwnerObjectTransfer::new(
        Arc::clone(&db),
        server,
        client,
        fences,
        &std::env::temp_dir().join("forge-integration-transfer"),
    ));
    Arc::new(
        IntegrationQueueWorker::new(
            Arc::clone(&db),
            owner,
            Arc::new(TaskStepIntegrationPort::new(Arc::clone(&db))),
            Arc::new(WorkspaceHeadFacts::new(Arc::clone(&db), router)),
            transfer,
            Arc::new(SystemClock),
            config,
        )
        .with_locations(locations),
    )
}

#[async_trait]
impl IntegrationLocationPort for RepoLocationService {
    async fn verify_location(&self, repo_location_id: &str) -> Result<()> {
        self.reverify(repo_location_id).await.map(|_| ())
    }
}
