//! Owner-local workspace operations. Placement lifecycle updates belong to callers.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use db::{PlacementOwnerKind, SqliteDb, Workspace, WorkspacePlacement, WorkspacePlacementRepo};

use crate::ServiceError;

pub use crate::merge_service::MergeOutcome;
pub use api_types::{ExecutionOutboxEntry, WorkspaceRunPurpose};
pub use daemon::DaemonWorkspaceBackend;
pub use embedded::EmbeddedWorkspaceBackend;

mod daemon;
mod embedded;
mod outbox;
mod review;

pub(crate) fn consume_embedded_execution_outbox(worktree: &str, execution_id: &str) -> Result<()> {
    let Some(path) = executors::execution_outbox_path(std::path::Path::new(worktree), execution_id)
    else {
        return Ok(());
    };
    if !path.is_dir() {
        return Ok(());
    }
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub type Result<T> = std::result::Result<T, WorkspaceBackendError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareSpec {
    pub base_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedWorkspace {
    pub handle: String,
    pub base_sha: String,
    pub branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceState {
    pub exists: bool,
    pub head_sha: Option<String>,
    pub dirty: bool,
    pub branch: Option<String>,
    pub locked: bool,
    pub active_execution_ids: Vec<String>,
    pub journaled_execution_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSpec {
    pub purpose: WorkspaceRunPurpose,
    pub command: String,
    pub env: BTreeMap<String, String>,
    pub timeout_secs: u64,
    pub max_output_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    pub exit_code: i32,
    pub stdout_tail: String,
    pub stderr_tail: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffSpec {
    pub base_ref: String,
    /// None includes the current worktree, matching the existing diff service.
    pub head_ref: Option<String>,
    pub max_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct Diff {
    pub response: api_types::DiffResponse,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeSpec {
    pub target_branch: String,
    pub expected_target_sha: String,
    pub handed_off_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetSpec {
    pub expected_head_sha: String,
    pub base_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupAck {
    pub removed: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OutboxHarvest {
    pub entries: Vec<ExecutionOutboxEntry>,
    pub rejected: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkspaceBackendError {
    #[error(
        "stale_generation: placement {placement_id} has generation {actual}, expected {expected}"
    )]
    StaleGeneration {
        placement_id: String,
        expected: i64,
        actual: i64,
    },
    #[error("wrong_owner: placement {placement_id} does not belong to this backend")]
    WrongOwner { placement_id: String },
    #[error("purpose_denied: workspace run purpose {purpose:?} is refused by the owner")]
    PurposeDenied { purpose: WorkspaceRunPurpose },
    #[error("owner_unreachable: daemon {daemon_id}")]
    OwnerUnreachable { daemon_id: String },
    #[error("owner_unsupported: workspace owner {owner_kind}")]
    OwnerUnsupported { owner_kind: PlacementOwnerKind },
    #[error(transparent)]
    Other(Box<ServiceError>),
}

impl From<ServiceError> for WorkspaceBackendError {
    fn from(error: ServiceError) -> Self {
        Self::Other(Box::new(error))
    }
}

impl From<db::DbError> for WorkspaceBackendError {
    fn from(error: db::DbError) -> Self {
        ServiceError::from(error).into()
    }
}

impl From<git::GitError> for WorkspaceBackendError {
    fn from(error: git::GitError) -> Self {
        ServiceError::from(error).into()
    }
}

impl From<workspace::WorkspaceError> for WorkspaceBackendError {
    fn from(error: workspace::WorkspaceError) -> Self {
        match error {
            workspace::WorkspaceError::Git(error) => ServiceError::Git(error),
            workspace::WorkspaceError::Io(error) => ServiceError::Git(git::GitError::Io(error)),
            error => ServiceError::invalid_operation(error.to_string()),
        }
        .into()
    }
}

impl From<std::io::Error> for WorkspaceBackendError {
    fn from(error: std::io::Error) -> Self {
        ServiceError::Git(git::GitError::Io(error)).into()
    }
}

impl From<WorkspaceBackendError> for ServiceError {
    fn from(error: WorkspaceBackendError) -> Self {
        match error {
            WorkspaceBackendError::Other(error) => *error,
            WorkspaceBackendError::OwnerUnreachable { daemon_id } => {
                Self::DaemonUnavailable { daemon_id }
            }
            error @ WorkspaceBackendError::StaleGeneration { .. } => {
                Self::Conflict(error.to_string())
            }
            error @ (WorkspaceBackendError::WrongOwner { .. }
            | WorkspaceBackendError::PurposeDenied { .. }) => Self::AuthorizationDenied {
                message: error.to_string(),
            },
            error @ WorkspaceBackendError::OwnerUnsupported { .. } => {
                Self::invalid_operation(error.to_string())
            }
        }
    }
}

#[async_trait]
pub trait WorkspaceBackend: Send + Sync {
    fn daemon_client(
        &self,
    ) -> Option<&crate::daemon_transport::workspace_client::DaemonWorkspaceClient> {
        None
    }

    async fn prepare(
        &self,
        placement: &WorkspacePlacement,
        base: &PrepareSpec,
    ) -> Result<PreparedWorkspace>;
    async fn describe(&self, placement: &WorkspacePlacement) -> Result<WorkspaceState>;
    async fn run(&self, placement: &WorkspacePlacement, spec: &RunSpec) -> Result<RunResult>;
    async fn diff(&self, placement: &WorkspacePlacement, spec: &DiffSpec) -> Result<Diff>;
    /// Repository-relative reads, plus the existing sibling `../plan.md` artifact.
    async fn read(
        &self,
        placement: &WorkspacePlacement,
        rel_path: &str,
        limit: u64,
    ) -> Result<Vec<u8>>;
    async fn merge(&self, placement: &WorkspacePlacement, spec: &MergeSpec)
        -> Result<MergeOutcome>;
    async fn reset(
        &self,
        placement: &WorkspacePlacement,
        spec: &ResetSpec,
    ) -> Result<PreparedWorkspace>;
    async fn cleanup(&self, placement: &WorkspacePlacement) -> Result<CleanupAck>;
    async fn harvest_outbox(
        &self,
        placement: &WorkspacePlacement,
        execution_id: &str,
    ) -> Result<OutboxHarvest>;
    async fn consume_outbox(
        &self,
        placement: &WorkspacePlacement,
        execution_id: &str,
    ) -> Result<()>;
}

#[derive(Clone)]
pub struct ResolvedWorkspace {
    pub placement: WorkspacePlacement,
    pub backend: Arc<dyn WorkspaceBackend>,
}

impl ResolvedWorkspace {
    pub fn handle(&self) -> Result<&str> {
        workspace_handle(&self.placement)
    }

    /// Only server-owned handles are paths on the Forge host.
    pub fn embedded_path(&self) -> Result<PathBuf> {
        embedded_path(&self.placement)
    }
}

fn workspace_handle(placement: &WorkspacePlacement) -> Result<&str> {
    placement
        .workspace_handle
        .as_deref()
        .filter(|handle| !handle.is_empty())
        .ok_or_else(|| {
            ServiceError::invalid_operation(format!(
                "workspace placement {} has no prepared handle",
                placement.id
            ))
            .into()
        })
}

fn embedded_path(placement: &WorkspacePlacement) -> Result<PathBuf> {
    if placement.owner_kind != PlacementOwnerKind::Server {
        return Err(WorkspaceBackendError::OwnerUnsupported {
            owner_kind: placement.owner_kind.clone(),
        });
    }
    Ok(PathBuf::from(workspace_handle(placement)?))
}

#[derive(Clone)]
pub struct WorkspaceBackendRouter {
    embedded: Arc<dyn WorkspaceBackend>,
    daemon: Option<Arc<dyn WorkspaceBackend>>,
}

impl WorkspaceBackendRouter {
    pub fn new(embedded: Arc<dyn WorkspaceBackend>) -> Self {
        Self {
            embedded,
            daemon: None,
        }
    }

    pub fn with_daemon(mut self, daemon: Arc<dyn WorkspaceBackend>) -> Self {
        self.daemon = Some(daemon);
        self
    }

    /// The entry point for reaching a workspace: use its persisted placement,
    /// never the Agent's daemon pin or `Workspace.worktree_path`.
    pub async fn resolve(&self, db: &SqliteDb, workspace: &Workspace) -> Result<ResolvedWorkspace> {
        let placement = Self::placement(db, workspace).await?;
        let backend = self.for_placement(&placement)?;
        Ok(ResolvedWorkspace { placement, backend })
    }

    /// For operations that must run on the Forge host, such as an embedded
    /// executor. A daemon handle is never interpreted as a local path.
    pub async fn embedded_path(&self, db: &SqliteDb, workspace: &Workspace) -> Result<PathBuf> {
        embedded_path(&Self::placement(db, workspace).await?)
    }

    async fn placement(db: &SqliteDb, workspace: &Workspace) -> Result<WorkspacePlacement> {
        match WorkspacePlacementRepo::get_by_workspace_id(db, &workspace.id).await? {
            Some(placement) => Ok(placement),
            None => EmbeddedWorkspaceBackend::ensure_recorded_server_placement(db, workspace).await,
        }
    }

    pub fn for_placement(
        &self,
        placement: &WorkspacePlacement,
    ) -> Result<Arc<dyn WorkspaceBackend>> {
        match placement.owner_kind {
            PlacementOwnerKind::Server => Ok(Arc::clone(&self.embedded)),
            PlacementOwnerKind::Daemon => self.daemon.as_ref().map(Arc::clone).ok_or_else(|| {
                WorkspaceBackendError::OwnerUnsupported {
                    owner_kind: placement.owner_kind.clone(),
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests;
