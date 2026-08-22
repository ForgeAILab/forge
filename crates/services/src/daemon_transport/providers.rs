use async_trait::async_trait;

use crate::ServiceError;

#[async_trait]
pub trait FilesystemProvider: Send + Sync {
    async fn list(
        &self,
        params: api_types::FsListParams,
    ) -> Result<api_types::FsListResult, ServiceError>;

    async fn branches(
        &self,
        params: api_types::FsBranchesParams,
    ) -> Result<api_types::FsBranchesResult, ServiceError>;
}

#[async_trait]
pub trait ExecutionProvider: Send + Sync {
    /// The authenticated connection incarnation that owns a remote
    /// execution, when this provider is backed by a daemon socket. Embedded
    /// providers return `None` and use their in-process lease owner.
    fn execution_lease_owner(&self) -> Option<String> {
        None
    }

    async fn start(
        &self,
        params: api_types::ExecutionStartParams,
    ) -> Result<api_types::ExecutionStartResult, ServiceError>;

    async fn cancel(
        &self,
        params: api_types::ExecutionCancelParams,
    ) -> Result<api_types::ExecutionCancelResult, ServiceError>;
}
