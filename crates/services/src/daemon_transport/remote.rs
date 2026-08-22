use std::sync::Arc;

use async_trait::async_trait;

use crate::daemon_transport::providers::{ExecutionProvider, FilesystemProvider};
use crate::daemon_transport::{execution_lease_owner, DaemonConnectionRegistry};
use crate::{Result, ServiceError};

#[derive(Clone)]
pub struct RemoteFilesystemProvider {
    registry: Arc<DaemonConnectionRegistry>,
    daemon_id: String,
}

impl RemoteFilesystemProvider {
    pub fn new(registry: Arc<DaemonConnectionRegistry>, daemon_id: String) -> Self {
        Self {
            registry,
            daemon_id,
        }
    }
}

#[async_trait]
impl FilesystemProvider for RemoteFilesystemProvider {
    async fn list(&self, params: api_types::FsListParams) -> Result<api_types::FsListResult> {
        self.registry
            .send_request(
                &self.daemon_id,
                api_types::METHOD_FS_LIST,
                params,
                api_types::DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
            )
            .await
    }

    async fn branches(
        &self,
        params: api_types::FsBranchesParams,
    ) -> Result<api_types::FsBranchesResult> {
        self.registry
            .send_request(
                &self.daemon_id,
                api_types::METHOD_FS_BRANCHES,
                params,
                api_types::DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
            )
            .await
    }
}

#[derive(Clone)]
pub struct RemoteExecutionProvider {
    registry: Arc<DaemonConnectionRegistry>,
    daemon_id: String,
    /// The authenticated socket incarnation selected for this dispatch.  A
    /// daemon reconnect keeps the durable id but must not receive requests or
    /// own the lease issued to the replaced connection.
    connection_id: u64,
}

impl RemoteExecutionProvider {
    pub fn new(registry: Arc<DaemonConnectionRegistry>, daemon_id: String) -> Self {
        let connection_id = registry
            .get(&daemon_id)
            .map(|connection| connection.id())
            .unwrap_or_default();
        Self {
            registry,
            daemon_id,
            connection_id,
        }
    }
}

#[async_trait]
impl ExecutionProvider for RemoteExecutionProvider {
    fn execution_lease_owner(&self) -> Option<String> {
        (self.connection_id != 0)
            .then(|| execution_lease_owner(&self.daemon_id, self.connection_id))
    }

    async fn start(
        &self,
        params: api_types::ExecutionStartParams,
    ) -> Result<api_types::ExecutionStartResult> {
        self.registry
            .send_request_for_connection(
                &self.daemon_id,
                self.connection_id,
                api_types::METHOD_EXECUTION_START,
                params,
                api_types::DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
            )
            .await
    }

    async fn cancel(
        &self,
        params: api_types::ExecutionCancelParams,
    ) -> Result<api_types::ExecutionCancelResult> {
        self.registry
            .send_request_for_connection(
                &self.daemon_id,
                self.connection_id,
                api_types::METHOD_EXECUTION_CANCEL,
                params,
                api_types::DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
            )
            .await
    }
}

pub(crate) fn daemon_error_to_service_error(
    daemon_id: &str,
    method: &str,
    error: api_types::DaemonErrorPayload,
) -> ServiceError {
    match error.code.as_str() {
        api_types::DAEMON_UNAVAILABLE => ServiceError::DaemonUnavailable {
            daemon_id: daemon_id.to_owned(),
        },
        api_types::DAEMON_TIMEOUT => ServiceError::DaemonTimeout {
            daemon_id: daemon_id.to_owned(),
            method: method.to_owned(),
        },
        api_types::INVALID_INPUT => ServiceError::terminal_invalid_input(error.message),
        api_types::PATH_GUARDRAIL | api_types::INVALID_FRAME | api_types::UNSUPPORTED_METHOD => {
            ServiceError::invalid_operation(error.message)
        }
        _ => ServiceError::Domain(error.message),
    }
}
