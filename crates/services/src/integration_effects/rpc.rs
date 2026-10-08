//! Socket effects only. The caller records operation admission, replies and ACKs.
use crate::{
    daemon_transport::{
        lock,
        workspace_client::{Result, WorkspaceClientError},
        DaemonConnection, DaemonConnectionRegistry,
    },
    ServiceError,
};
use api_types::*;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;

// Kept alive by the recorder until its post-exchange bookkeeping completes.
pub(crate) struct PendingRequest {
    pub(crate) connection: DaemonConnection,
    pub(crate) request_id: String,
}
impl Drop for PendingRequest {
    fn drop(&mut self) {
        lock(&self.connection.pending).remove(&self.request_id);
    }
}

/// Preparing has only in-memory effects; it gives the consumer the same point
/// at which to record admission before any frame is sent.
pub struct RpcExchange {
    registry: Arc<DaemonConnectionRegistry>,
    daemon_id: String,
    method: String,
    params: Value,
    receiver: Option<oneshot::Receiver<std::result::Result<Value, DaemonErrorPayload>>>,
    pending: PendingRequest,
}

impl RpcExchange {
    pub fn prepare(
        registry: Arc<DaemonConnectionRegistry>,
        daemon_id: &str,
        method: &str,
        params: Value,
    ) -> Result<Self> {
        let unavailable = || ServiceError::DaemonUnavailable {
            daemon_id: daemon_id.to_owned(),
        };
        let connection = registry
            .get(daemon_id)
            .filter(|connection| !connection.is_stale())
            .ok_or_else(unavailable)?;
        registry.ensure_protocol_dispatchable(daemon_id, &connection)?;
        let request_id = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        lock(&connection.pending).insert(request_id.clone(), sender);
        let pending = PendingRequest {
            connection: connection.clone(),
            request_id: request_id.clone(),
        };
        if !registry.is_current(daemon_id, connection.id())
            || !connection.protocol_allows_dispatch()
        {
            return Err(unavailable().into());
        }
        Ok(Self {
            registry,
            daemon_id: daemon_id.to_owned(),
            method: method.to_owned(),
            params,
            receiver: Some(receiver),
            pending,
        })
    }

    /// None means no RPC deadline. This primitive supplies no default.
    pub async fn execute(&mut self, deadline: Option<Duration>) -> Result<Value> {
        let daemon_id = self.daemon_id.as_str();
        let method = self.method.as_str();
        let unavailable = || ServiceError::DaemonUnavailable {
            daemon_id: daemon_id.to_owned(),
        };
        let connection = &self.pending.connection;
        let frame = DaemonFrame::Request {
            id: self.pending.request_id.clone(),
            method: method.to_owned(),
            params: self.params.clone(),
        };
        let receiver = self
            .receiver
            .take()
            .expect("one exchange per prepared request");
        let mut stale = connection.stale_receiver();
        let exchange = async {
            connection
                .outbound
                .send(frame)
                .await
                .map_err(|_| WorkspaceClientError::Transport(unavailable()))?;
            receiver
                .await
                .map_err(|_| WorkspaceClientError::Transport(unavailable()))?
                .map_err(WorkspaceClientError::Daemon)
        };
        tokio::select! {
            result = async {
                match deadline {
                    Some(timeout) => tokio::time::timeout(timeout, exchange).await.map_err(|_| {
                        WorkspaceClientError::Transport(ServiceError::DaemonTimeout { daemon_id: daemon_id.to_owned(), method: method.to_owned() })
                    })?,
                    None => exchange.await,
                }
            } => {
                let value = result?;
                if !self.registry.is_current(daemon_id, connection.id()) { return Err(unavailable().into()); }
                Ok(value)
            }
            _ = stale.changed() => Err(unavailable().into()),
        }
    }
}

pub fn decode_reply<R: DeserializeOwned>(method: &str, params: &Value, value: &Value) -> Result<R> {
    // Validate the complete typed payload before recording or acknowledging it.
    let result = serde_json::from_value(value.clone()).map_err(|error| {
        WorkspaceClientError::Transport(ServiceError::invalid_operation(format!(
            "invalid daemon response payload: {error}"
        )))
    })?;
    if let Some(operation_id) = params["operation_id"]
        .as_str()
        .filter(|_| method != METHOD_WORKSPACE_CANCEL)
    {
        if value["operation_id"].as_str() != Some(operation_id)
            || value["entry_id"].as_str().is_none_or(str::is_empty)
        {
            return Err(ServiceError::invalid_operation(
                "daemon returned a different workspace operation identity",
            )
            .into());
        }
    }
    Ok(result)
}

pub fn validate_merge_result(params: &Value, result: &WorkspaceMergeResult) -> Result<()> {
    if let WorkspaceMergeOutcome::Done {
        before_sha,
        after_sha,
        branch,
    } = &result.outcome
    {
        if Some(before_sha.as_str()) != params["expected_target_sha"].as_str()
            || after_sha.is_empty()
            || Some(branch.as_str()) != params["target_branch"].as_str()
            || params["reviewed_commit_sha"]
                .as_str()
                .is_some_and(|sha| sha != after_sha)
            || result.diffstat.is_none()
        {
            return Err(ServiceError::invalid_operation(
                "daemon merge result does not match the approved evidence",
            )
            .into());
        }
    }
    Ok(())
}
