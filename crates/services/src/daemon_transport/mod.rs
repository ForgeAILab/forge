use api_types::DAEMON_UNAVAILABLE;
use async_trait::async_trait;
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, MutexGuard, Weak,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use uuid::Uuid;

use crate::{ServiceError, TaskService};

pub mod execution_events;
pub mod execution_local;
pub mod fs_local;
pub mod providers;
pub mod remote;
pub mod router;

#[cfg(test)]
mod tests;

pub use execution_events::ServerExecutionEventSink;
pub use execution_local::EmbeddedExecutionProvider;
pub use fs_local::EmbeddedFilesystemProvider;
pub use providers::{ExecutionProvider, FilesystemProvider};
pub use remote::{RemoteExecutionProvider, RemoteFilesystemProvider};
pub use router::{select_execution_provider, select_filesystem_provider};

pub const DAEMON_OUTBOUND_BUFFER: usize = 256;
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Stable owner token for one authenticated daemon socket incarnation.  The
/// durable daemon id identifies the registered machine; the connection id
/// prevents a delayed frame from an old socket from renewing or terminalizing
/// the replacement attempt that uses the same daemon registration.
pub fn execution_lease_owner(daemon_id: &str, connection_id: u64) -> String {
    format!("daemon:{daemon_id}:connection:{connection_id}")
}

pub type PendingResponse = oneshot::Sender<Result<Value, api_types::DaemonErrorPayload>>;
pub type PendingRequests = HashMap<String, PendingResponse>;

#[async_trait]
pub trait DaemonExecutionEventHandler: Send + Sync {
    /// Handle the authenticated command-stream heartbeat.  This is separate
    /// from execution output: a remote daemon can keep an in-flight provider
    /// request alive even when it emits no log, text, or tool events.
    async fn handle_heartbeat(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _seq: u64,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_log(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: api_types::ExecutionLogNotification,
    ) -> Result<(), ServiceError>;

    async fn handle_terminal(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: api_types::ExecutionTerminalNotification,
    ) -> Result<(), ServiceError>;
}

#[async_trait]
pub trait DaemonTerminalEventHandler: Send + Sync {
    async fn handle_terminal_output(
        &self,
        _daemon_id: &str,
        _notification: api_types::TerminalOutputNotification,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_terminal_exited(
        &self,
        _daemon_id: &str,
        _notification: api_types::TerminalExitedNotification,
    ) -> Result<(), ServiceError> {
        Ok(())
    }
}

#[async_trait]
impl DaemonTerminalEventHandler for () {}

#[derive(Clone)]
struct EmbeddedExecutionContext {
    task_service: Weak<TaskService>,
    task_executor: Arc<dyn executors::TaskExecutor>,
}

#[derive(Clone)]
pub struct DaemonConnection {
    id: u64,
    pub daemon_id: String,
    pub outbound: mpsc::Sender<api_types::DaemonFrame>,
    pub pending: Arc<Mutex<PendingRequests>>,
    stale_tx: watch::Sender<bool>,
    stale_rx: watch::Receiver<bool>,
}

impl DaemonConnection {
    pub fn new(daemon_id: String) -> (Self, mpsc::Receiver<api_types::DaemonFrame>) {
        let (outbound, receiver) = mpsc::channel(DAEMON_OUTBOUND_BUFFER);
        let (stale_tx, stale_rx) = watch::channel(false);
        (
            Self {
                id: NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
                daemon_id,
                outbound,
                pending: Arc::new(Mutex::new(HashMap::new())),
                stale_tx,
                stale_rx,
            },
            receiver,
        )
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn stale_receiver(&self) -> watch::Receiver<bool> {
        self.stale_rx.clone()
    }

    pub fn mark_stale(&self) {
        let _ = self.stale_tx.send(true);
    }

    pub fn is_stale(&self) -> bool {
        *self.stale_rx.borrow()
    }
}

#[derive(Clone)]
pub struct DaemonConnectionRegistry {
    inner: Arc<DaemonConnectionRegistryInner>,
}

struct DaemonConnectionRegistryInner {
    connections: Mutex<HashMap<String, DaemonConnection>>,
    event_bus: Option<Arc<EventBus>>,
    execution_events: Mutex<Option<Arc<dyn DaemonExecutionEventHandler>>>,
    terminal_events: Mutex<Option<Arc<dyn DaemonTerminalEventHandler>>>,
    embedded_execution: Mutex<Option<EmbeddedExecutionContext>>,
}

impl DaemonConnectionRegistry {
    pub fn new(
        event_bus: Arc<EventBus>,
        execution_events: Arc<dyn DaemonExecutionEventHandler>,
    ) -> Self {
        Self {
            inner: Arc::new(DaemonConnectionRegistryInner {
                connections: Mutex::new(HashMap::new()),
                event_bus: Some(event_bus),
                execution_events: Mutex::new(Some(execution_events)),
                terminal_events: Mutex::new(None),
                embedded_execution: Mutex::new(None),
            }),
        }
    }

    pub fn without_handlers() -> Self {
        Self {
            inner: Arc::new(DaemonConnectionRegistryInner {
                connections: Mutex::new(HashMap::new()),
                event_bus: None,
                execution_events: Mutex::new(None),
                terminal_events: Mutex::new(None),
                embedded_execution: Mutex::new(None),
            }),
        }
    }

    pub fn set_terminal_event_handler(&self, handler: Arc<dyn DaemonTerminalEventHandler>) {
        *lock(&self.inner.terminal_events) = Some(handler);
    }

    pub fn set_embedded_execution_context(
        &self,
        task_service: Weak<TaskService>,
        task_executor: Arc<dyn executors::TaskExecutor>,
    ) {
        *lock(&self.inner.embedded_execution) = Some(EmbeddedExecutionContext {
            task_service,
            task_executor,
        });
    }

    pub(crate) fn embedded_execution_provider(
        &self,
    ) -> Result<Arc<dyn providers::ExecutionProvider>, ServiceError> {
        let Some(context) = lock(&self.inner.embedded_execution).clone() else {
            return Err(ServiceError::invalid_operation(
                "embedded execution provider is not configured",
            ));
        };
        let Some(task_service) = context.task_service.upgrade() else {
            return Err(ServiceError::invalid_operation(
                "embedded execution task service is unavailable",
            ));
        };
        Ok(Arc::new(EmbeddedExecutionProvider::new(
            task_service,
            context.task_executor,
        )))
    }

    pub fn register(
        &self,
        daemon_id: String,
        connection: DaemonConnection,
    ) -> Option<DaemonConnection> {
        let prior = lock(&self.inner.connections).insert(daemon_id.clone(), connection);
        if let Some(prior_connection) = &prior {
            prior_connection.mark_stale();
            fail_pending(
                prior_connection,
                api_types::DaemonErrorPayload {
                    code: DAEMON_UNAVAILABLE.to_owned(),
                    message: format!("daemon {daemon_id} connection replaced"),
                    details: None,
                },
            );
        }
        prior
    }

    pub fn unregister(&self, daemon_id: &str) {
        let removed = lock(&self.inner.connections).remove(daemon_id);
        if let Some(connection) = removed {
            fail_pending(
                &connection,
                api_types::DaemonErrorPayload {
                    code: DAEMON_UNAVAILABLE.to_owned(),
                    message: format!("daemon {daemon_id} disconnected"),
                    details: None,
                },
            );
            if let Some(event_bus) = self.inner.event_bus.as_ref() {
                event_bus.publish(ForgeEvent {
                    event_type: "daemon.disconnected".to_owned(),
                    entity_id: daemon_id.to_owned(),
                    timestamp: event_timestamp(),
                    context: EventContext::Empty {},
                });
            }
        }
    }

    pub fn get(&self, daemon_id: &str) -> Option<DaemonConnection> {
        lock(&self.inner.connections).get(daemon_id).cloned()
    }

    pub fn is_connected(&self, daemon_id: &str) -> bool {
        self.get(daemon_id)
            .is_some_and(|connection| !connection.is_stale())
    }

    pub fn is_current(&self, daemon_id: &str, connection_id: u64) -> bool {
        self.get(daemon_id)
            .is_some_and(|connection| connection.id() == connection_id && !connection.is_stale())
    }

    pub async fn send_request<P, R>(
        &self,
        daemon_id: &str,
        method: &str,
        params: P,
        timeout_secs: u64,
    ) -> Result<R, ServiceError>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        self.send_request_with_timeout(daemon_id, method, params, Duration::from_secs(timeout_secs))
            .await
    }

    /// Send a request through one specific authenticated socket incarnation.
    ///
    /// Remote execution providers pin the incarnation that was authenticated
    /// when the provider was selected.  Looking up only by durable daemon id
    /// here would let a reconnect silently route a start/cancel request to a
    /// replacement socket while the execution lease still belongs to the old
    /// owner token.
    pub async fn send_request_for_connection<P, R>(
        &self,
        daemon_id: &str,
        connection_id: u64,
        method: &str,
        params: P,
        timeout_secs: u64,
    ) -> Result<R, ServiceError>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        self.send_request_with_timeout_for_connection(
            daemon_id,
            connection_id,
            method,
            params,
            Duration::from_secs(timeout_secs),
        )
        .await
    }

    pub async fn send_request_with_timeout<P, R>(
        &self,
        daemon_id: &str,
        method: &str,
        params: P,
        timeout_duration: Duration,
    ) -> Result<R, ServiceError>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let connection = self
            .get(daemon_id)
            .ok_or_else(|| ServiceError::DaemonUnavailable {
                daemon_id: daemon_id.to_owned(),
            })?;
        let request_id = Uuid::new_v4().to_string();
        let params = serde_json::to_value(params).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid daemon request params: {error}"))
        })?;
        let (sender, receiver) = oneshot::channel();
        lock(&connection.pending).insert(request_id.clone(), sender);

        let frame = api_types::DaemonFrame::Request {
            id: request_id.clone(),
            method: method.to_owned(),
            params,
        };

        if connection.outbound.send(frame).await.is_err() {
            lock(&connection.pending).remove(&request_id);
            return Err(ServiceError::DaemonUnavailable {
                daemon_id: daemon_id.to_owned(),
            });
        }

        let result = match tokio::time::timeout(timeout_duration, receiver).await {
            Ok(Ok(Ok(result))) => result,
            Ok(Ok(Err(error))) => {
                return Err(remote::daemon_error_to_service_error(
                    daemon_id, method, error,
                ));
            }
            Ok(Err(_closed)) => {
                return Err(ServiceError::DaemonUnavailable {
                    daemon_id: daemon_id.to_owned(),
                });
            }
            Err(_elapsed) => {
                lock(&connection.pending).remove(&request_id);
                return Err(ServiceError::DaemonTimeout {
                    daemon_id: daemon_id.to_owned(),
                    method: method.to_owned(),
                });
            }
        };

        serde_json::from_value(result).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid daemon response payload: {error}"))
        })
    }

    pub async fn send_request_with_timeout_for_connection<P, R>(
        &self,
        daemon_id: &str,
        connection_id: u64,
        method: &str,
        params: P,
        timeout_duration: Duration,
    ) -> Result<R, ServiceError>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let connection = self
            .get(daemon_id)
            .filter(|connection| connection.id() == connection_id && !connection.is_stale())
            .ok_or_else(|| ServiceError::DaemonUnavailable {
                daemon_id: daemon_id.to_owned(),
            })?;
        let request_id = Uuid::new_v4().to_string();
        let params = serde_json::to_value(params).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid daemon request params: {error}"))
        })?;
        let (sender, receiver) = oneshot::channel();
        lock(&connection.pending).insert(request_id.clone(), sender);

        // The connection can be replaced after the lookup above.  Do not
        // leave a request registered on a stale incarnation, and never route
        // it through a replacement socket.
        if !self.is_current(daemon_id, connection_id) {
            lock(&connection.pending).remove(&request_id);
            return Err(ServiceError::DaemonUnavailable {
                daemon_id: daemon_id.to_owned(),
            });
        }

        let frame = api_types::DaemonFrame::Request {
            id: request_id.clone(),
            method: method.to_owned(),
            params,
        };

        if connection.outbound.send(frame).await.is_err() {
            lock(&connection.pending).remove(&request_id);
            return Err(ServiceError::DaemonUnavailable {
                daemon_id: daemon_id.to_owned(),
            });
        }

        let result = match tokio::time::timeout(timeout_duration, receiver).await {
            Ok(Ok(Ok(result))) => result,
            Ok(Ok(Err(error))) => {
                return Err(remote::daemon_error_to_service_error(
                    daemon_id, method, error,
                ));
            }
            Ok(Err(_closed)) => {
                return Err(ServiceError::DaemonUnavailable {
                    daemon_id: daemon_id.to_owned(),
                });
            }
            Err(_elapsed) => {
                lock(&connection.pending).remove(&request_id);
                return Err(ServiceError::DaemonTimeout {
                    daemon_id: daemon_id.to_owned(),
                    method: method.to_owned(),
                });
            }
        };

        serde_json::from_value(result).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid daemon response payload: {error}"))
        })
    }

    pub fn dispatch_incoming(&self, daemon_id: &str, frame: api_types::DaemonFrame) {
        let Some(connection) = self.get(daemon_id) else {
            tracing::warn!(
                daemon_id,
                "dropping daemon transport frame for unregistered daemon"
            );
            return;
        };

        self.dispatch_incoming_for_connection(daemon_id, connection.id(), frame);
    }

    /// Dispatch a frame only when it originated from the currently
    /// authenticated WebSocket incarnation.  A daemon reconnect keeps the
    /// same durable daemon id, so checking only that id would allow a delayed
    /// frame from the replaced socket to be delivered to the new connection's
    /// pending requests or execution event handler.
    pub fn dispatch_incoming_for_connection(
        &self,
        daemon_id: &str,
        connection_id: u64,
        frame: api_types::DaemonFrame,
    ) -> bool {
        if !self.is_current(daemon_id, connection_id) {
            tracing::debug!(
                daemon_id,
                connection_id,
                "dropping frame from stale daemon connection incarnation"
            );
            return false;
        }

        let Some(connection) = self.get(daemon_id) else {
            tracing::warn!(
                daemon_id,
                connection_id,
                "dropping daemon transport frame for unregistered daemon"
            );
            return false;
        };

        match frame {
            api_types::DaemonFrame::Response { id, result } => {
                if let Some(sender) = lock(&connection.pending).remove(&id) {
                    let _ = sender.send(Ok(result));
                } else {
                    tracing::warn!(
                        daemon_id,
                        request_id = %id,
                        "dropping daemon response with unknown request id"
                    );
                }
            }
            api_types::DaemonFrame::Error { id, error } => {
                let Some(id) = id else {
                    tracing::warn!(daemon_id, "dropping daemon error frame without request id");
                    return true;
                };
                if let Some(sender) = lock(&connection.pending).remove(&id) {
                    let _ = sender.send(Err(error));
                } else {
                    tracing::warn!(
                        daemon_id,
                        request_id = %id,
                        "dropping daemon error with unknown request id"
                    );
                }
            }
            api_types::DaemonFrame::Notification { method, params } => {
                self.dispatch_notification(daemon_id, connection_id, method, params);
            }
            api_types::DaemonFrame::Heartbeat { seq } => {
                tracing::trace!(daemon_id, seq, "received daemon heartbeat");
                let Some(handler) = lock(&self.inner.execution_events).clone() else {
                    return true;
                };
                let daemon_id = daemon_id.to_owned();
                tokio::spawn(async move {
                    if let Err(error) = handler
                        .handle_heartbeat(&daemon_id, connection_id, seq)
                        .await
                    {
                        tracing::warn!(%error, daemon_id = %daemon_id, connection_id, seq, "failed to renew remote execution leases");
                    }
                });
            }
            api_types::DaemonFrame::Request { id, method, .. } => {
                tracing::warn!(
                    daemon_id,
                    request_id = %id,
                    method,
                    "dropping daemon request frame received by server registry"
                );
            }
        }
        true
    }

    fn dispatch_notification(
        &self,
        daemon_id: &str,
        connection_id: u64,
        method: String,
        params: Value,
    ) {
        match method.as_str() {
            api_types::METHOD_EXECUTION_LOG => {
                let Some(handler) = lock(&self.inner.execution_events).clone() else {
                    tracing::info!(
                        daemon_id,
                        method,
                        "dropping daemon notification; no execution event handler is configured"
                    );
                    return;
                };
                match serde_json::from_value::<api_types::ExecutionLogNotification>(params) {
                    Ok(notification) => {
                        let daemon_id = daemon_id.to_owned();
                        tokio::spawn(async move {
                            if let Err(error) = handler
                                .handle_log(&daemon_id, connection_id, notification)
                                .await
                            {
                                tracing::warn!(%error, "failed to handle daemon execution log");
                            }
                        });
                    }
                    Err(error) => {
                        tracing::warn!(
                            daemon_id,
                            %error,
                            "dropping malformed execution.log notification"
                        );
                    }
                }
            }
            api_types::METHOD_EXECUTION_TERMINAL => {
                let Some(handler) = lock(&self.inner.execution_events).clone() else {
                    tracing::info!(
                        daemon_id,
                        method,
                        "dropping daemon notification; no execution event handler is configured"
                    );
                    return;
                };
                match serde_json::from_value::<api_types::ExecutionTerminalNotification>(params) {
                    Ok(notification) => {
                        let daemon_id = daemon_id.to_owned();
                        tokio::spawn(async move {
                            if let Err(error) = handler
                                .handle_terminal(&daemon_id, connection_id, notification)
                                .await
                            {
                                tracing::warn!(
                                    %error,
                                    "failed to handle daemon execution terminal notification"
                                );
                            }
                        });
                    }
                    Err(error) => {
                        tracing::warn!(
                            daemon_id,
                            %error,
                            "dropping malformed execution.terminal notification"
                        );
                    }
                }
            }
            api_types::METHOD_TERMINAL_OUTPUT => {
                let Some(handler) = lock(&self.inner.terminal_events).clone() else {
                    tracing::info!(
                        daemon_id,
                        method,
                        "dropping daemon notification; no terminal event handler is configured"
                    );
                    return;
                };
                match serde_json::from_value::<api_types::TerminalOutputNotification>(params) {
                    Ok(notification) => {
                        let daemon_id = daemon_id.to_owned();
                        tokio::spawn(async move {
                            if let Err(error) = handler
                                .handle_terminal_output(&daemon_id, notification)
                                .await
                            {
                                tracing::warn!(
                                    %error,
                                    "failed to handle daemon terminal output notification"
                                );
                            }
                        });
                    }
                    Err(error) => {
                        tracing::warn!(
                            daemon_id,
                            %error,
                            "dropping malformed terminal.output notification"
                        );
                    }
                }
            }
            api_types::METHOD_TERMINAL_EXITED => {
                let Some(handler) = lock(&self.inner.terminal_events).clone() else {
                    tracing::info!(
                        daemon_id,
                        method,
                        "dropping daemon notification; no terminal event handler is configured"
                    );
                    return;
                };
                match serde_json::from_value::<api_types::TerminalExitedNotification>(params) {
                    Ok(notification) => {
                        let daemon_id = daemon_id.to_owned();
                        tokio::spawn(async move {
                            if let Err(error) = handler
                                .handle_terminal_exited(&daemon_id, notification)
                                .await
                            {
                                tracing::warn!(
                                    %error,
                                    "failed to handle daemon terminal exited notification"
                                );
                            }
                        });
                    }
                    Err(error) => {
                        tracing::warn!(
                            daemon_id,
                            %error,
                            "dropping malformed terminal.exited notification"
                        );
                    }
                }
            }
            _ => {
                tracing::info!(
                    daemon_id,
                    method,
                    "dropping unsupported daemon notification"
                );
            }
        }
    }
}

impl Default for DaemonConnectionRegistry {
    fn default() -> Self {
        Self::without_handlers()
    }
}

fn fail_pending(connection: &DaemonConnection, error: api_types::DaemonErrorPayload) {
    let pending = std::mem::take(&mut *lock(&connection.pending));
    for sender in pending.into_values() {
        let _ = sender.send(Err(error.clone()));
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
