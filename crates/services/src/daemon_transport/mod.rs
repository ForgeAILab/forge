use api_types::{
    daemon_protocol_is_compatible, DaemonHandshakeNotification, JournalAckParams,
    DAEMON_PROTOCOL_INCOMPATIBLE, DAEMON_UNAVAILABLE, METHOD_DAEMON_HANDSHAKE, METHOD_JOURNAL_ACK,
};
use async_trait::async_trait;
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::{
    atomic::{AtomicU32, AtomicU8, Ordering},
    Arc, Mutex, MutexGuard, Weak,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, Notify};
use uuid::Uuid;

use crate::{ServiceError, TaskService};

pub mod execution_events;
pub mod execution_local;
pub mod fs_local;
pub mod providers;
pub mod remote;
pub mod router;
pub mod workspace_client;

#[cfg(test)]
mod tests;

pub use execution_events::ServerExecutionEventSink;
pub use execution_local::EmbeddedExecutionProvider;
pub use fs_local::EmbeddedFilesystemProvider;
pub use providers::{ExecutionProvider, FilesystemProvider};
pub use remote::{RemoteExecutionProvider, RemoteFilesystemProvider};
pub use router::{select_execution_provider, select_filesystem_provider};

pub const DAEMON_OUTBOUND_BUFFER: usize = 256;
fn new_connection_id() -> u64 {
    let uuid = Uuid::new_v4().as_u128();
    // Mix both halves so UUID version/variant bits do not reduce the numeric
    // token's entropy. Keep it positive for SQLite INTEGER retry epochs.
    (((uuid >> 64) as u64 ^ uuid as u64) & i64::MAX as u64).max(1)
}

const PROTOCOL_UNKNOWN: u8 = 0;
const PROTOCOL_COMPATIBLE: u8 = 1;
const PROTOCOL_INCOMPATIBLE: u8 = 2;

/// Stable owner token for one authenticated daemon socket incarnation.  The
/// durable daemon id identifies the registered machine; the connection id
/// prevents a delayed frame from an old socket from renewing or terminalizing
/// the replacement attempt that uses the same daemon registration.
pub fn execution_lease_owner(daemon_id: &str, connection_id: u64) -> String {
    format!("daemon:{daemon_id}:connection:{connection_id}")
}

pub type PendingResponse = oneshot::Sender<Result<Value, api_types::DaemonErrorPayload>>;
pub type PendingRequests = HashMap<String, PendingResponse>;

/// Result of handling a terminal notification. The registry sends the daemon
/// acknowledgement only for a durable success; conflicts deliberately leave
/// the daemon's retained record in place for operator/recovery handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaemonTerminalDisposition {
    Acknowledge,
    Pending,
    /// The report committed, but reconciliation must finish before cascade/ACK.
    AwaitingCascade,
    Conflict,
    Ignore,
}

#[async_trait]
pub trait DaemonExecutionEventHandler: Send + Sync {
    async fn handle_connected(&self, _daemon_id: &str) -> Result<(), ServiceError> {
        Ok(())
    }
    async fn handle_disconnected(&self, _daemon_id: &str) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_workspace_cleanup(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: api_types::WorkspaceCleanupResult,
    ) -> Result<DaemonTerminalDisposition, ServiceError> {
        Ok(DaemonTerminalDisposition::Ignore)
    }

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

    /// Handle a terminal notification and report whether the registry may
    /// delete the daemon's durable copy. A durable sink must return
    /// `Acknowledge` only after the terminal state, invocation settlement,
    /// usage reports, and its `(terminal_report_id, payload_digest)` receipt
    /// are committed together. It must return `Acknowledge` for an exact
    /// replay found in that durable receipt (including after a server restart),
    /// `Pending` while an identical report is still committing its outbox,
    /// `Conflict` for a reused identity with a different payload, and
    /// `Ignore` only for an unknown late report that has no durable receipt.
    /// Existing handlers keep the narrower method above; durable sinks
    /// override this contract to distinguish these outcomes.
    async fn handle_terminal_with_ack(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: api_types::ExecutionTerminalNotification,
    ) -> Result<DaemonTerminalDisposition, ServiceError> {
        self.handle_terminal(daemon_id, connection_id, notification)
            .await?;
        // A legacy handler that only implements `handle_terminal` has not
        // demonstrated a durable terminal receipt, so the daemon must retain
        // its report for a later durable sink/recovery pass.
        Ok(DaemonTerminalDisposition::Ignore)
    }
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

#[derive(Debug, Clone)]
pub struct DaemonConnectionSnapshot {
    pub connection_id: u64,
    pub handshake: DaemonHandshakeNotification,
    pub workspace_incapable: bool,
}

#[derive(Clone)]
pub struct DaemonConnection {
    id: u64,
    connected_at: String,
    pub daemon_id: String,
    pub outbound: mpsc::Sender<api_types::DaemonFrame>,
    pub pending: Arc<Mutex<PendingRequests>>,
    stale_tx: watch::Sender<bool>,
    stale_rx: watch::Receiver<bool>,
    protocol_state: Arc<AtomicU8>,
    protocol_revision: Arc<AtomicU32>,
    handshake: Arc<Mutex<Option<DaemonHandshakeNotification>>>,
}

impl DaemonConnection {
    pub fn new(daemon_id: String) -> (Self, mpsc::Receiver<api_types::DaemonFrame>) {
        let (outbound, receiver) = mpsc::channel(DAEMON_OUTBOUND_BUFFER);
        let (stale_tx, stale_rx) = watch::channel(false);
        (
            Self {
                id: new_connection_id(),
                connected_at: db::now_rfc3339(),
                daemon_id,
                outbound,
                pending: Arc::new(Mutex::new(HashMap::new())),
                stale_tx,
                stale_rx,
                protocol_state: Arc::new(AtomicU8::new(PROTOCOL_UNKNOWN)),
                protocol_revision: Arc::new(AtomicU32::new(0)),
                handshake: Arc::new(Mutex::new(None)),
            },
            receiver,
        )
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn connected_at(&self) -> &str {
        &self.connected_at
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

    pub fn protocol_known(&self) -> bool {
        self.protocol_state.load(Ordering::Acquire) != PROTOCOL_UNKNOWN
    }

    pub fn protocol_compatible(&self) -> bool {
        self.protocol_state.load(Ordering::Acquire) == PROTOCOL_COMPATIBLE
    }

    pub fn protocol_allows_dispatch(&self) -> bool {
        self.protocol_compatible()
    }

    pub fn negotiated_revision(&self) -> Option<u32> {
        (!self.is_stale() && self.protocol_known())
            .then(|| self.protocol_revision.load(Ordering::Acquire))
    }

    pub fn needs_upgrade(&self) -> bool {
        self.negotiated_revision()
            .is_some_and(|revision| revision < api_types::DAEMON_MIN_PROTOCOL_REVISION)
    }

    pub fn snapshot(&self) -> Option<DaemonConnectionSnapshot> {
        let retained = lock(&self.handshake);
        if self.is_stale() || !self.protocol_compatible() {
            return None;
        }
        let handshake = retained.clone()?;
        let workspace_incapable = handshake.protocol_revision
            < api_types::DAEMON_MIN_PROTOCOL_REVISION
            || !handshake
                .capabilities
                .iter()
                .any(|capability| capability == api_types::DAEMON_CAPABILITY_WORKSPACE);
        Some(DaemonConnectionSnapshot {
            connection_id: self.id,
            handshake,
            workspace_incapable,
        })
    }

    fn set_protocol_compatibility(&self, compatible: bool) {
        self.protocol_state.store(
            if compatible {
                PROTOCOL_COMPATIBLE
            } else {
                PROTOCOL_INCOMPATIBLE
            },
            Ordering::Release,
        );
    }
}

#[derive(Clone)]
pub struct DaemonConnectionRegistry {
    inner: Arc<DaemonConnectionRegistryInner>,
}

struct DaemonConnectionRegistryInner {
    connections: Mutex<HashMap<String, DaemonConnection>>,
    socket_lifecycle: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    event_bus: Option<Arc<EventBus>>,
    execution_events: Mutex<Option<Arc<dyn DaemonExecutionEventHandler>>>,
    terminal_events: Mutex<Option<Arc<dyn DaemonTerminalEventHandler>>>,
    embedded_execution: Mutex<Option<EmbeddedExecutionContext>>,
    reconciliation_notify: Arc<Notify>,
    journal_terminals:
        Mutex<HashMap<(String, u64, String), api_types::ExecutionTerminalNotification>>,
}

impl DaemonConnectionRegistry {
    pub fn new(
        event_bus: Arc<EventBus>,
        execution_events: Arc<dyn DaemonExecutionEventHandler>,
    ) -> Self {
        Self {
            inner: Arc::new(DaemonConnectionRegistryInner {
                connections: Mutex::new(HashMap::new()),
                socket_lifecycle: Mutex::new(HashMap::new()),
                event_bus: Some(event_bus),
                execution_events: Mutex::new(Some(execution_events)),
                terminal_events: Mutex::new(None),
                embedded_execution: Mutex::new(None),
                reconciliation_notify: Arc::new(Notify::new()),
                journal_terminals: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn without_handlers() -> Self {
        Self {
            inner: Arc::new(DaemonConnectionRegistryInner {
                connections: Mutex::new(HashMap::new()),
                socket_lifecycle: Mutex::new(HashMap::new()),
                event_bus: None,
                execution_events: Mutex::new(None),
                terminal_events: Mutex::new(None),
                embedded_execution: Mutex::new(None),
                reconciliation_notify: Arc::new(Notify::new()),
                journal_terminals: Mutex::new(HashMap::new()),
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

    /// Serialize command-socket registration/removal and their durable status
    /// writes, including lifecycle events, for one daemon across reconnects.
    pub fn socket_lifecycle_lock(&self, daemon_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = lock(&self.inner.socket_lifecycle);
        locks.retain(|_, entry| entry.strong_count() > 0);
        if let Some(existing) = locks.get(daemon_id).and_then(Weak::upgrade) {
            return existing;
        }
        let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(daemon_id.to_owned(), Arc::downgrade(&lifecycle));
        lifecycle
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
            self.notify_disconnected(&daemon_id);
        }
        prior
    }

    pub fn unregister(&self, daemon_id: &str) {
        let removed = lock(&self.inner.connections).remove(daemon_id);
        if let Some(connection) = removed {
            connection.mark_stale();
            fail_pending(
                &connection,
                api_types::DaemonErrorPayload {
                    code: DAEMON_UNAVAILABLE.to_owned(),
                    message: format!("daemon {daemon_id} disconnected"),
                    details: None,
                },
            );
            self.notify_disconnected(daemon_id);
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

    pub fn connection_snapshots(&self) -> BTreeMap<String, DaemonConnectionSnapshot> {
        lock(&self.inner.connections)
            .iter()
            .filter_map(|(id, connection)| connection.snapshot().map(|facts| (id.clone(), facts)))
            .collect()
    }

    pub fn reconciliation_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.inner.reconciliation_notify)
    }

    fn notify_disconnected(&self, daemon_id: &str) {
        lock(&self.inner.journal_terminals).retain(|(id, _, _), _| id != daemon_id);
        let Some(handler) = lock(&self.inner.execution_events).clone() else {
            return;
        };
        let daemon_id = daemon_id.to_owned();
        let notify = self.reconciliation_notify();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Err(error) = handler.handle_disconnected(&daemon_id).await {
                    tracing::warn!(%error, daemon_id, "failed to suspend disconnected placements");
                }
                notify.notify_one();
            });
        }
    }

    pub fn is_connected(&self, daemon_id: &str) -> bool {
        self.get(daemon_id)
            .is_some_and(|connection| !connection.is_stale())
    }

    pub fn is_current(&self, daemon_id: &str, connection_id: u64) -> bool {
        self.get(daemon_id)
            .is_some_and(|connection| connection.id() == connection_id && !connection.is_stale())
    }

    pub(crate) fn ensure_protocol_dispatchable(
        &self,
        daemon_id: &str,
        connection: &DaemonConnection,
    ) -> Result<(), ServiceError> {
        if connection.protocol_allows_dispatch() {
            return Ok(());
        }
        if connection.needs_upgrade() {
            return Err(ServiceError::DaemonUpgradeRequired {
                daemon_id: daemon_id.to_owned(),
            });
        }
        if !connection.protocol_known() {
            return Err(ServiceError::DaemonNotReady {
                daemon_id: daemon_id.to_owned(),
            });
        }
        Err(ServiceError::invalid_operation(format!(
            "{DAEMON_PROTOCOL_INCOMPATIBLE}: daemon {daemon_id} is missing required command capabilities"
        )))
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
        self.ensure_protocol_dispatchable(daemon_id, &connection)?;
        let request_id = Uuid::new_v4().to_string();
        let params = serde_json::to_value(params).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid daemon request params: {error}"))
        })?;
        let (sender, receiver) = oneshot::channel();
        lock(&connection.pending).insert(request_id.clone(), sender);
        let _pending = crate::integration_effects::rpc::PendingRequest {
            connection: connection.clone(),
            request_id: request_id.clone(),
        };

        if !connection.protocol_allows_dispatch() {
            lock(&connection.pending).remove(&request_id);
            return self
                .ensure_protocol_dispatchable(daemon_id, &connection)
                .and_then(|()| {
                    Err(ServiceError::DaemonUnavailable {
                        daemon_id: daemon_id.to_owned(),
                    })
                });
        }

        let frame = api_types::DaemonFrame::Request {
            id: request_id.clone(),
            method: method.to_owned(),
            params,
        };

        let deadline = tokio::time::Instant::now() + timeout_duration;
        let sent = tokio::time::timeout_at(deadline, connection.outbound.send(frame))
            .await
            .map_err(|_| ServiceError::DaemonTimeout {
                daemon_id: daemon_id.to_owned(),
                method: method.to_owned(),
            })?;
        if sent.is_err() {
            lock(&connection.pending).remove(&request_id);
            return Err(ServiceError::DaemonUnavailable {
                daemon_id: daemon_id.to_owned(),
            });
        }

        let result = match tokio::time::timeout_at(deadline, receiver).await {
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
        self.ensure_protocol_dispatchable(daemon_id, &connection)?;
        let request_id = Uuid::new_v4().to_string();
        let params = serde_json::to_value(params).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid daemon request params: {error}"))
        })?;
        let (sender, receiver) = oneshot::channel();
        lock(&connection.pending).insert(request_id.clone(), sender);
        let _pending = crate::integration_effects::rpc::PendingRequest {
            connection: connection.clone(),
            request_id: request_id.clone(),
        };

        // The connection can be replaced after the lookup above.  Do not
        // leave a request registered on a stale incarnation, and never route
        // it through a replacement socket.
        if !self.is_current(daemon_id, connection_id) || !connection.protocol_allows_dispatch() {
            lock(&connection.pending).remove(&request_id);
            if !connection.protocol_allows_dispatch() {
                return self
                    .ensure_protocol_dispatchable(daemon_id, &connection)
                    .and_then(|()| {
                        Err(ServiceError::DaemonUnavailable {
                            daemon_id: daemon_id.to_owned(),
                        })
                    });
            }
            return Err(ServiceError::DaemonUnavailable {
                daemon_id: daemon_id.to_owned(),
            });
        }

        let frame = api_types::DaemonFrame::Request {
            id: request_id.clone(),
            method: method.to_owned(),
            params,
        };

        let deadline = tokio::time::Instant::now() + timeout_duration;
        let sent = tokio::time::timeout_at(deadline, connection.outbound.send(frame))
            .await
            .map_err(|_| ServiceError::DaemonTimeout {
                daemon_id: daemon_id.to_owned(),
                method: method.to_owned(),
            })?;
        if sent.is_err() {
            lock(&connection.pending).remove(&request_id);
            return Err(ServiceError::DaemonUnavailable {
                daemon_id: daemon_id.to_owned(),
            });
        }

        let result = match tokio::time::timeout_at(deadline, receiver).await {
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
        if method == METHOD_DAEMON_HANDSHAKE {
            self.dispatch_handshake(daemon_id, connection_id, params);
            return;
        }

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
                match serde_json::from_value::<api_types::ExecutionTerminalNotification>(params) {
                    Ok(notification) => {
                        let key = (
                            daemon_id.to_owned(),
                            connection_id,
                            notification.execution_id.clone(),
                        );
                        let should_apply = {
                            let mut terminals = lock(&self.inner.journal_terminals);
                            if terminals.get(&key) == Some(&notification) {
                                false
                            } else {
                                terminals.insert(key, notification.clone());
                                true
                            }
                        };
                        if !should_apply {
                            return;
                        }
                        let registry = self.clone();
                        let daemon_id = daemon_id.to_owned();
                        tokio::spawn(async move {
                            if let Err(error) = registry
                                .apply_journal_terminal(&daemon_id, connection_id, notification)
                                .await
                            {
                                tracing::warn!(%error, daemon_id, connection_id,
                                    "failed to drain daemon terminal report");
                            }
                        });
                    }
                    Err(error) => tracing::warn!(daemon_id, %error,
                        "dropping malformed execution.terminal notification"),
                }
            }
            api_types::METHOD_WORKSPACE_CLEANUP => {
                let Some(handler) = lock(&self.inner.execution_events).clone() else {
                    return;
                };
                match serde_json::from_value::<api_types::WorkspaceCleanupResult>(params) {
                    Ok(notification) => {
                        let registry = self.clone();
                        let daemon_id = daemon_id.to_owned();
                        tokio::spawn(async move {
                            let result = async {
                                if handler.handle_workspace_cleanup(
                                    &daemon_id, connection_id, notification.clone(),
                                ).await? == DaemonTerminalDisposition::Acknowledge {
                                    let ack = registry.send_request_for_connection::<_, api_types::JournalAckResult>(
                                        &daemon_id, connection_id, METHOD_JOURNAL_ACK,
                                        JournalAckParams { entry_id: notification.entry_id },
                                        api_types::DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
                                    ).await?;
                                    if !ack.acknowledged {
                                        return Err(ServiceError::invalid_operation("daemon did not acknowledge cleanup journal entry"));
                                    }
                                }
                                Ok::<_, ServiceError>(())
                            }.await;
                            if let Err(error) = result {
                                tracing::warn!(%error, daemon_id, connection_id,
                                    "failed to apply daemon cleanup acknowledgement");
                            }
                        });
                    }
                    Err(error) => tracing::warn!(daemon_id, %error,
                        "dropping malformed workspace.cleanup notification"),
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

    async fn apply_journal_terminal(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: api_types::ExecutionTerminalNotification,
    ) -> Result<DaemonTerminalDisposition, ServiceError> {
        if !self.is_current(daemon_id, connection_id) {
            return Ok(DaemonTerminalDisposition::Ignore);
        }
        let Some(handler) = lock(&self.inner.execution_events).clone() else {
            return Ok(DaemonTerminalDisposition::Ignore);
        };
        let disposition = handler
            .handle_terminal_with_ack(daemon_id, connection_id, notification.clone())
            .await?;
        if matches!(
            disposition,
            DaemonTerminalDisposition::Acknowledge | DaemonTerminalDisposition::Ignore
        ) {
            let acknowledgement = self
                .send_request_for_connection::<_, api_types::JournalAckResult>(
                    daemon_id,
                    connection_id,
                    METHOD_JOURNAL_ACK,
                    JournalAckParams {
                        entry_id: notification.terminal_report_id.clone(),
                    },
                    api_types::DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
                )
                .await?;
            if !acknowledgement.acknowledged {
                return Err(ServiceError::invalid_operation(
                    "daemon did not acknowledge terminal journal entry",
                ));
            }
            lock(&self.inner.journal_terminals).remove(&(
                daemon_id.to_owned(),
                connection_id,
                notification.execution_id,
            ));
        }
        Ok(disposition)
    }

    pub(crate) fn retained_terminal_execution_ids(&self) -> Vec<String> {
        lock(&self.inner.journal_terminals)
            .iter()
            .filter(|((daemon_id, connection_id, _), _)| self.is_current(daemon_id, *connection_id))
            .map(|((_, _, execution_id), _)| execution_id.clone())
            .collect()
    }

    pub(crate) async fn retry_retained_terminals(
        &self,
        daemon_id: &str,
    ) -> Result<(), ServiceError> {
        let Some(connection) = self.get(daemon_id) else {
            return Ok(());
        };
        let notifications: Vec<_> = lock(&self.inner.journal_terminals)
            .iter()
            .filter(|((owner, incarnation, _), _)| {
                owner == daemon_id && *incarnation == connection.id()
            })
            .map(|(_, notification)| notification.clone())
            .collect();
        let mut first_error = None;
        for notification in notifications {
            if let Err(error) = self
                .apply_journal_terminal(daemon_id, connection.id(), notification)
                .await
            {
                // A failed cascade/ACK must not starve another placement on
                // the same daemon. The worker logs its placement/owner context.
                if matches!(
                    error,
                    ServiceError::DaemonTimeout { .. } | ServiceError::DaemonUnavailable { .. }
                ) {
                    return Err(error);
                }
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Describe replays the journal before its response. Drain the retained
    /// payloads through the same durable terminal sink as live notifications.
    pub async fn drain_execution_journal(
        &self,
        daemon_id: &str,
        connection_id: u64,
        execution_ids: &[String],
    ) -> Result<bool, ServiceError> {
        for execution_id in execution_ids {
            let notification = lock(&self.inner.journal_terminals)
                .get(&(daemon_id.to_owned(), connection_id, execution_id.clone()))
                .cloned();
            let Some(notification) = notification else {
                return Ok(false);
            };
            if matches!(
                self.apply_journal_terminal(daemon_id, connection_id, notification)
                    .await?,
                DaemonTerminalDisposition::Pending | DaemonTerminalDisposition::Conflict
            ) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn dispatch_handshake(&self, daemon_id: &str, connection_id: u64, params: Value) {
        let Some(connection) = self
            .get(daemon_id)
            .filter(|connection| connection.id() == connection_id)
        else {
            tracing::debug!(
                daemon_id,
                connection_id,
                "dropping handshake from stale daemon"
            );
            return;
        };

        match serde_json::from_value::<DaemonHandshakeNotification>(params) {
            Ok(handshake) => {
                connection
                    .protocol_revision
                    .store(handshake.protocol_revision, Ordering::Release);
                let compatible = daemon_protocol_is_compatible(
                    handshake.protocol_revision,
                    &handshake.capabilities,
                );
                {
                    let mut retained = lock(&connection.handshake);
                    *retained = Some(handshake.clone());
                    connection.set_protocol_compatibility(compatible);
                }
                if compatible {
                    if let Some(handler) = lock(&self.inner.execution_events).clone() {
                        let daemon_id = daemon_id.to_owned();
                        tokio::spawn(async move {
                            if let Err(error) = handler.handle_connected(&daemon_id).await {
                                tracing::warn!(%daemon_id,%error,"pending remote cancellations remain fenced");
                            }
                        });
                    }
                    tracing::debug!(
                        daemon_id,
                        connection_id,
                        protocol_revision = handshake.protocol_revision,
                        workspace_incapable = connection
                            .snapshot()
                            .is_none_or(|facts| facts.workspace_incapable),
                        "accepted daemon command protocol"
                    );
                    self.inner.reconciliation_notify.notify_one();
                } else if handshake.protocol_revision < api_types::DAEMON_MIN_PROTOCOL_REVISION {
                    self.send_upgrade_required(&connection);
                    self.inner.reconciliation_notify.notify_one();
                } else {
                    self.reject_incompatible_protocol(&connection, daemon_id, connection_id);
                }
            }
            Err(error) => {
                connection.set_protocol_compatibility(false);
                tracing::warn!(
                    daemon_id,
                    connection_id,
                    %error,
                    "rejecting malformed daemon protocol handshake"
                );
                self.reject_incompatible_protocol(&connection, daemon_id, connection_id);
            }
        }
    }

    fn send_upgrade_required(&self, connection: &DaemonConnection) {
        let _ = connection.outbound.try_send(api_types::DaemonFrame::Error {
            id: None,
            error: api_types::DaemonErrorPayload {
                code: api_types::DAEMON_UPGRADE_REQUIRED.to_owned(),
                message: api_types::DAEMON_UPGRADE_REQUIRED_MESSAGE.to_owned(),
                details: None,
            },
        });
    }

    fn reject_incompatible_protocol(
        &self,
        connection: &DaemonConnection,
        daemon_id: &str,
        connection_id: u64,
    ) {
        if connection.needs_upgrade() {
            self.send_upgrade_required(connection);
        } else {
            let _ = connection.outbound.try_send(api_types::DaemonFrame::Error {
                id: None,
                error: api_types::DaemonErrorPayload {
                    code: DAEMON_PROTOCOL_INCOMPATIBLE.to_owned(),
                    message:
                        "daemon handshake is malformed or missing required command capabilities"
                            .to_owned(),
                    details: None,
                },
            });
        }
        connection.mark_stale();
        tracing::warn!(
            daemon_id,
            connection_id,
            "daemon command connection rejected for incompatible protocol"
        );
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

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
