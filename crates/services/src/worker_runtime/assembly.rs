//! Shared worker identity and shutdown contracts.

use std::sync::Arc;
use tokio::sync::watch;

/// A one-shot process-local shutdown signal shared by every runtime worker.
///
/// This used to live in `api::state`; keeping it in `services` means a local
/// presentation can own the exact same signal without depending on Axum.
#[derive(Clone)]
pub struct ShutdownSignal {
    sender: Arc<watch::Sender<bool>>,
}

impl ShutdownSignal {
    #[must_use]
    pub fn new() -> Self {
        let (sender, _) = watch::channel(false);
        Self {
            sender: Arc::new(sender),
        }
    }

    /// Request shutdown. Repeated requests are intentionally harmless.
    pub fn request(&self) {
        self.sender.send_replace(true);
    }

    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.sender.subscribe()
    }

    pub async fn wait(&self) {
        let mut receiver = self.subscribe();
        if *receiver.borrow_and_update() {
            return;
        }

        while receiver.changed().await.is_ok() {
            if *receiver.borrow_and_update() {
                return;
            }
        }
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// Names of correctness-critical workers owned by the application supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuntimeWorker {
    CrashRecovery,
    NotificationProjection,
    OperatorStatusProjection,
    LifecycleProjection,
    TaskDispatcher,
    HeartbeatMonitor,
    AgentChatTurns,
    Memory,
    Coordination,
    Attention,
    WakeDelivery,
    ProjectHooks,
    ConflictHotspots,
    WorkspaceCleanup,
    DomainEventBroadcast,
    StorageMaintenance,
    TaskSteps,
    CheckRuns,
    IntegrationQueue,
    SessionJournalRetention,
}

/// Names of the durable consumers owned by the upper agent runtime.
///
/// Core status reporting knows which worker roles exist, while the agents
/// crate owns the concrete consumer implementations and their stable names.
pub trait AgentEventConsumerNames: Send + Sync {
    fn memory_consumer_name(&self) -> &'static str;
    fn wake_turn_consumer_name(&self) -> &'static str;
}

impl RuntimeWorker {
    pub(crate) fn event_consumer_name(
        self,
        agent_consumers: Option<&dyn AgentEventConsumerNames>,
    ) -> Option<&'static str> {
        match self {
            Self::NotificationProjection => Some(crate::notification_service::CONSUMER_NAME),
            Self::ProjectHooks => Some(crate::project_hooks::CONSUMER_NAME),
            Self::ConflictHotspots => Some(crate::worker_runtime::conflict_hotspot::CONSUMER_NAME),
            Self::Memory => agent_consumers.map(AgentEventConsumerNames::memory_consumer_name),
            Self::Coordination => Some(crate::coordination_consumer_name()),
            Self::Attention => Some(crate::attention_service::attention_consumer_name()),
            Self::WakeDelivery => {
                agent_consumers.map(AgentEventConsumerNames::wake_turn_consumer_name)
            }
            Self::DomainEventBroadcast => None, // Read-only tail, no durable checkpoint/lag.
            _ => None,
        }
    }
}

pub const COMMON_WORKERS: [RuntimeWorker; 20] = [
    RuntimeWorker::CrashRecovery,
    RuntimeWorker::NotificationProjection,
    RuntimeWorker::OperatorStatusProjection,
    RuntimeWorker::LifecycleProjection,
    RuntimeWorker::TaskDispatcher,
    RuntimeWorker::HeartbeatMonitor,
    RuntimeWorker::AgentChatTurns,
    RuntimeWorker::Memory,
    RuntimeWorker::Coordination,
    RuntimeWorker::Attention,
    RuntimeWorker::WakeDelivery,
    RuntimeWorker::ProjectHooks,
    RuntimeWorker::WorkspaceCleanup,
    RuntimeWorker::DomainEventBroadcast,
    RuntimeWorker::StorageMaintenance,
    RuntimeWorker::TaskSteps,
    RuntimeWorker::ConflictHotspots,
    RuntimeWorker::CheckRuns,
    RuntimeWorker::IntegrationQueue,
    RuntimeWorker::SessionJournalRetention,
];
