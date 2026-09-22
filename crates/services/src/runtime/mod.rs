//! Shared Forge application composition and worker lifecycle.
//!
//! The API server and Forge Solo are two presentations of the same local
//! domain runtime.  This module is deliberately transport agnostic: it owns
//! the service graph and the workers needed by domain behaviour, while HTTP,
//! MCP, web assets, and daemon transports remain in their respective entry
//! points.

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use config::{default_config_path, ForgeConfig};
use db::SqliteDb;
use events::EventBus;
use executors::{AdapterRegistry, FallbackExecutor, TaskExecutor};
use tokio::{sync::watch, task::JoinHandle};
use workspace::RepoCacheLockManager;

use crate::{
    AgentActionService, AgentChatMemoryConsumer, AgentChatTurnLogRoot, AgentChatTurnWorker,
    AgentInboxService, AgentService, AttentionService, AuthService, CommitmentService,
    CoordinationOutcomeConsumer, CrashRecovery, DaemonService, DomainEventBroadcastConsumer,
    EmbeddedAgentService, EmbeddedInquiryRunner, HeartbeatMonitor, MainChatTopicService,
    MemoryService, MergeService, NotificationService, OperatorStatusEmitter, OperatorStatusService,
    ProjectHookService, ProviderAuthorizationService, TaskDispatcher, TaskService,
    TerminalActivityTracker, TerminalService, WakeTurnConsumer, WorkspaceCleanupScheduler,
    WorkspaceExecutionLockManager,
};

const SUPERVISOR_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Stable module paths for callers that want to keep builder and supervisor
/// imports separate. The implementation intentionally stays in one module so
/// the component graph cannot drift between the two layers.
pub mod builder {
    pub use super::{ForgeRuntime, ForgeRuntimeBuilder};
}

pub mod supervisor {
    pub use super::{
        RuntimeAssemblyMode, RuntimeMode, RuntimeSupervisor, RuntimeTaskHandle, RuntimeWorker,
        ShutdownSignal,
    };
}

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

/// Explicit runtime assembly modes.  This is a composition choice rather
/// than a rollout flag: both modes use the same graph, but only the server
/// entry point adds network and remote-daemon components.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeAssemblyMode {
    Server,
    Solo,
}

/// Short alias for callers that prefer the concise name.
pub type RuntimeMode = RuntimeAssemblyMode;

/// Names of correctness-critical workers owned by [`RuntimeSupervisor`].
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
    WorkspaceCleanup,
    DomainEventBroadcast,
}

const COMMON_WORKERS: [RuntimeWorker; 14] = [
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
];

/// Abortable ownership for a compatibility worker that starts before a
/// [`RuntimeSupervisor`] exists. A supervisor can take the join handle; if no
/// supervisor ever takes it, dropping the last owner aborts the task instead
/// of detaching it from the application lifetime.
pub struct RuntimeTaskHandle {
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl RuntimeTaskHandle {
    fn install(&self, handle: JoinHandle<()>) {
        let mut slot = self.handle.lock().expect("runtime task handle lock");
        if let Some(previous) = slot.replace(handle) {
            previous.abort();
        }
    }

    fn take(&self) -> Option<JoinHandle<()>> {
        self.handle.lock().expect("runtime task handle lock").take()
    }
}

impl Default for RuntimeTaskHandle {
    fn default() -> Self {
        Self {
            handle: Mutex::new(None),
        }
    }
}

impl Drop for RuntimeTaskHandle {
    fn drop(&mut self) {
        let slot = self
            .handle
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(handle) = slot.take() {
            handle.abort();
        }
    }
}

/// Typed service and worker component graph shared by server and Solo.
#[derive(Clone)]
pub struct ForgeRuntime {
    pub db: Arc<SqliteDb>,
    pub pricing_repository: Arc<crate::pricing_db::SqlitePricingRepository>,
    pub models_dev_client: Arc<crate::pricing::ModelsDevClient>,
    pub task_service: Arc<TaskService>,
    pub agent_service: Arc<AgentService>,
    pub embedded_agent_service: Arc<EmbeddedAgentService>,
    pub agent_chat_service: Arc<crate::AgentChatService<SqliteDb>>,
    pub main_chat_topic_service: Arc<MainChatTopicService<SqliteDb>>,
    pub agent_inquiry_service: Arc<crate::agent_inquiry_service::AgentInquiryService<SqliteDb>>,
    pub agent_chat_turn_worker: Arc<AgentChatTurnWorker>,
    pub agent_chat_turn_logs: AgentChatTurnLogRoot,
    pub commitment_service: Arc<CommitmentService>,
    pub agent_inbox_service: Arc<AgentInboxService>,
    pub agent_action_service: Arc<AgentActionService>,
    pub daemon_service: Arc<DaemonService>,
    pub daemon_connections: Arc<crate::daemon_transport::DaemonConnectionRegistry>,
    pub workflow_template_service: Arc<crate::workflow::template_service::WorkflowTemplateService>,
    pub memory_service: Arc<MemoryService>,
    pub merge_service: Arc<MergeService>,
    pub notification_service: Arc<NotificationService>,
    pub project_hook_service: Arc<ProjectHookService>,
    pub terminal_service: Arc<TerminalService>,
    pub operator_status_service: Arc<OperatorStatusService>,
    pub operator_status_emitter: Arc<OperatorStatusEmitter>,
    /// Guards the one notification projection started by a supervisor. API
    /// compatibility constructors may start it eagerly, in which case this
    /// flag prevents a second copy when a caller later supplies a supervisor.
    notification_started: Arc<AtomicBool>,
    _notification_worker: Arc<RuntimeTaskHandle>,
    pub cleanup_scheduler: Arc<WorkspaceCleanupScheduler>,
    pub review_runner: Arc<review::ReviewRunner>,
    pub adapter_registry: Arc<AdapterRegistry>,
    pub task_executor: Arc<dyn TaskExecutor>,
    pub task_dispatcher: Arc<TaskDispatcher>,
    pub heartbeat_monitor: Arc<HeartbeatMonitor>,
    pub crash_recovery: Arc<CrashRecovery>,
    pub memory_consumer: Arc<AgentChatMemoryConsumer>,
    pub coordination_consumer: Arc<CoordinationOutcomeConsumer>,
    pub attention_projection: Arc<AttentionService>,
    pub wake_turn_consumer: Arc<WakeTurnConsumer>,
    pub domain_event_broadcast: Arc<DomainEventBroadcastConsumer>,
    pub lifecycle_emitter: Arc<crate::lifecycle::LifecycleEventEmitter>,
    pub workspace_exec_locks: Arc<WorkspaceExecutionLockManager>,
    pub repo_cache_locks: Arc<RepoCacheLockManager>,
    pub event_bus: Arc<EventBus>,
    pub shutdown_signal: ShutdownSignal,
    pub auth_service: Arc<AuthService>,
    pub oauth_service: Arc<crate::OAuthService>,
    pub provider_authorization_service: Arc<ProviderAuthorizationService>,
    pub config_path: Arc<PathBuf>,
    pub effective_config: Arc<ForgeConfig>,
}

impl ForgeRuntime {
    /// Re-point path-sensitive services when a caller resolves configuration
    /// after constructing a compatibility AppState.  New callers should pass
    /// the final config to the builder up front.
    pub fn with_effective_config(mut self, config: ForgeConfig) -> Self {
        self.embedded_agent_service
            .set_public_search_config(Some(config.public_search.clone()));
        self.embedded_agent_service
            .set_command_policy(&config.commands);
        self.embedded_agent_service
            .set_media_root(config.forge.data_dir.join("media"));
        self.embedded_agent_service.set_workspace_root(
            config.workspace.root.clone(),
            config.forge.data_dir.join("projects"),
        );
        self.agent_chat_turn_logs
            .set_root(agent_chat_turn_log_root(&config));
        self.oauth_service = Arc::new(crate::OAuthService::new(
            Arc::clone(&self.db),
            Arc::clone(&self.auth_service),
            config.mcp_resource_url(),
        ));
        self.provider_authorization_service
            .set_trusted_origins(config.trusted_web_origins());
        let terminal_activity = self.terminal_service.activity_tracker();
        let terminal_service = Arc::new(TerminalService::new_with_activity_tracker(
            Arc::clone(&self.db),
            Arc::clone(&self.event_bus),
            Arc::clone(&self.daemon_connections),
            Arc::clone(&self.workspace_exec_locks),
            config.terminal.clone(),
            self.cleanup_scheduler.workspace_root().to_path_buf(),
            terminal_activity,
        ));
        let terminal_cleanup_handler: Arc<dyn crate::workspace_cleanup::WorkspaceCleanupObserver> =
            terminal_service.clone();
        self.cleanup_scheduler
            .set_terminal_cleanup_handler(terminal_cleanup_handler);
        let terminal_event_handler: Arc<dyn crate::daemon_transport::DaemonTerminalEventHandler> =
            terminal_service.clone();
        self.daemon_connections
            .set_terminal_event_handler(terminal_event_handler);
        self.terminal_service = terminal_service;
        self.effective_config = Arc::new(config);
        self
    }

    #[must_use]
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        self.shutdown_signal.clone()
    }

    /// Returns ownership of an eagerly-started compatibility notification
    /// task's abort guard. API state retains this guard when it is built
    /// without a supervisor; normal server and Solo callers let the
    /// supervisor take the join handle after recovery instead.
    #[must_use]
    pub fn notification_worker_handle(&self) -> Arc<RuntimeTaskHandle> {
        Arc::clone(&self._notification_worker)
    }

    fn start_notification_projection(&self) {
        if self
            .notification_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self._notification_worker.install(
                Arc::clone(&self.notification_service)
                    .start_with_shutdown(self.shutdown_signal.subscribe()),
            );
        }
    }

    fn take_or_start_notification_projection(
        &self,
        shutdown: &watch::Receiver<bool>,
    ) -> Option<JoinHandle<()>> {
        if let Some(handle) = self._notification_worker.take() {
            return Some(handle);
        }
        if self
            .notification_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Some(
                Arc::clone(&self.notification_service).start_with_shutdown(shutdown.clone()),
            );
        }
        None
    }
}

/// Builder for the shared service graph.
pub struct ForgeRuntimeBuilder {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    adapter_registry: Arc<AdapterRegistry>,
    config: ForgeConfig,
    workspace_root: Option<PathBuf>,
    workflows_dir: Option<PathBuf>,
    jwt_secret: Vec<u8>,
    bcrypt_cost: u32,
    shutdown_signal: ShutdownSignal,
    config_path: PathBuf,
    start_notification_service: bool,
    merge_service: Option<Arc<MergeService>>,
    cleanup_scheduler: Option<Arc<WorkspaceCleanupScheduler>>,
    review_runner: Option<Arc<review::ReviewRunner>>,
}

impl ForgeRuntimeBuilder {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self {
            db,
            event_bus,
            adapter_registry: Arc::new(AdapterRegistry::new()),
            config: ForgeConfig::default(),
            workspace_root: None,
            workflows_dir: None,
            jwt_secret: b"test-jwt-secret-for-development".to_vec(),
            bcrypt_cost: 4,
            shutdown_signal: ShutdownSignal::new(),
            config_path: default_config_path(),
            start_notification_service: false,
            merge_service: None,
            cleanup_scheduler: None,
            review_runner: None,
        }
    }

    /// Start a builder with the caller's fully resolved configuration. This
    /// is the usual entry point for a local mode that has already applied
    /// CLI/environment/config-file precedence.
    #[must_use]
    pub fn from_config(db: Arc<SqliteDb>, event_bus: Arc<EventBus>, config: ForgeConfig) -> Self {
        Self::new(db, event_bus).with_config(config)
    }

    #[must_use]
    pub fn with_adapter_registry(mut self, adapter_registry: Arc<AdapterRegistry>) -> Self {
        self.adapter_registry = adapter_registry;
        self
    }

    #[must_use]
    pub fn with_config(mut self, config: ForgeConfig) -> Self {
        self.config = config;
        self
    }

    #[must_use]
    pub fn with_workspace_root(mut self, workspace_root: PathBuf) -> Self {
        self.workspace_root = Some(workspace_root);
        self
    }

    #[must_use]
    pub fn with_workflows_dir(mut self, workflows_dir: PathBuf) -> Self {
        self.workflows_dir = Some(workflows_dir);
        self
    }

    #[must_use]
    pub fn with_jwt_secret(mut self, jwt_secret: Vec<u8>) -> Self {
        self.jwt_secret = jwt_secret;
        self
    }

    #[must_use]
    pub fn with_bcrypt_cost(mut self, bcrypt_cost: u32) -> Self {
        self.bcrypt_cost = bcrypt_cost;
        self
    }

    #[must_use]
    pub fn with_shutdown_signal(mut self, shutdown_signal: ShutdownSignal) -> Self {
        self.shutdown_signal = shutdown_signal;
        self
    }

    #[must_use]
    pub fn with_config_path(mut self, config_path: PathBuf) -> Self {
        self.config_path = config_path;
        self
    }

    /// Start the notification projection during construction. This is kept
    /// for the synchronous AppState test/compatibility constructors; normal
    /// server and Solo callers let RuntimeSupervisor start it after recovery.
    #[must_use]
    pub fn start_notification_service(mut self) -> Self {
        self.start_notification_service = true;
        self
    }

    #[must_use]
    pub fn with_merge_service(mut self, merge_service: Arc<MergeService>) -> Self {
        self.merge_service = Some(merge_service);
        self
    }

    #[must_use]
    pub fn with_cleanup_scheduler(
        mut self,
        cleanup_scheduler: Arc<WorkspaceCleanupScheduler>,
    ) -> Self {
        self.cleanup_scheduler = Some(cleanup_scheduler);
        self
    }

    #[must_use]
    pub fn with_review_runner(mut self, review_runner: Arc<review::ReviewRunner>) -> Self {
        self.review_runner = Some(review_runner);
        self
    }

    /// Build one complete graph.  All service instances that need to share
    /// identity (executor routing, terminal activity, event sinks, and task
    /// dependencies) are created once here and wired by `Arc`.
    #[must_use]
    pub fn build(self) -> ForgeRuntime {
        let start_notification_service = self.start_notification_service;
        let workspace_root = self
            .workspace_root
            .clone()
            .or_else(|| {
                self.cleanup_scheduler
                    .as_ref()
                    .map(|s| s.workspace_root().to_path_buf())
            })
            .unwrap_or_else(|| self.config.workspace.root.clone());
        let workflows_dir = self
            .workflows_dir
            .clone()
            .unwrap_or_else(|| self.config.workflows_dir());
        let merge_service = self.merge_service.unwrap_or_else(|| {
            Arc::new(MergeService::new(
                Arc::clone(&self.db),
                Arc::clone(&self.event_bus),
                workspace_root.clone(),
            ))
        });
        let cleanup_scheduler = self.cleanup_scheduler.unwrap_or_else(|| {
            Arc::new(WorkspaceCleanupScheduler::new(
                Arc::clone(&self.db),
                Arc::clone(&self.event_bus),
                workspace_root.clone(),
            ))
        });
        let review_runner = self.review_runner.unwrap_or_else(|| {
            Arc::new(review::ReviewRunner::new(
                Arc::clone(&self.db),
                Arc::clone(&self.event_bus),
                Arc::clone(&self.adapter_registry),
            ))
        });
        let effective_config = self.config;
        let pricing_repository = Arc::new(crate::pricing_db::SqlitePricingRepository::new(
            Arc::clone(&self.db),
        ));
        let catalog_repository: Arc<dyn crate::pricing::PricingCatalogRepository> =
            pricing_repository.clone();
        let models_dev_client = Arc::new(
            crate::pricing::ModelsDevClient::new(catalog_repository)
                .expect("models.dev pricing client must initialize"),
        );
        let embedded_agent_service = Arc::new(EmbeddedAgentService::new(
            Arc::clone(&self.db),
            &self.jwt_secret,
        ));
        embedded_agent_service
            .set_public_search_config(Some(effective_config.public_search.clone()));
        embedded_agent_service.set_command_policy(&effective_config.commands);
        let agent_chat_service = Arc::new(crate::AgentChatService::new(Arc::clone(&self.db)));
        let main_chat_topic_service = Arc::new(MainChatTopicService::new(
            Arc::clone(&self.db),
            Arc::clone(&agent_chat_service),
            crate::ProductGenesisService::for_sqlite(Arc::clone(&self.db)),
        ));
        let agent_chat_turn_logs =
            AgentChatTurnLogRoot::new(agent_chat_turn_log_root(&effective_config));
        let agent_inquiry_service =
            Arc::new(crate::agent_inquiry_service::AgentInquiryService::new(
                Arc::clone(&self.db),
                Arc::clone(&agent_chat_service),
            ));
        let commitment_service = Arc::new(CommitmentService::new(Arc::clone(&self.db)));
        let agent_inbox_service = Arc::new(AgentInboxService::new(Arc::clone(&self.db)));
        let agent_action_service = Arc::new(AgentActionService::new(Arc::clone(&self.db)));
        let cli_task_executor: Arc<dyn TaskExecutor> =
            Arc::new(FallbackExecutor::new(Arc::clone(&self.adapter_registry)));
        let embedded_task_executor = Arc::new(crate::EmbeddedTaskExecutor::new(
            Arc::clone(&self.db),
            Arc::clone(&embedded_agent_service),
        ));
        let task_executor: Arc<dyn TaskExecutor> = Arc::new(crate::TaskExecutorRouter::new(
            cli_task_executor,
            embedded_task_executor,
        ));
        let review_runner = Arc::new(review_runner.with_task_executor(Arc::clone(&task_executor)));
        let workspace_exec_locks = Arc::new(WorkspaceExecutionLockManager::default());
        let repo_cache_locks = Arc::new(RepoCacheLockManager::default());
        let terminal_activity = Arc::new(TerminalActivityTracker::default());
        let memory_service = Arc::new(MemoryService::new(Arc::clone(&self.db)));
        let workflow_template_service = Arc::new(
            crate::workflow::template_service::WorkflowTemplateService::new(workflows_dir),
        );
        let execution_events = Arc::new(crate::daemon_transport::ServerExecutionEventSink::new(
            Arc::clone(&self.db),
            Arc::clone(&self.event_bus),
            workspace_root.clone(),
        ));
        let execution_event_handler: Arc<dyn crate::daemon_transport::DaemonExecutionEventHandler> =
            execution_events.clone();
        let daemon_connections = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::new(
            Arc::clone(&self.event_bus),
            execution_event_handler,
        ));
        execution_events.set_connection_registry(Arc::downgrade(&daemon_connections));
        let terminal_service = Arc::new(TerminalService::new_with_activity_tracker(
            Arc::clone(&self.db),
            Arc::clone(&self.event_bus),
            Arc::clone(&daemon_connections),
            Arc::clone(&workspace_exec_locks),
            effective_config.terminal.clone(),
            workspace_root.clone(),
            Arc::clone(&terminal_activity),
        ));
        let task_service = Arc::new(
            TaskService::new(Arc::clone(&self.db), Arc::clone(&self.event_bus))
                .with_merge_service(Arc::clone(&merge_service))
                .with_cleanup_scheduler(Arc::clone(&cleanup_scheduler))
                .with_review_runner(Arc::clone(&review_runner))
                .with_task_executor(Arc::clone(&task_executor))
                .with_daemon_connections(Arc::clone(&daemon_connections))
                .with_workspace_exec_locks(Arc::clone(&workspace_exec_locks))
                .with_terminal_activity_tracker(Arc::clone(&terminal_activity))
                .with_repo_cache_locks(Arc::clone(&repo_cache_locks))
                .with_memory_service(Arc::clone(&memory_service))
                .with_provider_credential_env(Arc::clone(&embedded_agent_service))
                .with_workspace_root(workspace_root.clone()),
        );
        execution_events.set_task_service(Arc::downgrade(&task_service));
        embedded_agent_service.set_task_service(Arc::clone(&task_service));
        let inquiry_runner = Arc::new(EmbeddedInquiryRunner::new(
            Arc::clone(&self.db),
            Arc::downgrade(&embedded_agent_service),
            agent_chat_turn_logs.clone(),
        ));
        embedded_agent_service.set_inquiry_runner(inquiry_runner.clone());
        agent_inquiry_service.set_runner(inquiry_runner);
        embedded_agent_service.set_media_root(effective_config.forge.data_dir.join("media"));
        embedded_agent_service.set_workspace_root(
            workspace_root.clone(),
            effective_config.forge.data_dir.join("projects"),
        );
        daemon_connections.set_embedded_execution_context(
            Arc::downgrade(&task_service),
            Arc::clone(&task_executor),
        );
        let agent_service = Arc::new(AgentService::new(
            Arc::clone(&self.db),
            Arc::clone(&self.event_bus),
        ));
        let daemon_service = Arc::new(
            DaemonService::new(Arc::clone(&self.db), Arc::clone(&self.event_bus))
                .with_task_service(Arc::clone(&task_service)),
        );
        let terminal_cleanup_handler: Arc<dyn crate::workspace_cleanup::WorkspaceCleanupObserver> =
            terminal_service.clone();
        cleanup_scheduler.set_terminal_cleanup_handler(terminal_cleanup_handler);
        let terminal_event_handler: Arc<dyn crate::daemon_transport::DaemonTerminalEventHandler> =
            terminal_service.clone();
        daemon_connections.set_terminal_event_handler(terminal_event_handler);
        let notification_service = Arc::new(NotificationService::new(
            Arc::clone(&self.db),
            Arc::clone(&self.event_bus),
        ));
        let project_hook_service = Arc::new(ProjectHookService::new(
            Arc::clone(&self.db),
            Arc::clone(&self.event_bus),
            Arc::clone(&task_service),
            Arc::clone(&notification_service),
        ));
        let operator_status_service = Arc::new(OperatorStatusService::new(Arc::clone(&self.db)));
        let operator_status_emitter =
            Arc::new(OperatorStatusEmitter::new(Arc::clone(&self.event_bus)));
        let agent_chat_turn_worker = Arc::new(AgentChatTurnWorker::new(
            Arc::clone(&self.db),
            Arc::clone(&embedded_agent_service),
            Arc::clone(&task_executor),
            agent_chat_turn_logs.clone(),
        ));
        let auth_service = Arc::new(AuthService::new(
            Arc::clone(&self.db),
            self.jwt_secret,
            self.bcrypt_cost,
        ));
        let oauth_service = Arc::new(crate::OAuthService::new(
            Arc::clone(&self.db),
            Arc::clone(&auth_service),
            effective_config.mcp_resource_url(),
        ));
        let provider_authorization_service = Arc::new(ProviderAuthorizationService::new(
            Arc::clone(&self.db),
            Arc::clone(&embedded_agent_service),
            effective_config.trusted_web_origins(),
        ));
        let task_dispatcher = Arc::new(TaskDispatcher::new(
            Arc::clone(&self.db),
            Arc::clone(&self.event_bus),
            Arc::clone(&task_service),
        ));
        let heartbeat_monitor = Arc::new(
            HeartbeatMonitor::new(Arc::clone(&self.db), Arc::clone(&self.event_bus))
                .with_task_service(Arc::clone(&task_service))
                .with_task_executor(Arc::clone(&task_executor))
                .with_daemon_connections(Arc::clone(&daemon_connections)),
        );
        let crash_recovery = Arc::new(CrashRecovery::new(
            Arc::clone(&self.db),
            Arc::clone(&self.event_bus),
        ));
        let memory_consumer = Arc::new(AgentChatMemoryConsumer::new(
            Arc::clone(&self.db),
            crate::memory_consumer_lease_owner(),
        ));
        let coordination_consumer = Arc::new(CoordinationOutcomeConsumer::new(
            Arc::clone(&self.db),
            crate::coordination_consumer_lease_owner(),
        ));
        let attention_projection = Arc::new(
            AttentionService::new(Arc::clone(&self.db)).with_event_bus(Arc::clone(&self.event_bus)),
        );
        let wake_turn_consumer = Arc::new(WakeTurnConsumer::new(
            Arc::clone(&self.db),
            crate::wake_turn_consumer_lease_owner(),
        ));
        let domain_event_broadcast = Arc::new(DomainEventBroadcastConsumer::new(
            Arc::clone(&self.db),
            Arc::clone(&self.event_bus),
        ));
        let plugin_registry = lifecycle_plugin_registry();
        let lifecycle_emitter = Arc::new(crate::lifecycle::LifecycleEventEmitter::new(
            Arc::clone(&self.db),
            plugin_registry,
        ));

        let runtime = ForgeRuntime {
            db: self.db,
            pricing_repository,
            models_dev_client,
            task_service,
            agent_service,
            embedded_agent_service,
            agent_chat_service,
            main_chat_topic_service,
            agent_inquiry_service,
            agent_chat_turn_worker,
            agent_chat_turn_logs,
            commitment_service,
            agent_inbox_service,
            agent_action_service,
            daemon_service,
            daemon_connections,
            workflow_template_service,
            memory_service,
            merge_service,
            notification_service,
            project_hook_service,
            terminal_service,
            operator_status_service,
            operator_status_emitter,
            notification_started: Arc::new(AtomicBool::new(false)),
            _notification_worker: Arc::new(RuntimeTaskHandle::default()),
            cleanup_scheduler,
            review_runner,
            adapter_registry: self.adapter_registry,
            task_executor,
            task_dispatcher,
            heartbeat_monitor,
            crash_recovery,
            memory_consumer,
            coordination_consumer,
            attention_projection,
            wake_turn_consumer,
            domain_event_broadcast,
            lifecycle_emitter,
            workspace_exec_locks,
            repo_cache_locks,
            event_bus: self.event_bus,
            shutdown_signal: self.shutdown_signal,
            auth_service,
            oauth_service,
            provider_authorization_service,
            config_path: Arc::new(self.config_path),
            effective_config: Arc::new(effective_config),
        };
        if start_notification_service {
            // Compatibility AppState constructors still start notifications
            // synchronously, but retain an abortable handle through the
            // runtime graph until AppState itself is dropped.
            runtime.start_notification_projection();
        }
        runtime
    }
}

/// Owns the shared worker handles and performs bounded, idempotent shutdown.
pub struct RuntimeSupervisor {
    runtime: Arc<ForgeRuntime>,
    mode: RuntimeAssemblyMode,
    started: bool,
    shutdown_started: bool,
    handles: Vec<(RuntimeWorker, JoinHandle<()>)>,
}

impl RuntimeSupervisor {
    #[must_use]
    pub fn new(runtime: Arc<ForgeRuntime>, mode: RuntimeAssemblyMode) -> Self {
        Self {
            runtime,
            mode,
            started: false,
            shutdown_started: false,
            handles: Vec::new(),
        }
    }

    #[must_use]
    pub fn runtime(&self) -> Arc<ForgeRuntime> {
        Arc::clone(&self.runtime)
    }

    #[must_use]
    pub fn mode(&self) -> RuntimeAssemblyMode {
        self.mode
    }

    #[must_use]
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        self.runtime.shutdown_signal.clone()
    }

    #[must_use]
    pub fn task_dispatcher(&self) -> Arc<TaskDispatcher> {
        Arc::clone(&self.runtime.task_dispatcher)
    }

    #[must_use]
    pub fn workers(&self) -> &'static [RuntimeWorker] {
        &COMMON_WORKERS
    }

    #[must_use]
    pub fn started(&self) -> bool {
        self.started
    }

    /// Number of long-lived child tasks currently owned by this supervisor.
    /// Crash recovery is a one-shot startup pass and therefore appears in
    /// [`workers`](Self::workers) but not in this handle count.
    #[must_use]
    pub fn worker_handle_count(&self) -> usize {
        self.handles.len()
    }

    /// Alias for callers that use a lifecycle-oriented vocabulary.
    pub async fn stop(&mut self) -> crate::Result<()> {
        self.shutdown().await
    }

    /// Alias for callers that separate requesting shutdown from joining the
    /// owned child tasks.
    pub async fn join(&mut self) -> crate::Result<()> {
        self.shutdown().await
    }

    /// Run recovery and initialize workflow templates once, then start each
    /// common worker in a deterministic order. Calling this again is a no-op.
    pub async fn start(&mut self) -> crate::Result<u64> {
        let mut shutdown = self.runtime.shutdown_signal.subscribe();
        if *shutdown.borrow_and_update() {
            return Err(crate::ServiceError::invalid_operation(
                "cannot start Forge runtime after shutdown was requested",
            ));
        }
        if self.started {
            return Ok(0);
        }

        let recovered = match self.runtime.crash_recovery.run().await {
            Ok(recovered) => recovered,
            Err(error) if self.mode == RuntimeAssemblyMode::Solo => return Err(error),
            Err(error) => {
                // Server startup historically logged recovery failures and
                // continued bringing up the API. Keep that behaviour while
                // ensuring the failure is visible to operators.
                tracing::warn!(%error, "crash recovery failed during runtime startup");
                0
            }
        };
        if let Err(error) = self.runtime.workflow_template_service.initialize().await {
            // A missing/broken workflow template must not prevent unrelated
            // API routes from starting, matching the legacy entry point.
            tracing::warn!(%error, "workflow template initialization failed");
        }

        // Projection startup intentionally follows recovery so all common
        // workers have one well-defined owner after the durable startup pass.
        // Compatibility AppState constructors may have pre-started the
        // notification task; take that same handle instead of duplicating it.
        if let Some(handle) = self
            .runtime
            .take_or_start_notification_projection(&shutdown)
        {
            self.handles
                .push((RuntimeWorker::NotificationProjection, handle));
        }
        self.handles.push((
            RuntimeWorker::OperatorStatusProjection,
            Arc::clone(&self.runtime.operator_status_emitter).start(shutdown.clone()),
        ));

        let lifecycle_emitter = Arc::clone(&self.runtime.lifecycle_emitter);
        let lifecycle_events = self.runtime.event_bus.subscribe();
        let lifecycle_shutdown = shutdown.clone();
        self.handles.push((
            RuntimeWorker::LifecycleProjection,
            tokio::spawn(async move {
                lifecycle_emitter
                    .run_with_shutdown(lifecycle_events, lifecycle_shutdown)
                    .await
            }),
        ));
        self.handles.push((
            RuntimeWorker::TaskDispatcher,
            Arc::clone(&self.runtime.task_dispatcher).start(),
        ));
        self.handles.push((
            RuntimeWorker::HeartbeatMonitor,
            Arc::clone(&self.runtime.heartbeat_monitor).start(),
        ));
        self.handles.push((
            RuntimeWorker::AgentChatTurns,
            Arc::clone(&self.runtime.agent_chat_turn_worker).start(shutdown.clone()),
        ));
        self.handles.push((
            RuntimeWorker::Memory,
            Arc::clone(&self.runtime.memory_consumer).start(shutdown.clone()),
        ));
        self.handles.push((
            RuntimeWorker::Coordination,
            Arc::clone(&self.runtime.coordination_consumer).start(shutdown.clone()),
        ));
        self.handles.push((
            RuntimeWorker::Attention,
            Arc::clone(&self.runtime.attention_projection).start(shutdown.clone()),
        ));
        self.handles.push((
            RuntimeWorker::WakeDelivery,
            Arc::clone(&self.runtime.wake_turn_consumer).start(shutdown.clone()),
        ));
        self.handles.push((
            RuntimeWorker::ProjectHooks,
            Arc::clone(&self.runtime.project_hook_service).start_with_shutdown(shutdown.clone()),
        ));
        self.handles.push((
            RuntimeWorker::WorkspaceCleanup,
            Arc::clone(&self.runtime.cleanup_scheduler).spawn(shutdown.clone()),
        ));
        self.handles.push((
            RuntimeWorker::DomainEventBroadcast,
            Arc::clone(&self.runtime.domain_event_broadcast).start(shutdown),
        ));
        self.started = true;
        tracing::info!(mode = ?self.mode, recovered, "Forge runtime started");
        Ok(recovered)
    }

    /// Request shutdown without awaiting workers. This is useful for an
    /// enclosing transport to wake its own listener before joining the core.
    pub fn request_shutdown(&self) {
        self.runtime.shutdown_signal.request();
    }

    /// Stop all common workers and settle active local Task executions within
    /// bounded deadlines. This method is safe to call more than once.
    pub async fn shutdown(&mut self) -> crate::Result<()> {
        if self.shutdown_started {
            return Ok(());
        }
        self.shutdown_started = true;
        let deadline = Instant::now() + SUPERVISOR_SHUTDOWN_TIMEOUT;
        self.runtime.shutdown_signal.request();
        self.runtime.heartbeat_monitor.stop();
        self.runtime.task_dispatcher.stop();

        let graceful = crate::GracefulShutdown::new(
            Arc::clone(&self.runtime.db),
            Arc::clone(&self.runtime.event_bus),
        )
        .with_task_executor(Arc::clone(&self.runtime.task_executor));
        let graceful_result = match remaining_until(deadline) {
            Some(remaining) => match tokio::time::timeout(remaining, graceful.shutdown()).await {
                Ok(result) => result,
                Err(_) => Err(crate::ServiceError::invalid_operation(
                    "runtime shutdown timed out",
                )),
            },
            None => Err(crate::ServiceError::invalid_operation(
                "runtime shutdown timed out",
            )),
        };

        for (worker, mut handle) in self.handles.drain(..) {
            let Some(remaining) = remaining_until(deadline) else {
                tracing::warn!(
                    ?worker,
                    "runtime worker did not stop before shutdown deadline"
                );
                handle.abort();
                continue;
            };
            match tokio::time::timeout(remaining, &mut handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => {
                    tracing::warn!(?worker, error = %error, "runtime worker failed during shutdown");
                }
                Err(_) => {
                    tracing::warn!(
                        ?worker,
                        "runtime worker did not stop before shutdown deadline"
                    );
                    handle.abort();
                }
            }
        }
        graceful_result
    }
}

impl Drop for RuntimeSupervisor {
    fn drop(&mut self) {
        self.runtime.shutdown_signal.request();
        self.runtime.heartbeat_monitor.stop();
        self.runtime.task_dispatcher.stop();
        for (_, handle) in self.handles.drain(..) {
            handle.abort();
        }
    }
}

fn remaining_until(deadline: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    (!remaining.is_zero()).then_some(remaining)
}

fn lifecycle_plugin_registry() -> Arc<crate::lifecycle::PluginRegistry> {
    let mut registry = crate::lifecycle::PluginRegistry::new();
    registry.register(Arc::new(
        crate::lifecycle::knowledge_inject::KnowledgeInjectPlugin,
    ));
    registry.register(Arc::new(
        crate::lifecycle::knowledge_capture::KnowledgeCapturePlugin,
    ));
    Arc::new(registry)
}

fn agent_chat_turn_log_root(config: &ForgeConfig) -> PathBuf {
    config.forge.data_dir.join("agent-chat-logs")
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{create_sqlite_pool, run_migrations};
    use std::sync::Arc;

    async fn runtime() -> Arc<ForgeRuntime> {
        runtime_with_notification(false).await
    }

    async fn runtime_with_notification(start_notification_service: bool) -> Arc<ForgeRuntime> {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        let db = Arc::new(SqliteDb::new(pool));
        let builder = ForgeRuntimeBuilder::new(Arc::clone(&db), Arc::new(EventBus::new(64)))
            .with_workspace_root(std::env::temp_dir().join("forge-runtime-test-workspaces"));
        let builder = if start_notification_service {
            builder.start_notification_service()
        } else {
            builder
        };
        Arc::new(builder.build())
    }

    #[test]
    fn worker_set_is_explicit_and_stable() {
        assert_eq!(COMMON_WORKERS.len(), 14);
        assert_eq!(COMMON_WORKERS[0], RuntimeWorker::CrashRecovery);
        assert_eq!(COMMON_WORKERS[1], RuntimeWorker::NotificationProjection);
        assert_eq!(COMMON_WORKERS[2], RuntimeWorker::OperatorStatusProjection);
        assert_eq!(COMMON_WORKERS[4], RuntimeWorker::TaskDispatcher);
        assert_eq!(COMMON_WORKERS[13], RuntimeWorker::DomainEventBroadcast);
    }

    #[tokio::test]
    async fn supervisor_start_is_idempotent_and_shutdown_is_bounded() {
        let runtime_graph = runtime().await;
        let mut supervisor =
            RuntimeSupervisor::new(Arc::clone(&runtime_graph), RuntimeAssemblyMode::Solo);
        assert!(!supervisor.started());
        supervisor.start().await.expect("runtime starts");
        assert!(supervisor.started());
        assert_eq!(supervisor.workers().len(), 14);
        assert_eq!(supervisor.worker_handle_count(), 13);
        assert_eq!(supervisor.start().await.expect("second start is no-op"), 0);
        supervisor.shutdown().await.expect("runtime shuts down");
        supervisor
            .shutdown()
            .await
            .expect("second shutdown is no-op");

        let second_runtime = runtime().await;
        let mut rejected = RuntimeSupervisor::new(second_runtime, RuntimeAssemblyMode::Solo);
        rejected.request_shutdown();
        assert!(rejected.start().await.is_err());
    }

    #[tokio::test]
    async fn compatibility_notification_worker_transfers_to_supervisor() {
        let runtime = runtime_with_notification(true).await;
        let mut supervisor = RuntimeSupervisor::new(runtime, RuntimeAssemblyMode::Server);

        supervisor.start().await.expect("runtime starts");
        assert_eq!(supervisor.worker_handle_count(), 13);
        supervisor.shutdown().await.expect("runtime shuts down");
    }
}
