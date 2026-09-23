use std::{path::PathBuf, sync::Arc};

use config::ForgeConfig;
use db::SqliteDb;
use events::EventBus;
use executors::{AdapterRegistry, TaskExecutor};
use services::{
    AgentActionService, AgentChatTurnLogRoot, AgentChatTurnWorker, AgentInboxService, AgentService,
    AuthService, CommitmentService, DaemonService, EmbeddedAgentService, MemoryService,
    MergeService, NotificationService, OperatorStatusEmitter, OperatorStatusService,
    ProjectHookService, ProviderAuthorizationService, TaskService, TerminalService,
    WorkspaceCleanupScheduler, WorkspaceExecutionLockManager,
};
use uuid::Uuid;
use workspace::RepoCacheLockManager;

const TEST_JWT_SECRET: &[u8] = b"test-jwt-secret-for-development";
const TEST_BCRYPT_COST: u32 = 4;

/// Kept at the API module path for existing route/test constructors while the
/// signal itself now belongs to the transport-neutral services runtime.
pub type ShutdownSignal = services::ShutdownSignal;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<SqliteDb>,
    /// Shared pricing persistence boundary for catalog, subject bindings,
    /// and retrospective estimate routes.
    pub pricing_repository: Arc<services::pricing_db::SqlitePricingRepository>,
    /// One process-wide models.dev client per AppState. Clones share the
    /// client's single-flight refresh gate and its fixed endpoint transport.
    pub models_dev_client: Arc<services::pricing::ModelsDevClient>,
    pub task_service: Arc<TaskService>,
    pub agent_service: Arc<AgentService>,
    pub embedded_agent_service: Arc<EmbeddedAgentService>,
    pub agent_chat_service: Arc<services::AgentChatService<SqliteDb>>,
    pub main_chat_topic_service: Arc<services::MainChatTopicService<SqliteDb>>,
    pub agent_inquiry_service: Arc<services::agent_inquiry_service::AgentInquiryService<SqliteDb>>,
    pub agent_chat_turn_worker: Arc<AgentChatTurnWorker>,
    /// Where each Agent Chat turn's durable activity log (tool calls,
    /// reasoning, reply deltas) lives; shared by the turn worker that writes
    /// it and the turn logs route that serves it.
    pub agent_chat_turn_logs: AgentChatTurnLogRoot,
    pub commitment_service: Arc<CommitmentService>,
    pub agent_inbox_service: Arc<AgentInboxService>,
    pub agent_action_service: Arc<AgentActionService>,
    pub daemon_service: Arc<DaemonService>,
    pub daemon_connections: Arc<services::daemon_transport::DaemonConnectionRegistry>,
    pub workflow_template_service:
        Arc<services::workflow::template_service::WorkflowTemplateService>,
    pub memory_service: Arc<MemoryService>,
    pub merge_service: Arc<MergeService>,
    pub notification_service: Arc<NotificationService>,
    pub project_hook_service: Arc<ProjectHookService>,
    pub terminal_service: Arc<TerminalService>,
    pub operator_status_service: Arc<OperatorStatusService>,
    pub operator_status_emitter: Arc<OperatorStatusEmitter>,
    pub cleanup_scheduler: Arc<WorkspaceCleanupScheduler>,
    pub review_runner: Arc<review::ReviewRunner>,
    pub adapter_registry: Arc<AdapterRegistry>,
    pub task_executor: Arc<dyn TaskExecutor>,
    pub task_dispatcher: Option<Arc<services::TaskDispatcher>>,
    pub workspace_exec_locks: Arc<WorkspaceExecutionLockManager>,
    pub repo_cache_locks: Arc<RepoCacheLockManager>,
    pub event_bus: Arc<EventBus>,
    pub shutdown_signal: ShutdownSignal,
    pub auth_service: Arc<AuthService>,
    pub oauth_service: Arc<services::OAuthService>,
    pub provider_authorization_service: Arc<ProviderAuthorizationService>,
    pub mcp_enabled: bool,
    pub config_path: Arc<PathBuf>,
    pub effective_config: Arc<ForgeConfig>,
    /// Keeps compatibility notification startup abortable when no runtime
    /// supervisor is present (for example in API-only test harnesses).
    _notification_worker: Arc<services::RuntimeTaskHandle>,
}

impl AppState {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>, mcp_enabled: bool) -> Self {
        Self::with_adapter_registry(db, event_bus, mcp_enabled, Arc::new(AdapterRegistry::new()))
    }

    pub fn with_adapter_registry(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        mcp_enabled: bool,
        adapter_registry: Arc<AdapterRegistry>,
    ) -> Self {
        Self::with_adapter_registry_and_shutdown(
            db,
            event_bus,
            mcp_enabled,
            adapter_registry,
            ShutdownSignal::new(),
        )
    }

    pub fn with_adapter_registry_and_shutdown(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        mcp_enabled: bool,
        adapter_registry: Arc<AdapterRegistry>,
        shutdown_signal: ShutdownSignal,
    ) -> Self {
        let workspace_root = default_workspace_root();
        let workflows_dir = test_workflows_dir();
        let merge_service = Arc::new(MergeService::new(
            Arc::clone(&db),
            Arc::clone(&event_bus),
            workspace_root.clone(),
        ));
        let cleanup_scheduler = Arc::new(WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::clone(&event_bus),
            workspace_root,
        ));
        let review_runner = Arc::new(review::ReviewRunner::new(
            Arc::clone(&db),
            Arc::clone(&event_bus),
            Arc::clone(&adapter_registry),
        ));
        Self::with_adapter_registry_services_and_shutdown(
            db,
            event_bus,
            mcp_enabled,
            adapter_registry,
            merge_service,
            cleanup_scheduler,
            review_runner,
            shutdown_signal,
            workflows_dir,
            test_jwt_secret(),
            test_bcrypt_cost(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_adapter_registry_services_and_shutdown(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        mcp_enabled: bool,
        adapter_registry: Arc<AdapterRegistry>,
        merge_service: Arc<MergeService>,
        cleanup_scheduler: Arc<WorkspaceCleanupScheduler>,
        review_runner: Arc<review::ReviewRunner>,
        shutdown_signal: ShutdownSignal,
        workflows_dir: PathBuf,
        jwt_secret: Vec<u8>,
        bcrypt_cost: u32,
    ) -> Self {
        let workspace_root = cleanup_scheduler.workspace_root().to_path_buf();
        let effective_config = effective_config_for_workspace(workspace_root);
        let runtime = services::ForgeRuntimeBuilder::new(db, event_bus)
            .with_adapter_registry(adapter_registry)
            .with_config(effective_config)
            .with_cleanup_scheduler(cleanup_scheduler)
            .with_merge_service(merge_service)
            .with_review_runner(review_runner)
            .with_shutdown_signal(shutdown_signal)
            .with_workflows_dir(workflows_dir)
            .with_jwt_secret(jwt_secret)
            .with_bcrypt_cost(bcrypt_cost)
            .start_notification_service()
            .build();
        Self::from_runtime(runtime, mcp_enabled)
    }

    /// Construct API state from the transport-neutral component graph.
    ///
    /// The dispatcher belongs to the shared runtime graph, so API state and
    /// every local presentation observe the same dispatcher identity.
    pub fn from_runtime(runtime: services::ForgeRuntime, mcp_enabled: bool) -> Self {
        Self::from_runtime_arc(Arc::new(runtime), mcp_enabled)
    }

    pub fn from_runtime_arc(runtime: Arc<services::ForgeRuntime>, mcp_enabled: bool) -> Self {
        Self {
            db: Arc::clone(&runtime.db),
            pricing_repository: Arc::clone(&runtime.pricing_repository),
            models_dev_client: Arc::clone(&runtime.models_dev_client),
            task_service: Arc::clone(&runtime.task_service),
            agent_service: Arc::clone(&runtime.agent_service),
            embedded_agent_service: Arc::clone(&runtime.embedded_agent_service),
            agent_chat_service: Arc::clone(&runtime.agent_chat_service),
            main_chat_topic_service: Arc::clone(&runtime.main_chat_topic_service),
            agent_inquiry_service: Arc::clone(&runtime.agent_inquiry_service),
            agent_chat_turn_worker: Arc::clone(&runtime.agent_chat_turn_worker),
            agent_chat_turn_logs: runtime.agent_chat_turn_logs.clone(),
            commitment_service: Arc::clone(&runtime.commitment_service),
            agent_inbox_service: Arc::clone(&runtime.agent_inbox_service),
            agent_action_service: Arc::clone(&runtime.agent_action_service),
            daemon_service: Arc::clone(&runtime.daemon_service),
            daemon_connections: Arc::clone(&runtime.daemon_connections),
            workflow_template_service: Arc::clone(&runtime.workflow_template_service),
            memory_service: Arc::clone(&runtime.memory_service),
            merge_service: Arc::clone(&runtime.merge_service),
            notification_service: Arc::clone(&runtime.notification_service),
            project_hook_service: Arc::clone(&runtime.project_hook_service),
            terminal_service: Arc::clone(&runtime.terminal_service),
            operator_status_service: Arc::clone(&runtime.operator_status_service),
            operator_status_emitter: Arc::clone(&runtime.operator_status_emitter),
            cleanup_scheduler: Arc::clone(&runtime.cleanup_scheduler),
            review_runner: Arc::clone(&runtime.review_runner),
            adapter_registry: Arc::clone(&runtime.adapter_registry),
            task_executor: Arc::clone(&runtime.task_executor),
            task_dispatcher: Some(Arc::clone(&runtime.task_dispatcher)),
            workspace_exec_locks: Arc::clone(&runtime.workspace_exec_locks),
            repo_cache_locks: Arc::clone(&runtime.repo_cache_locks),
            event_bus: Arc::clone(&runtime.event_bus),
            shutdown_signal: runtime.shutdown_signal.clone(),
            auth_service: Arc::clone(&runtime.auth_service),
            oauth_service: Arc::clone(&runtime.oauth_service),
            provider_authorization_service: Arc::clone(&runtime.provider_authorization_service),
            mcp_enabled,
            config_path: Arc::clone(&runtime.config_path),
            effective_config: Arc::clone(&runtime.effective_config),
            _notification_worker: runtime.notification_worker_handle(),
        }
    }

    pub fn with_config_path(mut self, config_path: PathBuf) -> Self {
        self.config_path = Arc::new(config_path);
        self
    }

    pub fn with_effective_config(mut self, config: ForgeConfig) -> Self {
        self.embedded_agent_service
            .set_public_search_config(Some(config.public_search.clone()));
        self.embedded_agent_service
            .set_command_policy(&config.commands);
        // The constructor only had `ForgeConfig::default()`, so every data-dir
        // path resolved against `~/.forge` rather than the server's actual
        // `--data-dir`. Re-point them here, where the real configuration first
        // becomes available.
        self.embedded_agent_service
            .set_media_root(config.forge.data_dir.join("media"));
        self.embedded_agent_service.set_workspace_root(
            config.workspace.root.clone(),
            config.forge.data_dir.join("projects"),
        );
        self.agent_chat_turn_logs
            .set_root(agent_chat_turn_log_root(&config));
        self.oauth_service = Arc::new(services::OAuthService::new(
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
        let terminal_cleanup_handler: Arc<
            dyn services::workspace_cleanup::WorkspaceCleanupObserver,
        > = terminal_service.clone();
        self.cleanup_scheduler
            .set_terminal_cleanup_handler(terminal_cleanup_handler);
        let terminal_event_handler: Arc<
            dyn services::daemon_transport::DaemonTerminalEventHandler,
        > = terminal_service.clone();
        self.daemon_connections
            .set_terminal_event_handler(terminal_event_handler);
        self.terminal_service = terminal_service;
        self.effective_config = Arc::new(config);
        self
    }

    pub fn with_task_dispatcher(mut self, task_dispatcher: Arc<services::TaskDispatcher>) -> Self {
        self.task_dispatcher = Some(task_dispatcher);
        self
    }

    /// Overrides the production catalog client for deterministic API tests.
    /// The normal constructor still creates exactly one client and transport
    /// for each AppState instance.
    pub fn with_models_dev_client(
        mut self,
        client: Arc<services::pricing::ModelsDevClient>,
    ) -> Self {
        self.models_dev_client = client;
        self
    }
}

fn default_workspace_root() -> PathBuf {
    std::env::var("FORGE_WORKSPACE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("forge").join("worktrees"))
}

pub fn test_workflows_dir() -> PathBuf {
    std::env::temp_dir().join(format!("forge-test-workflows-{}", Uuid::new_v4()))
}

pub fn test_jwt_secret() -> Vec<u8> {
    TEST_JWT_SECRET.to_vec()
}

pub fn test_bcrypt_cost() -> u32 {
    TEST_BCRYPT_COST
}

/// `<data-dir>/agent-chat-logs/<turn_job_id>.jsonl` holds one Agent Chat
/// turn's durable activity log.
fn agent_chat_turn_log_root(config: &ForgeConfig) -> PathBuf {
    config.forge.data_dir.join("agent-chat-logs")
}

fn effective_config_for_workspace(workspace_root: PathBuf) -> ForgeConfig {
    let mut config = ForgeConfig::default();
    config.workspace.root = workspace_root;
    config
}
