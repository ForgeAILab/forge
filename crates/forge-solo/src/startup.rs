//! Forge Solo process startup and terminal-owned lifetime.
//!
//! Startup is deliberately ordered: terminal preflight, read-only repository
//! resolution, marker claim, exact data-root resolution, runtime lock, layout,
//! migrations, shared runtime construction, local daemon readiness, and only
//! then bootstrap/recovery. No HTTP listener, MCP server, or remote daemon is
//! created here.

use std::{
    io,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use config::{ConfigOverrides, ForgeConfig};
use db::{Daemon, DaemonRepo, DaemonStatus, SqliteDb};
use executors::{AdapterRegistry, AvailabilityStatus, ExecutorKind};
use services::{
    solo_bootstrap::{
        SoloAgentCandidateInput, SoloAuthenticatedAgentRegistration, SoloBootstrapRequest,
        SoloBootstrapResult, SoloBootstrapService,
    },
    solo_session::{SoloSessionDependencies, SoloSessionScope, SoloSessionService},
    EmbeddedDaemon, ForgeRuntime, ForgeRuntimeBuilder, RuntimeAssemblyMode, RuntimeSupervisor,
    ServiceError,
};
use tokio::{sync::Mutex, time::sleep};

use crate::{
    backend::{BackendError, ChannelBackendEventSource},
    cli::{preflight, AgentExecutor, Cli, PreflightError, TerminalPreflight, TerminalStatus},
    data_dir::{resolve_data_paths_with_forge_root, DataDirError, SoloDataPaths},
    repository::{read_or_create_marker, resolve_git_repository, RepositoryError, SoloRepository},
    runtime_backend::RuntimeBackend,
    runtime_lock::{LockOwnerContext, RuntimeLock, RuntimeLockError},
    terminal::{PanicHookGuard, TerminalGuard},
    tracing::{init_tracing, TracingError, TracingGuard},
};

const EVENT_BUS_CAPACITY: usize = 1024;
const DAEMON_READY_TIMEOUT: Duration = Duration::from_secs(10);
const DAEMON_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Errors returned before or during the bounded Solo startup sequence.
#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error(transparent)]
    Preflight(#[from] PreflightError),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Repository(#[from] RepositoryError),

    #[error(transparent)]
    DataDir(#[from] DataDirError),

    #[error(transparent)]
    RuntimeLock(#[from] RuntimeLockError),

    #[error(transparent)]
    Config(#[from] config::ConfigError),

    #[error(transparent)]
    Db(#[from] db::DbError),

    #[error(transparent)]
    Services(#[from] ServiceError),

    #[error(transparent)]
    Tracing(#[from] TracingError),

    #[error("Solo Agent {executor} is not authenticated or available on this machine")]
    AgentUnavailable { executor: String },

    #[error("the embedded local Agent Runtime did not report readiness before timeout")]
    DaemonReadiness,

    #[error("Solo backend failed: {0}")]
    Backend(BackendError),

    #[error("terminal restoration failed: {0}")]
    TerminalRestore(io::Error),
}

/// All resources whose lifetime must span the TUI.
pub struct SoloStartup {
    pub repository: SoloRepository,
    pub paths: SoloDataPaths,
    pub config: ForgeConfig,
    pub runtime: Arc<ForgeRuntime>,
    pub supervisor: Arc<Mutex<RuntimeSupervisor>>,
    pub daemon: Arc<EmbeddedDaemon>,
    pub session: Arc<SoloSessionService>,
    pub bootstrap: SoloBootstrapResult,
    pub backend: Arc<RuntimeBackend>,
    lock: RuntimeLock,
    _tracing: Option<TracingGuard>,
    event_source: Option<ChannelBackendEventSource>,
}

impl std::fmt::Debug for SoloStartup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SoloStartup")
            .field("repository", &self.repository)
            .field("paths", &self.paths)
            .field("bootstrap", &self.bootstrap)
            .finish_non_exhaustive()
    }
}

impl SoloStartup {
    /// Perform real terminal preflight before any marker/data-root mutation.
    pub async fn initialize(cli: Cli) -> Result<Self, StartupError> {
        let preflight = preflight()?;
        Self::initialize_with_preflight(cli, preflight).await
    }

    /// Deterministic preflight seam used by embedders and integration tests.
    pub async fn initialize_with_preflight(
        cli: Cli,
        preflight: TerminalPreflight,
    ) -> Result<Self, StartupError> {
        if !preflight.is_interactive() {
            return Err(StartupError::Preflight(PreflightError::NonInteractive {
                stdin_status: terminal_status(preflight.stdin_is_terminal),
                stdout_status: terminal_status(preflight.stdout_is_terminal),
            }));
        }

        // Read-only operations come first. In particular, a failed Git
        // resolution cannot leave a marker or data directory behind.
        let launch_path = cli.resolve_path()?;
        let git = resolve_git_repository(&launch_path)?;
        let config_path = config::default_config_path();
        let base_config = ForgeConfig::load(Some(&config_path), ConfigOverrides::default())?;

        // This is the first mutating repository operation, now that TTY
        // preflight and read-only Git/config validation have succeeded.
        let marker = read_or_create_marker(git.common_dir())?;
        let repository = SoloRepository {
            git,
            repository_id: marker.repository_id,
        };
        let paths = resolve_data_paths_with_forge_root(
            repository.worktree_root(),
            repository.repository_id,
            &base_config.forge.data_dir,
            cli.data_dir.as_deref(),
        )?;

        // Validate all existing-root and sensitive-leaf invariants before
        // RuntimeLock can create runtime.lock or any later layout operation
        // can publish a repository binding.
        paths.preflight_existing_root(repository.repository_id)?;

        // Validate an existing explicit/default root before opening or
        // mutating runtime.lock. A mismatched binding must remain entirely
        // untouched so a failed launch cannot leak owner/process metadata.
        if let Some(actual) = paths.read_repository_binding()? {
            if actual != repository.repository_id {
                return Err(StartupError::DataDir(DataDirError::RepositoryMismatch {
                    path: paths.repository_binding.clone(),
                    expected: repository.repository_id,
                    actual,
                }));
            }
        }

        // Lock before layout/migrations so two launches cannot race the first
        // SQLite schema or bootstrap transaction.
        let mut lock = RuntimeLock::acquire_with_owner(
            &paths.root,
            LockOwnerContext {
                process_id: std::process::id(),
                repository_id: Some(repository.repository_id),
                project_id: None,
            },
        )?;
        paths.ensure_layout(repository.worktree_root(), repository.repository_id)?;

        let mut config = base_config;
        config.forge.data_dir = paths.root.clone();
        config.workspace.root = paths.worktrees.clone();
        // Solo owns no HTTP/MCP presentation and does not expose task
        // terminal sessions. The shared service graph remains the source of
        // domain behaviour, but these transport settings stay disabled.
        config.server.mcp_enabled = false;
        config.terminal.enabled = false;
        config.validate()?;

        let tracing = match init_tracing(&paths.logs) {
            Ok(guard) => Some(guard),
            Err(TracingError::AlreadyInitialized) => None,
            Err(error) => return Err(StartupError::Tracing(error)),
        };
        let jwt_secret = config.resolve_jwt_secret()?;
        let database_url = format!("sqlite:{}", paths.db_path.display());
        let pool = db::create_sqlite_pool(&database_url).await?;
        db::run_migrations(&pool).await?;
        let db = Arc::new(SqliteDb::new(pool));
        let event_bus = Arc::new(events::EventBus::new(EVENT_BUS_CAPACITY));
        let adapter_registry = Arc::new(cli_adapters::default_registry());

        let runtime = Arc::new(
            ForgeRuntimeBuilder::from_config(
                Arc::clone(&db),
                Arc::clone(&event_bus),
                config.clone(),
            )
            .with_adapter_registry(Arc::clone(&adapter_registry))
            .with_workspace_root(paths.worktrees.clone())
            .with_workflows_dir(paths.root.join("workflows"))
            .with_jwt_secret(jwt_secret)
            .with_bcrypt_cost(config.server.bcrypt_cost)
            .with_config_path(config_path)
            .build(),
        );

        let daemon = Arc::new(
            EmbeddedDaemon::new(
                Arc::clone(&db),
                Arc::clone(&event_bus),
                Arc::clone(&adapter_registry),
                paths.credentials.clone(),
                paths.worktrees.clone(),
            )
            .await?,
        );
        let daemon_handle = Arc::clone(&daemon).start();
        let daemon_record = match wait_for_embedded_daemon(&db).await {
            Ok(record) => record,
            Err(error) => {
                daemon.stop();
                daemon_handle.abort();
                return Err(error);
            }
        };

        let bootstrap_service = SoloBootstrapService::new(Arc::clone(&db))
            .with_embedded_agents(Arc::clone(&runtime.embedded_agent_service))
            .with_config_providers(config.providers.clone());
        let base_request = bootstrap_request(&repository, &paths);
        let setup = async {
            // First reconcile creates/resumes the owner and Project, giving
            // us the owner scope needed for owned CLI identity registration.
            let first = bootstrap_service.bootstrap(base_request.clone()).await?;
            let candidates = ensure_owned_agents(
                &bootstrap_service,
                &daemon_record,
                &adapter_registry,
                &first.owner_id,
            )
            .await?;
            let mut request = base_request.clone();
            request.agent_candidates = candidates;

            if let Some(requested) = cli.agent {
                let identity = request
                    .agent_candidates
                    .iter()
                    .find(|candidate| candidate.executor_type == requested.as_str())
                    .map(|candidate| candidate.identity_id.clone())
                    .ok_or_else(|| StartupError::AgentUnavailable {
                        executor: requested.as_str().to_owned(),
                    })?;
                request.selected_project_agent_id = Some(identity.clone());
                // A single local CLI identity is a valid Task Worker as well,
                // but this explicit assignment is still persisted through the
                // bootstrap service's independent worker field.
                request.selected_worker_agent_id = Some(identity);
            } else if first.selected_project_agent_id.is_some()
                && first.selected_worker_agent_id.is_some()
            {
                // Keep a durable prior selection visible to the final
                // reconciliation even when the CLI did not repeat --agent.
                request.selected_project_agent_id = first.selected_project_agent_id.clone();
                request.selected_worker_agent_id = first.selected_worker_agent_id.clone();
            }

            let result = bootstrap_service.bootstrap(request.clone()).await?;
            Ok::<_, StartupError>((request, result))
        }
        .await;
        let (bootstrap_template, bootstrap) = match setup {
            Ok(value) => value,
            Err(error) => {
                daemon.stop();
                daemon_handle.abort();
                return Err(error);
            }
        };

        // Keep the lock held while publishing the Project identity. A later
        // contender can now report repository + Project context without any
        // drop/reacquire window between bootstrap and session construction.
        if let Err(error) = lock.update_owner_context(LockOwnerContext {
            process_id: std::process::id(),
            repository_id: Some(repository.repository_id),
            project_id: Some(bootstrap.project_id.clone()),
        }) {
            daemon.stop();
            daemon_handle.abort();
            return Err(StartupError::RuntimeLock(error));
        }

        let scope = SoloSessionScope::new(
            bootstrap.owner_id.clone(),
            bootstrap.project_id.clone(),
            bootstrap.repo_id.clone(),
            bootstrap.project_chat_id.clone(),
        );
        let dependencies = SoloSessionDependencies::new(
            Arc::clone(&db),
            Arc::clone(&event_bus),
            Arc::clone(&runtime.agent_chat_service),
            Arc::clone(&runtime.task_service),
            runtime.agent_chat_turn_logs.clone(),
        )
        .with_execution_logs_root(paths.worktrees.clone());
        let session = match SoloSessionService::new(scope, dependencies) {
            Ok(session) => Arc::new(session),
            Err(error) => {
                daemon.stop();
                daemon_handle.abort();
                return Err(StartupError::Services(error));
            }
        };

        let supervisor = Arc::new(Mutex::new(RuntimeSupervisor::new(
            Arc::clone(&runtime),
            RuntimeAssemblyMode::Solo,
        )));
        if let Err(error) = supervisor.lock().await.start().await {
            daemon.stop();
            daemon_handle.abort();
            return Err(StartupError::Services(error));
        }
        let (backend, event_source) = RuntimeBackend::new(
            Arc::clone(&session),
            bootstrap_service,
            bootstrap_template,
            bootstrap.clone(),
            Arc::clone(&supervisor),
            Arc::clone(&daemon),
            daemon_record.id.clone(),
            daemon_handle,
            Arc::clone(&adapter_registry),
        );
        backend.mark_runtime_ready();

        Ok(Self {
            repository,
            paths,
            config,
            runtime,
            supervisor,
            daemon,
            session,
            bootstrap,
            backend,
            lock,
            _tracing: tracing,
            event_source: Some(event_source),
        })
    }

    /// Move the controller event source out of startup exactly once.
    pub fn take_event_source(&mut self) -> Option<ChannelBackendEventSource> {
        self.event_source.take()
    }

    /// The lock is intentionally private and held until this value is dropped.
    #[must_use]
    pub fn lock_path(&self) -> &Path {
        self.lock.lock_path()
    }

    /// Restore a terminal that has been entered by the caller. This helper is
    /// kept small so the binary can report restoration failures after the
    /// controller exits while Drop remains a final best-effort fallback.
    pub fn restore_terminal(guard: &mut TerminalGuard) -> Result<(), StartupError> {
        guard.restore().map_err(StartupError::TerminalRestore)
    }

    /// Install the process panic restoration hook for a TUI lifetime.
    #[must_use]
    pub fn install_panic_hook() -> PanicHookGuard {
        PanicHookGuard::install()
    }
}

fn terminal_status(is_terminal: bool) -> TerminalStatus {
    if is_terminal {
        TerminalStatus::Terminal
    } else {
        TerminalStatus::Redirected
    }
}

fn bootstrap_request(repository: &SoloRepository, paths: &SoloDataPaths) -> SoloBootstrapRequest {
    let root = repository.worktree_root();
    let repository_name = root
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .map(str::to_owned);
    SoloBootstrapRequest {
        repository_id: repository.repository_id.to_string(),
        canonical_repository: root.to_string_lossy().into_owned(),
        source_path: Some(root.to_string_lossy().into_owned()),
        data_root: paths.root.to_string_lossy().into_owned(),
        default_branch: repository.default_branch().unwrap_or("HEAD").to_owned(),
        repository_name,
        agent_candidates: Vec::new(),
        selected_project_agent_id: None,
        selected_worker_agent_id: None,
    }
}

async fn wait_for_embedded_daemon(db: &Arc<SqliteDb>) -> Result<Daemon, StartupError> {
    let machine_id = services::embedded_daemon::embedded_machine_id();
    let deadline = Instant::now() + DAEMON_READY_TIMEOUT;
    loop {
        if let Some(daemon) = DaemonRepo::get_by_machine_id(&**db, &machine_id).await? {
            if daemon.status == DaemonStatus::Online && daemon.last_report_at.is_some() {
                return Ok(daemon);
            }
        }
        if Instant::now() >= deadline {
            return Err(StartupError::DaemonReadiness);
        }
        sleep(DAEMON_POLL_INTERVAL).await;
    }
}

async fn ensure_owned_agents(
    bootstrap_service: &SoloBootstrapService,
    daemon: &Daemon,
    registry: &AdapterRegistry,
    owner_id: &str,
) -> Result<Vec<SoloAgentCandidateInput>, StartupError> {
    let supported = [
        AgentExecutor::Codex,
        AgentExecutor::ClaudeCode,
        AgentExecutor::Cursor,
        AgentExecutor::OpenCode,
        AgentExecutor::Gemini,
        AgentExecutor::Smith,
    ];
    let mut candidates = Vec::new();
    for executor in supported {
        let executor_type = executor.as_str();
        let kind = executor_type
            .parse::<ExecutorKind>()
            .map_err(|error| StartupError::Services(ServiceError::Domain(error)))?;
        let Some(adapter) = registry.get(&kind) else {
            continue;
        };
        let availability = adapter.check_availability();
        if !matches!(availability.status, AvailabilityStatus::Authenticated) {
            continue;
        }
        // Registration is replay-safe and keeps Agent/Profile creation behind
        // the typed bootstrap boundary. The adapter's structured auth result
        // is still checked immediately before this call.
        let registration = SoloAuthenticatedAgentRegistration::authenticated(
            format!(
                "forge-solo:{}:{executor_type}",
                services::embedded_daemon::embedded_machine_id()
            ),
            format!("Forge Solo {executor_type}"),
            executor_type,
            Some(daemon.id.clone()),
        );
        let candidate = bootstrap_service
            .ensure_authenticated_agent(owner_id, registration)
            .await?;
        candidates.push(SoloAgentCandidateInput {
            identity_id: candidate.identity_id,
            profile_id: candidate.profile_id,
            executor_type: candidate.executor_type,
            availability: candidate.availability,
            display_name: Some(candidate.display_name),
        });
    }
    // Config-declared direct Agents are appended after the CLI harnesses so
    // the first-run picker offers provider-backed models too.
    candidates.extend(bootstrap_service.config_agent_candidates(owner_id).await?);
    Ok(candidates)
}
