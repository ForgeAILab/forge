#![forbid(unsafe_code)]

use clap::Parser;
use config::{read_server_state, write_server_state, ConfigOverrides, ForgeConfig, ServerState};
use std::net::{IpAddr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::EnvFilter;

const DEFAULT_LOG_FILTER: &str = "forge=info,forge_cli=info,api=info,services=info,review=info,cli_adapters=info,executors=info,db=warn,tower_http=info,sqlx=warn";
const SERVER_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Parser)]
#[command(
    name = "forge",
    version,
    about = "Forge — local-first workflow engine for coding agents"
)]
struct Cli {
    #[arg(long)]
    demo: bool,
    #[arg(long = "no-mcp")]
    no_mcp: bool,
    #[arg(long = "no-embedded-daemon")]
    no_embedded_daemon: bool,
    /// Override the data directory (database, credentials, workflows).
    /// Defaults to ~/.forge. Use --data-dir ./test for local testing.
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let config = ForgeConfig::load(
        None,
        ConfigOverrides {
            mcp_enabled: if cli.no_mcp { Some(false) } else { None },
            data_dir: cli.data_dir,
            ..Default::default()
        },
    )
    .expect("Failed to load config");

    init_tracing(&config.forge.data_dir.join("logs"));

    let configured_addr: SocketAddr = config
        .server
        .bind
        .parse()
        .expect("Failed to parse server bind address");
    let listener = bind_server_listener(&config, configured_addr);
    listener
        .set_nonblocking(true)
        .expect("Failed to make server listener nonblocking");
    let listener =
        tokio::net::TcpListener::from_std(listener).expect("Failed to adopt server listener");
    let addr = listener
        .local_addr()
        .expect("Failed to read bound server address");
    let mut effective_config = config.clone();
    effective_config.server.bind = addr.to_string();
    let server_url = server_url_for_addr(addr);
    if let Err(error) = write_server_state(
        &effective_config.forge.data_dir,
        &ServerState::new(&effective_config.server.bind, &server_url),
    ) {
        warn!(
            %error,
            data_dir = %effective_config.forge.data_dir.display(),
            "failed to persist Forge server port"
        );
    }
    let web_dist = web_dist_dir();
    if !web_dist.join("index.html").is_file() {
        warn!(
            web_dist = %web_dist.display(),
            "web UI assets not found; API routes will still run, but browser navigation may return 404"
        );
    }
    let db_path = config.db_path();
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent).expect("Failed to create data directory");
    }
    let database_url = format!("sqlite:{}", db_path.display());
    let forge_home = db_path
        .parent()
        .map_or_else(|| PathBuf::from("."), PathBuf::from);
    let workspace_root = absolute_path(config.workspace.root.clone())
        .expect("Failed to resolve workspace root path");
    // Components that cannot be handed the resolved root explicitly (e.g.
    // Genesis repo provisioning inside the services crate) fall back to this
    // env var; export the configured value so every path agrees.
    std::env::set_var("FORGE_WORKSPACE_ROOT", &workspace_root);

    info!(
        bind_addr = %effective_config.server.bind,
        management_url = %local_url(addr.port(), "/"),
        api_base_url = %local_url(addr.port(), "/api/v1"),
        healthz_url = %local_url(addr.port(), "/healthz"),
        mcp_enabled = effective_config.server.mcp_enabled,
        embedded_daemon_enabled = !cli.no_embedded_daemon,
        demo_mode = cli.demo,
        data_dir = %effective_config.forge.data_dir.display(),
        db_path = %db_path.display(),
        workspace_root = %workspace_root.display(),
        "initializing forge"
    );

    // 1. Create database pool and run migrations
    let pool = db::create_sqlite_pool(&database_url)
        .await
        .expect("Failed to create database pool");
    db::run_migrations(&pool)
        .await
        .expect("Failed to run migrations");

    let db = Arc::new(db::SqliteDb::new(pool));
    let event_bus = Arc::new(events::EventBus::with_default_capacity());
    let mut registry = cli_adapters::default_registry();
    if cli.demo {
        registry.register(Box::new(cli_adapters::NullAdapter::new()));
    }
    let adapter_registry = Arc::new(registry);
    let merge_service = Arc::new(services::MergeService::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.clone(),
    ));
    let cleanup_scheduler = Arc::new(services::WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.clone(),
    ));
    let shared_media_cleanup_scheduler = Arc::new(services::SharedMediaCleanupScheduler::new(
        Arc::clone(&db),
        media_storage_root(&effective_config.forge.data_dir),
    ));
    let review_runner = Arc::new(review::ReviewRunner::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        Arc::clone(&adapter_registry),
    ));

    match services::ensure_default_agents(&db, &adapter_registry).await {
        Ok(agents) => info!(agent_count = agents.len(), "default agents ready"),
        Err(error) => warn!(%error, "default agent upsert failed"),
    }

    if cli.demo {
        if let Err(error) = services::install_demo_data(&db).await {
            error!(%error, "demo install failed");
            std::process::exit(1);
        }
    }

    let embedded_daemon = if cli.no_embedded_daemon {
        None
    } else {
        Some(Arc::new(
            services::EmbeddedDaemon::new(
                Arc::clone(&db),
                Arc::clone(&event_bus),
                Arc::clone(&adapter_registry),
                forge_home,
                workspace_root.clone(),
            )
            .await
            .expect("embedded daemon init"),
        ))
    };
    let _embedded_handle = embedded_daemon.as_ref().map(|d| Arc::clone(d).start());

    // 2. Run crash recovery
    let recovery = services::CrashRecovery::new(Arc::clone(&db), Arc::clone(&event_bus));
    match recovery.run().await {
        Ok(count) if count > 0 => info!(recovered_count = count, "recovered orphaned tasks"),
        Ok(_) => {}
        Err(error) => warn!(%error, "crash recovery failed"),
    }

    // 4. Start daemon monitor
    let daemon_monitor = Arc::new(services::DaemonMonitor::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
    ));
    let _daemon_monitor_handle = Arc::clone(&daemon_monitor).start();

    // Start lifecycle event emitter
    let mut plugin_registry = services::lifecycle::PluginRegistry::new();
    plugin_registry.register(Arc::new(
        services::lifecycle::knowledge_inject::KnowledgeInjectPlugin,
    ));
    plugin_registry.register(Arc::new(
        services::lifecycle::knowledge_capture::KnowledgeCapturePlugin,
    ));
    let plugin_registry = Arc::new(plugin_registry);
    let lifecycle_emitter = services::lifecycle::LifecycleEventEmitter::new(
        Arc::clone(&db),
        Arc::clone(&plugin_registry),
    );
    let lifecycle_rx = event_bus.subscribe();
    tokio::spawn(async move { lifecycle_emitter.run(lifecycle_rx).await });

    // 5. Build app state and start server
    if !effective_config.server.mcp_enabled {
        info!("mcp endpoint disabled");
    }
    let jwt_secret = config
        .resolve_jwt_secret()
        .expect("Failed to resolve JWT secret");
    let state = api::AppState::with_adapter_registry_services_and_shutdown(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        effective_config.server.mcp_enabled,
        adapter_registry,
        merge_service,
        Arc::clone(&cleanup_scheduler),
        review_runner,
        api::state::ShutdownSignal::new(),
        config.workflows_dir(),
        jwt_secret,
        effective_config.server.bcrypt_cost,
    )
    .with_effective_config(effective_config.clone());
    match state
        .daemon_service
        .mark_external_daemons_disconnected(
            &services::embedded_daemon::embedded_machine_id(),
            "server startup",
        )
        .await
    {
        Ok(count) if count > 0 => info!(
            disconnected_count = count,
            "marked stale external daemons offline at startup"
        ),
        Ok(_) => {}
        Err(error) => warn!(%error, "failed to mark stale external daemons offline at startup"),
    }
    let project_hook_service_handle = Arc::clone(&state.project_hook_service).start();
    let cleanup_handle = Arc::clone(&cleanup_scheduler).spawn(state.shutdown_signal.subscribe());
    let shared_media_cleanup_handle =
        Arc::clone(&shared_media_cleanup_scheduler).spawn(state.shutdown_signal.subscribe());
    let task_dispatcher = Arc::new(services::TaskDispatcher::new(
        Arc::clone(&state.db),
        Arc::clone(&state.event_bus),
        Arc::clone(&state.task_service),
    ));
    let monitor = Arc::new(
        services::HeartbeatMonitor::new(Arc::clone(&state.db), Arc::clone(&state.event_bus))
            .with_task_service(Arc::clone(&state.task_service))
            .with_task_executor(Arc::clone(&state.task_executor))
            .with_daemon_connections(Arc::clone(&state.daemon_connections)),
    );
    let monitor_handle = Arc::clone(&monitor).start();
    let task_dispatcher_handle = Arc::clone(&task_dispatcher).start();
    let external_sync = Arc::new(services::ExternalSyncService::new(
        Arc::clone(&state.db),
        Arc::clone(&state.event_bus),
        Arc::clone(&state.task_service),
    ));
    let _external_sync_handle = Arc::clone(&external_sync).start();
    let state = state.with_task_dispatcher(Arc::clone(&task_dispatcher));
    let mut agent_chat_turn_worker_handle =
        Arc::clone(&state.agent_chat_turn_worker).start(state.shutdown_signal.subscribe());
    let memory_consumer = Arc::new(services::AgentChatMemoryConsumer::new(
        Arc::clone(&state.db),
        services::memory_consumer_lease_owner(),
    ));
    let mut memory_consumer_handle = memory_consumer.start(state.shutdown_signal.subscribe());
    let coordination_consumer = Arc::new(services::CoordinationOutcomeConsumer::new(
        Arc::clone(&state.db),
        services::coordination_consumer_lease_owner(),
    ));
    let mut coordination_consumer_handle =
        coordination_consumer.start(state.shutdown_signal.subscribe());
    let attention_projection = Arc::new(services::AttentionService::new(Arc::clone(&state.db)));
    let mut attention_projection_handle =
        attention_projection.start(state.shutdown_signal.subscribe());

    if let Err(error) = state.workflow_template_service.initialize().await {
        warn!(%error, "workflow template initialization failed");
    }

    // 6. Install graceful shutdown
    let shutdown = Arc::new(
        services::GracefulShutdown::new(Arc::clone(&state.db), Arc::clone(&state.event_bus))
            .with_task_executor(Arc::clone(&state.task_executor)),
    );
    let server_shutdown_signal = state.shutdown_signal.clone();
    let shutdown_clone = Arc::clone(&shutdown);
    let handler_shutdown_signal = server_shutdown_signal.clone();
    let shutdown_handle = tokio::spawn(async move {
        termination_signal().await;
        info!("shutting down gracefully");
        handler_shutdown_signal.request();
        monitor.stop();
        daemon_monitor.stop();
        task_dispatcher.stop();
        external_sync.stop();
        if let Some(embedded_daemon) = &embedded_daemon {
            embedded_daemon.stop();
        }
        match tokio::time::timeout(Duration::from_secs(10), shutdown_clone.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(%error, "graceful shutdown failed"),
            Err(_) => warn!("graceful shutdown timed out"),
        }
    });

    if effective_config.server.mcp_enabled {
        info!(
            bind_addr = %effective_config.server.bind,
            management_url = %local_url(addr.port(), "/"),
            api_base_url = %local_url(addr.port(), "/api/v1"),
            healthz_url = %local_url(addr.port(), "/healthz"),
            mcp_url = %local_url(addr.port(), "/mcp"),
            workspace_root = %workspace_root.display(),
            port = addr.port(),
            "forge server listening"
        );
    } else {
        info!(
            bind_addr = %effective_config.server.bind,
            management_url = %local_url(addr.port(), "/"),
            api_base_url = %local_url(addr.port(), "/api/v1"),
            healthz_url = %local_url(addr.port(), "/healthz"),
            workspace_root = %workspace_root.display(),
            port = addr.port(),
            "forge server listening"
        );
    }

    let api_shutdown_signal = server_shutdown_signal.clone();
    let mut api_handle = tokio::spawn(api::serve_with_listener(
        listener,
        state,
        web_dist,
        async move {
            api_shutdown_signal.wait().await;
        },
    ));

    tokio::select! {
        result = &mut api_handle => {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    error!(%error, "forge api failed");
                    std::process::exit(1);
                }
                Err(error) => {
                    error!(%error, "forge api task failed");
                    std::process::exit(1);
                }
            }
        }
        _ = server_shutdown_signal.wait() => {
            match tokio::time::timeout(SERVER_GRACEFUL_SHUTDOWN_TIMEOUT, &mut api_handle).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    error!(%error, "forge api failed during shutdown");
                    std::process::exit(1);
                }
                Ok(Err(error)) => {
                    error!(%error, "forge api task failed during shutdown");
                    std::process::exit(1);
                }
                Err(_) => {
                    warn!("forge api graceful shutdown timed out; aborting server task");
                    api_handle.abort();
                    match api_handle.await {
                        Err(error) if error.is_cancelled() => {}
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => warn!(%error, "forge api failed after shutdown abort"),
                        Err(error) => warn!(%error, "forge api task failed after shutdown abort"),
                    }
                }
            }
        }
    }

    let _ = shutdown_handle.await;
    let _ = cleanup_handle.await;
    let _ = shared_media_cleanup_handle.await;
    match tokio::time::timeout(Duration::from_secs(5), monitor_handle).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "heartbeat monitor task failed during shutdown"),
        Err(_) => warn!("heartbeat monitor did not stop before shutdown timeout"),
    }
    match tokio::time::timeout(Duration::from_secs(5), task_dispatcher_handle).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "task dispatcher task failed during shutdown"),
        Err(_) => warn!("task dispatcher did not stop before shutdown timeout"),
    }
    project_hook_service_handle.abort();
    match tokio::time::timeout(Duration::from_secs(5), &mut agent_chat_turn_worker_handle).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "Agent Chat turn worker failed during shutdown"),
        Err(_) => {
            warn!("Agent Chat turn worker did not stop before shutdown timeout");
            agent_chat_turn_worker_handle.abort();
            let _ = agent_chat_turn_worker_handle.await;
        }
    }
    match tokio::time::timeout(Duration::from_secs(5), &mut memory_consumer_handle).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "Agent Chat memory consumer failed during shutdown"),
        Err(_) => {
            warn!("Agent Chat memory consumer did not stop before shutdown timeout");
            memory_consumer_handle.abort();
            let _ = memory_consumer_handle.await;
        }
    }
    match tokio::time::timeout(Duration::from_secs(5), &mut coordination_consumer_handle).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "Task coordination consumer failed during shutdown"),
        Err(_) => {
            warn!("Task coordination consumer did not stop before shutdown timeout");
            coordination_consumer_handle.abort();
            let _ = coordination_consumer_handle.await;
        }
    }
    match tokio::time::timeout(Duration::from_secs(5), &mut attention_projection_handle).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "Attention projection worker failed during shutdown"),
        Err(_) => {
            warn!("Attention projection worker did not stop before shutdown timeout");
            attention_projection_handle.abort();
            let _ = attention_projection_handle.await;
        }
    }
}

fn init_tracing(log_dir: &std::path::Path) {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));

    std::fs::create_dir_all(log_dir).expect("Failed to create log directory");
    let file_appender = tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_suffix("log")
        .build(log_dir)
        .expect("Failed to create log file appender");
    let writer = std::io::stderr.and(file_appender);

    if matches!(
        std::env::var("FORGE_LOG_FORMAT").as_deref(),
        Ok("json" | "JSON")
    ) {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(writer)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(writer)
            .compact()
            .init();
    }
}

fn absolute_path(path: PathBuf) -> std::io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn media_storage_root(data_dir: &Path) -> PathBuf {
    data_dir.join("media")
}

fn bind_server_listener(config: &ForgeConfig, configured_addr: SocketAddr) -> TcpListener {
    if configured_addr.port() == 0 {
        match read_server_state(&config.forge.data_dir) {
            Ok(Some(state)) => match state.bind.parse::<SocketAddr>() {
                Ok(addr) if addr.port() != 0 => match TcpListener::bind(addr) {
                    Ok(listener) => {
                        info!(bind_addr = %addr, "reusing persisted Forge server port");
                        return listener;
                    }
                    Err(error) => {
                        warn!(
                            bind_addr = %addr,
                            %error,
                            "persisted Forge server port unavailable; selecting a new port"
                        );
                    }
                },
                Ok(_) => {}
                Err(error) => {
                    warn!(
                        bind_addr = %state.bind,
                        %error,
                        "ignoring invalid persisted Forge server bind"
                    );
                }
            },
            Ok(None) => {}
            Err(error) => {
                warn!(
                    data_dir = %config.forge.data_dir.display(),
                    %error,
                    "failed to read persisted Forge server port"
                );
            }
        }
    }

    TcpListener::bind(configured_addr).expect("Failed to bind server listener")
}

fn server_url_for_addr(addr: SocketAddr) -> String {
    let host = match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => "127.0.0.1".to_owned(),
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) if ip.is_unspecified() => "[::1]".to_owned(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    };
    format!("http://{host}:{}", addr.port())
}

fn web_dist_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("FORGE_WEB_DIST_DIR") {
        return PathBuf::from(path);
    }

    let cwd_dist = PathBuf::from("web/dist");
    if cwd_dist.join("index.html").is_file() {
        return cwd_dist;
    }

    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(prefix) = exe_path.parent().and_then(Path::parent) {
            let installed_dist = prefix.join("share/forge/web/dist");
            if installed_dist.join("index.html").is_file() {
                return installed_dist;
            }
        }
    }

    cwd_dist
}

#[cfg(unix)]
async fn termination_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}

#[cfg(not(unix))]
async fn termination_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn local_url(port: u16, path: &str) -> String {
    format!("http://127.0.0.1:{port}{path}")
}

#[cfg(test)]
mod tests {
    use super::{absolute_path, media_storage_root, server_url_for_addr, web_dist_dir};
    use std::{
        net::SocketAddr,
        path::{Path, PathBuf},
    };

    #[test]
    fn absolute_path_preserves_absolute_paths() {
        let path = if cfg!(windows) {
            PathBuf::from(r"C:\forge\workspaces")
        } else {
            PathBuf::from("/tmp/forge/workspaces")
        };

        assert_eq!(absolute_path(path.clone()).expect("path resolves"), path);
    }

    #[test]
    fn absolute_path_resolves_relative_paths_from_current_dir() {
        let path = absolute_path(PathBuf::from("test/workspaces")).expect("path resolves");

        assert!(path.is_absolute());
        assert!(path.ends_with("test/workspaces"));
    }

    #[test]
    fn media_storage_root_matches_task_media_layout() {
        let data_dir = Path::new("/tmp/forge-data");

        assert_eq!(media_storage_root(data_dir), data_dir.join("media"));
    }

    #[test]
    fn web_dist_dir_honors_env_override() {
        let previous = std::env::var_os("FORGE_WEB_DIST_DIR");
        std::env::set_var("FORGE_WEB_DIST_DIR", "/tmp/forge-web-dist");

        assert_eq!(web_dist_dir(), PathBuf::from("/tmp/forge-web-dist"));

        if let Some(previous) = previous {
            std::env::set_var("FORGE_WEB_DIST_DIR", previous);
        } else {
            std::env::remove_var("FORGE_WEB_DIST_DIR");
        }
    }

    #[test]
    fn server_url_uses_loopback_for_unspecified_bind() {
        let addr: SocketAddr = "0.0.0.0:49152".parse().expect("addr parses");

        assert_eq!(server_url_for_addr(addr), "http://127.0.0.1:49152");
    }
}
