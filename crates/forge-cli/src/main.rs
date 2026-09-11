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
// Agent tools can synchronously poll deep recovery/workspace futures, especially
// in debug builds. Give runtime threads headroom beyond Tokio's default stack.
const RUNTIME_THREAD_STACK_SIZE: usize = 16 * 1024 * 1024;

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

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(RUNTIME_THREAD_STACK_SIZE)
        .enable_all()
        .build()
        .expect("Failed to build Forge runtime")
        .block_on(run());
}

async fn run() {
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
    // Same reason: Genesis provisioning runs the scaffold command from the
    // services crate and reads it from the environment.
    std::env::set_var("FORGE_SCAFFOLD_COMMAND", &config.scaffold.command);

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
        scaffold_command = %config.scaffold.command,
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
    let shared_media_cleanup_scheduler = Arc::new(services::SharedMediaCleanupScheduler::new(
        Arc::clone(&db),
        media_storage_root(&effective_config.forge.data_dir),
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

    // Compose the transport-neutral Forge core once.  The server adds its
    // listener, MCP, web assets, and daemon/sync workers below; Solo can use
    // this same graph without depending on the API crate.
    let jwt_secret = config
        .resolve_jwt_secret()
        .expect("Failed to resolve JWT secret");
    let mut runtime_config = effective_config.clone();
    runtime_config.workspace.root = workspace_root.clone();
    let runtime = Arc::new(
        services::ForgeRuntimeBuilder::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_adapter_registry(Arc::clone(&adapter_registry))
            .with_config(runtime_config)
            .with_workspace_root(workspace_root.clone())
            .with_workflows_dir(config.workflows_dir())
            .with_jwt_secret(jwt_secret)
            .with_bcrypt_cost(effective_config.server.bcrypt_cost)
            .build(),
    );

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

    // 5. Build app state and start server
    if !effective_config.server.mcp_enabled {
        info!("mcp endpoint disabled");
    }
    // Reconcile stale remote daemons before the shared supervisor launches
    // dispatch/heartbeat workers. Those workers must never observe an old
    // external daemon as still eligible for execution.
    match runtime
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
    let mut runtime_supervisor = services::RuntimeSupervisor::new(
        Arc::clone(&runtime),
        services::RuntimeAssemblyMode::Server,
    );
    match runtime_supervisor.start().await {
        Ok(count) if count > 0 => info!(recovered_count = count, "recovered orphaned tasks"),
        Ok(_) => {}
        Err(error) => warn!(%error, "shared runtime startup failed"),
    }
    // Server-only external daemon monitor. The shared supervisor owns local
    // core workers; this monitor starts after recovery, matching legacy
    // startup ordering, and remains outside the Solo graph.
    let daemon_monitor = Arc::new(services::DaemonMonitor::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
    ));
    let _daemon_monitor_handle = Arc::clone(&daemon_monitor).start();
    let state =
        api::AppState::from_runtime_arc(Arc::clone(&runtime), effective_config.server.mcp_enabled);
    let shared_media_cleanup_handle =
        Arc::clone(&shared_media_cleanup_scheduler).spawn(state.shutdown_signal.subscribe());
    let external_sync = Arc::new(services::ExternalSyncService::new(
        Arc::clone(&state.db),
        Arc::clone(&state.event_bus),
        Arc::clone(&state.task_service),
    ));
    let _external_sync_handle = Arc::clone(&external_sync).start();

    // 6. Install graceful shutdown. The shared supervisor owns the core
    // signal and joins its worker handles; this loop only coordinates the
    // server-only listener/daemon workers.
    let server_shutdown_signal = runtime_supervisor.shutdown_signal();

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
        _ = termination_signal() => {
            info!("shutting down gracefully");
            runtime_supervisor.request_shutdown();
            daemon_monitor.stop();
            external_sync.stop();
            if let Some(embedded_daemon) = &embedded_daemon {
                embedded_daemon.stop();
            }
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

    daemon_monitor.stop();
    external_sync.stop();
    if let Some(embedded_daemon) = &embedded_daemon {
        embedded_daemon.stop();
    }
    if let Err(error) = runtime_supervisor.shutdown().await {
        warn!(%error, "shared runtime graceful shutdown failed");
    }
    let _ = shared_media_cleanup_handle.await;
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
