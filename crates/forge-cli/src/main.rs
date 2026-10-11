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
    /// Maximum concurrent runs on the server host (default: automatic; 0: unlimited).
    #[arg(long)]
    max_concurrent_runs: Option<u32>,
    /// Build jobs per run (unset: automatic; 0: disabled).
    #[arg(long)]
    build_jobs_per_run: Option<u32>,
    /// Unix niceness increment for run children (0: off).
    #[arg(long, value_parser = clap::value_parser!(u32).range(0..=19))]
    run_nice: Option<u32>,
    /// Usage observation index budget in MiB (default: 128; 0: memoized full reads).
    #[arg(long)]
    usage_index_budget_mb: Option<u32>,
    /// Whole-check bundle wall timeout in seconds (default: 1800; passive until runner cutover).
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    check_run_timeout_seconds: Option<u32>,
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
    /// Cursor inactivity threshold for stalled event consumers, in seconds.
    #[arg(long)]
    event_consumer_stall_seconds: Option<u32>,
    /// Take the workspace root over for this database, then start as usual.
    /// Garbage collection only runs on a root this database owns; after a
    /// database reset the root still names the old one and nothing is
    /// reclaimed. Use this once, with no other Forge server on the root:
    /// directories the old database knew and this one does not are
    /// quarantined and deleted a day later.
    #[arg(long = "reclaim-workspace-gc")]
    reclaim_workspace_gc: bool,
    /// Move the workspace root (Task worktrees, repository clones, execution
    /// logs) to NEW_ROOT, or to <data dir>/worktrees when no path is given,
    /// then exit. Stop Forge first. Every file is kept: entries are renamed
    /// on one filesystem, otherwise copied, compared and only then removed;
    /// Git worktree links and every stored path are rewritten. A run that
    /// was interrupted is finished by running the command again.
    #[arg(
        long = "migrate-workspace-root",
        value_name = "NEW_ROOT",
        num_args = 0..=1,
        conflicts_with_all = [
            "demo",
            "no_mcp",
            "no_embedded_daemon",
            "reclaim_workspace_gc",
            "convert_db_to_incremental_vacuum"
        ]
    )]
    migrate_workspace_root: Option<Option<PathBuf>>,
    /// Convert an existing database to incremental auto-vacuum, then exit.
    /// Stop Forge first. Full VACUUM locks the database and needs extra disk space.
    #[arg(long, conflicts_with_all = ["demo", "no_mcp", "no_embedded_daemon"])]
    convert_db_to_incremental_vacuum: bool,
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
            server_max_concurrent_runs: cli.max_concurrent_runs,
            server_build_jobs_per_run: cli.build_jobs_per_run,
            server_run_nice: cli.run_nice,
            server_usage_index_budget_mb: cli.usage_index_budget_mb,
            server_check_run_timeout_seconds: cli.check_run_timeout_seconds,
            mcp_enabled: if cli.no_mcp { Some(false) } else { None },
            data_dir: cli.data_dir,
            event_consumer_stall_seconds: cli.event_consumer_stall_seconds,
            ..Default::default()
        },
    )
    .expect("Failed to load config");

    // The offline conversion and all Forge runtimes share this data-root
    // process lock. Hold it through shutdown (the OS releases it on crashes).
    let _runtime_lock = acquire_runtime_lock(&config.forge.data_dir).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    });
    if cli.convert_db_to_incremental_vacuum {
        let db_path = config.db_path();
        let database_url = format!("sqlite:{}", db_path.display());
        eprintln!("Converting {}: full VACUUM holds an exclusive database lock and may need up to twice the database size in additional free disk space.", db_path.display());
        if let Err(error) = db::convert_sqlite_to_incremental(&database_url).await {
            eprintln!("Database conversion failed: {error}");
            std::process::exit(1);
        }
        println!(
            "Database is in incremental auto-vacuum mode: {}",
            db_path.display()
        );
        return;
    }

    if let Some(target) = cli.migrate_workspace_root {
        std::process::exit(migrate_workspace_root(&config, target).await);
    }

    init_tracing(&config.forge.data_dir.join("logs"));
    executors::run_process::install_machine_policy(Arc::new(
        executors::run_process::MachineRunPolicy::new((&config.server).into()),
    ));

    // 1. Create database pool and run migrations, then settle the workspace
    // root against what this database recorded. A start that must be
    // refused is refused here: before a port is bound, before the server
    // state file is written, before anything is told where the root is.
    let db_path = config.db_path();
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent).expect("Failed to create data directory");
    }
    let database_url = format!("sqlite:{}", db_path.display());
    let forge_home = db_path
        .parent()
        .map_or_else(|| PathBuf::from("."), PathBuf::from);
    let pool = db::create_sqlite_pool(&database_url)
        .await
        .expect("Failed to create database pool");
    db::run_migrations(&pool)
        .await
        .expect("Failed to run migrations");
    let db = Arc::new(db::SqliteDb::new(pool));
    let workspace_root = settle_workspace_root(&db, &config).await;

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
    // The settled root is handed to the runtime builder below; code that
    // holds only the database reads the record `settle` wrote
    // (`services::workspace_root::root_of`). Nothing reads the environment.
    // Opt-in: nothing is installed while `workspace.compiler_cache.wrapper`
    // is unset.
    executors::compiler_cache::install_configured(
        &workspace_root,
        &config.workspace.compiler_cache,
    );
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
    // Only a running server adopts its workspace root, and only an adopted
    // root is ever garbage-collected: nothing built for a test does this.
    // The same goes for the locations older versions left in the system
    // temp directory.
    let ownership = if cli.reclaim_workspace_gc {
        runtime.cleanup_scheduler.reclaim_workspace_root().await
    } else {
        runtime.cleanup_scheduler.adopt_workspace_root().await
    };
    match ownership {
        Ok(executors::gc::Ownership::Mine) => {}
        Ok(ownership) => tracing::warn!(
            root = %workspace_root.display(),
            state = ownership.as_str(),
            "workspace garbage collection is OFF: the workspace root is owned by another Forge database or cannot be a workspace root, so nothing is reclaimed and the disk can fill. If the root belongs to this server, restart once with --reclaim-workspace-gc"
        ),
        Err(error) => {
            tracing::warn!(%error, "workspace root ownership could not be settled; garbage collection is off")
        }
    }
    runtime
        .cleanup_scheduler
        .set_legacy_temp_dir(std::env::temp_dir());
    runtime.enable_disk_admission();

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
    let periodic_workers = runtime.operator_status_service.periodic_workers();
    let _embedded_handle = embedded_daemon
        .as_ref()
        .map(|d| Arc::clone(d).start(&periodic_workers));

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
    let _daemon_monitor_handle = Arc::clone(&daemon_monitor).start(&periodic_workers);
    let state =
        api::AppState::from_runtime_arc(Arc::clone(&runtime), effective_config.server.mcp_enabled);
    executors::run_process::install_machine_policy(Arc::clone(&state.run_process_policy));
    let shared_media_cleanup_handle = Arc::clone(&shared_media_cleanup_scheduler)
        .spawn(&periodic_workers, state.shutdown_signal.subscribe());
    let external_sync = Arc::new(services::ExternalSyncService::new(
        Arc::clone(&state.db),
        Arc::clone(&state.event_bus),
        Arc::clone(&state.task_service),
    ));
    let _external_sync_handle = Arc::clone(&external_sync).start(&periodic_workers);

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

/// The workspace root this server runs on: the one its database recorded,
/// or on a first start the configured one (see `services::workspace_root`).
/// A configured root that would leave recorded workspaces behind, and a move
/// that did not finish, stop the start here.
async fn settle_workspace_root(db: &db::SqliteDb, config: &ForgeConfig) -> PathBuf {
    let choice = services::workspace_root::RootChoice {
        configured: absolute_path(config.workspace.root.clone())
            .expect("Failed to resolve workspace root path"),
        explicit: config.workspace.root_explicit,
        data_dir: config.forge.data_dir.clone(),
        system_temp: std::env::temp_dir(),
        migrate_command: migrate_command(config),
    };
    match services::workspace_root::settle(db, &choice).await {
        Ok(settled) => {
            if settled.in_system_temp {
                warn!(
                    "{}",
                    services::workspace_root::system_temp_warning(
                        &settled.root,
                        &choice.migrate_command
                    )
                );
            }
            for warning in &settled.warnings {
                warn!("{warning}");
            }
            settled.root
        }
        Err(error) => {
            // Tracing writes to stderr and to the log file.
            error!("Forge cannot start on this workspace root: {error}");
            std::process::exit(1);
        }
    }
}

/// The move command as this server's operator types it.
fn migrate_command(config: &ForgeConfig) -> String {
    services::workspace_root::RootChoice::migrate_command_for(
        &config.forge.data_dir,
        &config::default_data_dir(),
    )
}

/// `forge --migrate-workspace-root [NEW_ROOT]`: the process exit code. The
/// caller holds the data directory's runtime lock, so no server is running.
async fn migrate_workspace_root(config: &ForgeConfig, target: Option<PathBuf>) -> i32 {
    let opened = async {
        let data_dir = absolute_path(config.forge.data_dir.clone()).map_err(|e| e.to_string())?;
        std::fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;
        // The same file a start opens.
        let database_url = format!("sqlite:{}", config.db_path().display());
        let pool = db::create_sqlite_pool(&database_url)
            .await
            .map_err(|e| e.to_string())?;
        db::run_migrations(&pool).await.map_err(|e| e.to_string())?;
        Ok::<_, String>((db::SqliteDb::new(pool), data_dir))
    };
    let (db, data_dir) = match opened.await {
        Ok(opened) => opened,
        Err(error) => {
            eprintln!(
                "The workspace root was not moved: the database could not be opened ({error})."
            );
            return 1;
        }
    };
    // No path given: where this configuration would start, so the next
    // start is not refused for a root that is set and differs.
    let target = target.or_else(|| {
        config
            .workspace
            .root_explicit
            .then(|| config.workspace.root.clone())
    });
    let request = services::workspace_root::migrate::MigrateRequest::new(
        data_dir,
        target,
        std::env::temp_dir(),
        config.workspace.min_free_bytes,
    )
    .with_command(migrate_command(config));
    match services::workspace_root::migrate::migrate(&db, &request).await {
        Ok(report) => {
            print!("{report}");
            0
        }
        Err(error) => {
            eprintln!("{error}");
            1
        }
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

fn acquire_runtime_lock(data_dir: &Path) -> Result<std::fs::File, String> {
    std::fs::create_dir_all(data_dir)
        .map_err(|error| format!("Failed to create {}: {error}", data_dir.display()))?;
    let path = data_dir.join("runtime.lock");
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let file = options
        .open(&path)
        .map_err(|error| format!("Failed to open {}: {error}", path.display()))?;
    fs2::FileExt::try_lock_exclusive(&file).map_err(|error| format!(
        "Cannot lock {}: another Forge runtime or database conversion may be running. Stop it before retrying ({error}).", path.display()
    ))?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::{
        absolute_path, acquire_runtime_lock, media_storage_root, server_url_for_addr, web_dist_dir,
        Cli,
    };
    use clap::Parser;
    use std::{
        net::SocketAddr,
        path::{Path, PathBuf},
    };

    #[test]
    fn usage_index_budget_cli_accepts_zero_and_mib() {
        assert_eq!(
            Cli::try_parse_from(["forge"])
                .unwrap()
                .usage_index_budget_mb,
            None
        );
        for budget in ["0", "64", "4294967295"] {
            assert_eq!(
                Cli::try_parse_from(["forge", "--usage-index-budget-mb", budget])
                    .unwrap()
                    .usage_index_budget_mb,
                Some(budget.parse().unwrap())
            );
        }
        for budget in ["-1", "1.5", "4294967296"] {
            assert!(Cli::try_parse_from(["forge", "--usage-index-budget-mb", budget]).is_err());
        }
    }

    #[test]
    fn storage_cli_parses_stall_setting_and_offline_conversion() {
        let cli = Cli::try_parse_from(["forge", "--event-consumer-stall-seconds", "60"]).unwrap();
        assert_eq!(cli.event_consumer_stall_seconds, Some(60));
        assert!(
            Cli::try_parse_from(["forge", "--convert-db-to-incremental-vacuum"])
                .unwrap()
                .convert_db_to_incremental_vacuum
        );
        assert!(
            Cli::try_parse_from(["forge", "--convert-db-to-incremental-vacuum", "--demo"]).is_err()
        );
    }

    #[test]
    fn storage_conversion_runtime_lock_excludes_a_live_server() {
        let dir = std::env::temp_dir().join(format!("forge-outbox-lock-{}", db::new_uuid_v4()));
        let first = acquire_runtime_lock(&dir).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(first.metadata().unwrap().permissions().mode() & 0o077, 0);
        }
        assert!(acquire_runtime_lock(&dir).is_err());
        drop(first);
        drop(acquire_runtime_lock(&dir).unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }

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

#[cfg(test)]
mod migrate_workspace_root_tests {
    use super::*;

    #[test]
    fn flag_takes_an_optional_target_and_excludes_a_server_start() {
        assert_eq!(
            Cli::try_parse_from(["forge"])
                .unwrap()
                .migrate_workspace_root,
            None
        );
        assert_eq!(
            Cli::try_parse_from(["forge", "--migrate-workspace-root"])
                .unwrap()
                .migrate_workspace_root,
            Some(None)
        );
        assert_eq!(
            Cli::try_parse_from(["forge", "--migrate-workspace-root", "/srv/forge/worktrees"])
                .unwrap()
                .migrate_workspace_root,
            Some(Some(PathBuf::from("/srv/forge/worktrees")))
        );
        let with_data_dir =
            Cli::try_parse_from(["forge", "--data-dir", "./test", "--migrate-workspace-root"])
                .unwrap();
        assert_eq!(with_data_dir.migrate_workspace_root, Some(None));
        for other in [
            "--demo",
            "--no-mcp",
            "--no-embedded-daemon",
            "--reclaim-workspace-gc",
            "--convert-db-to-incremental-vacuum",
        ] {
            assert!(
                Cli::try_parse_from(["forge", "--migrate-workspace-root", other]).is_err(),
                "{other}"
            );
        }
    }
}

#[cfg(test)]
mod check_timeout_tests {
    use super::*;
    #[test]
    fn flag_accepts_seconds_and_rejects_zero() {
        assert_eq!(
            Cli::try_parse_from(["forge", "--check-run-timeout-seconds", "90"])
                .unwrap()
                .check_run_timeout_seconds,
            Some(90)
        );
        assert!(Cli::try_parse_from(["forge", "--check-run-timeout-seconds", "0"]).is_err());
        assert_eq!(
            Cli::try_parse_from(["forge"])
                .unwrap()
                .check_run_timeout_seconds,
            None
        );
    }
}
