mod commands;
mod connect;
mod credentials;
mod detect;
mod reporter;
mod terminal;

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use api_types::DaemonRegisterRequest;
use clap::Parser;
use credentials::DaemonCredentials;
use forge_client::daemon_link::{register_with_retry, DaemonClient};
use tokio::{sync::watch, time};
use tracing_subscriber::EnvFilter;

const DEFAULT_REPORT_INTERVAL_SECONDS: u64 = 60;
const REGISTRATION_RETRY_PAUSE: Duration = Duration::from_secs(30);
const DEFAULT_LOG_FILTER: &str =
    "forge_daemon=info,forge_client=info,cli_adapters=info,executors=info";

#[derive(Parser)]
#[command(name = "forge-daemon", about = "Forge standalone remote daemon")]
struct Cli {
    /// Maximum concurrent runs (default: automatic; 0: unlimited).
    #[arg(long)]
    max_concurrent_runs: Option<u32>,
    /// Forge server URL, for example https://forge.example.com.
    #[arg(long)]
    server: String,
    /// Directory this daemon should advertise and resolve relative filesystem commands from.
    #[arg(long)]
    workspace_root: PathBuf,
    /// Credentials file. Defaults to ~/.config/forge-daemon/{server_host}/credentials.json.
    #[arg(long)]
    credentials: Option<PathBuf>,
    /// Periodic daemon report interval.
    #[arg(long, default_value_t = DEFAULT_REPORT_INTERVAL_SECONDS)]
    interval_seconds: u64,
    /// User access token used during first registration.
    #[arg(long)]
    token: Option<String>,
    /// Extra label in key=value form. Can be repeated.
    #[arg(long = "label")]
    label: Vec<String>,
    /// Hostname to report to Forge.
    #[arg(long)]
    hostname: Option<String>,
    /// Stable daemon machine id.
    #[arg(long)]
    machine_id: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing();

    let workspace_root = prepare_workspace_root(&cli.workspace_root)?;
    let labels = parse_labels(&cli.label)?;
    let hostname = cli
        .hostname
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(local_hostname);
    let machine_id = cli
        .machine_id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| local_machine_id(&hostname));
    let credentials_path = cli
        .credentials
        .clone()
        .unwrap_or_else(|| credentials::default_path(&cli.server));
    let owner_token = resolve_owner_token(cli.token.as_deref());
    let daemon_config = forge_client::daemon_config::DaemonConfig::load(&credentials_path)?;
    let run_policy = daemon_config.run_policy();
    let max_concurrent_runs = daemon_config.run_cap(cli.max_concurrent_runs);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        termination_signal().await;
        let _ = shutdown_tx.send(true);
    });

    let mut client = DaemonClient::new(cli.server.clone())?;
    if register_or_load_credentials(
        &mut client,
        &credentials_path,
        &workspace_root,
        &machine_id,
        &hostname,
        &labels,
        max_concurrent_runs,
        owner_token.as_deref(),
        shutdown_rx.clone(),
    )
    .await?
    .is_none()
    {
        tracing::info!("daemon stopped before registration completed");
        return Ok(());
    }

    let client = Arc::new(client);
    let active_executions = forge_client::daemon_runtime::ActiveExecutionTracker::default();
    let reporter_handle = tokio::spawn(reporter::run(
        Arc::clone(&client),
        workspace_root.clone(),
        labels.clone(),
        max_concurrent_runs,
        cli.interval_seconds,
        active_executions.clone(),
        shutdown_rx.clone(),
    ));
    let mut connect_handle = tokio::spawn(connect::run(
        Arc::clone(&client),
        workspace_root.clone(),
        active_executions,
        shutdown_rx.clone(),
        run_policy,
    ));

    tokio::select! {
        result = &mut connect_handle => {
            reporter_handle.abort();
            result.context("daemon command loop task failed")??;
            if *shutdown_rx.borrow() { return Ok(()); }
            anyhow::bail!("daemon command loop stopped unexpectedly");
        }
        () = wait_for_shutdown(shutdown_rx.clone()) => {}
    }
    tracing::info!("forge-daemon shutting down");

    if let Err(error) = reporter_handle.await.context("join daemon reporter task")? {
        tracing::warn!(error = %error, "daemon reporter stopped with error");
    }

    connect_handle.abort();
    match connect_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(error = %error, "daemon command loop stopped with error"),
        Err(error) if error.is_cancelled() => {}
        Err(error) => tracing::warn!(error = %error, "daemon command loop task failed"),
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn register_or_load_credentials(
    client: &mut DaemonClient,
    credentials_path: &Path,
    workspace_root: &Path,
    machine_id: &str,
    hostname: &str,
    labels: &BTreeMap<String, String>,
    max_concurrent_runs: u32,
    owner_token: Option<&str>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<Option<DaemonCredentials>> {
    if owner_token.is_none() {
        if let Some(credentials) = credentials::load(credentials_path).await? {
            client.set_credentials(credentials.daemon_id.clone(), credentials.token.clone());
            // Fresh process: an empty active-execution claim (not None) is
            // deliberate — it lets the server reconcile executions stranded
            // by a previous daemon process as soon as we report.
            match reporter::report_once(
                client,
                workspace_root,
                labels,
                max_concurrent_runs,
                &forge_client::daemon_runtime::ActiveExecutionTracker::default(),
            )
            .await
            {
                Ok(daemon) => {
                    tracing::info!(
                        daemon_id = %daemon.id,
                        credentials_path = %credentials_path.display(),
                        "loaded existing daemon credentials"
                    );
                    return Ok(Some(credentials));
                }
                Err(error) if is_auth_failure(&error) => {
                    tracing::warn!(
                        error = %error,
                        credentials_path = %credentials_path.display(),
                        "existing daemon credentials were rejected; re-registering"
                    );
                    remove_credentials(credentials_path).await?;
                    client.daemon_id = None;
                    client.token = None;
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        credentials_path = %credentials_path.display(),
                        "could not verify existing daemon credentials; continuing with them"
                    );
                    return Ok(Some(credentials));
                }
            }
        }
    }

    loop {
        if *shutdown.borrow() {
            return Ok(None);
        }

        let request = registration_request(
            machine_id,
            hostname,
            workspace_root,
            labels,
            max_concurrent_runs,
        );
        tokio::select! {
            result = register_with_retry(client, &request, owner_token) => {
                match result {
                    Ok(response) => {
                        let credentials = DaemonCredentials {
                            daemon_id: response.daemon_id,
                            token: response.registration_token,
                        };
                        credentials::save(credentials_path, &credentials).await?;
                        tracing::info!(
                            daemon_id = %credentials.daemon_id,
                            credentials_path = %credentials_path.display(),
                            "registered daemon"
                        );
                        return Ok(Some(credentials));
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            retry_pause_secs = REGISTRATION_RETRY_PAUSE.as_secs(),
                            "daemon registration failed; retrying"
                        );
                    }
                }
            }
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    return Ok(None);
                }
            }
        }

        tokio::select! {
            () = time::sleep(REGISTRATION_RETRY_PAUSE) => {}
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    return Ok(None);
                }
            }
        }
    }
}

fn registration_request(
    machine_id: &str,
    hostname: &str,
    workspace_root: &Path,
    labels: &BTreeMap<String, String>,
    max_concurrent_runs: u32,
) -> DaemonRegisterRequest {
    DaemonRegisterRequest {
        max_concurrent_runs: Some(max_concurrent_runs),
        machine_id: machine_id.to_owned(),
        hostname: hostname.to_owned(),
        os: std::env::consts::OS.to_owned(),
        arch: std::env::consts::ARCH.to_owned(),
        agent_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
        labels: Some(reporter::labels_value(labels)),
        runtimes: Some(vec![reporter::runtime_report(workspace_root)]),
    }
}

async fn remove_credentials(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("remove daemon credentials at {}", path.display()))
        }
    }
}

fn parse_labels(input: &[String]) -> Result<BTreeMap<String, String>> {
    let mut labels = BTreeMap::from([("mode".to_owned(), "external".to_owned())]);
    for item in input {
        let Some((key, value)) = item.split_once('=') else {
            return Err(anyhow!("daemon label must use key=value form: {item}"));
        };
        let key = key.trim();
        if key.is_empty() {
            return Err(anyhow!("daemon label key must not be empty"));
        }
        labels.insert(key.to_owned(), value.trim().to_owned());
    }
    Ok(labels)
}

fn resolve_owner_token(explicit: Option<&str>) -> Option<String> {
    if let Some(value) = explicit.map(str::trim).filter(|value| !value.is_empty()) {
        return Some(value.to_owned());
    }

    std::env::var("FORGE_TOKEN")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn absolute_path(path: &Path) -> std::io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn prepare_workspace_root(path: &Path) -> Result<PathBuf> {
    let workspace_root = absolute_path(path)
        .with_context(|| format!("resolve workspace root {}", path.display()))?;
    fs::create_dir_all(&workspace_root)
        .with_context(|| format!("create workspace root {}", workspace_root.display()))?;
    Ok(workspace_root)
}

fn local_hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .filter(|value| !value.trim().is_empty())
        .or_else(command_hostname)
        .unwrap_or_else(|| "unknown-host".to_owned())
}

fn command_hostname() -> Option<String> {
    let output = std::process::Command::new("hostname").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let hostname = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if hostname.is_empty() {
        None
    } else {
        Some(hostname)
    }
}

fn local_machine_id(hostname: &str) -> String {
    read_machine_id()
        .map(|value| format!("external:{value}"))
        .unwrap_or_else(|| {
            format!(
                "external:{hostname}:{}:{}",
                std::env::consts::OS,
                std::env::consts::ARCH
            )
        })
}

fn read_machine_id() -> Option<String> {
    ["/etc/machine-id", "/var/lib/dbus/machine-id"]
        .iter()
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty())
}

fn init_tracing() {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .compact()
        .init();
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

fn is_auth_failure(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("401") || message.contains("403")
}

#[cfg(unix)]
async fn termination_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}

#[cfg(not(unix))]
async fn termination_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
