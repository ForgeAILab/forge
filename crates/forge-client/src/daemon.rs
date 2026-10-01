use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Command as StdCommand, Stdio},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use api_types::{
    DaemonRegisterRequest, DaemonRegisterResponse, DaemonReportRequest, DaemonResponse,
    DetectedCli, RuntimeReport,
};
use clap::Subcommand;
use executors::{AvailabilityStatus, ExecutorKind};
use serde::{Deserialize, Serialize};
use tokio::{process::Command as TokioCommand, sync::watch, time::timeout};

use crate::{
    client::ForgeClient,
    daemon_link::DaemonClient,
    daemon_runtime,
    output::{print_json, print_table_daemons},
    OutputFormat,
};

const CREDENTIALS_FILE: &str = "daemon_credentials.json";
const DEFAULT_REPORT_INTERVAL_SECONDS: u64 = 60;
const VERSION_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(clap::Args)]
pub struct DaemonArgs {
    #[command(subcommand)]
    cmd: DaemonCmd,
}

#[derive(Subcommand)]
enum DaemonCmd {
    /// Register this machine as a daemon and keep reporting local CLI availability.
    Link {
        /// Directory this daemon should advertise as its workspace root.
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        /// Stable daemon machine id. Defaults to the local machine id when available.
        #[arg(long)]
        machine_id: Option<String>,
        /// Hostname to report to Forge.
        #[arg(long)]
        hostname: Option<String>,
        /// Credentials file for daemon id/token reuse.
        #[arg(long)]
        credentials: Option<PathBuf>,
        /// User access token used to claim the daemon during initial registration.
        #[arg(long)]
        token: Option<String>,
        /// Extra label in key=value form. Can be repeated.
        #[arg(long = "label")]
        labels: Vec<String>,
        /// Report interval while the link command is running.
        #[arg(long, default_value_t = DEFAULT_REPORT_INTERVAL_SECONDS)]
        interval_seconds: u64,
        /// Register/report once and exit.
        #[arg(long)]
        once: bool,
    },
    /// Start a previously linked daemon using saved credentials.
    Start {
        /// Directory this daemon should advertise as its workspace root.
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        /// Credentials file created by daemon link.
        #[arg(long)]
        credentials: Option<PathBuf>,
        /// Extra label in key=value form. Can be repeated.
        #[arg(long = "label")]
        labels: Vec<String>,
        /// Report interval while the daemon is running.
        #[arg(long, default_value_t = DEFAULT_REPORT_INTERVAL_SECONDS)]
        interval_seconds: u64,
    },
    /// Send one report using existing credentials.
    Report {
        /// Directory this daemon should advertise as its workspace root.
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        /// Credentials file created by daemon link.
        #[arg(long)]
        credentials: Option<PathBuf>,
        /// Extra label in key=value form. Can be repeated.
        #[arg(long = "label")]
        labels: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DaemonCredentials {
    daemon_id: String,
    token: String,
}

impl DaemonArgs {
    pub async fn run(&self, client: &ForgeClient, output: &OutputFormat) -> Result<()> {
        match &self.cmd {
            DaemonCmd::Link {
                workspace_root,
                machine_id,
                hostname,
                credentials,
                token,
                labels,
                interval_seconds,
                once,
            } => {
                let workspace_root = prepare_workspace_root(workspace_root.as_deref())?;
                let credentials_path = credentials_path(credentials.as_deref());
                let hostname = hostname
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(local_hostname);
                let machine_id = machine_id
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| local_machine_id(&hostname));
                let owner_token = resolve_owner_token(token.as_deref());
                let labels = parse_labels(labels)?;

                let linked = link_and_report(
                    client,
                    &credentials_path,
                    &workspace_root,
                    &machine_id,
                    &hostname,
                    &labels,
                    owner_token.as_deref(),
                )
                .await?;
                print_daemon(output, &linked.daemon)?;

                if *once {
                    return Ok(());
                }

                println!(
                    "daemon linked as {}; reporting every {}s with command stream enabled",
                    linked.credentials.daemon_id, interval_seconds
                );
                // This process is about to own the command stream, so claim an
                // (empty) active-execution set: it lets the server reconcile
                // executions stranded by a previous daemon process right away.
                let active_executions = daemon_runtime::ActiveExecutionTracker::default();
                report_once(
                    client,
                    &linked.credentials,
                    &workspace_root,
                    &labels,
                    Some(&active_executions),
                )
                .await?;
                run_daemon_loop(
                    client,
                    output,
                    DaemonLoopConfig {
                        credentials_path: &credentials_path,
                        initial_credentials: &linked.credentials,
                        workspace_root: &workspace_root,
                        labels: &labels,
                        interval_seconds: *interval_seconds,
                        stopped_message: "daemon link stopped",
                        active_executions: &active_executions,
                    },
                )
                .await
            }
            DaemonCmd::Start {
                workspace_root,
                credentials,
                labels,
                interval_seconds,
            } => {
                let workspace_root = prepare_workspace_root(workspace_root.as_deref())?;
                let credentials_path = credentials_path(credentials.as_deref());
                let credentials = read_credentials(&credentials_path)?.ok_or_else(|| {
                    anyhow!(
                        "missing daemon credentials at {}; run `forge-ctl daemon link` first",
                        credentials_path.display()
                    )
                })?;
                let labels = parse_labels(labels)?;
                // Fresh process: nothing is running yet, so an empty
                // active-execution claim is truthful and lets the server
                // reconcile executions stranded by a previous daemon process.
                let active_executions = daemon_runtime::ActiveExecutionTracker::default();
                let daemon = report_once(
                    client,
                    &credentials,
                    &workspace_root,
                    &labels,
                    Some(&active_executions),
                )
                .await?;
                print_daemon(output, &daemon)?;
                println!(
                    "daemon started as {}; reporting every {}s with command stream enabled",
                    credentials.daemon_id, interval_seconds
                );
                run_daemon_loop(
                    client,
                    output,
                    DaemonLoopConfig {
                        credentials_path: &credentials_path,
                        initial_credentials: &credentials,
                        workspace_root: &workspace_root,
                        labels: &labels,
                        interval_seconds: *interval_seconds,
                        stopped_message: "daemon start stopped",
                        active_executions: &active_executions,
                    },
                )
                .await
            }
            DaemonCmd::Report {
                workspace_root,
                credentials,
                labels,
            } => {
                let workspace_root = prepare_workspace_root(workspace_root.as_deref())?;
                let credentials_path = credentials_path(credentials.as_deref());
                let credentials = read_credentials(&credentials_path)?.ok_or_else(|| {
                    anyhow!(
                        "missing daemon credentials at {}; run `forge-ctl daemon link` first",
                        credentials_path.display()
                    )
                })?;
                let labels = parse_labels(labels)?;
                let daemon =
                    report_once(client, &credentials, &workspace_root, &labels, None).await?;
                print_daemon(output, &daemon)
            }
        }
    }
}

struct LinkResult {
    credentials: DaemonCredentials,
    daemon: DaemonResponse,
}

struct DaemonLoopConfig<'a> {
    credentials_path: &'a Path,
    initial_credentials: &'a DaemonCredentials,
    workspace_root: &'a Path,
    labels: &'a BTreeMap<String, String>,
    interval_seconds: u64,
    stopped_message: &'a str,
    active_executions: &'a daemon_runtime::ActiveExecutionTracker,
}

async fn run_daemon_loop(
    client: &ForgeClient,
    output: &OutputFormat,
    config: DaemonLoopConfig<'_>,
) -> Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut daemon_client = DaemonClient::new(client.url("/"))?;
    daemon_client.set_credentials(
        config.initial_credentials.daemon_id.clone(),
        config.initial_credentials.token.clone(),
    );
    let active_executions = config.active_executions.clone();
    let connect_handle = tokio::spawn(daemon_runtime::run_command_stream(
        Arc::new(daemon_client),
        config.workspace_root.to_path_buf(),
        shutdown_rx,
        active_executions.clone(),
    ));
    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.context("listen for Ctrl-C")?;
                let _ = shutdown_tx.send(true);
                connect_handle.abort();
                match connect_handle.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => eprintln!("daemon command stream stopped: {error}"),
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => eprintln!("daemon command stream task failed: {error}"),
                }
                println!("{}", config.stopped_message);
                return Ok(());
            }
            () = tokio::time::sleep(Duration::from_secs(config.interval_seconds.max(1))) => {
                let credentials = read_credentials(config.credentials_path)?
                    .ok_or_else(|| anyhow!("missing daemon credentials at {}", config.credentials_path.display()))?;
                let daemon = report_once(
                    client,
                    &credentials,
                    config.workspace_root,
                    config.labels,
                    Some(&active_executions),
                )
                .await?;
                if matches!(output, OutputFormat::Json) {
                    print_json(&daemon)?;
                } else {
                    println!("reported daemon {}", daemon.id);
                }
            }
        }
    }
}

async fn link_and_report(
    client: &ForgeClient,
    credentials_path: &Path,
    workspace_root: &Path,
    machine_id: &str,
    hostname: &str,
    labels: &BTreeMap<String, String>,
    owner_token: Option<&str>,
) -> Result<LinkResult> {
    // A supplied user token should claim/rotate the daemon even if old daemon credentials exist.
    if owner_token.is_none() {
        if let Some(credentials) = read_credentials(credentials_path)? {
            match report_once(client, &credentials, workspace_root, labels, None).await {
                Ok(daemon) => {
                    return Ok(LinkResult {
                        credentials,
                        daemon,
                    });
                }
                Err(error) => {
                    eprintln!("could not use existing daemon credentials: {error}");
                }
            }
        }
    }

    let credentials = register(
        client,
        machine_id,
        hostname,
        workspace_root,
        labels,
        owner_token,
    )
    .await?;
    write_credentials(credentials_path, &credentials)?;
    let daemon = report_once(client, &credentials, workspace_root, labels, None).await?;
    Ok(LinkResult {
        credentials,
        daemon,
    })
}

async fn register(
    client: &ForgeClient,
    machine_id: &str,
    hostname: &str,
    workspace_root: &Path,
    labels: &BTreeMap<String, String>,
    owner_token: Option<&str>,
) -> Result<DaemonCredentials> {
    let request = DaemonRegisterRequest {
        machine_id: machine_id.to_owned(),
        hostname: hostname.to_owned(),
        os: std::env::consts::OS.to_owned(),
        arch: std::env::consts::ARCH.to_owned(),
        agent_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
        labels: Some(labels_value(labels)),
        runtimes: Some(vec![runtime_report(workspace_root)]),
    };
    let response: DaemonRegisterResponse = if let Some(token) = owner_token {
        client
            .post_bearer("/api/v1/daemons/register", token, &request)
            .await?
    } else {
        client.post("/api/v1/daemons/register", &request).await?
    };
    Ok(DaemonCredentials {
        daemon_id: response.daemon_id,
        token: response.registration_token,
    })
}

async fn report_once(
    client: &ForgeClient,
    credentials: &DaemonCredentials,
    workspace_root: &Path,
    labels: &BTreeMap<String, String>,
    active_executions: Option<&daemon_runtime::ActiveExecutionTracker>,
) -> Result<DaemonResponse> {
    let request = DaemonReportRequest {
        detected_clis: detect_clis().await,
        runtimes: Some(vec![runtime_report(workspace_root)]),
        labels: Some(labels_value(labels)),
        active_execution_ids: active_executions.map(|tracker| tracker.active_ids()),
    };
    client
        .post_bearer(
            &format!("/api/v1/daemons/{}/report", credentials.daemon_id),
            &credentials.token,
            &request,
        )
        .await
}

async fn detect_clis() -> Vec<DetectedCli> {
    let registry = cli_adapters::default_registry();
    let mut kinds = registry.kinds();
    kinds.sort_by_key(|kind| kind.to_string());

    let mut detected = Vec::with_capacity(kinds.len());
    for kind in kinds {
        let Some(adapter) = registry.get(&kind) else {
            continue;
        };
        let availability = adapter.check_availability();
        let (path, version) = cli_path_and_version(&kind).await;
        detected.push(DetectedCli {
            kind: kind.to_string(),
            availability: availability_status(&availability.status).to_owned(),
            config_path: availability.config_path,
            version,
            path,
        });
    }
    detected
}

fn availability_status(status: &AvailabilityStatus) -> &'static str {
    match status {
        AvailabilityStatus::Authenticated => "authenticated",
        AvailabilityStatus::Installed => "installed",
        AvailabilityStatus::NotFound => "not_found",
    }
}

async fn cli_path_and_version(kind: &ExecutorKind) -> (Option<String>, Option<String>) {
    match kind {
        ExecutorKind::Embedded => (None, None),
        ExecutorKind::Shell => shell_path_and_version(),
        ExecutorKind::Codex => binary_path_and_version("codex").await,
        ExecutorKind::ClaudeCode => binary_path_and_version("claude").await,
        ExecutorKind::Cursor => binary_path_and_version("cursor-agent").await,
        ExecutorKind::Opencode => binary_path_and_version("opencode").await,
        ExecutorKind::Gemini => binary_path_and_version("gemini").await,
        ExecutorKind::Smith => binary_path_and_version("smith").await,
        ExecutorKind::Null => (None, None),
    }
}

fn shell_path_and_version() -> (Option<String>, Option<String>) {
    if cfg!(windows) {
        (Some("cmd.exe".to_owned()), None)
    } else {
        (Some("/bin/sh".to_owned()), None)
    }
}

async fn binary_path_and_version(binary: &str) -> (Option<String>, Option<String>) {
    let path = which::which(binary)
        .ok()
        .map(|path| path.to_string_lossy().into_owned());
    let version = if path.is_some() {
        binary_version(binary).await
    } else {
        None
    };
    (path, version)
}

async fn binary_version(binary: &str) -> Option<String> {
    let mut command = TokioCommand::new(binary);
    command
        .arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let child = command.spawn().ok()?;
    let output = timeout(VERSION_TIMEOUT, child.wait_with_output())
        .await
        .ok()?
        .ok()?;
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
}

fn runtime_report(workspace_root: &Path) -> RuntimeReport {
    RuntimeReport {
        kind: "local".to_owned(),
        workspace_root: workspace_root.to_string_lossy().into_owned(),
        status: Some("ready".to_owned()),
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

fn labels_value(labels: &BTreeMap<String, String>) -> serde_json::Value {
    serde_json::to_value(labels).unwrap_or_else(|_| serde_json::json!({ "mode": "external" }))
}

fn credentials_path(explicit: Option<&Path>) -> PathBuf {
    explicit
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_forge_home().join(CREDENTIALS_FILE))
}

fn resolve_workspace_root(explicit: Option<&Path>) -> PathBuf {
    explicit
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_forge_home().join("workspaces"))
}

fn prepare_workspace_root(explicit: Option<&Path>) -> Result<PathBuf> {
    let workspace_root = resolve_workspace_root(explicit);
    let workspace_root = absolute_path(&workspace_root)
        .with_context(|| format!("resolve daemon workspace root {}", workspace_root.display()))?;
    fs::create_dir_all(&workspace_root)
        .with_context(|| format!("create daemon workspace root {}", workspace_root.display()))?;
    Ok(workspace_root)
}

fn absolute_path(path: &Path) -> std::io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn default_forge_home() -> PathBuf {
    if let Some(path) = std::env::var_os("FORGE_DAEMON_HOME").filter(|value| !value.is_empty()) {
        return PathBuf::from(path);
    }
    if let Some(path) = std::env::var_os("FORGE_DATA_DIR").filter(|value| !value.is_empty()) {
        return PathBuf::from(path);
    }
    let data_dir = config::default_data_dir();
    // Keep the client's USERPROFILE fallback when HOME is absent.
    if std::env::var_os("HOME").is_none() {
        if let Some(home) = std::env::var_os("USERPROFILE") {
            return PathBuf::from(home).join(data_dir);
        }
    }
    data_dir
}

fn read_credentials(path: &Path) -> Result<Option<DaemonCredentials>> {
    if !path.exists() {
        return Ok(None);
    }
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read daemon credentials from {}", path.display()))?;
    serde_json::from_str(&contents)
        .with_context(|| format!("parse daemon credentials at {}", path.display()))
        .map(Some)
}

fn write_credentials(path: &Path, credentials: &DaemonCredentials) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create daemon credentials directory {}", parent.display()))?;
    }
    let contents = serde_json::to_string_pretty(credentials)?;
    std::fs::write(path, contents)
        .with_context(|| format!("write daemon credentials to {}", path.display()))
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

fn local_hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(command_hostname)
        .unwrap_or_else(|| "unknown-host".to_owned())
}

fn command_hostname() -> Option<String> {
    let output = StdCommand::new("hostname").output().ok()?;
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

fn print_daemon(output: &OutputFormat, daemon: &DaemonResponse) -> Result<()> {
    match output {
        OutputFormat::Json => print_json(daemon),
        OutputFormat::Table => {
            print_table_daemons(std::slice::from_ref(daemon));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::prepare_workspace_root;

    #[test]
    fn prepare_workspace_root_creates_missing_directory() {
        let temp = tempfile::tempdir().expect("tempdir creates");
        let root = temp.path().join("missing").join("workspaces");

        let prepared = prepare_workspace_root(Some(&root)).expect("workspace root is prepared");

        assert_eq!(prepared, root);
        assert!(prepared.is_absolute());
        assert!(prepared.is_dir());
    }
}
