use std::{collections::BTreeMap, path::Path, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use api_types::{DaemonReportRequest, DaemonResponse, RuntimeReport};
use forge_client::daemon_link::{report_with_retry, DaemonClient};
use forge_client::daemon_runtime::ActiveExecutionTracker;
use tokio::{sync::watch, time};

use crate::detect;

pub async fn run(
    client: Arc<DaemonClient>,
    workspace_root: PathBuf,
    labels: BTreeMap<String, String>,
    max_concurrent_runs: u32,
    interval_seconds: u64,
    active_executions: ActiveExecutionTracker,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut ticker = time::interval(Duration::from_secs(interval_seconds.max(1)));

    loop {
        tokio::select! {
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    tracing::info!("daemon reporter stopped");
                    return Ok(());
                }
            }
            _ = ticker.tick() => {
                match report_once(&client, &workspace_root, &labels, max_concurrent_runs, &active_executions).await {
                    Ok(daemon) => {
                        tracing::info!(
                            daemon_id = %daemon.id,
                            workspace_root = %workspace_root.display(),
                            "daemon report submitted"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            workspace_root = %workspace_root.display(),
                            "daemon report failed"
                        );
                    }
                }
            }
        }
    }
}

pub async fn report_once(
    client: &DaemonClient,
    workspace_root: &Path,
    labels: &BTreeMap<String, String>,
    max_concurrent_runs: u32,
    active_executions: &ActiveExecutionTracker,
) -> Result<DaemonResponse> {
    let request = report_request(
        workspace_root,
        labels,
        max_concurrent_runs,
        active_executions,
    )
    .await;
    report_with_retry(client, &request).await
}

pub async fn report_request(
    workspace_root: &Path,
    labels: &BTreeMap<String, String>,
    max_concurrent_runs: u32,
    active_executions: &ActiveExecutionTracker,
) -> DaemonReportRequest {
    DaemonReportRequest {
        max_concurrent_runs: Some(max_concurrent_runs),
        detected_clis: detect::detect_clis().await,
        runtimes: Some(vec![runtime_report(workspace_root)]),
        labels: Some(labels_value(labels)),
        active_execution_ids: Some(active_executions.active_ids()),
    }
}

pub fn runtime_report(workspace_root: &Path) -> RuntimeReport {
    RuntimeReport {
        kind: "local".to_owned(),
        workspace_root: workspace_root.to_string_lossy().into_owned(),
        status: Some("ready".to_owned()),
    }
}

pub fn labels_value(labels: &BTreeMap<String, String>) -> serde_json::Value {
    serde_json::to_value(labels).unwrap_or_else(|_| serde_json::json!({ "mode": "external" }))
}
