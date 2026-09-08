//! Crash-safe persistence for terminal notifications emitted by a daemon.
//!
//! A terminal result is the only daemon message whose loss can leave the
//! server with a permanently running execution and an unaccounted provider
//! call.  The daemon therefore writes the complete notification to a bounded
//! directory before putting it on the command socket.  Files remain in place
//! until the authenticated server sends `execution.terminal.ack`; reconnects
//! simply enumerate the same files and replay them.

use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use anyhow::{bail, Context, Result};
use api_types::{
    ExecutionTerminalAckParams, ExecutionTerminalAckResult, ExecutionTerminalNotification,
    TERMINAL_REPORT_CONFLICT,
};

/// Terminal records are deliberately kept in a Forge-managed child of the
/// advertised workspace root.  The limits are high enough for normal
/// operation but make an accidentally unbounded provider/error payload
/// impossible to turn into a disk-filling queue.
pub const TERMINAL_REPORT_DIRECTORY: &str = ".forge-daemon/terminal-reports";
pub const MAX_TERMINAL_REPORTS: usize = 1024;
pub const MAX_TERMINAL_REPORT_BYTES: u64 = 32 * 1024 * 1024;
pub const MAX_TERMINAL_REPORT_SIZE: usize = 1024 * 1024;
const MAX_TERMINAL_REPORT_ID_BYTES: usize = 512;

/// Durable terminal notification queue for one daemon workspace.
pub struct DaemonTerminalStore {
    directory: PathBuf,
    max_reports: usize,
    max_bytes: u64,
    temp_sequence: AtomicU64,
    write_lock: Mutex<()>,
}

impl DaemonTerminalStore {
    /// Create a store rooted below `workspace_root`.
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self::with_directory(workspace_root.into().join(TERMINAL_REPORT_DIRECTORY))
    }

    /// Construct a store with custom limits. This is useful for deterministic
    /// tests and keeps the production path on the bounded defaults above.
    pub fn with_limits(
        workspace_root: impl Into<PathBuf>,
        max_reports: usize,
        max_bytes: u64,
    ) -> Self {
        Self {
            directory: workspace_root.into().join(TERMINAL_REPORT_DIRECTORY),
            max_reports,
            max_bytes,
            temp_sequence: AtomicU64::new(1),
            write_lock: Mutex::new(()),
        }
    }

    /// Construct a store over an exact directory. The directory is still
    /// bounded; this is intentionally public so persistence tests can use a
    /// temporary directory without manufacturing a workspace tree.
    pub fn with_directory(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            max_reports: MAX_TERMINAL_REPORTS,
            max_bytes: MAX_TERMINAL_REPORT_BYTES,
            temp_sequence: AtomicU64::new(1),
            write_lock: Mutex::new(()),
        }
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Persist one terminal notification atomically.
    ///
    /// An exact duplicate is a successful no-op. Reusing a terminal report ID
    /// for a different payload returns a conflict and leaves the first durable
    /// record unchanged.
    pub fn retain(&self, notification: &ExecutionTerminalNotification) -> Result<()> {
        validate_notification_identity(notification)?;
        let payload = serde_json::to_vec(notification).context("serialize terminal report")?;
        if payload.len() > MAX_TERMINAL_REPORT_SIZE {
            bail!(
                "terminal report exceeds bounded size ({} > {})",
                payload.len(),
                MAX_TERMINAL_REPORT_SIZE
            );
        }

        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        fs::create_dir_all(&self.directory).with_context(|| {
            format!(
                "create terminal report directory {}",
                self.directory.display()
            )
        })?;
        let final_path = self.path_for_report(&notification.terminal_report_id)?;

        if final_path.exists() {
            let existing = fs::read(&final_path)
                .with_context(|| format!("read terminal report {}", final_path.display()))?;
            if existing == payload {
                return Ok(());
            }
            bail!("{TERMINAL_REPORT_CONFLICT}: terminal report ID was reused with a different payload");
        }

        let (report_count, byte_count) = self.current_usage()?;
        if report_count >= self.max_reports {
            bail!(
                "terminal report queue reached its record bound ({})",
                self.max_reports
            );
        }
        let next_bytes = byte_count
            .checked_add(payload.len() as u64)
            .ok_or_else(|| anyhow::anyhow!("terminal report byte bound overflow"))?;
        if next_bytes > self.max_bytes {
            bail!(
                "terminal report queue reached its byte bound ({} > {})",
                next_bytes,
                self.max_bytes
            );
        }

        let temp_path = self.temp_path(&notification.terminal_report_id);
        let mut temp = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .with_context(|| format!("create terminal report temp file {}", temp_path.display()))?;
        if let Err(error) = write_and_sync(&mut temp, &payload) {
            let _ = fs::remove_file(&temp_path);
            return Err(error).with_context(|| "write terminal report temp file");
        }
        drop(temp);

        // The write lock serializes retain/ack operations in this process, so
        // rename cannot overwrite a concurrently-created durable record.
        if let Err(error) = fs::rename(&temp_path, &final_path) {
            let _ = fs::remove_file(&temp_path);
            return Err(error).with_context(|| {
                format!(
                    "atomically install terminal report {}",
                    final_path.display()
                )
            });
        }
        sync_directory(&self.directory).context("sync terminal report directory")
    }

    /// Return all retained terminal reports in deterministic order for replay.
    pub fn pending(&self) -> Result<Vec<ExecutionTerminalNotification>> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !self.directory.exists() {
            return Ok(Vec::new());
        }

        let mut paths = fs::read_dir(&self.directory)
            .with_context(|| {
                format!(
                    "read terminal report directory {}",
                    self.directory.display()
                )
            })?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<io::Result<Vec<_>>>()?;
        paths.retain(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        });
        paths.sort();

        paths
            .into_iter()
            .map(|path| {
                let bytes = fs::read(&path)
                    .with_context(|| format!("read retained terminal report {}", path.display()))?;
                serde_json::from_slice(&bytes)
                    .with_context(|| format!("decode retained terminal report {}", path.display()))
            })
            .collect()
    }

    /// Delete a report only after the server has sent an authenticated ack.
    /// Missing files are an idempotent success: the first ack already removed
    /// the record and a retried ack must not resurrect it.
    pub fn acknowledge(
        &self,
        params: &ExecutionTerminalAckParams,
    ) -> Result<ExecutionTerminalAckResult> {
        if params.terminal_report_id.trim().is_empty() {
            bail!("terminal_report_id must not be empty");
        }
        if params.execution_id.trim().is_empty() {
            bail!("execution_id must not be empty");
        }

        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let final_path = self.path_for_report(&params.terminal_report_id)?;
        if !final_path.exists() {
            return Ok(ExecutionTerminalAckResult {
                terminal_report_id: params.terminal_report_id.clone(),
                execution_id: params.execution_id.clone(),
                acknowledged: false,
            });
        }

        let bytes = fs::read(&final_path)
            .with_context(|| format!("read terminal report {}", final_path.display()))?;
        let notification: ExecutionTerminalNotification = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode terminal report {}", final_path.display()))?;
        if notification.terminal_report_id != params.terminal_report_id
            || notification.execution_id != params.execution_id
        {
            bail!("{TERMINAL_REPORT_CONFLICT}: terminal acknowledgement identity does not match");
        }

        fs::remove_file(&final_path).with_context(|| {
            format!(
                "remove acknowledged terminal report {}",
                final_path.display()
            )
        })?;
        sync_directory(&self.directory).context("sync terminal report removal")?;
        Ok(ExecutionTerminalAckResult {
            terminal_report_id: params.terminal_report_id.clone(),
            execution_id: params.execution_id.clone(),
            acknowledged: true,
        })
    }

    fn path_for_report(&self, report_id: &str) -> Result<PathBuf> {
        validate_report_id(report_id)?;
        Ok(self
            .directory
            .join(format!("{}.json", encode_component(report_id))))
    }

    fn temp_path(&self, report_id: &str) -> PathBuf {
        let sequence = self.temp_sequence.fetch_add(1, Ordering::Relaxed);
        self.directory.join(format!(
            ".{}.json.tmp.{}.{}",
            encode_component(report_id),
            std::process::id(),
            sequence
        ))
    }

    fn current_usage(&self) -> Result<(usize, u64)> {
        if !self.directory.exists() {
            return Ok((0, 0));
        }
        let mut count = 0;
        let mut bytes = 0_u64;
        for entry in fs::read_dir(&self.directory).with_context(|| {
            format!(
                "read terminal report directory {}",
                self.directory.display()
            )
        })? {
            let entry = entry?;
            let path = entry.path();
            if !path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                continue;
            }
            count += 1;
            bytes = bytes
                .checked_add(entry.metadata()?.len())
                .ok_or_else(|| anyhow::anyhow!("terminal report byte bound overflow"))?;
        }
        Ok((count, bytes))
    }
}

fn validate_notification_identity(notification: &ExecutionTerminalNotification) -> Result<()> {
    validate_report_id(&notification.terminal_report_id)?;
    if notification.execution_id.trim().is_empty() {
        bail!("execution_id must not be empty");
    }
    let mut report_ids = HashSet::with_capacity(notification.usage_reports.len());
    for report in &notification.usage_reports {
        validate_report_id(&report.report_id)?;
        if !report_ids.insert(&report.report_id) {
            bail!("usage report IDs must be unique within a terminal report");
        }
        if report
            .request_id
            .as_deref()
            .is_some_and(|request_id| request_id.trim().is_empty())
        {
            bail!("request_id must be non-empty when present");
        }
    }
    Ok(())
}

fn validate_report_id(report_id: &str) -> Result<()> {
    if report_id.trim().is_empty() {
        bail!("terminal_report_id must not be empty");
    }
    if report_id.len() > MAX_TERMINAL_REPORT_ID_BYTES {
        bail!("terminal_report_id exceeds bounded length");
    }
    Ok(())
}

fn write_and_sync(file: &mut File, payload: &[u8]) -> Result<()> {
    file.write_all(payload)?;
    file.sync_all()?;
    Ok(())
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

/// Encode every non-portable/path-significant byte so a report ID can never
/// escape the bounded directory or collide through lossy sanitization.
fn encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(hex_digit(byte >> 4));
            encoded.push(hex_digit(byte & 0x0f));
        }
    }
    encoded
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + value - 10) as char,
        _ => unreachable!("hex digit is four bits"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use api_types::{RemoteUsageReport, UsageTelemetryState};

    fn notification(report_id: &str, execution_id: &str) -> ExecutionTerminalNotification {
        ExecutionTerminalNotification {
            terminal_report_id: report_id.to_owned(),
            execution_id: execution_id.to_owned(),
            exit_code: Some(0),
            signal: None,
            error: None,
            ts: "2026-09-07T00:00:00Z".to_owned(),
            status: Some("completed".to_owned()),
            agent_session_id: None,
            summary: None,
            after_sha: None,
            usage_reports: vec![RemoteUsageReport {
                report_id: "report-1".to_owned(),
                request_id: Some("request-1".to_owned()),
                report_sequence: 0,
                candidate_key: Some("candidate-1".to_owned()),
                attempt_ordinal: 0,
                provider_id: Some("openai".to_owned()),
                model_id: Some("gpt-5".to_owned()),
                input_tokens: Some(1),
                output_tokens: Some(2),
                cache_read_tokens: Some(0),
                cache_write_tokens: Some(0),
                telemetry_state: UsageTelemetryState::Metered,
                context_tokens: None,
                selected_tier: None,
                reported_cost_usd: Some("0.000001".to_owned()),
                partial: false,
            }],
            failure_class: None,
            retry_at: None,
            resolved_candidate: None,
            route_attempts: None,
        }
    }

    #[test]
    fn retain_replay_and_authenticated_ack_are_crash_safe() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DaemonTerminalStore::with_directory(dir.path());
        let report = notification("report/one", "execution-1");

        store.retain(&report).expect("retain report");
        let replay = store.pending().expect("read replay queue");
        assert_eq!(replay, vec![report.clone()]);

        let ack = store
            .acknowledge(&ExecutionTerminalAckParams {
                terminal_report_id: "report/one".to_owned(),
                execution_id: "execution-1".to_owned(),
            })
            .expect("ack report");
        assert!(ack.acknowledged);
        assert!(store.pending().expect("read empty queue").is_empty());
        assert!(
            !store
                .acknowledge(&ExecutionTerminalAckParams {
                    terminal_report_id: "report/one".to_owned(),
                    execution_id: "execution-1".to_owned(),
                })
                .expect("duplicate ack")
                .acknowledged
        );
    }

    #[test]
    fn exact_duplicate_is_noop_and_conflicting_replay_is_rejected() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DaemonTerminalStore::with_directory(dir.path());
        let report = notification("report-1", "execution-1");
        store.retain(&report).expect("retain report");
        store.retain(&report).expect("exact duplicate is a no-op");

        let mut conflicting = report.clone();
        conflicting.summary = Some("different".to_owned());
        let error = store.retain(&conflicting).expect_err("conflict rejected");
        assert!(error.to_string().contains(TERMINAL_REPORT_CONFLICT));
        assert_eq!(store.pending().expect("read report")[0], report);
    }

    #[test]
    fn queue_bounds_are_enforced_without_dropping_unacknowledged_reports() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DaemonTerminalStore::with_limits(dir.path(), 1, u64::MAX);
        store
            .retain(&notification("report-1", "execution-1"))
            .expect("first report");
        let error = store
            .retain(&notification("report-2", "execution-2"))
            .expect_err("record bound");
        assert!(error.to_string().contains("record bound"));
        assert_eq!(store.pending().expect("read report").len(), 1);
    }
}
