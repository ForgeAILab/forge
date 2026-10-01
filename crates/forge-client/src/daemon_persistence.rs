//! One crash-safe journal for terminal reports, workspace results and cleanup.
//! Results are installed before emission. Operation receipts survive an ack so
//! resending an operation id cannot repeat its side effects.

use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use anyhow::{bail, Context, Result};
use api_types::{
    DaemonErrorPayload, ExecutionTerminalNotification, JournalAckParams, JournalAckResult,
    WorkspaceMutationFence, METHOD_EXECUTION_TERMINAL, METHOD_WORKSPACE_CLEANUP,
    METHOD_WORKSPACE_RUN, TERMINAL_REPORT_CONFLICT,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const JOURNAL_DIRECTORY: &str = ".forge/journal";
// Queue bounds apply to unacknowledged entries. Acknowledged operation
// receipts remain available for deduplication without starving new reports.
pub const MAX_JOURNAL_ENTRIES: usize = 1024;
pub const MAX_JOURNAL_BYTES: u64 = 32 * 1024 * 1024;
pub const MAX_TERMINAL_REPORT_SIZE: usize = 1024 * 1024;
// A run can retain one MiB per stream; escaped JSON may expand each byte six
// times. Terminal reports keep their separate one-MiB cap above.
const MAX_JOURNAL_ENTRY_SIZE: usize = 16 * 1024 * 1024;
const OLD_TERMINAL_DIRECTORY: &str = ".forge-daemon/terminal-reports";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JournalEntry {
    Terminal {
        report: ExecutionTerminalNotification,
    },
    Operation {
        operation: JournalOperation,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalOperation {
    pub entry_id: String,
    pub fence: WorkspaceMutationFence,
    pub workspace_handle: Option<String>,
    pub method: String,
    pub request: Value,
    // None is a durable intent. An interrupted shell command is never rerun.
    pub outcome: Option<std::result::Result<Value, DaemonErrorPayload>>,
    pub acknowledged: bool,
}

impl JournalEntry {
    fn has_unbounded_ci_output(&self) -> bool {
        matches!(self, Self::Operation { operation }
            if operation.method == METHOD_WORKSPACE_RUN
                && operation.request["purpose"].as_str() == Some("ci_step")
                && operation.request["max_output_bytes"].as_u64() == Some(u64::MAX))
    }

    fn is_pending(&self) -> bool {
        !matches!(self, Self::Operation { operation } if operation.acknowledged)
    }

    pub fn entry_id(&self) -> &str {
        match self {
            Self::Terminal { report } => &report.terminal_report_id,
            Self::Operation { operation } => &operation.entry_id,
        }
    }

    pub fn replay_notification(&self) -> Option<(&str, Value)> {
        match self {
            Self::Terminal { report } => Some((
                METHOD_EXECUTION_TERMINAL,
                serde_json::to_value(report).ok()?,
            )),
            Self::Operation { operation }
                if operation.method == METHOD_WORKSPACE_CLEANUP && !operation.acknowledged =>
            {
                match operation.outcome.as_ref()? {
                    Ok(result) => Some((METHOD_WORKSPACE_CLEANUP, result.clone())),
                    Err(_) => None,
                }
            }
            _ => None,
        }
    }
}

pub struct DaemonJournal {
    directory: PathBuf,
    workspace_root: Option<PathBuf>,
    max_entries: usize,
    max_bytes: u64,
    temp_sequence: AtomicU64,
    write_lock: Mutex<()>,
}

impl DaemonJournal {
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self::with_limits(workspace_root, MAX_JOURNAL_ENTRIES, MAX_JOURNAL_BYTES)
    }

    pub fn with_limits(
        workspace_root: impl Into<PathBuf>,
        max_entries: usize,
        max_bytes: u64,
    ) -> Self {
        let root = workspace_root.into();
        Self {
            directory: root.join(JOURNAL_DIRECTORY),
            workspace_root: Some(root),
            max_entries,
            max_bytes,
            temp_sequence: AtomicU64::new(1),
            write_lock: Mutex::new(()),
        }
    }

    pub fn with_directory(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            workspace_root: None,
            max_entries: MAX_JOURNAL_ENTRIES,
            max_bytes: MAX_JOURNAL_BYTES,
            temp_sequence: AtomicU64::new(1),
            write_lock: Mutex::new(()),
        }
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Convert the old queue on startup; delete each source only after the new
    /// entry and its directory have been synced. A partial conversion retries.
    pub fn initialize(&self) -> Result<()> {
        let Some(root) = &self.workspace_root else {
            return Ok(());
        };
        let old = root.join(OLD_TERMINAL_DIRECTORY);
        if !old.exists() {
            return Ok(());
        }
        let canonical_root = root.canonicalize()?;
        if !old.canonicalize()?.starts_with(&canonical_root) {
            bail!("old terminal queue escapes workspace root");
        }
        let mut paths = fs::read_dir(&old)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<io::Result<Vec<_>>>()?;
        paths.retain(|path| path.extension().is_some_and(|ext| ext == "json"));
        paths.sort();
        for path in paths {
            let report: ExecutionTerminalNotification = read_json(&path)?;
            self.retain(&report)?;
            fs::remove_file(&path)?;
            sync_directory(&old)?;
        }
        // Leave non-report files intact rather than deleting unknown data.
        if fs::read_dir(&old)?.next().is_none() {
            fs::remove_dir(&old)?;
        }
        Ok(())
    }

    pub fn retain(&self, report: &ExecutionTerminalNotification) -> Result<()> {
        validate_notification_identity(report)?;
        if serde_json::to_vec(report)?.len() > MAX_TERMINAL_REPORT_SIZE {
            bail!("terminal report exceeds bounded size");
        }
        self.retain_entry(&JournalEntry::Terminal {
            report: report.clone(),
        })
    }

    pub fn retain_entry(&self, entry: &JournalEntry) -> Result<()> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        let path = self.path_for_entry(entry.entry_id())?;
        if path.exists() {
            let existing = read_entry(&path)?;
            if serde_json::to_value(&existing)? == serde_json::to_value(entry)? {
                return Ok(());
            }
            bail!(
                "{TERMINAL_REPORT_CONFLICT}: journal entry ID was reused with a different payload"
            );
        }
        self.write_entry(entry)
    }

    /// Replace only a matching unfinished intent with its final result.
    pub fn finish_operation(&self, operation: &JournalOperation) -> Result<()> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        let existing = read_entry(&self.path_for_entry(&operation.entry_id)?)?;
        let JournalEntry::Operation { operation: intent } = existing else {
            bail!("{TERMINAL_REPORT_CONFLICT}: operation collides with a terminal report");
        };
        if intent.request != operation.request
            || intent.method != operation.method
            || intent.outcome.is_some()
        {
            bail!("{TERMINAL_REPORT_CONFLICT}: operation intent does not match");
        }
        self.write_entry(&JournalEntry::Operation {
            operation: operation.clone(),
        })
    }

    pub fn operation(&self, operation_id: &str) -> Result<Option<JournalOperation>> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        let path = self.path_for_entry(&operation_entry_id(operation_id))?;
        if !path.exists() {
            return Ok(None);
        }
        match read_entry(&path)? {
            JournalEntry::Operation { operation } => {
                if operation.fence.operation_id != operation_id
                    || operation.entry_id != operation_entry_id(operation_id)
                {
                    bail!("{TERMINAL_REPORT_CONFLICT}: operation identity does not match");
                }
                Ok(Some(operation))
            }
            _ => bail!("{TERMINAL_REPORT_CONFLICT}: operation identity collision"),
        }
    }

    /// One deterministic scan for every replayable result.
    pub fn pending(&self) -> Result<Vec<JournalEntry>> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        self.ensure_confined()?;
        if !self.directory.exists() {
            return Ok(Vec::new());
        }
        let mut paths = fs::read_dir(&self.directory)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<io::Result<Vec<_>>>()?;
        paths.retain(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("entry-"))
                && path.extension().is_some_and(|ext| ext == "json")
        });
        paths.sort();
        let mut entries = Vec::new();
        for path in paths {
            let entry = read_entry(&path)?;
            if entry.is_pending() {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    pub fn acknowledge(&self, params: &JournalAckParams) -> Result<JournalAckResult> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        let path = self.path_for_entry(&params.entry_id)?;
        if !path.exists() {
            return Ok(JournalAckResult {
                entry_id: params.entry_id.clone(),
                acknowledged: false,
            });
        }
        let mut entry = read_entry(&path)?;
        if entry.entry_id() != params.entry_id {
            bail!("{TERMINAL_REPORT_CONFLICT}: journal acknowledgement identity does not match");
        }
        match &mut entry {
            JournalEntry::Terminal { .. } => {
                fs::remove_file(&path)?;
                sync_directory(&self.directory)?;
            }
            JournalEntry::Operation { operation } => {
                if operation.outcome.is_none() {
                    bail!("cannot acknowledge an unfinished operation");
                }
                operation.acknowledged = true;
                self.write_entry(&entry)?;
            }
        }
        Ok(JournalAckResult {
            entry_id: params.entry_id.clone(),
            acknowledged: true,
        })
    }

    pub(crate) fn load_workspace_state<T: DeserializeOwned + Default>(&self) -> Result<T> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        self.ensure_confined()?;
        let path = self.directory.join("workspace-state.json");
        if path.exists() {
            read_json(&path)
        } else {
            Ok(T::default())
        }
    }

    pub(crate) fn save_workspace_state<T: Serialize>(&self, state: &T) -> Result<()> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        let payload = serde_json::to_vec(state)?;
        if payload.len() as u64 > MAX_JOURNAL_BYTES {
            bail!("workspace registry exceeds bounded size");
        }
        self.atomic_write(&self.directory.join("workspace-state.json"), &payload)
    }

    fn path_for_entry(&self, entry_id: &str) -> Result<PathBuf> {
        validate_id(entry_id)?;
        self.ensure_confined()?;
        Ok(self
            .directory
            .join(format!("entry-{}.json", encode_component(entry_id))))
    }

    fn ensure_confined(&self) -> Result<()> {
        if let Some(root) = &self.workspace_root {
            let canonical_root = root.canonicalize()?;
            let mut ancestor = self.directory.as_path();
            while !ancestor.exists() {
                if fs::symlink_metadata(ancestor).is_ok() {
                    bail!("journal contains a dangling symlink");
                }
                ancestor = ancestor
                    .parent()
                    .context("journal has no existing parent")?;
            }
            if !ancestor.canonicalize()?.starts_with(canonical_root) {
                bail!("journal escapes workspace root");
            }
        }
        Ok(())
    }

    fn write_entry(&self, entry: &JournalEntry) -> Result<()> {
        let payload = serde_json::to_vec(entry)?;
        if !entry.has_unbounded_ci_output() && payload.len() > MAX_JOURNAL_ENTRY_SIZE {
            bail!("journal entry exceeds bounded size");
        }
        let path = self.path_for_entry(entry.entry_id())?;
        let old_entry = path.exists().then(|| read_entry(&path)).transpose()?;
        let old_pending = old_entry.as_ref().is_some_and(JournalEntry::is_pending);
        let old_bytes = if old_pending
            && !old_entry
                .as_ref()
                .is_some_and(JournalEntry::has_unbounded_ci_output)
        {
            fs::metadata(&path)?.len()
        } else {
            0
        };
        let (count, bytes) = self.current_usage()?;
        let retained_count =
            count.saturating_sub(usize::from(old_pending)) + usize::from(entry.is_pending());
        if retained_count > self.max_entries {
            bail!("journal reached its record bound ({})", self.max_entries);
        }
        if bytes.saturating_sub(old_bytes).saturating_add(
            if entry.is_pending() && !entry.has_unbounded_ci_output() {
                payload.len() as u64
            } else {
                0
            },
        ) > self.max_bytes
        {
            bail!("journal reached its byte bound ({})", self.max_bytes);
        }
        self.atomic_write(&path, &payload)
    }

    fn atomic_write(&self, path: &Path, payload: &[u8]) -> Result<()> {
        self.ensure_confined()?;
        fs::create_dir_all(&self.directory)?;
        self.ensure_confined()?;
        let sequence = self.temp_sequence.fetch_add(1, Ordering::Relaxed);
        let temp_path =
            self.directory
                .join(format!(".journal.tmp.{}.{}", std::process::id(), sequence));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut temp = options.open(&temp_path)?;
        let result = (|| -> Result<()> {
            temp.write_all(payload)?;
            temp.sync_all()?;
            fs::rename(&temp_path, path)?;
            sync_directory(&self.directory)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        result
    }

    fn current_usage(&self) -> Result<(usize, u64)> {
        if !self.directory.exists() {
            return Ok((0, 0));
        }
        let mut count = 0;
        let mut bytes = 0_u64;
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().starts_with("entry-")
                && entry.path().extension().is_some_and(|ext| ext == "json")
            {
                let record = read_entry(&entry.path())?;
                if !record.is_pending() {
                    continue;
                }
                count += 1;
                if !record.has_unbounded_ci_output() {
                    bytes = bytes.saturating_add(entry.metadata()?.len());
                }
            }
        }
        Ok((count, bytes))
    }
}

pub fn operation_entry_id(operation_id: &str) -> String {
    format!("forge:operation:{operation_id}")
}

fn read_entry(path: &Path) -> Result<JournalEntry> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() {
        bail!("invalid journal file {}", path.display());
    }
    // Unbounded CI is also unbounded in its durable receipt. The other entry
    // kinds retain their queue and per-entry limits.
    let entry: JournalEntry = serde_json::from_reader(File::open(path)?)
        .with_context(|| format!("decode {}", path.display()))?;
    if !entry.has_unbounded_ci_output() && metadata.len() > MAX_JOURNAL_ENTRY_SIZE as u64 {
        bail!("oversized journal file {}", path.display());
    }
    Ok(entry)
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > MAX_JOURNAL_BYTES {
        bail!("invalid or oversized journal file {}", path.display());
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_JOURNAL_BYTES + 1)
        .read_to_end(&mut bytes)?;
    serde_json::from_slice(&bytes).with_context(|| format!("decode {}", path.display()))
}

fn validate_notification_identity(report: &ExecutionTerminalNotification) -> Result<()> {
    validate_id(&report.terminal_report_id)?;
    if report.execution_id.trim().is_empty() {
        bail!("execution_id must not be empty");
    }
    let mut ids = HashSet::new();
    for usage in &report.usage_reports {
        validate_id(&usage.report_id)?;
        if !ids.insert(&usage.report_id) {
            bail!("usage report IDs must be unique within a terminal report");
        }
        if usage
            .request_id
            .as_deref()
            .is_some_and(|id| id.trim().is_empty())
        {
            bail!("request_id must be non-empty when present");
        }
    }
    Ok(())
}

fn validate_id(id: &str) -> Result<()> {
    if id.trim().is_empty() || id.len() > 512 {
        bail!("journal entry_id must be non-empty and at most 512 bytes");
    }
    Ok(())
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

fn encode_component(value: &str) -> String {
    // Fixed-width names fit filesystem component limits; the entry's full
    // identity is checked on every read and acknowledgement.
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notification(id: &str) -> ExecutionTerminalNotification {
        serde_json::from_value(serde_json::json!({
            "terminal_report_id": id, "execution_id": "execution-1", "exit_code": 0,
            "signal": null, "error": null, "ts": "2026-09-29T00:00:00Z", "usage_reports": []
        }))
        .unwrap()
    }

    #[test]
    fn retain_replay_and_authenticated_ack_are_crash_safe() {
        let dir = tempfile::tempdir().unwrap();
        let store = DaemonJournal::new(dir.path());
        let report = notification("report/one");
        store.retain(&report).unwrap();
        store.retain(&report).unwrap();
        let restarted = DaemonJournal::new(dir.path());
        assert_eq!(
            restarted.pending().unwrap()[0]
                .replay_notification()
                .unwrap()
                .1,
            serde_json::to_value(report).unwrap()
        );
        assert!(
            restarted
                .acknowledge(&JournalAckParams {
                    entry_id: "report/one".into()
                })
                .unwrap()
                .acknowledged
        );
        assert!(restarted.pending().unwrap().is_empty());
        assert!(
            !restarted
                .acknowledge(&JournalAckParams {
                    entry_id: "report/one".into()
                })
                .unwrap()
                .acknowledged
        );
    }

    #[test]
    fn exact_duplicate_is_noop_and_conflicting_replay_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = DaemonJournal::new(dir.path());
        let report = notification("report-1");
        store.retain(&report).unwrap();
        let mut other = report.clone();
        other.summary = Some("different".into());
        assert!(store
            .retain(&other)
            .unwrap_err()
            .to_string()
            .contains(TERMINAL_REPORT_CONFLICT));
        assert_eq!(
            store.pending().unwrap()[0].replay_notification().unwrap().1,
            serde_json::to_value(report).unwrap()
        );
    }

    #[test]
    fn queue_bounds_preserve_unacknowledged_reports() {
        let dir = tempfile::tempdir().unwrap();
        let store = DaemonJournal::with_limits(dir.path(), 1, u64::MAX);
        store.retain(&notification("one")).unwrap();
        assert!(store
            .retain(&notification("two"))
            .unwrap_err()
            .to_string()
            .contains("record bound"));
        assert_eq!(store.pending().unwrap().len(), 1);
    }

    #[test]
    fn acknowledged_operation_receipts_do_not_consume_the_pending_queue_bound() {
        let dir = tempfile::tempdir().unwrap();
        let store = DaemonJournal::with_limits(dir.path(), 1, 2048);
        let operation = JournalOperation {
            entry_id: operation_entry_id("run-once"),
            fence: WorkspaceMutationFence {
                daemon_id: "daemon-1".into(),
                runtime_id: "runtime-1".into(),
                placement_id: "placement-1".into(),
                operation_id: "run-once".into(),
                generation: 1,
                expected: api_types::WorkspaceOperationExpected::Version { version: 1 },
            },
            workspace_handle: Some("handle".into()),
            method: api_types::METHOD_WORKSPACE_RUN.into(),
            request: serde_json::json!({"command":"printf once"}),
            outcome: Some(Ok(serde_json::json!({"stdout":"once"}))),
            acknowledged: false,
        };
        store
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
        store
            .acknowledge(&JournalAckParams {
                entry_id: operation.entry_id,
            })
            .unwrap();
        store.retain(&notification("terminal-after-run")).unwrap();
        assert_eq!(store.pending().unwrap().len(), 1);
        let receipt = store.operation("run-once").unwrap().unwrap();
        assert!(receipt.acknowledged);
        assert_eq!(
            receipt.outcome.unwrap().unwrap(),
            operation.outcome.unwrap().unwrap()
        );
    }

    #[test]
    fn unbounded_ci_receipt_retains_full_output_beyond_entry_and_queue_byte_limits() {
        let dir = tempfile::tempdir().unwrap();
        let store = DaemonJournal::with_limits(dir.path(), 2, 4096);
        let mut operation = JournalOperation {
            entry_id: operation_entry_id("full-ci-result"),
            fence: WorkspaceMutationFence {
                daemon_id: "daemon-1".into(),
                runtime_id: "runtime-1".into(),
                placement_id: "placement-1".into(),
                operation_id: "full-ci-result".into(),
                generation: 1,
                expected: api_types::WorkspaceOperationExpected::Version { version: 1 },
            },
            workspace_handle: Some("handle".into()),
            method: METHOD_WORKSPACE_RUN.into(),
            request: serde_json::json!({"purpose": "ci_step", "timeout_secs": 0, "max_output_bytes": u64::MAX}),
            outcome: None,
            acknowledged: false,
        };
        store
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
        let stdout = "x".repeat(MAX_JOURNAL_ENTRY_SIZE + 1);
        operation.outcome = Some(Ok(serde_json::json!({"stdout": stdout})));
        store.finish_operation(&operation).unwrap();
        // Full CI results must not prevent a bounded terminal report being retained.
        store
            .retain(&notification("terminal-after-full-ci"))
            .unwrap();
        let restarted = DaemonJournal::with_limits(dir.path(), 2, 4096);
        assert_eq!(restarted.pending().unwrap().len(), 2);
        restarted
            .acknowledge(&JournalAckParams {
                entry_id: operation.entry_id,
            })
            .unwrap();
        let receipt = restarted.operation("full-ci-result").unwrap().unwrap();
        assert!(receipt.acknowledged);
        assert_eq!(
            receipt.outcome.unwrap().unwrap()["stdout"]
                .as_str()
                .unwrap(),
            stdout
        );
        assert_eq!(restarted.pending().unwrap().len(), 1);
    }

    #[test]
    fn old_terminal_files_are_converted_without_data_loss() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join(OLD_TERMINAL_DIRECTORY);
        fs::create_dir_all(&old).unwrap();
        let report = notification("old-report");
        fs::write(old.join("old.json"), serde_json::to_vec(&report).unwrap()).unwrap();
        let store = DaemonJournal::new(dir.path());
        store.initialize().unwrap();
        store.initialize().unwrap();
        assert!(!old.exists());
        assert_eq!(
            store.pending().unwrap()[0].replay_notification().unwrap().1,
            serde_json::to_value(report).unwrap()
        );
    }
}
