//! One crash-safe journal for terminal reports, workspace results and cleanup.
//! Results are installed before emission and retained until the server durably
//! records them and acknowledges that replay is no longer needed.

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Write},
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
// Bounds include every receipt and the workspace registry.
pub const MAX_JOURNAL_ENTRIES: usize = 1024;
pub const MAX_JOURNAL_BYTES: u64 = 32 * 1024 * 1024;
pub const MAX_TERMINAL_REPORT_SIZE: usize = 1024 * 1024;
pub const MAX_CI_LOG_BYTES: usize = 1024 * 1024;
pub const CI_LOG_TRUNCATION_MARKER: &str = "\n[Forge: CI log truncated]\n";
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
    Check {
        operation: JournalCheckOperation,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalCheckOperation {
    pub entry_id: String,
    pub operation_id: String,
    pub request: Value,
    pub receipt: Option<api_types::CheckReceipt>,
    /// The server recorded the receipt. The key is kept until the request's
    /// deadline has passed, so a late duplicate still starts nothing.
    #[serde(default)]
    pub acknowledged: bool,
}

/// A check key is kept this long past its deadline when the server never
/// acknowledged it (or the owner restarted mid-run and nobody asked).
pub const CHECK_RETENTION: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);
const CHECK_PRUNE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

impl JournalCheckOperation {
    /// The request's absolute wall deadline. After it a duplicate request
    /// settles as timed out before any command starts, so the key is no
    /// longer needed to prevent a second process tree.
    fn deadline(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::parse_from_rfc3339(self.request.get("deadline")?.as_str()?)
            .ok()
            .map(|deadline| deadline.with_timezone(&chrono::Utc))
    }
    fn expired(&self, now: chrono::DateTime<chrono::Utc>) -> bool {
        let Some(deadline) = self.deadline() else {
            return true;
        };
        if self.acknowledged && self.receipt.is_some() {
            return now > deadline;
        }
        chrono::Duration::from_std(CHECK_RETENTION)
            .ok()
            .and_then(|retention| deadline.checked_add_signed(retention))
            .is_none_or(|until| now > until)
    }
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
    /// Old unfinished journal entries are conservatively considered started.
    #[serde(default = "effect_may_have_started")]
    pub effect_started: bool,
}
fn effect_may_have_started() -> bool {
    true
}

impl JournalEntry {
    pub fn entry_id(&self) -> &str {
        match self {
            Self::Terminal { report } => &report.terminal_report_id,
            Self::Operation { operation } => &operation.entry_id,
            Self::Check { operation } => &operation.entry_id,
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

#[derive(Default)]
struct JournalUsage {
    files: HashMap<PathBuf, u64>,
    bytes: u64,
    /// Entries that count against the record bound: everything the server
    /// has not acknowledged. See `retained`.
    entries: usize,
    reservations: HashMap<PathBuf, u64>,
    skipped: HashSet<PathBuf>,
    /// Acknowledged queue-attempt receipts. They are history kept for a
    /// duplicate of their key, not work in flight, so they hold no slot of
    /// the record bound: a journal full of them can never refuse (or make
    /// anyone evict) an intent or a receipt the server has not stored yet.
    /// Their bytes still count; the owner prunes them by fence, age and
    /// number (`prune_acknowledged_attempts`).
    retained: HashSet<PathBuf>,
}

pub struct DaemonJournal {
    directory: PathBuf,
    workspace_root: Option<PathBuf>,
    max_entries: usize,
    max_bytes: u64,
    temp_sequence: AtomicU64,
    write_lock: Mutex<()>,
    usage: Mutex<Option<JournalUsage>>,
    last_check_prune: Mutex<Option<std::time::Instant>>,
    #[cfg(test)]
    fail_next_directory_sync: std::sync::atomic::AtomicBool,
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
            usage: Mutex::new(None),
            last_check_prune: Mutex::new(None),
            #[cfg(test)]
            fail_next_directory_sync: std::sync::atomic::AtomicBool::new(false),
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
            usage: Mutex::new(None),
            last_check_prune: Mutex::new(None),
            #[cfg(test)]
            fail_next_directory_sync: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Convert the old queue on startup; delete each source only after the new
    /// entry and its directory have been synced. A partial conversion retries.
    pub fn initialize(&self) -> Result<()> {
        self.upgrade_integration_journal()?;
        {
            let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
            self.current_usage()?;
        }
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

    /// Forward migration of on-disk revision-3 intents. Wire decoding stays
    /// strict; only startup rewrites retained files before deserializing them.
    fn upgrade_integration_journal(&self) -> Result<()> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        self.ensure_confined()?;
        if !self.directory.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(&self.directory)? {
            let path = entry?.path();
            if !is_entry_path(&path) || !path.is_file() {
                continue;
            }
            let Ok(mut value) = read_json::<Value>(&path) else {
                continue;
            };
            if value["kind"] != "operation"
                || value.pointer("/operation/fence/integration").is_some()
                || !value
                    .pointer("/operation/fence")
                    .is_some_and(Value::is_object)
                || !value
                    .pointer("/operation/request")
                    .is_some_and(Value::is_object)
            {
                continue;
            }
            value["operation"]["fence"]["integration"] = serde_json::json!({"kind":"task_step"});
            if value["operation"]["request"].get("operation_id").is_some() {
                value["operation"]["request"]["integration"] =
                    serde_json::json!({"kind":"task_step"});
            }
            value["operation"]["effect_started"] = Value::Bool(true);
            let migrated: JournalEntry = serde_json::from_value(value)?;
            self.atomic_write(&path, &serde_json::to_vec(&migrated)?)?;
        }
        *self.usage.lock().unwrap_or_else(|p| p.into_inner()) = None;
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
        let entry = sanitized_entry(entry);
        let path = self.path_for_entry(entry.entry_id())?;
        self.current_usage()?;
        if path.exists() && !self.is_skipped(&path) {
            let existing = read_entry(&path)?;
            if serde_json::to_value(&existing)? == serde_json::to_value(&entry)? {
                return Ok(());
            }
            bail!(
                "{TERMINAL_REPORT_CONFLICT}: journal entry ID was reused with a different payload"
            );
        }
        self.write_entry(&entry).map(|_| ())
    }

    /// Replace only a matching unfinished intent with its final result.
    pub fn finish_operation(&self, operation: &JournalOperation) -> Result<JournalOperation> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        let JournalEntry::Operation { operation } = sanitized_entry(&JournalEntry::Operation {
            operation: operation.clone(),
        }) else {
            unreachable!()
        };
        let path = self.path_for_entry(&operation.entry_id)?;
        self.current_usage()?;
        if !path.exists() || self.is_skipped(&path) {
            bail!("operation intent is missing");
        }
        let existing = read_entry(&path)?;
        let JournalEntry::Operation { operation: intent } = existing else {
            bail!("{TERMINAL_REPORT_CONFLICT}: operation collides with a terminal report");
        };
        if intent.request != operation.request
            || intent.method != operation.method
            || intent.outcome.is_some()
        {
            bail!("{TERMINAL_REPORT_CONFLICT}: operation intent does not match");
        }
        let JournalEntry::Operation { operation } =
            self.write_entry(&JournalEntry::Operation { operation })?
        else {
            unreachable!()
        };
        Ok(operation)
    }

    pub fn start_operation(&self, operation_id: &str) -> Result<()> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        let path = self.path_for_entry(&operation_entry_id(operation_id))?;
        let JournalEntry::Operation { mut operation } = read_entry(&path)? else {
            bail!("operation intent is missing");
        };
        if operation.outcome.is_some() {
            bail!("operation already settled");
        }
        operation.effect_started = true;
        self.write_entry(&JournalEntry::Operation { operation })
            .map(|_| ())
    }

    pub fn check_operation(&self, operation_id: &str) -> Result<Option<JournalCheckOperation>> {
        match self.entry(&operation_entry_id(operation_id))? {
            Some(JournalEntry::Check { operation }) if operation.operation_id == operation_id => {
                Ok(Some(operation))
            }
            None => Ok(None),
            _ => bail!("{TERMINAL_REPORT_CONFLICT}: check operation identity collision"),
        }
    }

    pub fn finish_check(&self, operation: &JournalCheckOperation) -> Result<JournalCheckOperation> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        let path = self.path_for_entry(&operation.entry_id)?;
        self.current_usage()?;
        let JournalEntry::Check { operation: intent } = read_entry(&path)? else {
            bail!("check intent missing")
        };
        let JournalEntry::Check { operation } = sanitized_entry(&JournalEntry::Check {
            operation: operation.clone(),
        }) else {
            unreachable!()
        };
        if intent.operation_id != operation.operation_id
            || intent.request != operation.request
            || intent.receipt.is_some()
        {
            bail!("{TERMINAL_REPORT_CONFLICT}: check intent mismatch");
        }
        let JournalEntry::Check { operation } =
            self.write_entry(&JournalEntry::Check { operation })?
        else {
            unreachable!()
        };
        Ok(operation)
    }

    /// Delete check keys that can no longer prevent a second process tree:
    /// acknowledged receipts past their deadline, and anything
    /// [`CHECK_RETENTION`] past it. Returns how many were removed.
    pub fn prune_checks(&self, now: chrono::DateTime<chrono::Utc>) -> Result<usize> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        self.ensure_confined()?;
        self.current_usage()?;
        if !self.directory.exists() {
            return Ok(0);
        }
        let mut removed = 0;
        for entry in fs::read_dir(&self.directory)? {
            let path = entry?.path();
            if !is_entry_path(&path) || self.is_skipped(&path) {
                continue;
            }
            let Ok(JournalEntry::Check { operation }) = read_entry(&path) else {
                continue;
            };
            if !operation.expired(now) {
                continue;
            }
            fs::remove_file(&path)?;
            self.update_usage(&path, None, 0);
            removed += 1;
        }
        if removed > 0 {
            self.sync_directory()?;
        }
        Ok(removed)
    }

    /// Delete acknowledged queue-attempt receipts the owner says can no
    /// longer be asked for. Returns the removed operation ids.
    ///
    /// `prunable` also receives how long ago the entry was last written. An
    /// acknowledgement rewrites the entry, so for these that is the time
    /// since the server acknowledged it.
    pub fn prune_acknowledged_attempts(
        &self,
        mut prunable: impl FnMut(&JournalOperation, std::time::Duration) -> bool,
    ) -> Result<Vec<String>> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        self.ensure_confined()?;
        self.current_usage()?;
        if !self.directory.exists() {
            return Ok(Vec::new());
        }
        let mut removed = Vec::new();
        for entry in fs::read_dir(&self.directory)? {
            let path = entry?.path();
            if !is_entry_path(&path) || self.is_skipped(&path) {
                continue;
            }
            let Ok(JournalEntry::Operation { operation }) = read_entry(&path) else {
                continue;
            };
            if !operation.acknowledged || !is_queue_attempt(&operation) {
                continue;
            }
            let age = fs::metadata(&path)
                .and_then(|metadata| metadata.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .unwrap_or_default();
            if !prunable(&operation, age) {
                continue;
            }
            fs::remove_file(&path)?;
            self.update_usage(&path, None, 0);
            removed.push(operation.fence.operation_id);
        }
        if !removed.is_empty() {
            self.sync_directory()?;
        }
        Ok(removed)
    }

    /// How many acknowledged queue-attempt receipts are retained, and how
    /// long ago each was acknowledged (newest first).
    pub fn acknowledged_attempt_ages(&self) -> Result<Vec<std::time::Duration>> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        self.current_usage()?;
        let retained: Vec<PathBuf> = self
            .usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|usage| usage.retained.iter().cloned().collect())
            .unwrap_or_default();
        let mut ages: Vec<std::time::Duration> = retained
            .iter()
            .map(|path| {
                fs::metadata(path)
                    .and_then(|metadata| metadata.modified())
                    .ok()
                    .and_then(|modified| modified.elapsed().ok())
                    .unwrap_or_default()
            })
            .collect();
        ages.sort();
        Ok(ages)
    }

    /// [`Self::prune_checks`] at most once a minute: admission calls this
    /// before it looks a key up, so retention needs no timer of its own.
    pub fn prune_checks_when_due(&self) -> Result<usize> {
        {
            let mut last = self
                .last_check_prune
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if last.is_some_and(|at| at.elapsed() < CHECK_PRUNE_INTERVAL) {
                return Ok(0);
            }
            *last = Some(std::time::Instant::now());
        }
        self.prune_checks(chrono::Utc::now())
    }

    pub fn operation(&self, operation_id: &str) -> Result<Option<JournalOperation>> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        let path = self.path_for_entry(&operation_entry_id(operation_id))?;
        self.current_usage()?;
        if !path.exists() || self.is_skipped(&path) {
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
        self.current_usage()?;
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
            if self.is_skipped(&path) {
                continue;
            }
            let entry = read_entry(&path)?;
            if !matches!(&entry, JournalEntry::Operation { operation } if operation.acknowledged)
                && !matches!(&entry, JournalEntry::Check { operation } if operation.acknowledged)
            {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    pub fn acknowledge(&self, params: &JournalAckParams) -> Result<JournalAckResult> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        let path = self.path_for_entry(&params.entry_id)?;
        self.current_usage()?;
        if !path.exists() || self.is_skipped(&path) {
            return Ok(JournalAckResult {
                entry_id: params.entry_id.clone(),
                // A repeated ack after response loss must also settle the
                // server's durable acknowledgement receipt.
                acknowledged: true,
            });
        }
        let entry = read_entry(&path)?;
        if entry.entry_id() != params.entry_id {
            bail!("{TERMINAL_REPORT_CONFLICT}: journal acknowledgement identity does not match");
        }
        if matches!(&entry, JournalEntry::Operation { operation } if operation.outcome.is_none()) {
            bail!("cannot acknowledge an unfinished operation");
        }
        if matches!(&entry, JournalEntry::Check { operation } if operation.receipt.is_none()) {
            bail!("cannot acknowledge an unfinished check");
        }
        // A check key outlives its acknowledgement only until the request's
        // deadline: before it a duplicate must find the receipt, after it a
        // duplicate can no longer start a command.
        if let JournalEntry::Check { mut operation } = entry {
            if !operation.acknowledged {
                operation.acknowledged = true;
                if !operation.expired(chrono::Utc::now()) {
                    self.write_entry(&JournalEntry::Check { operation })?;
                    return Ok(JournalAckResult {
                        entry_id: params.entry_id.clone(),
                        acknowledged: true,
                    });
                }
            } else if !operation.expired(chrono::Utc::now()) {
                return Ok(JournalAckResult {
                    entry_id: params.entry_id.clone(),
                    acknowledged: true,
                });
            }
            fs::remove_file(&path)?;
            self.update_usage(&path, None, 0);
            self.sync_directory()?;
            return Ok(JournalAckResult {
                entry_id: params.entry_id.clone(),
                acknowledged: true,
            });
        }
        // An acknowledged attempt receipt stays readable: a repeated effect
        // key of the same claim generation still returns its stored result.
        // The owner prunes it once a newer fence makes that key unreachable
        // ([`Self::prune_acknowledged_attempts`]).
        if let JournalEntry::Operation { operation } = &entry {
            if is_queue_attempt(operation) {
                if !operation.acknowledged {
                    let mut operation = operation.clone();
                    operation.acknowledged = true;
                    self.write_entry(&JournalEntry::Operation { operation })?;
                }
                self.mark_retained(&path);
                return Ok(JournalAckResult {
                    entry_id: params.entry_id.clone(),
                    acknowledged: true,
                });
            }
        }
        self.current_usage()?;
        fs::remove_file(&path)?;
        self.update_usage(&path, None, 0);
        self.sync_directory()?;
        Ok(JournalAckResult {
            entry_id: params.entry_id.clone(),
            acknowledged: true,
        })
    }

    pub(crate) fn entry(&self, entry_id: &str) -> Result<Option<JournalEntry>> {
        let _guard = self.write_lock.lock().unwrap_or_else(|p| p.into_inner());
        let path = self.path_for_entry(entry_id)?;
        self.current_usage()?;
        if !path.exists() || self.is_skipped(&path) {
            return Ok(None);
        }
        let entry = read_entry(&path)?;
        if entry.entry_id() != entry_id {
            bail!("{TERMINAL_REPORT_CONFLICT}: journal entry identity does not match");
        }
        Ok(Some(entry))
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
        let path = self.directory.join("workspace-state.json");
        self.check_capacity(&path, payload.len() as u64)?;
        if let Err(error) = self.atomic_write(&path, &payload) {
            *self.usage.lock().unwrap_or_else(|p| p.into_inner()) = None;
            return Err(error);
        }
        self.update_usage(&path, Some(payload.len() as u64), 0);
        Ok(())
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

    fn write_entry(&self, entry: &JournalEntry) -> Result<JournalEntry> {
        let path = self.path_for_entry(entry.entry_id())?;
        let (_, bytes) = self.current_usage()?;
        let old_bytes = self
            .usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|usage| usage.files.get(&path))
            .copied()
            .unwrap_or(0);
        let reserved = self.reserved_bytes_except(&path);
        let available = self
            .max_bytes
            .saturating_sub(bytes.saturating_sub(old_bytes).saturating_add(reserved));
        let mut entry = entry.clone();
        fit_run_receipt(&mut entry, available.min(MAX_JOURNAL_ENTRY_SIZE as u64))?;
        let payload = serde_json::to_vec(&entry)?;
        let reservation = run_result_reservation(&entry)?;
        if (payload.len() as u64).saturating_add(reservation) > MAX_JOURNAL_ENTRY_SIZE as u64 {
            bail!("journal entry exceeds bounded size");
        }
        self.check_capacity(&path, (payload.len() as u64).saturating_add(reservation))?;
        if let Err(error) = self.atomic_write(&path, &payload) {
            *self.usage.lock().unwrap_or_else(|p| p.into_inner()) = None;
            return Err(error);
        }
        self.update_usage(&path, Some(payload.len() as u64), reservation);
        Ok(entry)
    }

    fn check_capacity(&self, path: &Path, new_bytes: u64) -> Result<()> {
        let (count, bytes) = self.current_usage()?;
        let usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        let old_bytes = usage.as_ref().and_then(|u| u.files.get(path)).copied();
        if count + usize::from(is_entry_path(path) && old_bytes.is_none()) > self.max_entries {
            bail!("journal reached its record bound ({})", self.max_entries);
        }
        if bytes
            .saturating_sub(old_bytes.unwrap_or(0))
            .saturating_add(new_bytes)
            .saturating_add(usage.as_ref().map_or(0, |u| {
                u.reservations
                    .iter()
                    .filter(|(p, _)| p.as_path() != path)
                    .map(|(_, bytes)| *bytes)
                    .sum::<u64>()
            }))
            > self.max_bytes
        {
            bail!("journal reached its byte bound ({})", self.max_bytes);
        }
        Ok(())
    }

    fn reserved_bytes_except(&self, path: &Path) -> u64 {
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map_or(0, |usage| {
                usage
                    .reservations
                    .iter()
                    .filter(|(p, _)| p.as_path() != path)
                    .map(|(_, bytes)| *bytes)
                    .sum()
            })
    }

    fn update_usage(&self, path: &Path, bytes: Option<u64>, reservation: u64) {
        let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(usage) = usage.as_mut() {
            usage.skipped.remove(path);
            usage.reservations.remove(path);
            if reservation > 0 {
                usage.reservations.insert(path.to_owned(), reservation);
            }
            if let Some(old) = usage.files.remove(path) {
                usage.bytes -= old;
                if !usage.retained.remove(path) {
                    usage.entries -= usize::from(is_entry_path(path));
                }
            }
            if let Some(bytes) = bytes {
                usage.files.insert(path.to_owned(), bytes);
                usage.bytes += bytes;
                usage.entries += usize::from(is_entry_path(path));
            }
        }
    }

    /// The entry at `path` is an acknowledged queue-attempt receipt: it stops
    /// counting against the record bound.
    fn mark_retained(&self, path: &Path) {
        let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(usage) = usage.as_mut() {
            if usage.files.contains_key(path) && usage.retained.insert(path.to_owned()) {
                usage.entries -= usize::from(is_entry_path(path));
            }
        }
    }

    fn sync_directory(&self) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_next_directory_sync.swap(false, Ordering::Relaxed) {
            return Err(io::Error::other("injected directory sync failure"));
        }
        sync_directory(&self.directory)
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
            self.sync_directory()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        result
    }

    // Load metadata once per process. RPC lookup reads only its hashed receipt;
    // writing updates counters without parsing every historical payload.
    fn current_usage(&self) -> Result<(usize, u64)> {
        let mut cached = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        if cached.is_none() {
            self.ensure_confined()?;
            let mut usage = JournalUsage::default();
            if self.directory.exists() {
                for entry in fs::read_dir(&self.directory)? {
                    let entry = entry?;
                    let path = entry.path();
                    let mut uncounted = false;
                    if !is_entry_path(&path) && entry.file_name() != "workspace-state.json" {
                        // Atomic-write scratch files are never replayable.
                        if entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with(".journal.tmp.")
                        {
                            if let Err(error) = fs::remove_file(&path) {
                                tracing::warn!(path = %path.display(), %error, "could not remove journal scratch file");
                            }
                        }
                        continue;
                    }
                    if is_entry_path(&path) {
                        // One-time compaction can read the pre-fix, unbounded
                        // CI receipts. Normal RPC reads are always bounded.
                        let decoded = (|| -> Result<JournalEntry> {
                            if !fs::symlink_metadata(&path)?.is_file() {
                                bail!("non-regular journal entry");
                            }
                            Ok(serde_json::from_reader(BufReader::new(File::open(&path)?))?)
                        })();
                        let record = match decoded {
                            Ok(record) => record,
                            Err(error) => {
                                let quarantine = self.directory.join(format!(
                                    "corrupt-{}",
                                    entry.file_name().to_string_lossy()
                                ));
                                match fs::rename(&path, &quarantine) {
                                    Ok(()) => {
                                        tracing::warn!(path = %path.display(), quarantine = %quarantine.display(), %error, "quarantined invalid daemon journal entry");
                                        if let Err(sync_error) = self.sync_directory() {
                                            tracing::warn!(path = %quarantine.display(), %sync_error, "could not sync journal quarantine");
                                        }
                                    }
                                    Err(quarantine_error) => {
                                        tracing::warn!(path = %path.display(), %error, %quarantine_error, "could not quarantine daemon journal entry; skipping it");
                                        usage.skipped.insert(path);
                                    }
                                }
                                continue;
                            }
                        };
                        if matches!(&record, JournalEntry::Operation { operation } if operation.acknowledged && !is_queue_attempt(operation))
                        {
                            if let Err(error) = fs::remove_file(&path) {
                                tracing::warn!(path = %path.display(), %error, "could not remove acknowledged journal entry; skipping it");
                                usage.skipped.insert(path);
                            } else if let Err(error) = self.sync_directory() {
                                tracing::warn!(path = %path.display(), %error, "could not sync acknowledged journal entry removal");
                            }
                            continue;
                        }
                        if matches!(&record, JournalEntry::Check { operation } if operation.expired(chrono::Utc::now()))
                        {
                            if let Err(error) = fs::remove_file(&path) {
                                tracing::warn!(path = %path.display(), %error, "could not remove expired check journal entry; skipping it");
                                usage.skipped.insert(path);
                            } else if let Err(error) = self.sync_directory() {
                                tracing::warn!(path = %path.display(), %error, "could not sync expired check journal entry removal");
                            }
                            continue;
                        }
                        uncounted = matches!(&record, JournalEntry::Operation { operation } if operation.acknowledged && is_queue_attempt(operation));
                        // Scrub existing pre-fix receipts as part of the one-time
                        // journal scan, retaining their replay identity and result.
                        let retained = sanitized_entry(&record);
                        // A check intent found at startup was interrupted: no
                        // receipt will ever be written for it, so it holds no
                        // completion headroom.
                        let reservation = if matches!(retained, JournalEntry::Check { .. }) {
                            0
                        } else {
                            run_result_reservation(&retained)?
                        };
                        if reservation > 0 {
                            usage.reservations.insert(path.clone(), reservation);
                        }
                        if serde_json::to_value(&retained)? != serde_json::to_value(&record)? {
                            self.atomic_write(&path, &serde_json::to_vec(&retained)?)?;
                        }
                    }
                    let bytes = fs::metadata(&path)?.len();
                    usage.bytes = usage.bytes.saturating_add(bytes);
                    if uncounted {
                        usage.retained.insert(path.clone());
                    } else {
                        usage.entries += usize::from(is_entry_path(&path));
                    }
                    usage.files.insert(path, bytes);
                }
            }
            // Pre-fix CI receipts were excluded from the queue byte bound.
            // Compact their logs to fit the shared budget without discarding
            // pending completion metadata or intents needed for crash replay.
            let reserved: u64 = usage.reservations.values().sum();
            if usage.bytes.saturating_add(reserved) > self.max_bytes {
                let mut paths = usage
                    .files
                    .iter()
                    .map(|(path, bytes)| (path.clone(), *bytes))
                    .collect::<Vec<_>>();
                paths.sort_by_key(|(_, bytes)| std::cmp::Reverse(*bytes));
                for (path, old_bytes) in paths {
                    if usage.bytes.saturating_add(reserved) <= self.max_bytes {
                        break;
                    }
                    if !is_entry_path(&path) {
                        continue;
                    }
                    let mut record = read_entry(&path)?;
                    let available = self.max_bytes.saturating_sub(
                        usage
                            .bytes
                            .saturating_sub(old_bytes)
                            .saturating_add(reserved),
                    );
                    fit_run_receipt(&mut record, available)?;
                    let payload = serde_json::to_vec(&record)?;
                    if (payload.len() as u64) < old_bytes {
                        self.atomic_write(&path, &payload)?;
                        usage.bytes = usage.bytes - old_bytes + payload.len() as u64;
                        usage.files.insert(path, payload.len() as u64);
                    }
                }
            }
            *cached = Some(usage);
        }
        let usage = cached.as_ref().expect("usage initialized");
        Ok((usage.entries, usage.bytes))
    }

    fn is_skipped(&self, path: &Path) -> bool {
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(|usage| usage.skipped.contains(path))
    }
}

/// A queue claim's effect, as opposed to a Task step's.
fn is_queue_attempt(operation: &JournalOperation) -> bool {
    matches!(
        operation.fence.integration,
        api_types::WorkspaceIntegrationBinding::Attempt { .. }
    )
}

fn is_entry_path(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|name| name.to_string_lossy().starts_with("entry-"))
        && path.extension().is_some_and(|ext| ext == "json")
}

// Bind replay to the redacted request, never a caller-supplied digest or secrets.
pub(crate) fn journal_request(request: &Value) -> Value {
    let mut retained = request.clone();
    let env = request_environment(request);
    if let Some(object) = retained.as_object_mut() {
        object.remove("_request_digest");
        for pointer in ["/spec/commands", "/cleanup_commands"] {
            if let Some(commands) = request.pointer(pointer).and_then(Value::as_array) {
                let commands = commands
                    .iter()
                    .cloned()
                    .map(|mut command| {
                        if let Some(text) = command.get_mut("shell_text") {
                            redact_text(text, &env);
                        }
                        command
                    })
                    .collect::<Vec<_>>();
                if pointer == "/spec/commands" {
                    if let Some(spec) = object.get_mut("spec").and_then(Value::as_object_mut) {
                        spec.insert("commands".into(), serde_json::json!(commands));
                    }
                } else {
                    object.insert("cleanup_commands".into(), serde_json::json!(commands));
                }
            }
        }
        if let Some(command) = object.get_mut("command") {
            redact_text(command, &env);
        }
        if let Some(environment) = object.get_mut("env") {
            // Already-sanitized names remain names during startup compaction.
            if !environment
                .as_array()
                .is_some_and(|names| names.iter().all(Value::is_string))
            {
                *environment = serde_json::json!(env.keys().collect::<Vec<_>>());
            }
        }
        if let Some(environment) = object
            .get_mut("operation")
            .and_then(|op| op.get_mut("environment"))
        {
            if let Some(values) = environment
                .get_mut("env")
                .filter(|values| values.is_object())
            {
                *values = serde_json::json!(values.as_object().unwrap().keys().collect::<Vec<_>>());
            }
            if let Some(checks) = environment.get_mut("checks").and_then(Value::as_array_mut) {
                for check in checks {
                    if let Some(command) = check.get_mut("command") {
                        redact_text(command, &env);
                    }
                }
            }
        }
        if let Some(operation) = object
            .get_mut("operation")
            .filter(|op| op["kind"] == "publish_plan")
        {
            if let Some(content) = operation["content"].as_str().map(str::to_owned) {
                operation["content_digest"] =
                    Value::String(format!("{:x}", Sha256::digest(content.as_bytes())));
                operation["content_length"] = serde_json::json!(content.len());
                operation.as_object_mut().unwrap().remove("content");
            }
        }
        let digest = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&retained).expect("JSON value serializes"))
        );
        retained["_request_digest"] = Value::String(digest);
    }
    retained
}

fn request_environment(request: &Value) -> std::collections::BTreeMap<String, String> {
    let mut env: std::collections::BTreeMap<String, String> = request
        .get("env")
        .cloned()
        .and_then(|v| serde_json::from_value::<Vec<(String, String)>>(v).ok())
        .unwrap_or_default()
        .into_iter()
        .collect();
    if let Some(values) = request
        .pointer("/operation/environment/env")
        .and_then(Value::as_object)
    {
        env.extend(values.iter().filter_map(|(key, value)| {
            value.as_str().map(|value| (key.clone(), value.to_owned()))
        }));
    }
    env
}

fn redact_text(value: &mut Value, env: &std::collections::BTreeMap<String, String>) {
    if let Value::String(text) = value {
        *text = executors::environment::redact_environment_values(text, env);
    }
}

pub(crate) fn sanitize_terminal_report(
    report: &mut ExecutionTerminalNotification,
    env: &std::collections::BTreeMap<String, String>,
) {
    for text in [&mut report.summary, &mut report.error]
        .into_iter()
        .flatten()
    {
        *text = executors::environment::redact_environment_values(text, env);
    }
    if let Some(text) = &mut report.plan_text {
        *text = executors::environment::redact_environment_values(text, env);
    }
    for entry in &mut report.outbox_entries {
        match entry {
            api_types::ExecutionOutboxEntry::Worklog { summary, .. } => {
                *summary = executors::environment::redact_environment_values(summary, env);
            }
            api_types::ExecutionOutboxEntry::Evidence {
                caption, content, ..
            } => {
                *caption = executors::environment::redact_environment_values(caption, env);
                if let Some(text) = content {
                    *text = executors::environment::redact_environment_values(text, env);
                }
            }
        }
    }
}

// A prefix says the beginning was omitted. Keep a UTF-8-safe byte tail.
fn marked_tail(text: &str, budget: usize) -> String {
    let body = text.strip_prefix(CI_LOG_TRUNCATION_MARKER).unwrap_or(text);
    if budget < CI_LOG_TRUNCATION_MARKER.len() {
        return String::new();
    }
    let mut start = body
        .len()
        .saturating_sub(budget - CI_LOG_TRUNCATION_MARKER.len());
    while !body.is_char_boundary(start) {
        start += 1;
    }
    format!("{CI_LOG_TRUNCATION_MARKER}{}", &body[start..])
}

fn sanitized_entry(entry: &JournalEntry) -> JournalEntry {
    let mut retained = entry.clone();
    if let JournalEntry::Operation { operation } = &mut retained {
        let env = request_environment(&operation.request);
        operation.request = journal_request(&operation.request);
        if let Some(outcome) = &mut operation.outcome {
            match outcome {
                Ok(value) => {
                    for stream in ["stdout", "stderr"] {
                        if let Some(text) = value.get_mut(stream) {
                            redact_text(text, &env);
                        }
                        if operation.method == METHOD_WORKSPACE_RUN {
                            if let Some(text) = value[stream].as_str().filter(|s| {
                                s.len() > MAX_CI_LOG_BYTES
                                    || (!s.starts_with(CI_LOG_TRUNCATION_MARKER)
                                        && value[format!("{stream}_truncated")] == true
                                        && operation.request["max_output_bytes"]
                                            .as_u64()
                                            .is_some_and(|cap| cap > MAX_CI_LOG_BYTES as u64))
                            }) {
                                value[stream] = Value::String(marked_tail(text, MAX_CI_LOG_BYTES));
                                value[format!("{stream}_truncated")] = Value::Bool(true);
                            }
                        }
                    }
                }
                Err(error) => {
                    error.message =
                        executors::environment::redact_environment_values(&error.message, &env);
                }
            }
        }
    }
    if let JournalEntry::Check { operation } = &mut retained {
        let env = request_environment(&operation.request);
        operation.request = journal_request(&operation.request);
        if let Some(receipt) = operation.receipt.as_mut() {
            for command in receipt
                .commands
                .iter_mut()
                .chain(receipt.cleanup.commands.iter_mut())
            {
                command.command =
                    executors::environment::redact_environment_values(&command.command, &env);
                for (text, truncated) in [
                    (&mut command.stdout_tail, &mut command.stdout_truncated),
                    (&mut command.stderr_tail, &mut command.stderr_truncated),
                ] {
                    *text = executors::environment::redact_environment_values(text, &env);
                    let mut start = text.len().saturating_sub(check_executor::OUTPUT_TAIL_BYTES);
                    while !text.is_char_boundary(start) {
                        start += 1;
                    }
                    if start > 0 {
                        *text = text[start..].to_owned();
                        *truncated = true;
                    }
                }
            }
            for message in [
                &mut receipt.infrastructure_message,
                &mut receipt.cleanup.message,
            ]
            .into_iter()
            .flatten()
            {
                *message = executors::environment::redact_environment_values(message, &env);
            }
        }
    }
    retained
}

// Admission reserves room for a run's completion without logs. Other writes
// cannot consume it while the command runs, including workspace-registry writes.
fn run_result_reservation(entry: &JournalEntry) -> Result<u64> {
    if let JournalEntry::Check { operation } = entry {
        if operation.receipt.is_some() {
            return Ok(0);
        }
        // Headroom for this request's own receipt: per declared command two
        // 4096-byte tails at worst-case JSON escaping (six bytes per byte)
        // plus its fixed fields and repeated command text.
        let commands = ["/spec/commands", "/cleanup_commands"]
            .iter()
            .filter_map(|pointer| operation.request.pointer(pointer)?.as_array())
            .map(Vec::len)
            .sum::<usize>() as u64;
        let request = serde_json::to_vec(&operation.request)?.len() as u64;
        return Ok(commands
            .saturating_mul(2 * 6 * check_executor::OUTPUT_TAIL_BYTES as u64 + 2048)
            .saturating_add(request)
            .saturating_add(8192));
    }
    let JournalEntry::Operation { operation } = entry else {
        return Ok(0);
    };
    if operation.method != METHOD_WORKSPACE_RUN || operation.outcome.is_some() {
        return Ok(0);
    }
    let mut completed = operation.clone();
    completed.outcome = Some(Ok(serde_json::json!({
        "entry_id": operation.entry_id, "operation_id": operation.fence.operation_id,
        "exit_code": i32::MIN, "duration_ms": u64::MAX, "timed_out": false,
        "stdout":"", "stderr":"", "stdout_truncated":true, "stderr_truncated":true,
        "stdout_drain_incomplete":true, "stderr_drain_incomplete":true
    })));
    Ok((serde_json::to_vec(&JournalEntry::Operation {
        operation: completed,
    })?
    .len() as u64)
        .saturating_sub(serde_json::to_vec(entry)?.len() as u64)
        .saturating_add(4096))
}

/// Preserve completion metadata under pressure, dropping all log text if needed.
fn fit_run_receipt(entry: &mut JournalEntry, available: u64) -> Result<()> {
    if serde_json::to_vec(entry)?.len() as u64 <= available {
        return Ok(());
    }
    let JournalEntry::Operation { operation } = entry else {
        return Ok(());
    };
    if operation.method != METHOD_WORKSPACE_RUN {
        return Ok(());
    }
    let Some(Ok(value)) = operation.outcome.as_mut() else {
        return Ok(());
    };
    let logs = ["stdout", "stderr"].map(|stream| {
        let text = value[stream].as_str().unwrap_or_default().to_owned();
        let flag = value.get(format!("{stream}_truncated")).cloned();
        if !text.is_empty() {
            value[stream] = Value::String(String::new());
            value[format!("{stream}_truncated")] = Value::Bool(true);
        }
        (stream, text, flag)
    });
    let overhead = serde_json::to_vec(entry)?.len() as u64;
    let streams = logs.iter().filter(|(_, text, _)| !text.is_empty()).count();
    if streams == 0 || overhead > available {
        return Ok(());
    }
    // JSON escaping expands each byte by at most six, including the marker.
    let budget = available.saturating_sub(overhead) / (6 * streams as u64);
    let JournalEntry::Operation { operation } = entry else {
        unreachable!();
    };
    let Some(Ok(value)) = operation.outcome.as_mut() else {
        unreachable!();
    };
    for (stream, text, flag) in logs {
        if text.is_empty() {
            continue;
        }
        if text.len() as u64 <= budget {
            value[stream] = Value::String(text);
            if let Some(flag) = flag {
                value[format!("{stream}_truncated")] = flag;
            } else if let Some(object) = value.as_object_mut() {
                object.remove(&format!("{stream}_truncated"));
            }
        } else {
            value[stream] = Value::String(marked_tail(&text, budget as usize));
        }
    }
    Ok(())
}

pub fn operation_entry_id(operation_id: &str) -> String {
    format!("forge:operation:{operation_id}")
}

fn read_entry(path: &Path) -> Result<JournalEntry> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() {
        bail!("invalid journal file {}", path.display());
    }
    if metadata.len() > MAX_JOURNAL_ENTRY_SIZE as u64 {
        bail!("oversized journal file {}", path.display());
    }
    let entry: JournalEntry = serde_json::from_reader(BufReader::new(
        File::open(path)?.take(MAX_JOURNAL_ENTRY_SIZE as u64 + 1),
    ))
    .with_context(|| format!("decode {}", path.display()))?;
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
            restarted
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

    fn attempt_receipt(generation: i64) -> JournalOperation {
        attempt_receipt_on("queue-1", generation)
    }

    fn attempt_receipt_on(queue: &str, generation: i64) -> JournalOperation {
        let request = api_types::WorkspaceIntegrationRequest {
            fence: api_types::IntegrationOwnerFence {
                queue_id: queue.into(),
                attempt_id: format!("attempt-{queue}"),
                generation,
                lease_owner: "queue-worker".into(),
                target_owner: serde_json::json!({"owner_kind":"daemon"}),
            },
            kind: api_types::WorkspaceIntegrationKind::FastForward,
            witness: serde_json::json!({}),
        };
        let mut operation = run_intent(&request.operation_id());
        operation.method = api_types::METHOD_WORKSPACE_MERGE.into();
        operation.request = serde_json::json!({});
        operation.fence.integration = api_types::WorkspaceIntegrationBinding::Attempt { request };
        operation.outcome = Some(Ok(serde_json::json!({"outcome":{"kind":"done"}})));
        operation
    }

    /// Two hundred merged attempts on one queue never approach the journal
    /// bound: each acknowledged receipt is pruned by the next generation.
    #[test]
    fn acknowledged_attempt_receipts_are_retained_then_pruned_within_the_bound() {
        let dir = tempfile::tempdir().unwrap();
        let store = DaemonJournal::with_limits(dir.path(), 4, MAX_JOURNAL_BYTES);
        store.initialize().unwrap();
        for generation in 1..=200_i64 {
            let operation = attempt_receipt(generation);
            store
                .retain_entry(&JournalEntry::Operation {
                    operation: operation.clone(),
                })
                .unwrap();
            // Unacknowledged receipts are never prunable.
            assert!(
                store
                    .prune_acknowledged_attempts(
                        |candidate, _| candidate.entry_id == operation.entry_id
                    )
                    .unwrap()
                    .is_empty()
            );
            assert!(
                store
                    .acknowledge(&JournalAckParams {
                        entry_id: operation.entry_id.clone(),
                    })
                    .unwrap()
                    .acknowledged
            );
            // Acknowledged: kept for a duplicate of the same key, no longer
            // replayed, and it survives a restart.
            let kept = store
                .operation(&operation.fence.operation_id)
                .unwrap()
                .unwrap();
            assert!(kept.acknowledged);
            assert!(store.pending().unwrap().is_empty());
            if generation % 50 == 0 {
                let restarted = DaemonJournal::with_limits(dir.path(), 4, MAX_JOURNAL_BYTES);
                restarted.initialize().unwrap();
                assert!(restarted
                    .operation(&operation.fence.operation_id)
                    .unwrap()
                    .is_some());
            }
            let removed = store
                .prune_acknowledged_attempts(|operation, _| {
                    matches!(&operation.fence.integration, api_types::WorkspaceIntegrationBinding::Attempt { request } if request.fence.generation < generation)
                })
                .unwrap();
            assert_eq!(removed.len(), usize::from(generation > 1));
        }
        let entries = fs::read_dir(store.directory())
            .unwrap()
            .filter(|entry| is_entry_path(&entry.as_ref().unwrap().path()))
            .count();
        assert_eq!(entries, 1);
    }

    /// A server that never acknowledges. The journal's bound is what it has
    /// not been acknowledged for: `max_entries` records (1024 in production)
    /// and `max_bytes` (32 MiB). At the bound a new record is refused, so the
    /// owner admits no further effect; nothing is evicted to make room, the
    /// directory does not grow, and the owner's pruning rules (fence moved
    /// on, age, number) cannot touch a receipt the server has not stored,
    /// whatever they say.
    #[test]
    fn a_server_that_never_acknowledges_neither_grows_the_journal_nor_loses_a_receipt() {
        fn usage(root: &Path) -> (usize, u64) {
            let mut total = (0, 0);
            let mut dirs = vec![root.to_owned()];
            while let Some(dir) = dirs.pop() {
                for entry in fs::read_dir(dir).unwrap() {
                    let entry = entry.unwrap();
                    let metadata = entry.metadata().unwrap();
                    if metadata.is_dir() {
                        dirs.push(entry.path());
                    } else {
                        total = (total.0 + 1, total.1 + metadata.len());
                    }
                }
            }
            total
        }
        let dir = tempfile::tempdir().unwrap();
        let store = DaemonJournal::with_limits(dir.path(), 8, MAX_JOURNAL_BYTES);
        store.initialize().unwrap();
        let kept: Vec<JournalOperation> = (0..8)
            .map(|index| attempt_receipt_on(&format!("queue-{index}"), 1))
            .collect();
        for operation in &kept {
            store
                .retain_entry(&JournalEntry::Operation {
                    operation: operation.clone(),
                })
                .unwrap();
        }
        let full = usage(dir.path());
        // Two hundred more claims arrive; the server has acknowledged nothing.
        for index in 0..200 {
            let refused = store
                .retain_entry(&JournalEntry::Operation {
                    operation: attempt_receipt_on(&format!("over-{index}"), 1),
                })
                .unwrap_err()
                .to_string();
            assert!(refused.contains("record bound"), "{refused}");
        }
        assert_eq!(usage(dir.path()), full, "a refused record writes nothing");
        // The pruning rules are handed "prune everything": nothing goes.
        assert!(store
            .prune_acknowledged_attempts(|_, _| true)
            .unwrap()
            .is_empty());
        assert_eq!(usage(dir.path()), full);
        assert_eq!(store.pending().unwrap().len(), 8);
        for operation in &kept {
            let stored = store
                .operation(&operation.fence.operation_id)
                .unwrap()
                .unwrap();
            assert!(!stored.acknowledged);
            assert_eq!(stored.entry_id, operation.entry_id);
        }
        // The byte bound refuses the same way.
        let small = tempfile::tempdir().unwrap();
        let bytes = DaemonJournal::with_limits(small.path(), MAX_JOURNAL_ENTRIES, 4096);
        bytes.initialize().unwrap();
        let mut accepted = 0;
        let refused = loop {
            match bytes.retain_entry(&JournalEntry::Operation {
                operation: attempt_receipt_on(&format!("bytes-{accepted}"), 1),
            }) {
                Ok(_) => accepted += 1,
                Err(error) => break error.to_string(),
            }
            assert!(accepted < 64, "the byte bound never refused");
        };
        assert!(refused.contains("byte bound"), "{refused}");
        assert_eq!(bytes.pending().unwrap().len(), accepted);
        assert!(bytes
            .prune_acknowledged_attempts(|_, _| true)
            .unwrap()
            .is_empty());
        // Once the server acknowledges, the slots are free again, and only
        // then do the pruning rules apply: to the acknowledged receipts.
        for operation in &kept {
            store
                .acknowledge(&JournalAckParams {
                    entry_id: operation.entry_id.clone(),
                })
                .unwrap();
        }
        let later: Vec<JournalOperation> = (0..8)
            .map(|index| attempt_receipt_on(&format!("later-{index}"), 1))
            .collect();
        for operation in &later {
            store
                .retain_entry(&JournalEntry::Operation {
                    operation: operation.clone(),
                })
                .unwrap();
        }
        let mut removed = store.prune_acknowledged_attempts(|_, _| true).unwrap();
        removed.sort();
        let mut acknowledged: Vec<String> = kept
            .iter()
            .map(|operation| operation.fence.operation_id.clone())
            .collect();
        acknowledged.sort();
        assert_eq!(removed, acknowledged);
        assert_eq!(store.pending().unwrap().len(), 8);
        for operation in &later {
            assert!(store
                .operation(&operation.fence.operation_id)
                .unwrap()
                .is_some());
        }
    }

    /// Four hundred acknowledged attempt receipts on a journal bounded at
    /// four records (each write is several fsyncs, so the count is kept to
    /// what runs in seconds): none of them takes a slot, so the bound keeps
    /// meaning "work the server has not acknowledged". The fifth
    /// unacknowledged entry is refused, no acknowledged or unacknowledged
    /// receipt is evicted to make room, and a restart counts the same way.
    #[test]
    fn acknowledged_attempt_receipts_never_fill_the_record_bound() {
        let dir = tempfile::tempdir().unwrap();
        let store = DaemonJournal::with_limits(dir.path(), 4, MAX_JOURNAL_BYTES);
        store.initialize().unwrap();
        for index in 0..400 {
            let operation = attempt_receipt_on(&format!("queue-{index}"), 1);
            store
                .retain_entry(&JournalEntry::Operation {
                    operation: operation.clone(),
                })
                .unwrap();
            store
                .acknowledge(&JournalAckParams {
                    entry_id: operation.entry_id.clone(),
                })
                .unwrap();
        }
        assert_eq!(store.acknowledged_attempt_ages().unwrap().len(), 400);
        let unacknowledged: Vec<JournalOperation> = (0..4)
            .map(|index| attempt_receipt_on(&format!("pending-{index}"), 1))
            .collect();
        for operation in &unacknowledged {
            store
                .retain_entry(&JournalEntry::Operation {
                    operation: operation.clone(),
                })
                .unwrap();
        }
        let refused = |store: &DaemonJournal| {
            store
                .retain_entry(&JournalEntry::Operation {
                    operation: attempt_receipt_on("pending-over", 1),
                })
                .unwrap_err()
                .to_string()
        };
        assert!(refused(&store).contains("record bound"));
        let restarted = DaemonJournal::with_limits(dir.path(), 4, MAX_JOURNAL_BYTES);
        restarted.initialize().unwrap();
        assert!(refused(&restarted).contains("record bound"));
        assert_eq!(restarted.pending().unwrap().len(), 4);
        assert_eq!(restarted.acknowledged_attempt_ages().unwrap().len(), 400);
        // Nothing was evicted: every receipt, acknowledged or not, is there.
        for operation in &unacknowledged {
            assert!(
                !restarted
                    .operation(&operation.fence.operation_id)
                    .unwrap()
                    .unwrap()
                    .acknowledged
            );
        }
        assert!(
            restarted
                .operation(&attempt_receipt_on("queue-0", 1).fence.operation_id)
                .unwrap()
                .unwrap()
                .acknowledged
        );
        // Acknowledging one frees its slot.
        restarted
            .acknowledge(&JournalAckParams {
                entry_id: unacknowledged[0].entry_id.clone(),
            })
            .unwrap();
        restarted
            .retain_entry(&JournalEntry::Operation {
                operation: attempt_receipt_on("pending-over", 1),
            })
            .unwrap();
        // The owner's rule prunes by number: the oldest go first.
        let ages = restarted.acknowledged_attempt_ages().unwrap();
        assert_eq!(ages.len(), 401);
        // (Ages only grow between the listing and the prune, so a few more
        // than the 201 strictly older ones may go; never an unacknowledged one.)
        let removed = restarted
            .prune_acknowledged_attempts(|_, age| age > ages[199])
            .unwrap();
        assert!(removed.len() >= 201, "{}", removed.len());
        assert_eq!(
            restarted.acknowledged_attempt_ages().unwrap().len(),
            401 - removed.len()
        );
        assert_eq!(restarted.pending().unwrap().len(), 4);
    }

    /// A long-lived daemon serving many queues that each merged once and
    /// went idle: the age rule removes their acknowledged receipts, so the
    /// journal does not grow without bound.
    #[test]
    fn idle_queue_receipts_expire_by_age() {
        let dir = tempfile::tempdir().unwrap();
        let store = DaemonJournal::with_limits(dir.path(), 301, MAX_JOURNAL_BYTES);
        store.initialize().unwrap();
        let queues = 300;
        let week = std::time::Duration::from_secs(7 * 24 * 60 * 60);
        for index in 0..queues {
            let operation = attempt_receipt_on(&format!("queue-{index}"), 1);
            store
                .retain_entry(&JournalEntry::Operation {
                    operation: operation.clone(),
                })
                .unwrap();
            store
                .acknowledge(&JournalAckParams {
                    entry_id: operation.entry_id.clone(),
                })
                .unwrap();
            if index % 3 != 0 {
                // Acknowledged more than a week ago.
                fs::File::options()
                    .write(true)
                    .open(store.path_for_entry(&operation.entry_id).unwrap())
                    .unwrap()
                    .set_modified(std::time::SystemTime::now() - week - week)
                    .unwrap();
            }
        }
        assert!(store.pending().unwrap().is_empty());
        store
            .retain_entry(&JournalEntry::Operation {
                operation: attempt_receipt_on("queue-new", 1),
            })
            .unwrap();
        // Acknowledged receipts hold no slot of the record bound.
        store
            .retain_entry(&JournalEntry::Operation {
                operation: attempt_receipt_on("queue-over", 1),
            })
            .unwrap();
        let removed = store
            .prune_acknowledged_attempts(|_, acknowledged_for| acknowledged_for > week)
            .unwrap();
        assert_eq!(removed.len(), 200);
        store
            .retain_entry(&JournalEntry::Operation {
                operation: attempt_receipt_on("queue-over", 1),
            })
            .unwrap();
        let restarted = DaemonJournal::with_limits(dir.path(), 301, MAX_JOURNAL_BYTES);
        restarted.initialize().unwrap();
        let entries = fs::read_dir(restarted.directory())
            .unwrap()
            .filter(|entry| is_entry_path(&entry.as_ref().unwrap().path()))
            .count();
        assert_eq!(entries, 102, "100 recent receipts and two pending intents");
        assert_eq!(restarted.pending().unwrap().len(), 2);
    }

    #[test]
    fn acknowledged_operation_receipts_do_not_consume_the_pending_queue_bound() {
        let dir = tempfile::tempdir().unwrap();
        let store = DaemonJournal::with_limits(dir.path(), 1, 2048);
        let operation = JournalOperation {
            entry_id: operation_entry_id("run-once"),
            fence: WorkspaceMutationFence {
                integration: api_types::WorkspaceIntegrationBinding::TaskStep,
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
            effect_started: true,
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
        assert!(store.operation("run-once").unwrap().is_none());
    }

    #[test]
    fn operation_receipt_is_bounded_replays_before_ack_and_shrinks_after_ack() {
        let dir = tempfile::tempdir().unwrap();
        let store = DaemonJournal::with_limits(dir.path(), 2, 2 * MAX_CI_LOG_BYTES as u64);
        let mut operation = JournalOperation {
            entry_id: operation_entry_id("ci-result"),
            fence: WorkspaceMutationFence {
                integration: api_types::WorkspaceIntegrationBinding::TaskStep,
                daemon_id: "daemon-1".into(),
                runtime_id: "runtime-1".into(),
                placement_id: "placement-1".into(),
                operation_id: "ci-result".into(),
                generation: 1,
                expected: api_types::WorkspaceOperationExpected::Version { version: 1 },
            },
            workspace_handle: Some("handle".into()),
            method: METHOD_WORKSPACE_RUN.into(),
            request: serde_json::json!({"purpose":"ci_step", "max_output_bytes":u64::MAX}),
            outcome: None,
            acknowledged: false,
            effect_started: true,
        };
        store
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
        operation.outcome = Some(Ok(
            serde_json::json!({"stdout": "🦀".repeat(MAX_CI_LOG_BYTES), "stderr":""}),
        ));
        store.finish_operation(&operation).unwrap();
        drop(store); // Crash between durable completion and server acknowledgement.
        let restarted = DaemonJournal::with_limits(dir.path(), 2, 2 * MAX_CI_LOG_BYTES as u64);
        let receipt = restarted.operation("ci-result").unwrap().unwrap();
        let result = receipt.outcome.unwrap().unwrap();
        let stdout = result["stdout"].as_str().unwrap();
        assert!(stdout.len() <= MAX_CI_LOG_BYTES);
        assert!(stdout.starts_with(CI_LOG_TRUNCATION_MARKER));
        assert_eq!(result["stdout_truncated"], true);
        let before = restarted.current_usage().unwrap().1;
        let too_small = DaemonJournal::with_limits(dir.path(), 2, before);
        assert!(too_small
            .retain(&notification("would-overflow"))
            .unwrap_err()
            .to_string()
            .contains("byte bound"));
        restarted
            .acknowledge(&JournalAckParams {
                entry_id: operation.entry_id.clone(),
            })
            .unwrap();
        assert!(restarted.current_usage().unwrap().1 < before);
        assert!(!restarted
            .path_for_entry(&operation.entry_id)
            .unwrap()
            .exists());
        assert!(restarted.operation("ci-result").unwrap().is_none());
        assert!(
            restarted
                .acknowledge(&JournalAckParams {
                    entry_id: operation.entry_id.clone()
                })
                .unwrap()
                .acknowledged
        );
        // Startup compacts pre-fix oversized CI logs to the shared byte cap.
        let mut legacy = operation;
        legacy.outcome = Some(Ok(
            serde_json::json!({"stdout": "x".repeat(MAX_JOURNAL_ENTRY_SIZE + 1), "stderr":""}),
        ));
        fs::write(
            restarted.path_for_entry(&legacy.entry_id).unwrap(),
            serde_json::to_vec(&JournalEntry::Operation {
                operation: legacy.clone(),
            })
            .unwrap(),
        )
        .unwrap();
        legacy.fence.operation_id = "other-legacy-ci".into();
        legacy.entry_id = operation_entry_id(&legacy.fence.operation_id);
        legacy.outcome = Some(Ok(
            serde_json::json!({"stdout": "x".repeat(MAX_CI_LOG_BYTES + 1), "stderr":""}),
        ));
        fs::write(
            restarted.path_for_entry(&legacy.entry_id).unwrap(),
            serde_json::to_vec(&JournalEntry::Operation { operation: legacy }).unwrap(),
        )
        .unwrap();
        let migrated = DaemonJournal::with_limits(dir.path(), 2, 2 * MAX_CI_LOG_BYTES as u64);
        migrated.initialize().unwrap();
        assert!(migrated.current_usage().unwrap().1 <= 2 * MAX_CI_LOG_BYTES as u64);
        assert_eq!(migrated.pending().unwrap().len(), 2);
    }

    #[test]
    fn revision_three_journal_is_migrated_without_accepting_old_wire() {
        let dir = tempfile::tempdir().unwrap();
        let store = DaemonJournal::new(dir.path());
        let operation = run_intent("old-integration-intent");
        let entry_id = operation.entry_id.clone();
        store
            .retain_entry(&JournalEntry::Operation { operation })
            .unwrap();
        let path = store.path_for_entry(&entry_id).unwrap();
        let mut old: Value = read_json(&path).unwrap();
        old["operation"]["fence"]
            .as_object_mut()
            .unwrap()
            .remove("integration");
        old["operation"]["request"]
            .as_object_mut()
            .unwrap()
            .remove("integration");
        old["operation"]
            .as_object_mut()
            .unwrap()
            .remove("effect_started");
        assert!(serde_json::from_value::<WorkspaceMutationFence>(
            old["operation"]["fence"].clone()
        )
        .is_err());
        old["operation"]["request"]["operation_id"] = serde_json::json!("old-integration-intent");
        fs::write(&path, serde_json::to_vec(&old).unwrap()).unwrap();
        let restart = DaemonJournal::new(dir.path());
        restart.initialize().unwrap();
        let migrated = restart
            .operation("old-integration-intent")
            .unwrap()
            .unwrap();
        assert!(matches!(
            migrated.fence.integration,
            api_types::WorkspaceIntegrationBinding::TaskStep
        ));
        assert!(migrated.effect_started);
        assert!(migrated.outcome.is_none());
        let disk: Value = read_json(&path).unwrap();
        assert_eq!(
            disk["operation"]["request"]["integration"]["kind"],
            "task_step"
        );
    }

    #[test]
    fn operation_journal_never_persists_environment_secret_values() {
        let dir = tempfile::tempdir().unwrap();
        let journal = DaemonJournal::new(dir.path());
        let secret = "super-secret-token-123";
        let mut operation = JournalOperation {
            entry_id: operation_entry_id("secret-run"),
            fence: WorkspaceMutationFence {
                integration: api_types::WorkspaceIntegrationBinding::TaskStep,
                daemon_id: "daemon-1".into(),
                runtime_id: "runtime-1".into(),
                placement_id: "placement-1".into(),
                operation_id: "secret-run".into(),
                generation: 1,
                expected: api_types::WorkspaceOperationExpected::Version { version: 1 },
            },
            workspace_handle: Some("handle".into()),
            method: METHOD_WORKSPACE_RUN.into(),
            request: serde_json::json!({"env":[["TOKEN",secret]], "command":format!("printf {secret}")}),
            outcome: None,
            acknowledged: false,
            effect_started: true,
        };
        journal
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
        let path = journal.path_for_entry(&operation.entry_id).unwrap();
        assert!(!fs::read_to_string(&path).unwrap().contains(secret));
        operation.outcome = Some(Ok(serde_json::json!({"stdout":secret})));
        journal.finish_operation(&operation).unwrap();
        let stored = fs::read_to_string(path).unwrap();
        assert!(!stored.contains(secret));
        let receipt = journal.operation("secret-run").unwrap().unwrap();
        assert_eq!(receipt.request["env"], serde_json::json!(["TOKEN"]));
        assert_eq!(receipt.request, journal_request(&operation.request));
        let mut changed = operation.request.clone();
        changed["env"][0][1] = serde_json::json!("changed-secret");
        changed["command"] = serde_json::json!("printf changed-secret");
        assert_eq!(receipt.request, journal_request(&changed)); // Secret values are deliberately excluded from replay identity.
        assert_eq!(receipt.outcome.unwrap().unwrap()["stdout"], "[REDACTED]");
        // Startup also scrubs receipts written by the pre-fix daemon.
        let path = journal.path_for_entry(&operation.entry_id).unwrap();
        drop(journal);
        fs::write(
            &path,
            serde_json::to_vec(&JournalEntry::Operation { operation }).unwrap(),
        )
        .unwrap();
        let restarted = DaemonJournal::new(dir.path());
        restarted.initialize().unwrap();
        assert!(!fs::read_to_string(path).unwrap().contains(secret));
        for kind in ["review_checkout", "materialize_assets"] {
            let mut operation = run_intent(kind);
            operation.method = api_types::METHOD_WORKSPACE_RESET.into();
            operation.request = serde_json::json!({"operation": {
                "kind": kind, "commit_sha":"commit-1", "prepare":true,
                "environment": {"env":{"TOKEN":secret}, "checks":[{"command":format!("echo {secret}")}], "assets":[]}
            }});
            restarted
                .retain_entry(&JournalEntry::Operation {
                    operation: operation.clone(),
                })
                .unwrap();
            let path = restarted.path_for_entry(&operation.entry_id).unwrap();
            assert!(!fs::read_to_string(&path).unwrap().contains(secret));
            operation.outcome = Some(Err(DaemonErrorPayload {
                code: "check_failed".into(),
                message: format!("check printed {secret}"),
                details: None,
            }));
            restarted.finish_operation(&operation).unwrap();
            assert!(!fs::read_to_string(&path).unwrap().contains(secret));
            let receipt = restarted.operation(kind).unwrap().unwrap();
            assert_eq!(
                receipt.request["operation"]["environment"]["env"],
                serde_json::json!(["TOKEN"])
            );
            assert_eq!(
                receipt.request["operation"]["environment"]["checks"][0]["command"],
                "echo [REDACTED]"
            );
            assert_eq!(
                receipt.outcome.unwrap().unwrap_err().message,
                "check printed [REDACTED]"
            );
            // Existing raw owner-operation receipts are scrubbed on startup too.
            fs::write(
                &path,
                serde_json::to_vec(&JournalEntry::Operation { operation }).unwrap(),
            )
            .unwrap();
            DaemonJournal::new(dir.path()).initialize().unwrap();
            assert!(!fs::read_to_string(path).unwrap().contains(secret));
        }
    }

    fn run_intent(id: &str) -> JournalOperation {
        JournalOperation {
            entry_id: operation_entry_id(id),
            fence: WorkspaceMutationFence {
                integration: api_types::WorkspaceIntegrationBinding::TaskStep,
                daemon_id: "daemon-1".into(),
                runtime_id: "runtime-1".into(),
                placement_id: "placement-1".into(),
                operation_id: id.into(),
                generation: 1,
                expected: api_types::WorkspaceOperationExpected::Version { version: 1 },
            },
            workspace_handle: Some("handle-1".into()),
            method: METHOD_WORKSPACE_RUN.into(),
            request: serde_json::json!({"command":"echo 1", "env":[["CI","1"]], "max_output_bytes":u64::MAX}),
            outcome: None,
            acknowledged: false,
            effect_started: true,
        }
    }

    #[test]
    fn short_environment_values_preserve_result_and_error_identity() {
        let dir = tempfile::tempdir().unwrap();
        let journal = DaemonJournal::new(dir.path());
        for failed in [false, true] {
            let mut operation = run_intent(if failed { "error-1" } else { "result-1" });
            let entry_id = operation.entry_id.clone();
            let operation_id = operation.fence.operation_id.clone();
            journal
                .retain_entry(&JournalEntry::Operation {
                    operation: operation.clone(),
                })
                .unwrap();
            operation.outcome = Some(if failed {
                Err(DaemonErrorPayload {
                    code: "error-1".into(),
                    message: "printed 1".into(),
                    details: Some(
                        serde_json::json!({"entry_id":entry_id,"operation_id":operation_id}),
                    ),
                })
            } else {
                Ok(
                    serde_json::json!({"entry_id":entry_id, "operation_id":operation_id,
                    "exit_code":1, "stdout":"1", "stderr":"1"}),
                )
            });
            journal.finish_operation(&operation).unwrap();
            let restarted = DaemonJournal::new(dir.path());
            restarted.initialize().unwrap();
            let stored = restarted.operation(&operation_id).unwrap().unwrap();
            assert_eq!(stored.entry_id, entry_id);
            assert_eq!(stored.fence.operation_id, operation_id);
            assert_eq!(stored.fence.daemon_id, "daemon-1");
            assert_eq!(stored.workspace_handle.as_deref(), Some("handle-1"));
            match stored.outcome.unwrap() {
                Ok(value) => {
                    assert_eq!(value["entry_id"], entry_id);
                    assert_eq!(value["operation_id"], operation_id);
                    assert_eq!(value["exit_code"], 1);
                    assert_eq!(value["stdout"], "[REDACTED]");
                    assert_eq!(value["stderr"], "[REDACTED]");
                }
                Err(error) => {
                    assert_eq!(error.code, "error-1");
                    assert_eq!(error.message, "printed [REDACTED]");
                    assert_eq!(error.details.unwrap()["entry_id"], entry_id);
                }
            }
            let bytes = fs::read_to_string(restarted.path_for_entry(&entry_id).unwrap()).unwrap();
            // Numeric exit codes and IDs containing 1 stay intact; the secret
            // string value itself is absent from the persisted JSON bytes.
            assert!(!bytes.contains("\"1\""));
            assert!(!bytes.contains("echo 1"));
            assert!(
                restarted
                    .acknowledge(&JournalAckParams { entry_id })
                    .unwrap()
                    .acknowledged
            );
            assert!(restarted.operation(&operation_id).unwrap().is_none());
        }
    }

    #[test]
    fn caller_digest_cannot_skip_redaction_or_hash_secret_values() {
        let request = serde_json::json!({"command":"echo 1", "env":[["CI","1"]], "_request_digest":"trusted?"});
        let clean = journal_request(&request);
        assert_eq!(clean["command"], "echo [REDACTED]");
        assert_eq!(clean["env"], serde_json::json!(["CI"]));
        assert_ne!(clean["_request_digest"], "trusted?");
        assert_eq!(clean, journal_request(&clean));
        assert_eq!(
            clean,
            journal_request(&serde_json::json!({"command":"echo 0", "env":[["CI","0"]]}))
        );
    }

    #[test]
    fn journal_tail_is_utf8_safe_and_stable_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let journal = DaemonJournal::new(dir.path());
        let mut operation = run_intent("tail");
        operation.outcome = Some(Ok(serde_json::json!({
            "stdout":format!("{}last failure lines", "🦀".repeat(MAX_CI_LOG_BYTES)),
            "stderr":"", "stdout_truncated":true
        })));
        journal
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
        let path = journal.path_for_entry(&operation.entry_id).unwrap();
        let bytes = fs::read(&path).unwrap();
        for _ in 0..3 {
            let restarted = DaemonJournal::new(dir.path());
            restarted.initialize().unwrap();
            assert_eq!(fs::read(&path).unwrap(), bytes);
            let result = restarted
                .operation("tail")
                .unwrap()
                .unwrap()
                .outcome
                .unwrap()
                .unwrap();
            let tail = result["stdout"].as_str().unwrap();
            assert!(tail.starts_with(CI_LOG_TRUNCATION_MARKER));
            assert!(tail.ends_with("last failure lines"));
            assert_eq!(tail.matches(CI_LOG_TRUNCATION_MARKER).count(), 1);
        }
    }

    #[test]
    fn undecodable_entry_is_quarantined_without_blocking_replay() {
        let dir = tempfile::tempdir().unwrap();
        let journal = DaemonJournal::new(dir.path());
        journal.retain(&notification("valid")).unwrap();
        let corrupt = journal.directory().join("entry-bad.json");
        fs::write(&corrupt, b"{partial").unwrap();
        let restarted = DaemonJournal::new(dir.path());
        restarted.initialize().unwrap();
        assert!(!corrupt.exists());
        assert_eq!(
            fs::read(journal.directory().join("corrupt-entry-bad.json")).unwrap(),
            b"{partial"
        );
        assert_eq!(restarted.pending().unwrap().len(), 1);
        restarted.retain(&notification("next")).unwrap();
    }

    #[test]
    fn quarantine_failures_and_non_regular_entries_do_not_stop_replay() {
        let dir = tempfile::tempdir().unwrap();
        let journal = DaemonJournal::new(dir.path());
        journal.retain(&notification("valid")).unwrap();
        let corrupt = journal.directory().join("entry-bad.json");
        fs::write(&corrupt, b"{partial").unwrap();
        let blocked = journal.directory().join("corrupt-entry-bad.json");
        fs::create_dir(&blocked).unwrap();
        fs::write(blocked.join("occupied"), b"keep").unwrap();
        fs::create_dir(journal.directory().join("entry-directory.json")).unwrap();
        let restarted = DaemonJournal::new(dir.path());
        restarted
            .fail_next_directory_sync
            .store(true, Ordering::Relaxed);
        restarted.initialize().unwrap();
        assert!(corrupt.exists()); // Rename failed, but the entry is skipped.
        assert_eq!(restarted.pending().unwrap().len(), 1);
        restarted.retain(&notification("next")).unwrap();
        assert_eq!(restarted.pending().unwrap().len(), 2);
    }

    #[test]
    fn skipped_entries_are_absent_for_ack_retain_and_finish() {
        let dir = tempfile::tempdir().unwrap();
        let journal = DaemonJournal::new(dir.path());
        journal.retain(&notification("valid")).unwrap();
        let mut operation = run_intent("quarantine-blocked");
        let path = journal.path_for_entry(&operation.entry_id).unwrap();
        fs::write(&path, b"{broken").unwrap();
        let quarantine = journal.directory().join(format!(
            "corrupt-{}",
            path.file_name().unwrap().to_string_lossy()
        ));
        fs::create_dir(&quarantine).unwrap();
        fs::write(quarantine.join("occupied"), b"keep").unwrap();
        let restarted = DaemonJournal::new(dir.path());
        restarted.initialize().unwrap();
        assert!(
            restarted
                .acknowledge(&JournalAckParams {
                    entry_id: operation.entry_id.clone()
                })
                .unwrap()
                .acknowledged
        );
        operation.outcome = Some(Ok(serde_json::json!({"exit_code":0})));
        assert!(restarted
            .finish_operation(&operation)
            .unwrap_err()
            .to_string()
            .contains("intent is missing"));
        operation.outcome = None;
        restarted
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
        assert!(restarted
            .operation(&operation.fence.operation_id)
            .unwrap()
            .is_some());
        operation.outcome = Some(Ok(serde_json::json!({"exit_code":0})));
        restarted.finish_operation(&operation).unwrap();
        assert_eq!(restarted.pending().unwrap().len(), 2);
    }

    #[test]
    fn startup_scratch_removal_and_acknowledged_entry_sync_errors_do_not_stop_replay() {
        let dir = tempfile::tempdir().unwrap();
        let journal = DaemonJournal::new(dir.path());
        journal.retain(&notification("valid")).unwrap();
        let mut operation = run_intent("old-acknowledged");
        operation.outcome = Some(Ok(serde_json::json!({"exit_code":0})));
        operation.acknowledged = true;
        fs::write(
            journal.path_for_entry(&operation.entry_id).unwrap(),
            serde_json::to_vec(&JournalEntry::Operation { operation }).unwrap(),
        )
        .unwrap();
        fs::create_dir(journal.directory().join(".journal.tmp.blocked")).unwrap();
        let restarted = DaemonJournal::new(dir.path());
        restarted
            .fail_next_directory_sync
            .store(true, Ordering::Relaxed);
        restarted.initialize().unwrap();
        assert_eq!(restarted.pending().unwrap().len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn startup_acknowledged_entry_removal_failure_is_skipped() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let journal = DaemonJournal::new(dir.path());
        journal.retain(&notification("valid")).unwrap();
        let mut operation = run_intent("cannot-remove");
        operation.outcome = Some(Ok(serde_json::json!({"exit_code":0})));
        operation.acknowledged = true;
        fs::write(
            journal.path_for_entry(&operation.entry_id).unwrap(),
            serde_json::to_vec(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap(),
        )
        .unwrap();
        let permissions = fs::metadata(journal.directory()).unwrap().permissions();
        fs::set_permissions(journal.directory(), fs::Permissions::from_mode(0o500)).unwrap();
        let restarted = DaemonJournal::new(dir.path());
        let result = restarted.initialize();
        fs::set_permissions(journal.directory(), permissions).unwrap();
        result.unwrap();
        assert_eq!(restarted.pending().unwrap().len(), 1);
        assert!(
            restarted
                .acknowledge(&JournalAckParams {
                    entry_id: operation.entry_id
                })
                .unwrap()
                .acknowledged
        );
    }

    #[test]
    fn completion_headroom_survives_a_full_shared_journal_without_logs() {
        let dir = tempfile::tempdir().unwrap();
        let journal = DaemonJournal::with_limits(dir.path(), 16, 16 * 1024);
        let mut operation = run_intent("reserved-result");
        journal
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
        let path = journal.path_for_entry(&operation.entry_id).unwrap();
        let usage = journal.current_usage().unwrap().1;
        let reservation = run_result_reservation(&JournalEntry::Operation {
            operation: operation.clone(),
        })
        .unwrap();
        journal
            .save_workspace_state(
                &"r".repeat((journal.max_bytes - usage - reservation - 2) as usize),
            )
            .unwrap();
        assert!(journal.retain(&notification("no-room")).is_err());
        operation.outcome = Some(Ok(serde_json::json!({
            "entry_id":operation.entry_id, "operation_id":operation.fence.operation_id,
            "exit_code":1, "stdout":"x".repeat(MAX_CI_LOG_BYTES), "stderr":"failure last line",
            "duration_ms":7, "timed_out":false, "stdout_truncated":false, "stderr_truncated":false
        })));
        let result = journal
            .finish_operation(&operation)
            .unwrap()
            .outcome
            .unwrap()
            .unwrap();
        assert_eq!(result["exit_code"], 1);
        assert_eq!(result["entry_id"], operation.entry_id);
        assert_eq!(result["stdout_truncated"], true);
        assert!(journal.current_usage().unwrap().1 <= journal.max_bytes);
        assert_eq!(
            journal
                .operation("reserved-result")
                .unwrap()
                .unwrap()
                .outcome
                .unwrap()
                .unwrap(),
            result
        );
        // A budget below even the marker retains only metadata and flags.
        let mut receipt = JournalEntry::Operation { operation };
        fit_run_receipt(&mut receipt, 0).unwrap();
        let JournalEntry::Operation { operation } = receipt else {
            unreachable!()
        };
        assert_eq!(operation.outcome.unwrap().unwrap()["stdout"], "");
        assert!(path.exists());
    }

    #[test]
    fn usage_tracks_rename_and_removal_even_when_directory_sync_fails() {
        let dir = tempfile::tempdir().unwrap();
        let journal = DaemonJournal::new(dir.path());
        journal.initialize().unwrap();
        journal
            .fail_next_directory_sync
            .store(true, Ordering::Relaxed);
        assert!(journal
            .retain(&notification("renamed"))
            .unwrap_err()
            .to_string()
            .contains("sync failure"));
        assert_eq!(journal.current_usage().unwrap().0, 1);
        let path = journal.path_for_entry("renamed").unwrap();
        assert_eq!(
            journal.current_usage().unwrap().1,
            fs::metadata(&path).unwrap().len()
        );
        journal
            .fail_next_directory_sync
            .store(true, Ordering::Relaxed);
        assert!(journal
            .acknowledge(&JournalAckParams {
                entry_id: "renamed".into()
            })
            .is_err());
        assert!(!path.exists());
        assert_eq!(journal.current_usage().unwrap(), (0, 0));
        journal.retain(&notification("next")).unwrap();
        assert_eq!(journal.current_usage().unwrap().0, 1);
    }

    #[test]
    fn terminal_output_redaction_preserves_accounting_identity() {
        let dir = tempfile::tempdir().unwrap();
        let journal = DaemonJournal::new(dir.path());
        let mut report = notification("report-1");
        report.summary = Some("printed 1".into());
        report.error = Some("error 1".into());
        report.plan_text = Some("- [ ] secret 1".into());
        report
            .outbox_entries
            .push(api_types::ExecutionOutboxEntry::Worklog {
                position: "1".into(),
                kind: api_types::ExecutionOutboxWorklogKind::Progress,
                summary: "1".into(),
            });
        sanitize_terminal_report(
            &mut report,
            &std::collections::BTreeMap::from([("CI".into(), "1".into())]),
        );
        journal.retain(&report).unwrap();
        let JournalEntry::Terminal { report: stored } = journal.pending().unwrap().remove(0) else {
            unreachable!()
        };
        assert_eq!(stored.terminal_report_id, "report-1");
        assert_eq!(stored.execution_id, "execution-1");
        assert_eq!(stored.summary.as_deref(), Some("printed [REDACTED]"));
        assert_eq!(stored.error.as_deref(), Some("error [REDACTED]"));
        assert_eq!(stored.plan_text.as_deref(), Some("- [ ] secret [REDACTED]"));
        let api_types::ExecutionOutboxEntry::Worklog {
            position, summary, ..
        } = &stored.outbox_entries[0]
        else {
            unreachable!()
        };
        assert_eq!(position, "1");
        assert_eq!(summary, "[REDACTED]");
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
