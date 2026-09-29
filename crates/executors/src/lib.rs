#![forbid(unsafe_code)]

pub mod adapter;
pub mod command;
pub mod config;
pub mod effective_policy;
pub mod environment;
pub mod log_reader;
pub mod log_schema;
pub mod log_writer;
pub mod shell;

pub use adapter::{
    AdapterExecutor, AdapterRegistry, AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter,
    DiscoverContext, DiscoveredOptions, ExecutionOverrides, ExecutorKind, FallbackExecutor,
    DEFAULT_ACCOUNT_COOLDOWN,
};
pub use command::{build_shell_command_plan, ShellCommandPlan};
pub use config::{
    account_key, build_ordered_fallback_routing, candidate_config_from_snapshot, candidate_key,
    candidate_key_from_snapshot, deserialize_config, merge_overrides, resolve_config_value,
    ClaudeCodeConfig, CodexConfig, CommandOverrides, CursorConfig, EmbeddedConfig,
    ExecutorCandidate, ExecutorRouting, GeminiConfig, NullConfig, OpencodeConfig, PermissionPolicy,
    RouteAttempt, RouteAttemptOutcome, ShellConfig, SmithConfig, FALLBACKS_CONFIG_KEY,
    ROUTING_POLICY_ORDERED_FALLBACK_V1, ROUTING_SNAPSHOT_KEY,
};
pub use log_reader::{LogReadResult, LogReader};
pub use log_schema::{LogEntry, LogKind, LogStream};
pub use log_writer::LogWriter;
pub use shell::{is_pid_alive, ShellExecutor};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const READ_ONLY_WORKTREE_KEY: &str = "_forge_read_only_worktree";

/// Snapshot key naming the Task role an execution claims.
///
/// The embedded runtime matches this against the execution's own role before
/// it opens a native session, so every producer of an execution snapshot must
/// use the same key.
pub const TASK_ROLE_CONFIG_KEY: &str = "_forge_task_role";

/// Stamp the server-selected Task role onto an in-memory executor config.
///
/// This is runtime authority, not authored profile configuration. Callers
/// must add it after loading the immutable execution snapshot so a profile
/// cannot grant itself a different role or execution boundary.
pub fn mark_task_role(config: &mut serde_json::Value, role: &str) {
    if let Some(object) = config.as_object_mut() {
        object.insert(
            TASK_ROLE_CONFIG_KEY.to_owned(),
            serde_json::Value::String(role.to_owned()),
        );
    }
}

/// Return the server-selected Task role carried by an in-memory config.
pub fn task_role(config: &serde_json::Value) -> Option<&str> {
    config
        .get(TASK_ROLE_CONFIG_KEY)
        .and_then(serde_json::Value::as_str)
        .filter(|role| !role.trim().is_empty())
}

/// Mark an executor config so the runtime restores the worktree after execution.
pub fn mark_worktree_read_only(config: &mut serde_json::Value) {
    if let Some(object) = config.as_object_mut() {
        object.insert(
            READ_ONLY_WORKTREE_KEY.to_owned(),
            serde_json::Value::Bool(true),
        );
    }
}

/// Whether the runtime must discard all tracked and untracked worktree changes.
pub fn is_worktree_read_only(config: &serde_json::Value) -> bool {
    config
        .get(READ_ONLY_WORKTREE_KEY)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// Environment variable naming an execution's outbox directory.
pub const FORGE_OUTBOX_ENV: &str = "FORGE_OUTBOX";
/// Environment variable naming the per-execution plan file that Forge may publish.
pub const FORGE_PLAN_PATH_ENV: &str = "FORGE_PLAN_PATH";
/// Worklog entries a CLI harness appends to its outbox, one JSON object per line.
pub const OUTBOX_WORKLOG_FILE: &str = "worklog.jsonl";
/// Evidence a CLI harness registers in its outbox, one JSON object per line.
pub const OUTBOX_EVIDENCE_FILE: &str = "evidence.jsonl";
/// Plan candidate produced by an authorized CLI planning or implementation role.
pub const OUTBOX_PLAN_FILE: &str = "plan.md";
const OUTBOX_DIR_NAME: &str = ".forge-outbox";

/// A Task execution outbox could not be resolved without crossing the Task
/// directory boundary.
#[derive(Debug, thiserror::Error)]
pub enum ExecutionOutboxError {
    #[error("execution id is not safe for an outbox path")]
    InvalidExecutionId,
    #[error("Task worktree has no parent directory: {0}")]
    MissingTaskRoot(PathBuf),
    #[error("execution outbox path is not a real directory: {0}")]
    InvalidDirectory(PathBuf),
    #[error("execution outbox path escapes the Task directory: {0}")]
    PathEscape(PathBuf),
    #[error("execution outbox I/O failed: {0}")]
    Io(#[from] io::Error),
}

/// Whether a Task role may publish or update the canonical plan artifact.
#[must_use]
pub fn task_role_can_write_plan(role: Option<&str>) -> bool {
    matches!(role, Some("planner" | "worker" | "coder" | "executor"))
}

/// Whether an execution id is safe to use as exactly one path component.
#[must_use]
pub fn execution_id_is_path_safe(execution_id: &str) -> bool {
    !execution_id.is_empty()
        && execution_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// The directory a CLI harness writes worklog and evidence into.
///
/// A CLI harness has no Forge tool channel, so it reports through files that
/// Forge ingests when the execution ends. The directory is a sibling of the
/// Git worktree — inside the Task directory but outside the checkout — so
/// nothing written there can be committed, and a read-only reviewer still has
/// one place it may write. Both the server and the daemon derive it from the
/// same two values, so no transport change is needed.
#[must_use]
pub fn execution_outbox_path(worktree_path: &Path, execution_id: &str) -> Option<PathBuf> {
    let (_, outbox_root, outbox) = outbox_components(worktree_path, execution_id).ok()?;
    let canonical_task_root = validate_task_root(worktree_path).ok()?;
    if let Some(canonical_outbox_root) =
        validate_optional_child_directory(&outbox_root, &canonical_task_root).ok()?
    {
        validate_optional_child_directory(&outbox, &canonical_outbox_root).ok()?;
    }
    Some(outbox)
}

/// Create and validate the two outbox directories without ever using a
/// recursive create through agent-controlled path components.
///
/// Both `.forge-outbox` and the execution directory must be real, direct
/// children of their expected parent. Symlinks and other file types fail
/// closed. Callers should use the returned path instead of reconstructing it.
pub fn prepare_execution_outbox(
    worktree_path: &Path,
    execution_id: &str,
) -> Result<PathBuf, ExecutionOutboxError> {
    let (_, outbox_root, outbox) = outbox_components(worktree_path, execution_id)?;
    let canonical_task_root = validate_task_root(worktree_path)?;
    let canonical_outbox_root = ensure_child_directory(&outbox_root, &canonical_task_root)?;
    ensure_child_directory(&outbox, &canonical_outbox_root)?;
    Ok(outbox)
}

/// Resolve an already-created execution outbox, returning `None` when it is
/// absent and an error when any component is a symlink, a non-directory, or
/// resolves outside the Task directory.
pub fn existing_execution_outbox(
    worktree_path: &Path,
    execution_id: &str,
) -> Result<Option<PathBuf>, ExecutionOutboxError> {
    let (_, outbox_root, outbox) = outbox_components(worktree_path, execution_id)?;
    let canonical_task_root = validate_task_root(worktree_path)?;
    let Some(canonical_outbox_root) =
        validate_optional_child_directory(&outbox_root, &canonical_task_root)?
    else {
        return Ok(None);
    };
    let Some(_) = validate_optional_child_directory(&outbox, &canonical_outbox_root)? else {
        return Ok(None);
    };
    Ok(Some(outbox))
}

/// Remove one execution's outbox only after validating both directory
/// components against the Task root. `remove_dir_all` itself does not follow
/// a leaf symlink; the checks here also reject a symlinked intermediate root.
pub fn remove_execution_outbox(
    worktree_path: &Path,
    execution_id: &str,
) -> Result<(), ExecutionOutboxError> {
    let Some(outbox) = existing_execution_outbox(worktree_path, execution_id)? else {
        return Ok(());
    };
    match fs::remove_dir_all(outbox) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ExecutionOutboxError::Io(error)),
    }
}

fn outbox_components(
    worktree_path: &Path,
    execution_id: &str,
) -> Result<(PathBuf, PathBuf, PathBuf), ExecutionOutboxError> {
    if !execution_id_is_path_safe(execution_id) {
        return Err(ExecutionOutboxError::InvalidExecutionId);
    }
    let task_root = worktree_path
        .parent()
        .ok_or_else(|| ExecutionOutboxError::MissingTaskRoot(worktree_path.to_path_buf()))?
        .to_path_buf();
    let outbox_root = task_root.join(OUTBOX_DIR_NAME);
    let outbox = outbox_root.join(execution_id);
    Ok((task_root, outbox_root, outbox))
}

fn validate_task_root(worktree_path: &Path) -> Result<PathBuf, ExecutionOutboxError> {
    let task_root = worktree_path
        .parent()
        .ok_or_else(|| ExecutionOutboxError::MissingTaskRoot(worktree_path.to_path_buf()))?;
    ensure_real_directory(task_root)?;
    ensure_real_directory(worktree_path)?;
    let canonical_task_root = fs::canonicalize(task_root)?;
    let canonical_worktree = fs::canonicalize(worktree_path)?;
    if canonical_worktree.parent() != Some(canonical_task_root.as_path()) {
        return Err(ExecutionOutboxError::PathEscape(canonical_worktree));
    }
    Ok(canonical_task_root)
}

fn ensure_real_directory(path: &Path) -> Result<(), ExecutionOutboxError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(ExecutionOutboxError::InvalidDirectory(path.to_path_buf()));
    }
    Ok(())
}

fn validate_optional_child_directory(
    path: &Path,
    canonical_parent: &Path,
) -> Result<Option<PathBuf>, ExecutionOutboxError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => return Err(ExecutionOutboxError::InvalidDirectory(path.to_path_buf())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ExecutionOutboxError::Io(error)),
    }
    let canonical = fs::canonicalize(path)?;
    if canonical.parent() != Some(canonical_parent) {
        return Err(ExecutionOutboxError::PathEscape(canonical));
    }
    Ok(Some(canonical))
}

fn ensure_child_directory(
    path: &Path,
    canonical_parent: &Path,
) -> Result<PathBuf, ExecutionOutboxError> {
    if let Some(canonical) = validate_optional_child_directory(path, canonical_parent)? {
        return Ok(canonical);
    }

    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(ExecutionOutboxError::Io(error)),
    }
    validate_optional_child_directory(path, canonical_parent)?.ok_or_else(|| {
        ExecutionOutboxError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            "execution outbox directory disappeared during creation",
        ))
    })
}

/// Context passed to an executor when running a task.
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    pub task_id: String,
    pub execution_id: String,
    pub worktree_path: String,
    pub description: String,
    pub agent_config: serde_json::Value,
    pub logs_path: String,
    pub heartbeat_interval_seconds: u64,
    pub max_turns: Option<u32>,
    pub log_sender: Option<tokio::sync::mpsc::UnboundedSender<LogEntry>>,
}

/// Durable admission callback invoked immediately before an executor crosses
/// the external-provider boundary.  The callback lives in this neutral crate
/// so adapters and services can agree on the lifecycle without coupling the
/// executor layer to SQLite or pricing.
#[async_trait]
pub trait ProviderCallAdmission: Send + Sync {
    async fn before_provider_call(
        &self,
        ctx: &ExecutionContext,
        candidate_key: &str,
        attempt_ordinal: u32,
    ) -> Result<(), ExecutorError>;
}

#[cfg(test)]
mod worktree_policy_tests {
    use super::*;

    #[test]
    fn read_only_worktree_policy_is_opt_in() {
        let mut config = serde_json::json!({ "executor_type": "claude_code" });
        assert!(!is_worktree_read_only(&config));

        mark_worktree_read_only(&mut config);

        assert!(is_worktree_read_only(&config));
        assert_eq!(config["executor_type"], "claude_code");
    }
}

#[cfg(test)]
mod outbox_path_tests {
    use super::*;

    #[test]
    fn outbox_requires_a_real_direct_child_worktree() {
        let temp = tempfile::tempdir().expect("temp dir");
        let worktree = temp.path().join("task").join("repo");
        fs::create_dir_all(&worktree).expect("worktree creates");

        let outbox = prepare_execution_outbox(&worktree, "exec-1").expect("outbox creates");

        assert_eq!(
            outbox,
            temp.path()
                .join("task")
                .join(".forge-outbox")
                .join("exec-1")
        );
        assert_eq!(
            existing_execution_outbox(&worktree, "exec-1").expect("outbox validates"),
            Some(outbox.clone())
        );
        remove_execution_outbox(&worktree, "exec-1").expect("outbox removes");
        assert!(!outbox.exists());
    }

    #[cfg(unix)]
    #[test]
    fn whole_outbox_root_symlink_is_rejected_without_touching_target() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("temp dir");
        let outside = tempfile::tempdir().expect("outside dir");
        let worktree = temp.path().join("task").join("repo");
        fs::create_dir_all(&worktree).expect("worktree creates");
        fs::write(outside.path().join("keep"), "untouched").expect("outside marker");
        symlink(
            outside.path(),
            temp.path().join("task").join(OUTBOX_DIR_NAME),
        )
        .expect("outbox-root symlink creates");

        assert!(execution_outbox_path(&worktree, "exec-1").is_none());
        assert!(matches!(
            prepare_execution_outbox(&worktree, "exec-1"),
            Err(ExecutionOutboxError::InvalidDirectory(_))
        ));
        assert!(matches!(
            remove_execution_outbox(&worktree, "exec-1"),
            Err(ExecutionOutboxError::InvalidDirectory(_))
        ));
        assert_eq!(
            fs::read_to_string(outside.path().join("keep")).expect("outside marker reads"),
            "untouched"
        );
        assert!(!outside.path().join("exec-1").exists());
    }

    #[cfg(unix)]
    #[test]
    fn per_execution_symlink_is_rejected_without_touching_target() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("temp dir");
        let outside = tempfile::tempdir().expect("outside dir");
        let worktree = temp.path().join("task").join("repo");
        let outbox_root = temp.path().join("task").join(OUTBOX_DIR_NAME);
        fs::create_dir_all(&worktree).expect("worktree creates");
        fs::create_dir(&outbox_root).expect("outbox root creates");
        fs::write(outside.path().join("keep"), "untouched").expect("outside marker");
        symlink(outside.path(), outbox_root.join("exec-1")).expect("execution symlink creates");

        assert!(execution_outbox_path(&worktree, "exec-1").is_none());
        assert!(matches!(
            prepare_execution_outbox(&worktree, "exec-1"),
            Err(ExecutionOutboxError::InvalidDirectory(_))
        ));
        assert!(matches!(
            remove_execution_outbox(&worktree, "exec-1"),
            Err(ExecutionOutboxError::InvalidDirectory(_))
        ));
        assert_eq!(
            fs::read_to_string(outside.path().join("keep")).expect("outside marker reads"),
            "untouched"
        );
    }
}

/// Telemetry disposition for one provider attempt.
///
/// `Metered` is deliberately independent from the values in [`UsageCounters`]:
/// a producer that authoritatively reports four zero counters sets every field
/// to `Some(0)`, while a producer that supplies no trustworthy counters uses
/// `Unmetered` with every field set to `None`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageTelemetryState {
    Metered,
    #[default]
    Unmetered,
    Pending,
    Unsettled,
}

/// Four disjoint token counters observed for one provider report.
///
/// A missing field is unknown; it is never silently converted to zero. The
/// native runtime and adapters that define all four fields can therefore
/// preserve explicit zeros without making a no-telemetry result look free.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageCounters {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
}

impl UsageCounters {
    /// Counters for an authoritative all-zero report.
    pub const fn explicit_zero() -> Self {
        Self {
            input_tokens: Some(0),
            output_tokens: Some(0),
            cache_read_tokens: Some(0),
            cache_write_tokens: Some(0),
        }
    }

    /// Whether at least one counter was authoritatively supplied.
    pub const fn has_any(&self) -> bool {
        self.input_tokens.is_some()
            || self.output_tokens.is_some()
            || self.cache_read_tokens.is_some()
            || self.cache_write_tokens.is_some()
    }

    /// Whether all supplied counters are zero and all four counters are known.
    pub const fn is_explicit_zero(&self) -> bool {
        matches!(
            (
                self.input_tokens,
                self.output_tokens,
                self.cache_read_tokens,
                self.cache_write_tokens
            ),
            (Some(0), Some(0), Some(0), Some(0))
        )
    }

    /// Add a documented non-overlapping delta without turning an omitted
    /// bucket into a fabricated zero. Overflow is returned explicitly so a
    /// caller can preserve the reports separately instead of silently
    /// truncating a provider's usage.
    pub fn checked_add_delta(&mut self, other: &Self) -> Result<(), UsageCounterMergeError> {
        fn add(
            left: Option<u64>,
            right: Option<u64>,
        ) -> Result<Option<u64>, UsageCounterMergeError> {
            match (left, right) {
                (None, None) => Ok(None),
                (Some(_), None) | (None, Some(_)) => Err(UsageCounterMergeError::MissingBucket),
                (Some(left), Some(right)) => left
                    .checked_add(right)
                    .map(Some)
                    .ok_or(UsageCounterMergeError::Overflow),
            }
        }

        let input_tokens = add(self.input_tokens, other.input_tokens)?;
        let output_tokens = add(self.output_tokens, other.output_tokens)?;
        let cache_read_tokens = add(self.cache_read_tokens, other.cache_read_tokens)?;
        let cache_write_tokens = add(self.cache_write_tokens, other.cache_write_tokens)?;
        self.input_tokens = input_tokens;
        self.output_tokens = output_tokens;
        self.cache_read_tokens = cache_read_tokens;
        self.cache_write_tokens = cache_write_tokens;
        Ok(())
    }
}

/// A checked token-counter merge could not produce one truthful aggregate.
/// Keeping overflow and missing buckets explicit prevents either condition
/// from being reported as a valid, wrapped, saturated, or synthetic count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageCounterMergeError {
    Overflow,
    MissingBucket,
}

impl std::fmt::Display for UsageCounterMergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Overflow => "usage counter overflow",
            Self::MissingBucket => "usage counter bucket is missing",
        })
    }
}

impl std::error::Error for UsageCounterMergeError {}

/// One adapter/provider report. A fallback execution carries one or more of
/// these records rather than a flattened logical total.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageReport {
    /// Stable producer identity. Fallback fills a deterministic value when an
    /// adapter has no provider request identifier.
    pub report_id: String,
    /// Optional provider request identity when the producer exposes one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Ordered report identity within a candidate invocation.
    #[serde(default)]
    pub report_sequence: u32,
    /// Immutable route candidate identity, assigned by the fallback layer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_key: Option<String>,
    /// Route attempt ordinal. Skipped candidates do not receive reports.
    #[serde(default)]
    pub attempt_ordinal: u32,
    /// Actual provider identity, never inferred from [`ExecutorKind`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    /// Actual provider model identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    pub counters: UsageCounters,
    pub telemetry_state: UsageTelemetryState,
    /// Optional request context evidence used by exact pricing tiers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_tier: Option<String>,
    /// Provider/runtime-reported amount. It is kept separate from estimates;
    /// the services/ledger layer must never add an estimate for this report.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported_cost_usd: Option<String>,
    /// Route outcome, when the fallback layer has classified it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<config::RouteAttemptOutcome>,
    /// True when this report came from an adapter failure after observing a
    /// partial provider response.
    #[serde(default)]
    pub partial: bool,
}

impl Default for UsageReport {
    fn default() -> Self {
        Self::unmetered(String::new())
    }
}

impl UsageReport {
    /// Build a metered report. `Some(0)` values are preserved as explicit
    /// telemetry, including an all-zero report.
    pub fn metered(report_id: impl Into<String>, counters: UsageCounters) -> Self {
        let telemetry_state = if counters.has_any() {
            UsageTelemetryState::Metered
        } else {
            UsageTelemetryState::Unmetered
        };
        Self {
            report_id: report_id.into(),
            counters,
            telemetry_state,
            ..Self::empty()
        }
    }

    /// Build an unmetered report without synthetic zero counters.
    pub fn unmetered(report_id: impl Into<String>) -> Self {
        Self {
            report_id: report_id.into(),
            ..Self::empty()
        }
    }

    fn empty() -> Self {
        Self {
            report_id: String::new(),
            request_id: None,
            report_sequence: 0,
            candidate_key: None,
            attempt_ordinal: 0,
            provider_id: None,
            model_id: None,
            counters: UsageCounters::default(),
            telemetry_state: UsageTelemetryState::Unmetered,
            context_tokens: None,
            selected_tier: None,
            reported_cost_usd: None,
            outcome: None,
            partial: false,
        }
    }

    /// Assign route identity while preserving a provider-supplied report ID.
    pub fn for_candidate(
        mut self,
        execution_id: &str,
        candidate_key: &str,
        attempt_ordinal: u32,
        report_sequence: u32,
    ) -> Self {
        if self.report_id.trim().is_empty() {
            self.report_id = stable_report_id(
                execution_id,
                candidate_key,
                attempt_ordinal,
                report_sequence,
            );
        }
        self.candidate_key = Some(candidate_key.to_owned());
        self.attempt_ordinal = attempt_ordinal;
        self.report_sequence = report_sequence;
        self.normalize_telemetry();
        self
    }

    /// Set configured identity only when the provider did not report an
    /// actual identity. This never substitutes the executor family.
    pub fn fill_identity(&mut self, provider_id: Option<&str>, model_id: Option<&str>) {
        if self.provider_id.is_none() {
            self.provider_id = provider_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
        }
        if self.model_id.is_none() {
            self.model_id = model_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
        }
    }

    /// Keep telemetry state consistent with sparse counters.
    pub fn normalize_telemetry(&mut self) {
        if self.telemetry_state == UsageTelemetryState::Metered && !self.counters.has_any() {
            self.telemetry_state = UsageTelemetryState::Unmetered;
        }
        if self.telemetry_state != UsageTelemetryState::Metered {
            self.counters = UsageCounters::default();
        }
    }

    /// Add a documented non-overlapping delta into this report.
    pub fn merge_delta(&mut self, other: &Self) -> Result<(), UsageCounterMergeError> {
        self.counters.checked_add_delta(&other.counters)?;
        if self.request_id.is_none() {
            self.request_id = other.request_id.clone();
        }
        if self.provider_id.is_none() {
            self.provider_id = other.provider_id.clone();
        }
        if self.model_id.is_none() {
            self.model_id = other.model_id.clone();
        }
        if self.context_tokens.is_none() {
            self.context_tokens = other.context_tokens;
        }
        if self.selected_tier.is_none() {
            self.selected_tier = other.selected_tier.clone();
        }
        if self.reported_cost_usd.is_none() {
            self.reported_cost_usd = other.reported_cost_usd.clone();
        }
        self.partial |= other.partial;
        if other.outcome.is_some() {
            self.outcome = other.outcome;
        }
        self.telemetry_state = if self.counters.has_any() {
            UsageTelemetryState::Metered
        } else {
            UsageTelemetryState::Unmetered
        };
        Ok(())
    }
}

/// Stable fallback report identity for adapters that expose no request ID.
pub fn stable_report_id(
    execution_id: &str,
    candidate_key: &str,
    attempt_ordinal: u32,
    report_sequence: u32,
) -> String {
    format!("forge:{execution_id}:{candidate_key}:{attempt_ordinal}:{report_sequence}")
}

/// Structured disposition of a failed execution. `TaskFailed` keeps the
/// existing budgeted retry semantics; `ExecutorUnavailable` means no
/// executor candidate could run (quota, missing CLI, or auth) and must not
/// consume task retry budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionFailureClass {
    TaskFailed,
    ExecutorUnavailable,
}

/// The candidate that actually ran an execution, as resolved by the
/// fallback layer. Persisted by the service layer for sticky selection.
#[derive(Debug, Clone)]
pub struct ResolvedExecutorCandidate {
    pub candidate_key: String,
    pub executor_type: ExecutorKind,
    pub config: serde_json::Value,
}

/// Result from an executor run.
#[derive(Debug, Clone, Default)]
pub struct ExecutionResult {
    pub status: ExecutionOutcome,
    pub after_sha: Option<String>,
    pub agent_session_id: Option<String>,
    /// Complete assistant response when an interactive executor exposes one.
    /// Task projections continue to use the bounded `summary`; Agent Chat may
    /// consume this field after applying its own message admission limits.
    pub assistant_output: Option<String>,
    pub summary: Option<String>,
    pub error: Option<String>,
    /// One report per provider request/candidate attempt. This vector is
    /// intentionally empty only when no adapter/provider call was made.
    pub usage_reports: Vec<UsageReport>,
    pub failure_class: Option<ExecutionFailureClass>,
    pub retry_after: Option<std::time::Duration>,
    pub resolved_candidate: Option<ResolvedExecutorCandidate>,
    /// Per-candidate attempt outcomes, in attempt order (route provenance).
    pub route_attempts: Vec<config::RouteAttempt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ExecutionOutcome {
    Completed,
    /// Default so `..Default::default()` in constructors can never fabricate
    /// a success.
    #[default]
    Failed,
    Cancelled,
}

#[async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError>;

    /// Execute with an optional lifecycle callback. Implementations that do
    /// not expose a candidate-level provider boundary may use the default;
    /// routed executors override this to invoke the callback for each actual
    /// candidate call. Keeping this default preserves existing third-party
    /// TaskExecutor implementations and test doubles.
    async fn execute_with_provider_call_admission(
        &self,
        ctx: ExecutionContext,
        _admission: Arc<dyn ProviderCallAdmission>,
    ) -> Result<ExecutionResult, ExecutorError> {
        self.execute(ctx).await
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The candidate's quota or rate limit is exhausted. Candidate-level
    /// control flow for the fallback layer only — never the terminal channel.
    /// Carries any reports the candidate accumulated before hitting the cap so
    /// fallback chains keep accounting truthful.
    #[error("usage exhausted")]
    UsageExhausted {
        retry_after: Option<std::time::Duration>,
        usage_reports: Vec<UsageReport>,
    },

    /// The candidate's CLI is missing or unauthenticated. Candidate-level
    /// control flow for the fallback layer only — never the terminal channel.
    #[error("executor unavailable: {reason}")]
    Unavailable {
        reason: String,
        usage_reports: Vec<UsageReport>,
    },

    #[error("executor error: {0}")]
    Other(String),
}

impl ExecutorError {
    /// Availability failures are the only errors that may advance a
    /// fallback chain.
    pub fn is_availability(&self) -> bool {
        matches!(self, Self::UsageExhausted { .. } | Self::Unavailable { .. })
    }

    /// Reports observed before an availability error, if any.
    pub fn usage_reports(&self) -> &[UsageReport] {
        match self {
            Self::UsageExhausted { usage_reports, .. }
            | Self::Unavailable { usage_reports, .. } => usage_reports,
            _ => &[],
        }
    }

    /// Construct an unavailable candidate error with no usage report.
    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self::Unavailable {
            reason: reason.into(),
            usage_reports: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn log_write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.jsonl");

        let mut writer = LogWriter::new(&log_path, "exec-1".to_string(), 1024 * 1024);

        for i in 0..100 {
            writer
                .write(
                    LogKind::Stdout,
                    LogStream::Main,
                    serde_json::json!({"line": format!("line {i}")}),
                )
                .await
                .unwrap();
        }

        assert_eq!(writer.sequence(), 100);

        // Read from_sequence=50, limit=10
        let result = LogReader::read(&log_path, 50, 10).await.unwrap();
        assert_eq!(result.entries.len(), 10);
        assert_eq!(result.entries[0].sequence, 50);
        assert_eq!(result.entries[9].sequence, 59);
        assert!(result.has_more);
        assert_eq!(result.next_sequence, Some(60));

        // Tail last 5
        let tail_result = LogReader::tail(&log_path, 5).await.unwrap();
        assert_eq!(tail_result.entries.len(), 5);
        assert_eq!(tail_result.entries[0].sequence, 95);
        assert_eq!(tail_result.entries[4].sequence, 99);
        assert!(tail_result.has_more);
        assert_eq!(tail_result.next_sequence, Some(100));

        writer
            .write(
                LogKind::SessionInfo,
                LogStream::Main,
                serde_json::json!({"method": "thread/started"}),
            )
            .await
            .unwrap();
        writer
            .write(
                LogKind::User,
                LogStream::Main,
                serde_json::json!({"text": "follow-up"}),
            )
            .await
            .unwrap();
        for i in 0..10 {
            writer
                .write(
                    LogKind::Stdout,
                    LogStream::Main,
                    serde_json::json!({"line": format!("follow-up line {i}")}),
                )
                .await
                .unwrap();
        }

        let turn_tail_result = LogReader::tail(&log_path, 5).await.unwrap();
        assert_eq!(turn_tail_result.entries[0].sequence, 101);
        assert!(turn_tail_result.has_more);
    }

    #[tokio::test]
    async fn log_read_empty_delta_preserves_requested_next_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("empty-delta.jsonl");

        let mut writer = LogWriter::new(&log_path, "exec-1".to_string(), 1024 * 1024);
        writer
            .write(
                LogKind::Stdout,
                LogStream::Main,
                serde_json::json!({"line": "hello"}),
            )
            .await
            .unwrap();

        let result = LogReader::read(&log_path, 1, 10).await.unwrap();
        assert!(result.entries.is_empty());
        assert!(!result.has_more);
        assert_eq!(result.next_sequence, Some(1));
    }

    #[tokio::test]
    async fn log_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("truncated.jsonl");

        // Very small max to trigger truncation quickly
        let mut writer = LogWriter::new(&log_path, "exec-2".to_string(), 500);

        for i in 0..100 {
            writer
                .write(
                    LogKind::Stdout,
                    LogStream::Main,
                    serde_json::json!({"line": format!("line {i}")}),
                )
                .await
                .unwrap();
        }

        assert!(writer.is_truncated());
        assert!(writer.sequence() < 100); // Should have stopped early

        // Last entry should be truncated
        let result = LogReader::tail(&log_path, 1).await.unwrap();
        assert_eq!(result.entries.len(), 1);
        assert!(result.entries[0].truncated);
    }

    #[tokio::test]
    async fn log_writer_appends_after_existing_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("append.jsonl");

        let mut first = LogWriter::new(&log_path, "exec-1".to_string(), 1024 * 1024);
        first
            .write(
                LogKind::User,
                LogStream::Main,
                serde_json::json!({"text": "first turn"}),
            )
            .await
            .unwrap();
        first
            .write(
                LogKind::Assistant,
                LogStream::Main,
                serde_json::json!({"text": "first response"}),
            )
            .await
            .unwrap();

        let mut second = LogWriter::new(&log_path, "exec-1".to_string(), 1024 * 1024);
        assert_eq!(second.sequence(), 2);
        second
            .write(
                LogKind::User,
                LogStream::Main,
                serde_json::json!({"text": "follow up"}),
            )
            .await
            .unwrap();

        let result = LogReader::read(&log_path, 0, 10).await.unwrap();
        let sequences = result
            .entries
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![0, 1, 2]);
    }

    #[test]
    fn explicit_zero_is_metered_but_missing_telemetry_is_unmetered() {
        let explicit_zero = UsageReport::metered("provider-report", UsageCounters::explicit_zero());
        assert_eq!(explicit_zero.telemetry_state, UsageTelemetryState::Metered);
        assert!(explicit_zero.counters.is_explicit_zero());

        let missing = UsageReport::unmetered("no-provider-report");
        assert_eq!(missing.telemetry_state, UsageTelemetryState::Unmetered);
        assert!(!missing.counters.has_any());
    }

    #[test]
    fn checked_usage_addition_reports_overflow_without_mutating_counters() {
        let mut counters = UsageCounters {
            output_tokens: Some(u64::MAX),
            ..Default::default()
        };
        let delta = UsageCounters {
            output_tokens: Some(1),
            ..Default::default()
        };
        assert_eq!(
            counters.checked_add_delta(&delta),
            Err(UsageCounterMergeError::Overflow)
        );
        assert_eq!(counters.output_tokens, Some(u64::MAX));
    }

    #[test]
    fn candidate_identity_is_stable_and_reports_keep_route_provenance() {
        let report = UsageReport::unmetered(String::new()).for_candidate(
            "execution-1",
            "smith:provider=one#1234abcd",
            2,
            1,
        );
        assert_eq!(
            report.report_id,
            "forge:execution-1:smith:provider=one#1234abcd:2:1"
        );
        assert_eq!(
            report.candidate_key.as_deref(),
            Some("smith:provider=one#1234abcd")
        );
        assert_eq!(report.attempt_ordinal, 2);
        assert_eq!(report.report_sequence, 1);
    }
}
