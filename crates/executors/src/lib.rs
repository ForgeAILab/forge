#![forbid(unsafe_code)]

pub mod adapter;
pub mod command;
pub mod config;
pub mod effective_policy;
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
    account_key, build_ordered_fallback_routing, candidate_key, deserialize_config,
    merge_overrides, resolve_config_value, ClaudeCodeConfig, CodexConfig, CommandOverrides,
    CursorConfig, EmbeddedConfig, ExecutorCandidate, ExecutorRouting, GeminiConfig, NullConfig,
    OpencodeConfig, PermissionPolicy, RouteAttempt, RouteAttemptOutcome, ShellConfig, SmithConfig,
    FALLBACKS_CONFIG_KEY, ROUTING_POLICY_ORDERED_FALLBACK_V1, ROUTING_SNAPSHOT_KEY,
};
pub use log_reader::{LogReadResult, LogReader};
pub use log_schema::{LogEntry, LogKind, LogStream};
pub use log_writer::LogWriter;
pub use shell::{is_pid_alive, ShellExecutor};

use async_trait::async_trait;

const READ_ONLY_WORKTREE_KEY: &str = "_forge_read_only_worktree";

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

/// Accumulated token usage from an executor run.
///
/// The three input counters are **disjoint**, whatever the upstream provider's
/// own convention: `input_tokens` excludes both cache reads and cache writes,
/// so context size is their sum and a total never double-counts a cached
/// prefix. Adapters normalize to this on the way in — Anthropic already
/// reports it, Codex and the embedded runtime report input inclusive of the
/// cached prefix and have it subtracted back out.
#[derive(Debug, Clone, Default)]
pub struct TokenUsage {
    /// Input tokens read fresh, excluding cache reads and cache writes.
    pub input_tokens: i64,
    pub output_tokens: i64,
    /// Input tokens served from the provider's prompt cache.
    pub cache_read_tokens: i64,
    /// Input tokens written to the provider's prompt cache.
    pub cache_write_tokens: i64,
    pub cost_usd: Option<f64>,
    pub model: Option<String>,
}

impl TokenUsage {
    /// Fold another candidate's usage into this one (fallback chains aggregate
    /// usage across every candidate that consumed billable tokens).
    pub fn absorb(&mut self, other: &TokenUsage) {
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        self.cache_read_tokens += other.cache_read_tokens;
        self.cache_write_tokens += other.cache_write_tokens;
        self.cost_usd = match (self.cost_usd, other.cost_usd) {
            (Some(a), Some(b)) => Some(a + b),
            (a, b) => a.or(b),
        };
        if self.model.is_none() {
            self.model = other.model.clone();
        }
    }
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
    pub usage: Option<TokenUsage>,
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
    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The candidate's quota or rate limit is exhausted. Candidate-level
    /// control flow for the fallback layer only — never the terminal channel.
    /// Carries any usage the candidate accumulated before hitting the cap so
    /// fallback chains keep accounting truthful.
    #[error("usage exhausted")]
    UsageExhausted {
        retry_after: Option<std::time::Duration>,
        usage: Option<TokenUsage>,
    },

    /// The candidate's CLI is missing or unauthenticated. Candidate-level
    /// control flow for the fallback layer only — never the terminal channel.
    #[error("executor unavailable: {0}")]
    Unavailable(String),

    #[error("executor error: {0}")]
    Other(String),
}

impl ExecutorError {
    /// Availability failures are the only errors that may advance a
    /// fallback chain.
    pub fn is_availability(&self) -> bool {
        matches!(self, Self::UsageExhausted { .. } | Self::Unavailable(_))
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
}
