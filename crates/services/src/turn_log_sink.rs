//! Durable JSONL sink for one native runtime turn.
//!
//! Every native turn Forge hosts, whether a Task execution or an Agent Chat
//! turn, writes the same Forge log schema (`executors::LogEntry`) so one log
//! reader and one chat renderer serve both. The sink records semantic
//! progress only: it never renews an owner lease itself, because a quiet
//! provider request or a long-running tool must stay live while its owner is
//! healthy.

use std::{path::PathBuf, sync::Arc};

use api_types::ToolResultSummary;
use async_trait::async_trait;
use executors::{LogEntry, LogKind, LogStream, LogWriter};
use forge_agent_host::TurnEventSink;
use tokio::sync::Mutex;

/// Notified after every durable turn event so a caller can bump a liveness
/// marker (for example an execution row's semantic progress timestamp).
#[async_trait]
pub trait TurnProgressObserver: Send + Sync {
    async fn record_progress(&self);
}

pub struct TurnLogSink {
    writer: Mutex<LogWriter>,
    progress: Option<Arc<dyn TurnProgressObserver>>,
}

impl std::fmt::Debug for TurnLogSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnLogSink")
            .finish_non_exhaustive()
    }
}

impl TurnLogSink {
    /// `log_id` is the `execution_id` stamped on every entry: the execution
    /// id for a Task, the turn job id for an Agent Chat turn.
    pub fn new(
        path: impl Into<PathBuf>,
        log_id: &str,
        sender: Option<tokio::sync::mpsc::UnboundedSender<LogEntry>>,
        progress: Option<Arc<dyn TurnProgressObserver>>,
    ) -> Self {
        let mut writer = LogWriter::new(path, log_id.to_owned(), 10 * 1024 * 1024);
        if let Some(sender) = sender {
            writer.set_log_sender(sender);
        }
        Self {
            writer: Mutex::new(writer),
            progress,
        }
    }

    pub async fn write(&self, kind: LogKind, payload: serde_json::Value) -> std::io::Result<()> {
        self.writer
            .lock()
            .await
            .write(kind, LogStream::Main, payload)
            .await
    }

    /// A visible boundary between two attempts of the same turn in one log:
    /// the chat renderer draws it as a divider (`system` / `turn_divider`).
    pub async fn write_attempt_divider(&self, attempt: i64) -> std::io::Result<()> {
        self.write(
            LogKind::System,
            serde_json::json!({
                "type": "turn_divider",
                "label": format!("Attempt {attempt}"),
                "attempt": attempt,
            }),
        )
        .await
    }

    /// Bump the caller's liveness marker, if one was attached.
    pub async fn record_progress(&self) {
        if let Some(progress) = self.progress.as_ref() {
            progress.record_progress().await;
        }
    }
}

#[async_trait]
impl TurnEventSink for TurnLogSink {
    async fn text_delta(&self, text: &str) {
        let _ = self
            .write(LogKind::AssistantDelta, serde_json::json!({"text": text}))
            .await;
        self.record_progress().await;
    }

    async fn reasoning_delta(&self, text: &str, redacted: bool) {
        if !redacted {
            let _ = self
                .write(LogKind::Thinking, serde_json::json!({"text": text}))
                .await;
        }
        self.record_progress().await;
    }

    async fn tool_call_started(
        &self,
        call_id: &str,
        name: &str,
        argument_keys: &[String],
        argument_preview: &serde_json::Map<String, serde_json::Value>,
    ) {
        let mut payload = serde_json::json!({
            "call_id": call_id,
            "name": name,
            "argument_keys": argument_keys,
        });
        if !argument_preview.is_empty() {
            payload["input"] = serde_json::Value::Object(argument_preview.clone());
        }
        let _ = self.write(LogKind::ToolCall, payload).await;
        self.record_progress().await;
    }

    async fn tool_call_finished(
        &self,
        call_id: &str,
        name: &str,
        is_error: bool,
        summary: &ToolResultSummary,
    ) {
        let _ = self
            .write(
                LogKind::ToolResult,
                serde_json::json!({
                    "call_id": call_id,
                    "name": name,
                    "is_error": is_error,
                    "success": !is_error,
                    "summary": summary,
                }),
            )
            .await;
        self.record_progress().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tool_call_finished_persists_the_bounded_summary_in_the_durable_log() {
        // Characterizes F14: the durable log keeps the structured outcome's
        // code, safe message, correlation id, operation, and recovery action,
        // never the protected internal cause.
        let dir = tempfile::tempdir().expect("temp dir creates");
        let log_path = dir.path().join("turn.jsonl");
        let sink = TurnLogSink::new(&log_path, "turn-1", None, None);

        let mut outcome = api_types::OrchestrationOutcome::failed(
            api_types::OutcomeCode::VersionConflict,
            "task.propose",
            api_types::CanonicalScopeRef::new(api_types::OutcomeScopeType::Task, "task-1"),
            "corr-1",
            "the authorized resource changed; refresh current state and retry",
        );
        outcome.retry = Some(api_types::RetryInstruction::new(
            api_types::RetryAction::RefreshAndRetry,
            true,
        ));
        // Simulates a protected internal cause a command boundary logs but
        // never returns to a caller. It must not reach the durable log
        // through the tool-result summary path.
        outcome.result = Some(serde_json::json!({
            "internal_cause": "db error: password=hunter2-secret-token",
        }));
        let summary = ToolResultSummary::from_orchestration_outcome(&outcome);

        sink.tool_call_finished(
            "call-1",
            "forge_project_orchestration_propose",
            true,
            &summary,
        )
        .await;

        let raw = tokio::fs::read_to_string(&log_path)
            .await
            .expect("durable log reads");
        assert!(!raw.contains("hunter2-secret-token"));
        assert!(!raw.contains("internal_cause"));

        let entry: serde_json::Value =
            serde_json::from_str(raw.lines().next().expect("one log line writes"))
                .expect("log entry parses as JSON");
        assert_eq!(entry["execution_id"], "turn-1");
        let payload = &entry["payload"];
        assert_eq!(payload["call_id"], "call-1");
        assert_eq!(payload["is_error"], true);
        assert_eq!(payload["success"], false);
        assert_eq!(payload["summary"]["status"], "failed");
        assert_eq!(payload["summary"]["code"], "version_conflict");
        assert_eq!(payload["summary"]["operation"], "task.propose");
        assert_eq!(
            payload["summary"]["safe_message"],
            "the authorized resource changed; refresh current state and retry"
        );
        assert_eq!(payload["summary"]["correlation_id"], "corr-1");
        assert_eq!(payload["summary"]["retryable"], true);
        assert_eq!(payload["summary"]["recovery_action"], "refresh_and_retry");
    }

    #[tokio::test]
    async fn the_full_turn_event_stream_lands_in_order_with_the_forge_log_schema() {
        let dir = tempfile::tempdir().expect("temp dir creates");
        let log_path = dir.path().join("turn.jsonl");
        let sink = TurnLogSink::new(&log_path, "turn-2", None, None);

        sink.write_attempt_divider(2).await.expect("divider writes");
        sink.reasoning_delta("weighing options", false).await;
        sink.reasoning_delta("hidden", true).await;
        // Runs the raw arguments through the real filter (rather than
        // hand-building a preview map) so this test also characterizes that
        // a denylisted key never survives into the durable log.
        let raw_arguments = serde_json::json!({
            "operation": "read",
            "api_key": "sk-super-secret",
        });
        let argument_preview = forge_agent_host::build_tool_argument_preview(&raw_arguments);
        sink.tool_call_started(
            "call-1",
            "forge_scope_read",
            &["operation".to_owned()],
            &argument_preview,
        )
        .await;
        sink.tool_call_finished(
            "call-1",
            "forge_scope_read",
            false,
            &ToolResultSummary::unclassified(false, "call-1"),
        )
        .await;
        sink.text_delta("Done.").await;

        let raw = tokio::fs::read_to_string(&log_path)
            .await
            .expect("durable log reads");
        let entries: Vec<executors::LogEntry> = raw
            .lines()
            .map(|line| serde_json::from_str(line).expect("entry parses"))
            .collect();
        let kinds: Vec<String> = entries.iter().map(|entry| entry.kind.to_string()).collect();
        assert_eq!(
            kinds,
            vec![
                "system",
                "thinking",
                "tool_call",
                "tool_result",
                "assistant_delta"
            ],
            "redacted reasoning is a liveness signal only and never lands in the log"
        );
        let sequences: Vec<u64> = entries.iter().map(|entry| entry.sequence).collect();
        assert_eq!(sequences, vec![0, 1, 2, 3, 4]);
        assert_eq!(entries[0].payload["type"], "turn_divider");
        assert_eq!(entries[0].payload["label"], "Attempt 2");
        assert_eq!(
            entries[2].payload["argument_keys"],
            serde_json::json!(["operation"])
        );
        assert_eq!(
            entries[2].payload["input"],
            serde_json::json!({"operation": "read"})
        );
        assert!(raw.lines().all(|line| !line.contains("hidden")));
        assert!(!raw.contains("api_key"));
        assert!(!raw.contains("sk-super-secret"));
    }
}
