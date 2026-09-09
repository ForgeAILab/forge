use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::{FsEntry, UsageTelemetryState};

pub const METHOD_FS_LIST: &str = "fs.list";
pub const METHOD_FS_BRANCHES: &str = "fs.branches";
pub const METHOD_EXECUTION_START: &str = "execution.start";
pub const METHOD_EXECUTION_CANCEL: &str = "execution.cancel";
pub const METHOD_EXECUTION_LOG: &str = "execution.log";
pub const METHOD_EXECUTION_TERMINAL: &str = "execution.terminal";
pub const METHOD_EXECUTION_TERMINAL_ACK: &str = "execution.terminal.ack";
pub const METHOD_DAEMON_HANDSHAKE: &str = "daemon.handshake";
pub const METHOD_TERMINAL_START: &str = "terminal.start";
pub const METHOD_TERMINAL_INPUT: &str = "terminal.input";
pub const METHOD_TERMINAL_RESIZE: &str = "terminal.resize";
pub const METHOD_TERMINAL_TERMINATE: &str = "terminal.terminate";
pub const METHOD_TERMINAL_OUTPUT: &str = "terminal.output";
pub const METHOD_TERMINAL_EXITED: &str = "terminal.exited";

pub const DAEMON_UNAVAILABLE: &str = "daemon_unavailable";
pub const DAEMON_TIMEOUT: &str = "daemon_timeout";
pub const UNSUPPORTED_METHOD: &str = "unsupported_method";
pub const INVALID_FRAME: &str = "invalid_frame";
pub const INVALID_INPUT: &str = "invalid_input";
pub const PATH_GUARDRAIL: &str = "path_guardrail";
pub const EXECUTION_NOT_FOUND: &str = "execution_not_found";
pub const DAEMON_PROTOCOL_INCOMPATIBLE: &str = "daemon_protocol_incompatible";
pub const TERMINAL_REPORT_CONFLICT: &str = "terminal_report_conflict";

/// The minimum daemon command protocol understood by this server/client pair.
/// Revision 2 is the first revision that carries per-attempt reports and
/// acknowledgement-gated terminal delivery.
pub const DAEMON_PROTOCOL_REVISION: u32 = 2;
pub const DAEMON_CAPABILITY_USAGE_REPORTS: &str = "execution.terminal.usage_reports";
pub const DAEMON_CAPABILITY_TERMINAL_ACK: &str = "execution.terminal.ack";
pub const DAEMON_REQUIRED_CAPABILITIES: &[&str] = &[
    DAEMON_CAPABILITY_USAGE_REPORTS,
    DAEMON_CAPABILITY_TERMINAL_ACK,
];

/// Whether a daemon handshake advertises the minimum terminal accounting
/// contract. Newer revisions remain wire-compatible as long as they retain
/// the required capabilities.
pub fn daemon_protocol_is_compatible(revision: u32, capabilities: &[String]) -> bool {
    revision >= DAEMON_PROTOCOL_REVISION
        && DAEMON_REQUIRED_CAPABILITIES
            .iter()
            .all(|required| capabilities.iter().any(|capability| capability == required))
}

pub const DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS: u64 = 30;
pub const DAEMON_HEARTBEAT_INTERVAL_SECS: u64 = 20;

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "lowercase")]
#[ts(export)]
pub enum DaemonFrame {
    Request {
        id: String,
        method: String,
        #[ts(type = "unknown")]
        params: serde_json::Value,
    },
    Response {
        id: String,
        #[ts(type = "unknown")]
        result: serde_json::Value,
    },
    Error {
        id: Option<String>,
        error: DaemonErrorPayload,
    },
    Notification {
        method: String,
        #[ts(type = "unknown")]
        params: serde_json::Value,
    },
    Heartbeat {
        seq: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct FsListParams {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct FsListResult {
    pub path: String,
    pub entries: Vec<FsEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct FsBranchesParams {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct FsBranchesResult {
    pub branches: Vec<String>,
    pub default_branch: Option<String>,
    pub origin_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ExecutionStartParams {
    pub task_id: String,
    pub execution_id: String,
    pub workspace_path: String,
    pub executor_type: String,
    #[ts(type = "unknown")]
    pub executor_config: serde_json::Value,
    #[ts(type = "unknown")]
    pub prompt: serde_json::Value,
    pub max_turns: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ExecutionStartResult {
    pub execution_id: String,
    pub accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ExecutionCancelParams {
    pub execution_id: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ExecutionCancelResult {
    pub execution_id: String,
    pub cancelled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ExecutionLogNotification {
    pub execution_id: String,
    pub seq: u64,
    pub stream: String,
    pub line: String,
    pub ts: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_stream: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "unknown")]
    pub payload: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct ExecutionTerminalNotification {
    // Stable identity for the complete terminal result. The daemon persists
    // this record and retains it until the authenticated server acknowledges
    // this exact report.
    pub terminal_report_id: String,
    pub execution_id: String,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
    pub error: Option<String>,
    pub ts: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_sha: Option<String>,
    // One report for every provider request/candidate attempt. This is
    // intentionally a vector: the server must not flatten fallback hops or
    // infer provider identity from executor family.
    pub usage_reports: Vec<RemoteUsageReport>,
    // Structured failure disposition. Absent on older daemons — the server
    // then falls back to generic executor-failed handling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<RemoteExecutionFailureClass>,
    // RFC3339 time when an unavailable executor route is worth retrying.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_candidate: Option<RemoteResolvedCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_attempts: Option<Vec<RemoteRouteAttempt>>,
}

/// The command-stream handshake sent by a daemon immediately after an
/// authenticated connection is established. A server may dispatch work only
/// after the advertised revision and required capabilities are accepted.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DaemonHandshakeNotification {
    pub protocol_revision: u32,
    pub capabilities: Vec<String>,
}

/// A server acknowledgement for a durable terminal notification. The
/// acknowledgement is a request because the daemon must return a response so
/// transport failures cannot be mistaken for a durable delete.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ExecutionTerminalAckParams {
    pub terminal_report_id: String,
    pub execution_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ExecutionTerminalAckResult {
    pub terminal_report_id: String,
    pub execution_id: String,
    pub acknowledged: bool,
}

/// Structured failure class carried across the daemon protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(rename_all = "snake_case")]
pub enum RemoteExecutionFailureClass {
    TaskFailed,
    ExecutorUnavailable,
}

/// The executor candidate that actually ran a remote execution.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct RemoteResolvedCandidate {
    pub candidate_key: String,
    pub executor_type: String,
    #[ts(type = "Record<string, unknown>")]
    pub config: serde_json::Value,
}

/// One candidate attempt outcome from a remote execution's fallback route.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct RemoteRouteAttempt {
    pub candidate_key: String,
    pub outcome: String,
}

/// Per-attempt usage transported over the daemon command stream.
///
/// Counters are nullable independently: a reported-money-only event, a
/// partially observed stream, and a producer with no telemetry are all
/// materially different from an explicit metered zero. `reported_cost_usd`
/// remains exact decimal text on the wire; clients must not parse it through a
/// binary floating-point number.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct RemoteUsageReport {
    pub report_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default)]
    pub report_sequence: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_key: Option<String>,
    #[serde(default)]
    pub attempt_ordinal: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
    #[serde(default)]
    pub cache_read_tokens: Option<u64>,
    #[serde(default)]
    pub cache_write_tokens: Option<u64>,
    pub telemetry_state: UsageTelemetryState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported_cost_usd: Option<String>,
    #[serde(default)]
    pub partial: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TerminalStartParams {
    pub session_id: String,
    pub workspace_path: String,
    pub rows: u16,
    pub cols: u16,
    pub shell: Option<String>,
    pub env: Option<Vec<(String, String)>>,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TerminalStartResult {
    pub session_id: String,
    pub pid: Option<u32>,
    pub started_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TerminalInputParams {
    pub session_id: String,
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TerminalInputResult {
    pub session_id: String,
    pub accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TerminalResizeParams {
    pub session_id: String,
    pub rows: u16,
    pub cols: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TerminalResizeResult {
    pub session_id: String,
    pub applied: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TerminalTerminateParams {
    pub session_id: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TerminalTerminateResult {
    pub session_id: String,
    pub terminated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TerminalOutputNotification {
    pub session_id: String,
    pub data: String,
    pub ts: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TerminalExitedNotification {
    pub session_id: String,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
    pub reason: Option<String>,
    pub ts: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DaemonErrorPayload {
    pub code: String,
    pub message: String,
    #[ts(type = "unknown")]
    pub details: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::{
        daemon_protocol_is_compatible, DaemonErrorPayload, DaemonFrame,
        ExecutionTerminalNotification, RemoteUsageReport, TerminalOutputNotification,
        DAEMON_PROTOCOL_REVISION, DAEMON_REQUIRED_CAPABILITIES,
    };
    use crate::UsageTelemetryState;

    #[test]
    fn request_frame_round_trips() {
        let frame = DaemonFrame::Request {
            id: "req-1".to_owned(),
            method: "fs.list".to_owned(),
            params: serde_json::json!({ "path": "/tmp" }),
        };

        let json = serde_json::to_value(&frame).expect("serialize request frame");
        assert_eq!(json["type"], "request");
        assert!(json.get("id").is_some());
        assert!(json.get("method").is_some());
        assert!(json.get("params").is_some());

        let decoded: DaemonFrame = serde_json::from_value(json).expect("deserialize request frame");
        assert!(matches!(decoded, DaemonFrame::Request { .. }));
    }

    #[test]
    fn response_frame_round_trips() {
        let frame = DaemonFrame::Response {
            id: "req-1".to_owned(),
            result: serde_json::json!({ "ok": true }),
        };

        let json = serde_json::to_value(&frame).expect("serialize response frame");
        assert_eq!(json["type"], "response");

        let decoded: DaemonFrame =
            serde_json::from_value(json).expect("deserialize response frame");
        assert!(matches!(decoded, DaemonFrame::Response { .. }));
    }

    #[test]
    fn error_frame_round_trips() {
        let frame = DaemonFrame::Error {
            id: Some("req-1".to_owned()),
            error: DaemonErrorPayload {
                code: "daemon_timeout".to_owned(),
                message: "daemon timed out".to_owned(),
                details: None,
            },
        };

        let json = serde_json::to_value(&frame).expect("serialize error frame");
        assert_eq!(json["type"], "error");

        let decoded: DaemonFrame = serde_json::from_value(json).expect("deserialize error frame");
        assert!(matches!(decoded, DaemonFrame::Error { .. }));
    }

    #[test]
    fn notification_frame_round_trips() {
        let frame = DaemonFrame::Notification {
            method: "execution.log".to_owned(),
            params: serde_json::json!({
                "execution_id": "exec-1",
                "seq": 1,
                "stream": "stdout",
                "line": "started",
                "ts": "2026-05-14T00:00:00Z"
            }),
        };

        let json = serde_json::to_value(&frame).expect("serialize notification frame");
        assert_eq!(json["type"], "notification");

        let decoded: DaemonFrame =
            serde_json::from_value(json).expect("deserialize notification frame");
        assert!(matches!(decoded, DaemonFrame::Notification { .. }));
    }

    #[test]
    fn terminal_output_notification_round_trips() {
        let notification = TerminalOutputNotification {
            session_id: "term-1".to_owned(),
            data: "hello\r\n".to_owned(),
            ts: "2026-05-20T00:00:00Z".to_owned(),
        };

        let json = serde_json::to_value(&notification).expect("serialize terminal output");
        assert_eq!(json["session_id"], "term-1");
        assert_eq!(json["data"], "hello\r\n");

        let decoded: TerminalOutputNotification =
            serde_json::from_value(json).expect("deserialize terminal output");
        assert_eq!(decoded.session_id, "term-1");
        assert_eq!(decoded.data, "hello\r\n");
        assert_eq!(decoded.ts, "2026-05-20T00:00:00Z");
    }

    #[test]
    fn terminal_usage_reports_preserve_nullable_counters_and_decimal_cost() {
        let notification = ExecutionTerminalNotification {
            terminal_report_id: "terminal-report-1".to_owned(),
            execution_id: "execution-1".to_owned(),
            exit_code: Some(0),
            signal: None,
            error: None,
            ts: "2026-05-20T00:00:00Z".to_owned(),
            status: Some("completed".to_owned()),
            agent_session_id: None,
            summary: None,
            after_sha: None,
            usage_reports: vec![RemoteUsageReport {
                report_id: "report-1".to_owned(),
                request_id: Some("request-1".to_owned()),
                report_sequence: 0,
                candidate_key: Some("candidate-a".to_owned()),
                attempt_ordinal: 1,
                provider_id: Some("openai".to_owned()),
                model_id: Some("gpt-5".to_owned()),
                input_tokens: None,
                output_tokens: Some(12),
                cache_read_tokens: Some(0),
                cache_write_tokens: None,
                telemetry_state: UsageTelemetryState::Metered,
                context_tokens: Some(20),
                selected_tier: Some("short".to_owned()),
                reported_cost_usd: Some("0.000000001".to_owned()),
                partial: true,
            }],
            failure_class: None,
            retry_at: None,
            resolved_candidate: None,
            route_attempts: None,
        };

        let value = serde_json::to_value(&notification).expect("terminal serializes");
        assert_eq!(value["terminal_report_id"], "terminal-report-1");
        assert_eq!(
            value["usage_reports"][0]["input_tokens"],
            serde_json::Value::Null
        );
        assert_eq!(
            value["usage_reports"][0]["reported_cost_usd"],
            "0.000000001"
        );

        let decoded: ExecutionTerminalNotification =
            serde_json::from_value(value).expect("terminal deserializes");
        assert_eq!(decoded.usage_reports, notification.usage_reports);
    }

    #[test]
    fn terminal_notification_without_report_vector_is_rejected() {
        let value = serde_json::json!({
            "terminal_report_id": "terminal-report-1",
            "execution_id": "execution-1",
            "exit_code": 0,
            "signal": null,
            "error": null,
            "ts": "2026-05-20T00:00:00Z"
        });
        assert!(serde_json::from_value::<ExecutionTerminalNotification>(value).is_err());
    }

    #[test]
    fn daemon_protocol_gate_requires_revision_and_capabilities() {
        let capabilities = DAEMON_REQUIRED_CAPABILITIES
            .iter()
            .map(|capability| (*capability).to_owned())
            .collect::<Vec<_>>();
        assert!(daemon_protocol_is_compatible(
            DAEMON_PROTOCOL_REVISION,
            &capabilities
        ));
        assert!(!daemon_protocol_is_compatible(
            DAEMON_PROTOCOL_REVISION - 1,
            &capabilities
        ));
        assert!(!daemon_protocol_is_compatible(
            DAEMON_PROTOCOL_REVISION,
            &capabilities[..1]
        ));
    }
}
