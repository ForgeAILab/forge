use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    pub schema_version: u32,
    pub sequence: u64,
    pub timestamp: String,
    pub execution_id: String,
    pub kind: LogKind,
    pub stream: LogStream,
    pub payload: serde_json::Value,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogKind {
    Stdout,
    Stderr,
    ToolCall,
    ToolResult,
    Assistant,
    AssistantDelta,
    Thinking,
    User,
    System,
    FileChange,
    ShellCommand,
    ApprovalQuestion,
    SessionInfo,
    #[serde(other)]
    Unknown,
}

impl std::fmt::Display for LogKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stdout => write!(f, "stdout"),
            Self::Stderr => write!(f, "stderr"),
            Self::ToolCall => write!(f, "tool_call"),
            Self::ToolResult => write!(f, "tool_result"),
            Self::Assistant => write!(f, "assistant"),
            Self::AssistantDelta => write!(f, "assistant_delta"),
            Self::Thinking => write!(f, "thinking"),
            Self::User => write!(f, "user"),
            Self::System => write!(f, "system"),
            Self::FileChange => write!(f, "file_change"),
            Self::ShellCommand => write!(f, "shell_command"),
            Self::ApprovalQuestion => write!(f, "approval_question"),
            Self::SessionInfo => write!(f, "session_info"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

impl std::str::FromStr for LogKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "stdout" => Ok(Self::Stdout),
            "stderr" => Ok(Self::Stderr),
            "tool_call" => Ok(Self::ToolCall),
            "tool_result" => Ok(Self::ToolResult),
            "assistant" => Ok(Self::Assistant),
            "assistant_delta" => Ok(Self::AssistantDelta),
            "thinking" => Ok(Self::Thinking),
            "user" => Ok(Self::User),
            "system" => Ok(Self::System),
            "file_change" => Ok(Self::FileChange),
            "shell_command" => Ok(Self::ShellCommand),
            "approval_question" => Ok(Self::ApprovalQuestion),
            "session_info" => Ok(Self::SessionInfo),
            "unknown" => Ok(Self::Unknown),
            other => Err(format!("unknown log kind: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Main,
    Heartbeat,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_log_kinds_serialize_as_snake_case() {
        assert_eq!(
            serde_json::to_value(LogKind::FileChange).unwrap(),
            serde_json::json!("file_change")
        );
        assert_eq!(
            serde_json::to_value(LogKind::ShellCommand).unwrap(),
            serde_json::json!("shell_command")
        );
        assert_eq!(
            serde_json::to_value(LogKind::ApprovalQuestion).unwrap(),
            serde_json::json!("approval_question")
        );
        assert_eq!(
            serde_json::to_value(LogKind::SessionInfo).unwrap(),
            serde_json::json!("session_info")
        );
        assert_eq!(
            serde_json::to_value(LogKind::AssistantDelta).unwrap(),
            serde_json::json!("assistant_delta")
        );
    }

    #[test]
    fn log_kind_string_conversions_include_assistant_delta() {
        assert_eq!(LogKind::AssistantDelta.to_string(), "assistant_delta");
        assert_eq!(
            "assistant_delta".parse::<LogKind>().unwrap(),
            LogKind::AssistantDelta
        );
    }

    #[test]
    fn unknown_log_kind_deserializes_to_unknown_and_preserves_payload() {
        let entry: LogEntry = serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "sequence": 7,
            "timestamp": "2026-04-13T00:00:00Z",
            "execution_id": "exec",
            "kind": "future_kind",
            "stream": "main",
            "payload": { "raw": true },
            "truncated": false
        }))
        .unwrap();

        assert_eq!(entry.kind, LogKind::Unknown);
        assert_eq!(entry.payload, serde_json::json!({ "raw": true }));
    }
}
