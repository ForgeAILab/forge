use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// A Main Agent inquiry (`inquiry.run`) is an ephemeral, read-only sub-agent
/// turn: no repo, no task flow, no state machine. The only user verb is
/// cancel — there is deliberately no retry, assignment, dependency,
/// milestone, or review concept. It is a run log, not a work item.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AgentInquiryStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

/// Mirrors `agent_host::AgentTurnOutput`. The four counters are DISJOINT —
/// context size is `input_tokens + cache_read_tokens + cache_write_tokens`.
/// Never sum them into one "input" number.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentInquiryTokenUsage {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
}

/// `owner_user_id`, `identity_id`, and `workspace_path` are deliberately not
/// part of this response: internal, not part of the public surface.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentInquiryResponse {
    pub id: String,
    pub chat_id: String,
    pub title: String,
    pub question: String,
    pub status: AgentInquiryStatus,
    pub findings: Option<String>,
    pub findings_path: Option<String>,
    pub error: Option<String>,
    pub token_usage: AgentInquiryTokenUsage,
    pub duration_ms: Option<i64>,
    pub version: i64,
    pub created_at: String,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentInquiryListResponse {
    pub items: Vec<AgentInquiryResponse>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct CancelAgentInquiryRequest {
    pub expected_version: i64,
}
