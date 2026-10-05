use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ProjectEscalateRequest {
    pub need: String,
    #[serde(default)]
    pub task_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct AnswerProjectEscalationRequest {
    pub expected_version: i64,
    pub answer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ProjectEscalationResponse {
    pub id: String,
    pub project_id: String,
    pub need: String,
    pub task_ids: Vec<String>,
    pub attention_id: String,
    pub notification_id: String,
    pub status: String,
    pub answer: Option<String>,
    pub version: i64,
}
