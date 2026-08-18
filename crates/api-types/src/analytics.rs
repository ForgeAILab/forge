use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ProjectAnalyticsResponse {
    pub ci_steps: Vec<CiStepAnalytics>,
    pub token_usage: TokenUsageAnalytics,
    pub review_summary: ReviewSummaryAnalytics,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CiStepAnalytics {
    pub command: String,
    pub total_runs: i64,
    pub pass_count: i64,
    pub fail_count: i64,
    pub success_rate: f64,
    pub avg_duration_ms: Option<i64>,
    pub p50_duration_ms: Option<i64>,
    pub p95_duration_ms: Option<i64>,
    pub last_run_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TokenUsageAnalytics {
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_cache_read_tokens: i64,
    pub total_cache_write_tokens: i64,
    pub total_cost_usd: Option<f64>,
    pub execution_count: i64,
    pub by_model: Vec<ModelTokenBreakdown>,
    pub by_agent: Vec<AgentTokenBreakdown>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ModelTokenBreakdown {
    pub provider: String,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub cost_usd: Option<f64>,
    pub execution_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentTokenBreakdown {
    pub agent_id: String,
    pub agent_name: String,
    pub executor_type: String,
    pub model: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub cost_usd: Option<f64>,
    pub execution_count: i64,
    pub success_rate: Option<f64>,
    pub avg_duration_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ReviewSummaryAnalytics {
    pub total_reviews: i64,
    pub passed: i64,
    pub failed: i64,
    pub cancelled: i64,
    pub avg_duration_ms: Option<i64>,
    pub pass_rate: f64,
}
