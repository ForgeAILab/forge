use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::{AnalyticsWindow, OutcomeCostMetric, ProjectUsageBreakdown, UsageAnalytics};

/// Project analytics for one immutable half-open accounting window.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct ProjectAnalyticsResponse {
    pub window: AnalyticsWindow,
    pub ci_steps: Vec<CiStepAnalytics>,
    pub token_usage: UsageAnalytics,
    pub review_summary: ReviewSummaryAnalytics,
    pub outcome_economics: OutcomeCostMetric,
}

/// CI-step execution counts and timing analytics.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct CiStepAnalytics {
    pub command: String,
    #[ts(type = "number")]
    pub total_runs: i64,
    #[ts(type = "number")]
    pub pass_count: i64,
    #[ts(type = "number")]
    pub fail_count: i64,
    pub success_rate: f64,
    #[ts(type = "number | null")]
    pub avg_duration_ms: Option<i64>,
    #[ts(type = "number | null")]
    pub p50_duration_ms: Option<i64>,
    #[ts(type = "number | null")]
    pub p95_duration_ms: Option<i64>,
    pub last_run_at: Option<String>,
}

/// Account-scoped usage analytics across every product surface.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AccountUsageAnalyticsResponse {
    pub window: AnalyticsWindow,
    pub token_usage: UsageAnalytics,
    pub by_project: Vec<ProjectUsageBreakdown>,
}

/// Review counts remain separate from token/cost analytics.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct ReviewSummaryAnalytics {
    #[ts(type = "number")]
    pub total_reviews: i64,
    #[ts(type = "number")]
    pub passed: i64,
    #[ts(type = "number")]
    pub failed: i64,
    #[ts(type = "number")]
    pub cancelled: i64,
    #[ts(type = "number | null")]
    pub avg_duration_ms: Option<i64>,
    pub pass_rate: f64,
}
