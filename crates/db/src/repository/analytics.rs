//! Project review and usage analytics repository contracts.

use super::*;

#[derive(Debug, Clone, PartialEq)]
pub struct CiStepStats {
    pub command: String,
    pub total_runs: i64,
    pub pass_count: i64,
    pub fail_count: i64,
    pub avg_duration_ms: Option<i64>,
    pub p50_duration_ms: Option<i64>,
    pub p95_duration_ms: Option<i64>,
    pub last_run_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectReviewSummary {
    pub total_reviews: i64,
    pub passed: i64,
    pub failed: i64,
    pub cancelled: i64,
    pub avg_duration_ms: Option<i64>,
    pub pass_rate: f64,
}

#[async_trait]
pub trait ProjectAnalyticsRepo: Send + Sync {
    async fn get_project_ci_analytics(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<CiStepStats>>;
    async fn get_project_review_summary(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<ProjectReviewSummary>;
}

/// Ledger-backed usage and cost projections.  The projection deliberately
/// returns the shared wire contract so every API surface uses the same
/// fixed-point money and coverage semantics. Implementations must only read
/// the immutable invocation/event ledger for usage and cost; legacy aggregate
/// rows are not an accounting source.
#[async_trait]
pub trait UsageAnalyticsRepo: Send + Sync {
    async fn get_project_usage_analytics(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<api_types::UsageAnalytics>;

    async fn get_account_usage_analytics(
        &self,
        owner_user_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<api_types::AccountUsageAnalyticsResponse>;

    async fn count_project_released_milestones(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<i64>;
}
