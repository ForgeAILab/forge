//! Shared provider pricing, usage cost, and retrospective-estimation API
//! contracts.
//!
//! Monetary values deliberately remain decimal strings on the wire.  The
//! server owns fixed-point parsing and arithmetic; clients must not interpret
//! these values as JavaScript numbers.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

/// A USD amount represented by canonical non-negative decimal text.
///
/// `decimal` has no exponent and at most nine fractional digits.  The
/// currency is fixed to USD for this revision, even though it is represented
/// as a string so that the JSON contract remains straightforward for clients.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct MoneyAmount {
    /// Always `USD` in this API revision.
    #[ts(type = "\"USD\"")]
    pub currency: String,
    /// Canonical non-negative decimal text; never an exponent.
    pub decimal: String,
}

/// A USD rate per one million tokens.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct RateAmount {
    /// Always `USD` in this API revision.
    #[ts(type = "\"USD\"")]
    pub currency: String,
    /// Canonical non-negative decimal text; never an exponent.
    pub decimal_per_million: String,
}

/// The independently measurable token buckets used for estimation.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct TokenCounters {
    #[ts(type = "number")]
    pub input_tokens: i64,
    #[ts(type = "number")]
    pub output_tokens: i64,
    #[ts(type = "number")]
    pub cache_read_tokens: i64,
    #[ts(type = "number")]
    pub cache_write_tokens: i64,
}

/// The four measurable per-million-token price buckets.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct RateBuckets {
    pub input: Option<RateAmount>,
    pub output: Option<RateAmount>,
    pub cache_read: Option<RateAmount>,
    pub cache_write: Option<RateAmount>,
}

/// Whether an amount came from a provider or Forge's fixed-point estimator.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CostKind {
    ProviderReported,
    Estimated,
    Mixed,
    Unknown,
    None,
}

/// Coverage of all cost-bearing activity in a projection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CostCoverage {
    Complete,
    Partial,
    Unavailable,
    Pending,
    NoUsage,
}

/// A reason why one or more usage events cannot contribute a complete cost.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CostCoverageReasonCode {
    Pending,
    Unsettled,
    Unmetered,
    MissingProvider,
    MissingModel,
    MissingBinding,
    MissingRate,
    UnresolvedTier,
    IdentityMismatch,
    InvalidLegacyUsage,
}

/// The origin of a reported or selected pricing rate.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CostSourceKind {
    ProviderReported,
    LegacyProviderReported,
    ModelsDevCatalog,
    ManualOverride,
}

/// Freshness captured with a historical estimate's selected source.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CostSourceFreshness {
    Fresh,
    Stale,
    RefreshFailed,
    NotApplicable,
}

/// Immutable provenance for one reported amount or selected estimate rate.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct CostSourceRef {
    pub source_kind: CostSourceKind,
    pub rate_revision_id: Option<String>,
    pub catalog_snapshot_id: Option<String>,
    pub catalog_digest: Option<String>,
    pub effective_at: Option<String>,
    pub fetched_at: Option<String>,
    pub freshness: CostSourceFreshness,
    pub retrospective: bool,
    pub formula_revision: Option<String>,
}

/// Run/turn and provider-attempt coverage dimensions for one cost summary.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct UsageCostCoverage {
    #[ts(type = "number")]
    pub total_runs_or_turns: i64,
    #[ts(type = "number")]
    pub pending_runs_or_turns: i64,
    #[ts(type = "number")]
    pub no_provider_call_runs_or_turns: i64,
    #[ts(type = "number")]
    pub fully_metered_runs_or_turns: i64,
    #[ts(type = "number")]
    pub fully_costed_runs_or_turns: i64,
    #[ts(type = "number")]
    pub partially_costed_runs_or_turns: i64,
    #[ts(type = "number")]
    pub unavailable_cost_runs_or_turns: i64,
    #[ts(type = "number")]
    pub total_provider_attempts: i64,
    #[ts(type = "number")]
    pub settled_provider_attempts: i64,
    #[ts(type = "number")]
    pub pending_provider_attempts: i64,
    #[ts(type = "number")]
    pub unsettled_provider_attempts: i64,
    #[ts(type = "number")]
    pub metered_provider_attempts: i64,
    #[ts(type = "number")]
    pub unmetered_provider_attempts: i64,
    #[ts(type = "number")]
    pub costed_provider_attempts: i64,
    #[ts(type = "number")]
    pub unpriced_provider_attempts: i64,
    pub priced_tokens: TokenCounters,
    pub unpriced_tokens: TokenCounters,
    pub reasons: Vec<CostCoverageReason>,
}

/// Counts and token evidence attached to one coverage reason.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct CostCoverageReason {
    pub code: CostCoverageReasonCode,
    #[ts(type = "number")]
    pub run_or_turn_count: i64,
    #[ts(type = "number")]
    pub provider_attempt_count: i64,
    pub tokens: TokenCounters,
}

/// One normalized summary of reported/estimated money and its coverage.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct CostSummary {
    pub kind: CostKind,
    pub coverage: CostCoverage,
    pub provider_reported: Option<MoneyAmount>,
    pub estimated: Option<MoneyAmount>,
    pub known_subtotal: Option<MoneyAmount>,
    pub complete_total: Option<MoneyAmount>,
    pub usage_coverage: UsageCostCoverage,
    pub sources: Vec<CostSourceRef>,
}

/// Attribution captured when a usage event is admitted.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct UsageAttribution {
    pub pricing_subject_revision: Option<String>,
    pub agent_id: Option<String>,
    pub profile_id: Option<String>,
    pub executor_type: Option<String>,
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
    pub candidate_key: Option<String>,
    #[ts(type = "number")]
    pub attempt_ordinal: i64,
}

/// Product surface that produced one provider invocation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum UsageSurface {
    TaskExecution,
    ProjectChat,
    MainChat,
    GenesisChat,
    MainInquiry,
}

/// Token telemetry/settlement state for one usage invocation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum UsageTelemetryState {
    Metered,
    Unmetered,
    Pending,
    Unsettled,
}

/// A normalized invocation/event row used by execution observability and
/// typed chat usage responses.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct UsageBreakdown {
    pub invocation_id: String,
    pub usage_event_id: Option<String>,
    pub surface: UsageSurface,
    pub telemetry_state: UsageTelemetryState,
    pub attribution: UsageAttribution,
    pub counters: Option<TokenCounters>,
    #[ts(type = "number | null")]
    pub context_tokens: Option<i64>,
    pub selected_tier: Option<String>,
    pub occurred_at: String,
    pub cost: CostSummary,
}

/// Activity dimensions preserved independently rather than merged into an
/// ambiguous execution count.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ActivityCounts {
    #[ts(type = "number")]
    pub task_execution_count: i64,
    #[ts(type = "number")]
    pub chat_turn_count: i64,
    #[ts(type = "number")]
    pub inquiry_count: i64,
    #[ts(type = "number")]
    pub provider_attempt_count: i64,
}

/// Shared aggregate used by every analytics grouping.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct UsageAggregate {
    pub counts: ActivityCounts,
    pub tokens: TokenCounters,
    pub cost: CostSummary,
}

/// Aggregate usage for one product surface.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct SurfaceUsageBreakdown {
    pub surface: UsageSurface,
    pub counts: ActivityCounts,
    pub tokens: TokenCounters,
    pub cost: CostSummary,
}

/// Aggregate usage for one exact provider/model pair.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ModelUsageBreakdown {
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
    pub counts: ActivityCounts,
    pub tokens: TokenCounters,
    pub cost: CostSummary,
}

/// Aggregate usage for one immutable agent/profile attribution.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AgentUsageBreakdown {
    pub agent_id: Option<String>,
    pub agent_name_snapshot: Option<String>,
    pub profile_id: Option<String>,
    pub executor_type: Option<String>,
    pub counts: ActivityCounts,
    pub tokens: TokenCounters,
    pub cost: CostSummary,
}

/// Aggregate usage for a Project grouping in account analytics.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ProjectUsageBreakdown {
    pub project_id: Option<String>,
    pub project_name_snapshot: Option<String>,
    pub counts: ActivityCounts,
    pub tokens: TokenCounters,
    pub cost: CostSummary,
}

/// Immutable window used by Project and account analytics.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AnalyticsWindow {
    pub from: Option<String>,
    pub to: Option<String>,
}

/// Usage and cost across all surfaces visible to the caller.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct UsageAnalytics {
    pub counts: ActivityCounts,
    pub tokens: TokenCounters,
    pub cost: CostSummary,
    pub by_surface: Vec<SurfaceUsageBreakdown>,
    pub by_model: Vec<ModelUsageBreakdown>,
    pub by_agent: Vec<AgentUsageBreakdown>,
}

/// Canonical outcome metric for the cost of immutable released milestones.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum OutcomeKind {
    ReleasedMilestone,
}

/// Eligibility of a cost-per-outcome result.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum OutcomeEligibility {
    Eligible,
    NoOutcomes,
    IncompleteCost,
    PendingCost,
}

/// Why a released-milestone outcome metric is not eligible.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum OutcomeIneligibilityReason {
    NoReleasedMilestones,
    NoUsageCost,
    CostPending,
    CostPartial,
    CostUnavailable,
}

/// Project and half-open time-window scope for an outcome metric.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct OutcomeCostScope {
    pub project_id: String,
    pub from: Option<String>,
    pub to: Option<String>,
}

/// Cost per successful immutable released-milestone snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct OutcomeCostMetric {
    pub outcome_kind: OutcomeKind,
    pub numerator: Option<MoneyAmount>,
    #[ts(type = "number")]
    pub denominator: i64,
    pub amount_per_outcome: Option<MoneyAmount>,
    pub scope: OutcomeCostScope,
    pub eligibility: OutcomeEligibility,
    pub ineligibility_reason: Option<OutcomeIneligibilityReason>,
}

/// The status of the active server-owned models.dev snapshot.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum PricingCatalogState {
    Absent,
    Fresh,
    Stale,
    RefreshFailed,
}

/// Current server-side catalog status and last-known-good metadata.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct PricingCatalogStatus {
    pub state: PricingCatalogState,
    pub active_snapshot_id: Option<String>,
    pub revision: Option<String>,
    pub etag: Option<String>,
    pub fetched_at: Option<String>,
    pub last_checked_at: Option<String>,
    pub stale_after: Option<String>,
    pub last_error_code: Option<String>,
}

/// Explicit authorized catalog refresh request.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct PricingCatalogRefreshRequest {
    pub idempotency_key: String,
}

/// Source kind for a catalog model row.  Catalog rows never use a provider
/// entry's manual override source.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CatalogModelSourceKind {
    ModelsDevCatalog,
}

/// One exact provider-scoped models.dev model rate row.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct CatalogModelRate {
    pub snapshot_id: String,
    pub rate_revision_id: String,
    pub provider_id: String,
    pub model_id: String,
    pub rates: RateBuckets,
    /// Source-preserved exact context-tier bands. Unknown sibling fields are
    /// not promoted into this public contract.
    #[ts(type = "unknown")]
    pub tiers: Value,
    pub source_last_updated: Option<String>,
    pub source_kind: CatalogModelSourceKind,
}

/// Query parameters for the opaque-cursor catalog-model listing.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct PricingCatalogModelsQuery {
    #[ts(type = "number | null")]
    pub limit: Option<i64>,
    pub cursor: Option<String>,
    pub provider_id: Option<String>,
    pub query: Option<String>,
}

/// Cursor-paginated catalog model rates.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct PricingCatalogModelsResponse {
    pub items: Vec<CatalogModelRate>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
}

/// Which exact rate source a provider/runtime binding uses.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum PricingBindingSourceKind {
    ModelsDevCatalog,
    ManualOverride,
}

/// Exact runtime-model pricing binding and its immutable rate provenance.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct PricingBinding {
    pub id: String,
    pub runtime_model: String,
    pub subject_revision_digest: String,
    pub source_kind: PricingBindingSourceKind,
    pub catalog_provider_id: Option<String>,
    pub catalog_model_id: Option<String>,
    pub catalog_rate_revision_id: Option<String>,
    pub manual_rates: Option<RateBuckets>,
    pub effective_at: String,
    pub retired_at: Option<String>,
    #[ts(type = "number")]
    pub version: i64,
}

/// Provider entry or discovered CLI runtime pricing configuration.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ProviderPricing {
    pub subject_id: String,
    pub subject_revision_digest: String,
    #[ts(type = "number")]
    pub version: i64,
    pub bindings: Vec<PricingBinding>,
}

/// Desired exact binding in a replace-all pricing mutation.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ReplaceProviderPricingBinding {
    pub runtime_model: String,
    pub source_kind: PricingBindingSourceKind,
    pub catalog_provider_id: Option<String>,
    pub catalog_model_id: Option<String>,
    pub catalog_rate_revision_id: Option<String>,
    pub manual_rates: Option<RateBuckets>,
}

/// Replace the complete exact binding set for a provider/runtime subject.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ReplaceProviderPricingRequest {
    #[ts(type = "number")]
    pub expected_version: i64,
    pub idempotency_key: String,
    pub subject_revision_digest: String,
    pub bindings: Vec<ReplaceProviderPricingBinding>,
}

/// Request an explicit retrospective cost-estimation preview for one snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct CreateCostEstimationPreviewRequest {
    pub snapshot_id: String,
    pub from: Option<String>,
    pub to: Option<String>,
    pub idempotency_key: String,
}

/// Preview of an explicit retrospective estimate against one snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct CostEstimationPreview {
    pub id: String,
    pub project_id: String,
    pub snapshot_id: String,
    pub usage_set_digest: String,
    #[ts(type = "number")]
    pub eligible_event_count: i64,
    #[ts(type = "number")]
    pub unmatched_event_count: i64,
    #[ts(type = "number")]
    pub already_reported_event_count: i64,
    pub projected_cost: CostSummary,
    pub expires_at: String,
}

/// Commit one exact, immutable retrospective preview.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct CreateCostEstimationRunRequest {
    pub preview_id: String,
    pub usage_set_digest: String,
    pub idempotency_key: String,
}

/// Lifecycle status of a retrospective estimation run.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CostEstimationRunStatus {
    Pending,
    Committed,
    Failed,
    Conflicted,
    Superseded,
}

/// Durable result/provenance of a retrospective estimation run.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct CostEstimationRun {
    pub id: String,
    pub project_id: String,
    pub preview_id: String,
    pub snapshot_id: String,
    pub usage_set_digest: String,
    pub status: CostEstimationRunStatus,
    #[ts(type = "number")]
    pub applied_event_count: i64,
    #[ts(type = "number")]
    pub unmatched_event_count: i64,
    pub cost: CostSummary,
    pub created_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn money_and_rate_amounts_are_decimal_strings() {
        let amount = MoneyAmount {
            currency: "USD".to_owned(),
            decimal: "0.000000001".to_owned(),
        };
        let rate = RateAmount {
            currency: "USD".to_owned(),
            decimal_per_million: "1.25".to_owned(),
        };

        assert_eq!(
            serde_json::to_value(amount).expect("money serializes"),
            json!({"currency": "USD", "decimal": "0.000000001"})
        );
        assert_eq!(
            serde_json::to_value(rate).expect("rate serializes"),
            json!({"currency": "USD", "decimal_per_million": "1.25"})
        );
    }

    #[test]
    fn cost_estimation_preview_request_preserves_rfc3339_offsets_as_strings() {
        let request = CreateCostEstimationPreviewRequest {
            snapshot_id: "snapshot-1".to_owned(),
            from: Some("2026-09-07T12:34:56-04:00".to_owned()),
            to: Some("2026-09-08T16:00:00+00:00".to_owned()),
            idempotency_key: "retry-key".to_owned(),
        };

        assert_eq!(
            serde_json::to_value(request).expect("preview request serializes"),
            json!({
                "snapshot_id": "snapshot-1",
                "from": "2026-09-07T12:34:56-04:00",
                "to": "2026-09-08T16:00:00+00:00",
                "idempotency_key": "retry-key"
            })
        );
    }

    #[test]
    fn catalog_model_rate_keeps_snapshot_and_rate_revision_ids_distinct() {
        let encoded = serde_json::to_value(CatalogModelRate {
            snapshot_id: "snapshot-opaque".to_owned(),
            rate_revision_id: "rate-revision-opaque".to_owned(),
            provider_id: "provider-opaque".to_owned(),
            model_id: "model-opaque".to_owned(),
            rates: RateBuckets {
                input: None,
                output: None,
                cache_read: None,
                cache_write: None,
            },
            tiers: json!({}),
            source_last_updated: None,
            source_kind: CatalogModelSourceKind::ModelsDevCatalog,
        })
        .expect("catalog model rate serializes");

        assert_eq!(encoded["snapshot_id"], json!("snapshot-opaque"));
        assert_eq!(encoded["rate_revision_id"], json!("rate-revision-opaque"));
        assert_ne!(encoded["snapshot_id"], encoded["rate_revision_id"]);
    }

    #[test]
    fn closed_vocabularies_use_frozen_wire_names() {
        assert_eq!(
            serde_json::to_value(CostKind::ProviderReported).expect("kind serializes"),
            json!("provider_reported")
        );
        assert_eq!(
            serde_json::to_value(CostCoverageReasonCode::InvalidLegacyUsage)
                .expect("reason serializes"),
            json!("invalid_legacy_usage")
        );
        assert_eq!(
            serde_json::to_value(UsageSurface::MainInquiry).expect("surface serializes"),
            json!("main_inquiry")
        );
    }

    #[test]
    fn cost_summary_has_no_ambiguous_cost_usd_field() {
        let encoded = serde_json::to_value(CostSummary {
            kind: CostKind::None,
            coverage: CostCoverage::NoUsage,
            provider_reported: None,
            estimated: None,
            known_subtotal: None,
            complete_total: None,
            usage_coverage: UsageCostCoverage {
                total_runs_or_turns: 0,
                pending_runs_or_turns: 0,
                no_provider_call_runs_or_turns: 0,
                fully_metered_runs_or_turns: 0,
                fully_costed_runs_or_turns: 0,
                partially_costed_runs_or_turns: 0,
                unavailable_cost_runs_or_turns: 0,
                total_provider_attempts: 0,
                settled_provider_attempts: 0,
                pending_provider_attempts: 0,
                unsettled_provider_attempts: 0,
                metered_provider_attempts: 0,
                unmetered_provider_attempts: 0,
                costed_provider_attempts: 0,
                unpriced_provider_attempts: 0,
                priced_tokens: TokenCounters {
                    input_tokens: 0,
                    output_tokens: 0,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                },
                unpriced_tokens: TokenCounters {
                    input_tokens: 0,
                    output_tokens: 0,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                },
                reasons: Vec::new(),
            },
            sources: Vec::new(),
        })
        .expect("summary serializes");

        assert!(encoded.get("cost_usd").is_none());
        assert!(encoded.get("known_subtotal").is_some());
    }
}
