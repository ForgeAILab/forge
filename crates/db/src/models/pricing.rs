//! Pricing catalog, subject, binding, and selection records.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingCatalogSourceKind {
    ModelsDevCatalog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingCatalogStateKind {
    Absent,
    Fresh,
    Stale,
    RefreshFailed,
}

/// Frozen freshness provenance for a catalog-backed estimate preview.
/// Unlike [`PricingCatalogStateKind`], this intentionally excludes `absent`:
/// a retrospective preview must reference an immutable catalog snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingCatalogFreshness {
    Fresh,
    Stale,
    RefreshFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingRateSourceKind {
    ModelsDevCatalog,
    ManualOverride,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingSubjectKind {
    ProviderEntry,
    CliRuntime,
}

/// How an owner's price differs from the models.dev list price.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingAdjustmentMode {
    List,
    Discount,
    Fixed,
}

/// What a pricing adjustment applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PricingAdjustmentScope {
    ProviderEntry(String),
    CliRuntime {
        daemon_id: String,
        executor_type: String,
    },
    Agent(String),
}

impl PricingAdjustmentScope {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ProviderEntry(_) => "provider_entry",
            Self::CliRuntime { .. } => "cli_runtime",
            Self::Agent(_) => "agent",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingAdjustment {
    pub id: String,
    pub owner_user_id: String,
    pub scope: PricingAdjustmentScope,
    pub mode: PricingAdjustmentMode,
    /// Hundredths of a percent, present only for `Discount`.
    pub discount_bps: Option<i64>,
    /// Present only for `Fixed`.
    pub fixed_rates: RateBuckets,
    pub catalog_provider_id: Option<String>,
    pub catalog_model_id: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertPricingAdjustment {
    pub owner_user_id: String,
    pub scope: PricingAdjustmentScope,
    pub mode: PricingAdjustmentMode,
    pub discount_bps: Option<i64>,
    pub fixed_rates: RateBuckets,
    pub catalog_provider_id: Option<String>,
    pub catalog_model_id: Option<String>,
    /// `0` creates; otherwise the row must be at this version.
    pub expected_version: i64,
    pub now: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingSubjectState {
    Active,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingSelectionStatus {
    Priced,
    Unpriced,
    Invalid,
}

/// Provenance vocabulary used by admission selections and invocations. Usage
/// events use [`UsageEventProvenanceKind`] because their runtime spelling is
/// `runtime_report`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingAdmissionProvenanceKind {
    Runtime,
    LegacyExecutionAggregate,
    LegacyChat,
    LegacyInquiry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingDomainKind {
    Execution,
    Chat,
    Inquiry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageSurface {
    TaskExecution,
    ProjectChat,
    MainChat,
    GenesisChat,
    MainInquiry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageInvocationLifecycle {
    Admitted,
    Started,
    PendingSettlement,
    Settled,
    Unsettled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageTelemetryState {
    Pending,
    Metered,
    Unmetered,
    Unsettled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageEventReportMode {
    Delta,
    FinalSnapshot,
    ReportedMoney,
    LegacyAggregate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageEventProvenanceKind {
    RuntimeReport,
    LegacyExecutionAggregate,
    LegacyChat,
    LegacyInquiry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageCostKind {
    ProviderReported,
    Estimated,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CostEstimationPreviewStatus {
    Active,
    Expired,
    Committed,
    Invalidated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CostEstimationRunStatus {
    Pending,
    Committed,
    Failed,
    Conflicted,
    Superseded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CostEstimateRevisionState {
    Applied,
    Unmatched,
    Invalid,
}

enum_strings!(PricingCatalogSourceKind {
    ModelsDevCatalog => "models_dev_catalog",
});
enum_strings!(PricingCatalogStateKind {
    Absent => "absent",
    Fresh => "fresh",
    Stale => "stale",
    RefreshFailed => "refresh_failed",
});
enum_strings!(PricingCatalogFreshness {
    Fresh => "fresh",
    Stale => "stale",
    RefreshFailed => "refresh_failed",
});
enum_strings!(PricingRateSourceKind {
    ModelsDevCatalog => "models_dev_catalog",
    ManualOverride => "manual_override",
});
enum_strings!(PricingSubjectKind {
    ProviderEntry => "provider_entry",
    CliRuntime => "cli_runtime",
});
enum_strings!(PricingAdjustmentMode {
    List => "list",
    Discount => "discount",
    Fixed => "fixed",
});
enum_strings!(PricingSubjectState {
    Active => "active",
    Retired => "retired",
});
enum_strings!(PricingSelectionStatus {
    Priced => "priced",
    Unpriced => "unpriced",
    Invalid => "invalid",
});
enum_strings!(PricingAdmissionProvenanceKind {
    Runtime => "runtime",
    LegacyExecutionAggregate => "legacy_execution_aggregate",
    LegacyChat => "legacy_chat",
    LegacyInquiry => "legacy_inquiry",
});
enum_strings!(PricingDomainKind {
    Execution => "execution",
    Chat => "chat",
    Inquiry => "inquiry",
});
enum_strings!(UsageSurface {
    TaskExecution => "task_execution",
    ProjectChat => "project_chat",
    MainChat => "main_chat",
    GenesisChat => "genesis_chat",
    MainInquiry => "main_inquiry",
});
enum_strings!(UsageInvocationLifecycle {
    Admitted => "admitted",
    Started => "started",
    PendingSettlement => "pending_settlement",
    Settled => "settled",
    Unsettled => "unsettled",
});
enum_strings!(UsageTelemetryState {
    Pending => "pending",
    Metered => "metered",
    Unmetered => "unmetered",
    Unsettled => "unsettled",
});
enum_strings!(UsageEventReportMode {
    Delta => "delta",
    FinalSnapshot => "final_snapshot",
    ReportedMoney => "reported_money",
    LegacyAggregate => "legacy_aggregate",
});
enum_strings!(UsageEventProvenanceKind {
    RuntimeReport => "runtime_report",
    LegacyExecutionAggregate => "legacy_execution_aggregate",
    LegacyChat => "legacy_chat",
    LegacyInquiry => "legacy_inquiry",
});
enum_strings!(UsageCostKind {
    ProviderReported => "provider_reported",
    Estimated => "estimated",
    None => "none",
});
enum_strings!(CostCoverageReasonCode {
    Pending => "pending",
    Unsettled => "unsettled",
    Unmetered => "unmetered",
    MissingProvider => "missing_provider",
    MissingModel => "missing_model",
    MissingBinding => "missing_binding",
    MissingRate => "missing_rate",
    UnresolvedTier => "unresolved_tier",
    IdentityMismatch => "identity_mismatch",
    InvalidLegacyUsage => "invalid_legacy_usage",
});
enum_strings!(CostEstimationPreviewStatus {
    Active => "active",
    Expired => "expired",
    Committed => "committed",
    Invalidated => "invalidated",
});
enum_strings!(CostEstimationRunStatus {
    Pending => "pending",
    Committed => "committed",
    Failed => "failed",
    Conflicted => "conflicted",
    Superseded => "superseded",
});
enum_strings!(CostEstimateRevisionState {
    Applied => "applied",
    Unmatched => "unmatched",
    Invalid => "invalid",
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingCatalogSnapshot {
    pub id: String,
    pub source_kind: PricingCatalogSourceKind,
    pub source_url: String,
    pub http_etag: Option<String>,
    pub payload_sha256: String,
    pub parser_revision: String,
    pub revision_digest: String,
    pub payload_json: String,
    pub fetched_at: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePricingCatalogSnapshot {
    pub id: String,
    pub source_kind: PricingCatalogSourceKind,
    pub source_url: String,
    pub http_etag: Option<String>,
    pub payload_sha256: String,
    pub parser_revision: String,
    pub revision_digest: String,
    pub payload_json: String,
    pub fetched_at: String,
    pub created_at: String,
}

pub type CreateCatalogSnapshot = CreatePricingCatalogSnapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingCatalogState {
    pub id: String,
    pub active_snapshot_id: Option<String>,
    pub state: PricingCatalogStateKind,
    pub http_etag: Option<String>,
    pub last_checked_at: Option<String>,
    pub last_successful_check_at: Option<String>,
    pub stale_after: Option<String>,
    pub last_error_code: Option<String>,
    pub last_idempotency_key: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePricingCatalogState {
    pub id: String,
    pub expected_version: i64,
    pub active_snapshot_id: Option<Option<String>>,
    pub state: PricingCatalogStateKind,
    pub http_etag: Option<Option<String>>,
    pub last_checked_at: Option<Option<String>>,
    pub last_successful_check_at: Option<Option<String>>,
    pub stale_after: Option<Option<String>>,
    pub last_error_code: Option<Option<String>>,
    pub last_idempotency_key: Option<Option<String>>,
    pub updated_at: String,
}

/// A successful `200`, successful `304`, or bounded failed check.  A 304
/// carries no new immutable snapshot; a failure retains the active pointer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordPricingCatalogCheck {
    pub id: String,
    pub expected_version: i64,
    pub state: PricingCatalogStateKind,
    pub active_snapshot_id: Option<Option<String>>,
    pub http_etag: Option<Option<String>>,
    pub checked_at: String,
    pub successful_check_at: Option<Option<String>>,
    pub stale_after: Option<Option<String>>,
    pub last_error_code: Option<Option<String>>,
    pub idempotency_key: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivatePricingCatalog {
    pub snapshot_id: String,
    pub expected_version: i64,
    pub http_etag: Option<String>,
    pub checked_at: String,
    pub stale_after: Option<String>,
    pub idempotency_key: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingCatalogModelRate {
    pub rate_revision_id: String,
    pub snapshot_id: String,
    pub provider_id: String,
    pub model_id: String,
    pub rates: RateBuckets,
    pub tiers_json: String,
    pub source_last_updated: Option<String>,
    pub source_kind: PricingRateSourceKind,
    pub rate_digest: String,
    pub effective_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingCatalogModelQuery {
    pub page: PageRequest,
    pub provider_id: Option<String>,
    pub query: Option<String>,
    pub snapshot_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingRateRevision {
    pub id: String,
    pub source_kind: PricingRateSourceKind,
    pub owner_user_id: Option<String>,
    pub catalog_snapshot_id: Option<String>,
    pub catalog_provider_id: Option<String>,
    pub catalog_model_id: Option<String>,
    pub pricing_subject_revision_id: Option<String>,
    pub pricing_subject_revision_digest: Option<String>,
    pub runtime_model: Option<String>,
    pub source_model_key: Option<String>,
    pub source_last_updated: Option<String>,
    pub currency: String,
    pub rates: RateBuckets,
    pub tiers_json: String,
    pub legacy_context_over_200k_json: Option<String>,
    pub context_tier_state: String,
    pub received_rates_json: String,
    pub rate_digest: String,
    pub effective_at: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePricingRateRevision {
    pub id: String,
    pub source_kind: PricingRateSourceKind,
    pub owner_user_id: Option<String>,
    pub catalog_snapshot_id: Option<String>,
    pub catalog_provider_id: Option<String>,
    pub catalog_model_id: Option<String>,
    pub pricing_subject_revision_id: Option<String>,
    pub pricing_subject_revision_digest: Option<String>,
    pub runtime_model: Option<String>,
    pub source_model_key: Option<String>,
    pub source_last_updated: Option<String>,
    pub currency: String,
    pub rates: RateBuckets,
    pub tiers_json: String,
    pub legacy_context_over_200k_json: Option<String>,
    pub context_tier_state: String,
    pub received_rates_json: String,
    pub rate_digest: String,
    pub effective_at: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingSubject {
    pub id: String,
    pub owner_user_id: String,
    pub subject_kind: PricingSubjectKind,
    pub provider_entry_id: Option<String>,
    pub daemon_id: Option<String>,
    pub executor_type: Option<String>,
    pub current_revision_id: Option<String>,
    pub state: PricingSubjectState,
    pub last_idempotency_key: Option<String>,
    pub last_update_digest: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePricingSubject {
    pub id: String,
    pub owner_user_id: String,
    pub subject_kind: PricingSubjectKind,
    pub provider_entry_id: Option<String>,
    pub daemon_id: Option<String>,
    pub executor_type: Option<String>,
    pub current_revision_id: Option<String>,
    pub state: PricingSubjectState,
    pub last_idempotency_key: Option<String>,
    pub last_update_digest: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePricingSubject {
    pub id: String,
    pub expected_version: i64,
    pub current_revision_id: Option<Option<String>>,
    pub state: Option<PricingSubjectState>,
    pub last_idempotency_key: Option<Option<String>>,
    pub last_update_digest: Option<Option<String>>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingSubjectRevision {
    pub id: String,
    pub subject_id: String,
    pub owner_user_id: Option<String>,
    pub revision: i64,
    pub revision_digest: String,
    pub subject_kind: PricingSubjectKind,
    pub provider_entry_id: Option<String>,
    pub daemon_id: Option<String>,
    pub executor_type: Option<String>,
    pub provider_kind: String,
    pub credential_method: String,
    pub endpoint_class: String,
    pub runtime_fingerprint: Option<String>,
    pub schema_revision: String,
    pub non_secret_identity_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePricingSubjectRevision {
    pub id: String,
    pub subject_id: String,
    pub owner_user_id: String,
    pub revision: i64,
    pub revision_digest: String,
    pub subject_kind: PricingSubjectKind,
    pub provider_entry_id: Option<String>,
    pub daemon_id: Option<String>,
    pub executor_type: Option<String>,
    pub provider_kind: String,
    pub credential_method: String,
    pub endpoint_class: String,
    pub runtime_fingerprint: Option<String>,
    pub schema_revision: String,
    pub non_secret_identity_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingSubjectBinding {
    pub id: String,
    pub owner_user_id: String,
    pub subject_id: String,
    pub subject_revision_id: String,
    pub subject_revision_digest: String,
    /// `''` for the provider-wide binding, `agent:<id>` for one agent's own.
    pub scope_key: String,
    pub runtime_model: String,
    pub source_kind: PricingRateSourceKind,
    pub catalog_provider_id: Option<String>,
    pub catalog_model_id: Option<String>,
    pub rate_revision_id: String,
    pub binding_digest: String,
    pub state: PricingSubjectState,
    pub version: i64,
    pub effective_at: String,
    pub retired_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePricingSubjectBinding {
    pub id: String,
    pub owner_user_id: String,
    pub subject_id: String,
    pub subject_revision_id: String,
    pub subject_revision_digest: String,
    pub scope_key: String,
    pub runtime_model: String,
    pub source_kind: PricingRateSourceKind,
    pub catalog_provider_id: Option<String>,
    pub catalog_model_id: Option<String>,
    pub rate_revision_id: String,
    pub binding_digest: String,
    pub effective_at: String,
    pub created_at: String,
    pub updated_at: String,
}

/// The exact active pricing source selected for one subject revision/model.
///
/// Keeping this joined value behind the DB repository boundary prevents
/// services from reimplementing manual-over-catalog precedence (and from
/// accidentally selecting a non-deterministic row when both sources exist).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPricingSubjectBinding {
    pub subject: PricingSubject,
    pub revision: PricingSubjectRevision,
    pub binding: Option<PricingSubjectBinding>,
    pub rate: Option<PricingRateRevision>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePricingSubjectBinding {
    pub id: String,
    pub expected_version: i64,
    pub source_kind: PricingRateSourceKind,
    pub catalog_provider_id: Option<String>,
    pub catalog_model_id: Option<String>,
    pub rate_revision_id: String,
    pub binding_digest: String,
    pub effective_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetirePricingSubjectBinding {
    pub id: String,
    pub expected_version: i64,
    pub retired_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingSelection {
    pub id: String,
    pub owner_user_id: Option<String>,
    pub project_id: Option<String>,
    pub domain_kind: PricingDomainKind,
    pub surface: UsageSurface,
    pub source_id: String,
    pub execution_id: Option<String>,
    pub task_id: Option<String>,
    pub candidate_key: Option<String>,
    pub attempt_ordinal: i64,
    pub invocation_id: Option<String>,
    pub subject_id: Option<String>,
    pub subject_revision_id: Option<String>,
    pub subject_revision_digest: Option<String>,
    pub binding_id: Option<String>,
    pub rate_revision_id: Option<String>,
    pub catalog_snapshot_id: Option<String>,
    /// Catalog freshness captured at admission. Manual selections use
    /// `not_applicable`; legacy rows may leave this null.
    pub catalog_freshness: Option<String>,
    pub runtime_model: Option<String>,
    pub admitted_provider_id: Option<String>,
    pub admitted_model_id: Option<String>,
    pub source_kind: Option<PricingRateSourceKind>,
    pub provenance_kind: PricingAdmissionProvenanceKind,
    pub selection_status: PricingSelectionStatus,
    /// Admission-time reason when this candidate is explicitly unpriced.
    /// This is immutable provenance, not a mutable settlement diagnosis.
    pub selection_reason: Option<CostCoverageReasonCode>,
    pub selection_digest: String,
    pub selected_at: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePricingSelection {
    pub id: String,
    pub owner_user_id: Option<String>,
    pub project_id: Option<String>,
    pub domain_kind: PricingDomainKind,
    pub surface: UsageSurface,
    pub source_id: String,
    pub execution_id: Option<String>,
    pub task_id: Option<String>,
    pub candidate_key: Option<String>,
    pub attempt_ordinal: i64,
    pub subject_id: Option<String>,
    pub subject_revision_id: Option<String>,
    pub subject_revision_digest: Option<String>,
    pub binding_id: Option<String>,
    pub rate_revision_id: Option<String>,
    pub catalog_snapshot_id: Option<String>,
    pub catalog_freshness: Option<String>,
    pub runtime_model: Option<String>,
    pub admitted_provider_id: Option<String>,
    pub admitted_model_id: Option<String>,
    pub source_kind: Option<PricingRateSourceKind>,
    pub provenance_kind: PricingAdmissionProvenanceKind,
    pub selection_status: PricingSelectionStatus,
    pub selection_reason: Option<CostCoverageReasonCode>,
    pub selection_digest: String,
    pub selected_at: String,
    pub created_at: String,
}
