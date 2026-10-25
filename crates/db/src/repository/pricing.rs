//! Pricing catalog, subjects, usage ledger, and retrospective repositories.

use super::*;

// -------------------------------------------------------------------------
// Provider pricing and usage ledger (V135)
// -------------------------------------------------------------------------

/// Immutable models.dev snapshots, their active-state pointer, and normalized
/// catalog rates.  The `_in_tx` methods are intentionally part of the public
/// DB contract so a catalog refresh can insert a complete snapshot/rate set
/// and activate it atomically.
#[async_trait]
pub trait PricingCatalogRepo: Send + Sync {
    async fn create_pricing_catalog_snapshot(
        &self,
        input: CreatePricingCatalogSnapshot,
    ) -> Result<PricingCatalogSnapshot>;
    async fn create_pricing_catalog_snapshot_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingCatalogSnapshot,
    ) -> Result<PricingCatalogSnapshot>;
    async fn get_pricing_catalog_snapshot(
        &self,
        id: &str,
    ) -> Result<Option<PricingCatalogSnapshot>>;
    async fn get_pricing_catalog_snapshot_by_revision(
        &self,
        revision_digest: &str,
    ) -> Result<Option<PricingCatalogSnapshot>>;
    async fn list_pricing_catalog_models(
        &self,
        query: PricingCatalogModelQuery,
    ) -> Result<Page<PricingCatalogModelRate>>;
    async fn get_pricing_catalog_state(&self) -> Result<Option<PricingCatalogState>>;
    async fn ensure_pricing_catalog_state(&self, now: &str) -> Result<PricingCatalogState>;
    async fn update_pricing_catalog_state(
        &self,
        input: UpdatePricingCatalogState,
    ) -> Result<PricingCatalogState>;
    async fn record_pricing_catalog_check(
        &self,
        input: RecordPricingCatalogCheck,
    ) -> Result<PricingCatalogState>;
    async fn activate_pricing_catalog(
        &self,
        input: ActivatePricingCatalog,
    ) -> Result<PricingCatalogState>;
    async fn activate_pricing_catalog_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: ActivatePricingCatalog,
    ) -> Result<PricingCatalogState>;
    async fn create_pricing_rate_revision(
        &self,
        input: CreatePricingRateRevision,
    ) -> Result<PricingRateRevision>;
    async fn create_pricing_rate_revision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingRateRevision,
    ) -> Result<PricingRateRevision>;
    async fn get_pricing_rate_revision(&self, id: &str) -> Result<Option<PricingRateRevision>>;
    async fn get_pricing_rate_revision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        id: &str,
    ) -> Result<Option<PricingRateRevision>>;
    async fn get_pricing_rate_revision_by_digest(
        &self,
        rate_digest: &str,
    ) -> Result<Option<PricingRateRevision>>;
    async fn get_catalog_rate_revision(
        &self,
        snapshot_id: &str,
        provider_id: &str,
        model_id: &str,
    ) -> Result<Option<PricingRateRevision>>;
    async fn list_pricing_rate_revisions_for_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Vec<PricingRateRevision>>;
    async fn list_pricing_rate_revisions_for_subject_model(
        &self,
        subject_revision_id: &str,
        runtime_model: &str,
    ) -> Result<Vec<PricingRateRevision>>;
}

/// Versioned non-secret pricing subjects, immutable subject revisions, and
/// exact runtime-model bindings.  Binding updates use `expected_version` and
/// return `DbError::VersionConflict` on a stale writer.
#[async_trait]
pub trait PricingSubjectRepo: Send + Sync {
    async fn create_pricing_subject(&self, input: CreatePricingSubject) -> Result<PricingSubject>;
    async fn create_pricing_subject_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingSubject,
    ) -> Result<PricingSubject>;
    async fn get_pricing_subject(&self, id: &str) -> Result<Option<PricingSubject>>;
    async fn get_pricing_subject_for_provider(
        &self,
        owner_user_id: &str,
        provider_entry_id: &str,
    ) -> Result<Option<PricingSubject>>;
    async fn get_pricing_subject_for_cli_runtime(
        &self,
        owner_user_id: &str,
        daemon_id: &str,
        executor_type: &str,
    ) -> Result<Option<PricingSubject>>;
    async fn list_pricing_subjects(&self, owner_user_id: &str) -> Result<Vec<PricingSubject>>;
    async fn update_pricing_subject(&self, input: UpdatePricingSubject) -> Result<PricingSubject>;
    async fn update_pricing_subject_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdatePricingSubject,
    ) -> Result<PricingSubject>;
    async fn create_pricing_subject_revision(
        &self,
        input: CreatePricingSubjectRevision,
    ) -> Result<PricingSubjectRevision>;
    async fn create_pricing_subject_revision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingSubjectRevision,
    ) -> Result<PricingSubjectRevision>;
    async fn get_pricing_subject_revision(
        &self,
        id: &str,
    ) -> Result<Option<PricingSubjectRevision>>;
    async fn list_pricing_subject_revisions(
        &self,
        subject_id: &str,
    ) -> Result<Vec<PricingSubjectRevision>>;
    async fn create_pricing_subject_binding(
        &self,
        input: CreatePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding>;
    async fn create_pricing_subject_binding_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding>;
    async fn get_pricing_subject_binding(&self, id: &str) -> Result<Option<PricingSubjectBinding>>;
    async fn get_active_pricing_subject_binding(
        &self,
        subject_id: &str,
        runtime_model: &str,
    ) -> Result<Option<PricingSubjectBinding>>;
    /// Resolves one subject's current revision and active exact binding in an
    /// existing transaction. Manual overrides take precedence over catalog
    /// bindings; the returned rate is validated against that binding.
    #[allow(clippy::too_many_arguments)]
    async fn resolve_active_pricing_subject_binding_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        owner_user_id: &str,
        provider_entry_id: Option<&str>,
        daemon_id: Option<&str>,
        executor_type: Option<&str>,
        scope_key: &str,
        runtime_model: &str,
    ) -> Result<Option<ResolvedPricingSubjectBinding>>;
    async fn get_pricing_adjustment(
        &self,
        owner_user_id: &str,
        scope: &PricingAdjustmentScope,
    ) -> Result<Option<PricingAdjustment>>;
    async fn get_pricing_adjustment_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        owner_user_id: &str,
        scope: &PricingAdjustmentScope,
    ) -> Result<Option<PricingAdjustment>>;
    /// Creates (`expected_version == 0`) or replaces the adjustment for one
    /// scope. A stale writer gets `DbError::VersionConflict`.
    async fn upsert_pricing_adjustment(
        &self,
        input: UpsertPricingAdjustment,
    ) -> Result<PricingAdjustment>;
    /// Removes one scope's adjustment so it falls back to its parent.
    async fn delete_pricing_adjustment(
        &self,
        owner_user_id: &str,
        scope: &PricingAdjustmentScope,
        expected_version: i64,
    ) -> Result<()>;
    async fn list_pricing_subject_bindings(
        &self,
        subject_id: &str,
        include_retired: bool,
    ) -> Result<Vec<PricingSubjectBinding>>;
    async fn update_pricing_subject_binding(
        &self,
        input: UpdatePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding>;
    async fn update_pricing_subject_binding_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdatePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding>;
    async fn retire_pricing_subject_binding(
        &self,
        input: RetirePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding>;
    async fn retire_pricing_subject_binding_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: RetirePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding>;
}

/// Admission-time immutable selections and the append-only invocation/event
/// ledger.  Invocation lifecycle and event insertion have transaction forms
/// for the terminal settlement boundary; top-level methods are convenience
/// wrappers that own a SQLite transaction.
#[async_trait]
pub trait UsageLedgerRepo: Send + Sync {
    async fn create_pricing_selection(
        &self,
        input: CreatePricingSelection,
    ) -> Result<PricingSelection>;
    async fn create_pricing_selection_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingSelection,
    ) -> Result<PricingSelection>;
    async fn get_pricing_selection(&self, id: &str) -> Result<Option<PricingSelection>>;
    async fn list_pricing_selections_for_source(
        &self,
        source_id: &str,
    ) -> Result<Vec<PricingSelection>>;
    async fn list_pricing_selections_for_source_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        source_id: &str,
    ) -> Result<Vec<PricingSelection>>;
    async fn create_usage_invocation(
        &self,
        input: CreateUsageInvocation,
    ) -> Result<UsageInvocation>;
    async fn create_usage_invocation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateUsageInvocation,
    ) -> Result<UsageInvocation>;
    async fn get_usage_invocation(&self, id: &str) -> Result<Option<UsageInvocation>>;
    async fn get_usage_invocation_by_idempotency(
        &self,
        domain_idempotency_key: &str,
    ) -> Result<Option<UsageInvocation>>;
    async fn list_usage_invocations_for_source(
        &self,
        source_id: &str,
    ) -> Result<Vec<UsageInvocation>>;
    async fn list_usage_invocations_for_source_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        source_id: &str,
    ) -> Result<Vec<UsageInvocation>>;
    /// Return invocation rows whose lifecycle still needs a durable outcome.
    /// Recovery uses this to distinguish an abandoned provider call from an
    /// invocation that already settled.
    async fn list_usage_invocations_needing_settlement(
        &self,
        limit: i64,
    ) -> Result<Vec<UsageInvocation>>;
    async fn list_usage_invocations_for_project(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<UsageInvocation>>;
    async fn start_usage_invocation(&self, input: StartUsageInvocation) -> Result<UsageInvocation>;
    async fn start_usage_invocation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: StartUsageInvocation,
    ) -> Result<UsageInvocation>;
    async fn mark_usage_invocation_pending_settlement(
        &self,
        input: MarkUsageInvocationPendingSettlement,
    ) -> Result<UsageInvocation>;
    async fn mark_usage_invocation_pending_settlement_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: MarkUsageInvocationPendingSettlement,
    ) -> Result<UsageInvocation>;
    async fn settle_usage_invocation(
        &self,
        input: SettleUsageInvocation,
    ) -> Result<UsageInvocation>;
    async fn settle_usage_invocation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: SettleUsageInvocation,
    ) -> Result<UsageInvocation>;
    async fn mark_usage_invocation_unsettled(
        &self,
        input: MarkUsageInvocationUnsettled,
    ) -> Result<UsageInvocation>;
    async fn mark_usage_invocation_unsettled_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: MarkUsageInvocationUnsettled,
    ) -> Result<UsageInvocation>;
    async fn append_usage_event(&self, input: CreateUsageEvent) -> Result<UsageEvent>;
    async fn append_usage_event_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateUsageEvent,
    ) -> Result<UsageEvent>;
    /// Settle one or more started/pending invocations and append their events
    /// atomically. This is the late-drain path for a cancelled execution.
    async fn settle_usage_invocations_with_events(
        &self,
        settlements: Vec<UsageLedgerSettlement>,
    ) -> Result<Vec<UsageInvocation>>;
    async fn get_usage_event(&self, id: &str) -> Result<Option<UsageEvent>>;
    async fn get_usage_event_by_idempotency(
        &self,
        event_idempotency_key: &str,
    ) -> Result<Option<UsageEvent>>;
    async fn list_usage_events_for_invocation(
        &self,
        invocation_id: &str,
    ) -> Result<Vec<UsageEvent>>;
    async fn list_usage_events_for_invocation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        invocation_id: &str,
    ) -> Result<Vec<UsageEvent>>;
    async fn list_usage_events_for_project(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<UsageEvent>>;
    async fn list_usage_events_for_owner(
        &self,
        owner_user_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<UsageEvent>>;
}

/// Explicit, immutable retrospective estimation records.  Preview/run
/// envelopes are versioned and idempotent; per-event estimate revisions are
/// append-only and never overwrite provider-reported usage events.
#[async_trait]
pub trait RetrospectiveEstimateRepo: Send + Sync {
    async fn create_cost_estimation_preview(
        &self,
        input: CreateCostEstimationPreview,
    ) -> Result<CostEstimationPreview>;
    async fn create_cost_estimation_preview_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateCostEstimationPreview,
    ) -> Result<CostEstimationPreview>;
    async fn get_cost_estimation_preview(&self, id: &str) -> Result<Option<CostEstimationPreview>>;
    async fn get_cost_estimation_preview_by_idempotency(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<CostEstimationPreview>>;
    async fn list_cost_estimation_previews(
        &self,
        project_id: &str,
    ) -> Result<Vec<CostEstimationPreview>>;
    async fn update_cost_estimation_preview(
        &self,
        input: UpdateCostEstimationPreview,
    ) -> Result<CostEstimationPreview>;
    async fn update_cost_estimation_preview_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdateCostEstimationPreview,
    ) -> Result<CostEstimationPreview>;
    async fn create_cost_estimation_run(
        &self,
        input: CreateCostEstimationRun,
    ) -> Result<CostEstimationRun>;
    async fn create_cost_estimation_run_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateCostEstimationRun,
    ) -> Result<CostEstimationRun>;
    async fn get_cost_estimation_run(&self, id: &str) -> Result<Option<CostEstimationRun>>;
    async fn get_cost_estimation_run_by_idempotency(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<CostEstimationRun>>;
    async fn list_cost_estimation_runs(&self, project_id: &str) -> Result<Vec<CostEstimationRun>>;
    async fn update_cost_estimation_run(
        &self,
        input: UpdateCostEstimationRun,
    ) -> Result<CostEstimationRun>;
    async fn update_cost_estimation_run_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdateCostEstimationRun,
    ) -> Result<CostEstimationRun>;
    async fn create_cost_estimate_revision(
        &self,
        input: CreateCostEstimateRevision,
    ) -> Result<CostEstimateRevision>;
    async fn create_cost_estimate_revision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateCostEstimateRevision,
    ) -> Result<CostEstimateRevision>;
    async fn get_cost_estimate_revision(&self, id: &str) -> Result<Option<CostEstimateRevision>>;
    async fn get_cost_estimate_revision_by_digest(
        &self,
        estimate_digest: &str,
    ) -> Result<Option<CostEstimateRevision>>;
    async fn list_cost_estimate_revisions_for_run(
        &self,
        run_id: &str,
    ) -> Result<Vec<CostEstimateRevision>>;
    /// Every revision for every event of one invocation, grouped by event and
    /// newest first within each event.
    async fn list_cost_estimate_revisions_for_invocation(
        &self,
        invocation_id: &str,
    ) -> Result<Vec<CostEstimateRevision>>;
}
