//! SQLite persistence for the V135 pricing catalog and usage ledger.
//!
//! The tables intentionally keep immutable provenance separate from mutable
//! pointers (catalog state, subject bindings, and estimation envelopes).
//! Every mutating operation has a transaction form so domain services can
//! compose admission/settlement with their own state changes.

use super::*;
use crate::{models::*, repository::*};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

mod catalog;
mod ledger;
mod retrospective;
mod subject;
mod usage_read;

pub use usage_read::*;

/// Parse a usage-event occurrence timestamp as an instant.
///
/// `occurred_at` is persisted as RFC3339 text for portability, but lexical
/// ordering is not instant ordering when rows use different offsets.  Keep
/// this conversion at the repository boundary so all callers use the same
/// exact semantics and malformed legacy rows cannot silently disappear from
/// a retrospective window.
fn usage_event_instant(value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| DbError::InvalidTransition)
}

fn enum_value<T: FromStr<Err = String>>(row: &SqliteRow, column: &str) -> Result<T> {
    parse_enum(row.try_get(column)?)
}

fn map_catalog_snapshot(row: SqliteRow) -> Result<PricingCatalogSnapshot> {
    Ok(PricingCatalogSnapshot {
        id: row.try_get("id")?,
        source_kind: enum_value(&row, "source_kind")?,
        source_url: row.try_get("source_url")?,
        http_etag: row.try_get("http_etag")?,
        payload_sha256: row.try_get("payload_sha256")?,
        parser_revision: row.try_get("parser_revision")?,
        revision_digest: row.try_get("revision_digest")?,
        payload_json: row.try_get("payload_json")?,
        fetched_at: row.try_get("fetched_at")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_catalog_state(row: SqliteRow) -> Result<PricingCatalogState> {
    Ok(PricingCatalogState {
        id: row.try_get("id")?,
        active_snapshot_id: row.try_get("active_snapshot_id")?,
        state: enum_value(&row, "state")?,
        http_etag: row.try_get("http_etag")?,
        last_checked_at: row.try_get("last_checked_at")?,
        last_successful_check_at: row.try_get("last_successful_check_at")?,
        stale_after: row.try_get("stale_after")?,
        last_error_code: row.try_get("last_error_code")?,
        last_idempotency_key: row.try_get("last_idempotency_key")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_rate_revision(row: SqliteRow) -> Result<PricingRateRevision> {
    Ok(PricingRateRevision {
        id: row.try_get("id")?,
        source_kind: enum_value(&row, "source_kind")?,
        owner_user_id: row.try_get("owner_user_id")?,
        catalog_snapshot_id: row.try_get("catalog_snapshot_id")?,
        catalog_provider_id: row.try_get("catalog_provider_id")?,
        catalog_model_id: row.try_get("catalog_model_id")?,
        pricing_subject_revision_id: row.try_get("pricing_subject_revision_id")?,
        pricing_subject_revision_digest: row.try_get("pricing_subject_revision_digest")?,
        runtime_model: row.try_get("runtime_model")?,
        source_model_key: row.try_get("source_model_key")?,
        source_last_updated: row.try_get("source_last_updated")?,
        currency: row.try_get("currency")?,
        rates: RateBuckets::new(
            row.try_get("input_nano_usd_per_million")?,
            row.try_get("output_nano_usd_per_million")?,
            row.try_get("cache_read_nano_usd_per_million")?,
            row.try_get("cache_write_nano_usd_per_million")?,
        ),
        tiers_json: row.try_get("tiers_json")?,
        legacy_context_over_200k_json: row.try_get("legacy_context_over_200k_json")?,
        context_tier_state: row.try_get("context_tier_state")?,
        received_rates_json: row.try_get("received_rates_json")?,
        rate_digest: row.try_get("rate_digest")?,
        effective_at: row.try_get("effective_at")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_catalog_model_rate(row: SqliteRow) -> Result<PricingCatalogModelRate> {
    Ok(PricingCatalogModelRate {
        rate_revision_id: row.try_get("rate_revision_id")?,
        snapshot_id: row.try_get("snapshot_id")?,
        provider_id: row.try_get("provider_id")?,
        model_id: row.try_get("model_id")?,
        rates: RateBuckets::new(
            row.try_get("input_nano_usd_per_million")?,
            row.try_get("output_nano_usd_per_million")?,
            row.try_get("cache_read_nano_usd_per_million")?,
            row.try_get("cache_write_nano_usd_per_million")?,
        ),
        tiers_json: row.try_get("tiers_json")?,
        source_last_updated: row.try_get("source_last_updated")?,
        source_kind: enum_value(&row, "source_kind")?,
        rate_digest: row.try_get("rate_digest")?,
        effective_at: row.try_get("effective_at")?,
    })
}

fn map_subject(row: SqliteRow) -> Result<PricingSubject> {
    Ok(PricingSubject {
        id: row.try_get("id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        subject_kind: enum_value(&row, "subject_kind")?,
        provider_entry_id: row.try_get("provider_entry_id")?,
        daemon_id: row.try_get("daemon_id")?,
        executor_type: row.try_get("executor_type")?,
        current_revision_id: row.try_get("current_revision_id")?,
        state: enum_value(&row, "state")?,
        last_idempotency_key: row.try_get("last_idempotency_key")?,
        last_update_digest: row.try_get("last_update_digest")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_subject_revision(row: SqliteRow) -> Result<PricingSubjectRevision> {
    Ok(PricingSubjectRevision {
        id: row.try_get("id")?,
        subject_id: row.try_get("subject_id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        revision: row.try_get("revision")?,
        revision_digest: row.try_get("revision_digest")?,
        subject_kind: enum_value(&row, "subject_kind")?,
        provider_entry_id: row.try_get("provider_entry_id")?,
        daemon_id: row.try_get("daemon_id")?,
        executor_type: row.try_get("executor_type")?,
        provider_kind: row.try_get("provider_kind")?,
        credential_method: row.try_get("credential_method")?,
        endpoint_class: row.try_get("endpoint_class")?,
        runtime_fingerprint: row.try_get("runtime_fingerprint")?,
        schema_revision: row.try_get("schema_revision")?,
        non_secret_identity_json: row.try_get("non_secret_identity_json")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_adjustment(row: SqliteRow) -> Result<PricingAdjustment> {
    let scope_kind: String = row.try_get("scope_kind")?;
    let scope = match scope_kind.as_str() {
        "provider_entry" => {
            PricingAdjustmentScope::ProviderEntry(row.try_get("provider_entry_id")?)
        }
        "cli_runtime" => PricingAdjustmentScope::CliRuntime {
            daemon_id: row.try_get("daemon_id")?,
            executor_type: row.try_get("executor_type")?,
        },
        "agent" => PricingAdjustmentScope::Agent(row.try_get("agent_id")?),
        other => {
            return Err(DbError::Check(format!(
                "invalid pricing adjustment scope: {other}"
            )))
        }
    };
    Ok(PricingAdjustment {
        id: row.try_get("id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        scope,
        mode: enum_value(&row, "mode")?,
        discount_bps: row.try_get("discount_bps")?,
        fixed_rates: RateBuckets::new(
            row.try_get("input_nano_usd_per_million")?,
            row.try_get("output_nano_usd_per_million")?,
            row.try_get("cache_read_nano_usd_per_million")?,
            row.try_get("cache_write_nano_usd_per_million")?,
        ),
        catalog_provider_id: row.try_get("catalog_provider_id")?,
        catalog_model_id: row.try_get("catalog_model_id")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// `(provider_entry_id, daemon_id, executor_type, agent_id)` for a scope.
fn adjustment_scope_columns(
    scope: &PricingAdjustmentScope,
) -> (Option<&str>, Option<&str>, Option<&str>, Option<&str>) {
    match scope {
        PricingAdjustmentScope::ProviderEntry(id) => (Some(id), None, None, None),
        PricingAdjustmentScope::CliRuntime {
            daemon_id,
            executor_type,
        } => (None, Some(daemon_id), Some(executor_type), None),
        PricingAdjustmentScope::Agent(id) => (None, None, None, Some(id)),
    }
}

const ADJUSTMENT_SCOPE_FILTER: &str = "owner_user_id = ? AND scope_kind = ?
       AND provider_entry_id IS ? AND daemon_id IS ? AND executor_type IS ?
       AND agent_id IS ?";

async fn get_adjustment_on<'e, E>(
    executor: E,
    owner_user_id: &str,
    scope: &PricingAdjustmentScope,
) -> Result<Option<PricingAdjustment>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let (provider_entry_id, daemon_id, executor_type, agent_id) = adjustment_scope_columns(scope);
    sqlx::query(&format!(
        "SELECT * FROM pricing_adjustment WHERE {ADJUSTMENT_SCOPE_FILTER}"
    ))
    .bind(owner_user_id)
    .bind(scope.kind())
    .bind(provider_entry_id)
    .bind(daemon_id)
    .bind(executor_type)
    .bind(agent_id)
    .fetch_optional(executor)
    .await?
    .map(map_adjustment)
    .transpose()
}

fn map_binding(row: SqliteRow) -> Result<PricingSubjectBinding> {
    Ok(PricingSubjectBinding {
        id: row.try_get("id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        subject_id: row.try_get("subject_id")?,
        subject_revision_id: row.try_get("subject_revision_id")?,
        subject_revision_digest: row.try_get("subject_revision_digest")?,
        scope_key: row.try_get("scope_key")?,
        runtime_model: row.try_get("runtime_model")?,
        source_kind: enum_value(&row, "source_kind")?,
        catalog_provider_id: row.try_get("catalog_provider_id")?,
        catalog_model_id: row.try_get("catalog_model_id")?,
        rate_revision_id: row.try_get("rate_revision_id")?,
        binding_digest: row.try_get("binding_digest")?,
        state: enum_value(&row, "state")?,
        version: row.try_get("version")?,
        effective_at: row.try_get("effective_at")?,
        retired_at: row.try_get("retired_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) fn map_selection(row: SqliteRow) -> Result<PricingSelection> {
    Ok(PricingSelection {
        id: row.try_get("id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        project_id: row.try_get("project_id")?,
        domain_kind: enum_value(&row, "domain_kind")?,
        surface: enum_value(&row, "surface")?,
        source_id: row.try_get("source_id")?,
        execution_id: row.try_get("execution_id")?,
        task_id: row.try_get("task_id")?,
        candidate_key: row.try_get("candidate_key")?,
        attempt_ordinal: row.try_get("attempt_ordinal")?,
        invocation_id: row.try_get("invocation_id")?,
        subject_id: row.try_get("subject_id")?,
        subject_revision_id: row.try_get("subject_revision_id")?,
        subject_revision_digest: row.try_get("subject_revision_digest")?,
        binding_id: row.try_get("binding_id")?,
        rate_revision_id: row.try_get("rate_revision_id")?,
        catalog_snapshot_id: row.try_get("catalog_snapshot_id")?,
        catalog_freshness: row.try_get("catalog_freshness")?,
        runtime_model: row.try_get("runtime_model")?,
        admitted_provider_id: row.try_get("admitted_provider_id")?,
        admitted_model_id: row.try_get("admitted_model_id")?,
        source_kind: row
            .try_get::<Option<String>, _>("source_kind")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| DbError::InvalidTransition)?,
        provenance_kind: enum_value(&row, "provenance_kind")?,
        selection_status: enum_value(&row, "selection_status")?,
        selection_reason: row
            .try_get::<Option<String>, _>("selection_reason")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| DbError::InvalidTransition)?,
        selection_digest: row.try_get("selection_digest")?,
        selected_at: row.try_get("selected_at")?,
        created_at: row.try_get("created_at")?,
    })
}

pub(super) fn map_invocation(row: SqliteRow) -> Result<UsageInvocation> {
    Ok(UsageInvocation {
        id: row.try_get("id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        project_id: row.try_get("project_id")?,
        domain_kind: enum_value(&row, "domain_kind")?,
        surface: enum_value(&row, "surface")?,
        source_id: row.try_get("source_id")?,
        execution_id: row.try_get("execution_id")?,
        task_id: row.try_get("task_id")?,
        domain_idempotency_key: row.try_get("domain_idempotency_key")?,
        candidate_key: row.try_get("candidate_key")?,
        attempt_ordinal: row.try_get("attempt_ordinal")?,
        pricing_selection_id: row.try_get("pricing_selection_id")?,
        admitted_provider_id: row.try_get("admitted_provider_id")?,
        admitted_model_id: row.try_get("admitted_model_id")?,
        admitted_runtime_model: row.try_get("admitted_runtime_model")?,
        pricing_subject_id: row.try_get("pricing_subject_id")?,
        pricing_subject_revision_id: row.try_get("pricing_subject_revision_id")?,
        subject_revision_digest: row.try_get("subject_revision_digest")?,
        agent_id: row.try_get("agent_id")?,
        profile_id: row.try_get("profile_id")?,
        agent_name_snapshot: row.try_get("agent_name_snapshot")?,
        project_name_snapshot: row.try_get("project_name_snapshot")?,
        executor_type: row.try_get("executor_type")?,
        backend_kind: row.try_get("backend_kind")?,
        provenance_kind: enum_value(&row, "provenance_kind")?,
        lifecycle: enum_value(&row, "lifecycle")?,
        telemetry_state: enum_value(&row, "telemetry_state")?,
        terminal_reason: row.try_get("terminal_reason")?,
        version: row.try_get("version")?,
        admitted_at: row.try_get("admitted_at")?,
        started_at: row.try_get("started_at")?,
        settled_at: row.try_get("settled_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_usage_event(row: SqliteRow) -> Result<UsageEvent> {
    Ok(UsageEvent {
        id: row.try_get("id")?,
        invocation_id: row.try_get("invocation_id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        project_id: row.try_get("project_id")?,
        surface: enum_value(&row, "surface")?,
        source_id: row.try_get("source_id")?,
        execution_id: row.try_get("execution_id")?,
        task_id: row.try_get("task_id")?,
        event_idempotency_key: row.try_get("event_idempotency_key")?,
        source_report_id: row.try_get("source_report_id")?,
        report_sequence: row.try_get("report_sequence")?,
        report_mode: enum_value(&row, "report_mode")?,
        provenance_kind: enum_value(&row, "provenance_kind")?,
        legacy_source_table: row.try_get("legacy_source_table")?,
        legacy_source_id: row.try_get("legacy_source_id")?,
        legacy_provider_raw: row.try_get("legacy_provider_raw")?,
        legacy_provider_sqlite_type: row.try_get("legacy_provider_sqlite_type")?,
        legacy_provider_sql_literal: row.try_get("legacy_provider_sql_literal")?,
        legacy_model_raw: row.try_get("legacy_model_raw")?,
        legacy_model_sqlite_type: row.try_get("legacy_model_sqlite_type")?,
        legacy_model_sql_literal: row.try_get("legacy_model_sql_literal")?,
        legacy_counter_values_json: row.try_get("legacy_counter_values_json")?,
        legacy_cost_usd_raw: row.try_get("legacy_cost_usd_raw")?,
        legacy_created_at_raw: row.try_get("legacy_created_at_raw")?,
        legacy_project_owner_raw: row.try_get("legacy_project_owner_raw")?,
        legacy_invalid_usage: row.try_get::<i64, _>("legacy_invalid_usage")? != 0,
        provider_id: row.try_get("provider_id")?,
        model_id: row.try_get("model_id")?,
        runtime_model: row.try_get("runtime_model")?,
        candidate_key: row.try_get("candidate_key")?,
        attempt_ordinal: row.try_get("attempt_ordinal")?,
        agent_id: row.try_get("agent_id")?,
        profile_id: row.try_get("profile_id")?,
        agent_name_snapshot: row.try_get("agent_name_snapshot")?,
        project_name_snapshot: row.try_get("project_name_snapshot")?,
        executor_type: row.try_get("executor_type")?,
        pricing_subject_revision_id: row.try_get("pricing_subject_revision_id")?,
        subject_revision_digest: row.try_get("subject_revision_digest")?,
        telemetry_state: enum_value(&row, "telemetry_state")?,
        input_tokens: row.try_get("input_tokens")?,
        output_tokens: row.try_get("output_tokens")?,
        cache_read_tokens: row.try_get("cache_read_tokens")?,
        cache_write_tokens: row.try_get("cache_write_tokens")?,
        context_tokens: row.try_get("context_tokens")?,
        selected_tier: row.try_get("selected_tier")?,
        provider_reported_nano_usd: row.try_get("provider_reported_nano_usd")?,
        legacy_reported_cost_usd: row.try_get("legacy_reported_cost_usd")?,
        estimated_nano_usd: row.try_get("estimated_nano_usd")?,
        cost_kind: enum_value(&row, "cost_kind")?,
        rate_revision_id: row.try_get("rate_revision_id")?,
        catalog_snapshot_id: row.try_get("catalog_snapshot_id")?,
        formula_revision: row.try_get("formula_revision")?,
        retrospective: row.try_get::<i64, _>("retrospective")? != 0,
        coverage_reason_code: row
            .try_get::<Option<String>, _>("coverage_reason_code")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| DbError::InvalidTransition)?,
        occurred_at: row.try_get("occurred_at")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_preview(row: SqliteRow) -> Result<CostEstimationPreview> {
    Ok(CostEstimationPreview {
        id: row.try_get("id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        project_id: row.try_get("project_id")?,
        catalog_snapshot_id: row.try_get("catalog_snapshot_id")?,
        catalog_freshness: enum_value(&row, "catalog_freshness")?,
        usage_set_digest: row.try_get("usage_set_digest")?,
        window_from: row.try_get("window_from")?,
        window_to: row.try_get("window_to")?,
        filters_json: row.try_get("filters_json")?,
        eligible_event_count: row.try_get("eligible_event_count")?,
        unmatched_event_count: row.try_get("unmatched_event_count")?,
        already_reported_event_count: row.try_get("already_reported_event_count")?,
        projected_cost_summary_json: row.try_get("projected_cost_summary_json")?,
        status: enum_value(&row, "status")?,
        idempotency_key: row.try_get("idempotency_key")?,
        version: row.try_get("version")?,
        expires_at: row.try_get("expires_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_run(row: SqliteRow) -> Result<CostEstimationRun> {
    Ok(CostEstimationRun {
        id: row.try_get("id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        project_id: row.try_get("project_id")?,
        preview_id: row.try_get("preview_id")?,
        catalog_snapshot_id: row.try_get("catalog_snapshot_id")?,
        usage_set_digest: row.try_get("usage_set_digest")?,
        status: enum_value(&row, "status")?,
        applied_event_count: row.try_get("applied_event_count")?,
        unmatched_event_count: row.try_get("unmatched_event_count")?,
        already_reported_event_count: row.try_get("already_reported_event_count")?,
        cost_summary_json: row.try_get("cost_summary_json")?,
        idempotency_key: row.try_get("idempotency_key")?,
        supersedes_run_id: row.try_get("supersedes_run_id")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

fn map_estimate_revision(row: SqliteRow) -> Result<CostEstimateRevision> {
    Ok(CostEstimateRevision {
        id: row.try_get("id")?,
        run_id: row.try_get("run_id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        project_id: row.try_get("project_id")?,
        usage_event_id: row.try_get("usage_event_id")?,
        revision: row.try_get("revision")?,
        supersedes_revision_id: row.try_get("supersedes_revision_id")?,
        state: enum_value(&row, "state")?,
        rate_revision_id: row.try_get("rate_revision_id")?,
        catalog_snapshot_id: row.try_get("catalog_snapshot_id")?,
        estimated_nano_usd: row.try_get("estimated_nano_usd")?,
        formula_revision: row.try_get("formula_revision")?,
        retrospective: row.try_get::<i64, _>("retrospective")? != 0,
        reason_code: row
            .try_get::<Option<String>, _>("reason_code")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| DbError::InvalidTransition)?,
        estimate_digest: row.try_get("estimate_digest")?,
        created_at: row.try_get("created_at")?,
    })
}

fn catalog_snapshot_matches_input(
    existing: &PricingCatalogSnapshot,
    input: &CreatePricingCatalogSnapshot,
) -> bool {
    existing.id == input.id
        && existing.source_kind == input.source_kind
        && existing.source_url == input.source_url
        && existing.http_etag == input.http_etag
        && existing.payload_sha256 == input.payload_sha256
        && existing.parser_revision == input.parser_revision
        && existing.revision_digest == input.revision_digest
        && existing.payload_json == input.payload_json
        && existing.fetched_at == input.fetched_at
        && existing.created_at == input.created_at
}

fn rate_revision_matches_input(
    existing: &PricingRateRevision,
    input: &CreatePricingRateRevision,
) -> bool {
    existing.id == input.id
        && existing.source_kind == input.source_kind
        && existing.owner_user_id == input.owner_user_id
        && existing.catalog_snapshot_id == input.catalog_snapshot_id
        && existing.catalog_provider_id == input.catalog_provider_id
        && existing.catalog_model_id == input.catalog_model_id
        && existing.pricing_subject_revision_id == input.pricing_subject_revision_id
        && existing.pricing_subject_revision_digest == input.pricing_subject_revision_digest
        && existing.runtime_model == input.runtime_model
        && existing.source_model_key == input.source_model_key
        && existing.source_last_updated == input.source_last_updated
        && existing.currency == input.currency
        && existing.rates == input.rates
        && existing.tiers_json == input.tiers_json
        && existing.legacy_context_over_200k_json == input.legacy_context_over_200k_json
        && existing.context_tier_state == input.context_tier_state
        && existing.received_rates_json == input.received_rates_json
        && existing.rate_digest == input.rate_digest
        && existing.effective_at == input.effective_at
        && existing.created_at == input.created_at
}

fn subject_revision_matches_input(
    existing: &PricingSubjectRevision,
    input: &CreatePricingSubjectRevision,
) -> bool {
    existing.id == input.id
        && existing.subject_id == input.subject_id
        && existing.owner_user_id.as_deref() == Some(input.owner_user_id.as_str())
        && existing.revision == input.revision
        && existing.revision_digest == input.revision_digest
        && existing.subject_kind == input.subject_kind
        && existing.provider_entry_id == input.provider_entry_id
        && existing.daemon_id == input.daemon_id
        && existing.executor_type == input.executor_type
        && existing.provider_kind == input.provider_kind
        && existing.credential_method == input.credential_method
        && existing.endpoint_class == input.endpoint_class
        && existing.runtime_fingerprint == input.runtime_fingerprint
        && existing.schema_revision == input.schema_revision
        && existing.non_secret_identity_json == input.non_secret_identity_json
        && existing.created_at == input.created_at
}

fn conflict_or_not_found<T>(id: &str, expected: i64, actual: Option<i64>) -> Result<T> {
    if actual.is_some() {
        Err(DbError::VersionConflict)
    } else {
        let _ = (id, expected);
        Err(DbError::NotFound)
    }
}

async fn fetch_version(
    transaction: &mut Transaction<'_, Sqlite>,
    table: &str,
    id: &str,
) -> Result<Option<i64>> {
    // `table` is always a compile-time string from this module; it is not a
    // caller-provided SQL fragment.
    let sql = format!("SELECT version FROM {table} WHERE id = ?");
    Ok(sqlx::query_scalar::<_, i64>(&sql)
        .bind(id)
        .fetch_optional(&mut **transaction)
        .await?)
}

fn duplicate_error(error: sqlx::Error) -> DbError {
    if let sqlx::Error::Database(database_error) = &error {
        let message = database_error.message().to_ascii_lowercase();
        if message.contains("unique") {
            return DbError::IdempotencyConflict;
        }
    }
    check_error(error)
}

pub(crate) async fn select_invocation_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<Option<UsageInvocation>> {
    sqlx::query("SELECT * FROM usage_invocation WHERE id = ?")
        .bind(id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(check_error)?
        .map(map_invocation)
        .transpose()
}

/// Verify that a settlement submitted for an already-settled invocation is an
/// exact replay.  Lifecycle state alone is not an idempotency key: a changed
/// terminal reason, settlement timestamps, or any event field must surface as
/// an IdempotencyConflict instead of being silently accepted.
pub(crate) async fn validate_settled_usage_invocation_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    existing: &UsageInvocation,
    settlement: &UsageLedgerSettlement,
) -> Result<()> {
    if existing.lifecycle != UsageInvocationLifecycle::Settled
        || existing.telemetry_state != settlement.telemetry_state
        || existing.terminal_reason != settlement.terminal_reason
        || existing.settled_at.as_deref() != Some(settlement.settled_at.as_str())
        || existing.updated_at != settlement.updated_at
    {
        return Err(DbError::IdempotencyConflict);
    }
    let mut existing_events = sqlx::query(
        "SELECT * FROM usage_event
         WHERE invocation_id = ? ORDER BY id ASC",
    )
    .bind(&settlement.invocation_id)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(map_usage_event)
    .collect::<Result<Vec<_>>>()?;
    let mut expected_events = settlement.events.clone();
    // Compare a deterministic multiset pairing.  An `any(...)` comparison
    // can match the same stored event more than once when a malformed retry
    // repeats an event, which would weaken the full-payload replay contract.
    existing_events.sort_by(|left, right| left.id.cmp(&right.id));
    expected_events.sort_by(|left, right| left.id.cmp(&right.id));
    if existing_events.len() != expected_events.len()
        || existing_events
            .iter()
            .zip(expected_events.iter())
            .any(|(existing, input)| !usage_event_matches_input(existing, input))
    {
        return Err(DbError::IdempotencyConflict);
    }
    Ok(())
}

async fn select_usage_event_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<Option<UsageEvent>> {
    sqlx::query("SELECT * FROM usage_event WHERE id = ?")
        .bind(id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(check_error)?
        .map(map_usage_event)
        .transpose()
}

async fn list_events_with_scope(
    pool: &SqlitePool,
    project_id: Option<&str>,
    owner_user_id: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<Vec<UsageEvent>> {
    let from = from.map(usage_event_instant).transpose()?;
    let to = to.map(usage_event_instant).transpose()?;

    // Do not put RFC3339 bounds in SQLite's text predicate.  Offset-bearing
    // representations are not lexically ordered by instant, so a textual
    // prefilter could create false negatives.  Fetch the scoped rows and
    // apply the exact instant filter below.  The scope predicate remains in
    // SQL to avoid crossing account/project visibility boundaries.
    let mut builder = sqlx::QueryBuilder::<Sqlite>::new("SELECT * FROM usage_event WHERE ");
    if let Some(project_id) = project_id {
        builder.push("project_id = ").push_bind(project_id);
    } else if let Some(owner_user_id) = owner_user_id {
        builder.push("owner_user_id = ").push_bind(owner_user_id);
    } else {
        builder.push("1 = 1");
    }
    let mut events = builder
        .build()
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(map_usage_event)
        .map(|event| {
            let event = event?;
            let occurred_at = usage_event_instant(&event.occurred_at)?;
            Ok((event, occurred_at))
        })
        .collect::<Result<Vec<_>>>()?;

    events.retain(|(_, occurred_at)| {
        from.as_ref().is_none_or(|from| occurred_at >= from)
            && to.as_ref().is_none_or(|to| occurred_at < to)
    });
    events.sort_by(|(left, left_at), (right, right_at)| {
        left_at.cmp(right_at).then_with(|| left.id.cmp(&right.id))
    });
    Ok(events.into_iter().map(|(event, _)| event).collect())
}

fn invocation_matches_input(existing: &UsageInvocation, input: &CreateUsageInvocation) -> bool {
    existing.id == input.id
        && existing.owner_user_id == input.owner_user_id
        && existing.project_id == input.project_id
        && existing.domain_kind == input.domain_kind
        && existing.surface == input.surface
        && existing.source_id == input.source_id
        && existing.execution_id == input.execution_id
        && existing.task_id == input.task_id
        && existing.candidate_key == input.candidate_key
        && existing.attempt_ordinal == input.attempt_ordinal
        && existing.pricing_selection_id == input.pricing_selection_id
        && existing.admitted_provider_id == input.admitted_provider_id
        && existing.admitted_model_id == input.admitted_model_id
        && existing.admitted_runtime_model == input.admitted_runtime_model
        && existing.pricing_subject_id == input.pricing_subject_id
        && existing.pricing_subject_revision_id == input.pricing_subject_revision_id
        && existing.subject_revision_digest == input.subject_revision_digest
        && existing.agent_id == input.agent_id
        && existing.profile_id == input.profile_id
        && existing.agent_name_snapshot == input.agent_name_snapshot
        && existing.project_name_snapshot == input.project_name_snapshot
        && existing.executor_type == input.executor_type
        && existing.backend_kind == input.backend_kind
        && existing.provenance_kind == input.provenance_kind
        && existing.admitted_at == input.admitted_at
        && existing.created_at == input.created_at
}

fn pricing_selection_matches_input(
    existing: &PricingSelection,
    input: &CreatePricingSelection,
) -> bool {
    existing.id == input.id
        && existing.owner_user_id == input.owner_user_id
        && existing.project_id == input.project_id
        && existing.domain_kind == input.domain_kind
        && existing.surface == input.surface
        && existing.source_id == input.source_id
        && existing.execution_id == input.execution_id
        && existing.task_id == input.task_id
        && existing.candidate_key == input.candidate_key
        && existing.attempt_ordinal == input.attempt_ordinal
        && existing.subject_id == input.subject_id
        && existing.subject_revision_id == input.subject_revision_id
        && existing.subject_revision_digest == input.subject_revision_digest
        && existing.binding_id == input.binding_id
        && existing.rate_revision_id == input.rate_revision_id
        && existing.catalog_snapshot_id == input.catalog_snapshot_id
        && existing.catalog_freshness == input.catalog_freshness
        && existing.runtime_model == input.runtime_model
        && existing.admitted_provider_id == input.admitted_provider_id
        && existing.admitted_model_id == input.admitted_model_id
        && existing.source_kind == input.source_kind
        && existing.provenance_kind == input.provenance_kind
        && existing.selection_status == input.selection_status
        && existing.selection_reason == input.selection_reason
        && existing.selection_digest == input.selection_digest
        && existing.selected_at == input.selected_at
        && existing.created_at == input.created_at
}

fn usage_event_matches_input(existing: &UsageEvent, input: &CreateUsageEvent) -> bool {
    existing.id == input.id
        && existing.invocation_id == input.invocation_id
        && existing.owner_user_id == input.owner_user_id
        && existing.project_id == input.project_id
        && existing.surface == input.surface
        && existing.source_id == input.source_id
        && existing.execution_id == input.execution_id
        && existing.task_id == input.task_id
        && existing.event_idempotency_key == input.event_idempotency_key
        && existing.source_report_id == input.source_report_id
        && existing.report_sequence == input.report_sequence
        && existing.report_mode == input.report_mode
        && existing.provenance_kind == input.provenance_kind
        && existing.legacy_source_table == input.legacy_source_table
        && existing.legacy_source_id == input.legacy_source_id
        && existing.legacy_provider_raw == input.legacy_provider_raw
        && existing.legacy_provider_sqlite_type == input.legacy_provider_sqlite_type
        && existing.legacy_provider_sql_literal == input.legacy_provider_sql_literal
        && existing.legacy_model_raw == input.legacy_model_raw
        && existing.legacy_model_sqlite_type == input.legacy_model_sqlite_type
        && existing.legacy_model_sql_literal == input.legacy_model_sql_literal
        && existing.legacy_counter_values_json == input.legacy_counter_values_json
        && existing.legacy_cost_usd_raw == input.legacy_cost_usd_raw
        && existing.legacy_created_at_raw == input.legacy_created_at_raw
        && existing.legacy_project_owner_raw == input.legacy_project_owner_raw
        && existing.legacy_invalid_usage == input.legacy_invalid_usage
        && existing.provider_id == input.provider_id
        && existing.model_id == input.model_id
        && existing.runtime_model == input.runtime_model
        && existing.candidate_key == input.candidate_key
        && existing.attempt_ordinal == input.attempt_ordinal
        && existing.agent_id == input.agent_id
        && existing.profile_id == input.profile_id
        && existing.agent_name_snapshot == input.agent_name_snapshot
        && existing.project_name_snapshot == input.project_name_snapshot
        && existing.executor_type == input.executor_type
        && existing.pricing_subject_revision_id == input.pricing_subject_revision_id
        && existing.subject_revision_digest == input.subject_revision_digest
        && existing.telemetry_state == input.telemetry_state
        && existing.input_tokens == input.input_tokens
        && existing.output_tokens == input.output_tokens
        && existing.cache_read_tokens == input.cache_read_tokens
        && existing.cache_write_tokens == input.cache_write_tokens
        && existing.context_tokens == input.context_tokens
        && existing.selected_tier == input.selected_tier
        && existing.provider_reported_nano_usd == input.provider_reported_nano_usd
        && option_f64_bits_equal(
            existing.legacy_reported_cost_usd,
            input.legacy_reported_cost_usd,
        )
        && existing.estimated_nano_usd == input.estimated_nano_usd
        && existing.cost_kind == input.cost_kind
        && existing.rate_revision_id == input.rate_revision_id
        && existing.catalog_snapshot_id == input.catalog_snapshot_id
        && existing.formula_revision == input.formula_revision
        && existing.retrospective == input.retrospective
        && existing.coverage_reason_code == input.coverage_reason_code
        && existing.occurred_at == input.occurred_at
        && existing.created_at == input.created_at
}

fn option_f64_bits_equal(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.to_bits() == right.to_bits(),
        (None, None) => true,
        _ => false,
    }
}
