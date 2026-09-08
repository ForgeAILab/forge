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

fn map_binding(row: SqliteRow) -> Result<PricingSubjectBinding> {
    Ok(PricingSubjectBinding {
        id: row.try_get("id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        subject_id: row.try_get("subject_id")?,
        subject_revision_id: row.try_get("subject_revision_id")?,
        subject_revision_digest: row.try_get("subject_revision_digest")?,
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

#[async_trait]
impl PricingCatalogRepo for SqliteDb {
    async fn create_pricing_catalog_snapshot(
        &self,
        input: CreatePricingCatalogSnapshot,
    ) -> Result<PricingCatalogSnapshot> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let snapshot = self
            .create_pricing_catalog_snapshot_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(snapshot)
    }

    async fn create_pricing_catalog_snapshot_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingCatalogSnapshot,
    ) -> Result<PricingCatalogSnapshot> {
        let result = sqlx::query(
            "INSERT INTO pricing_catalog_snapshot (
                id, source_kind, source_url, http_etag, payload_sha256,
                parser_revision, revision_digest, payload_json, fetched_at, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(input.source_kind.to_string())
        .bind(&input.source_url)
        .bind(input.http_etag.as_deref())
        .bind(&input.payload_sha256)
        .bind(&input.parser_revision)
        .bind(&input.revision_digest)
        .bind(&input.payload_json)
        .bind(&input.fetched_at)
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await;
        if let Err(error) = result {
            if let Some(row) =
                sqlx::query("SELECT * FROM pricing_catalog_snapshot WHERE revision_digest = ?")
                    .bind(&input.revision_digest)
                    .fetch_optional(&mut **transaction)
                    .await?
            {
                let existing = map_catalog_snapshot(row)?;
                if catalog_snapshot_matches_input(&existing, &input) {
                    return Ok(existing);
                }
                return Err(DbError::IdempotencyConflict);
            }
            return Err(duplicate_error(error));
        }
        sqlx::query("SELECT * FROM pricing_catalog_snapshot WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_catalog_snapshot)
    }

    async fn get_pricing_catalog_snapshot(
        &self,
        id: &str,
    ) -> Result<Option<PricingCatalogSnapshot>> {
        sqlx::query("SELECT * FROM pricing_catalog_snapshot WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_catalog_snapshot)
            .transpose()
    }

    async fn get_pricing_catalog_snapshot_by_revision(
        &self,
        revision_digest: &str,
    ) -> Result<Option<PricingCatalogSnapshot>> {
        sqlx::query("SELECT * FROM pricing_catalog_snapshot WHERE revision_digest = ?")
            .bind(revision_digest)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_catalog_snapshot)
            .transpose()
    }

    async fn list_pricing_catalog_models(
        &self,
        query: PricingCatalogModelQuery,
    ) -> Result<Page<PricingCatalogModelRate>> {
        let offset = decode_offset(&query.page.cursor)?;
        let mut builder = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT r.id AS rate_revision_id, r.catalog_snapshot_id AS snapshot_id,
                    r.catalog_provider_id AS provider_id,
                    r.catalog_model_id AS model_id,
                    r.input_nano_usd_per_million,
                    r.output_nano_usd_per_million,
                    r.cache_read_nano_usd_per_million,
                    r.cache_write_nano_usd_per_million,
                    r.tiers_json, r.source_last_updated, r.source_kind,
                    r.rate_digest, r.effective_at
             FROM pricing_rate_revision r
             WHERE r.source_kind = 'models_dev_catalog'",
        );
        if let Some(snapshot_id) = &query.snapshot_id {
            builder
                .push(" AND r.catalog_snapshot_id = ")
                .push_bind(snapshot_id);
        }
        if let Some(provider_id) = &query.provider_id {
            builder
                .push(" AND r.catalog_provider_id = ")
                .push_bind(provider_id);
        }
        if let Some(search) = &query.query {
            builder
                .push(" AND (r.catalog_provider_id LIKE ")
                .push_bind(format!("%{search}%"))
                .push(" OR r.catalog_model_id LIKE ")
                .push_bind(format!("%{search}%"))
                .push(')');
        }
        builder
            .push(" ORDER BY r.catalog_provider_id ASC, r.catalog_model_id ASC, r.id ASC LIMIT ");
        builder.push_bind(limit(&query.page) + 1);
        builder.push(" OFFSET ").push_bind(offset);
        let rows = builder.build().fetch_all(&self.pool).await?;
        let items = rows
            .into_iter()
            .map(map_catalog_model_rate)
            .collect::<Result<Vec<_>>>()?;

        let mut count_builder = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT COUNT(*) FROM pricing_rate_revision r
             WHERE r.source_kind = 'models_dev_catalog'",
        );
        if let Some(snapshot_id) = &query.snapshot_id {
            count_builder
                .push(" AND r.catalog_snapshot_id = ")
                .push_bind(snapshot_id);
        }
        if let Some(provider_id) = &query.provider_id {
            count_builder
                .push(" AND r.catalog_provider_id = ")
                .push_bind(provider_id);
        }
        if let Some(search) = &query.query {
            count_builder
                .push(" AND (r.catalog_provider_id LIKE ")
                .push_bind(format!("%{search}%"))
                .push(" OR r.catalog_model_id LIKE ")
                .push_bind(format!("%{search}%"))
                .push(')');
        }
        let total = if query.page.include_total {
            Some(
                count_builder
                    .build_query_scalar::<i64>()
                    .fetch_one(&self.pool)
                    .await?,
            )
        } else {
            None
        };
        page_from_items(items, &query.page, offset, total)
    }

    async fn get_pricing_catalog_state(&self) -> Result<Option<PricingCatalogState>> {
        sqlx::query("SELECT * FROM pricing_catalog_state WHERE id = 'models_dev_catalog'")
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_catalog_state)
            .transpose()
    }

    async fn ensure_pricing_catalog_state(&self, now: &str) -> Result<PricingCatalogState> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        sqlx::query(
            "INSERT OR IGNORE INTO pricing_catalog_state
                (id, state, version, created_at, updated_at)
             VALUES ('models_dev_catalog', 'absent', 1, ?, ?)",
        )
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        let row =
            sqlx::query("SELECT * FROM pricing_catalog_state WHERE id = 'models_dev_catalog'")
                .fetch_one(&mut *transaction)
                .await
                .map_err(check_error)?;
        let state = map_catalog_state(row)?;
        transaction.commit().await?;
        Ok(state)
    }

    async fn update_pricing_catalog_state(
        &self,
        input: UpdatePricingCatalogState,
    ) -> Result<PricingCatalogState> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let state = self
            .update_catalog_state_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(state)
    }

    async fn record_pricing_catalog_check(
        &self,
        input: RecordPricingCatalogCheck,
    ) -> Result<PricingCatalogState> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let state = self
            .record_catalog_check_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(state)
    }

    async fn activate_pricing_catalog(
        &self,
        input: ActivatePricingCatalog,
    ) -> Result<PricingCatalogState> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let state = self
            .activate_pricing_catalog_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(state)
    }

    async fn activate_pricing_catalog_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: ActivatePricingCatalog,
    ) -> Result<PricingCatalogState> {
        let snapshot_exists: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM pricing_catalog_snapshot WHERE id = ?")
                .bind(&input.snapshot_id)
                .fetch_optional(&mut **transaction)
                .await?;
        if snapshot_exists.is_none() {
            return Err(DbError::NotFound);
        }
        sqlx::query(
            "INSERT OR IGNORE INTO pricing_catalog_state
                (id, state, version, created_at, updated_at)
             VALUES ('models_dev_catalog', 'absent', 1, ?, ?)",
        )
        .bind(&input.checked_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        let current = self
            .catalog_state_in_tx(transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        if input
            .idempotency_key
            .as_deref()
            .is_some_and(|key| current.last_idempotency_key.as_deref() == Some(key))
        {
            return Ok(current);
        }
        let result = sqlx::query(
            "UPDATE pricing_catalog_state
             SET active_snapshot_id = ?, state = 'fresh', http_etag = ?,
                 last_checked_at = ?, last_successful_check_at = ?, stale_after = ?,
                 last_error_code = NULL, last_idempotency_key = ?,
                 version = version + 1, updated_at = ?
             WHERE id = 'models_dev_catalog' AND version = ?",
        )
        .bind(&input.snapshot_id)
        .bind(input.http_etag.as_deref())
        .bind(&input.checked_at)
        .bind(&input.checked_at)
        .bind(input.stale_after.as_deref())
        .bind(input.idempotency_key.as_deref())
        .bind(&input.updated_at)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => return Err(check_error(error)),
        };
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                "models_dev_catalog",
                input.expected_version,
                fetch_version(transaction, "pricing_catalog_state", "models_dev_catalog").await?,
            );
        }
        self.catalog_state_in_tx(transaction)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn create_pricing_rate_revision(
        &self,
        input: CreatePricingRateRevision,
    ) -> Result<PricingRateRevision> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let revision = self
            .create_pricing_rate_revision_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(revision)
    }

    async fn create_pricing_rate_revision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingRateRevision,
    ) -> Result<PricingRateRevision> {
        let result = sqlx::query(
            "INSERT INTO pricing_rate_revision (
                id, source_kind, owner_user_id, catalog_snapshot_id,
                catalog_provider_id, catalog_model_id,
                pricing_subject_revision_id, pricing_subject_revision_digest,
                runtime_model, source_model_key, source_last_updated, currency,
                input_nano_usd_per_million, output_nano_usd_per_million,
                cache_read_nano_usd_per_million, cache_write_nano_usd_per_million,
                tiers_json, legacy_context_over_200k_json, context_tier_state,
                received_rates_json, rate_digest, effective_at, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(input.source_kind.to_string())
        .bind(input.owner_user_id.as_deref())
        .bind(input.catalog_snapshot_id.as_deref())
        .bind(input.catalog_provider_id.as_deref())
        .bind(input.catalog_model_id.as_deref())
        .bind(input.pricing_subject_revision_id.as_deref())
        .bind(input.pricing_subject_revision_digest.as_deref())
        .bind(input.runtime_model.as_deref())
        .bind(input.source_model_key.as_deref())
        .bind(input.source_last_updated.as_deref())
        .bind(&input.currency)
        .bind(input.rates.input)
        .bind(input.rates.output)
        .bind(input.rates.cache_read)
        .bind(input.rates.cache_write)
        .bind(&input.tiers_json)
        .bind(input.legacy_context_over_200k_json.as_deref())
        .bind(&input.context_tier_state)
        .bind(&input.received_rates_json)
        .bind(&input.rate_digest)
        .bind(&input.effective_at)
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await;
        if let Err(error) = result {
            if let Some(row) =
                sqlx::query("SELECT * FROM pricing_rate_revision WHERE rate_digest = ?")
                    .bind(&input.rate_digest)
                    .fetch_optional(&mut **transaction)
                    .await?
            {
                let existing = map_rate_revision(row)?;
                if rate_revision_matches_input(&existing, &input) {
                    return Ok(existing);
                }
                return Err(DbError::IdempotencyConflict);
            }
            return Err(duplicate_error(error));
        }
        sqlx::query("SELECT * FROM pricing_rate_revision WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_rate_revision)
    }

    async fn get_pricing_rate_revision(&self, id: &str) -> Result<Option<PricingRateRevision>> {
        sqlx::query("SELECT * FROM pricing_rate_revision WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_rate_revision)
            .transpose()
    }

    async fn get_pricing_rate_revision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        id: &str,
    ) -> Result<Option<PricingRateRevision>> {
        sqlx::query("SELECT * FROM pricing_rate_revision WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(check_error)?
            .map(map_rate_revision)
            .transpose()
    }

    async fn get_pricing_rate_revision_by_digest(
        &self,
        rate_digest: &str,
    ) -> Result<Option<PricingRateRevision>> {
        sqlx::query("SELECT * FROM pricing_rate_revision WHERE rate_digest = ?")
            .bind(rate_digest)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_rate_revision)
            .transpose()
    }

    async fn get_catalog_rate_revision(
        &self,
        snapshot_id: &str,
        provider_id: &str,
        model_id: &str,
    ) -> Result<Option<PricingRateRevision>> {
        sqlx::query(
            "SELECT * FROM pricing_rate_revision
             WHERE source_kind = 'models_dev_catalog'
               AND catalog_snapshot_id = ? AND catalog_provider_id = ?
               AND catalog_model_id = ?",
        )
        .bind(snapshot_id)
        .bind(provider_id)
        .bind(model_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(check_error)?
        .map(map_rate_revision)
        .transpose()
    }

    async fn list_pricing_rate_revisions_for_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Vec<PricingRateRevision>> {
        sqlx::query(
            "SELECT * FROM pricing_rate_revision
             WHERE catalog_snapshot_id = ? ORDER BY catalog_provider_id ASC,
                catalog_model_id ASC, id ASC",
        )
        .bind(snapshot_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_rate_revision)
        .collect()
    }

    async fn list_pricing_rate_revisions_for_subject_model(
        &self,
        subject_revision_id: &str,
        runtime_model: &str,
    ) -> Result<Vec<PricingRateRevision>> {
        sqlx::query(
            "SELECT * FROM pricing_rate_revision
             WHERE source_kind = 'manual_override'
               AND pricing_subject_revision_id = ? AND runtime_model = ?
             ORDER BY effective_at DESC, id DESC",
        )
        .bind(subject_revision_id)
        .bind(runtime_model)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_rate_revision)
        .collect()
    }
}

impl SqliteDb {
    async fn catalog_state_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<Option<PricingCatalogState>> {
        sqlx::query("SELECT * FROM pricing_catalog_state WHERE id = 'models_dev_catalog'")
            .fetch_optional(&mut **transaction)
            .await
            .map_err(check_error)?
            .map(map_catalog_state)
            .transpose()
    }

    async fn update_catalog_state_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdatePricingCatalogState,
    ) -> Result<PricingCatalogState> {
        let current = self
            .catalog_state_in_tx(transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let result = sqlx::query(
            "UPDATE pricing_catalog_state
             SET active_snapshot_id = ?, state = ?, http_etag = ?,
                 last_checked_at = ?, last_successful_check_at = ?, stale_after = ?,
                 last_error_code = ?, last_idempotency_key = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(
            input
                .active_snapshot_id
                .unwrap_or(current.active_snapshot_id),
        )
        .bind(input.state.to_string())
        .bind(input.http_etag.unwrap_or(current.http_etag))
        .bind(input.last_checked_at.unwrap_or(current.last_checked_at))
        .bind(
            input
                .last_successful_check_at
                .unwrap_or(current.last_successful_check_at),
        )
        .bind(input.stale_after.unwrap_or(current.stale_after))
        .bind(input.last_error_code.unwrap_or(current.last_error_code))
        .bind(
            input
                .last_idempotency_key
                .unwrap_or(current.last_idempotency_key),
        )
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "pricing_catalog_state", &input.id).await?,
            );
        }
        self.catalog_state_in_tx(transaction)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn record_catalog_check_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: RecordPricingCatalogCheck,
    ) -> Result<PricingCatalogState> {
        let current = self
            .catalog_state_in_tx(transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        if input
            .idempotency_key
            .as_deref()
            .is_some_and(|key| current.last_idempotency_key.as_deref() == Some(key))
        {
            return Ok(current);
        }
        let result = sqlx::query(
            "UPDATE pricing_catalog_state
             SET active_snapshot_id = ?, state = ?, http_etag = ?,
                 last_checked_at = ?, last_successful_check_at = ?, stale_after = ?,
                 last_error_code = ?, last_idempotency_key = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(
            input
                .active_snapshot_id
                .unwrap_or(current.active_snapshot_id),
        )
        .bind(input.state.to_string())
        .bind(input.http_etag.unwrap_or(current.http_etag))
        .bind(&input.checked_at)
        .bind(
            input
                .successful_check_at
                .unwrap_or(current.last_successful_check_at),
        )
        .bind(input.stale_after.unwrap_or(current.stale_after))
        .bind(input.last_error_code.unwrap_or(current.last_error_code))
        .bind(input.idempotency_key)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "pricing_catalog_state", &input.id).await?,
            );
        }
        self.catalog_state_in_tx(transaction)
            .await?
            .ok_or(DbError::NotFound)
    }
}

#[async_trait]
impl PricingSubjectRepo for SqliteDb {
    async fn create_pricing_subject(&self, input: CreatePricingSubject) -> Result<PricingSubject> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let subject = self
            .create_pricing_subject_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(subject)
    }

    async fn create_pricing_subject_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingSubject,
    ) -> Result<PricingSubject> {
        // A revision belongs to an already-created subject.  Insert the
        // subject without its optional pointer, then validate/set the pointer
        // through the same CAS path when a caller supplied one.
        sqlx::query(
            "INSERT INTO pricing_subject (
                id, owner_user_id, subject_kind, provider_entry_id, daemon_id,
                executor_type, current_revision_id, state, last_idempotency_key,
                last_update_digest, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.owner_user_id)
        .bind(input.subject_kind.to_string())
        .bind(input.provider_entry_id.as_deref())
        .bind(input.daemon_id.as_deref())
        .bind(input.executor_type.as_deref())
        .bind(input.state.to_string())
        .bind(input.last_idempotency_key.as_deref())
        .bind(input.last_update_digest.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;

        if let Some(revision_id) = input.current_revision_id {
            sqlx::query(
                "UPDATE pricing_subject SET current_revision_id = ?, version = version + 1,
                    updated_at = ? WHERE id = ? AND version = 1",
            )
            .bind(revision_id)
            .bind(&input.updated_at)
            .bind(&input.id)
            .execute(&mut **transaction)
            .await
            .map_err(check_error)?;
        }
        sqlx::query("SELECT * FROM pricing_subject WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_subject)
    }

    async fn get_pricing_subject(&self, id: &str) -> Result<Option<PricingSubject>> {
        sqlx::query("SELECT * FROM pricing_subject WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_subject)
            .transpose()
    }

    async fn get_pricing_subject_for_provider(
        &self,
        owner_user_id: &str,
        provider_entry_id: &str,
    ) -> Result<Option<PricingSubject>> {
        sqlx::query(
            "SELECT * FROM pricing_subject
             WHERE owner_user_id = ? AND subject_kind = 'provider_entry'
               AND provider_entry_id = ?",
        )
        .bind(owner_user_id)
        .bind(provider_entry_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(check_error)?
        .map(map_subject)
        .transpose()
    }

    async fn get_pricing_subject_for_cli_runtime(
        &self,
        owner_user_id: &str,
        daemon_id: &str,
        executor_type: &str,
    ) -> Result<Option<PricingSubject>> {
        sqlx::query(
            "SELECT * FROM pricing_subject
             WHERE owner_user_id = ? AND subject_kind = 'cli_runtime'
               AND daemon_id = ? AND executor_type = ?",
        )
        .bind(owner_user_id)
        .bind(daemon_id)
        .bind(executor_type)
        .fetch_optional(&self.pool)
        .await
        .map_err(check_error)?
        .map(map_subject)
        .transpose()
    }

    async fn resolve_active_pricing_subject_binding_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        owner_user_id: &str,
        provider_entry_id: Option<&str>,
        daemon_id: Option<&str>,
        executor_type: Option<&str>,
        runtime_model: &str,
    ) -> Result<Option<ResolvedPricingSubjectBinding>> {
        let subject_row = if let Some(provider_entry_id) = provider_entry_id {
            sqlx::query(
                "SELECT * FROM pricing_subject
                 WHERE owner_user_id = ? AND subject_kind = 'provider_entry'
                   AND provider_entry_id = ? AND state = 'active'",
            )
            .bind(owner_user_id)
            .bind(provider_entry_id)
            .fetch_optional(&mut **transaction)
            .await?
        } else if let (Some(daemon_id), Some(executor_type)) = (daemon_id, executor_type) {
            sqlx::query(
                "SELECT * FROM pricing_subject
                 WHERE owner_user_id = ? AND subject_kind = 'cli_runtime'
                   AND daemon_id = ? AND executor_type = ? AND state = 'active'",
            )
            .bind(owner_user_id)
            .bind(daemon_id)
            .bind(executor_type)
            .fetch_optional(&mut **transaction)
            .await?
        } else {
            None
        };
        let Some(subject) = subject_row.map(map_subject).transpose()? else {
            return Ok(None);
        };
        let Some(revision_id) = subject.current_revision_id.as_deref() else {
            return Ok(None);
        };
        let Some(revision) = sqlx::query(
            "SELECT * FROM pricing_subject_revision
             WHERE id = ? AND subject_id = ?",
        )
        .bind(revision_id)
        .bind(&subject.id)
        .fetch_optional(&mut **transaction)
        .await?
        .map(map_subject_revision)
        .transpose()?
        else {
            return Ok(None);
        };
        if revision.revision_digest.trim().is_empty()
            || revision.subject_id != subject.id
            || revision.subject_kind != subject.subject_kind
            || revision.provider_entry_id != subject.provider_entry_id
            || revision.daemon_id != subject.daemon_id
            || revision.executor_type != subject.executor_type
        {
            return Err(DbError::Check(
                "pricing subject current revision identity is invalid".to_owned(),
            ));
        }
        let binding = sqlx::query(
            "SELECT * FROM pricing_subject_binding
             WHERE subject_id = ? AND subject_revision_id = ?
               AND subject_revision_digest = ? AND runtime_model = ?
               AND state = 'active'
             ORDER BY CASE source_kind
                        WHEN 'manual_override' THEN 0
                        WHEN 'models_dev_catalog' THEN 1
                        ELSE 2
                      END,
                      effective_at DESC, id DESC
             LIMIT 1",
        )
        .bind(&subject.id)
        .bind(&revision.id)
        .bind(&revision.revision_digest)
        .bind(runtime_model)
        .fetch_optional(&mut **transaction)
        .await?
        .map(map_binding)
        .transpose()?;
        let Some(binding) = binding else {
            return Ok(Some(ResolvedPricingSubjectBinding {
                subject,
                revision,
                binding: None,
                rate: None,
            }));
        };
        if binding.owner_user_id != subject.owner_user_id
            || binding.subject_revision_digest != revision.revision_digest
        {
            return Err(DbError::Check(
                "pricing binding subject revision identity is invalid".to_owned(),
            ));
        }
        let Some(rate) = sqlx::query("SELECT * FROM pricing_rate_revision WHERE id = ?")
            .bind(&binding.rate_revision_id)
            .fetch_optional(&mut **transaction)
            .await?
            .map(map_rate_revision)
            .transpose()?
        else {
            return Err(DbError::Check(
                "pricing binding rate revision is missing".to_owned(),
            ));
        };
        if rate.source_kind != binding.source_kind {
            return Err(DbError::Check(
                "pricing binding source differs from rate revision".to_owned(),
            ));
        }
        match binding.source_kind {
            PricingRateSourceKind::ManualOverride => {
                if rate.catalog_snapshot_id.is_some()
                    || rate.catalog_provider_id.is_some()
                    || rate.catalog_model_id.is_some()
                    || rate.pricing_subject_revision_id.as_deref() != Some(revision.id.as_str())
                    || rate.pricing_subject_revision_digest.as_deref()
                        != Some(revision.revision_digest.as_str())
                    || rate.runtime_model.as_deref() != Some(runtime_model)
                    || binding.catalog_provider_id.is_some()
                    || binding.catalog_model_id.is_some()
                {
                    return Err(DbError::Check(
                        "manual pricing binding canonical identity is invalid".to_owned(),
                    ));
                }
            }
            PricingRateSourceKind::ModelsDevCatalog => {
                if rate.catalog_snapshot_id.is_none()
                    || rate.catalog_provider_id.as_deref() != binding.catalog_provider_id.as_deref()
                    || rate.catalog_model_id.as_deref() != binding.catalog_model_id.as_deref()
                    || binding.catalog_provider_id.is_none()
                    || binding.catalog_model_id.is_none()
                    || rate.pricing_subject_revision_id.is_some()
                    || rate.pricing_subject_revision_digest.is_some()
                    || rate.runtime_model.is_some()
                {
                    return Err(DbError::Check(
                        "catalog pricing binding canonical identity is invalid".to_owned(),
                    ));
                }
            }
        }
        Ok(Some(ResolvedPricingSubjectBinding {
            subject,
            revision,
            binding: Some(binding),
            rate: Some(rate),
        }))
    }

    async fn list_pricing_subjects(&self, owner_user_id: &str) -> Result<Vec<PricingSubject>> {
        sqlx::query(
            "SELECT * FROM pricing_subject
             WHERE owner_user_id = ? ORDER BY updated_at DESC, id DESC",
        )
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_subject)
        .collect()
    }

    async fn update_pricing_subject(&self, input: UpdatePricingSubject) -> Result<PricingSubject> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let subject = self
            .update_pricing_subject_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(subject)
    }

    async fn update_pricing_subject_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdatePricingSubject,
    ) -> Result<PricingSubject> {
        let current = sqlx::query("SELECT * FROM pricing_subject WHERE id = ?")
            .bind(&input.id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(check_error)?
            .map(map_subject)
            .transpose()?
            .ok_or(DbError::NotFound)?;
        let result = sqlx::query(
            "UPDATE pricing_subject
             SET current_revision_id = ?, state = ?, last_idempotency_key = ?,
                 last_update_digest = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(
            input
                .current_revision_id
                .unwrap_or(current.current_revision_id),
        )
        .bind(input.state.unwrap_or(current.state).to_string())
        .bind(
            input
                .last_idempotency_key
                .unwrap_or(current.last_idempotency_key),
        )
        .bind(
            input
                .last_update_digest
                .unwrap_or(current.last_update_digest),
        )
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "pricing_subject", &input.id).await?,
            );
        }
        sqlx::query("SELECT * FROM pricing_subject WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_subject)
    }

    async fn create_pricing_subject_revision(
        &self,
        input: CreatePricingSubjectRevision,
    ) -> Result<PricingSubjectRevision> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let revision = self
            .create_pricing_subject_revision_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(revision)
    }

    async fn create_pricing_subject_revision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingSubjectRevision,
    ) -> Result<PricingSubjectRevision> {
        let result = sqlx::query(
            "INSERT INTO pricing_subject_revision (
                id, subject_id, owner_user_id, revision, revision_digest,
                subject_kind, provider_entry_id, daemon_id, executor_type,
                provider_kind, credential_method, endpoint_class,
                runtime_fingerprint, schema_revision, non_secret_identity_json,
                created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.subject_id)
        .bind(&input.owner_user_id)
        .bind(input.revision)
        .bind(&input.revision_digest)
        .bind(input.subject_kind.to_string())
        .bind(input.provider_entry_id.as_deref())
        .bind(input.daemon_id.as_deref())
        .bind(input.executor_type.as_deref())
        .bind(&input.provider_kind)
        .bind(&input.credential_method)
        .bind(&input.endpoint_class)
        .bind(input.runtime_fingerprint.as_deref())
        .bind(&input.schema_revision)
        .bind(&input.non_secret_identity_json)
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await;
        if let Err(error) = result {
            if let Some(row) = sqlx::query(
                "SELECT * FROM pricing_subject_revision
                 WHERE subject_id = ? AND revision_digest = ?",
            )
            .bind(&input.subject_id)
            .bind(&input.revision_digest)
            .fetch_optional(&mut **transaction)
            .await?
            {
                let existing = map_subject_revision(row)?;
                if subject_revision_matches_input(&existing, &input) {
                    return Ok(existing);
                }
                return Err(DbError::IdempotencyConflict);
            }
            return Err(duplicate_error(error));
        }
        sqlx::query("SELECT * FROM pricing_subject_revision WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_subject_revision)
    }

    async fn get_pricing_subject_revision(
        &self,
        id: &str,
    ) -> Result<Option<PricingSubjectRevision>> {
        sqlx::query("SELECT * FROM pricing_subject_revision WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_subject_revision)
            .transpose()
    }

    async fn list_pricing_subject_revisions(
        &self,
        subject_id: &str,
    ) -> Result<Vec<PricingSubjectRevision>> {
        sqlx::query(
            "SELECT * FROM pricing_subject_revision
             WHERE subject_id = ? ORDER BY revision DESC, id DESC",
        )
        .bind(subject_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_subject_revision)
        .collect()
    }

    async fn create_pricing_subject_binding(
        &self,
        input: CreatePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let binding = self
            .create_pricing_subject_binding_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(binding)
    }

    async fn create_pricing_subject_binding_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding> {
        sqlx::query(
            "INSERT INTO pricing_subject_binding (
                id, owner_user_id, subject_id, subject_revision_id,
                subject_revision_digest, runtime_model, source_kind,
                catalog_provider_id, catalog_model_id, rate_revision_id,
                binding_digest, state, version, effective_at, retired_at,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', 1, ?, NULL, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.owner_user_id)
        .bind(&input.subject_id)
        .bind(&input.subject_revision_id)
        .bind(&input.subject_revision_digest)
        .bind(&input.runtime_model)
        .bind(input.source_kind.to_string())
        .bind(input.catalog_provider_id.as_deref())
        .bind(input.catalog_model_id.as_deref())
        .bind(&input.rate_revision_id)
        .bind(&input.binding_digest)
        .bind(&input.effective_at)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        sqlx::query("SELECT * FROM pricing_subject_binding WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_binding)
    }

    async fn get_pricing_subject_binding(&self, id: &str) -> Result<Option<PricingSubjectBinding>> {
        sqlx::query("SELECT * FROM pricing_subject_binding WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_binding)
            .transpose()
    }

    async fn get_active_pricing_subject_binding(
        &self,
        subject_id: &str,
        runtime_model: &str,
    ) -> Result<Option<PricingSubjectBinding>> {
        sqlx::query(
            "SELECT * FROM pricing_subject_binding
             WHERE subject_id = ? AND runtime_model = ? AND state = 'active'
             ORDER BY CASE source_kind
                        WHEN 'manual_override' THEN 0
                        WHEN 'models_dev_catalog' THEN 1
                        ELSE 2
                      END,
                      effective_at DESC, id DESC
             LIMIT 1",
        )
        .bind(subject_id)
        .bind(runtime_model)
        .fetch_optional(&self.pool)
        .await
        .map_err(check_error)?
        .map(map_binding)
        .transpose()
    }

    async fn list_pricing_subject_bindings(
        &self,
        subject_id: &str,
        include_retired: bool,
    ) -> Result<Vec<PricingSubjectBinding>> {
        let sql = if include_retired {
            "SELECT * FROM pricing_subject_binding
             WHERE subject_id = ? ORDER BY runtime_model ASC, effective_at DESC, id DESC"
        } else {
            "SELECT * FROM pricing_subject_binding
             WHERE subject_id = ? AND state = 'active'
             ORDER BY runtime_model ASC, effective_at DESC, id DESC"
        };
        sqlx::query(sql)
            .bind(subject_id)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(map_binding)
            .collect()
    }

    async fn update_pricing_subject_binding(
        &self,
        input: UpdatePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let binding = self
            .update_pricing_subject_binding_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(binding)
    }

    async fn update_pricing_subject_binding_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdatePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding> {
        let result = sqlx::query(
            "UPDATE pricing_subject_binding
             SET source_kind = ?, catalog_provider_id = ?, catalog_model_id = ?,
                 rate_revision_id = ?, binding_digest = ?, effective_at = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND state = 'active'",
        )
        .bind(input.source_kind.to_string())
        .bind(input.catalog_provider_id.as_deref())
        .bind(input.catalog_model_id.as_deref())
        .bind(&input.rate_revision_id)
        .bind(&input.binding_digest)
        .bind(&input.effective_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            let actual = fetch_version(transaction, "pricing_subject_binding", &input.id).await?;
            return conflict_or_not_found(&input.id, input.expected_version, actual);
        }
        sqlx::query("SELECT * FROM pricing_subject_binding WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_binding)
    }

    async fn retire_pricing_subject_binding(
        &self,
        input: RetirePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let binding = self
            .retire_pricing_subject_binding_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(binding)
    }

    async fn retire_pricing_subject_binding_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: RetirePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding> {
        let result = sqlx::query(
            "UPDATE pricing_subject_binding
             SET state = 'retired', retired_at = ?, version = version + 1,
                 updated_at = ?
             WHERE id = ? AND version = ? AND state = 'active'",
        )
        .bind(&input.retired_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            let actual = fetch_version(transaction, "pricing_subject_binding", &input.id).await?;
            return conflict_or_not_found(&input.id, input.expected_version, actual);
        }
        sqlx::query("SELECT * FROM pricing_subject_binding WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_binding)
    }
}

#[async_trait]
impl UsageLedgerRepo for SqliteDb {
    async fn create_pricing_selection(
        &self,
        input: CreatePricingSelection,
    ) -> Result<PricingSelection> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let selection = self
            .create_pricing_selection_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(selection)
    }

    async fn create_pricing_selection_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingSelection,
    ) -> Result<PricingSelection> {
        // The candidate identity has its own unique index.  Resolve it before
        // looking at the digest so a retry with a different digest cannot
        // accidentally return the existing candidate row.
        if let Some(row) = sqlx::query(
            "SELECT * FROM pricing_selection
             WHERE owner_user_id IS ? AND project_id IS ?
               AND domain_kind = ? AND surface = ? AND source_id = ?
               AND candidate_key IS ? AND attempt_ordinal = ?",
        )
        .bind(input.owner_user_id.as_deref())
        .bind(input.project_id.as_deref())
        .bind(input.domain_kind.to_string())
        .bind(input.surface.to_string())
        .bind(&input.source_id)
        .bind(input.candidate_key.as_deref())
        .bind(input.attempt_ordinal)
        .fetch_optional(&mut **transaction)
        .await?
        {
            let existing = map_selection(row)?;
            if existing.selection_digest == input.selection_digest
                && pricing_selection_matches_input(&existing, &input)
            {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }
        let result = sqlx::query(
            "INSERT INTO pricing_selection (
                id, owner_user_id, project_id, domain_kind, surface, source_id,
                execution_id, task_id, candidate_key, attempt_ordinal,
                invocation_id, subject_id, subject_revision_id,
                subject_revision_digest, binding_id, rate_revision_id,
                catalog_snapshot_id, catalog_freshness, runtime_model, admitted_provider_id,
                admitted_model_id, source_kind, provenance_kind, selection_status,
                selection_reason, selection_digest, selected_at, created_at
             ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             )",
        )
        .bind(&input.id)
        .bind(input.owner_user_id.as_deref())
        .bind(input.project_id.as_deref())
        .bind(input.domain_kind.to_string())
        .bind(input.surface.to_string())
        .bind(&input.source_id)
        .bind(input.execution_id.as_deref())
        .bind(input.task_id.as_deref())
        .bind(input.candidate_key.as_deref())
        .bind(input.attempt_ordinal)
        .bind(input.subject_id.as_deref())
        .bind(input.subject_revision_id.as_deref())
        .bind(input.subject_revision_digest.as_deref())
        .bind(input.binding_id.as_deref())
        .bind(input.rate_revision_id.as_deref())
        .bind(input.catalog_snapshot_id.as_deref())
        .bind(input.catalog_freshness.as_deref())
        .bind(input.runtime_model.as_deref())
        .bind(input.admitted_provider_id.as_deref())
        .bind(input.admitted_model_id.as_deref())
        .bind(input.source_kind.map(|value| value.to_string()))
        .bind(input.provenance_kind.to_string())
        .bind(input.selection_status.to_string())
        .bind(input.selection_reason.map(|value| value.to_string()))
        .bind(&input.selection_digest)
        .bind(&input.selected_at)
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await;
        if let Err(error) = result {
            if let Some(row) = sqlx::query(
                "SELECT * FROM pricing_selection
                 WHERE owner_user_id IS ? AND project_id IS ?
                   AND domain_kind = ? AND surface = ? AND source_id = ?
                   AND candidate_key IS ? AND attempt_ordinal = ?",
            )
            .bind(input.owner_user_id.as_deref())
            .bind(input.project_id.as_deref())
            .bind(input.domain_kind.to_string())
            .bind(input.surface.to_string())
            .bind(&input.source_id)
            .bind(input.candidate_key.as_deref())
            .bind(input.attempt_ordinal)
            .fetch_optional(&mut **transaction)
            .await?
            {
                let existing = map_selection(row)?;
                if existing.selection_digest == input.selection_digest
                    && pricing_selection_matches_input(&existing, &input)
                {
                    return Ok(existing);
                }
                return Err(DbError::IdempotencyConflict);
            }
            return Err(duplicate_error(error));
        }
        sqlx::query("SELECT * FROM pricing_selection WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_selection)
    }

    async fn get_pricing_selection(&self, id: &str) -> Result<Option<PricingSelection>> {
        sqlx::query("SELECT * FROM pricing_selection WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_selection)
            .transpose()
    }

    async fn list_pricing_selections_for_source(
        &self,
        source_id: &str,
    ) -> Result<Vec<PricingSelection>> {
        sqlx::query(
            "SELECT * FROM pricing_selection
             WHERE source_id = ? ORDER BY attempt_ordinal ASC, id ASC",
        )
        .bind(source_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_selection)
        .collect()
    }

    async fn list_pricing_selections_for_source_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        source_id: &str,
    ) -> Result<Vec<PricingSelection>> {
        sqlx::query(
            "SELECT * FROM pricing_selection
             WHERE source_id = ? ORDER BY attempt_ordinal ASC, id ASC",
        )
        .bind(source_id)
        .fetch_all(&mut **transaction)
        .await?
        .into_iter()
        .map(map_selection)
        .collect()
    }

    async fn create_usage_invocation(
        &self,
        input: CreateUsageInvocation,
    ) -> Result<UsageInvocation> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let invocation = self
            .create_usage_invocation_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(invocation)
    }

    async fn create_usage_invocation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateUsageInvocation,
    ) -> Result<UsageInvocation> {
        if let Some(row) =
            sqlx::query("SELECT * FROM usage_invocation WHERE domain_idempotency_key = ?")
                .bind(&input.domain_idempotency_key)
                .fetch_optional(&mut **transaction)
                .await?
        {
            let existing = map_invocation(row)?;
            if invocation_matches_input(&existing, &input) {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }
        sqlx::query(
            "INSERT INTO usage_invocation (
                id, owner_user_id, project_id, domain_kind, surface, source_id,
                execution_id, task_id, domain_idempotency_key, candidate_key,
                attempt_ordinal, pricing_selection_id, admitted_provider_id,
                admitted_model_id, admitted_runtime_model, pricing_subject_id,
                pricing_subject_revision_id, subject_revision_digest, agent_id,
                profile_id, agent_name_snapshot, project_name_snapshot,
                executor_type, backend_kind, provenance_kind, lifecycle,
                telemetry_state, terminal_reason, version, admitted_at,
                started_at, settled_at, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'admitted', 'pending', NULL, 1, ?, NULL, NULL, ?, ?)",
        )
        .bind(&input.id)
        .bind(input.owner_user_id.as_deref())
        .bind(input.project_id.as_deref())
        .bind(input.domain_kind.to_string())
        .bind(input.surface.to_string())
        .bind(&input.source_id)
        .bind(input.execution_id.as_deref())
        .bind(input.task_id.as_deref())
        .bind(&input.domain_idempotency_key)
        .bind(input.candidate_key.as_deref())
        .bind(input.attempt_ordinal)
        .bind(&input.pricing_selection_id)
        .bind(input.admitted_provider_id.as_deref())
        .bind(input.admitted_model_id.as_deref())
        .bind(input.admitted_runtime_model.as_deref())
        .bind(input.pricing_subject_id.as_deref())
        .bind(input.pricing_subject_revision_id.as_deref())
        .bind(input.subject_revision_digest.as_deref())
        .bind(input.agent_id.as_deref())
        .bind(input.profile_id.as_deref())
        .bind(input.agent_name_snapshot.as_deref())
        .bind(input.project_name_snapshot.as_deref())
        .bind(input.executor_type.as_deref())
        .bind(input.backend_kind.as_deref())
        .bind(input.provenance_kind.to_string())
        .bind(&input.admitted_at)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;

        let link = sqlx::query(
            "UPDATE pricing_selection SET invocation_id = ?
             WHERE id = ? AND invocation_id IS NULL",
        )
        .bind(&input.id)
        .bind(&input.pricing_selection_id)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if link.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        sqlx::query("SELECT * FROM usage_invocation WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_invocation)
    }

    async fn get_usage_invocation(&self, id: &str) -> Result<Option<UsageInvocation>> {
        sqlx::query("SELECT * FROM usage_invocation WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_invocation)
            .transpose()
    }

    async fn get_usage_invocation_by_idempotency(
        &self,
        domain_idempotency_key: &str,
    ) -> Result<Option<UsageInvocation>> {
        sqlx::query("SELECT * FROM usage_invocation WHERE domain_idempotency_key = ?")
            .bind(domain_idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_invocation)
            .transpose()
    }

    async fn list_usage_invocations_for_source(
        &self,
        source_id: &str,
    ) -> Result<Vec<UsageInvocation>> {
        sqlx::query(
            "SELECT * FROM usage_invocation
             WHERE source_id = ? ORDER BY attempt_ordinal ASC, id ASC",
        )
        .bind(source_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_invocation)
        .collect()
    }

    async fn list_usage_invocations_for_source_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        source_id: &str,
    ) -> Result<Vec<UsageInvocation>> {
        sqlx::query(
            "SELECT * FROM usage_invocation
             WHERE source_id = ? ORDER BY attempt_ordinal ASC, id ASC",
        )
        .bind(source_id)
        .fetch_all(&mut **transaction)
        .await?
        .into_iter()
        .map(map_invocation)
        .collect()
    }

    async fn list_usage_invocations_needing_settlement(
        &self,
        limit: i64,
    ) -> Result<Vec<UsageInvocation>> {
        sqlx::query(
            "SELECT * FROM usage_invocation
             WHERE lifecycle IN ('started', 'pending_settlement')
             ORDER BY COALESCE(started_at, admitted_at) ASC, id ASC
             LIMIT ?",
        )
        .bind(limit.clamp(1, 5000))
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_invocation)
        .collect()
    }

    async fn list_usage_invocations_for_project(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<UsageInvocation>> {
        let mut builder =
            sqlx::QueryBuilder::<Sqlite>::new("SELECT * FROM usage_invocation WHERE project_id = ");
        builder.push_bind(project_id);
        if let Some(from) = from {
            builder.push(" AND admitted_at >= ").push_bind(from);
        }
        if let Some(to) = to {
            builder.push(" AND admitted_at < ").push_bind(to);
        }
        builder.push(" ORDER BY admitted_at ASC, id ASC");
        builder
            .build()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(map_invocation)
            .collect()
    }

    async fn start_usage_invocation(&self, input: StartUsageInvocation) -> Result<UsageInvocation> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let invocation = self
            .start_usage_invocation_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(invocation)
    }

    async fn start_usage_invocation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: StartUsageInvocation,
    ) -> Result<UsageInvocation> {
        let result = sqlx::query(
            "UPDATE usage_invocation
             SET lifecycle = 'started', telemetry_state = 'pending', started_at = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND lifecycle = 'admitted'
               AND started_at IS NULL",
        )
        .bind(&input.started_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "usage_invocation", &input.id).await?,
            );
        }
        select_invocation_in_tx(transaction, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn mark_usage_invocation_pending_settlement(
        &self,
        input: MarkUsageInvocationPendingSettlement,
    ) -> Result<UsageInvocation> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let invocation = self
            .mark_usage_invocation_pending_settlement_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(invocation)
    }

    async fn mark_usage_invocation_pending_settlement_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: MarkUsageInvocationPendingSettlement,
    ) -> Result<UsageInvocation> {
        let result = sqlx::query(
            "UPDATE usage_invocation
             SET lifecycle = 'pending_settlement', updated_at = ?, version = version + 1
             WHERE id = ? AND version = ? AND lifecycle = 'started'",
        )
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "usage_invocation", &input.id).await?,
            );
        }
        select_invocation_in_tx(transaction, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn settle_usage_invocation(
        &self,
        input: SettleUsageInvocation,
    ) -> Result<UsageInvocation> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let invocation = self
            .settle_usage_invocation_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(invocation)
    }

    async fn settle_usage_invocation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: SettleUsageInvocation,
    ) -> Result<UsageInvocation> {
        if !matches!(
            input.telemetry_state,
            UsageTelemetryState::Metered | UsageTelemetryState::Unmetered
        ) {
            return Err(DbError::Check(
                "settled invocation must be metered or unmetered".to_owned(),
            ));
        }
        let result = sqlx::query(
            "UPDATE usage_invocation
             SET lifecycle = 'settled', telemetry_state = ?, terminal_reason = ?,
                 settled_at = ?, updated_at = ?, version = version + 1
             WHERE id = ? AND version = ?
               AND lifecycle IN ('started', 'pending_settlement')
               AND settled_at IS NULL",
        )
        .bind(input.telemetry_state.to_string())
        .bind(input.terminal_reason.as_deref())
        .bind(&input.settled_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "usage_invocation", &input.id).await?,
            );
        }
        select_invocation_in_tx(transaction, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn mark_usage_invocation_unsettled(
        &self,
        input: MarkUsageInvocationUnsettled,
    ) -> Result<UsageInvocation> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let invocation = self
            .mark_usage_invocation_unsettled_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(invocation)
    }

    async fn mark_usage_invocation_unsettled_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: MarkUsageInvocationUnsettled,
    ) -> Result<UsageInvocation> {
        if input.terminal_reason.trim().is_empty() {
            return Err(DbError::Check(
                "unsettled invocation needs a reason".to_owned(),
            ));
        }
        let result = sqlx::query(
            "UPDATE usage_invocation
             SET lifecycle = 'unsettled', telemetry_state = 'unsettled',
                 terminal_reason = ?, settled_at = ?, updated_at = ?, version = version + 1
             WHERE id = ? AND version = ?
               AND lifecycle IN ('started', 'pending_settlement')
               AND settled_at IS NULL",
        )
        .bind(&input.terminal_reason)
        .bind(&input.settled_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "usage_invocation", &input.id).await?,
            );
        }
        select_invocation_in_tx(transaction, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn append_usage_event(&self, input: CreateUsageEvent) -> Result<UsageEvent> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let event = self
            .append_usage_event_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(event)
    }

    async fn append_usage_event_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateUsageEvent,
    ) -> Result<UsageEvent> {
        if let Some(row) = sqlx::query("SELECT * FROM usage_event WHERE event_idempotency_key = ?")
            .bind(&input.event_idempotency_key)
            .fetch_optional(&mut **transaction)
            .await?
        {
            let existing = map_usage_event(row)?;
            if usage_event_matches_input(&existing, &input) {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }

        // Report identity is independent from the caller's idempotency key.
        // A changed retry payload must not create a second billable event.
        if let Some(row) = sqlx::query(
            "SELECT * FROM usage_event
             WHERE invocation_id = ? AND source_report_id = ?",
        )
        .bind(&input.invocation_id)
        .bind(&input.source_report_id)
        .fetch_optional(&mut **transaction)
        .await?
        {
            let existing = map_usage_event(row)?;
            if usage_event_matches_input(&existing, &input) {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }
        if let Some(row) = sqlx::query(
            "SELECT * FROM usage_event
             WHERE invocation_id = ? AND report_mode = ? AND report_sequence = ?",
        )
        .bind(&input.invocation_id)
        .bind(input.report_mode.to_string())
        .bind(input.report_sequence)
        .fetch_optional(&mut **transaction)
        .await?
        {
            let existing = map_usage_event(row)?;
            if usage_event_matches_input(&existing, &input) {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }

        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "INSERT INTO usage_event (
                id, invocation_id, owner_user_id, project_id, surface, source_id,
                execution_id, task_id, event_idempotency_key, source_report_id,
                report_sequence, report_mode, provenance_kind, legacy_source_table,
                legacy_source_id, legacy_provider_raw, legacy_provider_sqlite_type,
                legacy_provider_sql_literal, legacy_model_raw, legacy_model_sqlite_type,
                legacy_model_sql_literal, legacy_counter_values_json,
                legacy_cost_usd_raw, legacy_created_at_raw, legacy_project_owner_raw,
                legacy_invalid_usage, provider_id, model_id, runtime_model,
                candidate_key, attempt_ordinal, agent_id, profile_id,
                agent_name_snapshot, project_name_snapshot, executor_type,
                pricing_subject_revision_id, subject_revision_digest, telemetry_state,
                input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                context_tokens, selected_tier, provider_reported_nano_usd,
                legacy_reported_cost_usd, estimated_nano_usd, cost_kind,
                rate_revision_id, catalog_snapshot_id, formula_revision,
                retrospective, coverage_reason_code, occurred_at, created_at
             ) VALUES (",
        );
        query.push_bind(&input.id).push(", ");
        query.push_bind(&input.invocation_id).push(", ");
        query.push_bind(input.owner_user_id.as_deref()).push(", ");
        query.push_bind(input.project_id.as_deref()).push(", ");
        query.push_bind(input.surface.to_string()).push(", ");
        query.push_bind(&input.source_id).push(", ");
        query.push_bind(input.execution_id.as_deref()).push(", ");
        query.push_bind(input.task_id.as_deref()).push(", ");
        query.push_bind(&input.event_idempotency_key).push(", ");
        query.push_bind(&input.source_report_id).push(", ");
        query.push_bind(input.report_sequence).push(", ");
        query.push_bind(input.report_mode.to_string()).push(", ");
        query
            .push_bind(input.provenance_kind.to_string())
            .push(", ");
        query
            .push_bind(input.legacy_source_table.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_source_id.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_provider_raw.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_provider_sqlite_type.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_provider_sql_literal.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_model_raw.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_model_sqlite_type.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_model_sql_literal.as_deref())
            .push(", ");
        query
            .push_bind(&input.legacy_counter_values_json)
            .push(", ");
        query
            .push_bind(input.legacy_cost_usd_raw.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_created_at_raw.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_project_owner_raw.as_deref())
            .push(", ");
        query
            .push_bind(i64::from(input.legacy_invalid_usage))
            .push(", ");
        query.push_bind(input.provider_id.as_deref()).push(", ");
        query.push_bind(input.model_id.as_deref()).push(", ");
        query.push_bind(input.runtime_model.as_deref()).push(", ");
        query.push_bind(input.candidate_key.as_deref()).push(", ");
        query.push_bind(input.attempt_ordinal).push(", ");
        query.push_bind(input.agent_id.as_deref()).push(", ");
        query.push_bind(input.profile_id.as_deref()).push(", ");
        query
            .push_bind(input.agent_name_snapshot.as_deref())
            .push(", ");
        query
            .push_bind(input.project_name_snapshot.as_deref())
            .push(", ");
        query.push_bind(input.executor_type.as_deref()).push(", ");
        query
            .push_bind(input.pricing_subject_revision_id.as_deref())
            .push(", ");
        query
            .push_bind(input.subject_revision_digest.as_deref())
            .push(", ");
        query
            .push_bind(input.telemetry_state.to_string())
            .push(", ");
        query.push_bind(input.input_tokens).push(", ");
        query.push_bind(input.output_tokens).push(", ");
        query.push_bind(input.cache_read_tokens).push(", ");
        query.push_bind(input.cache_write_tokens).push(", ");
        query.push_bind(input.context_tokens).push(", ");
        query.push_bind(input.selected_tier.as_deref()).push(", ");
        query.push_bind(input.provider_reported_nano_usd).push(", ");
        query.push_bind(input.legacy_reported_cost_usd).push(", ");
        query.push_bind(input.estimated_nano_usd).push(", ");
        query.push_bind(input.cost_kind.to_string()).push(", ");
        query
            .push_bind(input.rate_revision_id.as_deref())
            .push(", ");
        query
            .push_bind(input.catalog_snapshot_id.as_deref())
            .push(", ");
        query
            .push_bind(input.formula_revision.as_deref())
            .push(", ");
        query.push_bind(i64::from(input.retrospective)).push(", ");
        query
            .push_bind(input.coverage_reason_code.map(|value| value.to_string()))
            .push(", ");
        query.push_bind(&input.occurred_at).push(", ");
        query.push_bind(&input.created_at).push(")");
        query
            .build()
            .execute(&mut **transaction)
            .await
            .map_err(duplicate_error)?;
        select_usage_event_in_tx(transaction, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn settle_usage_invocations_with_events(
        &self,
        settlements: Vec<UsageLedgerSettlement>,
    ) -> Result<Vec<UsageInvocation>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let mut settled = Vec::with_capacity(settlements.len());
        for settlement in settlements {
            let existing = select_invocation_in_tx(&mut transaction, &settlement.invocation_id)
                .await?
                .ok_or(DbError::NotFound)?;
            if existing.lifecycle == UsageInvocationLifecycle::Settled {
                validate_settled_usage_invocation_in_tx(&mut transaction, &existing, &settlement)
                    .await?;
                settled.push(existing);
                continue;
            }
            if existing.lifecycle == UsageInvocationLifecycle::Unsettled {
                return Err(DbError::IdempotencyConflict);
            }
            let invocation = self
                .settle_usage_invocation_in_tx(
                    &mut transaction,
                    SettleUsageInvocation {
                        id: settlement.invocation_id.clone(),
                        expected_version: settlement.expected_version,
                        telemetry_state: settlement.telemetry_state,
                        terminal_reason: settlement.terminal_reason,
                        settled_at: settlement.settled_at,
                        updated_at: settlement.updated_at,
                    },
                )
                .await?;
            for event in settlement.events {
                self.append_usage_event_in_tx(&mut transaction, event)
                    .await?;
            }
            settled.push(invocation);
        }
        transaction.commit().await?;
        Ok(settled)
    }

    async fn get_usage_event(&self, id: &str) -> Result<Option<UsageEvent>> {
        sqlx::query("SELECT * FROM usage_event WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_usage_event)
            .transpose()
    }

    async fn get_usage_event_by_idempotency(
        &self,
        event_idempotency_key: &str,
    ) -> Result<Option<UsageEvent>> {
        sqlx::query("SELECT * FROM usage_event WHERE event_idempotency_key = ?")
            .bind(event_idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_usage_event)
            .transpose()
    }

    async fn list_usage_events_for_invocation(
        &self,
        invocation_id: &str,
    ) -> Result<Vec<UsageEvent>> {
        sqlx::query(
            "SELECT * FROM usage_event
             WHERE invocation_id = ? ORDER BY occurred_at ASC, id ASC",
        )
        .bind(invocation_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_usage_event)
        .collect()
    }

    async fn list_usage_events_for_invocation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        invocation_id: &str,
    ) -> Result<Vec<UsageEvent>> {
        sqlx::query(
            "SELECT * FROM usage_event
             WHERE invocation_id = ? ORDER BY occurred_at ASC, id ASC",
        )
        .bind(invocation_id)
        .fetch_all(&mut **transaction)
        .await?
        .into_iter()
        .map(map_usage_event)
        .collect()
    }

    async fn list_usage_events_for_project(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<UsageEvent>> {
        list_events_with_scope(&self.pool, Some(project_id), None, from, to).await
    }

    async fn list_usage_events_for_owner(
        &self,
        owner_user_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<UsageEvent>> {
        list_events_with_scope(&self.pool, None, Some(owner_user_id), from, to).await
    }
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

#[async_trait]
impl RetrospectiveEstimateRepo for SqliteDb {
    async fn create_cost_estimation_preview(
        &self,
        input: CreateCostEstimationPreview,
    ) -> Result<CostEstimationPreview> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let preview = self
            .create_cost_estimation_preview_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(preview)
    }

    async fn create_cost_estimation_preview_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateCostEstimationPreview,
    ) -> Result<CostEstimationPreview> {
        if let Some(row) =
            sqlx::query("SELECT * FROM cost_estimation_preview WHERE idempotency_key = ?")
                .bind(&input.idempotency_key)
                .fetch_optional(&mut **transaction)
                .await?
        {
            let existing = map_preview(row)?;
            if existing.id == input.id
                && existing.owner_user_id == input.owner_user_id
                && existing.project_id == input.project_id
                && existing.catalog_snapshot_id == input.catalog_snapshot_id
                && existing.catalog_freshness == input.catalog_freshness
                && existing.usage_set_digest == input.usage_set_digest
                && existing.window_from == input.window_from
                && existing.window_to == input.window_to
                && existing.filters_json == input.filters_json
                && existing.expires_at == input.expires_at
                && existing.created_at == input.created_at
            {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }
        sqlx::query(
            "INSERT INTO cost_estimation_preview (
                id, owner_user_id, project_id, catalog_snapshot_id,
                catalog_freshness, usage_set_digest, window_from, window_to, filters_json,
                eligible_event_count, unmatched_event_count,
                already_reported_event_count, projected_cost_summary_json,
                status, idempotency_key, version, expires_at, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, 1, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.owner_user_id)
        .bind(&input.project_id)
        .bind(&input.catalog_snapshot_id)
        .bind(input.catalog_freshness.to_string())
        .bind(&input.usage_set_digest)
        .bind(input.window_from.as_deref())
        .bind(input.window_to.as_deref())
        .bind(&input.filters_json)
        .bind(input.eligible_event_count)
        .bind(input.unmatched_event_count)
        .bind(input.already_reported_event_count)
        .bind(&input.projected_cost_summary_json)
        .bind(&input.idempotency_key)
        .bind(&input.expires_at)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        sqlx::query("SELECT * FROM cost_estimation_preview WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_preview)
    }

    async fn get_cost_estimation_preview(&self, id: &str) -> Result<Option<CostEstimationPreview>> {
        sqlx::query("SELECT * FROM cost_estimation_preview WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_preview)
            .transpose()
    }

    async fn get_cost_estimation_preview_by_idempotency(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<CostEstimationPreview>> {
        sqlx::query("SELECT * FROM cost_estimation_preview WHERE idempotency_key = ?")
            .bind(idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_preview)
            .transpose()
    }

    async fn list_cost_estimation_previews(
        &self,
        project_id: &str,
    ) -> Result<Vec<CostEstimationPreview>> {
        sqlx::query(
            "SELECT * FROM cost_estimation_preview
             WHERE project_id = ? ORDER BY created_at DESC, id DESC",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_preview)
        .collect()
    }

    async fn update_cost_estimation_preview(
        &self,
        input: UpdateCostEstimationPreview,
    ) -> Result<CostEstimationPreview> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let preview = self
            .update_cost_estimation_preview_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(preview)
    }

    async fn update_cost_estimation_preview_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdateCostEstimationPreview,
    ) -> Result<CostEstimationPreview> {
        let current = sqlx::query("SELECT * FROM cost_estimation_preview WHERE id = ?")
            .bind(&input.id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(check_error)?
            .map(map_preview)
            .transpose()?
            .ok_or(DbError::NotFound)?;
        let result = sqlx::query(
            "UPDATE cost_estimation_preview
             SET status = ?, eligible_event_count = ?, unmatched_event_count = ?,
                 already_reported_event_count = ?, projected_cost_summary_json = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(input.status.to_string())
        .bind(
            input
                .eligible_event_count
                .unwrap_or(current.eligible_event_count),
        )
        .bind(
            input
                .unmatched_event_count
                .unwrap_or(current.unmatched_event_count),
        )
        .bind(
            input
                .already_reported_event_count
                .unwrap_or(current.already_reported_event_count),
        )
        .bind(
            input
                .projected_cost_summary_json
                .unwrap_or(current.projected_cost_summary_json),
        )
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "cost_estimation_preview", &input.id).await?,
            );
        }
        sqlx::query("SELECT * FROM cost_estimation_preview WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_preview)
    }

    async fn create_cost_estimation_run(
        &self,
        input: CreateCostEstimationRun,
    ) -> Result<CostEstimationRun> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let run = self
            .create_cost_estimation_run_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(run)
    }

    async fn create_cost_estimation_run_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateCostEstimationRun,
    ) -> Result<CostEstimationRun> {
        if let Some(row) =
            sqlx::query("SELECT * FROM cost_estimation_run WHERE idempotency_key = ?")
                .bind(&input.idempotency_key)
                .fetch_optional(&mut **transaction)
                .await?
        {
            let existing = map_run(row)?;
            if existing.id == input.id
                && existing.owner_user_id == input.owner_user_id
                && existing.project_id == input.project_id
                && existing.preview_id == input.preview_id
                && existing.catalog_snapshot_id == input.catalog_snapshot_id
                && existing.usage_set_digest == input.usage_set_digest
                && existing.supersedes_run_id == input.supersedes_run_id
                && existing.created_at == input.created_at
            {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }
        sqlx::query(
            "INSERT INTO cost_estimation_run (
                id, owner_user_id, project_id, preview_id, catalog_snapshot_id,
                usage_set_digest, status, applied_event_count,
                unmatched_event_count, already_reported_event_count,
                cost_summary_json, idempotency_key, supersedes_run_id, version,
                created_at, updated_at, completed_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, 1, ?, ?, NULL)",
        )
        .bind(&input.id)
        .bind(&input.owner_user_id)
        .bind(&input.project_id)
        .bind(&input.preview_id)
        .bind(&input.catalog_snapshot_id)
        .bind(&input.usage_set_digest)
        .bind(input.status.to_string())
        .bind(&input.cost_summary_json)
        .bind(&input.idempotency_key)
        .bind(input.supersedes_run_id.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        sqlx::query("SELECT * FROM cost_estimation_run WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_run)
    }

    async fn get_cost_estimation_run(&self, id: &str) -> Result<Option<CostEstimationRun>> {
        sqlx::query("SELECT * FROM cost_estimation_run WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_run)
            .transpose()
    }

    async fn get_cost_estimation_run_by_idempotency(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<CostEstimationRun>> {
        sqlx::query("SELECT * FROM cost_estimation_run WHERE idempotency_key = ?")
            .bind(idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_run)
            .transpose()
    }

    async fn list_cost_estimation_runs(&self, project_id: &str) -> Result<Vec<CostEstimationRun>> {
        sqlx::query(
            "SELECT * FROM cost_estimation_run
             WHERE project_id = ? ORDER BY created_at DESC, id DESC",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_run)
        .collect()
    }

    async fn update_cost_estimation_run(
        &self,
        input: UpdateCostEstimationRun,
    ) -> Result<CostEstimationRun> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let run = self
            .update_cost_estimation_run_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(run)
    }

    async fn update_cost_estimation_run_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdateCostEstimationRun,
    ) -> Result<CostEstimationRun> {
        let current = sqlx::query("SELECT * FROM cost_estimation_run WHERE id = ?")
            .bind(&input.id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(check_error)?
            .map(map_run)
            .transpose()?
            .ok_or(DbError::NotFound)?;
        let result = sqlx::query(
            "UPDATE cost_estimation_run
             SET status = ?, applied_event_count = ?, unmatched_event_count = ?,
                 already_reported_event_count = ?, cost_summary_json = ?,
                 completed_at = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(input.status.to_string())
        .bind(
            input
                .applied_event_count
                .unwrap_or(current.applied_event_count),
        )
        .bind(
            input
                .unmatched_event_count
                .unwrap_or(current.unmatched_event_count),
        )
        .bind(
            input
                .already_reported_event_count
                .unwrap_or(current.already_reported_event_count),
        )
        .bind(input.cost_summary_json.unwrap_or(current.cost_summary_json))
        .bind(input.completed_at.unwrap_or(current.completed_at))
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "cost_estimation_run", &input.id).await?,
            );
        }
        sqlx::query("SELECT * FROM cost_estimation_run WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_run)
    }

    async fn create_cost_estimate_revision(
        &self,
        input: CreateCostEstimateRevision,
    ) -> Result<CostEstimateRevision> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let revision = self
            .create_cost_estimate_revision_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(revision)
    }

    async fn create_cost_estimate_revision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateCostEstimateRevision,
    ) -> Result<CostEstimateRevision> {
        if let Some(row) = sqlx::query(
            "SELECT * FROM cost_estimate_revision
             WHERE run_id = ? AND usage_event_id = ?",
        )
        .bind(&input.run_id)
        .bind(&input.usage_event_id)
        .fetch_optional(&mut **transaction)
        .await?
        {
            let existing = map_estimate_revision(row)?;
            if existing.id == input.id
                && existing.owner_user_id == input.owner_user_id
                && existing.project_id == input.project_id
                && existing.usage_event_id == input.usage_event_id
                && existing.revision == input.revision
                && existing.supersedes_revision_id == input.supersedes_revision_id
                && existing.estimate_digest == input.estimate_digest
                && existing.state == input.state
                && existing.rate_revision_id == input.rate_revision_id
                && existing.catalog_snapshot_id == input.catalog_snapshot_id
                && existing.estimated_nano_usd == input.estimated_nano_usd
                && existing.formula_revision == input.formula_revision
                && existing.reason_code == input.reason_code
                && existing.created_at == input.created_at
            {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }
        if input.state == CostEstimateRevisionState::Applied {
            let provider_reported: Option<String> =
                sqlx::query_scalar("SELECT cost_kind FROM usage_event WHERE id = ?")
                    .bind(&input.usage_event_id)
                    .fetch_optional(&mut **transaction)
                    .await?;
            if provider_reported.as_deref() == Some("provider_reported") {
                return Err(DbError::Check(
                    "retrospective estimates cannot replace provider-reported cost".to_owned(),
                ));
            }
        }
        sqlx::query(
            "INSERT INTO cost_estimate_revision (
                id, run_id, owner_user_id, project_id, usage_event_id, revision,
                supersedes_revision_id, state, rate_revision_id,
                catalog_snapshot_id, estimated_nano_usd, formula_revision,
                retrospective, reason_code, estimate_digest, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.run_id)
        .bind(&input.owner_user_id)
        .bind(&input.project_id)
        .bind(&input.usage_event_id)
        .bind(input.revision)
        .bind(input.supersedes_revision_id.as_deref())
        .bind(input.state.to_string())
        .bind(input.rate_revision_id.as_deref())
        .bind(input.catalog_snapshot_id.as_deref())
        .bind(input.estimated_nano_usd)
        .bind(input.formula_revision.as_deref())
        .bind(input.reason_code.map(|value| value.to_string()))
        .bind(&input.estimate_digest)
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        sqlx::query("SELECT * FROM cost_estimate_revision WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_estimate_revision)
    }

    async fn get_cost_estimate_revision(&self, id: &str) -> Result<Option<CostEstimateRevision>> {
        sqlx::query("SELECT * FROM cost_estimate_revision WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_estimate_revision)
            .transpose()
    }

    async fn get_cost_estimate_revision_by_digest(
        &self,
        estimate_digest: &str,
    ) -> Result<Option<CostEstimateRevision>> {
        sqlx::query("SELECT * FROM cost_estimate_revision WHERE estimate_digest = ?")
            .bind(estimate_digest)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_estimate_revision)
            .transpose()
    }

    async fn list_cost_estimate_revisions_for_run(
        &self,
        run_id: &str,
    ) -> Result<Vec<CostEstimateRevision>> {
        sqlx::query(
            "SELECT * FROM cost_estimate_revision
             WHERE run_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_estimate_revision)
        .collect()
    }

    async fn list_cost_estimate_revisions_for_event(
        &self,
        usage_event_id: &str,
    ) -> Result<Vec<CostEstimateRevision>> {
        sqlx::query(
            "SELECT * FROM cost_estimate_revision
             WHERE usage_event_id = ? ORDER BY revision DESC, created_at DESC, id DESC",
        )
        .bind(usage_event_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_estimate_revision)
        .collect()
    }
}
