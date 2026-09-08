//! SQLite adapters for the pricing service-domain boundaries.
//!
//! The pricing domain deliberately does not depend on a concrete store.  This
//! module is the explicit conversion layer for the V135 SQLite repositories:
//! immutable catalog snapshots/rate revisions, exact subject bindings, and
//! retrospective estimate envelopes/revisions.  All multi-row writes use the
//! public transaction forms exposed by `db`; a failed conversion or CAS drops
//! the transaction and therefore leaves the previous last-known-good state
//! untouched.

use std::{collections::BTreeMap, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, Transaction};

use crate::pricing::{
    self, CatalogFreshness, CatalogModelRate, CatalogRefreshErrorCode, CatalogRefreshOutcome,
    CatalogRepositoryError, CatalogSnapshot, CatalogState, CatalogStatus, ContextTierState,
    EventBucketRates, EventTokenCounts, ManualRateOverride, NanoUsd, NanoUsdPerMillion,
    PriceSelectionReasonCode, PricingBindingRepository, PricingBindingSource, PricingBindingState,
    PricingCatalogRepository, PricingConfiguration, PricingSubjectIdentity, ReplacePricingRequest,
    RetrospectiveCommitRequest, RetrospectiveEstimateCandidate, RetrospectiveEstimateRepository,
    RetrospectivePreview, RetrospectiveRepositoryError, RetrospectiveRun,
    RetrospectiveUnmatchedEvent, RetrospectiveUsageEvent,
};

/// Concrete V135 persistence boundary used by services and tests.
#[derive(Debug, Clone)]
pub struct SqlitePricingRepository {
    db: Arc<db::SqliteDb>,
}

/// SQLite text columns and service identifiers are intentionally bounded at
/// the service boundary.  The migration also rejects blank values, but a
/// bounded check here keeps malformed input from becoming a large error or
/// digest payload before SQL validation runs.
const MAX_PRICING_IDENTIFIER_BYTES: usize = 256;
const MAX_PRICING_OPTIONAL_TEXT_BYTES: usize = 512;
const MAX_PRICING_BINDINGS: usize = 1_000;
const MAX_PRICING_CONTEXT_TIERS: usize = 1_000;
const MAX_RETROSPECTIVE_SOURCE_EVENTS: usize = 100_000;
const MAX_RETROSPECTIVE_PREVIEW_TTL: std::time::Duration =
    std::time::Duration::from_secs(24 * 60 * 60);
const CATALOG_REFRESH_OPERATION_SCOPE: &str = "catalog_refresh";
const RETROSPECTIVE_COMMIT_OPERATION_SCOPE: &str = "retrospective_commit";

fn retrospective_commit_idempotency_key(key: &str) -> String {
    format!("retrospective_commit:{key}")
}

fn validate_identifier(value: &str, field: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if value.len() > MAX_PRICING_IDENTIFIER_BYTES {
        return Err(format!("{field} exceeds the identifier length limit"));
    }
    Ok(())
}

fn validate_optional_text(value: Option<&str>, field: &str) -> Result<(), String> {
    if let Some(value) = value {
        if value.len() > MAX_PRICING_OPTIONAL_TEXT_BYTES {
            return Err(format!("{field} exceeds the text length limit"));
        }
    }
    Ok(())
}

fn validate_event_bucket_rates(rates: EventBucketRates, field: &str) -> Result<(), String> {
    for (name, value) in [
        ("input", rates.input),
        ("output", rates.output),
        ("cache_read", rates.cache_read),
        ("cache_write", rates.cache_write),
    ] {
        if value.is_some_and(|value| {
            value.as_nano_usd_per_million() > pricing::MODELS_DEV_MAX_RATE_NANO_USD_PER_MILLION
        }) {
            return Err(format!(
                "{field}.{name} exceeds the supported $1,000,000/million bound"
            ));
        }
    }
    Ok(())
}

/// Bounds caller-provided replacement data before it participates in a
/// request digest or is compared with mutable rows.  Catalog bindings are
/// normally produced from an already bounded models.dev snapshot, but this
/// boundary is also reachable from API deserializers and tests, so it must not
/// assume that provenance fragments or nested tiers are trusted.
fn validate_replace_request_bounds(
    subject_id: &str,
    request: &ReplacePricingRequest,
) -> Result<(), CatalogRepositoryError> {
    validate_identifier(subject_id, "pricing subject id").map_err(catalog_conversion)?;
    validate_identifier(
        &request.subject_revision_digest,
        "pricing subject revision digest",
    )
    .map_err(catalog_conversion)?;
    if request.bindings.len() > MAX_PRICING_BINDINGS {
        return Err(catalog_conversion(
            "pricing binding request contains too many bindings",
        ));
    }

    let mut provenance_bytes = 0usize;
    for (index, binding) in request.bindings.iter().enumerate() {
        let path = format!("pricing bindings[{index}]");
        validate_identifier(&binding.runtime_model, &format!("{path}.runtime_model"))
            .map_err(catalog_conversion)?;
        match &binding.source {
            pricing::DesiredPricingSource::ModelsDev {
                provider_id,
                model_id,
                snapshot_id,
                rate_revision_id,
                rates,
                tiers,
                legacy_context_over_200k,
                ..
            } => {
                for (field, value) in [
                    ("provider_id", provider_id),
                    ("model_id", model_id),
                    ("snapshot_id", snapshot_id),
                    ("rate_revision_id", rate_revision_id),
                ] {
                    validate_identifier(value, &format!("{path}.{field}"))
                        .map_err(catalog_conversion)?;
                }
                validate_event_bucket_rates(*rates, &format!("{path}.rates"))
                    .map_err(catalog_conversion)?;
                if tiers.len() > MAX_PRICING_CONTEXT_TIERS {
                    return Err(catalog_conversion(format!(
                        "{path}.tiers contains too many context tiers"
                    )));
                }
                for (tier_index, tier) in tiers.iter().enumerate() {
                    if tier.threshold_tokens == 0 {
                        return Err(catalog_conversion(format!(
                            "{path}.tiers[{tier_index}].threshold_tokens must be positive"
                        )));
                    }
                    validate_event_bucket_rates(
                        tier.rates,
                        &format!("{path}.tiers[{tier_index}].rates"),
                    )
                    .map_err(catalog_conversion)?;
                    provenance_bytes = provenance_bytes
                        .checked_add(tier.raw_json.len())
                        .ok_or_else(|| {
                            catalog_conversion("pricing binding provenance size overflow")
                        })?;
                }
                if let Some(legacy) = legacy_context_over_200k {
                    validate_event_bucket_rates(
                        legacy.rates,
                        &format!("{path}.legacy_context_over_200k.rates"),
                    )
                    .map_err(catalog_conversion)?;
                    provenance_bytes = provenance_bytes
                        .checked_add(legacy.raw_json.len())
                        .ok_or_else(|| {
                            catalog_conversion("pricing binding provenance size overflow")
                        })?;
                }
                if provenance_bytes > pricing::MODELS_DEV_MAX_RESPONSE_BYTES {
                    return Err(catalog_conversion(
                        "pricing binding provenance exceeds the catalog size bound",
                    ));
                }
            }
            pricing::DesiredPricingSource::Manual { rates } => {
                validate_event_bucket_rates(*rates, &format!("{path}.rates"))
                    .map_err(catalog_conversion)?;
            }
        }
    }
    Ok(())
}

/// Hash a tagged sequence with explicit presence markers and length prefixes.
/// Delimiter-joined digests are ambiguous when identifiers contain the
/// delimiter (model IDs may legally contain `/`, `|`, or newlines).
fn structured_digest(tag: &str, fields: impl IntoIterator<Item = Option<String>>) -> String {
    let mut bytes = Vec::new();
    append_digest_field(&mut bytes, Some(tag));
    for field in fields {
        append_digest_field(&mut bytes, field.as_deref());
    }
    pricing::sha256_hex(&bytes)
}

fn append_digest_field(bytes: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => bytes.push(0),
        Some(value) => {
            bytes.push(1);
            let length = u64::try_from(value.len()).unwrap_or(u64::MAX);
            bytes.extend_from_slice(&length.to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
    }
}

/// Looks up an immutable pricing-operation receipt while the caller owns the
/// write transaction.  A key is scoped by operation, and reusing it with a
/// different canonical request digest is always a conflict, even if a newer
/// mutation has since advanced the mutable state pointer.
async fn operation_receipt_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    operation_scope: &str,
    idempotency_key: &str,
    request_digest: &str,
) -> Result<Option<String>, String> {
    let Some(row) = sqlx::query(
        "SELECT request_digest, result_json
         FROM pricing_operation_receipt
         WHERE operation_scope = ? AND idempotency_key = ?",
    )
    .bind(operation_scope)
    .bind(idempotency_key)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| format!("pricing operation receipt lookup failed: {error}"))?
    else {
        return Ok(None);
    };
    let stored_digest: String = row
        .try_get("request_digest")
        .map_err(|error| format!("pricing operation receipt digest is invalid: {error}"))?;
    if stored_digest != request_digest {
        return Err(format!(
            "idempotency key {idempotency_key:?} was reused with a different request payload"
        ));
    }
    let result_json: String = row
        .try_get("result_json")
        .map_err(|error| format!("pricing operation receipt result is invalid: {error}"))?;
    Ok(Some(result_json))
}

/// Inserts an immutable operation receipt after the operation's durable rows
/// have been written, but before the surrounding transaction commits.
async fn insert_operation_receipt_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    operation_scope: &str,
    idempotency_key: &str,
    request_digest: &str,
    result_json: &str,
    created_at: &str,
) -> Result<(), String> {
    if result_json.len() > pricing::MODELS_DEV_MAX_RESPONSE_BYTES {
        return Err("pricing operation receipt result exceeds the size bound".to_owned());
    }
    sqlx::query(
        "INSERT INTO pricing_operation_receipt (
            operation_scope, idempotency_key, request_digest, result_json, created_at
         ) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(operation_scope)
    .bind(idempotency_key)
    .bind(request_digest)
    .bind(result_json)
    .bind(created_at)
    .execute(&mut **transaction)
    .await
    .map_err(|error| format!("pricing operation receipt insert failed: {error}"))?;
    Ok(())
}

impl SqlitePricingRepository {
    /// Creates an adapter over an already configured SQLite pool.
    pub fn new(db: Arc<db::SqliteDb>) -> Self {
        Self { db }
    }

    /// Returns the underlying database handle for composition by a service.
    pub fn database(&self) -> &Arc<db::SqliteDb> {
        &self.db
    }

    /// Loads and validates one immutable catalog snapshot for API callers.
    ///
    /// The route layer must not duplicate payload parsing or source-endpoint
    /// validation. Keeping this conversion here also means a corrupted row
    /// is surfaced as a bounded repository failure rather than becoming a
    /// second, subtly different snapshot representation at the HTTP edge.
    pub async fn catalog_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<CatalogSnapshot>, CatalogRepositoryError> {
        let snapshot = db::PricingCatalogRepo::get_pricing_catalog_snapshot(&*self.db, snapshot_id)
            .await
            .map_err(catalog_error)?;
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        snapshot_from_db(snapshot)
            .await
            .map(Some)
            .map_err(catalog_conversion)
    }
}

fn db_time(time: SystemTime) -> String {
    DateTime::<Utc>::from(time).to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn system_time(value: &str) -> Result<SystemTime, String> {
    DateTime::parse_from_rfc3339(value)
        .map(|parsed| parsed.with_timezone(&Utc).into())
        .map_err(|error| format!("invalid RFC3339 timestamp: {error}"))
}

fn optional_system_time(value: Option<&str>) -> Result<Option<SystemTime>, String> {
    value.map(system_time).transpose()
}

fn db_rates(rates: EventBucketRates) -> db::RateBuckets {
    db::RateBuckets::new(
        rates.input.map(NanoUsdPerMillion::as_nano_usd_per_million),
        rates.output.map(NanoUsdPerMillion::as_nano_usd_per_million),
        rates
            .cache_read
            .map(NanoUsdPerMillion::as_nano_usd_per_million),
        rates
            .cache_write
            .map(NanoUsdPerMillion::as_nano_usd_per_million),
    )
}

fn service_rate(value: Option<i64>, field: &str) -> Result<Option<NanoUsdPerMillion>, String> {
    value
        .map(|value| {
            NanoUsdPerMillion::from_nano_usd(value)
                .ok_or_else(|| format!("{field} contains a negative nano-USD rate"))
        })
        .transpose()
}

fn service_rates(rates: db::RateBuckets) -> Result<EventBucketRates, String> {
    Ok(EventBucketRates::new(
        service_rate(rates.input, "input")?,
        service_rate(rates.output, "output")?,
        service_rate(rates.cache_read, "cache_read")?,
        service_rate(rates.cache_write, "cache_write")?,
    ))
}

fn catalog_error(error: db::DbError) -> CatalogRepositoryError {
    match error {
        db::DbError::VersionConflict | db::DbError::IdempotencyConflict => {
            CatalogRepositoryError::Conflict(error.to_string())
        }
        other => CatalogRepositoryError::Unavailable(other.to_string()),
    }
}

fn catalog_sqlx(error: sqlx::Error) -> CatalogRepositoryError {
    catalog_error(db::DbError::Sqlx(error))
}

fn catalog_conversion(error: impl Into<String>) -> CatalogRepositoryError {
    CatalogRepositoryError::Unavailable(error.into())
}

fn catalog_operation_error(error: String) -> CatalogRepositoryError {
    if error.contains("reused with a different request payload") {
        CatalogRepositoryError::Conflict(error)
    } else {
        CatalogRepositoryError::Unavailable(error)
    }
}

fn retrospective_error(error: db::DbError) -> RetrospectiveRepositoryError {
    match error {
        db::DbError::VersionConflict | db::DbError::IdempotencyConflict => {
            RetrospectiveRepositoryError::Conflict
        }
        other => RetrospectiveRepositoryError::Unavailable(other.to_string()),
    }
}

fn retrospective_sqlx(error: sqlx::Error) -> RetrospectiveRepositoryError {
    retrospective_error(db::DbError::Sqlx(error))
}

fn retrospective_conversion(error: impl Into<String>) -> RetrospectiveRepositoryError {
    RetrospectiveRepositoryError::Unavailable(error.into())
}

fn retrospective_operation_error(error: String) -> RetrospectiveRepositoryError {
    if error.contains("reused with a different request payload") {
        RetrospectiveRepositoryError::Conflict
    } else {
        RetrospectiveRepositoryError::Unavailable(error)
    }
}

fn catalog_state(value: db::PricingCatalogStateKind) -> CatalogState {
    match value {
        db::PricingCatalogStateKind::Absent => CatalogState::Absent,
        db::PricingCatalogStateKind::Fresh => CatalogState::Fresh,
        db::PricingCatalogStateKind::Stale => CatalogState::Stale,
        db::PricingCatalogStateKind::RefreshFailed => CatalogState::RefreshFailed,
    }
}

fn catalog_status_from_db(
    value: db::PricingCatalogState,
    revision: Option<String>,
) -> Result<CatalogStatus, String> {
    Ok(CatalogStatus {
        state: catalog_state(value.state),
        active_snapshot_id: value.active_snapshot_id,
        revision,
        etag: value.http_etag,
        last_checked_at: optional_system_time(value.last_checked_at.as_deref())?,
        last_successful_check_at: optional_system_time(value.last_successful_check_at.as_deref())?,
        stale_after: optional_system_time(value.stale_after.as_deref())?,
        last_error_code: value.last_error_code,
        last_idempotency_key: value.last_idempotency_key,
        version: value
            .version
            .try_into()
            .map_err(|_| "catalog state version is negative".to_owned())?,
    })
}

fn catalog_state_from_name(value: &str) -> Result<CatalogState, String> {
    match value {
        "absent" => Ok(CatalogState::Absent),
        "fresh" => Ok(CatalogState::Fresh),
        "stale" => Ok(CatalogState::Stale),
        "refresh_failed" => Ok(CatalogState::RefreshFailed),
        other => Err(format!("unknown catalog status state {other:?}")),
    }
}

fn optional_json_string(value: Option<&Value>, field: &str) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::String(value) => {
            validate_optional_text(Some(value), field)?;
            Ok(Some(value.clone()))
        }
        _ => Err(format!(
            "catalog receipt field {field} must be a string or null"
        )),
    }
}

fn optional_json_time(value: Option<&Value>, field: &str) -> Result<Option<SystemTime>, String> {
    optional_json_string(value, field)?
        .map(|value| system_time(&value))
        .transpose()
}

fn catalog_status_result_json(
    status: &CatalogStatus,
    outcome: &str,
    snapshot_id: Option<&str>,
    error_code: Option<&str>,
) -> String {
    json!({
        "kind": "catalog_refresh",
        "outcome": outcome,
        "snapshot_id": snapshot_id,
        "error_code": error_code,
        "status": {
            "state": catalog_state_name(status.state),
            "active_snapshot_id": status.active_snapshot_id,
            "revision": status.revision,
            "etag": status.etag,
            "last_checked_at": status.last_checked_at.map(db_time),
            "last_successful_check_at": status.last_successful_check_at.map(db_time),
            "stale_after": status.stale_after.map(db_time),
            "last_error_code": status.last_error_code,
            "last_idempotency_key": status.last_idempotency_key,
            "version": status.version,
        },
    })
    .to_string()
}

fn catalog_refresh_error_code(value: &str) -> Result<CatalogRefreshErrorCode, String> {
    match value {
        "transport" => Ok(CatalogRefreshErrorCode::Transport),
        "http_status" => Ok(CatalogRefreshErrorCode::HttpStatus),
        "content_type" => Ok(CatalogRefreshErrorCode::ContentType),
        "response_too_large" => Ok(CatalogRefreshErrorCode::ResponseTooLarge),
        "invalid_payload" => Ok(CatalogRefreshErrorCode::InvalidPayload),
        "missing_snapshot" => Ok(CatalogRefreshErrorCode::MissingSnapshot),
        "invalid_request" => Ok(CatalogRefreshErrorCode::InvalidRequest),
        other => Err(format!("unknown catalog refresh error code {other:?}")),
    }
}

fn catalog_status_from_result_json(value: &str) -> Result<CatalogStatus, String> {
    let value: Value = serde_json::from_str(value)
        .map_err(|error| format!("catalog operation receipt result is invalid JSON: {error}"))?;
    let status = value
        .get("status")
        .and_then(Value::as_object)
        .ok_or_else(|| "catalog operation receipt omits status".to_owned())?;
    let state = status
        .get("state")
        .and_then(Value::as_str)
        .ok_or_else(|| "catalog operation receipt omits status.state".to_owned())
        .and_then(catalog_state_from_name)?;
    let active_snapshot_id =
        optional_json_string(status.get("active_snapshot_id"), "active_snapshot_id")?;
    let revision = optional_json_string(status.get("revision"), "revision")?;
    let etag = optional_json_string(status.get("etag"), "etag")?;
    let last_checked_at = optional_json_time(status.get("last_checked_at"), "last_checked_at")?;
    let last_successful_check_at = optional_json_time(
        status.get("last_successful_check_at"),
        "last_successful_check_at",
    )?;
    let stale_after = optional_json_time(status.get("stale_after"), "stale_after")?;
    let last_error_code = optional_json_string(status.get("last_error_code"), "last_error_code")?;
    let last_idempotency_key =
        optional_json_string(status.get("last_idempotency_key"), "last_idempotency_key")?;
    let version = status
        .get("version")
        .and_then(Value::as_u64)
        .ok_or_else(|| "catalog operation receipt omits status.version".to_owned())?;
    Ok(CatalogStatus {
        state,
        active_snapshot_id,
        revision,
        etag,
        last_checked_at,
        last_successful_check_at,
        stale_after,
        last_error_code,
        last_idempotency_key,
        version,
    })
}

fn context_tier_state(value: ContextTierState) -> &'static str {
    match value {
        ContextTierState::None => "none",
        ContextTierState::Resolved => "resolved",
        ContextTierState::ThresholdUnknown => "threshold_unknown",
    }
}

fn service_context_tier_state(value: &str) -> Result<ContextTierState, String> {
    match value {
        "none" => Ok(ContextTierState::None),
        "resolved" => Ok(ContextTierState::Resolved),
        "threshold_unknown" => Ok(ContextTierState::ThresholdUnknown),
        other => Err(format!("unknown context tier state {other:?}")),
    }
}

fn binding_state(value: db::PricingSubjectState) -> PricingBindingState {
    match value {
        db::PricingSubjectState::Active => PricingBindingState::Active,
        db::PricingSubjectState::Retired => PricingBindingState::Retired,
    }
}

fn source_rate_digest(rates: EventBucketRates) -> String {
    structured_digest(
        "pricing-rate-buckets-v1",
        [
            rates.input,
            rates.output,
            rates.cache_read,
            rates.cache_write,
        ]
        .iter()
        .map(|rate| rate.map(|rate| rate.as_nano_usd_per_million().to_string())),
    )
}

fn binding_digest(
    subject_id: &str,
    subject_revision_digest: &str,
    source: &PricingBindingSource,
) -> String {
    let mut fields = vec![
        Some(subject_id.to_owned()),
        Some(subject_revision_digest.to_owned()),
        Some(source.runtime_model().to_owned()),
        Some(source.kind().as_str().to_owned()),
    ];
    match source {
        PricingBindingSource::ModelsDev(binding) => {
            fields.extend([
                Some(binding.rate_revision_id.clone()),
                Some(binding.snapshot_id.clone()),
                Some(binding.provider_id.clone()),
                Some(binding.model_id.clone()),
                Some(source_rate_digest(binding.rates)),
                Some(binding.tiers.len().to_string()),
                Some(catalog_freshness_name(binding.catalog_freshness).to_owned()),
            ]);
            match binding.legacy_context_over_200k.as_ref() {
                Some(legacy) => fields.extend([
                    Some(source_rate_digest(legacy.rates)),
                    Some(legacy.raw_json.clone()),
                ]),
                None => fields.extend([None, None]),
            }
            for tier in &binding.tiers {
                fields.extend([
                    Some(tier.threshold_tokens.to_string()),
                    Some(source_rate_digest(tier.rates)),
                    Some(tier.raw_json.clone()),
                ]);
            }
        }
        PricingBindingSource::Manual(override_) => {
            fields.extend([
                Some(override_.rate_revision_id.clone()),
                Some(source_rate_digest(override_.rates)),
                Some(db_time(override_.effective_at)),
                override_.retired_at.map(db_time),
            ]);
        }
    }
    structured_digest("pricing-binding-v2", fields)
}

fn catalog_refresh_request_digest() -> String {
    // A refresh has no caller-supplied payload. Its idempotency key is the
    // operation identity; server timestamps, conditional response headers,
    // and the eventual outcome must not turn a retry into a new request.
    structured_digest("catalog-refresh-request-v2", [None])
}

fn manual_rates_json(rates: EventBucketRates) -> String {
    json!({
        "input_nano_usd_per_million": rates.input.map(NanoUsdPerMillion::as_nano_usd_per_million),
        "output_nano_usd_per_million": rates.output.map(NanoUsdPerMillion::as_nano_usd_per_million),
        "cache_read_nano_usd_per_million": rates.cache_read.map(NanoUsdPerMillion::as_nano_usd_per_million),
        "cache_write_nano_usd_per_million": rates.cache_write.map(NanoUsdPerMillion::as_nano_usd_per_million),
    })
    .to_string()
}

fn rates_result_json(rates: EventBucketRates) -> Value {
    json!({
        "input": rates.input.map(NanoUsdPerMillion::as_nano_usd_per_million),
        "output": rates.output.map(NanoUsdPerMillion::as_nano_usd_per_million),
        "cache_read": rates.cache_read.map(NanoUsdPerMillion::as_nano_usd_per_million),
        "cache_write": rates.cache_write.map(NanoUsdPerMillion::as_nano_usd_per_million),
    })
}

fn result_rate(
    value: &serde_json::Map<String, Value>,
    field: &str,
    path: &str,
) -> Result<Option<NanoUsdPerMillion>, String> {
    let value = value
        .get(field)
        .ok_or_else(|| format!("{path} omits {field}"))?;
    match value {
        Value::Null => Ok(None),
        Value::Number(value) => {
            let value = value
                .as_i64()
                .ok_or_else(|| format!("{path}.{field} must be a non-negative integer"))?;
            let rate = NanoUsdPerMillion::from_nano_usd(value)
                .ok_or_else(|| format!("{path}.{field} is negative"))?;
            if rate.as_nano_usd_per_million() > pricing::MAX_MANUAL_RATE_NANO_USD_PER_MILLION {
                return Err(format!("{path}.{field} exceeds the supported rate bound"));
            }
            Ok(Some(rate))
        }
        _ => Err(format!("{path}.{field} must be an integer or null")),
    }
}

fn result_rates(value: &Value, path: &str) -> Result<EventBucketRates, String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{path} must be an object"))?;
    Ok(EventBucketRates::new(
        result_rate(object, "input", path)?,
        result_rate(object, "output", path)?,
        result_rate(object, "cache_read", path)?,
        result_rate(object, "cache_write", path)?,
    ))
}

fn binding_state_name(state: PricingBindingState) -> &'static str {
    match state {
        PricingBindingState::Active => "active",
        PricingBindingState::Retired => "retired",
    }
}

fn binding_state_from_name(value: &str) -> Result<PricingBindingState, String> {
    match value {
        "active" => Ok(PricingBindingState::Active),
        "retired" => Ok(PricingBindingState::Retired),
        other => Err(format!("unknown pricing binding state {other:?}")),
    }
}

fn configuration_result_json(configuration: &PricingConfiguration) -> String {
    let bindings = configuration
        .bindings
        .iter()
        .map(|source| match source {
            PricingBindingSource::ModelsDev(binding) => json!({
                "source_kind": "models_dev_catalog",
                "id": binding.id,
                "provider_id": binding.provider_id,
                "model_id": binding.model_id,
                "rate_revision_id": binding.rate_revision_id,
                "snapshot_id": binding.snapshot_id,
                "runtime_model": binding.runtime_model,
                "rates": rates_result_json(binding.rates),
                "tiers": binding.tiers.iter().map(|tier| json!({
                    "threshold_tokens": tier.threshold_tokens,
                    "rates": rates_result_json(tier.rates),
                    "raw_json": tier.raw_json,
                })).collect::<Vec<_>>(),
                "legacy_context_over_200k": binding.legacy_context_over_200k.as_ref().map(|legacy| json!({
                    "rates": rates_result_json(legacy.rates),
                    "raw_json": legacy.raw_json,
                })),
                "catalog_freshness": catalog_freshness_name(binding.catalog_freshness),
                "state": binding_state_name(binding.state),
            }),
            PricingBindingSource::Manual(override_) => json!({
                "source_kind": "manual_override",
                "id": override_.id,
                "subject_revision_digest": override_.subject_revision_digest,
                "runtime_model": override_.runtime_model,
                "rate_revision_id": override_.rate_revision_id,
                "rates": rates_result_json(override_.rates),
                "effective_at": db_time(override_.effective_at),
                "retired_at": override_.retired_at.map(db_time),
            }),
        })
        .collect::<Vec<_>>();
    json!({
        "kind": "pricing_configuration",
        "result_ref": {
            "subject_id": configuration.subject_id,
            "version": configuration.version,
        },
        "configuration": {
            "subject_id": configuration.subject_id,
            "subject_revision_digest": configuration.subject_revision_digest,
            "version": configuration.version,
            "bindings": bindings,
            "last_idempotency_key": configuration.last_idempotency_key,
            "last_update_digest": configuration.last_update_digest,
        },
    })
    .to_string()
}

fn optional_result_string(
    object: &serde_json::Map<String, Value>,
    field: &str,
    path: &str,
) -> Result<Option<String>, String> {
    optional_json_string(object.get(field), path)
}

fn required_result_string(
    object: &serde_json::Map<String, Value>,
    field: &str,
    path: &str,
) -> Result<String, String> {
    let value = object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{path} omits {field}"))?;
    validate_identifier(value, path)?;
    Ok(value.to_owned())
}

fn required_result_text(
    object: &serde_json::Map<String, Value>,
    field: &str,
    path: &str,
    max_bytes: usize,
) -> Result<String, String> {
    let value = object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{path} omits {field}"))?;
    if value.is_empty() {
        return Err(format!("{path}.{field} must not be empty"));
    }
    if value.len() > max_bytes {
        return Err(format!("{path}.{field} exceeds the text length limit"));
    }
    Ok(value.to_owned())
}

fn result_tiers(value: Option<&Value>, path: &str) -> Result<Vec<pricing::ContextTier>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let tiers = value
        .as_array()
        .ok_or_else(|| format!("{path} must be an array"))?;
    if tiers.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS {
        return Err(format!("{path} contains too many tiers"));
    }
    let mut result = Vec::with_capacity(tiers.len());
    let mut thresholds = std::collections::BTreeSet::new();
    for (index, tier) in tiers.iter().enumerate() {
        let path = format!("{path}[{index}]");
        let object = tier
            .as_object()
            .ok_or_else(|| format!("{path} must be an object"))?;
        let threshold_tokens = object
            .get("threshold_tokens")
            .and_then(Value::as_u64)
            .filter(|value| *value > 0)
            .ok_or_else(|| format!("{path}.threshold_tokens must be positive"))?;
        if !thresholds.insert(threshold_tokens) {
            return Err(format!("{path}.threshold_tokens is duplicated"));
        }
        let raw_json = required_result_text(
            object,
            "raw_json",
            &path,
            pricing::MODELS_DEV_MAX_RESPONSE_BYTES,
        )?;
        result.push(pricing::ContextTier {
            threshold_tokens,
            rates: result_rates(
                object
                    .get("rates")
                    .ok_or_else(|| format!("{path} omits rates"))?,
                &format!("{path}.rates"),
            )?,
            raw_json,
        });
    }
    Ok(result)
}

fn result_legacy(
    value: Option<&Value>,
    path: &str,
) -> Result<Option<pricing::LegacyContextRate>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let object = value
        .as_object()
        .ok_or_else(|| format!("{path} must be an object or null"))?;
    let raw_json = required_result_text(
        object,
        "raw_json",
        path,
        pricing::MODELS_DEV_MAX_RESPONSE_BYTES,
    )?;
    Ok(Some(pricing::LegacyContextRate {
        rates: result_rates(
            object
                .get("rates")
                .ok_or_else(|| format!("{path} omits rates"))?,
            &format!("{path}.rates"),
        )?,
        raw_json,
    }))
}

fn configuration_from_result_json(value: &str) -> Result<PricingConfiguration, String> {
    let value: Value = serde_json::from_str(value)
        .map_err(|error| format!("pricing configuration receipt is invalid JSON: {error}"))?;
    let configuration = value
        .get("configuration")
        .and_then(Value::as_object)
        .ok_or_else(|| "pricing configuration receipt omits configuration".to_owned())?;
    let subject_id = required_result_string(configuration, "subject_id", "configuration")?;
    let subject_revision_digest =
        required_result_string(configuration, "subject_revision_digest", "configuration")?;
    let version = configuration
        .get("version")
        .and_then(Value::as_u64)
        .ok_or_else(|| "pricing configuration receipt omits configuration.version".to_owned())?;
    let bindings_value = configuration
        .get("bindings")
        .and_then(Value::as_array)
        .ok_or_else(|| "pricing configuration receipt omits configuration.bindings".to_owned())?;
    if bindings_value.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS {
        return Err("pricing configuration receipt contains too many bindings".to_owned());
    }
    let mut bindings = Vec::with_capacity(bindings_value.len());
    for (index, binding) in bindings_value.iter().enumerate() {
        let path = format!("configuration.bindings[{index}]");
        let object = binding
            .as_object()
            .ok_or_else(|| format!("{path} must be an object"))?;
        let source_kind = required_result_string(object, "source_kind", &path)?;
        let id = required_result_string(object, "id", &path)?;
        let runtime_model = required_result_string(object, "runtime_model", &path)?;
        let rates = result_rates(
            object
                .get("rates")
                .ok_or_else(|| format!("{path} omits rates"))?,
            &format!("{path}.rates"),
        )?;
        match source_kind.as_str() {
            "models_dev_catalog" => {
                let provider_id = required_result_string(object, "provider_id", &path)?;
                let model_id = required_result_string(object, "model_id", &path)?;
                let rate_revision_id = required_result_string(object, "rate_revision_id", &path)?;
                let snapshot_id = required_result_string(object, "snapshot_id", &path)?;
                let freshness = object
                    .get("catalog_freshness")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("{path} omits catalog_freshness"))
                    .and_then(parse_catalog_freshness)?;
                let state = object
                    .get("state")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("{path} omits state"))
                    .and_then(binding_state_from_name)?;
                bindings.push(PricingBindingSource::ModelsDev(
                    pricing::CatalogRateBinding {
                        id,
                        provider_id,
                        model_id,
                        rate_revision_id,
                        snapshot_id,
                        runtime_model,
                        rates,
                        tiers: result_tiers(object.get("tiers"), &format!("{path}.tiers"))?,
                        legacy_context_over_200k: result_legacy(
                            object.get("legacy_context_over_200k"),
                            &format!("{path}.legacy_context_over_200k"),
                        )?,
                        catalog_freshness: freshness,
                        state,
                    },
                ));
            }
            "manual_override" => {
                let subject_revision_digest =
                    required_result_string(object, "subject_revision_digest", &path)?;
                let rate_revision_id = required_result_string(object, "rate_revision_id", &path)?;
                let effective_at = object
                    .get("effective_at")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("{path} omits effective_at"))
                    .and_then(system_time)?;
                let retired_at = optional_json_time(object.get("retired_at"), "retired_at")?;
                bindings.push(PricingBindingSource::Manual(ManualRateOverride {
                    id,
                    subject_revision_digest,
                    runtime_model,
                    rate_revision_id,
                    rates,
                    effective_at,
                    retired_at,
                }));
            }
            other => return Err(format!("{path} has unknown source kind {other:?}")),
        }
    }
    Ok(PricingConfiguration {
        subject_id,
        subject_revision_digest,
        version,
        bindings,
        last_idempotency_key: optional_result_string(
            configuration,
            "last_idempotency_key",
            "configuration.last_idempotency_key",
        )?,
        last_update_digest: optional_result_string(
            configuration,
            "last_update_digest",
            "configuration.last_update_digest",
        )?,
    })
}

fn binding_request_digest(subject_id: &str, request: &ReplacePricingRequest) -> String {
    let mut binding_digests = request
        .bindings
        .iter()
        .map(pricing::binding_revision_digest)
        .collect::<Vec<_>>();
    binding_digests.sort();
    let mut fields = vec![
        Some(subject_id.to_owned()),
        Some(request.expected_version.to_string()),
        Some(request.subject_revision_digest.clone()),
    ];
    fields.extend(binding_digests.into_iter().map(Some));
    structured_digest("pricing-binding-replace-request-v1", fields)
}

fn snapshot_input(
    snapshot: &CatalogSnapshot,
) -> Result<db::CreatePricingCatalogSnapshot, CatalogRepositoryError> {
    validate_identifier(&snapshot.id, "catalog snapshot id").map_err(catalog_conversion)?;
    if snapshot.source_url != pricing::MODELS_DEV_ENDPOINT {
        return Err(catalog_conversion(
            "catalog source URL is not the fixed models.dev endpoint",
        ));
    }
    if snapshot.raw_payload.len() > pricing::MODELS_DEV_MAX_RESPONSE_BYTES {
        return Err(catalog_conversion(
            "catalog payload exceeds the configured size bound",
        ));
    }
    if pricing::sha256_hex(&snapshot.raw_payload) != snapshot.payload_sha256 {
        return Err(catalog_conversion(
            "catalog payload SHA-256 does not match its bytes",
        ));
    }
    if pricing::catalog_revision_digest(&snapshot.payload_sha256, &snapshot.parser_revision)
        != snapshot.revision_digest
    {
        return Err(catalog_conversion(
            "catalog revision digest does not match metadata",
        ));
    }
    validate_identifier(&snapshot.payload_sha256, "catalog payload digest")
        .map_err(catalog_conversion)?;
    validate_identifier(&snapshot.parser_revision, "catalog parser revision")
        .map_err(catalog_conversion)?;
    validate_identifier(&snapshot.revision_digest, "catalog revision digest")
        .map_err(catalog_conversion)?;
    validate_optional_text(snapshot.etag.as_deref(), "catalog ETag").map_err(catalog_conversion)?;
    let parsed = pricing::parse_models_dev_catalog(&snapshot.raw_payload).map_err(|error| {
        catalog_conversion(format!(
            "catalog normalized rows cannot be reconstructed from the payload: {error}"
        ))
    })?;
    if parsed.payload_sha256 != snapshot.payload_sha256
        || parsed.parser_revision != snapshot.parser_revision
        || parsed.models != snapshot.models
    {
        return Err(catalog_conversion(
            "catalog normalized rows do not match the immutable source payload",
        ));
    }
    let mut model_keys = std::collections::BTreeSet::new();
    for model in &snapshot.models {
        validate_identifier(&model.provider_id, "catalog provider id")
            .map_err(catalog_conversion)?;
        validate_identifier(&model.model_id, "catalog model id").map_err(catalog_conversion)?;
        validate_identifier(&model.source_model_key, "catalog source model key")
            .map_err(catalog_conversion)?;
        if model.source_model_key != model.model_id
            || !model_keys.insert((model.provider_id.as_str(), model.model_id.as_str()))
        {
            return Err(catalog_conversion(
                "catalog model keys must be unique and equal their exact model identifiers",
            ));
        }
        validate_optional_text(model.source_last_updated.as_deref(), "catalog last_updated")
            .map_err(catalog_conversion)?;
        validate_event_bucket_rates(model.rates, "catalog rates").map_err(catalog_conversion)?;
        let mut thresholds = std::collections::BTreeSet::new();
        for tier in &model.tiers {
            if tier.threshold_tokens == 0 || !thresholds.insert(tier.threshold_tokens) {
                return Err(catalog_conversion(
                    "catalog context tiers must have unique positive thresholds",
                ));
            }
            validate_event_bucket_rates(tier.rates, "catalog tier rates")
                .map_err(catalog_conversion)?;
            if tier.raw_json.len() > pricing::MODELS_DEV_MAX_RESPONSE_BYTES {
                return Err(catalog_conversion("catalog tier provenance is too large"));
            }
        }
        if let Some(legacy) = &model.legacy_context_over_200k {
            validate_event_bucket_rates(legacy.rates, "catalog legacy rates")
                .map_err(catalog_conversion)?;
            if legacy.raw_json.len() > pricing::MODELS_DEV_MAX_RESPONSE_BYTES {
                return Err(catalog_conversion("catalog legacy provenance is too large"));
            }
        }
        if model.received_rates_json().len() > pricing::MODELS_DEV_MAX_RESPONSE_BYTES {
            return Err(catalog_conversion(
                "catalog received rates provenance is too large",
            ));
        }
    }
    let payload_json = String::from_utf8(snapshot.raw_payload.clone())
        .map_err(|_| catalog_conversion("catalog payload is not valid UTF-8"))?;
    Ok(db::CreatePricingCatalogSnapshot {
        id: snapshot.id.clone(),
        source_kind: db::PricingCatalogSourceKind::ModelsDevCatalog,
        source_url: snapshot.source_url.clone(),
        http_etag: snapshot.etag.clone(),
        payload_sha256: snapshot.payload_sha256.clone(),
        parser_revision: snapshot.parser_revision.clone(),
        revision_digest: snapshot.revision_digest.clone(),
        payload_json,
        fetched_at: db_time(snapshot.fetched_at),
        created_at: db_time(snapshot.created_at),
    })
}

fn rate_revision_input(
    snapshot: &CatalogSnapshot,
    model: &CatalogModelRate,
) -> db::CreatePricingRateRevision {
    let rate_digest = model.rate_digest(&snapshot.id);
    db::CreatePricingRateRevision {
        id: rate_digest.clone(),
        source_kind: db::PricingRateSourceKind::ModelsDevCatalog,
        owner_user_id: None,
        catalog_snapshot_id: Some(snapshot.id.clone()),
        catalog_provider_id: Some(model.provider_id.clone()),
        catalog_model_id: Some(model.model_id.clone()),
        pricing_subject_revision_id: None,
        pricing_subject_revision_digest: None,
        runtime_model: None,
        source_model_key: Some(model.source_model_key.clone()),
        source_last_updated: model.source_last_updated.clone(),
        currency: "USD".to_owned(),
        rates: db_rates(model.rates),
        tiers_json: model.tiers_json(),
        legacy_context_over_200k_json: model.legacy_context_over_200k_json().map(str::to_owned),
        context_tier_state: context_tier_state(model.context_tier_state).to_owned(),
        received_rates_json: model.received_rates_json().to_owned(),
        rate_digest,
        effective_at: db_time(snapshot.fetched_at),
        created_at: db_time(snapshot.created_at),
    }
}

async fn revision_for_snapshot_in_tx(
    db: &db::SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    snapshot: &CatalogSnapshot,
    model: &CatalogModelRate,
) -> db::Result<db::PricingRateRevision> {
    db::PricingCatalogRepo::create_pricing_rate_revision_in_tx(
        db,
        transaction,
        rate_revision_input(snapshot, model),
    )
    .await
}

async fn active_revision_digest_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    snapshot_id: Option<&str>,
) -> Result<Option<String>, db::DbError> {
    match snapshot_id {
        Some(snapshot_id) => Ok(sqlx::query_scalar(
            "SELECT revision_digest FROM pricing_catalog_snapshot WHERE id = ?",
        )
        .bind(snapshot_id)
        .fetch_optional(&mut **transaction)
        .await?),
        None => Ok(None),
    }
}

async fn catalog_state_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<Option<db::PricingCatalogState>, db::DbError> {
    let Some(row) = sqlx::query(
        "SELECT id, active_snapshot_id, state, http_etag, last_checked_at,
                last_successful_check_at, stale_after, last_error_code,
                last_idempotency_key, version, created_at, updated_at
         FROM pricing_catalog_state WHERE id = 'models_dev_catalog'",
    )
    .fetch_optional(&mut **transaction)
    .await?
    else {
        return Ok(None);
    };
    let state: String = row.try_get("state")?;
    Ok(Some(db::PricingCatalogState {
        id: row.try_get("id")?,
        active_snapshot_id: row.try_get("active_snapshot_id")?,
        state: state
            .parse()
            .map_err(|error| db::DbError::Check(format!("invalid catalog state: {error}")))?,
        http_etag: row.try_get("http_etag")?,
        last_checked_at: row.try_get("last_checked_at")?,
        last_successful_check_at: row.try_get("last_successful_check_at")?,
        stale_after: row.try_get("stale_after")?,
        last_error_code: row.try_get("last_error_code")?,
        last_idempotency_key: row.try_get("last_idempotency_key")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    }))
}

async fn catalog_status_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<CatalogStatus, RetrospectiveRepositoryError> {
    let Some(state) = catalog_state_in_tx(transaction)
        .await
        .map_err(retrospective_error)?
    else {
        return Ok(CatalogStatus::absent());
    };
    let revision = active_revision_digest_in_tx(transaction, state.active_snapshot_id.as_deref())
        .await
        .map_err(retrospective_error)?;
    catalog_status_from_db(state, revision).map_err(retrospective_conversion)
}

struct CatalogCheckUpdate<'a> {
    current: &'a db::PricingCatalogState,
    state: db::PricingCatalogStateKind,
    http_etag: Option<&'a str>,
    checked_at: &'a str,
    successful_check_at: Option<&'a str>,
    stale_after: Option<&'a str>,
    last_error_code: Option<&'a str>,
    idempotency_key: &'a str,
    updated_at: &'a str,
}

async fn update_catalog_check_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    update: CatalogCheckUpdate<'_>,
) -> Result<db::PricingCatalogState, CatalogRepositoryError> {
    let result = sqlx::query(
        "UPDATE pricing_catalog_state
         SET active_snapshot_id = ?, state = ?, http_etag = ?,
             last_checked_at = ?, last_successful_check_at = ?, stale_after = ?,
             last_error_code = ?, last_idempotency_key = ?,
             version = version + 1, updated_at = ?
         WHERE id = 'models_dev_catalog' AND version = ?",
    )
    .bind(update.current.active_snapshot_id.as_deref())
    .bind(update.state.to_string())
    .bind(update.http_etag)
    .bind(update.checked_at)
    .bind(update.successful_check_at)
    .bind(update.stale_after)
    .bind(update.last_error_code)
    .bind(update.idempotency_key)
    .bind(update.updated_at)
    .bind(update.current.version)
    .execute(&mut **transaction)
    .await
    .map_err(catalog_sqlx)?;
    if result.rows_affected() == 0 {
        return Err(CatalogRepositoryError::Conflict(
            "catalog state changed while applying its operation".to_owned(),
        ));
    }
    catalog_state_in_tx(transaction)
        .await
        .map_err(catalog_error)?
        .ok_or_else(|| catalog_conversion("catalog state disappeared during its operation"))
}

pub(crate) async fn catalog_freshness_for_snapshot_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    snapshot_id: &str,
    now: SystemTime,
) -> Result<CatalogFreshness, CatalogRepositoryError> {
    validate_identifier(snapshot_id, "catalog snapshot id").map_err(catalog_conversion)?;
    let status = catalog_status_in_tx(transaction)
        .await
        .map_err(|error| catalog_conversion(error.to_string()))?;
    if status.active_snapshot_id.as_deref() == Some(snapshot_id) {
        return Ok(status.freshness_at(now));
    }
    // An explicitly configured catalog binding may intentionally remain
    // frozen to an older immutable snapshot after a refresh. It is still a
    // catalog source, so return that snapshot's own age rather than
    // `not_applicable` (which is reserved for manual/non-catalog selections).
    let Some(snapshot) = catalog_snapshot_in_tx(transaction, snapshot_id).await? else {
        return Err(catalog_conversion(
            "catalog pricing binding references a missing snapshot",
        ));
    };
    Ok(snapshot.freshness_at(now))
}

async fn catalog_snapshot_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    snapshot_id: &str,
) -> Result<Option<CatalogSnapshot>, CatalogRepositoryError> {
    let Some(row) = sqlx::query(
        "SELECT id, source_kind, source_url, http_etag, payload_sha256,
                parser_revision, revision_digest, payload_json, fetched_at, created_at
         FROM pricing_catalog_snapshot WHERE id = ?",
    )
    .bind(snapshot_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(catalog_sqlx)?
    else {
        return Ok(None);
    };
    let snapshot = db::PricingCatalogSnapshot {
        id: row.try_get("id").map_err(catalog_sqlx)?,
        source_kind: row
            .try_get::<String, _>("source_kind")
            .map_err(catalog_sqlx)?
            .parse()
            .map_err(|error| catalog_conversion(format!("invalid catalog source kind: {error}")))?,
        source_url: row.try_get("source_url").map_err(catalog_sqlx)?,
        http_etag: row.try_get("http_etag").map_err(catalog_sqlx)?,
        payload_sha256: row.try_get("payload_sha256").map_err(catalog_sqlx)?,
        parser_revision: row.try_get("parser_revision").map_err(catalog_sqlx)?,
        revision_digest: row.try_get("revision_digest").map_err(catalog_sqlx)?,
        payload_json: row.try_get("payload_json").map_err(catalog_sqlx)?,
        fetched_at: row.try_get("fetched_at").map_err(catalog_sqlx)?,
        created_at: row.try_get("created_at").map_err(catalog_sqlx)?,
    };
    snapshot_from_db(snapshot)
        .await
        .map(Some)
        .map_err(catalog_conversion)
}

fn reviewed_auto_binding_id(
    subject_id: &str,
    subject_revision_digest: &str,
    runtime_model: &str,
    provider_id: &str,
    rate_revision_id: &str,
) -> String {
    structured_digest(
        "reviewed-auto-binding-v1",
        [
            Some(subject_id.to_owned()),
            Some(subject_revision_digest.to_owned()),
            Some(runtime_model.to_owned()),
            Some(provider_id.to_owned()),
            Some(rate_revision_id.to_owned()),
        ],
    )
}

pub(crate) async fn ensure_reviewed_provider_binding_in_tx(
    db: &db::SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    subject_id: &str,
    runtime_model: &str,
    now: SystemTime,
) -> Result<bool, CatalogRepositoryError> {
    validate_identifier(subject_id, "pricing subject id").map_err(catalog_conversion)?;
    validate_identifier(runtime_model, "pricing runtime model").map_err(catalog_conversion)?;
    let Some(subject_row) = sqlx::query(
        "SELECT owner_user_id, subject_kind, provider_entry_id, daemon_id,
                executor_type, current_revision_id, state, version
         FROM pricing_subject WHERE id = ?",
    )
    .bind(subject_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(catalog_sqlx)?
    else {
        return Ok(false);
    };
    let state: String = subject_row.try_get("state").map_err(catalog_sqlx)?;
    if state != "active" {
        return Ok(false);
    }
    let subject_kind = subject_row
        .try_get::<String, _>("subject_kind")
        .map_err(catalog_sqlx)?
        .parse::<db::PricingSubjectKind>()
        .map_err(|error| catalog_conversion(format!("invalid pricing subject kind: {error}")))?;
    let provider_entry_id: Option<String> = subject_row
        .try_get("provider_entry_id")
        .map_err(catalog_sqlx)?;
    let daemon_id: Option<String> = subject_row.try_get("daemon_id").map_err(catalog_sqlx)?;
    let executor_type: Option<String> =
        subject_row.try_get("executor_type").map_err(catalog_sqlx)?;
    let revision_id: Option<String> = subject_row
        .try_get("current_revision_id")
        .map_err(catalog_sqlx)?;
    let subject_version: i64 = subject_row.try_get("version").map_err(catalog_sqlx)?;
    let Some(revision_id) = revision_id else {
        return Ok(false);
    };
    let Some(revision_row) = sqlx::query(
        "SELECT id, subject_id, owner_user_id, subject_kind, provider_entry_id,
                daemon_id, executor_type, provider_kind, credential_method,
                endpoint_class, runtime_fingerprint, schema_revision
         FROM pricing_subject_revision WHERE id = ? AND subject_id = ?",
    )
    .bind(&revision_id)
    .bind(subject_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(catalog_sqlx)?
    else {
        return Ok(false);
    };
    let identity = PricingSubjectIdentity {
        subject_kind: revision_row
            .try_get::<String, _>("subject_kind")
            .map_err(catalog_sqlx)?,
        provider_kind: revision_row
            .try_get("provider_kind")
            .map_err(catalog_sqlx)?,
        credential_method: revision_row
            .try_get("credential_method")
            .map_err(catalog_sqlx)?,
        endpoint_class: revision_row
            .try_get("endpoint_class")
            .map_err(catalog_sqlx)?,
        runtime_fingerprint: revision_row
            .try_get("runtime_fingerprint")
            .map_err(catalog_sqlx)?,
        schema_revision: revision_row
            .try_get("schema_revision")
            .map_err(catalog_sqlx)?,
    };
    let Some(alias) = pricing::reviewed_provider_alias(&identity) else {
        return Ok(false);
    };
    let stored_subject_kind = revision_row
        .try_get::<String, _>("subject_kind")
        .map_err(catalog_sqlx)?
        .parse::<db::PricingSubjectKind>()
        .map_err(|error| catalog_conversion(format!("invalid subject revision kind: {error}")))?;
    let revision_provider_entry_id: Option<String> = revision_row
        .try_get("provider_entry_id")
        .map_err(catalog_sqlx)?;
    let revision_daemon_id: Option<String> =
        revision_row.try_get("daemon_id").map_err(catalog_sqlx)?;
    let revision_executor_type: Option<String> = revision_row
        .try_get("executor_type")
        .map_err(catalog_sqlx)?;
    if stored_subject_kind != subject_kind
        || revision_provider_entry_id != provider_entry_id
        || revision_daemon_id != daemon_id
        || revision_executor_type != executor_type
    {
        return Err(catalog_conversion(
            "pricing subject revision identity does not match its subject",
        ));
    }
    let revision_digest = pricing::pricing_subject_revision_digest(&identity);
    let stored_revision_digest: String =
        sqlx::query_scalar("SELECT revision_digest FROM pricing_subject_revision WHERE id = ?")
            .bind(&revision_id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(catalog_sqlx)?;
    if stored_revision_digest != revision_digest {
        return Err(catalog_conversion(
            "pricing subject revision digest does not match its identity",
        ));
    }
    let Some(state_row) = catalog_state_in_tx(transaction)
        .await
        .map_err(catalog_error)?
    else {
        return Ok(false);
    };
    let Some(snapshot_id) = state_row.active_snapshot_id.clone() else {
        return Ok(false);
    };
    let Some(snapshot) = catalog_snapshot_in_tx(transaction, &snapshot_id).await? else {
        return Err(catalog_conversion(
            "catalog state points to a missing active snapshot",
        ));
    };
    let Some(model) = snapshot.model_rate(alias.models_dev_provider_id, runtime_model) else {
        return Ok(false);
    };
    let rate_revision_id = model.rate_digest(&snapshot.id);
    let rate_exists: Option<String> = sqlx::query_scalar(
        "SELECT id FROM pricing_rate_revision
         WHERE id = ? AND source_kind = 'models_dev_catalog'
           AND catalog_snapshot_id = ? AND catalog_provider_id = ?
           AND catalog_model_id = ? AND owner_user_id IS NULL",
    )
    .bind(&rate_revision_id)
    .bind(&snapshot.id)
    .bind(alias.models_dev_provider_id)
    .bind(runtime_model)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(catalog_sqlx)?;
    if rate_exists.is_none() {
        return Err(catalog_conversion(
            "active catalog model has no normalized rate revision",
        ));
    }
    let existing = sqlx::query(
        "SELECT id, version, rate_revision_id, catalog_provider_id, catalog_model_id
         FROM pricing_subject_binding
         WHERE subject_id = ? AND subject_revision_id = ?
           AND subject_revision_digest = ? AND runtime_model = ?
           AND source_kind = 'models_dev_catalog' AND state = 'active'",
    )
    .bind(subject_id)
    .bind(&revision_id)
    .bind(&stored_revision_digest)
    .bind(runtime_model)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(catalog_sqlx)?;
    if let Some(existing) = existing {
        let existing_id: String = existing.try_get("id").map_err(catalog_sqlx)?;
        let existing_version: i64 = existing.try_get("version").map_err(catalog_sqlx)?;
        let existing_rate: String = existing.try_get("rate_revision_id").map_err(catalog_sqlx)?;
        let existing_provider: Option<String> = existing
            .try_get("catalog_provider_id")
            .map_err(catalog_sqlx)?;
        let existing_model: Option<String> =
            existing.try_get("catalog_model_id").map_err(catalog_sqlx)?;
        if existing_rate == rate_revision_id
            && existing_provider.as_deref() == Some(alias.models_dev_provider_id)
            && existing_model.as_deref() == Some(runtime_model)
        {
            return Ok(true);
        }
        let existing_auto_id = reviewed_auto_binding_id(
            subject_id,
            &stored_revision_digest,
            runtime_model,
            existing_provider.as_deref().unwrap_or_default(),
            &existing_rate,
        );
        if existing_id != existing_auto_id {
            // An explicit catalog binding is user intent and remains frozen
            // to its immutable snapshot. Only deterministic automatic rows
            // are advanced when the active catalog changes.
            return Ok(true);
        }
        db::PricingSubjectRepo::retire_pricing_subject_binding_in_tx(
            db,
            transaction,
            db::RetirePricingSubjectBinding {
                id: existing_id,
                expected_version: existing_version,
                retired_at: db_time(now),
                updated_at: db_time(now),
            },
        )
        .await
        .map_err(catalog_error)?;
    }
    let freshness = catalog_status_from_db(state_row, Some(snapshot.revision_digest.clone()))
        .map_err(catalog_conversion)?
        .freshness_for_snapshot(&snapshot, now);
    let source = PricingBindingSource::ModelsDev(pricing::CatalogRateBinding {
        id: String::new(),
        provider_id: alias.models_dev_provider_id.to_owned(),
        model_id: runtime_model.to_owned(),
        rate_revision_id: rate_revision_id.clone(),
        snapshot_id: snapshot.id.clone(),
        runtime_model: runtime_model.to_owned(),
        rates: model.rates,
        tiers: model.tiers.clone(),
        legacy_context_over_200k: model.legacy_context_over_200k.clone(),
        catalog_freshness: freshness,
        state: PricingBindingState::Active,
    });
    let binding_id = reviewed_auto_binding_id(
        subject_id,
        &stored_revision_digest,
        runtime_model,
        alias.models_dev_provider_id,
        &rate_revision_id,
    );
    let binding_digest = binding_digest(subject_id, &stored_revision_digest, &source);
    let owner_user_id: String = subject_row.try_get("owner_user_id").map_err(catalog_sqlx)?;
    db::PricingSubjectRepo::create_pricing_subject_binding_in_tx(
        db,
        transaction,
        db::CreatePricingSubjectBinding {
            id: binding_id,
            owner_user_id,
            subject_id: subject_id.to_owned(),
            subject_revision_id: revision_id,
            subject_revision_digest: stored_revision_digest,
            runtime_model: runtime_model.to_owned(),
            source_kind: db::PricingRateSourceKind::ModelsDevCatalog,
            catalog_provider_id: Some(alias.models_dev_provider_id.to_owned()),
            catalog_model_id: Some(runtime_model.to_owned()),
            rate_revision_id,
            binding_digest,
            effective_at: db_time(now),
            created_at: db_time(now),
            updated_at: db_time(now),
        },
    )
    .await
    .map_err(catalog_error)?;
    // Automatic reconciliation changes the active binding set even though it
    // is not a user PUT. Advance the subject CAS version so a configuration
    // writer that loaded the previous automatic row cannot return or persist
    // a stale source after this transaction commits.
    db::PricingSubjectRepo::update_pricing_subject_in_tx(
        db,
        transaction,
        db::UpdatePricingSubject {
            id: subject_id.to_owned(),
            expected_version: subject_version,
            current_revision_id: None,
            state: None,
            last_idempotency_key: None,
            last_update_digest: None,
            updated_at: db_time(now),
        },
    )
    .await
    .map_err(catalog_error)?;
    Ok(true)
}

async fn ensure_catalog_state_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    now: &str,
) -> Result<db::PricingCatalogState, db::DbError> {
    sqlx::query(
        "INSERT OR IGNORE INTO pricing_catalog_state
            (id, state, version, created_at, updated_at)
         VALUES ('models_dev_catalog', 'absent', 1, ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    catalog_state_in_tx(transaction)
        .await?
        .ok_or(db::DbError::NotFound)
}

async fn status_for_state(
    db: &db::SqliteDb,
    state: db::PricingCatalogState,
) -> Result<CatalogStatus, CatalogRepositoryError> {
    let revision = match state.active_snapshot_id.as_deref() {
        Some(id) => db::PricingCatalogRepo::get_pricing_catalog_snapshot(db, id)
            .await
            .map_err(catalog_error)?
            .map(|snapshot| snapshot.revision_digest),
        None => None,
    };
    catalog_status_from_db(state, revision).map_err(catalog_conversion)
}

async fn snapshot_from_db(snapshot: db::PricingCatalogSnapshot) -> Result<CatalogSnapshot, String> {
    if snapshot.source_url != pricing::MODELS_DEV_ENDPOINT {
        return Err("stored catalog source URL is not the fixed models.dev endpoint".to_owned());
    }
    let raw_payload = snapshot.payload_json.into_bytes();
    if raw_payload.len() > pricing::MODELS_DEV_MAX_RESPONSE_BYTES {
        return Err("stored catalog payload exceeds the configured size bound".to_owned());
    }
    if pricing::sha256_hex(&raw_payload) != snapshot.payload_sha256 {
        return Err("stored catalog payload SHA-256 does not match its bytes".to_owned());
    }
    let parsed = pricing::parse_models_dev_catalog(&raw_payload)
        .map_err(|error| format!("stored catalog payload cannot be reparsed: {error}"))?;
    if parsed.payload_sha256 != snapshot.payload_sha256
        || parsed.parser_revision != snapshot.parser_revision
        || pricing::catalog_revision_digest(&snapshot.payload_sha256, &snapshot.parser_revision)
            != snapshot.revision_digest
    {
        return Err("stored catalog revision metadata does not match its payload".to_owned());
    }
    Ok(CatalogSnapshot {
        id: snapshot.id,
        source_url: snapshot.source_url,
        etag: snapshot.http_etag,
        payload_sha256: snapshot.payload_sha256,
        parser_revision: snapshot.parser_revision,
        revision_digest: snapshot.revision_digest,
        raw_payload,
        models: parsed.models,
        fetched_at: system_time(&snapshot.fetched_at)?,
        created_at: system_time(&snapshot.created_at)?,
    })
}

async fn catalog_model_for_revision(
    db: &db::SqliteDb,
    revision: &db::PricingRateRevision,
) -> Result<(CatalogModelRate, String), String> {
    let snapshot_id = revision
        .catalog_snapshot_id
        .as_deref()
        .ok_or_else(|| "catalog rate revision has no snapshot".to_owned())?;
    let provider_id = revision
        .catalog_provider_id
        .as_deref()
        .ok_or_else(|| "catalog rate revision has no provider".to_owned())?;
    let model_id = revision
        .catalog_model_id
        .as_deref()
        .ok_or_else(|| "catalog rate revision has no model".to_owned())?;
    let snapshot = db::PricingCatalogRepo::get_pricing_catalog_snapshot(db, snapshot_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "catalog rate revision references a missing snapshot".to_owned())?;
    let snapshot = snapshot_from_db(snapshot).await?;
    let model = snapshot
        .model_rate(provider_id, model_id)
        .cloned()
        .ok_or_else(|| "catalog rate revision references a missing model".to_owned())?;
    let rates = service_rates(revision.rates)?;
    validate_event_bucket_rates(rates, "stored catalog rates")?;
    if revision.id != model.rate_digest(&snapshot.id)
        || revision.source_model_key.as_deref() != Some(model.source_model_key.as_str())
        || revision.source_last_updated.as_deref() != model.source_last_updated.as_deref()
        || revision.received_rates_json != model.received_rates_json()
        || revision.currency != "USD"
    {
        return Err("catalog rate revision provenance differs from its snapshot".to_owned());
    }
    if rates != model.rates {
        return Err("catalog rate revision differs from its immutable source row".to_owned());
    }
    if revision.tiers_json != model.tiers_json()
        || revision.legacy_context_over_200k_json.as_deref()
            != model.legacy_context_over_200k_json()
        || service_context_tier_state(&revision.context_tier_state)? != model.context_tier_state
    {
        return Err(
            "catalog rate revision normalized metadata differs from its snapshot".to_owned(),
        );
    }
    Ok((model, snapshot.id))
}

#[async_trait]
impl PricingCatalogRepository for SqlitePricingRepository {
    async fn catalog_status(&self) -> Result<CatalogStatus, CatalogRepositoryError> {
        let Some(state) = db::PricingCatalogRepo::get_pricing_catalog_state(&*self.db)
            .await
            .map_err(catalog_error)?
        else {
            return Ok(CatalogStatus::absent());
        };
        status_for_state(&self.db, state).await
    }

    async fn replay_catalog_refresh(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<CatalogRefreshOutcome>, CatalogRepositoryError> {
        validate_identifier(idempotency_key, "catalog idempotency key")
            .map_err(catalog_conversion)?;
        let Some(row) = sqlx::query(
            "SELECT request_digest, result_json
             FROM pricing_operation_receipt
             WHERE operation_scope = ? AND idempotency_key = ?",
        )
        .bind(CATALOG_REFRESH_OPERATION_SCOPE)
        .bind(idempotency_key)
        .fetch_optional(self.db.pool())
        .await
        .map_err(catalog_sqlx)?
        else {
            return Ok(None);
        };
        let request_digest: String = row.try_get("request_digest").map_err(catalog_sqlx)?;
        if request_digest != catalog_refresh_request_digest() {
            return Err(catalog_conversion(
                "catalog refresh receipt has an incompatible request digest",
            ));
        }
        let result_json: String = row.try_get("result_json").map_err(catalog_sqlx)?;
        let status = catalog_status_from_result_json(&result_json).map_err(catalog_conversion)?;
        let value: Value = serde_json::from_str(&result_json).map_err(|error| {
            catalog_conversion(format!("invalid catalog receipt JSON: {error}"))
        })?;
        let outcome = value
            .get("outcome")
            .and_then(Value::as_str)
            .unwrap_or("already_refreshed");
        match outcome {
            "activated" => {
                let snapshot_id = value
                    .get("snapshot_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| catalog_conversion("activated receipt omits snapshot_id"))?;
                let snapshot =
                    db::PricingCatalogRepo::get_pricing_catalog_snapshot(&*self.db, snapshot_id)
                        .await
                        .map_err(catalog_error)?
                        .ok_or_else(|| {
                            catalog_conversion(
                                "activated receipt points to a missing catalog snapshot",
                            )
                        })?;
                let snapshot = snapshot_from_db(snapshot)
                    .await
                    .map_err(catalog_conversion)?;
                Ok(Some(CatalogRefreshOutcome::Activated {
                    snapshot: Box::new(snapshot),
                    status,
                }))
            }
            "not_modified" => Ok(Some(CatalogRefreshOutcome::NotModified { status })),
            "failed" => {
                let error_code = value
                    .get("error_code")
                    .and_then(Value::as_str)
                    .ok_or_else(|| catalog_conversion("failed receipt omits error_code"))?;
                let code = catalog_refresh_error_code(error_code).map_err(catalog_conversion)?;
                Ok(Some(CatalogRefreshOutcome::Failed { status, code }))
            }
            "already_refreshed" => Ok(Some(CatalogRefreshOutcome::AlreadyRefreshed { status })),
            other => Err(catalog_conversion(format!(
                "unknown catalog refresh receipt outcome {other:?}"
            ))),
        }
    }

    async fn record_catalog_refresh_already_refreshed(
        &self,
        checked_at: SystemTime,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        validate_identifier(idempotency_key, "catalog idempotency key")
            .map_err(catalog_conversion)?;
        let checked_at = db_time(checked_at);
        let request_digest = catalog_refresh_request_digest();
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(catalog_sqlx)?;
        if let Some(result_json) = operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
        )
        .await
        .map_err(catalog_operation_error)?
        {
            let status =
                catalog_status_from_result_json(&result_json).map_err(catalog_conversion)?;
            transaction.commit().await.map_err(catalog_sqlx)?;
            return Ok(status);
        }
        let state = ensure_catalog_state_in_tx(&mut transaction, &checked_at)
            .await
            .map_err(catalog_error)?;
        let revision =
            active_revision_digest_in_tx(&mut transaction, state.active_snapshot_id.as_deref())
                .await
                .map_err(catalog_error)?;
        let status = catalog_status_from_db(state, revision)
            .map_err(catalog_conversion)?
            .at(system_time(&checked_at).map_err(catalog_conversion)?);
        let result_json = catalog_status_result_json(&status, "already_refreshed", None, None);
        insert_operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
            &result_json,
            &checked_at,
        )
        .await
        .map_err(catalog_conversion)?;
        transaction.commit().await.map_err(catalog_sqlx)?;
        Ok(status)
    }

    async fn active_catalog_snapshot(
        &self,
    ) -> Result<Option<CatalogSnapshot>, CatalogRepositoryError> {
        let Some(state) = db::PricingCatalogRepo::get_pricing_catalog_state(&*self.db)
            .await
            .map_err(catalog_error)?
        else {
            return Ok(None);
        };
        let Some(snapshot_id) = state.active_snapshot_id else {
            return Ok(None);
        };
        let snapshot =
            db::PricingCatalogRepo::get_pricing_catalog_snapshot(&*self.db, &snapshot_id)
                .await
                .map_err(catalog_error)?
                .ok_or_else(|| catalog_conversion("catalog state points to a missing snapshot"))?;
        snapshot_from_db(snapshot)
            .await
            .map(Some)
            .map_err(catalog_conversion)
    }

    async fn activate_catalog_snapshot(
        &self,
        snapshot: CatalogSnapshot,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        if idempotency_key.trim().is_empty() || idempotency_key.len() > 256 {
            return Err(catalog_conversion(
                "catalog idempotency key is empty or too long",
            ));
        }
        if snapshot.models.is_empty() {
            return Err(catalog_conversion(
                "catalog snapshot has no normalized models",
            ));
        }
        let input = snapshot_input(&snapshot)?;
        let request_digest = catalog_refresh_request_digest();
        let checked_at = input.fetched_at.clone();
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(catalog_sqlx)?;
        if let Some(result_json) = operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
        )
        .await
        .map_err(catalog_operation_error)?
        {
            let status =
                catalog_status_from_result_json(&result_json).map_err(catalog_conversion)?;
            transaction.commit().await.map_err(catalog_sqlx)?;
            return Ok(status);
        }
        let current = ensure_catalog_state_in_tx(&mut transaction, &checked_at)
            .await
            .map_err(catalog_error)?;

        // `revision_digest` is the immutable source identity.  SQLite keeps
        // it unique, so a retry or a parser-normalized replay may arrive with
        // a fresh application snapshot ID.  Reuse the retained row's ID and
        // rate foreign keys instead of attempting to create a second logical
        // snapshot (or leaving an orphan row when activation later fails).
        let existing_snapshot = sqlx::query(
            "SELECT id, source_url, payload_sha256, parser_revision,
                    revision_digest, payload_json
             FROM pricing_catalog_snapshot WHERE revision_digest = ?",
        )
        .bind(&snapshot.revision_digest)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(catalog_sqlx)?;
        let has_existing_snapshot = existing_snapshot.is_some();
        let activation_snapshot_id = if let Some(row) = existing_snapshot {
            let existing_source_url: String = row.try_get("source_url").map_err(catalog_sqlx)?;
            let existing_payload_sha256: String =
                row.try_get("payload_sha256").map_err(catalog_sqlx)?;
            let existing_parser_revision: String =
                row.try_get("parser_revision").map_err(catalog_sqlx)?;
            let existing_revision_digest: String =
                row.try_get("revision_digest").map_err(catalog_sqlx)?;
            let existing_payload_json: String =
                row.try_get("payload_json").map_err(catalog_sqlx)?;
            if existing_source_url != snapshot.source_url
                || existing_payload_sha256 != snapshot.payload_sha256
                || existing_parser_revision != snapshot.parser_revision
                || existing_revision_digest != snapshot.revision_digest
                || existing_payload_json.as_bytes() != snapshot.raw_payload.as_slice()
            {
                return Err(catalog_conversion(
                    "catalog revision digest collides with different immutable payload metadata",
                ));
            }
            row.try_get::<String, _>("id").map_err(catalog_sqlx)?
        } else {
            snapshot.id.clone()
        };
        let mut persisted_snapshot = snapshot.clone();
        persisted_snapshot.id = activation_snapshot_id.clone();

        if !has_existing_snapshot {
            db::PricingCatalogRepo::create_pricing_catalog_snapshot_in_tx(
                &*self.db,
                &mut transaction,
                input,
            )
            .await
            .map_err(catalog_error)?;
        }
        for model in &persisted_snapshot.models {
            revision_for_snapshot_in_tx(&self.db, &mut transaction, &persisted_snapshot, model)
                .await
                .map_err(catalog_error)?;
        }
        let state = db::PricingCatalogRepo::activate_pricing_catalog_in_tx(
            &*self.db,
            &mut transaction,
            db::ActivatePricingCatalog {
                snapshot_id: activation_snapshot_id,
                expected_version: current.version,
                http_etag: snapshot.etag.clone(),
                checked_at: checked_at.clone(),
                stale_after: Some(
                    DateTime::parse_from_rfc3339(&checked_at)
                        .map_err(|error| catalog_conversion(error.to_string()))?
                        .with_timezone(&Utc)
                        .checked_add_signed(
                            chrono::Duration::from_std(pricing::MODELS_DEV_STALE_AFTER)
                                .map_err(|error| catalog_conversion(error.to_string()))?,
                        )
                        .ok_or_else(|| catalog_conversion("catalog stale boundary overflow"))?
                        .to_rfc3339_opts(SecondsFormat::Nanos, true),
                ),
                idempotency_key: Some(idempotency_key.to_owned()),
                updated_at: checked_at.clone(),
            },
        )
        .await
        .map_err(catalog_error)?;
        let status = catalog_status_from_db(state, Some(snapshot.revision_digest))
            .map_err(catalog_conversion)?;
        let result_json = catalog_status_result_json(
            &status,
            "activated",
            status.active_snapshot_id.as_deref(),
            None,
        );
        insert_operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
            &result_json,
            &checked_at,
        )
        .await
        .map_err(catalog_conversion)?;
        transaction.commit().await.map_err(catalog_sqlx)?;
        Ok(status)
    }

    async fn record_catalog_not_modified(
        &self,
        checked_at: SystemTime,
        etag: Option<String>,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        if idempotency_key.trim().is_empty() || idempotency_key.len() > 256 {
            return Err(catalog_conversion(
                "catalog idempotency key is empty or too long",
            ));
        }
        validate_optional_text(etag.as_deref(), "catalog ETag").map_err(catalog_conversion)?;
        let checked_at = db_time(checked_at);
        let request_digest = catalog_refresh_request_digest();
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(catalog_sqlx)?;
        if let Some(result_json) = operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
        )
        .await
        .map_err(catalog_operation_error)?
        {
            let status =
                catalog_status_from_result_json(&result_json).map_err(catalog_conversion)?;
            transaction.commit().await.map_err(catalog_sqlx)?;
            return Ok(status);
        }
        let current = ensure_catalog_state_in_tx(&mut transaction, &checked_at)
            .await
            .map_err(catalog_error)?;
        if current.active_snapshot_id.is_none() {
            return Err(catalog_conversion(
                "cannot record a 304 response without a retained catalog snapshot",
            ));
        }
        let stale_after = DateTime::parse_from_rfc3339(&checked_at)
            .map_err(|error| catalog_conversion(error.to_string()))?
            .with_timezone(&Utc)
            .checked_add_signed(
                chrono::Duration::from_std(pricing::MODELS_DEV_STALE_AFTER)
                    .map_err(|error| catalog_conversion(error.to_string()))?,
            )
            .ok_or_else(|| catalog_conversion("catalog stale boundary overflow"))?
            .to_rfc3339_opts(SecondsFormat::Nanos, true);
        let state = update_catalog_check_in_tx(
            &mut transaction,
            CatalogCheckUpdate {
                current: &current,
                state: db::PricingCatalogStateKind::Fresh,
                http_etag: etag.as_deref().or(current.http_etag.as_deref()),
                checked_at: &checked_at,
                successful_check_at: Some(&checked_at),
                stale_after: Some(&stale_after),
                last_error_code: None,
                idempotency_key,
                updated_at: &checked_at,
            },
        )
        .await?;
        let revision =
            active_revision_digest_in_tx(&mut transaction, state.active_snapshot_id.as_deref())
                .await
                .map_err(catalog_error)?;
        let status = catalog_status_from_db(state, revision).map_err(catalog_conversion)?;
        let result_json = catalog_status_result_json(&status, "not_modified", None, None);
        insert_operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
            &result_json,
            &checked_at,
        )
        .await
        .map_err(catalog_conversion)?;
        transaction.commit().await.map_err(catalog_sqlx)?;
        Ok(status)
    }

    async fn record_catalog_refresh_failure(
        &self,
        checked_at: SystemTime,
        code: CatalogRefreshErrorCode,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        if idempotency_key.trim().is_empty() || idempotency_key.len() > 256 {
            return Err(catalog_conversion(
                "catalog idempotency key is empty or too long",
            ));
        }
        let checked_at = db_time(checked_at);
        let request_digest = catalog_refresh_request_digest();
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(catalog_sqlx)?;
        if let Some(result_json) = operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
        )
        .await
        .map_err(catalog_operation_error)?
        {
            let status =
                catalog_status_from_result_json(&result_json).map_err(catalog_conversion)?;
            transaction.commit().await.map_err(catalog_sqlx)?;
            return Ok(status);
        }
        let current = ensure_catalog_state_in_tx(&mut transaction, &checked_at)
            .await
            .map_err(catalog_error)?;
        let state = update_catalog_check_in_tx(
            &mut transaction,
            CatalogCheckUpdate {
                current: &current,
                state: db::PricingCatalogStateKind::RefreshFailed,
                http_etag: current.http_etag.as_deref(),
                checked_at: &checked_at,
                successful_check_at: current.last_successful_check_at.as_deref(),
                stale_after: current.stale_after.as_deref(),
                last_error_code: Some(code.as_str()),
                idempotency_key,
                updated_at: &checked_at,
            },
        )
        .await?;
        let revision =
            active_revision_digest_in_tx(&mut transaction, state.active_snapshot_id.as_deref())
                .await
                .map_err(catalog_error)?;
        let status = catalog_status_from_db(state, revision).map_err(catalog_conversion)?;
        let result_json = catalog_status_result_json(&status, "failed", None, Some(code.as_str()));
        insert_operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
            &result_json,
            &checked_at,
        )
        .await
        .map_err(catalog_conversion)?;
        transaction.commit().await.map_err(catalog_sqlx)?;
        Ok(status)
    }
}

struct LoadedConfiguration {
    subject: db::PricingSubject,
    revision: db::PricingSubjectRevision,
    bindings: Vec<db::PricingSubjectBinding>,
    configuration: PricingConfiguration,
}

impl SqlitePricingRepository {
    async fn binding_source(
        &self,
        binding: &db::PricingSubjectBinding,
        revision: &db::PricingSubjectRevision,
        now: SystemTime,
    ) -> Result<PricingBindingSource, CatalogRepositoryError> {
        let rate =
            db::PricingCatalogRepo::get_pricing_rate_revision(&*self.db, &binding.rate_revision_id)
                .await
                .map_err(catalog_error)?
                .ok_or_else(|| {
                    catalog_conversion("pricing binding points to a missing rate revision")
                })?;
        if rate.source_kind != binding.source_kind {
            return Err(catalog_conversion(
                "pricing binding source differs from its rate revision",
            ));
        }
        let state = binding_state(binding.state);
        match binding.source_kind {
            db::PricingRateSourceKind::ManualOverride => {
                if rate.pricing_subject_revision_id.as_deref() != Some(revision.id.as_str())
                    || rate.pricing_subject_revision_digest.as_deref()
                        != Some(revision.revision_digest.as_str())
                    || rate.runtime_model.as_deref() != Some(binding.runtime_model.as_str())
                {
                    return Err(catalog_conversion(
                        "manual pricing binding points to a different subject revision",
                    ));
                }
                Ok(PricingBindingSource::Manual(ManualRateOverride {
                    id: binding.id.clone(),
                    subject_revision_digest: binding.subject_revision_digest.clone(),
                    runtime_model: binding.runtime_model.clone(),
                    rate_revision_id: rate.id,
                    rates: {
                        let rates = service_rates(rate.rates).map_err(catalog_conversion)?;
                        validate_event_bucket_rates(rates, "stored manual rates")
                            .map_err(catalog_conversion)?;
                        rates
                    },
                    effective_at: system_time(&binding.effective_at).map_err(catalog_conversion)?,
                    retired_at: optional_system_time(binding.retired_at.as_deref())
                        .map_err(catalog_conversion)?,
                }))
            }
            db::PricingRateSourceKind::ModelsDevCatalog => {
                let provider_id = binding.catalog_provider_id.as_deref().ok_or_else(|| {
                    catalog_conversion("catalog pricing binding has no provider identifier")
                })?;
                let model_id = binding.catalog_model_id.as_deref().ok_or_else(|| {
                    catalog_conversion("catalog pricing binding has no model identifier")
                })?;
                let (model, snapshot_id) = catalog_model_for_revision(&self.db, &rate)
                    .await
                    .map_err(catalog_conversion)?;
                if provider_id != model.provider_id
                    || model_id != model.model_id
                    || snapshot_id != rate.catalog_snapshot_id.as_deref().unwrap_or_default()
                {
                    return Err(catalog_conversion(
                        "catalog pricing binding differs from its immutable rate revision",
                    ));
                }
                let snapshot =
                    db::PricingCatalogRepo::get_pricing_catalog_snapshot(&*self.db, &snapshot_id)
                        .await
                        .map_err(catalog_error)?
                        .ok_or_else(|| {
                            catalog_conversion(
                                "catalog pricing binding references a missing snapshot",
                            )
                        })?;
                let snapshot = snapshot_from_db(snapshot)
                    .await
                    .map_err(catalog_conversion)?;
                let catalog_freshness = self
                    .catalog_status()
                    .await?
                    .freshness_for_snapshot(&snapshot, now);
                Ok(PricingBindingSource::ModelsDev(
                    pricing::CatalogRateBinding {
                        id: binding.id.clone(),
                        provider_id: provider_id.to_owned(),
                        model_id: model_id.to_owned(),
                        rate_revision_id: rate.id,
                        snapshot_id,
                        runtime_model: binding.runtime_model.clone(),
                        rates: {
                            let rates = service_rates(rate.rates).map_err(catalog_conversion)?;
                            validate_event_bucket_rates(rates, "stored catalog rates")
                                .map_err(catalog_conversion)?;
                            rates
                        },
                        tiers: model.tiers,
                        legacy_context_over_200k: model.legacy_context_over_200k,
                        catalog_freshness,
                        state,
                    },
                ))
            }
        }
    }

    async fn load_configuration(
        &self,
        subject_id: &str,
    ) -> Result<LoadedConfiguration, CatalogRepositoryError> {
        let subject = db::PricingSubjectRepo::get_pricing_subject(&*self.db, subject_id)
            .await
            .map_err(catalog_error)?
            .ok_or_else(|| catalog_conversion("pricing subject was not found"))?;
        let revision_id = subject
            .current_revision_id
            .as_deref()
            .ok_or_else(|| catalog_conversion("pricing subject has no current revision"))?;
        let revision = db::PricingSubjectRepo::get_pricing_subject_revision(&*self.db, revision_id)
            .await
            .map_err(catalog_error)?
            .ok_or_else(|| catalog_conversion("pricing subject current revision was not found"))?;
        if revision.subject_id != subject.id {
            return Err(catalog_conversion(
                "pricing subject revision targets another subject",
            ));
        }
        let bindings =
            db::PricingSubjectRepo::list_pricing_subject_bindings(&*self.db, subject_id, true)
                .await
                .map_err(catalog_error)?;
        let now = SystemTime::now();
        let mut sources = Vec::new();
        for binding in &bindings {
            // Older immutable subject revisions remain readable for history,
            // but the mutable configuration only resolves the current one.
            if binding.subject_revision_id != revision.id
                || binding.subject_revision_digest != revision.revision_digest
            {
                continue;
            }
            sources.push(self.binding_source(binding, &revision, now).await?);
        }
        let version = subject
            .version
            .try_into()
            .map_err(|_| catalog_conversion("pricing subject version is negative"))?;
        let last_idempotency_key = subject.last_idempotency_key.clone();
        let last_update_digest = subject.last_update_digest.clone();
        let subject_revision_digest = revision.revision_digest.clone();
        Ok(LoadedConfiguration {
            subject,
            revision: revision.clone(),
            bindings,
            configuration: PricingConfiguration {
                subject_id: subject_id.to_owned(),
                subject_revision_digest,
                version,
                bindings: sources,
                last_idempotency_key,
                last_update_digest,
            },
        })
    }
}

fn active_binding(source: &PricingBindingSource) -> bool {
    match source {
        PricingBindingSource::ModelsDev(binding) => binding.state == PricingBindingState::Active,
        PricingBindingSource::Manual(override_) => override_.retired_at.is_none(),
    }
}

fn source_runtime(source: &PricingBindingSource) -> &str {
    match source {
        PricingBindingSource::ModelsDev(binding) => &binding.runtime_model,
        PricingBindingSource::Manual(override_) => &override_.runtime_model,
    }
}

fn validate_binding_source(source: &PricingBindingSource) -> Result<(), CatalogRepositoryError> {
    validate_identifier(source.runtime_model(), "pricing runtime model")
        .map_err(catalog_conversion)?;
    match source {
        PricingBindingSource::ModelsDev(binding) => {
            validate_identifier(&binding.id, "pricing catalog binding id")
                .map_err(catalog_conversion)?;
            validate_identifier(&binding.provider_id, "pricing catalog provider id")
                .map_err(catalog_conversion)?;
            validate_identifier(&binding.model_id, "pricing catalog model id")
                .map_err(catalog_conversion)?;
            validate_identifier(
                &binding.rate_revision_id,
                "pricing catalog rate revision id",
            )
            .map_err(catalog_conversion)?;
            validate_identifier(&binding.snapshot_id, "pricing catalog snapshot id")
                .map_err(catalog_conversion)?;
            validate_event_bucket_rates(binding.rates, "pricing catalog rates")
                .map_err(catalog_conversion)?;
            for tier in &binding.tiers {
                validate_event_bucket_rates(tier.rates, "pricing catalog tier rates")
                    .map_err(catalog_conversion)?;
            }
            if let Some(legacy) = &binding.legacy_context_over_200k {
                validate_event_bucket_rates(legacy.rates, "pricing catalog legacy rates")
                    .map_err(catalog_conversion)?;
            }
        }
        PricingBindingSource::Manual(override_) => {
            validate_identifier(&override_.id, "manual binding id").map_err(catalog_conversion)?;
            validate_identifier(
                &override_.subject_revision_digest,
                "manual subject revision digest",
            )
            .map_err(catalog_conversion)?;
            validate_identifier(&override_.rate_revision_id, "manual rate revision id")
                .map_err(catalog_conversion)?;
            validate_event_bucket_rates(override_.rates, "manual rates")
                .map_err(catalog_conversion)?;
        }
    }
    Ok(())
}

#[async_trait]
impl PricingBindingRepository for SqlitePricingRepository {
    async fn pricing_configuration(
        &self,
        subject_id: &str,
    ) -> Result<PricingConfiguration, CatalogRepositoryError> {
        Ok(self.load_configuration(subject_id).await?.configuration)
    }

    async fn replace_pricing_configuration(
        &self,
        subject_id: &str,
        request: ReplacePricingRequest,
        now: SystemTime,
    ) -> Result<PricingConfiguration, CatalogRepositoryError> {
        validate_replace_request_bounds(subject_id, &request)?;
        if request.idempotency_key.trim().is_empty() || request.idempotency_key.len() > 256 {
            return Err(catalog_conversion(
                "pricing binding idempotency key is empty or too long",
            ));
        }
        let operation_scope = format!("pricing_configuration:{subject_id}");
        let request_digest = binding_request_digest(subject_id, &request);

        // Check immutable operation history before loading mutable subject
        // state. This preserves A,B,A replay semantics after B advances the
        // subject version and last-key pointer.
        let mut receipt_transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(catalog_sqlx)?;
        let Some(subject_state) =
            sqlx::query_scalar::<_, String>("SELECT state FROM pricing_subject WHERE id = ?")
                .bind(subject_id)
                .fetch_optional(&mut *receipt_transaction)
                .await
                .map_err(catalog_sqlx)?
        else {
            return Err(catalog_conversion("pricing subject was not found"));
        };
        if subject_state != "active" {
            return Err(CatalogRepositoryError::Conflict(
                "pricing subject is retired and cannot be configured".to_owned(),
            ));
        }
        if let Some(result_json) = operation_receipt_in_tx(
            &mut receipt_transaction,
            &operation_scope,
            &request.idempotency_key,
            &request_digest,
        )
        .await
        .map_err(catalog_operation_error)?
        {
            let configuration =
                configuration_from_result_json(&result_json).map_err(catalog_conversion)?;
            receipt_transaction.commit().await.map_err(catalog_sqlx)?;
            return Ok(configuration);
        }
        receipt_transaction.commit().await.map_err(catalog_sqlx)?;

        let loaded = self.load_configuration(subject_id).await?;
        let mut proposed = loaded.configuration.clone();
        proposed
            .replace(request.clone(), now)
            .map_err(|error| CatalogRepositoryError::Conflict(error.to_string()))?;

        // A manual override shadows (rather than deletes) the catalog source
        // for the same runtime.  The pure configuration operation treats a
        // replacement as a complete source set, so retain the prior active
        // catalog row as a fallback whenever this request publishes a manual
        // row for that runtime.  The catalog row remains immutable and can be
        // retired explicitly by a catalog-only/empty replacement.
        for desired in &request.bindings {
            if !matches!(
                &desired.source,
                pricing::DesiredPricingSource::Manual { .. }
            ) {
                continue;
            }
            let runtime_model = desired.runtime_model.as_str();
            let has_active_catalog = proposed.bindings.iter().any(|source| {
                matches!(source, PricingBindingSource::ModelsDev(binding)
                    if binding.state == PricingBindingState::Active
                        && binding.runtime_model == runtime_model)
            });
            if has_active_catalog {
                continue;
            }
            if let Some(PricingBindingSource::ModelsDev(existing)) =
                loaded.configuration.bindings.iter().find(|source| {
                    matches!(source, PricingBindingSource::ModelsDev(binding)
                        if binding.state == PricingBindingState::Active
                            && binding.runtime_model == runtime_model)
                })
            {
                // replace retires the catalog source when only the manual
                // source is desired. Restore the same immutable row as the
                // fallback instead of appending a duplicate source ID.
                if let Some(PricingBindingSource::ModelsDev(current)) =
                    proposed.bindings.iter_mut().find(|source| {
                        matches!(source, PricingBindingSource::ModelsDev(binding)
                            if binding.id == existing.id)
                    })
                {
                    current.state = PricingBindingState::Active;
                } else {
                    proposed
                        .bindings
                        .push(PricingBindingSource::ModelsDev(existing.clone()));
                }
            }
        }

        // A new key for the same desired set still records its idempotency
        // identity, but does not manufacture a new immutable rate/binding row.
        // Restoring a catalog fallback after a manual replacement can undo
        // the pure-domain retirement marker. Compare the final immutable
        // source rows, not that intermediate marker, so replaying an
        // unchanged desired set does not manufacture a synthetic mutation.
        if proposed.bindings == loaded.configuration.bindings {
            let updated_at = db_time(now);
            let mut transaction = db::begin_immediate(self.db.pool())
                .await
                .map_err(catalog_sqlx)?;
            if let Some(result_json) = operation_receipt_in_tx(
                &mut transaction,
                &operation_scope,
                &request.idempotency_key,
                &request_digest,
            )
            .await
            .map_err(catalog_operation_error)?
            {
                let configuration =
                    configuration_from_result_json(&result_json).map_err(catalog_conversion)?;
                transaction.commit().await.map_err(catalog_sqlx)?;
                return Ok(configuration);
            }
            let updated_subject = db::PricingSubjectRepo::update_pricing_subject_in_tx(
                &*self.db,
                &mut transaction,
                db::UpdatePricingSubject {
                    id: subject_id.to_owned(),
                    expected_version: loaded.subject.version,
                    current_revision_id: None,
                    state: None,
                    last_idempotency_key: Some(proposed.last_idempotency_key.clone()),
                    last_update_digest: Some(proposed.last_update_digest.clone()),
                    updated_at: updated_at.clone(),
                },
            )
            .await
            .map_err(catalog_error)?;
            proposed.version = u64::try_from(updated_subject.version)
                .map_err(|_| catalog_conversion("pricing subject version is negative"))?;
            let result_json = configuration_result_json(&proposed);
            insert_operation_receipt_in_tx(
                &mut transaction,
                &operation_scope,
                &request.idempotency_key,
                &request_digest,
                &result_json,
                &updated_at,
            )
            .await
            .map_err(catalog_conversion)?;
            transaction.commit().await.map_err(catalog_sqlx)?;
            return Ok(proposed);
        }

        let desired_active = proposed
            .bindings
            .iter()
            .filter(|source| active_binding(source))
            .collect::<Vec<_>>();
        validate_identifier(subject_id, "pricing subject id").map_err(catalog_conversion)?;
        for source in &desired_active {
            validate_binding_source(source)?;
        }
        // Validate every immutable catalog reference before opening the write
        // transaction.  The referenced rows are immutable, and this catches a
        // mismatched snapshot/rate/provider tuple without relying on a trigger
        // error whose message is less useful at the service boundary.
        for source in &desired_active {
            let PricingBindingSource::ModelsDev(binding) = source else {
                continue;
            };
            let rate = db::PricingCatalogRepo::get_pricing_rate_revision(
                &*self.db,
                &binding.rate_revision_id,
            )
            .await
            .map_err(catalog_error)?
            .ok_or_else(|| {
                catalog_conversion("catalog binding references a missing rate revision")
            })?;
            if rate.source_kind != db::PricingRateSourceKind::ModelsDevCatalog
                || rate.catalog_snapshot_id.as_deref() != Some(binding.snapshot_id.as_str())
                || rate.catalog_provider_id.as_deref() != Some(binding.provider_id.as_str())
                || rate.catalog_model_id.as_deref() != Some(binding.model_id.as_str())
            {
                return Err(catalog_conversion(
                    "catalog binding does not match its immutable rate revision",
                ));
            }
            let (model, _) = catalog_model_for_revision(&self.db, &rate)
                .await
                .map_err(catalog_conversion)?;
            if model.rates != binding.rates
                || model.tiers != binding.tiers
                || model.legacy_context_over_200k != binding.legacy_context_over_200k
            {
                return Err(catalog_conversion(
                    "catalog binding normalized values differ from its source snapshot",
                ));
            }
        }

        let desired_active_ids = desired_active
            .iter()
            .map(|source| match source {
                PricingBindingSource::ModelsDev(binding) => binding.id.as_str(),
                PricingBindingSource::Manual(override_) => override_.id.as_str(),
            })
            .collect::<std::collections::BTreeSet<_>>();
        let existing_by_id = loaded
            .bindings
            .iter()
            .map(|binding| (binding.id.as_str(), binding))
            .collect::<BTreeMap<_, _>>();
        let updated_at = db_time(now);
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(catalog_sqlx)?;
        if let Some(result_json) = operation_receipt_in_tx(
            &mut transaction,
            &operation_scope,
            &request.idempotency_key,
            &request_digest,
        )
        .await
        .map_err(catalog_operation_error)?
        {
            let configuration =
                configuration_from_result_json(&result_json).map_err(catalog_conversion)?;
            transaction.commit().await.map_err(catalog_sqlx)?;
            return Ok(configuration);
        }

        // Retire omitted/replaced rows first, which releases the active
        // runtime uniqueness slot before an exact replacement is inserted.
        for binding in &loaded.bindings {
            if binding.subject_revision_id == loaded.revision.id
                && binding.subject_revision_digest == loaded.revision.revision_digest
                && binding.state == db::PricingSubjectState::Active
                && !desired_active_ids.contains(binding.id.as_str())
            {
                db::PricingSubjectRepo::retire_pricing_subject_binding_in_tx(
                    &*self.db,
                    &mut transaction,
                    db::RetirePricingSubjectBinding {
                        id: binding.id.clone(),
                        expected_version: binding.version,
                        retired_at: updated_at.clone(),
                        updated_at: updated_at.clone(),
                    },
                )
                .await
                .map_err(catalog_error)?;
            }
        }

        for source in desired_active {
            let (id, rate_revision_id, source_kind, provider_id, model_id) = match source {
                PricingBindingSource::ModelsDev(binding) => (
                    binding.id.clone(),
                    binding.rate_revision_id.clone(),
                    db::PricingRateSourceKind::ModelsDevCatalog,
                    Some(binding.provider_id.clone()),
                    Some(binding.model_id.clone()),
                ),
                PricingBindingSource::Manual(override_) => {
                    let rate_input = db::CreatePricingRateRevision {
                        id: override_.rate_revision_id.clone(),
                        source_kind: db::PricingRateSourceKind::ManualOverride,
                        owner_user_id: Some(loaded.subject.owner_user_id.clone()),
                        catalog_snapshot_id: None,
                        catalog_provider_id: None,
                        catalog_model_id: None,
                        pricing_subject_revision_id: Some(loaded.revision.id.clone()),
                        pricing_subject_revision_digest: Some(
                            loaded.revision.revision_digest.clone(),
                        ),
                        runtime_model: Some(override_.runtime_model.clone()),
                        source_model_key: None,
                        source_last_updated: None,
                        currency: "USD".to_owned(),
                        rates: db_rates(override_.rates),
                        tiers_json: "[]".to_owned(),
                        legacy_context_over_200k_json: None,
                        context_tier_state: "none".to_owned(),
                        received_rates_json: manual_rates_json(override_.rates),
                        rate_digest: override_.rate_revision_id.clone(),
                        effective_at: db_time(override_.effective_at),
                        created_at: updated_at.clone(),
                    };
                    db::PricingCatalogRepo::create_pricing_rate_revision_in_tx(
                        &*self.db,
                        &mut transaction,
                        rate_input,
                    )
                    .await
                    .map_err(catalog_error)?;
                    (
                        override_.id.clone(),
                        override_.rate_revision_id.clone(),
                        db::PricingRateSourceKind::ManualOverride,
                        None,
                        None,
                    )
                }
            };
            if existing_by_id
                .get(id.as_str())
                .is_some_and(|binding| binding.state == db::PricingSubjectState::Active)
            {
                continue;
            }
            let binding_digest =
                binding_digest(subject_id, &loaded.revision.revision_digest, source);
            db::PricingSubjectRepo::create_pricing_subject_binding_in_tx(
                &*self.db,
                &mut transaction,
                db::CreatePricingSubjectBinding {
                    id,
                    owner_user_id: loaded.subject.owner_user_id.clone(),
                    subject_id: subject_id.to_owned(),
                    subject_revision_id: loaded.revision.id.clone(),
                    subject_revision_digest: loaded.revision.revision_digest.clone(),
                    runtime_model: source_runtime(source).to_owned(),
                    source_kind,
                    catalog_provider_id: provider_id,
                    catalog_model_id: model_id,
                    rate_revision_id,
                    binding_digest,
                    effective_at: updated_at.clone(),
                    created_at: updated_at.clone(),
                    updated_at: updated_at.clone(),
                },
            )
            .await
            .map_err(catalog_error)?;
        }

        let last_update_digest = proposed
            .last_update_digest
            .clone()
            .ok_or_else(|| catalog_conversion("binding mutation did not produce a digest"))?;
        let updated_subject = db::PricingSubjectRepo::update_pricing_subject_in_tx(
            &*self.db,
            &mut transaction,
            db::UpdatePricingSubject {
                id: subject_id.to_owned(),
                expected_version: loaded.subject.version,
                current_revision_id: Some(Some(loaded.revision.id.clone())),
                state: Some(loaded.subject.state),
                last_idempotency_key: Some(Some(request.idempotency_key.clone())),
                last_update_digest: Some(Some(last_update_digest)),
                updated_at: updated_at.clone(),
            },
        )
        .await
        .map_err(catalog_error)?;
        proposed.version = u64::try_from(updated_subject.version)
            .map_err(|_| catalog_conversion("pricing subject version is negative"))?;
        let result_json = configuration_result_json(&proposed);
        insert_operation_receipt_in_tx(
            &mut transaction,
            &operation_scope,
            &request.idempotency_key,
            &request_digest,
            &result_json,
            &updated_at,
        )
        .await
        .map_err(catalog_conversion)?;
        transaction.commit().await.map_err(catalog_sqlx)?;
        Ok(proposed)
    }
}

struct PreviewRecord {
    owner_user_id: String,
    project_id: String,
    catalog_snapshot_id: String,
    catalog_freshness: db::PricingCatalogFreshness,
    usage_set_digest: String,
    eligible_event_count: i64,
    unmatched_event_count: i64,
    already_reported_event_count: i64,
    filters_json: String,
    projected_cost_summary_json: String,
    status: db::CostEstimationPreviewStatus,
    version: i64,
    expires_at: String,
    created_at: String,
}

async fn preview_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<Option<PreviewRecord>, db::DbError> {
    let Some(row) = sqlx::query(
        "SELECT owner_user_id, project_id, catalog_snapshot_id,
                catalog_freshness, usage_set_digest,
                eligible_event_count, unmatched_event_count,
                already_reported_event_count, filters_json,
                projected_cost_summary_json, status, version, expires_at, created_at
         FROM cost_estimation_preview WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await?
    else {
        return Ok(None);
    };
    let status: String = row.try_get("status")?;
    Ok(Some(PreviewRecord {
        owner_user_id: row.try_get("owner_user_id")?,
        project_id: row.try_get("project_id")?,
        catalog_snapshot_id: row.try_get("catalog_snapshot_id")?,
        catalog_freshness: row
            .try_get::<String, _>("catalog_freshness")?
            .parse()
            .map_err(|error| db::DbError::Check(format!("invalid catalog freshness: {error}")))?,
        usage_set_digest: row.try_get("usage_set_digest")?,
        eligible_event_count: row.try_get("eligible_event_count")?,
        unmatched_event_count: row.try_get("unmatched_event_count")?,
        already_reported_event_count: row.try_get("already_reported_event_count")?,
        filters_json: row.try_get("filters_json")?,
        projected_cost_summary_json: row.try_get("projected_cost_summary_json")?,
        status: status
            .parse()
            .map_err(|error| db::DbError::Check(format!("invalid preview status: {error}")))?,
        version: row.try_get("version")?,
        expires_at: row.try_get("expires_at")?,
        created_at: row.try_get("created_at")?,
    }))
}

fn preview_source_event_ids_from_filters(filters_json: &str) -> Result<Vec<String>, String> {
    let value: Value = serde_json::from_str(filters_json).map_err(|error| {
        format!("stored retrospective preview filters are invalid JSON: {error}")
    })?;
    let object = value
        .as_object()
        .ok_or_else(|| "stored retrospective preview filters must be an object".to_owned())?;
    let ids = object
        .get("source_event_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| "stored retrospective preview omits source_event_ids".to_owned())?;
    if ids.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS {
        return Err("stored retrospective preview contains too many source events".to_owned());
    }
    let mut result = Vec::with_capacity(ids.len());
    for id in ids {
        let id = id
            .as_str()
            .ok_or_else(|| "stored retrospective source event IDs must be strings".to_owned())?;
        validate_identifier(id, "retrospective source event id")?;
        result.push(id.to_owned());
    }
    result.sort();
    result.dedup();
    if result.len() != ids.len() {
        return Err("stored retrospective preview contains duplicate source event IDs".to_owned());
    }
    Ok(result)
}

fn preview_snapshot_revision_digest_from_filters(filters_json: &str) -> Result<String, String> {
    let value: Value = serde_json::from_str(filters_json).map_err(|error| {
        format!("stored retrospective preview filters are invalid JSON: {error}")
    })?;
    let object = value
        .as_object()
        .ok_or_else(|| "stored retrospective preview filters must be an object".to_owned())?;
    let digest = object
        .get("snapshot_revision_digest")
        .and_then(Value::as_str)
        .ok_or_else(|| "stored retrospective preview omits snapshot_revision_digest".to_owned())?;
    validate_identifier(digest, "retrospective snapshot revision digest")?;
    Ok(digest.to_owned())
}

fn preview_filters_json(
    preview: &RetrospectivePreview,
    snapshot_revision_digest: &str,
    catalog_status: &CatalogStatus,
    catalog_freshness: CatalogFreshness,
    now: SystemTime,
) -> String {
    let catalog_status_at = catalog_status.at(now);
    let mut source_event_ids = preview.source_event_ids.clone();
    source_event_ids.sort();
    json!({
        "schema": "retrospective-preview-v1",
        "source_event_ids": source_event_ids,
        "snapshot_revision_digest": snapshot_revision_digest,
        "catalog_freshness": catalog_freshness_name(catalog_freshness),
        "catalog_status": {
            "state": catalog_state_name(catalog_status_at.state),
            "active_snapshot_id": catalog_status.active_snapshot_id.clone(),
            "revision": catalog_status.revision.clone(),
            "last_checked_at": catalog_status_at.last_checked_at.map(db_time),
            "last_successful_check_at": catalog_status_at.last_successful_check_at.map(db_time),
            "stale_after": catalog_status_at.stale_after.map(db_time),
        },
        "eligible_events": preview.eligible_events.iter().map(|candidate| json!({
            "event_id": candidate.event_id,
            "amount_nano_usd": candidate.amount.as_nano_usd(),
            "rate_revision_id": candidate.rate_revision_id,
            "snapshot_id": candidate.snapshot_id,
            "selected_tier": candidate.selected_tier,
        })).collect::<Vec<_>>(),
        "unmatched_events": preview.unmatched_events.iter().map(|event| json!({
            "event_id": event.event_id,
            "reason": event.reason.as_str(),
        })).collect::<Vec<_>>(),
    })
    .to_string()
}

fn catalog_state_name(state: CatalogState) -> &'static str {
    match state {
        CatalogState::Absent => "absent",
        CatalogState::Fresh => "fresh",
        CatalogState::Stale => "stale",
        CatalogState::RefreshFailed => "refresh_failed",
    }
}

fn catalog_freshness_name(freshness: CatalogFreshness) -> &'static str {
    match freshness {
        CatalogFreshness::Fresh => "fresh",
        CatalogFreshness::Stale => "stale",
        CatalogFreshness::RefreshFailed => "refresh_failed",
        CatalogFreshness::NotApplicable => "not_applicable",
    }
}

fn db_catalog_freshness(
    freshness: CatalogFreshness,
) -> Result<db::PricingCatalogFreshness, String> {
    match freshness {
        CatalogFreshness::Fresh => Ok(db::PricingCatalogFreshness::Fresh),
        CatalogFreshness::Stale => Ok(db::PricingCatalogFreshness::Stale),
        CatalogFreshness::RefreshFailed => Ok(db::PricingCatalogFreshness::RefreshFailed),
        CatalogFreshness::NotApplicable => {
            Err("catalog retrospective freshness cannot be not_applicable".to_owned())
        }
    }
}

fn service_catalog_freshness(freshness: db::PricingCatalogFreshness) -> CatalogFreshness {
    match freshness {
        db::PricingCatalogFreshness::Fresh => CatalogFreshness::Fresh,
        db::PricingCatalogFreshness::Stale => CatalogFreshness::Stale,
        db::PricingCatalogFreshness::RefreshFailed => CatalogFreshness::RefreshFailed,
    }
}

fn parse_catalog_freshness(value: &str) -> Result<CatalogFreshness, String> {
    match value {
        "fresh" => Ok(CatalogFreshness::Fresh),
        "stale" => Ok(CatalogFreshness::Stale),
        "refresh_failed" => Ok(CatalogFreshness::RefreshFailed),
        "not_applicable" => {
            Err("catalog retrospective freshness cannot be not_applicable".to_owned())
        }
        other => Err(format!("unknown catalog retrospective freshness {other:?}")),
    }
}

fn catalog_freshness_from_json(
    json_text: &str,
    field: &str,
) -> Result<CatalogFreshness, RetrospectiveRepositoryError> {
    let value: Value = serde_json::from_str(json_text).map_err(|error| {
        retrospective_conversion(format!("invalid persisted {field} JSON: {error}"))
    })?;
    let freshness = value
        .as_object()
        .and_then(|object| object.get("catalog_freshness"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            retrospective_conversion(format!("persisted {field} omits catalog_freshness"))
        })?;
    parse_catalog_freshness(freshness).map_err(retrospective_conversion)
}

fn canonical_event_list_equal(
    left: &[RetrospectiveEstimateCandidate],
    right: &[RetrospectiveEstimateCandidate],
) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_by(|a, b| a.event_id.cmp(&b.event_id));
    right.sort_by(|a, b| a.event_id.cmp(&b.event_id));
    left == right
}

fn canonical_unmatched_list_equal(
    left: &[RetrospectiveUnmatchedEvent],
    right: &[RetrospectiveUnmatchedEvent],
) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_by(|a, b| a.event_id.cmp(&b.event_id));
    right.sort_by(|a, b| a.event_id.cmp(&b.event_id));
    left == right
}

async fn usage_event_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    event_id: &str,
    owner_user_id: &str,
    project_id: &str,
) -> Result<RetrospectiveUsageEvent, RetrospectiveRepositoryError> {
    validate_identifier(event_id, "retrospective source event id")
        .map_err(retrospective_conversion)?;
    let row = sqlx::query(
        "SELECT id, owner_user_id, project_id, provider_id, model_id,
                input_tokens, output_tokens, cache_read_tokens,
                cache_write_tokens, context_tokens, provider_reported_nano_usd,
                legacy_reported_cost_usd IS NOT NULL AS has_legacy_reported_cost,
                cost_kind, occurred_at
         FROM usage_event WHERE id = ?",
    )
    .bind(event_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(retrospective_sqlx)?
    .ok_or(RetrospectiveRepositoryError::UsageSetConflict)?;

    let stored_id: String = row
        .try_get("id")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let stored_owner: Option<String> = row
        .try_get("owner_user_id")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let stored_project: Option<String> = row
        .try_get("project_id")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    if stored_id != event_id
        || stored_owner.as_deref() != Some(owner_user_id)
        || stored_project.as_deref() != Some(project_id)
    {
        return Err(RetrospectiveRepositoryError::UsageSetConflict);
    }

    let optional_identifier = |value: Option<String>, field: &str| {
        value
            .filter(|value| !value.trim().is_empty())
            .map(|value| {
                validate_identifier(&value, field)
                    .map_err(retrospective_conversion)
                    .map(|()| value)
            })
            .transpose()
    };
    let provider_id: Option<String> = optional_identifier(
        row.try_get("provider_id")
            .map_err(|error| retrospective_conversion(error.to_string()))?,
        "retrospective provider id",
    )?;
    let model_id: Option<String> = optional_identifier(
        row.try_get("model_id")
            .map_err(|error| retrospective_conversion(error.to_string()))?,
        "retrospective model id",
    )?;

    let input_tokens: Option<i64> = row
        .try_get("input_tokens")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let output_tokens: Option<i64> = row
        .try_get("output_tokens")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let cache_read_tokens: Option<i64> = row
        .try_get("cache_read_tokens")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let cache_write_tokens: Option<i64> = row
        .try_get("cache_write_tokens")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let counters = match (
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_write_tokens,
    ) {
        (Some(input), Some(output), Some(cache_read), Some(cache_write))
            if input >= 0 && output >= 0 && cache_read >= 0 && cache_write >= 0 =>
        {
            Some(EventTokenCounts::new(
                u64::try_from(input).map_err(|_| {
                    retrospective_conversion("input token count exceeds the supported range")
                })?,
                u64::try_from(output).map_err(|_| {
                    retrospective_conversion("output token count exceeds the supported range")
                })?,
                u64::try_from(cache_read).map_err(|_| {
                    retrospective_conversion("cache-read token count exceeds the supported range")
                })?,
                u64::try_from(cache_write).map_err(|_| {
                    retrospective_conversion("cache-write token count exceeds the supported range")
                })?,
            ))
        }
        (Some(_), Some(_), Some(_), Some(_)) => {
            return Err(RetrospectiveRepositoryError::UsageSetConflict);
        }
        _ => None,
    };

    let context_tokens: Option<i64> = row
        .try_get("context_tokens")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let context_tokens = match context_tokens {
        Some(value) if value >= 0 => Some(u64::try_from(value).map_err(|_| {
            retrospective_conversion("context token count exceeds the supported range")
        })?),
        Some(_) => return Err(RetrospectiveRepositoryError::UsageSetConflict),
        None => None,
    };
    let provider_reported_nano_usd: Option<i64> = row
        .try_get("provider_reported_nano_usd")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    // Legacy rows store an old REAL amount.  We only need to know whether
    // that authoritative provider-reported field was populated; selecting a
    // boolean SQLite expression avoids loading/coercing money through a
    // floating-point type.
    let has_legacy_reported_cost: i64 = row
        .try_get("has_legacy_reported_cost")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let has_legacy_reported_cost = has_legacy_reported_cost != 0;
    let cost_kind: String = row
        .try_get("cost_kind")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let provider_reported_amount = match provider_reported_nano_usd {
        Some(value) => Some(
            NanoUsd::from_nano_usd(value).ok_or(RetrospectiveRepositoryError::UsageSetConflict)?,
        ),
        None => None,
    };
    let provider_reported_amount = if provider_reported_amount.is_some()
        || has_legacy_reported_cost
        || cost_kind == "provider_reported"
    {
        // A legacy REAL amount is intentionally not converted through a
        // floating-point representation.
        // It still proves that the event is provider-reported and therefore
        // must not receive a Forge estimate when its typed amount is absent.
        provider_reported_amount
    } else {
        None
    };
    let occurred_at: String = row
        .try_get("occurred_at")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let occurred_at = system_time(&occurred_at).map_err(retrospective_conversion)?;
    Ok(RetrospectiveUsageEvent {
        event_id: stored_id,
        provider_id,
        model_id,
        // Typed provider-reported money is enough for the domain to exclude
        // the row and the original counters remain part of the source digest.
        // A legacy REAL amount or an untyped provider-reported marker has no
        // safe fixed-point representation, so it is deliberately unmetered.
        counters: if provider_reported_amount.is_some() {
            counters
        } else if has_legacy_reported_cost || cost_kind == "provider_reported" {
            None
        } else {
            counters
        },
        provider_reported_amount,
        context_tokens,
        occurred_at,
    })
}

async fn source_events_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    preview: &RetrospectivePreview,
    owner_user_id: &str,
) -> Result<Vec<RetrospectiveUsageEvent>, RetrospectiveRepositoryError> {
    if preview.source_event_ids.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS {
        return Err(retrospective_conversion(
            "retrospective preview contains too many source events",
        ));
    }
    let mut ids = preview.source_event_ids.clone();
    ids.sort();
    ids.dedup();
    if ids.len() != preview.source_event_ids.len() {
        return Err(RetrospectiveRepositoryError::UsageSetConflict);
    }
    let mut events = Vec::with_capacity(ids.len());
    for event_id in ids {
        events.push(
            usage_event_in_tx(transaction, &event_id, owner_user_id, &preview.project_id).await?,
        );
    }
    Ok(events)
}

async fn next_estimate_revision_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    event_id: &str,
) -> Result<(i64, Option<String>), db::DbError> {
    let row = sqlx::query(
        "SELECT revision, id FROM cost_estimate_revision
         WHERE usage_event_id = ? ORDER BY revision DESC, id DESC LIMIT 1",
    )
    .bind(event_id)
    .fetch_optional(&mut **transaction)
    .await?;
    match row {
        Some(row) => {
            let revision: i64 = row.try_get("revision")?;
            let id: String = row.try_get("id")?;
            Ok((
                revision
                    .checked_add(1)
                    .ok_or_else(|| db::DbError::Check("estimate revision overflow".to_owned()))?,
                Some(id),
            ))
        }
        None => Ok((1, None)),
    }
}

fn persisted_reason(
    reason: PriceSelectionReasonCode,
) -> Result<db::CostCoverageReasonCode, RetrospectiveRepositoryError> {
    match reason {
        PriceSelectionReasonCode::MissingProvider => {
            Ok(db::CostCoverageReasonCode::MissingProvider)
        }
        PriceSelectionReasonCode::MissingModel => Ok(db::CostCoverageReasonCode::MissingModel),
        PriceSelectionReasonCode::MissingBinding | PriceSelectionReasonCode::RetiredBinding => {
            Ok(db::CostCoverageReasonCode::MissingBinding)
        }
        PriceSelectionReasonCode::IdentityMismatch => {
            Ok(db::CostCoverageReasonCode::IdentityMismatch)
        }
        PriceSelectionReasonCode::MissingRate => Ok(db::CostCoverageReasonCode::MissingRate),
        PriceSelectionReasonCode::UnresolvedTier => Ok(db::CostCoverageReasonCode::UnresolvedTier),
        PriceSelectionReasonCode::Unmetered => Err(retrospective_conversion(
            "V135 estimate revisions have no unmetered reason code",
        )),
    }
}

fn preview_summary(
    preview: &RetrospectivePreview,
    catalog_status: &CatalogStatus,
    catalog_freshness: CatalogFreshness,
    now: SystemTime,
) -> String {
    let catalog_status_at = catalog_status.at(now);
    json!({
        "currency": "USD",
        "decimal": preview.projected_cost.map(NanoUsd::to_usd_decimal),
        "nano_usd": preview.projected_cost.map(NanoUsd::as_nano_usd),
        "eligible_event_count": preview.eligible_event_count,
        "unmatched_event_count": preview.unmatched_event_count,
        "already_reported_event_count": preview.already_reported_event_count,
        "catalog_freshness": catalog_freshness_name(catalog_freshness),
        "catalog_state": catalog_state_name(catalog_status_at.state),
        "catalog_active_snapshot_id": catalog_status_at.active_snapshot_id.clone(),
        "catalog_revision": catalog_status_at.revision.clone(),
        "catalog_last_checked_at": catalog_status_at.last_checked_at.map(db_time),
        "catalog_last_successful_check_at": catalog_status_at.last_successful_check_at.map(db_time),
        "catalog_stale_after": catalog_status_at.stale_after.map(db_time),
    })
    .to_string()
}

fn cost_from_summary(summary: &str) -> Result<Option<NanoUsd>, RetrospectiveRepositoryError> {
    let value: Value = serde_json::from_str(summary)
        .map_err(|error| retrospective_conversion(format!("invalid cost summary JSON: {error}")))?;
    let nano = value
        .as_object()
        .and_then(|object| object.get("nano_usd"))
        .ok_or_else(|| retrospective_conversion("cost summary omits nano_usd"))?;
    match nano {
        Value::Null => Ok(None),
        Value::Number(number) => {
            let value = number.as_i64().ok_or_else(|| {
                retrospective_conversion("cost summary nano_usd is not an integer")
            })?;
            NanoUsd::from_nano_usd(value)
                .ok_or_else(|| retrospective_conversion("cost summary nano_usd is negative"))
                .map(Some)
        }
        _ => Err(retrospective_conversion(
            "cost summary nano_usd must be an integer or null",
        )),
    }
}

fn count_i64(value: usize, field: &str) -> Result<i64, RetrospectiveRepositoryError> {
    value.try_into().map_err(|_| {
        retrospective_conversion(format!("{field} does not fit the database integer range"))
    })
}

fn validate_retrospective_commit_bounds(
    preview: &RetrospectivePreview,
    request: &RetrospectiveCommitRequest,
) -> Result<(), RetrospectiveRepositoryError> {
    validate_identifier(&request.preview_id, "retrospective preview id")
        .map_err(retrospective_conversion)?;
    validate_identifier(&request.usage_set_digest, "retrospective usage-set digest")
        .map_err(retrospective_conversion)?;
    validate_identifier(&request.idempotency_key, "retrospective idempotency key")
        .map_err(retrospective_conversion)?;
    validate_identifier(&preview.id, "retrospective preview id")
        .map_err(retrospective_conversion)?;
    validate_identifier(&preview.project_id, "retrospective project id")
        .map_err(retrospective_conversion)?;
    validate_identifier(&preview.snapshot_id, "retrospective snapshot id")
        .map_err(retrospective_conversion)?;
    validate_identifier(&preview.usage_set_digest, "retrospective usage-set digest")
        .map_err(retrospective_conversion)?;
    if preview.source_event_ids.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS
        || preview.eligible_events.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS
        || preview.unmatched_events.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS
    {
        return Err(retrospective_conversion(
            "retrospective preview contains too many events",
        ));
    }
    for event_id in &preview.source_event_ids {
        validate_identifier(event_id, "retrospective source event id")
            .map_err(retrospective_conversion)?;
    }
    for candidate in &preview.eligible_events {
        validate_identifier(&candidate.event_id, "retrospective candidate event id")
            .map_err(retrospective_conversion)?;
        validate_identifier(
            &candidate.rate_revision_id,
            "retrospective candidate rate revision id",
        )
        .map_err(retrospective_conversion)?;
        validate_identifier(
            &candidate.snapshot_id,
            "retrospective candidate snapshot id",
        )
        .map_err(retrospective_conversion)?;
        validate_optional_text(
            candidate.selected_tier.as_deref(),
            "retrospective candidate selected tier",
        )
        .map_err(retrospective_conversion)?;
    }
    for unmatched in &preview.unmatched_events {
        validate_identifier(&unmatched.event_id, "retrospective unmatched event id")
            .map_err(retrospective_conversion)?;
    }
    Ok(())
}

fn retrospective_commit_request_digest(
    preview: &RetrospectivePreview,
    request: &RetrospectiveCommitRequest,
) -> String {
    let mut source_event_ids = preview.source_event_ids.clone();
    source_event_ids.sort();
    let mut candidates = preview
        .eligible_events
        .iter()
        .map(|candidate| {
            json!({
                "event_id": candidate.event_id,
                "amount_nano_usd": candidate.amount.as_nano_usd(),
                "rate_revision_id": candidate.rate_revision_id,
                "snapshot_id": candidate.snapshot_id,
                "selected_tier": candidate.selected_tier,
            })
            .to_string()
        })
        .collect::<Vec<_>>();
    candidates.sort();
    let mut unmatched = preview
        .unmatched_events
        .iter()
        .map(|event| {
            json!({
                "event_id": event.event_id,
                "reason": event.reason.as_str(),
            })
            .to_string()
        })
        .collect::<Vec<_>>();
    unmatched.sort();
    structured_digest(
        "retrospective-commit-request-v1",
        [
            Some(request.preview_id.clone()),
            Some(request.usage_set_digest.clone()),
            Some(request.idempotency_key.clone()),
            Some(preview.id.clone()),
            Some(preview.project_id.clone()),
            Some(preview.snapshot_id.clone()),
            Some(catalog_freshness_name(preview.catalog_freshness).to_owned()),
            Some(preview.usage_set_digest.clone()),
            Some(serde_json::to_string(&source_event_ids).unwrap_or_default()),
            Some(preview.eligible_event_count.to_string()),
            Some(preview.unmatched_event_count.to_string()),
            Some(preview.already_reported_event_count.to_string()),
            preview
                .projected_cost
                .map(|cost| cost.as_nano_usd().to_string()),
            Some(serde_json::to_string(&candidates).unwrap_or_default()),
            Some(serde_json::to_string(&unmatched).unwrap_or_default()),
            Some(db_time(preview.expires_at)),
        ],
    )
}

fn retrospective_run_result_json(run: &db::CostEstimationRun) -> String {
    json!({
        "kind": "retrospective_run",
        "result_ref": {
            "run_id": run.id,
            "preview_id": run.preview_id,
            "project_id": run.project_id,
            "snapshot_id": run.catalog_snapshot_id,
            "usage_set_digest": run.usage_set_digest,
        },
    })
    .to_string()
}

fn retrospective_run_id_from_result_json(
    value: &str,
    preview: &RetrospectivePreview,
) -> Result<String, RetrospectiveRepositoryError> {
    let value: Value = serde_json::from_str(value)
        .map_err(|error| retrospective_conversion(format!("invalid run receipt JSON: {error}")))?;
    let reference = value
        .get("result_ref")
        .and_then(Value::as_object)
        .ok_or_else(|| retrospective_conversion("run receipt omits result_ref"))?;
    for (field, expected) in [
        ("preview_id", preview.id.as_str()),
        ("project_id", preview.project_id.as_str()),
        ("snapshot_id", preview.snapshot_id.as_str()),
        ("usage_set_digest", preview.usage_set_digest.as_str()),
    ] {
        if reference.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(RetrospectiveRepositoryError::Conflict);
        }
    }
    let run_id = reference
        .get("run_id")
        .and_then(Value::as_str)
        .ok_or_else(|| retrospective_conversion("run receipt omits result_ref.run_id"))?;
    validate_identifier(run_id, "retrospective run id").map_err(retrospective_conversion)?;
    Ok(run_id.to_owned())
}

fn retrospective_run(
    run: db::CostEstimationRun,
    cost: Option<NanoUsd>,
) -> Result<RetrospectiveRun, RetrospectiveRepositoryError> {
    Ok(RetrospectiveRun {
        id: run.id,
        project_id: run.project_id,
        preview_id: run.preview_id,
        snapshot_id: run.catalog_snapshot_id,
        usage_set_digest: run.usage_set_digest,
        applied_event_count: run.applied_event_count.try_into().map_err(|_| {
            retrospective_conversion("stored retrospective applied count is negative")
        })?,
        unmatched_event_count: run.unmatched_event_count.try_into().map_err(|_| {
            retrospective_conversion("stored retrospective unmatched count is negative")
        })?,
        cost,
        created_at: system_time(&run.created_at).map_err(retrospective_conversion)?,
    })
}

#[async_trait]
impl RetrospectiveEstimateRepository for SqlitePricingRepository {
    async fn commit_retrospective_preview(
        &self,
        preview: RetrospectivePreview,
        request: RetrospectiveCommitRequest,
        now: SystemTime,
    ) -> Result<RetrospectiveRun, RetrospectiveRepositoryError> {
        if request.idempotency_key.trim().is_empty() || request.idempotency_key.len() > 256 {
            return Err(RetrospectiveRepositoryError::Conflict);
        }
        validate_retrospective_commit_bounds(&preview, &request)?;
        let request_digest = retrospective_commit_request_digest(&preview, &request);
        let mut receipt_transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(retrospective_sqlx)?;
        if let Some(result_json) = operation_receipt_in_tx(
            &mut receipt_transaction,
            RETROSPECTIVE_COMMIT_OPERATION_SCOPE,
            &request.idempotency_key,
            &request_digest,
        )
        .await
        .map_err(retrospective_operation_error)?
        {
            let run_id = retrospective_run_id_from_result_json(&result_json, &preview)?;
            receipt_transaction
                .commit()
                .await
                .map_err(retrospective_sqlx)?;
            let run = db::RetrospectiveEstimateRepo::get_cost_estimation_run(&*self.db, &run_id)
                .await
                .map_err(retrospective_error)?
                .ok_or_else(|| {
                    retrospective_conversion(
                        "retrospective operation receipt points to a missing run",
                    )
                })?;
            if run.status != db::CostEstimationRunStatus::Committed {
                return Err(RetrospectiveRepositoryError::Conflict);
            }
            return retrospective_run(run.clone(), cost_from_summary(&run.cost_summary_json)?);
        }
        receipt_transaction
            .commit()
            .await
            .map_err(retrospective_sqlx)?;

        if request.preview_id != preview.id || request.usage_set_digest != preview.usage_set_digest
        {
            return Err(RetrospectiveRepositoryError::UsageSetConflict);
        }
        validate_identifier(&preview.id, "retrospective preview id")
            .map_err(retrospective_conversion)?;
        validate_identifier(&preview.project_id, "retrospective project id")
            .map_err(retrospective_conversion)?;
        validate_identifier(&preview.snapshot_id, "retrospective snapshot id")
            .map_err(retrospective_conversion)?;
        if preview.source_event_ids.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS {
            return Err(retrospective_conversion(
                "retrospective preview contains too many source events",
            ));
        }
        let project = db::ProjectRepo::get_by_id(&*self.db, &preview.project_id)
            .await
            .map_err(retrospective_error)?
            .ok_or_else(|| retrospective_conversion("retrospective project was not found"))?;
        let owner_user_id = project.owner_id.ok_or_else(|| {
            retrospective_conversion("retrospective project has no owner for DB scope")
        })?;
        let snapshot =
            db::PricingCatalogRepo::get_pricing_catalog_snapshot(&*self.db, &preview.snapshot_id)
                .await
                .map_err(retrospective_error)?
                .ok_or_else(|| retrospective_conversion("retrospective snapshot was not found"))?;
        let snapshot = snapshot_from_db(snapshot)
            .await
            .map_err(retrospective_conversion)?;
        if snapshot.id != preview.snapshot_id {
            return Err(retrospective_conversion(
                "retrospective snapshot identity does not match the preview",
            ));
        }

        let operation_key = retrospective_commit_idempotency_key(&request.idempotency_key);

        let now_text = db_time(now);

        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(retrospective_sqlx)?;
        // Recheck the immutable receipt after acquiring the commit
        // transaction.  The earlier read is an inexpensive fast path, but
        // two callers can both pass it before either one commits.  A
        // transaction-local preflight makes the receipt the durable
        // all-key idempotency authority and prevents the second caller from
        // touching the preview, usage set, or estimate revisions.
        if let Some(result_json) = operation_receipt_in_tx(
            &mut transaction,
            RETROSPECTIVE_COMMIT_OPERATION_SCOPE,
            &request.idempotency_key,
            &request_digest,
        )
        .await
        .map_err(retrospective_operation_error)?
        {
            let run_id = retrospective_run_id_from_result_json(&result_json, &preview)?;
            transaction.commit().await.map_err(retrospective_sqlx)?;
            let run = db::RetrospectiveEstimateRepo::get_cost_estimation_run(&*self.db, &run_id)
                .await
                .map_err(retrospective_error)?
                .ok_or_else(|| {
                    retrospective_conversion(
                        "retrospective operation receipt points to a missing run",
                    )
                })?;
            if run.status != db::CostEstimationRunStatus::Committed {
                return Err(RetrospectiveRepositoryError::Conflict);
            }
            return retrospective_run(run.clone(), cost_from_summary(&run.cost_summary_json)?);
        }
        let stored_preview = preview_in_tx(&mut transaction, &preview.id)
            .await
            .map_err(retrospective_error)?;
        let had_stored_preview = stored_preview.is_some();
        let source_event_ids = if let Some(stored) = &stored_preview {
            if preview_snapshot_revision_digest_from_filters(&stored.filters_json)
                .map_err(|_| RetrospectiveRepositoryError::UsageSetConflict)?
                != snapshot.revision_digest
            {
                return Err(RetrospectiveRepositoryError::UsageSetConflict);
            }
            preview_source_event_ids_from_filters(&stored.filters_json)
                .map_err(|_| RetrospectiveRepositoryError::UsageSetConflict)?
        } else {
            preview.source_event_ids.clone()
        };
        let mut source_preview = preview.clone();
        source_preview.source_event_ids = source_event_ids;
        let source_events =
            source_events_in_tx(&mut transaction, &source_preview, &owner_user_id).await?;
        // Freshness belongs to the mutable catalog status, not the immutable
        // snapshot payload.  Read it inside the same write transaction used
        // for preview verification so a successful 304 is reflected and the
        // persisted provenance cannot be based on a stale pre-transaction
        // status read.
        let catalog_status = catalog_status_in_tx(&mut transaction).await?;
        let current_catalog_freshness = catalog_status.freshness_for_snapshot(&snapshot, now);
        let catalog_freshness = stored_preview
            .as_ref()
            .map(|stored| service_catalog_freshness(stored.catalog_freshness))
            .unwrap_or(current_catalog_freshness);
        // Recompute every candidate, amount, tier, and digest from the
        // append-only ledger while holding the write transaction.  A caller
        // can mutate its in-memory preview, but it cannot mutate this server
        // side reconstruction without changing the usage-set digest.
        let mut canonical_preview = pricing::preview_retrospective_estimates_with_freshness(
            preview.project_id.clone(),
            &snapshot,
            &source_events,
            now,
            std::time::Duration::ZERO,
            catalog_freshness,
        )
        .map_err(|error| retrospective_conversion(error.to_string()))?;
        if canonical_preview.id != preview.id
            || canonical_preview.catalog_freshness != preview.catalog_freshness
            || canonical_preview.usage_set_digest != preview.usage_set_digest
            || canonical_preview.source_event_ids != source_preview.source_event_ids
            || canonical_preview.eligible_event_count != preview.eligible_event_count
            || canonical_preview.unmatched_event_count != preview.unmatched_event_count
            || canonical_preview.already_reported_event_count
                != preview.already_reported_event_count
            || canonical_preview.projected_cost != preview.projected_cost
            || !canonical_event_list_equal(
                &canonical_preview.eligible_events,
                &preview.eligible_events,
            )
            || !canonical_unmatched_list_equal(
                &canonical_preview.unmatched_events,
                &preview.unmatched_events,
            )
        {
            return Err(RetrospectiveRepositoryError::UsageSetConflict);
        }
        for unmatched in &canonical_preview.unmatched_events {
            // Fail before any estimate row is inserted when the current DB
            // vocabulary cannot retain a reason without changing its meaning.
            let _ = persisted_reason(unmatched.reason)?;
        }
        if let Some(stored) = &stored_preview {
            if catalog_freshness_from_json(&stored.filters_json, "retrospective preview filters")?
                != catalog_freshness
                || catalog_freshness_from_json(
                    &stored.projected_cost_summary_json,
                    "retrospective preview summary",
                )? != catalog_freshness
            {
                return Err(RetrospectiveRepositoryError::UsageSetConflict);
            }
        }

        let snapshot_revision_digest = snapshot.revision_digest.clone();
        let summary = preview_summary(&canonical_preview, &catalog_status, catalog_freshness, now);
        let filters_json = preview_filters_json(
            &canonical_preview,
            &snapshot_revision_digest,
            &catalog_status,
            catalog_freshness,
            now,
        );
        let eligible_count = count_i64(
            canonical_preview.eligible_event_count,
            "eligible event count",
        )?;
        let unmatched_count = count_i64(
            canonical_preview.unmatched_event_count,
            "unmatched event count",
        )?;
        let reported_count = count_i64(
            canonical_preview.already_reported_event_count,
            "already reported event count",
        )?;

        let (preview_version, preview_status, preview_created_at, expires_text) =
            if let Some(stored) = stored_preview {
                if stored.owner_user_id != owner_user_id
                    || stored.project_id != canonical_preview.project_id
                    || stored.catalog_snapshot_id != canonical_preview.snapshot_id
                    || service_catalog_freshness(stored.catalog_freshness) != catalog_freshness
                    || stored.usage_set_digest != canonical_preview.usage_set_digest
                    || stored.eligible_event_count != eligible_count
                    || stored.unmatched_event_count != unmatched_count
                    || stored.already_reported_event_count != reported_count
                    || cost_from_summary(&stored.projected_cost_summary_json)?
                        != canonical_preview.projected_cost
                {
                    return Err(RetrospectiveRepositoryError::Conflict);
                }
                if stored.status == db::CostEstimationPreviewStatus::Expired
                    || (stored.status == db::CostEstimationPreviewStatus::Active
                        && now_text >= stored.expires_at)
                {
                    return Err(RetrospectiveRepositoryError::Expired);
                }
                if stored.status == db::CostEstimationPreviewStatus::Invalidated {
                    return Err(RetrospectiveRepositoryError::Conflict);
                }
                (
                    stored.version,
                    stored.status,
                    stored.created_at,
                    stored.expires_at,
                )
            } else {
                if now >= preview.expires_at {
                    return Err(RetrospectiveRepositoryError::Expired);
                }
                let maximum_expiry = now
                    .checked_add(MAX_RETROSPECTIVE_PREVIEW_TTL)
                    .ok_or_else(|| retrospective_conversion("retrospective expiry overflow"))?;
                let expires_at = preview.expires_at.min(maximum_expiry);
                (
                    1,
                    db::CostEstimationPreviewStatus::Active,
                    now_text.clone(),
                    db_time(expires_at),
                )
            };
        canonical_preview.expires_at =
            system_time(&expires_text).map_err(retrospective_conversion)?;

        if !had_stored_preview {
            db::RetrospectiveEstimateRepo::create_cost_estimation_preview_in_tx(
                &*self.db,
                &mut transaction,
                db::CreateCostEstimationPreview {
                    id: canonical_preview.id.clone(),
                    owner_user_id: owner_user_id.clone(),
                    project_id: canonical_preview.project_id.clone(),
                    catalog_snapshot_id: canonical_preview.snapshot_id.clone(),
                    catalog_freshness: db_catalog_freshness(catalog_freshness)
                        .map_err(retrospective_conversion)?,
                    usage_set_digest: canonical_preview.usage_set_digest.clone(),
                    window_from: None,
                    window_to: None,
                    filters_json: filters_json.clone(),
                    eligible_event_count: eligible_count,
                    unmatched_event_count: unmatched_count,
                    already_reported_event_count: reported_count,
                    projected_cost_summary_json: summary.clone(),
                    idempotency_key: format!("retrospective_preview:{}", canonical_preview.id),
                    expires_at: expires_text.clone(),
                    created_at: preview_created_at.clone(),
                    updated_at: now_text.clone(),
                },
            )
            .await
            .map_err(retrospective_error)?;
        }

        let run_id = structured_digest(
            "retrospective-run-v2",
            [
                Some(canonical_preview.id.clone()),
                Some(canonical_preview.usage_set_digest.clone()),
                Some(request.idempotency_key.clone()),
            ],
        );
        let run = db::RetrospectiveEstimateRepo::create_cost_estimation_run_in_tx(
            &*self.db,
            &mut transaction,
            db::CreateCostEstimationRun {
                id: run_id.clone(),
                owner_user_id: owner_user_id.clone(),
                project_id: canonical_preview.project_id.clone(),
                preview_id: canonical_preview.id.clone(),
                catalog_snapshot_id: canonical_preview.snapshot_id.clone(),
                usage_set_digest: canonical_preview.usage_set_digest.clone(),
                status: db::CostEstimationRunStatus::Pending,
                cost_summary_json: summary.clone(),
                idempotency_key: operation_key,
                supersedes_run_id: None,
                created_at: preview_created_at,
                updated_at: now_text.clone(),
            },
        )
        .await
        .map_err(retrospective_error)?;
        if run.status == db::CostEstimationRunStatus::Committed {
            let cost = cost_from_summary(&run.cost_summary_json)?;
            let result_json = retrospective_run_result_json(&run);
            insert_operation_receipt_in_tx(
                &mut transaction,
                RETROSPECTIVE_COMMIT_OPERATION_SCOPE,
                &request.idempotency_key,
                &request_digest,
                &result_json,
                &now_text,
            )
            .await
            .map_err(retrospective_conversion)?;
            transaction.commit().await.map_err(retrospective_sqlx)?;
            return retrospective_run(run, cost);
        }

        for candidate in &canonical_preview.eligible_events {
            create_candidate_revision(
                &self.db,
                &mut transaction,
                &run,
                &owner_user_id,
                &canonical_preview,
                candidate,
                &now_text,
            )
            .await
            .map_err(retrospective_error)?;
        }
        for unmatched in &canonical_preview.unmatched_events {
            create_unmatched_revision(
                &self.db,
                &mut transaction,
                &run,
                &owner_user_id,
                &canonical_preview,
                unmatched,
                &now_text,
            )
            .await
            .map_err(retrospective_error)?;
        }

        if preview_status == db::CostEstimationPreviewStatus::Active {
            db::RetrospectiveEstimateRepo::update_cost_estimation_preview_in_tx(
                &*self.db,
                &mut transaction,
                db::UpdateCostEstimationPreview {
                    id: canonical_preview.id.clone(),
                    expected_version: preview_version,
                    status: db::CostEstimationPreviewStatus::Committed,
                    eligible_event_count: Some(eligible_count),
                    unmatched_event_count: Some(unmatched_count),
                    already_reported_event_count: Some(reported_count),
                    projected_cost_summary_json: Some(summary.clone()),
                    updated_at: now_text.clone(),
                },
            )
            .await
            .map_err(retrospective_error)?;
        }
        let committed = db::RetrospectiveEstimateRepo::update_cost_estimation_run_in_tx(
            &*self.db,
            &mut transaction,
            db::UpdateCostEstimationRun {
                id: run.id.clone(),
                expected_version: run.version,
                status: db::CostEstimationRunStatus::Committed,
                applied_event_count: Some(eligible_count),
                unmatched_event_count: Some(unmatched_count),
                already_reported_event_count: Some(reported_count),
                cost_summary_json: Some(summary),
                completed_at: Some(Some(now_text.clone())),
                updated_at: now_text.clone(),
            },
        )
        .await
        .map_err(retrospective_error)?;
        let result_json = retrospective_run_result_json(&committed);
        insert_operation_receipt_in_tx(
            &mut transaction,
            RETROSPECTIVE_COMMIT_OPERATION_SCOPE,
            &request.idempotency_key,
            &request_digest,
            &result_json,
            &now_text,
        )
        .await
        .map_err(retrospective_conversion)?;
        transaction.commit().await.map_err(retrospective_sqlx)?;
        retrospective_run(committed, canonical_preview.projected_cost)
    }
}

async fn create_candidate_revision(
    db: &db::SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    run: &db::CostEstimationRun,
    owner_user_id: &str,
    preview: &RetrospectivePreview,
    candidate: &RetrospectiveEstimateCandidate,
    now_text: &str,
) -> db::Result<db::CostEstimateRevision> {
    if candidate.snapshot_id != preview.snapshot_id || candidate.rate_revision_id.trim().is_empty()
    {
        return Err(db::DbError::Check(
            "retrospective candidate provenance does not match its preview".to_owned(),
        ));
    }
    let (revision, supersedes_revision_id) =
        next_estimate_revision_in_tx(transaction, &candidate.event_id).await?;
    let amount = candidate.amount.as_nano_usd();
    let estimate_digest = structured_digest(
        "retrospective-estimate-v2",
        [
            Some(run.id.clone()),
            Some(candidate.event_id.clone()),
            Some(revision.to_string()),
            Some(candidate.rate_revision_id.clone()),
            Some(candidate.snapshot_id.clone()),
            Some(amount.to_string()),
            candidate.selected_tier.clone(),
        ],
    );
    db::RetrospectiveEstimateRepo::create_cost_estimate_revision_in_tx(
        db,
        transaction,
        db::CreateCostEstimateRevision {
            id: estimate_digest.clone(),
            run_id: run.id.clone(),
            owner_user_id: owner_user_id.to_owned(),
            project_id: preview.project_id.clone(),
            usage_event_id: candidate.event_id.clone(),
            revision,
            supersedes_revision_id,
            state: db::CostEstimateRevisionState::Applied,
            rate_revision_id: Some(candidate.rate_revision_id.clone()),
            catalog_snapshot_id: Some(candidate.snapshot_id.clone()),
            estimated_nano_usd: Some(amount),
            formula_revision: Some(pricing::COST_FORMULA_REVISION.to_owned()),
            reason_code: None,
            estimate_digest,
            created_at: now_text.to_owned(),
        },
    )
    .await
}

async fn create_unmatched_revision(
    db: &db::SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    run: &db::CostEstimationRun,
    owner_user_id: &str,
    preview: &RetrospectivePreview,
    unmatched: &RetrospectiveUnmatchedEvent,
    now_text: &str,
) -> db::Result<db::CostEstimateRevision> {
    let reason = persisted_reason(unmatched.reason)
        .map_err(|error| db::DbError::Check(error.to_string()))?;
    let (revision, supersedes_revision_id) =
        next_estimate_revision_in_tx(transaction, &unmatched.event_id).await?;
    let estimate_digest = structured_digest(
        "retrospective-unmatched-v2",
        [
            Some(run.id.clone()),
            Some(unmatched.event_id.clone()),
            Some(revision.to_string()),
            Some(reason.to_string()),
        ],
    );
    db::RetrospectiveEstimateRepo::create_cost_estimate_revision_in_tx(
        db,
        transaction,
        db::CreateCostEstimateRevision {
            id: estimate_digest.clone(),
            run_id: run.id.clone(),
            owner_user_id: owner_user_id.to_owned(),
            project_id: preview.project_id.clone(),
            usage_event_id: unmatched.event_id.clone(),
            revision,
            supersedes_revision_id,
            state: db::CostEstimateRevisionState::Unmatched,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            estimated_nano_usd: None,
            formula_revision: None,
            reason_code: Some(reason),
            estimate_digest,
            created_at: now_text.to_owned(),
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pricing::{
        parse_models_dev_catalog, preview_retrospective_estimates, CatalogRefreshOutcome,
        CatalogRefreshRequest, DesiredPricingBinding, EventTokenCounts, ModelsDevClient,
        ModelsDevHttpResponse, ModelsDevTransport, ModelsDevTransportError, NanoUsdPerMillion,
        RetrospectiveUsageEvent,
    };
    use db::{
        create_sqlite_pool, run_migrations, CreatePricingSubject, CreatePricingSubjectRevision,
        PricingCatalogRepo, PricingSubjectKind, PricingSubjectRepo, PricingSubjectState,
        ProjectRepo, User, UserRepo,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::Notify;

    async fn database() -> Arc<db::SqliteDb> {
        let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
        run_migrations(&pool).await.expect("migrations");
        Arc::new(db::SqliteDb::new(pool))
    }

    fn at(seconds: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
    }

    fn catalog_body() -> Vec<u8> {
        br#"{
          "openai": {
            "id": "openai",
            "name": "OpenAI",
            "models": {
              "gpt-test": {
                "id": "gpt-test",
                "last_updated": "2026-09-01",
                "cost": {
                  "input": 1,
                  "output": 2,
                  "cache_read": 0
                }
              }
            }
          }
        }"#
        .to_vec()
    }

    fn snapshot(id: &str, when: SystemTime) -> CatalogSnapshot {
        parse_models_dev_catalog(&catalog_body())
            .expect("fixture parses")
            .into_snapshot(id, Some("etag-1".to_owned()), when, when)
            .expect("snapshot materializes")
    }

    fn snapshot_variant(id: &str, when: SystemTime, model_id: &str) -> CatalogSnapshot {
        let body = String::from_utf8(catalog_body())
            .expect("fixture UTF-8")
            .replace("gpt-test", model_id)
            .into_bytes();
        parse_models_dev_catalog(&body)
            .expect("variant parses")
            .into_snapshot(id, Some(format!("etag-{model_id}")), when, when)
            .expect("variant materializes")
    }

    fn snapshot_rate_variant(id: &str, when: SystemTime, input_rate: &str) -> CatalogSnapshot {
        let body = String::from_utf8(catalog_body())
            .expect("fixture UTF-8")
            .replacen("\"input\": 1", &format!("\"input\": {input_rate}"), 1)
            .into_bytes();
        parse_models_dev_catalog(&body)
            .expect("rate variant parses")
            .into_snapshot(id, Some(format!("etag-{input_rate}")), when, when)
            .expect("rate variant materializes")
    }

    struct CountingTransport {
        responses: Mutex<Vec<Result<ModelsDevHttpResponse, ModelsDevTransportError>>>,
        fetch_count: AtomicUsize,
    }

    impl CountingTransport {
        fn new(responses: Vec<Result<ModelsDevHttpResponse, ModelsDevTransportError>>) -> Self {
            Self {
                responses: Mutex::new(responses),
                fetch_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl ModelsDevTransport for CountingTransport {
        async fn fetch(
            &self,
            _if_none_match: Option<&str>,
        ) -> Result<ModelsDevHttpResponse, ModelsDevTransportError> {
            self.fetch_count.fetch_add(1, Ordering::Relaxed);
            let mut responses = self.responses.lock().expect("transport response lock");
            if responses.is_empty() {
                Err(ModelsDevTransportError::Request(
                    "unexpected third transport fetch".to_owned(),
                ))
            } else {
                responses.remove(0)
            }
        }
    }

    struct BlockingFirstTransport {
        response: Mutex<Option<Result<ModelsDevHttpResponse, ModelsDevTransportError>>>,
        first_started: Notify,
        release_first: Notify,
        fetch_count: AtomicUsize,
    }

    impl BlockingFirstTransport {
        fn new(response: Result<ModelsDevHttpResponse, ModelsDevTransportError>) -> Self {
            Self {
                response: Mutex::new(Some(response)),
                first_started: Notify::new(),
                release_first: Notify::new(),
                fetch_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl ModelsDevTransport for BlockingFirstTransport {
        async fn fetch(
            &self,
            _if_none_match: Option<&str>,
        ) -> Result<ModelsDevHttpResponse, ModelsDevTransportError> {
            let fetch_number = self.fetch_count.fetch_add(1, Ordering::Relaxed);
            if fetch_number == 0 {
                self.first_started.notify_one();
                self.release_first.notified().await;
            }
            self.response
                .lock()
                .expect("blocking transport response lock")
                .take()
                .unwrap_or_else(|| {
                    Err(ModelsDevTransportError::Request(
                        "unexpected second transport fetch".to_owned(),
                    ))
                })
        }
    }

    struct SignallingCatalogRepository {
        inner: Arc<SqlitePricingRepository>,
        status_calls: AtomicUsize,
        second_initial_status: Notify,
    }

    #[async_trait::async_trait]
    impl PricingCatalogRepository for SignallingCatalogRepository {
        async fn catalog_status(&self) -> Result<CatalogStatus, CatalogRepositoryError> {
            if self.status_calls.fetch_add(1, Ordering::Relaxed) >= 2 {
                self.second_initial_status.notify_one();
            }
            self.inner.catalog_status().await
        }

        async fn replay_catalog_refresh(
            &self,
            idempotency_key: &str,
        ) -> Result<Option<CatalogRefreshOutcome>, CatalogRepositoryError> {
            self.inner.replay_catalog_refresh(idempotency_key).await
        }

        async fn record_catalog_refresh_already_refreshed(
            &self,
            checked_at: SystemTime,
            idempotency_key: &str,
        ) -> Result<CatalogStatus, CatalogRepositoryError> {
            self.inner
                .record_catalog_refresh_already_refreshed(checked_at, idempotency_key)
                .await
        }

        async fn active_catalog_snapshot(
            &self,
        ) -> Result<Option<CatalogSnapshot>, CatalogRepositoryError> {
            self.inner.active_catalog_snapshot().await
        }

        async fn activate_catalog_snapshot(
            &self,
            snapshot: CatalogSnapshot,
            idempotency_key: &str,
        ) -> Result<CatalogStatus, CatalogRepositoryError> {
            self.inner
                .activate_catalog_snapshot(snapshot, idempotency_key)
                .await
        }

        async fn record_catalog_not_modified(
            &self,
            checked_at: SystemTime,
            etag: Option<String>,
            idempotency_key: &str,
        ) -> Result<CatalogStatus, CatalogRepositoryError> {
            self.inner
                .record_catalog_not_modified(checked_at, etag, idempotency_key)
                .await
        }

        async fn record_catalog_refresh_failure(
            &self,
            checked_at: SystemTime,
            code: CatalogRefreshErrorCode,
            idempotency_key: &str,
        ) -> Result<CatalogStatus, CatalogRepositoryError> {
            self.inner
                .record_catalog_refresh_failure(checked_at, code, idempotency_key)
                .await
        }
    }

    async fn subject_fixture(db: &db::SqliteDb) -> (String, String, String) {
        let now = db_time(at(100));
        UserRepo::create_user(
            db,
            &User {
                id: "pricing-owner".to_owned(),
                email: "pricing-owner@example.test".to_owned(),
                password_hash: "test".to_owned(),
                display_name: Some("Pricing owner".to_owned()),
                is_admin: false,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("user creates");
        let subject_id = "pricing-subject".to_owned();
        PricingSubjectRepo::create_pricing_subject(
            db,
            CreatePricingSubject {
                id: subject_id.clone(),
                owner_user_id: "pricing-owner".to_owned(),
                subject_kind: PricingSubjectKind::ProviderEntry,
                provider_entry_id: Some("provider-entry".to_owned()),
                daemon_id: None,
                executor_type: None,
                current_revision_id: None,
                state: PricingSubjectState::Active,
                last_idempotency_key: None,
                last_update_digest: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("subject creates");
        let revision_id = "pricing-subject-revision".to_owned();
        let revision_digest =
            pricing::pricing_subject_revision_digest(&pricing::PricingSubjectIdentity {
                subject_kind: "provider_entry".to_owned(),
                provider_kind: "openai".to_owned(),
                credential_method: "api_key".to_owned(),
                endpoint_class: "https://api.openai.com/v1".to_owned(),
                runtime_fingerprint: None,
                schema_revision: "pricing-subject-v1".to_owned(),
            });
        PricingSubjectRepo::create_pricing_subject_revision(
            db,
            CreatePricingSubjectRevision {
                id: revision_id.clone(),
                subject_id: subject_id.clone(),
                owner_user_id: "pricing-owner".to_owned(),
                revision: 1,
                revision_digest: revision_digest.clone(),
                subject_kind: PricingSubjectKind::ProviderEntry,
                provider_entry_id: Some("provider-entry".to_owned()),
                daemon_id: None,
                executor_type: None,
                provider_kind: "openai".to_owned(),
                credential_method: "api_key".to_owned(),
                endpoint_class: "https://api.openai.com/v1".to_owned(),
                runtime_fingerprint: None,
                schema_revision: "pricing-subject-v1".to_owned(),
                non_secret_identity_json: "{}".to_owned(),
                created_at: now.clone(),
            },
        )
        .await
        .expect("subject revision creates");
        PricingSubjectRepo::update_pricing_subject(
            db,
            db::UpdatePricingSubject {
                id: subject_id.clone(),
                expected_version: 1,
                current_revision_id: Some(Some(revision_id.clone())),
                state: None,
                last_idempotency_key: None,
                last_update_digest: None,
                updated_at: now,
            },
        )
        .await
        .expect("subject points at revision");
        (subject_id, revision_id, revision_digest)
    }

    async fn second_subject_fixture(db: &db::SqliteDb) -> (String, String, String) {
        let now = db_time(at(100));
        let subject_id = "pricing-subject-second".to_owned();
        PricingSubjectRepo::create_pricing_subject(
            db,
            CreatePricingSubject {
                id: subject_id.clone(),
                owner_user_id: "pricing-owner".to_owned(),
                subject_kind: PricingSubjectKind::ProviderEntry,
                provider_entry_id: Some("provider-entry-second".to_owned()),
                daemon_id: None,
                executor_type: None,
                current_revision_id: None,
                state: PricingSubjectState::Active,
                last_idempotency_key: None,
                last_update_digest: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("second subject creates");
        let revision_id = "pricing-subject-revision-second".to_owned();
        let revision_digest =
            pricing::pricing_subject_revision_digest(&pricing::PricingSubjectIdentity {
                subject_kind: "provider_entry".to_owned(),
                provider_kind: "openai".to_owned(),
                credential_method: "api_key".to_owned(),
                endpoint_class: "https://api.openai.com/v1".to_owned(),
                runtime_fingerprint: None,
                schema_revision: "pricing-subject-v1".to_owned(),
            });
        PricingSubjectRepo::create_pricing_subject_revision(
            db,
            CreatePricingSubjectRevision {
                id: revision_id.clone(),
                subject_id: subject_id.clone(),
                owner_user_id: "pricing-owner".to_owned(),
                revision: 1,
                revision_digest: revision_digest.clone(),
                subject_kind: PricingSubjectKind::ProviderEntry,
                provider_entry_id: Some("provider-entry-second".to_owned()),
                daemon_id: None,
                executor_type: None,
                provider_kind: "openai".to_owned(),
                credential_method: "api_key".to_owned(),
                endpoint_class: "https://api.openai.com/v1".to_owned(),
                runtime_fingerprint: None,
                schema_revision: "pricing-subject-v1".to_owned(),
                non_secret_identity_json: "{}".to_owned(),
                created_at: now.clone(),
            },
        )
        .await
        .expect("second subject revision creates");
        PricingSubjectRepo::update_pricing_subject(
            db,
            db::UpdatePricingSubject {
                id: subject_id.clone(),
                expected_version: 1,
                current_revision_id: Some(Some(revision_id.clone())),
                state: None,
                last_idempotency_key: None,
                last_update_digest: None,
                updated_at: now,
            },
        )
        .await
        .expect("second subject points at revision");
        (subject_id, revision_id, revision_digest)
    }

    async fn project_fixture(db: &db::SqliteDb) -> String {
        let now = db_time(at(100));
        let project_id = "pricing-project".to_owned();
        ProjectRepo::create(
            db,
            db::CreateProject {
                id: project_id.clone(),
                name: "Pricing project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: Some("pricing-owner".to_owned()),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("project creates");
        project_id
    }

    async fn usage_event_fixture(
        db: &db::SqliteDb,
        project_id: &str,
        suffix: &str,
        counters: Option<EventTokenCounts>,
        provider_reported_nano_usd: Option<i64>,
    ) -> String {
        let now = db_time(at(100));
        let source_id = format!("execution-{suffix}");
        let execution_id = format!("execution-row-{suffix}");
        let selection_id = format!("pricing-selection-{suffix}");
        db::UsageLedgerRepo::create_pricing_selection(
            db,
            db::CreatePricingSelection {
                id: selection_id.clone(),
                owner_user_id: Some("pricing-owner".to_owned()),
                project_id: Some(project_id.to_owned()),
                domain_kind: db::PricingDomainKind::Execution,
                surface: db::UsageSurface::TaskExecution,
                source_id: source_id.clone(),
                execution_id: Some(execution_id.clone()),
                task_id: None,
                candidate_key: Some("candidate-0".to_owned()),
                attempt_ordinal: 0,
                subject_id: None,
                subject_revision_id: None,
                subject_revision_digest: None,
                binding_id: None,
                rate_revision_id: None,
                catalog_snapshot_id: None,
                catalog_freshness: None,
                runtime_model: Some("gpt-test".to_owned()),
                admitted_provider_id: Some("openai".to_owned()),
                admitted_model_id: Some("gpt-test".to_owned()),
                source_kind: None,
                provenance_kind: db::PricingAdmissionProvenanceKind::Runtime,
                selection_status: db::PricingSelectionStatus::Unpriced,
                selection_reason: None,
                selection_digest: format!("selection-digest-{suffix}"),
                selected_at: now.clone(),
                created_at: now.clone(),
            },
        )
        .await
        .expect("selection creates");
        let invocation_id = format!("pricing-invocation-{suffix}");
        db::UsageLedgerRepo::create_usage_invocation(
            db,
            db::CreateUsageInvocation {
                id: invocation_id.clone(),
                owner_user_id: Some("pricing-owner".to_owned()),
                project_id: Some(project_id.to_owned()),
                domain_kind: db::PricingDomainKind::Execution,
                surface: db::UsageSurface::TaskExecution,
                source_id: source_id.clone(),
                execution_id: Some(execution_id.clone()),
                task_id: None,
                domain_idempotency_key: format!("invocation-key-{suffix}"),
                candidate_key: Some("candidate-0".to_owned()),
                attempt_ordinal: 0,
                pricing_selection_id: selection_id,
                admitted_provider_id: Some("openai".to_owned()),
                admitted_model_id: Some("gpt-test".to_owned()),
                admitted_runtime_model: Some("gpt-test".to_owned()),
                pricing_subject_id: None,
                pricing_subject_revision_id: None,
                subject_revision_digest: None,
                agent_id: None,
                profile_id: None,
                agent_name_snapshot: None,
                project_name_snapshot: Some("Pricing project".to_owned()),
                executor_type: Some("test".to_owned()),
                backend_kind: Some("fixture".to_owned()),
                provenance_kind: db::PricingAdmissionProvenanceKind::Runtime,
                admitted_at: now.clone(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("invocation creates");
        db::UsageLedgerRepo::start_usage_invocation(
            db,
            db::StartUsageInvocation {
                id: invocation_id.clone(),
                expected_version: 1,
                started_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("invocation starts");

        let (telemetry_state, report_mode, cost_kind, reason) =
            if provider_reported_nano_usd.is_some() {
                (
                    db::UsageTelemetryState::Unmetered,
                    db::UsageEventReportMode::ReportedMoney,
                    db::UsageCostKind::ProviderReported,
                    None,
                )
            } else {
                (
                    db::UsageTelemetryState::Metered,
                    db::UsageEventReportMode::FinalSnapshot,
                    db::UsageCostKind::None,
                    Some(db::CostCoverageReasonCode::MissingBinding),
                )
            };
        let (input_tokens, output_tokens, cache_read_tokens, cache_write_tokens) =
            counters.map_or((None, None, None, None), |counts| {
                (
                    Some(i64::try_from(counts.input).expect("input fits")),
                    Some(i64::try_from(counts.output).expect("output fits")),
                    Some(i64::try_from(counts.cache_read).expect("cache read fits")),
                    Some(i64::try_from(counts.cache_write).expect("cache write fits")),
                )
            });
        let event_id = format!("pricing-event-{suffix}");
        db::UsageLedgerRepo::settle_usage_invocations_with_events(
            db,
            vec![db::UsageLedgerSettlement {
                invocation_id: invocation_id.clone(),
                expected_version: 2,
                telemetry_state,
                terminal_reason: Some("fixture complete".to_owned()),
                settled_at: now.clone(),
                updated_at: now.clone(),
                events: vec![db::CreateUsageEvent {
                    id: event_id.clone(),
                    invocation_id,
                    owner_user_id: Some("pricing-owner".to_owned()),
                    project_id: Some(project_id.to_owned()),
                    surface: db::UsageSurface::TaskExecution,
                    source_id,
                    execution_id: Some(execution_id),
                    task_id: None,
                    event_idempotency_key: format!("event-key-{suffix}"),
                    source_report_id: format!("report-{suffix}"),
                    report_sequence: 0,
                    report_mode,
                    provenance_kind: db::UsageEventProvenanceKind::RuntimeReport,
                    legacy_source_table: None,
                    legacy_source_id: None,
                    legacy_provider_raw: None,
                    legacy_provider_sqlite_type: None,
                    legacy_provider_sql_literal: None,
                    legacy_model_raw: None,
                    legacy_model_sqlite_type: None,
                    legacy_model_sql_literal: None,
                    legacy_counter_values_json: "{}".to_owned(),
                    legacy_cost_usd_raw: None,
                    legacy_created_at_raw: None,
                    legacy_project_owner_raw: None,
                    legacy_invalid_usage: false,
                    provider_id: Some("openai".to_owned()),
                    model_id: Some("gpt-test".to_owned()),
                    runtime_model: Some("gpt-test".to_owned()),
                    candidate_key: Some("candidate-0".to_owned()),
                    attempt_ordinal: 0,
                    agent_id: None,
                    profile_id: None,
                    agent_name_snapshot: None,
                    project_name_snapshot: Some("Pricing project".to_owned()),
                    executor_type: Some("test".to_owned()),
                    pricing_subject_revision_id: None,
                    subject_revision_digest: None,
                    telemetry_state,
                    input_tokens,
                    output_tokens,
                    cache_read_tokens,
                    cache_write_tokens,
                    context_tokens: None,
                    selected_tier: None,
                    provider_reported_nano_usd,
                    legacy_reported_cost_usd: None,
                    estimated_nano_usd: None,
                    cost_kind,
                    rate_revision_id: None,
                    catalog_snapshot_id: None,
                    formula_revision: None,
                    retrospective: false,
                    coverage_reason_code: reason,
                    occurred_at: now.clone(),
                    created_at: now,
                }],
            }],
        )
        .await
        .expect("invocation settles");
        event_id
    }

    #[tokio::test]
    async fn catalog_activation_304_failure_and_lkg_round_trip() {
        let db = database().await;
        let repository = SqlitePricingRepository::new(db.clone());
        let snapshot = snapshot("catalog-snapshot-1", at(10));
        let status = repository
            .activate_catalog_snapshot(snapshot.clone(), "refresh-1")
            .await
            .expect("activation succeeds");
        assert_eq!(status.state, CatalogState::Fresh);
        assert_eq!(
            status.active_snapshot_id.as_deref(),
            Some(snapshot.id.as_str())
        );
        assert_eq!(status.revision, Some(snapshot.revision_digest.clone()));

        let active = repository
            .active_catalog_snapshot()
            .await
            .expect("active snapshot reads")
            .expect("active snapshot exists");
        assert_eq!(active.raw_payload, snapshot.raw_payload);
        assert_eq!(active.models.len(), 1);
        assert_eq!(
            active.models[0].rates.input,
            Some(NanoUsdPerMillion::from_nano_usd(1_000_000_000).unwrap())
        );

        let failed = repository
            .record_catalog_refresh_failure(at(20), CatalogRefreshErrorCode::Transport, "refresh-2")
            .await
            .expect("failure records");
        assert_eq!(failed.state, CatalogState::RefreshFailed);
        assert_eq!(failed.active_snapshot_id, Some(snapshot.id.clone()));
        assert_eq!(failed.last_error_code.as_deref(), Some("transport"));
        assert_eq!(
            repository
                .active_catalog_snapshot()
                .await
                .expect("LKG reads")
                .expect("LKG exists")
                .id,
            snapshot.id
        );

        let refreshed = repository
            .record_catalog_not_modified(at(30), Some("etag-2".to_owned()), "refresh-3")
            .await
            .expect("304 records");
        assert_eq!(refreshed.state, CatalogState::Fresh);
        assert_eq!(refreshed.last_error_code, None);
        assert_eq!(refreshed.active_snapshot_id, Some(snapshot.id));
        assert_eq!(refreshed.etag.as_deref(), Some("etag-2"));
    }

    #[tokio::test]
    async fn catalog_refresh_receipts_replay_original_key_after_a_later_activation() {
        let db = database().await;
        let repository = SqlitePricingRepository::new(db.clone());
        let first = snapshot("catalog-receipt-a", at(10));
        let first_status = repository
            .activate_catalog_snapshot(first.clone(), "refresh-a")
            .await
            .expect("first activation");
        let second = snapshot_variant("catalog-receipt-b", at(20), "gpt-other");
        repository
            .activate_catalog_snapshot(second.clone(), "refresh-b")
            .await
            .expect("second activation");

        let replay_status = repository
            .activate_catalog_snapshot(first, "refresh-a")
            .await
            .expect("first activation replays");
        assert_eq!(replay_status, first_status);
        assert_eq!(
            repository
                .active_catalog_snapshot()
                .await
                .expect("active snapshot reads")
                .expect("active snapshot")
                .id,
            second.id
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM pricing_operation_receipt
                 WHERE operation_scope = 'catalog_refresh'",
            )
            .fetch_one(db.pool())
            .await
            .expect("receipt count"),
            2
        );
    }

    #[tokio::test]
    async fn catalog_client_replays_durable_refresh_outcome_without_transport_after_restart() {
        let db = database().await;
        let repository = Arc::new(SqlitePricingRepository::new(db));
        let transport = Arc::new(CountingTransport::new(vec![
            Ok(ModelsDevHttpResponse::ok(
                catalog_body(),
                Some("etag-a".to_owned()),
            )),
            Ok(ModelsDevHttpResponse::ok(
                snapshot_variant("unused", at(20), "gpt-other").raw_payload,
                Some("etag-b".to_owned()),
            )),
        ]));
        let client = ModelsDevClient::with_transport(repository.clone(), transport.clone());
        let first = client
            .refresh(CatalogRefreshRequest::at("refresh-a", at(10)))
            .await
            .expect("first refresh activates");
        let replayable_first = first.clone();
        let first_snapshot_id = match &first {
            CatalogRefreshOutcome::Activated { snapshot, .. } => snapshot.id.clone(),
            other => panic!("expected activated first refresh, got {other:?}"),
        };

        let second = client
            .refresh(CatalogRefreshRequest::at("refresh-b", at(20)))
            .await
            .expect("second refresh activates");
        assert!(matches!(second, CatalogRefreshOutcome::Activated { .. }));
        assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 2);

        // A fresh client instance must consult the durable receipt before
        // attempting transport, and return the exact original status/snapshot
        // even though a later refresh has advanced the active pointer.
        let restarted = ModelsDevClient::with_transport(repository.clone(), transport.clone());
        let replay = restarted
            .refresh(CatalogRefreshRequest::at(
                "refresh-a",
                at(10 + pricing::MODELS_DEV_STALE_AFTER.as_secs() + 1),
            ))
            .await
            .expect("refresh receipt replays");
        assert_eq!(replay, replayable_first);
        assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 2);
        assert_ne!(
            repository
                .active_catalog_snapshot()
                .await
                .expect("active snapshot reads")
                .expect("active snapshot")
                .id,
            first_snapshot_id
        );

        // A losing key can be durably recorded as a no-op and then replayed
        // without issuing a fetch on a later process/client instance.
        repository
            .record_catalog_refresh_already_refreshed(at(30), "refresh-loser")
            .await
            .expect("losing refresh receipt records");
        let loser = restarted
            .refresh(CatalogRefreshRequest::at("refresh-loser", at(31)))
            .await
            .expect("losing refresh receipt replays");
        assert!(matches!(
            loser,
            CatalogRefreshOutcome::AlreadyRefreshed { .. }
        ));
        assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn transport_error_receipt_persists_only_the_stable_redacted_code() {
        let db = database().await;
        let repository = Arc::new(SqlitePricingRepository::new(db.clone()));
        let sensitive = "GET https://models.dev/api.json bearer=secret-token body=private-payload";
        let transport = Arc::new(CountingTransport::new(vec![Err(
            ModelsDevTransportError::Request(sensitive.to_owned()),
        )]));
        let client = ModelsDevClient::with_transport(repository, transport);
        let result = client
            .refresh(CatalogRefreshRequest::at("transport-redaction", at(10)))
            .await
            .expect("transport failures are represented as a failed outcome");
        assert!(matches!(
            result,
            CatalogRefreshOutcome::Failed {
                code: CatalogRefreshErrorCode::Transport,
                ..
            }
        ));

        let receipt: String = sqlx::query_scalar(
            "SELECT result_json FROM pricing_operation_receipt
             WHERE operation_scope = 'catalog_refresh'
               AND idempotency_key = 'transport-redaction'",
        )
        .fetch_one(db.pool())
        .await
        .expect("transport receipt");
        assert!(receipt.contains("transport"));
        assert!(!receipt.contains("secret-token"));
        assert!(!receipt.contains("private-payload"));
        assert!(!receipt.contains("models.dev/api.json"));
    }

    #[tokio::test]
    async fn concurrent_refresh_loser_gets_durable_noop_and_replays_without_fetch() {
        let db = database().await;
        let inner = Arc::new(SqlitePricingRepository::new(db));
        let repository = Arc::new(SignallingCatalogRepository {
            inner,
            status_calls: AtomicUsize::new(0),
            second_initial_status: Notify::new(),
        });
        let transport = Arc::new(BlockingFirstTransport::new(Ok(ModelsDevHttpResponse::ok(
            catalog_body(),
            Some("etag-concurrent".to_owned()),
        ))));
        let client = ModelsDevClient::with_transport(repository.clone(), transport.clone());

        let first_client = client.clone();
        let first = tokio::spawn(async move {
            first_client
                .refresh(CatalogRefreshRequest::at("refresh-concurrent-a", at(10)))
                .await
        });
        transport.first_started.notified().await;

        let second_client = client.clone();
        let second = tokio::spawn(async move {
            second_client
                .refresh(CatalogRefreshRequest::at("refresh-concurrent-b", at(11)))
                .await
        });
        // Wait until B has captured the pre-A status and is queued behind the
        // single-flight gate before allowing A to commit its snapshot.
        repository.second_initial_status.notified().await;
        transport.release_first.notify_one();

        let first_outcome = first
            .await
            .expect("first refresh task")
            .expect("first refresh");
        assert!(matches!(
            first_outcome,
            CatalogRefreshOutcome::Activated { .. }
        ));
        let second_outcome = second
            .await
            .expect("second refresh task")
            .expect("second refresh");
        assert!(matches!(
            second_outcome,
            CatalogRefreshOutcome::AlreadyRefreshed { .. }
        ));
        assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 1);

        let restarted = ModelsDevClient::with_transport(repository, transport.clone());
        let replay = restarted
            .refresh(CatalogRefreshRequest::at("refresh-concurrent-b", at(999)))
            .await
            .expect("losing refresh replays after restart");
        assert_eq!(replay, second_outcome);
        assert_eq!(transport.fetch_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn catalog_activation_rolls_back_snapshot_and_rates_on_failure() {
        let db = database().await;
        let repository = SqlitePricingRepository::new(db.clone());
        let mut invalid = snapshot("catalog-invalid", at(10));
        invalid.models[0].provider_id.clear();
        let error = repository
            .activate_catalog_snapshot(invalid, "refresh-invalid")
            .await
            .expect_err("invalid normalized row is rejected");
        assert!(matches!(error, CatalogRepositoryError::Unavailable(_)));
        assert!(db
            .get_pricing_catalog_snapshot("catalog-invalid")
            .await
            .expect("snapshot lookup")
            .is_none());
        assert!(db
            .get_pricing_catalog_state()
            .await
            .expect("state lookup")
            .is_none());
        let rate_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pricing_rate_revision")
            .fetch_one(db.pool())
            .await
            .expect("rate count");
        assert_eq!(rate_count, 0);
    }

    #[tokio::test]
    async fn bindings_create_immutable_manual_revision_and_retire_exactly() {
        let db = database().await;
        let (subject_id, _revision_id, revision_digest) = subject_fixture(&db).await;
        let repository = SqlitePricingRepository::new(db.clone());
        let rates = EventBucketRates::new(
            Some(NanoUsdPerMillion::from_nano_usd(3_000_000_000).unwrap()),
            Some(NanoUsdPerMillion::ZERO),
            None,
            None,
        );
        let request = ReplacePricingRequest {
            expected_version: 2,
            idempotency_key: "binding-1".to_owned(),
            subject_revision_digest: revision_digest.clone(),
            bindings: vec![DesiredPricingBinding::manual("custom-model", rates)],
        };
        let configured = repository
            .replace_pricing_configuration(&subject_id, request.clone(), at(200))
            .await
            .expect("manual binding persists");
        let selected = configured.resolve("custom-model").expect("exact binding");
        assert!(matches!(selected, PricingBindingSource::Manual(_)));
        let manual_rate_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pricing_rate_revision WHERE source_kind = 'manual_override'",
        )
        .fetch_one(db.pool())
        .await
        .expect("manual rate count");
        assert_eq!(manual_rate_count, 1);

        let configured_b = repository
            .replace_pricing_configuration(
                &subject_id,
                ReplacePricingRequest {
                    expected_version: configured.version,
                    idempotency_key: "binding-2".to_owned(),
                    subject_revision_digest: revision_digest.clone(),
                    bindings: vec![DesiredPricingBinding::manual(
                        "custom-model",
                        EventBucketRates::new(
                            Some(NanoUsdPerMillion::from_nano_usd(4_000_000_000).unwrap()),
                            Some(NanoUsdPerMillion::ZERO),
                            None,
                            None,
                        ),
                    )],
                },
                at(250),
            )
            .await
            .expect("second manual binding persists");
        assert_ne!(configured_b, configured);

        // The original request is replayable even after a later mutation
        // advanced the mutable subject version.
        let replay = repository
            .replace_pricing_configuration(&subject_id, request, at(999))
            .await
            .expect("same request replays");
        assert_eq!(replay, configured);
        let conflicting = repository
            .replace_pricing_configuration(
                &subject_id,
                ReplacePricingRequest {
                    expected_version: configured_b.version,
                    idempotency_key: "binding-1".to_owned(),
                    subject_revision_digest: revision_digest.clone(),
                    bindings: vec![DesiredPricingBinding::manual(
                        "custom-model",
                        EventBucketRates::new(Some(NanoUsdPerMillion::ZERO), None, None, None),
                    )],
                },
                at(300),
            )
            .await
            .expect_err("same key with another payload conflicts");
        assert!(matches!(conflicting, CatalogRepositoryError::Conflict(_)));

        let retired = repository
            .replace_pricing_configuration(
                &subject_id,
                ReplacePricingRequest {
                    expected_version: configured_b.version,
                    idempotency_key: "binding-retire".to_owned(),
                    subject_revision_digest: revision_digest,
                    bindings: Vec::new(),
                },
                at(400),
            )
            .await
            .expect("binding retires");
        assert!(retired.resolve("custom-model").is_none());
        let states: Vec<String> =
            sqlx::query_scalar("SELECT state FROM pricing_subject_binding WHERE subject_id = ?")
                .bind(&subject_id)
                .fetch_all(db.pool())
                .await
                .expect("binding states");
        assert_eq!(states, vec!["retired", "retired"]);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM pricing_rate_revision WHERE source_kind = 'manual_override'",
            )
            .fetch_one(db.pool())
            .await
            .expect("immutable rate remains"),
            2
        );
    }

    #[tokio::test]
    async fn same_manual_request_key_and_identity_are_independent_per_subject() {
        let db = database().await;
        let (first_subject, _first_revision, first_digest) = subject_fixture(&db).await;
        let (second_subject, _second_revision, second_digest) = second_subject_fixture(&db).await;
        assert_eq!(first_digest, second_digest);
        let repository = SqlitePricingRepository::new(db.clone());
        let rates = EventBucketRates::new(
            Some(NanoUsdPerMillion::from_nano_usd(3_000_000_000).unwrap()),
            Some(NanoUsdPerMillion::ZERO),
            None,
            None,
        );
        let replace = |digest: String| ReplacePricingRequest {
            expected_version: 2,
            idempotency_key: "same-client-key".to_owned(),
            subject_revision_digest: digest,
            bindings: vec![DesiredPricingBinding::manual("same-model", rates)],
        };
        let first = repository
            .replace_pricing_configuration(&first_subject, replace(first_digest), at(200))
            .await
            .expect("first subject configures");
        let second = repository
            .replace_pricing_configuration(&second_subject, replace(second_digest), at(200))
            .await
            .expect("second subject configures with the same key");

        let first_manual = match first.resolve("same-model").expect("first manual") {
            PricingBindingSource::Manual(override_) => override_,
            PricingBindingSource::ModelsDev(_) => panic!("expected manual source"),
        };
        let second_manual = match second.resolve("same-model").expect("second manual") {
            PricingBindingSource::Manual(override_) => override_,
            PricingBindingSource::ModelsDev(_) => panic!("expected manual source"),
        };
        assert_ne!(
            first_manual.rate_revision_id,
            second_manual.rate_revision_id
        );
        assert_ne!(first_manual.id, second_manual.id);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM pricing_operation_receipt
                 WHERE operation_scope LIKE 'pricing_configuration:%'
                   AND idempotency_key = 'same-client-key'",
            )
            .fetch_one(db.pool())
            .await
            .expect("scoped receipt count"),
            2
        );
    }

    #[tokio::test]
    async fn retired_subject_rejects_configuration_before_receipt_or_binding_write() {
        let db = database().await;
        let (subject_id, _revision_id, revision_digest) = subject_fixture(&db).await;
        PricingSubjectRepo::update_pricing_subject(
            &*db,
            db::UpdatePricingSubject {
                id: subject_id.clone(),
                expected_version: 2,
                current_revision_id: None,
                state: Some(PricingSubjectState::Retired),
                last_idempotency_key: None,
                last_update_digest: None,
                updated_at: db_time(at(150)),
            },
        )
        .await
        .expect("subject retires");

        let repository = SqlitePricingRepository::new(db.clone());
        let error = repository
            .replace_pricing_configuration(
                &subject_id,
                ReplacePricingRequest {
                    expected_version: 2,
                    idempotency_key: "retired-subject-replace".to_owned(),
                    subject_revision_digest: revision_digest,
                    bindings: vec![DesiredPricingBinding::manual(
                        "custom-model",
                        EventBucketRates::new(Some(NanoUsdPerMillion::ZERO), None, None, None),
                    )],
                },
                at(200),
            )
            .await
            .expect_err("retired subjects cannot be configured");
        assert!(matches!(
            error,
            CatalogRepositoryError::Conflict(message)
                if message.contains("subject is retired")
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pricing_operation_receipt")
                .fetch_one(db.pool())
                .await
                .expect("receipt count"),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pricing_subject_binding")
                .fetch_one(db.pool())
                .await
                .expect("binding count"),
            0
        );
    }

    #[tokio::test]
    async fn catalog_binding_reconstructs_exact_rate_and_manual_replacement_takes_precedence() {
        let db = database().await;
        let (subject_id, _revision_id, revision_digest) = subject_fixture(&db).await;
        let repository = SqlitePricingRepository::new(db.clone());
        let snapshot = snapshot("catalog-binding", at(10));
        repository
            .activate_catalog_snapshot(snapshot.clone(), "catalog-binding-refresh")
            .await
            .expect("catalog activates");
        let model = snapshot
            .model_rate("openai", "gpt-test")
            .expect("model exists");
        let rate = PricingCatalogRepo::get_catalog_rate_revision(
            &*db,
            &snapshot.id,
            &model.provider_id,
            &model.model_id,
        )
        .await
        .expect("catalog rate lookup")
        .expect("catalog rate exists");
        let configured = repository
            .replace_pricing_configuration(
                &subject_id,
                ReplacePricingRequest {
                    expected_version: 2,
                    idempotency_key: "catalog-binding-1".to_owned(),
                    subject_revision_digest: revision_digest.clone(),
                    bindings: vec![DesiredPricingBinding::models_dev(
                        "runtime-gpt",
                        model.provider_id.clone(),
                        model.model_id.clone(),
                        snapshot.id.clone(),
                        rate.id.clone(),
                        model.rates,
                        model.tiers.clone(),
                        model.legacy_context_over_200k.clone(),
                        CatalogFreshness::Fresh,
                    )],
                },
                at(200),
            )
            .await
            .expect("catalog binding persists");
        let catalog = configured.resolve("runtime-gpt").expect("catalog resolves");
        let PricingBindingSource::ModelsDev(catalog) = catalog else {
            panic!("expected catalog source");
        };
        assert_eq!(catalog.provider_id, "openai");
        assert_eq!(catalog.model_id, "gpt-test");
        assert_eq!(catalog.snapshot_id, snapshot.id);
        assert_eq!(catalog.rate_revision_id, rate.id);
        assert_eq!(catalog.rates, model.rates);
        assert_eq!(catalog.tiers, model.tiers);
        assert_eq!(
            catalog.legacy_context_over_200k,
            model.legacy_context_over_200k
        );

        let reloaded = repository
            .pricing_configuration(&subject_id)
            .await
            .expect("catalog binding reloads");
        assert_eq!(reloaded.subject_id, configured.subject_id);
        assert_eq!(
            reloaded.subject_revision_digest,
            configured.subject_revision_digest
        );
        assert_eq!(reloaded.version, configured.version);
        let PricingBindingSource::ModelsDev(reloaded_catalog) = reloaded
            .resolve("runtime-gpt")
            .expect("reloaded catalog resolves")
        else {
            panic!("reloaded source should remain catalog");
        };
        assert_eq!(reloaded_catalog.id, catalog.id);
        assert_eq!(reloaded_catalog.rate_revision_id, catalog.rate_revision_id);
        assert_eq!(reloaded_catalog.snapshot_id, catalog.snapshot_id);
        assert_eq!(reloaded_catalog.rates, catalog.rates);
        assert_eq!(reloaded_catalog.tiers, catalog.tiers);

        let manual = repository
            .replace_pricing_configuration(
                &subject_id,
                ReplacePricingRequest {
                    expected_version: configured.version,
                    idempotency_key: "manual-precedence-1".to_owned(),
                    subject_revision_digest: revision_digest,
                    bindings: vec![DesiredPricingBinding::manual(
                        "runtime-gpt",
                        EventBucketRates::new(
                            Some(NanoUsdPerMillion::from_nano_usd(9_000_000_000).unwrap()),
                            Some(NanoUsdPerMillion::ZERO),
                            None,
                            None,
                        ),
                    )],
                },
                at(300),
            )
            .await
            .expect("manual replacement persists");
        assert!(matches!(
            manual.resolve("runtime-gpt"),
            Some(PricingBindingSource::Manual(_))
        ));
        let states: Vec<(String, String)> = sqlx::query_as(
            "SELECT source_kind, state FROM pricing_subject_binding
             WHERE subject_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(&subject_id)
        .fetch_all(db.pool())
        .await
        .expect("binding history");
        assert_eq!(
            states,
            vec![
                ("models_dev_catalog".to_owned(), "active".to_owned()),
                ("manual_override".to_owned(), "active".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn reviewed_alias_is_applied_by_admission_only_for_exact_provider_identity() {
        let db = database().await;
        let (subject_id, _revision_id, _revision_digest) = subject_fixture(&db).await;
        let repository = SqlitePricingRepository::new(db.clone());
        let snapshot = snapshot("catalog-reviewed-alias", at(10));
        repository
            .activate_catalog_snapshot(snapshot, "catalog-reviewed-alias-refresh")
            .await
            .expect("catalog activates");

        let mut transaction = db::begin_immediate(db.pool()).await.expect("transaction");
        assert!(ensure_reviewed_provider_binding_in_tx(
            &db,
            &mut transaction,
            &subject_id,
            "gpt-test",
            at(20),
        )
        .await
        .expect("reviewed alias resolves"));
        transaction.commit().await.expect("transaction commits");

        let configuration = repository
            .pricing_configuration(&subject_id)
            .await
            .expect("configuration reads");
        let Some(PricingBindingSource::ModelsDev(binding)) = configuration.resolve("gpt-test")
        else {
            panic!("reviewed alias should install a catalog binding");
        };
        assert_eq!(binding.provider_id, "openai");
        assert_eq!(binding.model_id, "gpt-test");
    }

    #[tokio::test]
    async fn automatic_alias_advances_but_explicit_catalog_and_manual_sources_remain_frozen() {
        let db = database().await;
        let (subject_id, _revision_id, _revision_digest) = subject_fixture(&db).await;
        let repository = SqlitePricingRepository::new(db.clone());
        let first = snapshot("catalog-auto-a", at(10));
        repository
            .activate_catalog_snapshot(first.clone(), "catalog-auto-refresh-a")
            .await
            .expect("first catalog activates");

        let mut transaction = db::begin_immediate(db.pool()).await.expect("transaction");
        assert!(ensure_reviewed_provider_binding_in_tx(
            &db,
            &mut transaction,
            &subject_id,
            "gpt-test",
            at(10),
        )
        .await
        .expect("automatic A binding"));
        transaction.commit().await.expect("automatic A commits");
        let automatic_a = repository
            .pricing_configuration(&subject_id)
            .await
            .expect("automatic A reads");
        let PricingBindingSource::ModelsDev(automatic_a) = automatic_a
            .resolve("gpt-test")
            .expect("automatic A resolves")
        else {
            panic!("automatic A should be a catalog source");
        };
        assert_eq!(automatic_a.snapshot_id, first.id);

        let second = snapshot_rate_variant("catalog-auto-b", at(20), "3");
        repository
            .activate_catalog_snapshot(second.clone(), "catalog-auto-refresh-b")
            .await
            .expect("second catalog activates");
        let mut transaction = db::begin_immediate(db.pool()).await.expect("transaction");
        assert!(ensure_reviewed_provider_binding_in_tx(
            &db,
            &mut transaction,
            &subject_id,
            "gpt-test",
            at(20),
        )
        .await
        .expect("automatic B binding"));
        transaction.commit().await.expect("automatic B commits");
        let automatic_b = repository
            .pricing_configuration(&subject_id)
            .await
            .expect("automatic B reads");
        let PricingBindingSource::ModelsDev(automatic_b) = automatic_b
            .resolve("gpt-test")
            .expect("automatic B resolves")
        else {
            panic!("automatic B should be a catalog source");
        };
        assert_eq!(automatic_b.snapshot_id, second.id);
        assert_ne!(automatic_b.rate_revision_id, automatic_a.rate_revision_id);

        // An explicit catalog binding is not mistaken for the deterministic
        // automatic row and remains pinned to snapshot A after B activates.
        let explicit_revision_digest =
            pricing::pricing_subject_revision_digest(&pricing::PricingSubjectIdentity {
                subject_kind: "provider_entry".to_owned(),
                provider_kind: "openai".to_owned(),
                credential_method: "api_key".to_owned(),
                endpoint_class: "https://api.openai.com/v1".to_owned(),
                runtime_fingerprint: None,
                schema_revision: "pricing-subject-explicit-v1".to_owned(),
            });
        let explicit_subject = {
            let now = db_time(at(100));
            let id = "pricing-subject-explicit".to_owned();
            // V135 currently makes revision digests globally unique.  Use a
            // distinct reviewed schema revision for this second subject so
            // the fixture does not collide with `subject_fixture`; the
            // production schema should scope this identity by subject.
            PricingSubjectRepo::create_pricing_subject(
                &*db,
                CreatePricingSubject {
                    id: id.clone(),
                    owner_user_id: "pricing-owner".to_owned(),
                    subject_kind: PricingSubjectKind::ProviderEntry,
                    provider_entry_id: Some("provider-entry-explicit".to_owned()),
                    daemon_id: None,
                    executor_type: None,
                    current_revision_id: None,
                    state: PricingSubjectState::Active,
                    last_idempotency_key: None,
                    last_update_digest: None,
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
            )
            .await
            .expect("explicit subject creates");
            let revision_id = "pricing-subject-explicit-revision".to_owned();
            PricingSubjectRepo::create_pricing_subject_revision(
                &*db,
                CreatePricingSubjectRevision {
                    id: revision_id.clone(),
                    subject_id: id.clone(),
                    owner_user_id: "pricing-owner".to_owned(),
                    revision: 1,
                    revision_digest: explicit_revision_digest.clone(),
                    subject_kind: PricingSubjectKind::ProviderEntry,
                    provider_entry_id: Some("provider-entry-explicit".to_owned()),
                    daemon_id: None,
                    executor_type: None,
                    provider_kind: "openai".to_owned(),
                    credential_method: "api_key".to_owned(),
                    endpoint_class: "https://api.openai.com/v1".to_owned(),
                    runtime_fingerprint: None,
                    schema_revision: "pricing-subject-explicit-v1".to_owned(),
                    non_secret_identity_json: "{}".to_owned(),
                    created_at: now.clone(),
                },
            )
            .await
            .expect("explicit revision creates");
            PricingSubjectRepo::update_pricing_subject(
                &*db,
                db::UpdatePricingSubject {
                    id: id.clone(),
                    expected_version: 1,
                    current_revision_id: Some(Some(revision_id)),
                    state: None,
                    last_idempotency_key: None,
                    last_update_digest: None,
                    updated_at: now,
                },
            )
            .await
            .expect("explicit subject points at revision");
            id
        };
        let first_model = first.model_rate("openai", "gpt-test").expect("A model");
        let first_rate =
            PricingCatalogRepo::get_catalog_rate_revision(&*db, &first.id, "openai", "gpt-test")
                .await
                .expect("A rate lookup")
                .expect("A rate");
        repository
            .replace_pricing_configuration(
                &explicit_subject,
                ReplacePricingRequest {
                    expected_version: 2,
                    idempotency_key: "explicit-a".to_owned(),
                    subject_revision_digest: explicit_revision_digest,
                    bindings: vec![DesiredPricingBinding::models_dev(
                        "gpt-test",
                        "openai",
                        "gpt-test",
                        first.id.clone(),
                        first_rate.id.clone(),
                        first_model.rates,
                        first_model.tiers.clone(),
                        first_model.legacy_context_over_200k.clone(),
                        CatalogFreshness::Fresh,
                    )],
                },
                at(30),
            )
            .await
            .expect("explicit A persists");
        let mut transaction = db::begin_immediate(db.pool()).await.expect("transaction");
        assert!(ensure_reviewed_provider_binding_in_tx(
            &db,
            &mut transaction,
            &explicit_subject,
            "gpt-test",
            at(30),
        )
        .await
        .expect("explicit catalog remains accepted"));
        transaction
            .commit()
            .await
            .expect("explicit transaction commits");
        let explicit = repository
            .pricing_configuration(&explicit_subject)
            .await
            .expect("explicit config reads");
        let PricingBindingSource::ModelsDev(explicit) = explicit
            .resolve("gpt-test")
            .expect("explicit catalog resolves")
        else {
            panic!("explicit source should remain catalog");
        };
        assert_eq!(explicit.snapshot_id, first.id);
    }

    #[tokio::test]
    async fn retrospective_commit_reconstructs_snapshot_rates_and_replays_without_touching_reported_cost(
    ) {
        let db = database().await;
        let _ = subject_fixture(&db).await;
        let project_id = project_fixture(&db).await;
        let repository = SqlitePricingRepository::new(db.clone());
        let snapshot = snapshot("catalog-retrospective", at(10));
        repository
            .activate_catalog_snapshot(snapshot.clone(), "catalog-retrospective-refresh")
            .await
            .expect("catalog activates");
        let stored_rate =
            PricingCatalogRepo::get_catalog_rate_revision(&*db, &snapshot.id, "openai", "gpt-test")
                .await
                .expect("catalog rate lookup")
                .expect("catalog rate exists");
        assert_eq!(stored_rate.id, snapshot.models[0].rate_digest(&snapshot.id));

        let estimated_event = usage_event_fixture(
            &db,
            &project_id,
            "retrospective-estimated",
            Some(EventTokenCounts::new(1, 0, 0, 0)),
            None,
        )
        .await;
        let reported_event =
            usage_event_fixture(&db, &project_id, "retrospective-reported", None, Some(42)).await;
        let preview = preview_retrospective_estimates(
            project_id.clone(),
            &snapshot,
            &[
                RetrospectiveUsageEvent {
                    event_id: estimated_event.clone(),
                    provider_id: Some("openai".to_owned()),
                    model_id: Some("gpt-test".to_owned()),
                    counters: Some(EventTokenCounts::new(1, 0, 0, 0)),
                    provider_reported_amount: None,
                    context_tokens: None,
                    occurred_at: at(100),
                },
                RetrospectiveUsageEvent {
                    event_id: reported_event.clone(),
                    provider_id: Some("openai".to_owned()),
                    model_id: Some("gpt-test".to_owned()),
                    counters: None,
                    provider_reported_amount: Some(NanoUsd::from_nano_usd(42).unwrap()),
                    context_tokens: None,
                    occurred_at: at(100),
                },
            ],
            at(110),
            Duration::from_secs(1_000),
        )
        .expect("preview builds");
        assert_eq!(preview.eligible_event_count, 1);
        assert_eq!(preview.unmatched_event_count, 0);
        assert_eq!(preview.already_reported_event_count, 1);
        assert_eq!(
            preview.projected_cost,
            Some(NanoUsd::from_nano_usd(1_000).unwrap())
        );

        let request = RetrospectiveCommitRequest {
            preview_id: preview.id.clone(),
            usage_set_digest: preview.usage_set_digest.clone(),
            idempotency_key: "retrospective-commit-1".to_owned(),
        };
        let mut tampered_preview = preview.clone();
        tampered_preview.eligible_events[0].amount = NanoUsd::ZERO;
        assert!(matches!(
            repository
                .commit_retrospective_preview(tampered_preview, request.clone(), at(111),)
                .await,
            Err(RetrospectiveRepositoryError::UsageSetConflict)
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM cost_estimation_run")
                .fetch_one(db.pool())
                .await
                .expect("run count after rejected preview"),
            0
        );
        let committed = repository
            .commit_retrospective_preview(preview.clone(), request.clone(), at(111))
            .await
            .expect("retrospective commit");
        assert_eq!(committed.project_id, project_id);
        assert_eq!(committed.applied_event_count, 1);
        assert_eq!(committed.unmatched_event_count, 0);
        assert_eq!(committed.cost, preview.projected_cost);
        let stored_preview =
            db::RetrospectiveEstimateRepo::get_cost_estimation_preview(&*db, &preview.id)
                .await
                .expect("preview lookup")
                .expect("preview exists");
        assert_eq!(
            stored_preview.catalog_freshness,
            db::PricingCatalogFreshness::Fresh
        );

        let revisions = db::RetrospectiveEstimateRepo::list_cost_estimate_revisions_for_run(
            &*db,
            &committed.id,
        )
        .await
        .expect("estimate revisions list");
        assert_eq!(revisions.len(), 1);
        assert_eq!(revisions[0].usage_event_id, estimated_event);
        assert_eq!(revisions[0].state, db::CostEstimateRevisionState::Applied);
        assert_eq!(revisions[0].rate_revision_id, Some(stored_rate.id));
        assert_eq!(revisions[0].catalog_snapshot_id, Some(snapshot.id.clone()));
        assert_eq!(revisions[0].estimated_nano_usd, Some(1_000));
        assert_eq!(
            revisions[0].formula_revision.as_deref(),
            Some(pricing::COST_FORMULA_REVISION)
        );

        let reported = db::UsageLedgerRepo::get_usage_event(&*db, &reported_event)
            .await
            .expect("reported event lookup")
            .expect("reported event exists");
        assert_eq!(reported.provider_reported_nano_usd, Some(42));
        assert_eq!(reported.estimated_nano_usd, None);
        assert_eq!(reported.cost_kind, db::UsageCostKind::ProviderReported);

        let replay = repository
            .commit_retrospective_preview(preview, request, at(999))
            .await
            .expect("idempotent replay");
        assert_eq!(replay, committed);
        assert_eq!(
            db::RetrospectiveEstimateRepo::list_cost_estimate_revisions_for_run(
                &*db,
                &committed.id,
            )
            .await
            .expect("revisions remain immutable")
            .len(),
            1
        );
    }
}
