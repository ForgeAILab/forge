//! SQLite adapters for the pricing service-domain boundaries.
//!
//! The pricing domain deliberately does not depend on a concrete store.  This
//! module is the explicit conversion layer for the V135 SQLite repositories:
//! immutable catalog snapshots/rate revisions, exact subject bindings, and
//! retrospective estimate envelopes/revisions.  All multi-row writes use the
//! public transaction forms exposed by `db`; a failed conversion or CAS drops
//! the transaction and therefore leaves the previous last-known-good state
//! untouched.

use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, Transaction};

use crate::{
    self as pricing, CatalogFreshness, CatalogModelRate, CatalogRefreshErrorCode,
    CatalogRefreshOutcome, CatalogRepositoryError, CatalogSnapshot, CatalogState, CatalogStatus,
    ContextTierState, EventBucketRates, EventTokenCounts, NanoUsd, NanoUsdPerMillion,
    PriceSelectionReasonCode, PricingCatalogRepository, RetrospectiveCommitRequest,
    RetrospectiveEstimateCandidate, RetrospectiveEstimateRepository, RetrospectivePreview,
    RetrospectiveRepositoryError, RetrospectiveRun, RetrospectiveUnmatchedEvent,
    RetrospectiveUsageEvent,
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

fn catalog_refresh_request_digest() -> String {
    // A refresh has no caller-supplied payload. Its idempotency key is the
    // operation identity; server timestamps, conditional response headers,
    // and the eventual outcome must not turn a retry into a new request.
    structured_digest("catalog-refresh-request-v2", [None])
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

pub async fn catalog_freshness_for_snapshot_in_tx(
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

mod catalog;
mod retrospective;

fn catalog_state_name(state: CatalogState) -> &'static str {
    match state {
        CatalogState::Absent => "absent",
        CatalogState::Fresh => "fresh",
        CatalogState::Stale => "stale",
        CatalogState::RefreshFailed => "refresh_failed",
    }
}

/// Fixtures shared by this crate's tests and by `services` tests that
/// exercise pricing rows through the usage projection.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
mod tests;
