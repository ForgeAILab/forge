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

use crate::pricing::{
    self, CatalogFreshness, CatalogModelRate, CatalogRefreshErrorCode, CatalogRefreshOutcome,
    CatalogRepositoryError, CatalogSnapshot, CatalogState, CatalogStatus, ContextTierState,
    EventBucketRates, EventTokenCounts, NanoUsd, NanoUsdPerMillion, PriceSelectionReasonCode,
    PricingCatalogRepository, RetrospectiveCommitRequest, RetrospectiveEstimateCandidate,
    RetrospectiveEstimateRepository, RetrospectivePreview, RetrospectiveRepositoryError,
    RetrospectiveRun, RetrospectiveUnmatchedEvent, RetrospectiveUsageEvent,
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
        CatalogRefreshRequest, EventTokenCounts, ModelsDevClient, ModelsDevHttpResponse,
        ModelsDevTransport, ModelsDevTransportError, NanoUsdPerMillion, RetrospectiveUsageEvent,
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

    const FIXTURE_AGENT_ID: &str = "pricing-agent";

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
                agent_id: Some(FIXTURE_AGENT_ID.to_owned()),
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
                    agent_id: Some(FIXTURE_AGENT_ID.to_owned()),
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

        // The public projection overlays the applied revision onto the
        // immutable event, including the preview's frozen provenance.
        let breakdowns = crate::usage_projection::usage_breakdowns_for_source(
            &db,
            "execution-retrospective-estimated",
        )
        .await
        .expect("breakdowns project");
        assert_eq!(breakdowns.len(), 1);
        let cost = &breakdowns[0].cost;
        assert_eq!(cost.kind, api_types::CostKind::Estimated);
        assert_eq!(cost.sources.len(), 1);
        assert!(cost.sources[0].retrospective);
        assert_eq!(
            cost.sources[0].rate_revision_id,
            revisions[0].rate_revision_id
        );
        assert_eq!(
            cost.sources[0].catalog_snapshot_id.as_deref(),
            Some(snapshot.id.as_str())
        );
        assert_eq!(
            cost.sources[0].freshness,
            api_types::CostSourceFreshness::Fresh
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

    #[tokio::test]
    async fn agent_usage_cache_reuses_until_the_ledger_moves() {
        let db = database().await;
        let _ = subject_fixture(&db).await;
        let project_id = project_fixture(&db).await;
        usage_event_fixture(
            &db,
            &project_id,
            "cache-first",
            Some(EventTokenCounts::new(1, 0, 0, 0)),
            None,
        )
        .await;
        let cache = crate::usage_projection::AgentUsageAggregateCache::default();
        let first = cache.get(&db, FIXTURE_AGENT_ID).await.expect("aggregate");
        assert_eq!(first.tokens.input_tokens, 1);

        // An unchanged ledger is served from the cache, not recomputed.
        let mut sentinel = first.clone();
        sentinel.tokens.input_tokens = 999;
        cache.replace_cached_aggregate(FIXTURE_AGENT_ID, sentinel);
        assert_eq!(
            cache
                .get(&db, FIXTURE_AGENT_ID)
                .await
                .expect("hit")
                .tokens
                .input_tokens,
            999
        );

        // A new event moves the fingerprint and forces a recompute.
        usage_event_fixture(
            &db,
            &project_id,
            "cache-second",
            Some(EventTokenCounts::new(2, 0, 0, 0)),
            None,
        )
        .await;
        assert_eq!(
            cache
                .get(&db, FIXTURE_AGENT_ID)
                .await
                .expect("miss")
                .tokens
                .input_tokens,
            3
        );
    }
}
