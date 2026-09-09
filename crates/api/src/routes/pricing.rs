//! REST boundary for provider pricing, the models.dev catalog, and
//! retrospective cost estimation.
//!
//! The handlers in this module deliberately contain no SQL.  They resolve
//! visibility through the existing repositories, translate wire contracts to
//! the fixed-point pricing domain, and leave atomic mutation/idempotency work
//! to `services::pricing` and `services::pricing_db`.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::{Duration, SystemTime},
};

use api_types::{
    CatalogModelRate, CatalogModelSourceKind, CostCoverage, CostCoverageReason,
    CostCoverageReasonCode, CostEstimationPreview as ApiCostEstimationPreview,
    CostEstimationRun as ApiCostEstimationRun, CostEstimationRunStatus as ApiRunStatus, CostKind,
    CostSourceFreshness, CostSourceKind, CostSourceRef, CostSummary,
    CreateCostEstimationPreviewRequest, CreateCostEstimationRunRequest, MoneyAmount,
    PricingBinding as ApiPricingBinding, PricingBindingSourceKind, PricingCatalogModelsQuery,
    PricingCatalogModelsResponse, PricingCatalogRefreshRequest, PricingCatalogState,
    PricingCatalogStatus, ProviderPricing, RateAmount, RateBuckets as ApiRateBuckets,
    ReplaceProviderPricingBinding, ReplaceProviderPricingRequest, TokenCounters, UsageCostCoverage,
};
use axum::{
    extract::{Path, Query, State},
    http::{
        header::{ETAG, IF_NONE_MATCH},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
};
use chrono::{DateTime, SecondsFormat, Utc};
use db::{
    new_uuid_v4, now_rfc3339, CostEstimationPreview as DbCostEstimationPreview,
    CostEstimationRun as DbCostEstimationRun, CredentialHandle, CredentialHandleRepo, Daemon,
    DaemonRepo, DbError, PageRequest, PricingCatalogModelQuery as DbCatalogModelQuery,
    PricingCatalogRepo, PricingRateSourceKind, PricingSubject, PricingSubjectKind,
    PricingSubjectRepo, PricingSubjectState, Project, ProjectRepo, RetrospectiveEstimateRepo,
    SortBy, SortOrder, UsageEvent, UsageLedgerRepo,
};
use serde_json::{json, Value};
use services::{
    pricing::{
        self, CatalogClientError, CatalogFreshness, CatalogSnapshot, CatalogStatus,
        DesiredPricingBinding, EventBucketRates, EventTokenCounts, NanoUsd, NanoUsdPerMillion,
        PriceSelectionReasonCode, PricingBindingRepository, PricingBindingSource,
        PricingConfiguration, ReplacePricingRequest, RetrospectiveCommitRequest,
        RetrospectivePreview, RetrospectiveRepositoryError, RetrospectiveUsageEvent,
    },
    pricing_db::SqlitePricingRepository,
};

use crate::{
    errors::{ApiError, ApiResult},
    json::Json,
    routes::auth::AuthenticatedUser,
    routes::scoped_idempotency_key,
    state::AppState,
};

const MAX_QUERY_TEXT_BYTES: usize = 256;
const MAX_CURSOR_BYTES: usize = 2_048;
const MAX_BINDINGS: usize = 1_000;
const PREVIEW_TTL: Duration = Duration::from_secs(24 * 60 * 60);

// -------------------------------------------------------------------------
// Catalog status/refresh/listing
// -------------------------------------------------------------------------

pub async fn get_pricing_catalog_status(
    State(state): State<AppState>,
    _user: AuthenticatedUser,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let status = state
        .models_dev_client
        .status_at(SystemTime::now())
        .await
        .map_err(map_catalog_client_error)?;
    let snapshot = load_snapshot(
        &state.pricing_repository,
        status.active_snapshot_id.as_deref(),
    )
    .await?;
    let response = catalog_status_response(&status, snapshot.as_ref(), headers.get(IF_NONE_MATCH));
    Ok(response)
}

pub async fn refresh_pricing_catalog(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Json(request): Json<PricingCatalogRefreshRequest>,
) -> ApiResult<Response> {
    // The catalog is account-visible, but its immutable snapshots are shared
    // process state. Keep the client key intact so retries across authorized
    // sessions converge on the same single-flight receipt.
    let _ = user;
    let outcome = state
        .models_dev_client
        .refresh(pricing::CatalogRefreshRequest::new(request.idempotency_key))
        .await
        .map_err(map_catalog_client_error)?;
    let (status, snapshot) = match outcome {
        pricing::CatalogRefreshOutcome::Activated { snapshot, status } => (status, Some(*snapshot)),
        pricing::CatalogRefreshOutcome::NotModified { status }
        | pricing::CatalogRefreshOutcome::AlreadyRefreshed { status }
        | pricing::CatalogRefreshOutcome::Failed { status, .. } => {
            let snapshot = load_snapshot(
                &state.pricing_repository,
                status.active_snapshot_id.as_deref(),
            )
            .await?;
            (status, snapshot)
        }
    };
    Ok(catalog_status_response(&status, snapshot.as_ref(), None))
}

pub async fn list_pricing_catalog_models(
    State(state): State<AppState>,
    _user: AuthenticatedUser,
    Query(query): Query<PricingCatalogModelsQuery>,
) -> ApiResult<Json<PricingCatalogModelsResponse>> {
    validate_catalog_query(&query)?;
    let limit = query.limit.unwrap_or(20);
    let status = state
        .models_dev_client
        .status_at(SystemTime::now())
        .await
        .map_err(map_catalog_client_error)?;
    let Some(snapshot_id) = status.active_snapshot_id else {
        return Ok(Json(PricingCatalogModelsResponse {
            items: Vec::new(),
            has_more: false,
            next_cursor: None,
        }));
    };
    let page = PricingCatalogRepo::list_pricing_catalog_models(
        &*state.db,
        DbCatalogModelQuery {
            page: PageRequest {
                cursor: query.cursor,
                limit,
                include_total: false,
                sort_by: SortBy::Id,
                sort_order: SortOrder::Asc,
            },
            provider_id: query.provider_id,
            query: query.query,
            snapshot_id: Some(snapshot_id),
        },
    )
    .await
    .map_err(map_db_read_error)?;
    let items = page
        .items
        .into_iter()
        .map(catalog_model_response)
        .collect::<ApiResult<Vec<_>>>()?;
    Ok(Json(PricingCatalogModelsResponse {
        has_more: page.next_cursor.is_some(),
        next_cursor: page.next_cursor,
        items,
    }))
}

fn validate_catalog_query(query: &PricingCatalogModelsQuery) -> ApiResult<()> {
    if let Some(limit) = query.limit {
        if !(1..=100).contains(&limit) {
            return Err(ApiError::validation("limit must be between 1 and 100"));
        }
    }
    validate_optional_query_text(query.cursor.as_deref(), "cursor", MAX_CURSOR_BYTES)?;
    validate_optional_query_text(
        query.provider_id.as_deref(),
        "provider_id",
        MAX_QUERY_TEXT_BYTES,
    )?;
    validate_optional_query_text(query.query.as_deref(), "query", MAX_QUERY_TEXT_BYTES)?;
    Ok(())
}

fn validate_optional_query_text(value: Option<&str>, field: &str, max: usize) -> ApiResult<()> {
    if value.is_some_and(|value| value.len() > max) {
        return Err(ApiError::validation(format!(
            "{field} exceeds the supported length"
        )));
    }
    Ok(())
}

fn catalog_model_response(model: db::PricingCatalogModelRate) -> ApiResult<CatalogModelRate> {
    let tiers = serde_json::from_str::<Value>(&model.tiers_json)
        .map_err(|_| ApiError::internal("stored catalog tier data is invalid"))?;
    Ok(CatalogModelRate {
        snapshot_id: model.snapshot_id,
        rate_revision_id: model.rate_revision_id,
        provider_id: model.provider_id,
        model_id: model.model_id,
        rates: api_rate_buckets(model.rates)?,
        tiers,
        source_last_updated: model.source_last_updated,
        source_kind: CatalogModelSourceKind::ModelsDevCatalog,
    })
}

fn catalog_status_response(
    status: &CatalogStatus,
    snapshot: Option<&CatalogSnapshot>,
    if_none_match: Option<&HeaderValue>,
) -> Response {
    let etag = status.etag.clone();
    if let (Some(expected), Some(actual)) = (
        etag.as_deref(),
        if_none_match.and_then(|value| value.to_str().ok()),
    ) {
        let matched = actual == "*"
            || actual
                .split(',')
                .map(str::trim)
                .any(|candidate| candidate == expected);
        if matched {
            let mut response = StatusCode::NOT_MODIFIED.into_response();
            if let Ok(value) = HeaderValue::from_str(expected) {
                response.headers_mut().insert(ETAG, value);
            }
            return response;
        }
    }
    let body = PricingCatalogStatus {
        state: api_catalog_state(status.state),
        active_snapshot_id: status.active_snapshot_id.clone(),
        revision: status.revision.clone(),
        etag: etag.clone(),
        fetched_at: snapshot.map(|snapshot| rfc3339(snapshot.fetched_at)),
        last_checked_at: status.last_checked_at.map(rfc3339),
        stale_after: status.stale_after.map(rfc3339),
        last_error_code: status.last_error_code.clone(),
    };
    let mut response = Json(body).into_response();
    if let Some(etag) = etag
        .as_deref()
        .and_then(|value| HeaderValue::from_str(value).ok())
    {
        response.headers_mut().insert(ETAG, etag);
    }
    response
}

fn api_catalog_state(state: pricing::CatalogState) -> PricingCatalogState {
    match state {
        pricing::CatalogState::Absent => PricingCatalogState::Absent,
        pricing::CatalogState::Fresh => PricingCatalogState::Fresh,
        pricing::CatalogState::Stale => PricingCatalogState::Stale,
        pricing::CatalogState::RefreshFailed => PricingCatalogState::RefreshFailed,
    }
}

async fn load_snapshot(
    repository: &SqlitePricingRepository,
    snapshot_id: Option<&str>,
) -> ApiResult<Option<CatalogSnapshot>> {
    match snapshot_id {
        Some(id) => repository
            .catalog_snapshot(id)
            .await
            .map_err(map_catalog_repository_error),
        None => Ok(None),
    }
}

// -------------------------------------------------------------------------
// Provider entry and CLI-runtime pricing subjects
// -------------------------------------------------------------------------

pub async fn get_provider_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<ProviderPricing>> {
    let subject = provider_subject(&state, &user.user_id, &id).await?;
    get_subject_pricing(&state, subject).await.map(Json)
}

pub async fn replace_provider_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<ReplaceProviderPricingRequest>,
) -> ApiResult<Json<ProviderPricing>> {
    let subject = provider_subject(&state, &user.user_id, &id).await?;
    replace_subject_pricing(&state, subject, request)
        .await
        .map(Json)
}

pub async fn get_cli_runtime_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((daemon_id, executor_type)): Path<(String, String)>,
) -> ApiResult<Json<ProviderPricing>> {
    let subject = cli_runtime_subject(&state, &user.user_id, &daemon_id, &executor_type).await?;
    get_subject_pricing(&state, subject).await.map(Json)
}

pub async fn replace_cli_runtime_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((daemon_id, executor_type)): Path<(String, String)>,
    Json(request): Json<ReplaceProviderPricingRequest>,
) -> ApiResult<Json<ProviderPricing>> {
    let subject = cli_runtime_subject(&state, &user.user_id, &daemon_id, &executor_type).await?;
    replace_subject_pricing(&state, subject, request)
        .await
        .map(Json)
}

async fn get_subject_pricing(
    state: &AppState,
    subject: PricingSubject,
) -> ApiResult<ProviderPricing> {
    let config = state
        .pricing_repository
        .pricing_configuration(&subject.id)
        .await
        .map_err(map_catalog_repository_error)?;
    subject_pricing_response(state, &subject, config).await
}

async fn replace_subject_pricing(
    state: &AppState,
    subject: PricingSubject,
    request: ReplaceProviderPricingRequest,
) -> ApiResult<ProviderPricing> {
    if subject.state != PricingSubjectState::Active {
        return Err(ApiError::conflict_with_code(
            "pricing_subject_retired",
            "pricing for this disconnected source is retired",
        ));
    }
    let domain_request = desired_pricing_request(state, request).await?;
    let config = state
        .pricing_repository
        .replace_pricing_configuration(&subject.id, domain_request, SystemTime::now())
        .await
        .map_err(map_binding_mutation_error)?;
    let subject = PricingSubjectRepo::get_pricing_subject(&*state.db, &subject.id)
        .await
        .map_err(map_db_read_error)?
        .ok_or_else(|| ApiError::not_found("pricing_subject", subject.id.clone()))?;
    subject_pricing_response(state, &subject, config).await
}

async fn desired_pricing_request(
    state: &AppState,
    request: ReplaceProviderPricingRequest,
) -> ApiResult<ReplacePricingRequest> {
    if request.expected_version < 0 {
        return Err(ApiError::validation(
            "expected_version must be zero or greater",
        ));
    }
    validate_required_identifier(&request.idempotency_key, "idempotency_key")?;
    if request.idempotency_key.len() > 256 {
        return Err(ApiError::validation(
            "idempotency_key exceeds the supported length",
        ));
    }
    validate_required_identifier(&request.subject_revision_digest, "subject_revision_digest")?;
    if request.bindings.len() > MAX_BINDINGS {
        return Err(ApiError::validation("too many pricing bindings"));
    }
    let status = state
        .models_dev_client
        .status_at(SystemTime::now())
        .await
        .map_err(map_catalog_client_error)?;
    let freshness = status.freshness_at(SystemTime::now());
    let mut bindings = Vec::with_capacity(request.bindings.len());
    for binding in request.bindings {
        bindings.push(desired_binding(state, &binding, freshness).await?);
    }
    Ok(ReplacePricingRequest {
        expected_version: u64::try_from(request.expected_version)
            .map_err(|_| ApiError::validation("expected_version is invalid"))?,
        idempotency_key: request.idempotency_key,
        subject_revision_digest: request.subject_revision_digest,
        bindings,
    })
}

async fn desired_binding(
    state: &AppState,
    binding: &ReplaceProviderPricingBinding,
    catalog_freshness: CatalogFreshness,
) -> ApiResult<DesiredPricingBinding> {
    validate_required_identifier(&binding.runtime_model, "runtime_model")?;
    match binding.source_kind {
        PricingBindingSourceKind::ManualOverride => {
            if binding.catalog_provider_id.is_some()
                || binding.catalog_model_id.is_some()
                || binding.catalog_rate_revision_id.is_some()
            {
                return Err(ApiError::validation(
                    "manual pricing bindings cannot contain catalog references",
                ));
            }
            let rates = binding.manual_rates.as_ref().ok_or_else(|| {
                ApiError::validation("manual pricing bindings require manual_rates")
            })?;
            Ok(DesiredPricingBinding::manual(
                binding.runtime_model.clone(),
                service_rate_buckets(rates)?,
            ))
        }
        PricingBindingSourceKind::ModelsDevCatalog => {
            if binding.manual_rates.is_some() {
                return Err(ApiError::validation(
                    "catalog pricing bindings cannot contain manual_rates",
                ));
            }
            let provider_id = binding.catalog_provider_id.as_deref().ok_or_else(|| {
                ApiError::validation("catalog pricing bindings require catalog_provider_id")
            })?;
            let model_id = binding.catalog_model_id.as_deref().ok_or_else(|| {
                ApiError::validation("catalog pricing bindings require catalog_model_id")
            })?;
            let rate_revision_id =
                binding.catalog_rate_revision_id.as_deref().ok_or_else(|| {
                    ApiError::validation(
                        "catalog pricing bindings require catalog_rate_revision_id",
                    )
                })?;
            validate_required_identifier(provider_id, "catalog_provider_id")?;
            validate_required_identifier(model_id, "catalog_model_id")?;
            validate_required_identifier(rate_revision_id, "catalog_rate_revision_id")?;
            let rate = PricingCatalogRepo::get_pricing_rate_revision(&*state.db, rate_revision_id)
                .await
                .map_err(map_db_read_error)?
                .ok_or_else(|| ApiError::validation("catalog rate revision is not available"))?;
            if rate.source_kind != PricingRateSourceKind::ModelsDevCatalog
                || rate.currency != "USD"
                || rate.catalog_provider_id.as_deref() != Some(provider_id)
                || rate.catalog_model_id.as_deref() != Some(model_id)
            {
                return Err(ApiError::validation(
                    "catalog rate revision does not match the requested provider/model",
                ));
            }
            let snapshot_id = rate
                .catalog_snapshot_id
                .as_deref()
                .ok_or_else(|| ApiError::validation("catalog rate revision has no snapshot"))?;
            let snapshot = state
                .pricing_repository
                .catalog_snapshot(snapshot_id)
                .await
                .map_err(map_catalog_repository_error)?
                .ok_or_else(|| ApiError::validation("catalog snapshot is not available"))?;
            let model = snapshot.model_rate(provider_id, model_id).ok_or_else(|| {
                ApiError::validation("catalog rate revision does not match its snapshot")
            })?;
            if model.rate_digest(&snapshot.id) != rate.id
                || rate.rates
                    != db::RateBuckets::new(
                        model
                            .rates
                            .input
                            .map(NanoUsdPerMillion::as_nano_usd_per_million),
                        model
                            .rates
                            .output
                            .map(NanoUsdPerMillion::as_nano_usd_per_million),
                        model
                            .rates
                            .cache_read
                            .map(NanoUsdPerMillion::as_nano_usd_per_million),
                        model
                            .rates
                            .cache_write
                            .map(NanoUsdPerMillion::as_nano_usd_per_million),
                    )
            {
                return Err(ApiError::validation(
                    "catalog rate revision provenance is invalid",
                ));
            }
            let tiers = pricing::parse_persisted_context_tiers(&rate.tiers_json)
                .map_err(|_| ApiError::validation("catalog context tiers are invalid"))?;
            let legacy_context = rate
                .legacy_context_over_200k_json
                .as_deref()
                .map(pricing::parse_persisted_legacy_context_rate)
                .transpose()
                .map_err(|_| ApiError::validation("catalog legacy context rates are invalid"))?;
            Ok(DesiredPricingBinding::models_dev(
                binding.runtime_model.clone(),
                provider_id.to_owned(),
                model_id.to_owned(),
                snapshot.id.clone(),
                rate_revision_id.to_owned(),
                model.rates,
                tiers,
                legacy_context,
                catalog_freshness,
            ))
        }
    }
}

async fn subject_pricing_response(
    state: &AppState,
    subject: &PricingSubject,
    config: PricingConfiguration,
) -> ApiResult<ProviderPricing> {
    let rows = PricingSubjectRepo::list_pricing_subject_bindings(&*state.db, &subject.id, true)
        .await
        .map_err(map_db_read_error)?;
    let rows_by_id = rows
        .into_iter()
        .map(|row| (row.id.clone(), row))
        .collect::<HashMap<_, _>>();
    let mut bindings = Vec::with_capacity(config.bindings.len());
    for source in config.bindings {
        let id = source_id(&source);
        let row = rows_by_id.get(&id).ok_or_else(|| {
            ApiError::internal("stored pricing binding is missing its database row")
        })?;
        bindings.push(api_binding(source, row)?);
    }
    bindings.sort_by(|left, right| {
        left.runtime_model
            .cmp(&right.runtime_model)
            .then_with(|| {
                let left_kind =
                    matches!(left.source_kind, PricingBindingSourceKind::ManualOverride);
                let right_kind =
                    matches!(right.source_kind, PricingBindingSourceKind::ManualOverride);
                right_kind.cmp(&left_kind)
            })
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(ProviderPricing {
        subject_id: config.subject_id,
        subject_revision_digest: config.subject_revision_digest,
        version: i64::try_from(config.version)
            .map_err(|_| ApiError::internal("pricing subject version exceeds the API range"))?,
        bindings,
    })
}

fn source_id(source: &PricingBindingSource) -> String {
    match source {
        PricingBindingSource::ModelsDev(binding) => binding.id.clone(),
        PricingBindingSource::Manual(binding) => binding.id.clone(),
    }
}

fn api_binding(
    source: PricingBindingSource,
    row: &db::PricingSubjectBinding,
) -> ApiResult<ApiPricingBinding> {
    match source {
        PricingBindingSource::ModelsDev(binding) => Ok(ApiPricingBinding {
            id: binding.id,
            runtime_model: binding.runtime_model,
            subject_revision_digest: row.subject_revision_digest.clone(),
            source_kind: PricingBindingSourceKind::ModelsDevCatalog,
            catalog_provider_id: Some(binding.provider_id),
            catalog_model_id: Some(binding.model_id),
            catalog_rate_revision_id: Some(binding.rate_revision_id),
            manual_rates: None,
            effective_at: row.effective_at.clone(),
            retired_at: row.retired_at.clone(),
            version: row.version,
        }),
        PricingBindingSource::Manual(binding) => Ok(ApiPricingBinding {
            id: binding.id,
            runtime_model: binding.runtime_model,
            subject_revision_digest: binding.subject_revision_digest,
            source_kind: PricingBindingSourceKind::ManualOverride,
            catalog_provider_id: None,
            catalog_model_id: None,
            catalog_rate_revision_id: Some(binding.rate_revision_id),
            manual_rates: Some(api_rate_buckets_from_service(binding.rates)?),
            effective_at: rfc3339(binding.effective_at),
            retired_at: binding
                .retired_at
                .map(rfc3339)
                .or_else(|| row.retired_at.clone()),
            version: row.version,
        }),
    }
}

async fn provider_subject(
    state: &AppState,
    user_id: &str,
    provider_entry_id: &str,
) -> ApiResult<PricingSubject> {
    let handle = CredentialHandleRepo::get_credential_handle_for_owner(
        &*state.db,
        provider_entry_id,
        user_id,
    )
    .await
    .map_err(map_db_read_error)?
    .ok_or_else(|| ApiError::not_found("provider_entry", provider_entry_id.to_owned()))?;
    if handle.status == "revoked" {
        // Revocation is terminal for the credential's pricing subject. Keep
        // its historical rows, but do not allow a GET/PUT to resurrect or
        // provision a subject for a credential that no longer exists.
        return Err(ApiError::not_found(
            "provider_entry",
            provider_entry_id.to_owned(),
        ));
    }
    let identity = provider_identity(&handle);
    if let Some(subject) =
        PricingSubjectRepo::get_pricing_subject_for_provider(&*state.db, user_id, provider_entry_id)
            .await
            .map_err(map_db_read_error)?
    {
        return ensure_subject_identity(
            state,
            subject,
            identity,
            Some(provider_entry_id.to_owned()),
            None,
            None,
        )
        .await;
    }
    ensure_subject(
        state,
        user_id,
        PricingSubjectKind::ProviderEntry,
        Some(provider_entry_id.to_owned()),
        None,
        None,
        identity,
    )
    .await
}

async fn cli_runtime_subject(
    state: &AppState,
    user_id: &str,
    daemon_id: &str,
    executor_type: &str,
) -> ApiResult<PricingSubject> {
    let daemon = DaemonRepo::get_by_id(&*state.db, daemon_id)
        .await
        .map_err(map_db_read_error)?
        .filter(|daemon| {
            daemon
                .owner_id
                .as_deref()
                .is_none_or(|owner| owner == user_id)
        })
        .ok_or_else(|| {
            ApiError::not_found("cli_runtime", format!("{daemon_id}/{executor_type}"))
        })?;
    let detected = detected_cli(&daemon, executor_type).ok_or_else(|| {
        ApiError::not_found("cli_runtime", format!("{daemon_id}/{executor_type}"))
    })?;
    if let Some(subject) = PricingSubjectRepo::get_pricing_subject_for_cli_runtime(
        &*state.db,
        user_id,
        daemon_id,
        executor_type,
    )
    .await
    .map_err(map_db_read_error)?
    {
        return ensure_subject_identity(
            state,
            subject,
            cli_identity(executor_type, &detected),
            None,
            Some(daemon_id.to_owned()),
            Some(executor_type.to_owned()),
        )
        .await;
    }
    ensure_subject(
        state,
        user_id,
        PricingSubjectKind::CliRuntime,
        None,
        Some(daemon_id.to_owned()),
        Some(executor_type.to_owned()),
        cli_identity(executor_type, &detected),
    )
    .await
}

fn provider_identity(handle: &CredentialHandle) -> pricing::PricingSubjectIdentity {
    let endpoint_class = services::embedded_agent_service::entry_base_url(handle)
        .ok()
        .and_then(|base_url| canonical_endpoint_class(&base_url))
        .filter(|host| !host.is_empty())
        .unwrap_or_else(|| handle.provider.clone());
    let credential_method = if handle.credential_method == "oauth_bundle" {
        "oauth".to_owned()
    } else {
        handle.credential_method.clone()
    };
    pricing::PricingSubjectIdentity {
        subject_kind: "provider_entry".to_owned(),
        provider_kind: handle.provider.clone(),
        credential_method,
        endpoint_class,
        runtime_fingerprint: None,
        schema_revision: "pricing-subject-v1".to_owned(),
    }
}

/// Canonicalizes the non-secret endpoint identity used by pricing subjects.
/// Scheme, host, explicit port, and custom path all participate in the
/// digest; credentials, query parameters, and fragments never do. Trailing
/// path slashes are normalized so equivalent root/API spellings do not create
/// needless subject revisions.
fn canonical_endpoint_class(base_url: &str) -> Option<String> {
    let parsed = url::Url::parse(base_url).ok()?;
    let scheme = parsed.scheme().trim().to_ascii_lowercase();
    let host = parsed.host_str()?.trim().to_ascii_lowercase();
    if scheme.is_empty() || host.is_empty() {
        return None;
    }
    let port = parsed
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    let trimmed_path = parsed.path().trim_end_matches('/');
    let path = if trimmed_path.is_empty() {
        "/"
    } else {
        trimmed_path
    };
    Some(format!("{scheme}://{host}{port}{path}"))
}

fn cli_identity(
    executor_type: &str,
    detected: &api_types::DetectedCli,
) -> pricing::PricingSubjectIdentity {
    // Daemon reports are untrusted input. In particular, CLI paths and config
    // paths may be URL-shaped or contain copied credentials. Keep the raw
    // values in memory only while computing a domain-separated digest; the
    // subject row and API response receive only that fixed-size opaque value.
    let fingerprint = cli_fingerprint(executor_type, detected);
    pricing::PricingSubjectIdentity {
        subject_kind: "cli_runtime".to_owned(),
        provider_kind: executor_type.to_owned(),
        credential_method: "cli_login".to_owned(),
        endpoint_class: "cli_runtime".to_owned(),
        runtime_fingerprint: Some(fingerprint),
        schema_revision: "pricing-subject-v1".to_owned(),
    }
}

const CLI_FINGERPRINT_DOMAIN: &str = "forge-pricing-cli-runtime-fingerprint-v1";

fn cli_fingerprint(executor_type: &str, detected: &api_types::DetectedCli) -> String {
    let mut bytes = Vec::with_capacity(256);
    append_cli_fingerprint_field(&mut bytes, Some(CLI_FINGERPRINT_DOMAIN));
    append_cli_fingerprint_field(&mut bytes, Some(executor_type));
    // Availability/authentication is deliberately excluded: it is a
    // reversible eligibility state, not runtime identity, and must not mint
    // a new subject revision on routine login/disable transitions.
    append_cli_fingerprint_field(&mut bytes, detected.version.as_deref());
    append_cli_fingerprint_field(&mut bytes, detected.path.as_deref());
    append_cli_fingerprint_field(&mut bytes, detected.config_path.as_deref());
    format!("sha256:{}", pricing::sha256_hex(&bytes))
}

fn append_cli_fingerprint_field(bytes: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => bytes.push(0),
        Some(value) => {
            bytes.push(1);
            let value = value.as_bytes();
            let length = match u64::try_from(value.len()) {
                Ok(length) => length,
                Err(_) => {
                    // This is unreachable on supported targets, but retain a
                    // deterministic bounded representation if usize ever
                    // exceeds the digest length prefix.
                    bytes.push(2);
                    bytes.extend_from_slice(&u64::MAX.to_be_bytes());
                    bytes.extend_from_slice(pricing::sha256_hex(value).as_bytes());
                    return;
                }
            };
            bytes.extend_from_slice(&length.to_be_bytes());
            bytes.extend_from_slice(value);
        }
    }
}

fn detected_cli(daemon: &Daemon, executor_type: &str) -> Option<api_types::DetectedCli> {
    serde_json::from_str::<Vec<api_types::DetectedCli>>(&daemon.detected_clis_json)
        .ok()?
        .into_iter()
        .find(|cli| cli.kind == executor_type)
}

async fn ensure_subject(
    state: &AppState,
    user_id: &str,
    subject_kind: PricingSubjectKind,
    provider_entry_id: Option<String>,
    daemon_id: Option<String>,
    executor_type: Option<String>,
    identity: pricing::PricingSubjectIdentity,
) -> ApiResult<PricingSubject> {
    for attempt in 0..2 {
        match provision_subject(
            state,
            user_id,
            subject_kind,
            provider_entry_id.clone(),
            daemon_id.clone(),
            executor_type.clone(),
            &identity,
        )
        .await
        {
            Ok(subject) => return Ok(subject),
            Err(DbError::IdempotencyConflict) if attempt == 0 => {
                // Another request may have provisioned the same subject between
                // the visibility lookup and this insert. Re-read through the
                // visibility-scoped key; never expose the competing row by its
                // generated ID. If the read races the commit, one retry gives
                // the writer time to publish the pointer.
                if let Some(subject) = lookup_subject(
                    state,
                    user_id,
                    subject_kind,
                    provider_entry_id.as_deref(),
                    daemon_id.as_deref(),
                    executor_type.as_deref(),
                )
                .await?
                {
                    return ensure_subject_identity(
                        state,
                        subject,
                        identity.clone(),
                        provider_entry_id.clone(),
                        daemon_id.clone(),
                        executor_type.clone(),
                    )
                    .await;
                }
            }
            Err(error) => return Err(map_db_mutation_error(error)),
        }
    }
    Err(ApiError::internal(
        "pricing subject could not be provisioned",
    ))
}

async fn provision_subject(
    state: &AppState,
    user_id: &str,
    subject_kind: PricingSubjectKind,
    provider_entry_id: Option<String>,
    daemon_id: Option<String>,
    executor_type: Option<String>,
    identity: &pricing::PricingSubjectIdentity,
) -> Result<PricingSubject, DbError> {
    let now = now_rfc3339();
    let subject_id = new_uuid_v4();
    let mut transaction = db::begin_immediate(state.db.pool()).await?;
    let subject = PricingSubjectRepo::create_pricing_subject_in_tx(
        &*state.db,
        &mut transaction,
        db::CreatePricingSubject {
            id: subject_id,
            owner_user_id: user_id.to_owned(),
            subject_kind,
            provider_entry_id: provider_entry_id.clone(),
            daemon_id: daemon_id.clone(),
            executor_type: executor_type.clone(),
            current_revision_id: None,
            state: PricingSubjectState::Active,
            last_idempotency_key: None,
            last_update_digest: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await?;
    let revision_id = new_uuid_v4();
    let revision_digest = pricing::pricing_subject_revision_digest(identity);
    let revision = PricingSubjectRepo::create_pricing_subject_revision_in_tx(
        &*state.db,
        &mut transaction,
        subject_revision_input(SubjectRevisionDraft {
            id: revision_id,
            subject_id: subject.id.clone(),
            owner_user_id: user_id,
            revision: 1,
            revision_digest,
            subject_kind,
            provider_entry_id,
            daemon_id,
            executor_type,
            identity,
            created_at: now.clone(),
        }),
    )
    .await?;
    let subject = PricingSubjectRepo::update_pricing_subject_in_tx(
        &*state.db,
        &mut transaction,
        db::UpdatePricingSubject {
            id: subject.id,
            expected_version: subject.version,
            current_revision_id: Some(Some(revision.id)),
            state: None,
            last_idempotency_key: None,
            last_update_digest: None,
            updated_at: now,
        },
    )
    .await?;
    transaction.commit().await?;
    Ok(subject)
}

async fn ensure_subject_identity(
    state: &AppState,
    subject: PricingSubject,
    identity: pricing::PricingSubjectIdentity,
    provider_entry_id: Option<String>,
    daemon_id: Option<String>,
    executor_type: Option<String>,
) -> ApiResult<PricingSubject> {
    // Retirement is terminal for a pricing subject. Keep the immutable
    // historical revision/configuration readable, but do not mint a new
    // revision after the source has been disconnected or disabled.
    if subject.state == PricingSubjectState::Retired {
        return Ok(subject);
    }
    let expected_digest = pricing::pricing_subject_revision_digest(&identity);
    let mut subject = subject;
    for attempt in 0..3 {
        let current_revision = match subject.current_revision_id.as_deref() {
            Some(revision_id) => {
                PricingSubjectRepo::get_pricing_subject_revision(&*state.db, revision_id)
                    .await
                    .map_err(map_db_read_error)?
            }
            None => None,
        };
        if current_revision
            .as_ref()
            .is_some_and(|revision| revision.revision_digest == expected_digest)
        {
            return Ok(subject);
        }
        let next_revision = match current_revision.as_ref() {
            Some(revision) => revision
                .revision
                .checked_add(1)
                .ok_or_else(|| ApiError::internal("pricing subject revision overflow"))?,
            None => 1,
        };
        match append_subject_revision(
            state,
            &subject,
            identity.clone(),
            provider_entry_id.clone(),
            daemon_id.clone(),
            executor_type.clone(),
            next_revision,
        )
        .await
        {
            Ok(updated) => return Ok(updated),
            Err(DbError::VersionConflict | DbError::IdempotencyConflict) if attempt < 2 => {
                subject = lookup_subject(
                    state,
                    &subject.owner_user_id,
                    subject.subject_kind,
                    provider_entry_id.as_deref(),
                    daemon_id.as_deref(),
                    executor_type.as_deref(),
                )
                .await?
                .ok_or_else(|| ApiError::not_found("pricing_subject", subject.id.clone()))?;
            }
            Err(error) => return Err(map_db_mutation_error(error)),
        }
    }
    Err(ApiError::conflict_with_code(
        "pricing.version_conflict",
        "pricing subject identity changed while it was being refreshed",
    ))
}

async fn append_subject_revision(
    state: &AppState,
    subject: &PricingSubject,
    identity: pricing::PricingSubjectIdentity,
    provider_entry_id: Option<String>,
    daemon_id: Option<String>,
    executor_type: Option<String>,
    revision_number: i64,
) -> Result<PricingSubject, DbError> {
    let now = now_rfc3339();
    let mut transaction = db::begin_immediate(state.db.pool()).await?;
    let revision = PricingSubjectRepo::create_pricing_subject_revision_in_tx(
        &*state.db,
        &mut transaction,
        subject_revision_input(SubjectRevisionDraft {
            id: new_uuid_v4(),
            subject_id: subject.id.clone(),
            owner_user_id: &subject.owner_user_id,
            revision: revision_number,
            revision_digest: pricing::pricing_subject_revision_digest(&identity),
            subject_kind: subject.subject_kind,
            provider_entry_id,
            daemon_id,
            executor_type,
            identity: &identity,
            created_at: now.clone(),
        }),
    )
    .await?;
    let subject = PricingSubjectRepo::update_pricing_subject_in_tx(
        &*state.db,
        &mut transaction,
        db::UpdatePricingSubject {
            id: subject.id.clone(),
            expected_version: subject.version,
            current_revision_id: Some(Some(revision.id)),
            state: None,
            last_idempotency_key: None,
            last_update_digest: None,
            updated_at: now,
        },
    )
    .await?;
    transaction.commit().await?;
    Ok(subject)
}

struct SubjectRevisionDraft<'a> {
    id: String,
    subject_id: String,
    owner_user_id: &'a str,
    revision: i64,
    revision_digest: String,
    subject_kind: PricingSubjectKind,
    provider_entry_id: Option<String>,
    daemon_id: Option<String>,
    executor_type: Option<String>,
    identity: &'a pricing::PricingSubjectIdentity,
    created_at: String,
}

fn subject_revision_input(input: SubjectRevisionDraft<'_>) -> db::CreatePricingSubjectRevision {
    db::CreatePricingSubjectRevision {
        id: input.id,
        subject_id: input.subject_id,
        owner_user_id: input.owner_user_id.to_owned(),
        revision: input.revision,
        revision_digest: input.revision_digest,
        subject_kind: input.subject_kind,
        provider_entry_id: input.provider_entry_id,
        daemon_id: input.daemon_id,
        executor_type: input.executor_type,
        provider_kind: input.identity.provider_kind.clone(),
        credential_method: input.identity.credential_method.clone(),
        endpoint_class: input.identity.endpoint_class.clone(),
        runtime_fingerprint: input.identity.runtime_fingerprint.clone(),
        schema_revision: input.identity.schema_revision.clone(),
        non_secret_identity_json: serde_json::to_string(&identity_json(input.identity))
            .unwrap_or_else(|_| "{}".to_owned()),
        created_at: input.created_at,
    }
}

async fn lookup_subject(
    state: &AppState,
    user_id: &str,
    subject_kind: PricingSubjectKind,
    provider_entry_id: Option<&str>,
    daemon_id: Option<&str>,
    executor_type: Option<&str>,
) -> ApiResult<Option<PricingSubject>> {
    match subject_kind {
        PricingSubjectKind::ProviderEntry => PricingSubjectRepo::get_pricing_subject_for_provider(
            &*state.db,
            user_id,
            provider_entry_id.unwrap_or_default(),
        )
        .await
        .map_err(map_db_read_error),
        PricingSubjectKind::CliRuntime => PricingSubjectRepo::get_pricing_subject_for_cli_runtime(
            &*state.db,
            user_id,
            daemon_id.unwrap_or_default(),
            executor_type.unwrap_or_default(),
        )
        .await
        .map_err(map_db_read_error),
    }
}

fn identity_json(identity: &pricing::PricingSubjectIdentity) -> Value {
    json!({
        "subject_kind": identity.subject_kind.clone(),
        "provider_kind": identity.provider_kind.clone(),
        "credential_method": identity.credential_method.clone(),
        "endpoint_class": identity.endpoint_class.clone(),
        "runtime_fingerprint": identity.runtime_fingerprint.clone(),
        "schema_revision": identity.schema_revision.clone(),
    })
}

// -------------------------------------------------------------------------
// Project-scoped retrospective preview/commit/read
// -------------------------------------------------------------------------

pub async fn create_cost_estimation_preview(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<CreateCostEstimationPreviewRequest>,
) -> ApiResult<Json<ApiCostEstimationPreview>> {
    let project = visible_project(&state, &project_id, &user.user_id).await?;
    validate_preview_request(&request)?;
    let owner_user_id = project.owner_id.clone().ok_or_else(|| {
        ApiError::forbidden_with_code(
            "project_owner_required",
            "Project ownership is required for cost-estimation mutation",
        )
    })?;
    let operation_key = scoped_idempotency_key(
        "cost_estimation_preview",
        &project_id,
        &user.user_id,
        &request.idempotency_key,
    );
    if let Some(existing) = RetrospectiveEstimateRepo::get_cost_estimation_preview_by_idempotency(
        &*state.db,
        &operation_key,
    )
    .await
    .map_err(map_db_read_error)?
    {
        if existing.project_id != project_id
            || existing.catalog_snapshot_id != request.snapshot_id
            || existing.window_from != request.from
            || existing.window_to != request.to
        {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "idempotency key was reused with a different preview request",
            ));
        }
        return Ok(Json(api_preview_from_db(&existing)?));
    }

    let snapshot = state
        .pricing_repository
        .catalog_snapshot(&request.snapshot_id)
        .await
        .map_err(map_catalog_repository_error)?
        .ok_or_else(|| {
            ApiError::not_found("pricing_catalog_snapshot", request.snapshot_id.clone())
        })?;
    let events = UsageLedgerRepo::list_usage_events_for_project(
        &*state.db,
        &project_id,
        request.from.as_deref(),
        request.to.as_deref(),
    )
    .await
    .map_err(map_db_read_error)?;
    let domain_events = events
        .iter()
        .map(retrospective_event)
        .collect::<ApiResult<Vec<_>>>()?;
    let now = SystemTime::now();
    let status = state
        .models_dev_client
        .status_at(now)
        .await
        .map_err(map_catalog_client_error)?;
    let freshness = status.freshness_for_snapshot(&snapshot, now);
    if freshness == CatalogFreshness::NotApplicable {
        return Err(ApiError::validation(
            "snapshot is not an active models.dev catalog snapshot",
        ));
    }
    let preview = pricing::preview_cost_estimation_with_freshness(
        project_id.clone(),
        &snapshot,
        &domain_events,
        now,
        PREVIEW_TTL,
        freshness,
    )
    .map_err(|_| ApiError::validation("usage events cannot be estimated against this snapshot"))?;
    let summary = cost_summary(&preview, &domain_events, &snapshot)?;
    let summary_json = persisted_summary(&summary, preview.projected_cost, freshness);
    let filters_json = json!({
        "schema": "retrospective-preview-v1",
        "source_event_ids": preview.source_event_ids.clone(),
        "snapshot_revision_digest": snapshot.revision_digest.clone(),
        "catalog_freshness": freshness.as_str(),
    })
    .to_string();
    let now_text = rfc3339(now);
    let input = db::CreateCostEstimationPreview {
        id: preview.id.clone(),
        owner_user_id,
        project_id: project_id.clone(),
        catalog_snapshot_id: request.snapshot_id.clone(),
        catalog_freshness: db_catalog_freshness(freshness)?,
        usage_set_digest: preview.usage_set_digest.clone(),
        window_from: request.from.clone(),
        window_to: request.to.clone(),
        filters_json,
        eligible_event_count: i64::try_from(preview.eligible_event_count)
            .map_err(|_| ApiError::internal("eligible event count exceeds the supported range"))?,
        unmatched_event_count: i64::try_from(preview.unmatched_event_count)
            .map_err(|_| ApiError::internal("unmatched event count exceeds the supported range"))?,
        already_reported_event_count: i64::try_from(preview.already_reported_event_count)
            .map_err(|_| ApiError::internal("reported event count exceeds the supported range"))?,
        projected_cost_summary_json: summary_json,
        idempotency_key: operation_key.clone(),
        expires_at: rfc3339(preview.expires_at),
        created_at: now_text.clone(),
        updated_at: now_text,
    };
    let stored = match RetrospectiveEstimateRepo::create_cost_estimation_preview(&*state.db, input)
        .await
    {
        Ok(stored) => stored,
        Err(DbError::IdempotencyConflict) => {
            RetrospectiveEstimateRepo::get_cost_estimation_preview_by_idempotency(
                &*state.db,
                &operation_key,
            )
            .await
            .map_err(map_db_read_error)?
            .ok_or_else(|| {
                ApiError::conflict_with_code("idempotency_conflict", "preview mutation conflicted")
            })?
        }
        Err(error) => return Err(map_db_mutation_error(error)),
    };
    Ok(Json(api_preview_from_db_with_summary(
        &stored,
        Some(summary),
    )?))
}

pub async fn create_cost_estimation_run(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<CreateCostEstimationRunRequest>,
) -> ApiResult<Json<ApiCostEstimationRun>> {
    let project = visible_project(&state, &project_id, &user.user_id).await?;
    validate_run_request(&request)?;
    let owner_user_id = project.owner_id.clone().ok_or_else(|| {
        ApiError::forbidden_with_code(
            "project_owner_required",
            "Project ownership is required for cost-estimation mutation",
        )
    })?;
    let operation_key = scoped_idempotency_key(
        "cost_estimation_run",
        &project_id,
        &user.user_id,
        &request.idempotency_key,
    );
    let service_operation_key = format!("retrospective_commit:{operation_key}");
    if let Some(existing) = RetrospectiveEstimateRepo::get_cost_estimation_run_by_idempotency(
        &*state.db,
        &service_operation_key,
    )
    .await
    .map_err(map_db_read_error)?
    {
        if existing.project_id != project_id
            || existing.preview_id != request.preview_id
            || existing.usage_set_digest != request.usage_set_digest
        {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "idempotency key was reused with a different run request",
            ));
        }
        return Ok(Json(api_run_from_db(&existing)?));
    }
    let stored_preview =
        RetrospectiveEstimateRepo::get_cost_estimation_preview(&*state.db, &request.preview_id)
            .await
            .map_err(map_db_read_error)?
            .filter(|preview| {
                preview.project_id == project_id && preview.owner_user_id == owner_user_id
            })
            .ok_or_else(|| {
                ApiError::not_found("cost_estimation_preview", request.preview_id.clone())
            })?;
    if stored_preview.usage_set_digest != request.usage_set_digest {
        return Err(ApiError::conflict_with_code(
            "usage_set_conflict",
            "usage-set digest does not match the preview",
        ));
    }
    let snapshot = state
        .pricing_repository
        .catalog_snapshot(&stored_preview.catalog_snapshot_id)
        .await
        .map_err(map_catalog_repository_error)?
        .ok_or_else(|| {
            ApiError::not_found(
                "pricing_catalog_snapshot",
                stored_preview.catalog_snapshot_id.clone(),
            )
        })?;
    let source_event_ids = preview_source_event_ids(&stored_preview.filters_json)?;
    let current_events = UsageLedgerRepo::list_usage_events_for_project(
        &*state.db,
        &project_id,
        stored_preview.window_from.as_deref(),
        stored_preview.window_to.as_deref(),
    )
    .await
    .map_err(map_db_read_error)?;
    let mut current_event_ids = current_events
        .iter()
        .map(|event| event.id.clone())
        .collect::<Vec<_>>();
    current_event_ids.sort();
    current_event_ids.dedup();
    if current_event_ids != source_event_ids {
        return Err(ApiError::conflict_with_code(
            "usage_set_conflict",
            "project usage changed after the preview; create a new preview",
        ));
    }
    let events = load_source_events(&state, &project_id, &owner_user_id, &source_event_ids).await?;
    let domain_events = events
        .iter()
        .map(retrospective_event)
        .collect::<ApiResult<Vec<_>>>()?;
    let created_at = parse_system_time(&stored_preview.created_at, "preview.created_at")?;
    let expires_at = parse_system_time(&stored_preview.expires_at, "preview.expires_at")?;
    let expires_after = expires_at.duration_since(created_at).map_err(|_| {
        ApiError::conflict_with_code("usage_set_conflict", "preview expiry metadata is invalid")
    })?;
    let freshness = service_catalog_freshness(stored_preview.catalog_freshness);
    let preview = pricing::preview_cost_estimation_with_freshness(
        project_id.clone(),
        &snapshot,
        &domain_events,
        created_at,
        expires_after,
        freshness,
    )
    .map_err(|_| {
        ApiError::conflict_with_code("usage_set_conflict", "preview source set changed")
    })?;
    let eligible_event_count = checked_i64(preview.eligible_event_count, "eligible event count")?;
    let unmatched_event_count =
        checked_i64(preview.unmatched_event_count, "unmatched event count")?;
    let already_reported_event_count =
        checked_i64(preview.already_reported_event_count, "reported event count")?;
    if preview.id != stored_preview.id
        || preview.usage_set_digest != stored_preview.usage_set_digest
        || eligible_event_count != stored_preview.eligible_event_count
        || unmatched_event_count != stored_preview.unmatched_event_count
        || already_reported_event_count != stored_preview.already_reported_event_count
    {
        return Err(ApiError::conflict_with_code(
            "usage_set_conflict",
            "preview source set changed; create a new preview",
        ));
    }
    let run = pricing::commit_retrospective_preview(
        &*state.pricing_repository,
        preview,
        RetrospectiveCommitRequest {
            preview_id: request.preview_id,
            usage_set_digest: request.usage_set_digest,
            idempotency_key: operation_key,
        },
        SystemTime::now(),
    )
    .await
    .map_err(map_retrospective_error)?;
    let stored_run = RetrospectiveEstimateRepo::get_cost_estimation_run(&*state.db, &run.id)
        .await
        .map_err(map_db_read_error)?
        .ok_or_else(|| ApiError::internal("committed cost-estimation run is missing"))?;
    Ok(Json(api_run_from_db(&stored_run)?))
}

pub async fn get_cost_estimation_run(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, run_id)): Path<(String, String)>,
) -> ApiResult<Json<ApiCostEstimationRun>> {
    let _project = visible_project(&state, &project_id, &user.user_id).await?;
    let run = RetrospectiveEstimateRepo::get_cost_estimation_run(&*state.db, &run_id)
        .await
        .map_err(map_db_read_error)?
        .filter(|run| run.project_id == project_id)
        .ok_or_else(|| ApiError::not_found("cost_estimation_run", run_id))?;
    Ok(Json(api_run_from_db(&run)?))
}

async fn visible_project(state: &AppState, project_id: &str, user_id: &str) -> ApiResult<Project> {
    ProjectRepo::get_visible_by_id(&*state.db, project_id, user_id)
        .await
        .map_err(map_db_read_error)?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))
}

fn validate_preview_request(request: &CreateCostEstimationPreviewRequest) -> ApiResult<()> {
    validate_required_identifier(&request.snapshot_id, "snapshot_id")?;
    validate_required_identifier(&request.idempotency_key, "idempotency_key")?;
    if request.idempotency_key.len() > 256 {
        return Err(ApiError::validation(
            "idempotency_key exceeds the supported length",
        ));
    }
    validate_window(request.from.as_deref(), request.to.as_deref())
}

fn validate_run_request(request: &CreateCostEstimationRunRequest) -> ApiResult<()> {
    validate_required_identifier(&request.preview_id, "preview_id")?;
    validate_required_identifier(&request.usage_set_digest, "usage_set_digest")?;
    validate_required_identifier(&request.idempotency_key, "idempotency_key")?;
    if request.idempotency_key.len() > 256 {
        return Err(ApiError::validation(
            "idempotency_key exceeds the supported length",
        ));
    }
    Ok(())
}

fn validate_window(from: Option<&str>, to: Option<&str>) -> ApiResult<()> {
    let parsed_from = from
        .map(|value| parse_system_time(value, "from"))
        .transpose()?;
    let parsed_to = to.map(|value| parse_system_time(value, "to")).transpose()?;
    if let (Some(from), Some(to)) = (parsed_from, parsed_to) {
        if from >= to {
            return Err(ApiError::validation("from must be before to"));
        }
    }
    Ok(())
}

fn preview_source_event_ids(filters_json: &str) -> ApiResult<Vec<String>> {
    let value: Value = serde_json::from_str(filters_json).map_err(|_| {
        ApiError::conflict_with_code("usage_set_conflict", "preview filters are invalid")
    })?;
    let ids = value
        .get("source_event_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ApiError::conflict_with_code("usage_set_conflict", "preview source set is invalid")
        })?;
    if ids.len() > 100_000 {
        return Err(ApiError::conflict_with_code(
            "usage_set_conflict",
            "preview source set is too large",
        ));
    }
    let mut result = Vec::with_capacity(ids.len());
    for id in ids {
        let id = id.as_str().ok_or_else(|| {
            ApiError::conflict_with_code("usage_set_conflict", "preview source set is invalid")
        })?;
        validate_required_identifier(id, "source_event_id").map_err(|_| {
            ApiError::conflict_with_code("usage_set_conflict", "preview source set is invalid")
        })?;
        result.push(id.to_owned());
    }
    result.sort();
    result.dedup();
    if result.len() != ids.len() {
        return Err(ApiError::conflict_with_code(
            "usage_set_conflict",
            "preview source set is invalid",
        ));
    }
    Ok(result)
}

async fn load_source_events(
    state: &AppState,
    project_id: &str,
    owner_user_id: &str,
    ids: &[String],
) -> ApiResult<Vec<UsageEvent>> {
    let mut events = Vec::with_capacity(ids.len());
    for id in ids {
        let event = UsageLedgerRepo::get_usage_event(&*state.db, id)
            .await
            .map_err(map_db_read_error)?
            .filter(|event| {
                event.project_id.as_deref() == Some(project_id)
                    && event.owner_user_id.as_deref() == Some(owner_user_id)
            })
            .ok_or_else(|| {
                ApiError::conflict_with_code("usage_set_conflict", "preview source set changed")
            })?;
        events.push(event);
    }
    Ok(events)
}

// -------------------------------------------------------------------------
// Wire/domain conversion helpers
// -------------------------------------------------------------------------

fn api_rate_buckets(rates: db::RateBuckets) -> ApiResult<ApiRateBuckets> {
    Ok(ApiRateBuckets {
        input: api_rate_amount(rates.input)?,
        output: api_rate_amount(rates.output)?,
        cache_read: api_rate_amount(rates.cache_read)?,
        cache_write: api_rate_amount(rates.cache_write)?,
    })
}

fn api_rate_buckets_from_service(rates: EventBucketRates) -> ApiResult<ApiRateBuckets> {
    Ok(ApiRateBuckets {
        input: rates.input.map(service_rate_amount).transpose()?,
        output: rates.output.map(service_rate_amount).transpose()?,
        cache_read: rates.cache_read.map(service_rate_amount).transpose()?,
        cache_write: rates.cache_write.map(service_rate_amount).transpose()?,
    })
}

fn api_rate_amount(value: Option<i64>) -> ApiResult<Option<RateAmount>> {
    value
        .map(|value| {
            let rate = NanoUsdPerMillion::from_nano_usd(value)
                .ok_or_else(|| ApiError::internal("stored pricing rate is negative"))?;
            Ok(RateAmount {
                currency: "USD".to_owned(),
                decimal_per_million: rate.to_usd_decimal_per_million(),
            })
        })
        .transpose()
}

fn service_rate_amount(rate: NanoUsdPerMillion) -> ApiResult<RateAmount> {
    Ok(RateAmount {
        currency: "USD".to_owned(),
        decimal_per_million: rate.to_usd_decimal_per_million(),
    })
}

fn service_rate_buckets(rates: &ApiRateBuckets) -> ApiResult<EventBucketRates> {
    Ok(EventBucketRates::new(
        service_rate(rates.input.as_ref(), "input")?,
        service_rate(rates.output.as_ref(), "output")?,
        service_rate(rates.cache_read.as_ref(), "cache_read")?,
        service_rate(rates.cache_write.as_ref(), "cache_write")?,
    ))
}

fn service_rate(value: Option<&RateAmount>, field: &str) -> ApiResult<Option<NanoUsdPerMillion>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.currency != "USD" {
        return Err(ApiError::validation(format!(
            "{field} currency must be USD"
        )));
    }
    NanoUsdPerMillion::parse_manual_usd_per_million(&value.decimal_per_million)
        .map(Some)
        .map_err(|_| ApiError::validation(format!("{field} is not a valid USD rate")))
}

fn retrospective_event(event: &UsageEvent) -> ApiResult<RetrospectiveUsageEvent> {
    let provider_id = normalize_event_identifier(event.provider_id.as_deref(), "provider_id")?;
    let model_id = normalize_event_identifier(event.model_id.as_deref(), "model_id")?;
    let counters = match (
        event.input_tokens,
        event.output_tokens,
        event.cache_read_tokens,
        event.cache_write_tokens,
    ) {
        (Some(input), Some(output), Some(cache_read), Some(cache_write))
            if input >= 0 && output >= 0 && cache_read >= 0 && cache_write >= 0 =>
        {
            Some(EventTokenCounts::new(
                u64::try_from(input)
                    .map_err(|_| ApiError::validation("input token count is invalid"))?,
                u64::try_from(output)
                    .map_err(|_| ApiError::validation("output token count is invalid"))?,
                u64::try_from(cache_read)
                    .map_err(|_| ApiError::validation("cache-read token count is invalid"))?,
                u64::try_from(cache_write)
                    .map_err(|_| ApiError::validation("cache-write token count is invalid"))?,
            ))
        }
        (Some(_), Some(_), Some(_), Some(_)) => {
            return Err(ApiError::internal(
                "stored usage token counters are invalid",
            ));
        }
        _ => None,
    };
    let context_tokens = match event.context_tokens {
        Some(value) if value >= 0 => Some(
            u64::try_from(value)
                .map_err(|_| ApiError::internal("stored context token count is invalid"))?,
        ),
        Some(_) => return Err(ApiError::internal("stored context token count is invalid")),
        None => None,
    };
    let typed_reported = event
        .provider_reported_nano_usd
        .map(|value| {
            NanoUsd::from_nano_usd(value)
                .ok_or_else(|| ApiError::internal("stored provider-reported amount is invalid"))
        })
        .transpose()?;
    let provider_reported_amount = if typed_reported.is_some()
        || event.legacy_reported_cost_usd.is_some()
        || event.cost_kind == db::UsageCostKind::ProviderReported
    {
        typed_reported
    } else {
        None
    };
    let counters = if provider_reported_amount.is_some() {
        counters
    } else if event.legacy_reported_cost_usd.is_some()
        || event.cost_kind == db::UsageCostKind::ProviderReported
    {
        None
    } else {
        counters
    };
    Ok(RetrospectiveUsageEvent {
        event_id: event.id.clone(),
        provider_id,
        model_id,
        counters,
        provider_reported_amount,
        context_tokens,
        occurred_at: parse_system_time(&event.occurred_at, "occurred_at")?,
    })
}

fn normalize_event_identifier(value: Option<&str>, field: &str) -> ApiResult<Option<String>> {
    let Some(value) = value.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    validate_required_identifier(value, field)?;
    Ok(Some(value.to_owned()))
}

fn cost_summary(
    preview: &RetrospectivePreview,
    events: &[RetrospectiveUsageEvent],
    snapshot: &CatalogSnapshot,
) -> ApiResult<CostSummary> {
    let eligible_ids = preview
        .eligible_events
        .iter()
        .map(|candidate| candidate.event_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut provider_nanos: i128 = 0;
    let mut has_provider_reported = false;
    for event in events {
        if let Some(amount) = event.provider_reported_amount {
            has_provider_reported = true;
            provider_nanos = provider_nanos
                .checked_add(i128::from(amount.as_nano_usd()))
                .ok_or_else(|| ApiError::internal("cost-estimation arithmetic overflow"))?;
        }
    }
    let estimated_nanos = preview
        .projected_cost
        .map(|amount| i128::from(amount.as_nano_usd()));
    let provider_reported = has_provider_reported
        .then(|| money_from_nanos(provider_nanos))
        .transpose()?;
    let estimated = estimated_nanos.map(money_from_nanos).transpose()?;
    let known_nanos = provider_nanos
        .checked_add(estimated_nanos.unwrap_or(0))
        .ok_or_else(|| ApiError::internal("cost-estimation arithmetic overflow"))?;
    let known_subtotal = (has_provider_reported || estimated_nanos.is_some())
        .then(|| money_from_nanos(known_nanos))
        .transpose()?;
    let complete_total = if preview.unmatched_event_count == 0 {
        known_subtotal.clone()
    } else {
        None
    };
    let kind = match (provider_reported.is_some(), estimated.is_some()) {
        (true, true) => CostKind::Mixed,
        (true, false) => CostKind::ProviderReported,
        (false, true) => CostKind::Estimated,
        (false, false) => CostKind::None,
    };
    let coverage = if events.is_empty() {
        CostCoverage::NoUsage
    } else if preview.unmatched_event_count == 0 {
        CostCoverage::Complete
    } else if preview.eligible_event_count > 0 || preview.already_reported_event_count > 0 {
        CostCoverage::Partial
    } else {
        CostCoverage::Unavailable
    };
    let mut priced_tokens = TokenCounters {
        input_tokens: 0,
        output_tokens: 0,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    };
    let mut unpriced_tokens = priced_tokens.clone();
    let mut metered = 0_i64;
    for event in events {
        let Some(counters) = event.counters else {
            continue;
        };
        metered = metered
            .checked_add(1)
            .ok_or_else(|| ApiError::internal("cost-estimation count overflow"))?;
        let destination = if eligible_ids.contains(event.event_id.as_str())
            || event.provider_reported_amount.is_some()
        {
            &mut priced_tokens
        } else {
            &mut unpriced_tokens
        };
        add_tokens(destination, counters)?;
    }
    let mut reasons = BTreeMap::<CostCoverageReasonCode, (i64, TokenCounters)>::new();
    let event_by_id = events
        .iter()
        .map(|event| (event.event_id.as_str(), event))
        .collect::<HashMap<_, _>>();
    for unmatched in &preview.unmatched_events {
        let code = api_reason(unmatched.reason);
        let entry = reasons.entry(code).or_insert((
            0,
            TokenCounters {
                input_tokens: 0,
                output_tokens: 0,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
            },
        ));
        entry.0 = entry
            .0
            .checked_add(1)
            .ok_or_else(|| ApiError::internal("cost-estimation count overflow"))?;
        if let Some(event) = event_by_id.get(unmatched.event_id.as_str()) {
            if let Some(counters) = event.counters {
                add_tokens(&mut entry.1, counters)?;
            }
        }
    }
    let reasons = reasons
        .into_iter()
        .map(|(code, (count, tokens))| CostCoverageReason {
            code,
            run_or_turn_count: count,
            provider_attempt_count: count,
            tokens,
        })
        .collect::<Vec<_>>();
    let fully_costed = preview
        .eligible_event_count
        .checked_add(preview.already_reported_event_count)
        .ok_or_else(|| ApiError::internal("cost-estimation count overflow"))?;
    let unavailable = preview.unmatched_event_count;
    let total_events = checked_i64(events.len(), "usage event count")?;
    let fully_costed_count = checked_i64(fully_costed, "fully costed event count")?;
    let unavailable_count = checked_i64(unavailable, "unavailable event count")?;
    let unmetered = total_events
        .checked_sub(metered)
        .ok_or_else(|| ApiError::internal("cost-estimation count overflow"))?;
    let mut sources = Vec::new();
    if preview.already_reported_event_count > 0 {
        sources.push(CostSourceRef {
            source_kind: CostSourceKind::ProviderReported,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            catalog_digest: None,
            effective_at: None,
            fetched_at: None,
            freshness: CostSourceFreshness::NotApplicable,
            retrospective: true,
            formula_revision: None,
        });
    }
    let mut candidate_rates = preview
        .eligible_events
        .iter()
        .map(|candidate| candidate.rate_revision_id.as_str())
        .collect::<Vec<_>>();
    candidate_rates.sort_unstable();
    candidate_rates.dedup();
    for rate_revision_id in candidate_rates {
        if snapshot
            .models
            .iter()
            .find(|model| model.rate_digest(&snapshot.id) == rate_revision_id)
            .is_none()
        {
            continue;
        }
        sources.push(CostSourceRef {
            source_kind: CostSourceKind::ModelsDevCatalog,
            rate_revision_id: Some(rate_revision_id.to_owned()),
            catalog_snapshot_id: Some(snapshot.id.clone()),
            catalog_digest: Some(snapshot.revision_digest.clone()),
            effective_at: Some(rfc3339(snapshot.fetched_at)),
            fetched_at: Some(rfc3339(snapshot.fetched_at)),
            freshness: api_freshness(preview.catalog_freshness),
            retrospective: true,
            formula_revision: Some(pricing::COST_FORMULA_REVISION.to_owned()),
        });
    }
    Ok(CostSummary {
        kind,
        coverage,
        provider_reported,
        estimated,
        known_subtotal,
        complete_total,
        usage_coverage: UsageCostCoverage {
            total_runs_or_turns: total_events,
            pending_runs_or_turns: 0,
            no_provider_call_runs_or_turns: 0,
            fully_metered_runs_or_turns: metered,
            fully_costed_runs_or_turns: fully_costed_count,
            partially_costed_runs_or_turns: 0,
            unavailable_cost_runs_or_turns: unavailable_count,
            total_provider_attempts: total_events,
            settled_provider_attempts: total_events,
            pending_provider_attempts: 0,
            unsettled_provider_attempts: 0,
            metered_provider_attempts: metered,
            unmetered_provider_attempts: unmetered,
            costed_provider_attempts: fully_costed_count,
            unpriced_provider_attempts: unavailable_count,
            priced_tokens,
            unpriced_tokens,
            reasons,
        },
        sources,
    })
}

fn add_tokens(target: &mut TokenCounters, counters: EventTokenCounts) -> ApiResult<()> {
    target.input_tokens = target
        .input_tokens
        .checked_add(checked_i64(counters.input, "input token count")?)
        .ok_or_else(|| ApiError::internal("cost-estimation token count overflow"))?;
    target.output_tokens = target
        .output_tokens
        .checked_add(checked_i64(counters.output, "output token count")?)
        .ok_or_else(|| ApiError::internal("cost-estimation token count overflow"))?;
    target.cache_read_tokens = target
        .cache_read_tokens
        .checked_add(checked_i64(counters.cache_read, "cache-read token count")?)
        .ok_or_else(|| ApiError::internal("cost-estimation token count overflow"))?;
    target.cache_write_tokens = target
        .cache_write_tokens
        .checked_add(checked_i64(
            counters.cache_write,
            "cache-write token count",
        )?)
        .ok_or_else(|| ApiError::internal("cost-estimation token count overflow"))?;
    Ok(())
}

fn money_from_nanos(nanos: i128) -> ApiResult<MoneyAmount> {
    let nanos = i64::try_from(nanos)
        .map_err(|_| ApiError::internal("cost-estimation amount exceeds the supported range"))?;
    let amount = NanoUsd::from_nano_usd(nanos)
        .ok_or_else(|| ApiError::internal("cost-estimation amount is invalid"))?;
    Ok(MoneyAmount {
        currency: "USD".to_owned(),
        decimal: amount.to_usd_decimal(),
    })
}

fn persisted_summary(
    summary: &CostSummary,
    amount: Option<NanoUsd>,
    freshness: CatalogFreshness,
) -> String {
    let mut value = serde_json::to_value(summary).unwrap_or_else(|_| json!({}));
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "nano_usd".to_owned(),
            amount
                .map(|amount| json!(amount.as_nano_usd()))
                .unwrap_or(Value::Null),
        );
        object.insert("catalog_freshness".to_owned(), json!(freshness.as_str()));
    }
    value.to_string()
}

fn api_preview_from_db(preview: &DbCostEstimationPreview) -> ApiResult<ApiCostEstimationPreview> {
    api_preview_from_db_with_summary(preview, None)
}

fn api_preview_from_db_with_summary(
    preview: &DbCostEstimationPreview,
    summary: Option<CostSummary>,
) -> ApiResult<ApiCostEstimationPreview> {
    let projected_cost = match summary
        .or_else(|| serde_json::from_str::<CostSummary>(&preview.projected_cost_summary_json).ok())
    {
        Some(summary) => summary,
        None => empty_summary_from_nano(
            summary_nano(&preview.projected_cost_summary_json),
            preview.eligible_event_count,
            preview.unmatched_event_count,
            Some(service_catalog_freshness(preview.catalog_freshness)),
        )?,
    };
    Ok(ApiCostEstimationPreview {
        id: preview.id.clone(),
        project_id: preview.project_id.clone(),
        snapshot_id: preview.catalog_snapshot_id.clone(),
        usage_set_digest: preview.usage_set_digest.clone(),
        eligible_event_count: preview.eligible_event_count,
        unmatched_event_count: preview.unmatched_event_count,
        already_reported_event_count: preview.already_reported_event_count,
        projected_cost,
        expires_at: preview.expires_at.clone(),
    })
}

fn api_run_from_db(run: &DbCostEstimationRun) -> ApiResult<ApiCostEstimationRun> {
    let cost = match serde_json::from_str::<CostSummary>(&run.cost_summary_json) {
        Ok(cost) => cost,
        Err(_) => empty_summary_from_nano(
            summary_nano(&run.cost_summary_json),
            run.applied_event_count,
            run.unmatched_event_count,
            None,
        )?,
    };
    Ok(ApiCostEstimationRun {
        id: run.id.clone(),
        project_id: run.project_id.clone(),
        preview_id: run.preview_id.clone(),
        snapshot_id: run.catalog_snapshot_id.clone(),
        usage_set_digest: run.usage_set_digest.clone(),
        status: api_run_status(run.status),
        applied_event_count: run.applied_event_count,
        unmatched_event_count: run.unmatched_event_count,
        cost,
        created_at: run.created_at.clone(),
    })
}

fn summary_nano(summary: &str) -> Option<i64> {
    serde_json::from_str::<Value>(summary)
        .ok()?
        .get("nano_usd")
        .and_then(Value::as_i64)
}

fn empty_summary_from_nano(
    nano: Option<i64>,
    eligible: i64,
    unmatched: i64,
    freshness: Option<CatalogFreshness>,
) -> ApiResult<CostSummary> {
    if eligible < 0 || unmatched < 0 {
        return Err(ApiError::internal(
            "stored cost-estimation counts are invalid",
        ));
    }
    let total = eligible
        .checked_add(unmatched)
        .ok_or_else(|| ApiError::internal("stored cost-estimation counts overflow"))?;
    let amount = nano
        .map(|value| {
            let amount = NanoUsd::from_nano_usd(value)
                .ok_or_else(|| ApiError::internal("stored cost-estimation amount is invalid"))?;
            Ok::<MoneyAmount, ApiError>(MoneyAmount {
                currency: "USD".to_owned(),
                decimal: amount.to_usd_decimal(),
            })
        })
        .transpose()?;
    Ok(CostSummary {
        kind: amount
            .as_ref()
            .map_or(CostKind::None, |_| CostKind::Estimated),
        coverage: if unmatched > 0 {
            CostCoverage::Partial
        } else if eligible > 0 {
            CostCoverage::Complete
        } else {
            CostCoverage::NoUsage
        },
        provider_reported: None,
        estimated: amount.clone(),
        known_subtotal: amount.clone(),
        complete_total: (unmatched == 0).then_some(amount).flatten(),
        usage_coverage: UsageCostCoverage {
            total_runs_or_turns: total,
            pending_runs_or_turns: 0,
            no_provider_call_runs_or_turns: 0,
            fully_metered_runs_or_turns: eligible,
            fully_costed_runs_or_turns: eligible,
            partially_costed_runs_or_turns: 0,
            unavailable_cost_runs_or_turns: unmatched,
            total_provider_attempts: total,
            settled_provider_attempts: total,
            pending_provider_attempts: 0,
            unsettled_provider_attempts: 0,
            metered_provider_attempts: eligible,
            unmetered_provider_attempts: unmatched,
            costed_provider_attempts: eligible,
            unpriced_provider_attempts: unmatched,
            priced_tokens: zero_tokens(),
            unpriced_tokens: zero_tokens(),
            reasons: Vec::new(),
        },
        sources: freshness
            .map(|freshness| CostSourceRef {
                source_kind: CostSourceKind::ModelsDevCatalog,
                rate_revision_id: None,
                catalog_snapshot_id: None,
                catalog_digest: None,
                effective_at: None,
                fetched_at: None,
                freshness: api_freshness(freshness),
                retrospective: true,
                formula_revision: Some(pricing::COST_FORMULA_REVISION.to_owned()),
            })
            .into_iter()
            .collect(),
    })
}

fn zero_tokens() -> TokenCounters {
    TokenCounters {
        input_tokens: 0,
        output_tokens: 0,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    }
}

fn api_reason(reason: PriceSelectionReasonCode) -> CostCoverageReasonCode {
    match reason {
        PriceSelectionReasonCode::MissingProvider => CostCoverageReasonCode::MissingProvider,
        PriceSelectionReasonCode::MissingModel => CostCoverageReasonCode::MissingModel,
        PriceSelectionReasonCode::MissingBinding | PriceSelectionReasonCode::RetiredBinding => {
            CostCoverageReasonCode::MissingBinding
        }
        PriceSelectionReasonCode::IdentityMismatch => CostCoverageReasonCode::IdentityMismatch,
        PriceSelectionReasonCode::Unmetered => CostCoverageReasonCode::Unmetered,
        PriceSelectionReasonCode::MissingRate => CostCoverageReasonCode::MissingRate,
        PriceSelectionReasonCode::UnresolvedTier => CostCoverageReasonCode::UnresolvedTier,
    }
}

fn api_freshness(freshness: CatalogFreshness) -> CostSourceFreshness {
    match freshness {
        CatalogFreshness::Fresh => CostSourceFreshness::Fresh,
        CatalogFreshness::Stale => CostSourceFreshness::Stale,
        CatalogFreshness::RefreshFailed => CostSourceFreshness::RefreshFailed,
        CatalogFreshness::NotApplicable => CostSourceFreshness::NotApplicable,
    }
}

fn db_catalog_freshness(freshness: CatalogFreshness) -> ApiResult<db::PricingCatalogFreshness> {
    match freshness {
        CatalogFreshness::Fresh => Ok(db::PricingCatalogFreshness::Fresh),
        CatalogFreshness::Stale => Ok(db::PricingCatalogFreshness::Stale),
        CatalogFreshness::RefreshFailed => Ok(db::PricingCatalogFreshness::RefreshFailed),
        CatalogFreshness::NotApplicable => {
            Err(ApiError::validation("catalog freshness is unavailable"))
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

fn api_run_status(status: db::CostEstimationRunStatus) -> ApiRunStatus {
    match status {
        db::CostEstimationRunStatus::Pending => ApiRunStatus::Pending,
        db::CostEstimationRunStatus::Committed => ApiRunStatus::Committed,
        db::CostEstimationRunStatus::Failed => ApiRunStatus::Failed,
        db::CostEstimationRunStatus::Conflicted => ApiRunStatus::Conflicted,
        db::CostEstimationRunStatus::Superseded => ApiRunStatus::Superseded,
    }
}

fn rfc3339(time: SystemTime) -> String {
    DateTime::<Utc>::from(time).to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn parse_system_time(value: &str, field: &str) -> ApiResult<SystemTime> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc).into())
        .map_err(|_| ApiError::validation(format!("{field} must be a valid RFC3339 timestamp")))
}

fn validate_required_identifier(value: &str, field: &str) -> ApiResult<()> {
    if value.trim().is_empty() {
        return Err(ApiError::validation(format!("{field} is required")));
    }
    if value.len() > MAX_QUERY_TEXT_BYTES {
        return Err(ApiError::validation(format!(
            "{field} exceeds the supported length"
        )));
    }
    Ok(())
}

fn checked_i64<T>(value: T, field: &str) -> ApiResult<i64>
where
    T: TryInto<i64>,
{
    value
        .try_into()
        .map_err(|_| ApiError::internal(format!("{field} exceeds the supported range")))
}

// -------------------------------------------------------------------------
// Bounded boundary error mapping
// -------------------------------------------------------------------------

fn map_catalog_client_error(error: CatalogClientError) -> ApiError {
    match error {
        CatalogClientError::InvalidRequest(_) => {
            ApiError::validation("pricing catalog refresh request is invalid")
        }
        CatalogClientError::Repository(error) => map_catalog_repository_error(error),
    }
}

fn map_catalog_repository_error(error: services::pricing::CatalogRepositoryError) -> ApiError {
    match error {
        services::pricing::CatalogRepositoryError::Conflict(_) => ApiError::conflict_with_code(
            "pricing_catalog.conflict",
            "pricing catalog changed before the operation completed",
        ),
        services::pricing::CatalogRepositoryError::Unavailable(_) => {
            ApiError::internal("pricing catalog is temporarily unavailable")
        }
    }
}

fn map_binding_mutation_error(error: services::pricing::CatalogRepositoryError) -> ApiError {
    match error {
        services::pricing::CatalogRepositoryError::Conflict(message) => {
            let lower = message.to_ascii_lowercase();
            if lower.contains("version conflict")
                || lower.contains("idempotency")
                || lower.contains("changed while")
            {
                ApiError::conflict_with_code(
                    if lower.contains("idempotency") {
                        "idempotency_conflict"
                    } else {
                        "pricing.version_conflict"
                    },
                    "pricing configuration changed before the operation completed",
                )
            } else {
                ApiError::validation("pricing binding request is invalid")
            }
        }
        services::pricing::CatalogRepositoryError::Unavailable(_) => {
            ApiError::internal("pricing configuration is temporarily unavailable")
        }
    }
}

fn map_db_read_error(error: DbError) -> ApiError {
    match error {
        DbError::InvalidCursor => ApiError::validation("cursor is invalid"),
        DbError::VersionConflict | DbError::IdempotencyConflict => ApiError::conflict_with_code(
            "conflict",
            "the requested pricing resource changed before it could be read",
        ),
        DbError::NotFound => ApiError::not_found("pricing_resource", "requested"),
        _ => ApiError::internal("pricing database is temporarily unavailable"),
    }
}

fn map_db_mutation_error(error: DbError) -> ApiError {
    match error {
        DbError::VersionConflict => ApiError::conflict_with_code(
            "version_conflict",
            "pricing resource changed before the mutation completed",
        ),
        DbError::IdempotencyConflict => ApiError::conflict_with_code(
            "idempotency_conflict",
            "idempotency key was reused with a different mutation",
        ),
        DbError::NotFound => ApiError::not_found("pricing_resource", "requested"),
        DbError::InvalidCursor => ApiError::validation("cursor is invalid"),
        _ => ApiError::internal("pricing database is temporarily unavailable"),
    }
}

fn map_retrospective_error(error: RetrospectiveRepositoryError) -> ApiError {
    match error {
        RetrospectiveRepositoryError::Expired => ApiError::conflict_with_code(
            "cost_estimation.preview_expired",
            "cost-estimation preview has expired",
        ),
        RetrospectiveRepositoryError::UsageSetConflict => ApiError::conflict_with_code(
            "usage_set_conflict",
            "preview source usage changed; create a new preview",
        ),
        RetrospectiveRepositoryError::Conflict => {
            ApiError::conflict_with_code("conflict", "cost-estimation mutation conflicted")
        }
        RetrospectiveRepositoryError::Unavailable(_) => {
            ApiError::internal("cost-estimation service is temporarily unavailable")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_identity_keeps_scheme_port_and_custom_path_distinct() {
        assert_eq!(
            canonical_endpoint_class("HTTPS://API.OpenAI.com/v1/").as_deref(),
            Some("https://api.openai.com/v1")
        );
        assert_eq!(
            canonical_endpoint_class(
                "https://user:sentinel-secret@api.openai.com/v1/?token=sentinel#fragment"
            )
            .as_deref(),
            Some("https://api.openai.com/v1")
        );
        assert_ne!(
            canonical_endpoint_class("https://api.openai.com/v1"),
            canonical_endpoint_class("https://api.openai.com:8443/v1")
        );
        assert_ne!(
            canonical_endpoint_class("https://api.openai.com/v1"),
            canonical_endpoint_class("https://api.openai.com/v2")
        );
    }

    #[test]
    fn retrospective_amount_overflow_is_an_error_not_silent_clamping() {
        assert!(money_from_nanos(i128::from(i64::MAX) + 1).is_err());
        let mut counters = TokenCounters {
            input_tokens: 1,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        };
        let max = u64::try_from(i64::MAX).expect("i64 max fits u64");
        assert!(add_tokens(&mut counters, EventTokenCounts::new(max, 0, 0, 0)).is_err());
    }

    #[test]
    fn cli_identity_persists_only_an_opaque_digest() {
        let detected = api_types::DetectedCli {
            kind: "shell".to_owned(),
            availability: "authenticated".to_owned(),
            config_path: Some(
                "https://user:sentinel-secret@example.test/config?token=sentinel".to_owned(),
            ),
            version: Some(
                "https://user:sentinel-secret@example.test/version?token=sentinel".to_owned(),
            ),
            path: Some("https://user:sentinel-secret@example.test/bin?token=sentinel".to_owned()),
        };
        let identity = cli_identity("shell", &detected);
        let persisted = identity_json(&identity).to_string();
        assert!(!persisted.contains("sentinel"));
        assert!(!persisted.contains("example.test"));
        assert!(identity
            .runtime_fingerprint
            .as_deref()
            .is_some_and(|value| value.starts_with("sha256:")));

        let response = serde_json::to_string(&ProviderPricing {
            subject_id: "subject-1".to_owned(),
            subject_revision_digest: pricing::pricing_subject_revision_digest(&identity),
            version: 1,
            bindings: Vec::new(),
        })
        .expect("pricing response serializes");
        assert!(!response.contains("sentinel"));
        assert!(!response.contains("example.test"));
    }
}
