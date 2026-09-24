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
    AgentPricing, CatalogModelRate, CatalogModelSourceKind, CostCoverage, CostCoverageReason,
    CostCoverageReasonCode, CostEstimationPreview as ApiCostEstimationPreview,
    CostEstimationRun as ApiCostEstimationRun, CostEstimationRunStatus as ApiRunStatus, CostKind,
    CostSourceFreshness, CostSourceKind, CostSourceRef, CostSummary,
    CreateCostEstimationPreviewRequest, CreateCostEstimationRunRequest, DeletePricingSettingsQuery,
    MoneyAmount, PricingAdjustmentSource, PricingCatalogModelsQuery, PricingCatalogModelsResponse,
    PricingCatalogRefreshRequest, PricingCatalogState, PricingCatalogStatus, PricingMode,
    PricingResolutionStatus, PricingSettings, RateAmount, RateBuckets as ApiRateBuckets,
    SubjectPricingResponse, TokenCounters, UpdatePricingSettingsRequest, UsageCostCoverage,
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
    now_rfc3339, AgentRepo, CostEstimationPreview as DbCostEstimationPreview,
    CostEstimationRun as DbCostEstimationRun, CredentialHandleRepo, DaemonRepo, DbError,
    PageRequest, PricingAdjustment, PricingAdjustmentMode, PricingAdjustmentScope,
    PricingCatalogModelQuery as DbCatalogModelQuery, PricingCatalogRepo, PricingSubjectRepo,
    Project, ProjectRepo, RetrospectiveEstimateRepo, SortBy, SortOrder, UsageEvent,
    UsageLedgerRepo,
};
use serde_json::{json, Value};
use services::{
    pricing::{
        self, CatalogClientError, CatalogFreshness, CatalogSnapshot, CatalogStatus,
        EventBucketRates, EventTokenCounts, NanoUsd, NanoUsdPerMillion, PriceSelectionReasonCode,
        RetrospectiveCommitRequest, RetrospectivePreview, RetrospectiveRepositoryError,
        RetrospectiveUsageEvent,
    },
    pricing_auto,
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
// Provider entry, CLI runtime, and agent pricing adjustments
// -------------------------------------------------------------------------

pub async fn get_provider_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<SubjectPricingResponse>> {
    let scope = provider_scope(&state, &user.user_id, &id).await?;
    subject_pricing_response(&state, &user.user_id, &scope)
        .await
        .map(Json)
}

pub async fn update_provider_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<UpdatePricingSettingsRequest>,
) -> ApiResult<Json<SubjectPricingResponse>> {
    let scope = provider_scope(&state, &user.user_id, &id).await?;
    upsert_adjustment(&state, &user.user_id, scope.clone(), request).await?;
    subject_pricing_response(&state, &user.user_id, &scope)
        .await
        .map(Json)
}

pub async fn delete_provider_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Query(query): Query<DeletePricingSettingsQuery>,
) -> ApiResult<Json<SubjectPricingResponse>> {
    let scope = provider_scope(&state, &user.user_id, &id).await?;
    delete_adjustment(&state, &user.user_id, &scope, query.version).await?;
    subject_pricing_response(&state, &user.user_id, &scope)
        .await
        .map(Json)
}

pub async fn get_cli_runtime_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((daemon_id, executor_type)): Path<(String, String)>,
) -> ApiResult<Json<SubjectPricingResponse>> {
    let scope = cli_runtime_scope(&state, &user.user_id, &daemon_id, &executor_type).await?;
    subject_pricing_response(&state, &user.user_id, &scope)
        .await
        .map(Json)
}

pub async fn update_cli_runtime_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((daemon_id, executor_type)): Path<(String, String)>,
    Json(request): Json<UpdatePricingSettingsRequest>,
) -> ApiResult<Json<SubjectPricingResponse>> {
    let scope = cli_runtime_scope(&state, &user.user_id, &daemon_id, &executor_type).await?;
    upsert_adjustment(&state, &user.user_id, scope.clone(), request).await?;
    subject_pricing_response(&state, &user.user_id, &scope)
        .await
        .map(Json)
}

pub async fn delete_cli_runtime_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((daemon_id, executor_type)): Path<(String, String)>,
    Query(query): Query<DeletePricingSettingsQuery>,
) -> ApiResult<Json<SubjectPricingResponse>> {
    let scope = cli_runtime_scope(&state, &user.user_id, &daemon_id, &executor_type).await?;
    delete_adjustment(&state, &user.user_id, &scope, query.version).await?;
    subject_pricing_response(&state, &user.user_id, &scope)
        .await
        .map(Json)
}

pub async fn get_agent_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<AgentPricing>> {
    let agent = owned_agent(&state, &user.user_id, &id).await?;
    agent_pricing_response(&state, &user.user_id, &agent)
        .await
        .map(Json)
}

pub async fn update_agent_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<UpdatePricingSettingsRequest>,
) -> ApiResult<Json<AgentPricing>> {
    let agent = owned_agent(&state, &user.user_id, &id).await?;
    upsert_adjustment(
        &state,
        &user.user_id,
        PricingAdjustmentScope::Agent(agent.id.clone()),
        request,
    )
    .await?;
    agent_pricing_response(&state, &user.user_id, &agent)
        .await
        .map(Json)
}

pub async fn delete_agent_pricing(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Query(query): Query<DeletePricingSettingsQuery>,
) -> ApiResult<Json<AgentPricing>> {
    let agent = owned_agent(&state, &user.user_id, &id).await?;
    delete_adjustment(
        &state,
        &user.user_id,
        &PricingAdjustmentScope::Agent(agent.id.clone()),
        query.version,
    )
    .await?;
    agent_pricing_response(&state, &user.user_id, &agent)
        .await
        .map(Json)
}

async fn provider_scope(
    state: &AppState,
    user_id: &str,
    provider_entry_id: &str,
) -> ApiResult<PricingAdjustmentScope> {
    let handle = CredentialHandleRepo::get_credential_handle_for_owner(
        &*state.db,
        provider_entry_id,
        user_id,
    )
    .await
    .map_err(map_db_read_error)?
    .filter(|handle| handle.status != "revoked")
    .ok_or_else(|| ApiError::not_found("provider_entry", provider_entry_id.to_owned()))?;
    Ok(PricingAdjustmentScope::ProviderEntry(handle.id))
}

async fn cli_runtime_scope(
    state: &AppState,
    user_id: &str,
    daemon_id: &str,
    executor_type: &str,
) -> ApiResult<PricingAdjustmentScope> {
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
    if pricing_auto::detected_cli(&daemon.detected_clis_json, executor_type).is_none() {
        return Err(ApiError::not_found(
            "cli_runtime",
            format!("{daemon_id}/{executor_type}"),
        ));
    }
    Ok(PricingAdjustmentScope::CliRuntime {
        daemon_id: daemon_id.to_owned(),
        executor_type: executor_type.to_owned(),
    })
}

async fn owned_agent(state: &AppState, user_id: &str, agent_id: &str) -> ApiResult<db::Agent> {
    AgentRepo::get_by_id(&*state.db, agent_id)
        .await
        .map_err(map_db_read_error)?
        .filter(|agent| agent.owner_id.as_deref() == Some(user_id))
        .ok_or_else(|| ApiError::not_found("agent", agent_id.to_owned()))
}

async fn subject_pricing_response(
    state: &AppState,
    user_id: &str,
    scope: &PricingAdjustmentScope,
) -> ApiResult<SubjectPricingResponse> {
    let adjustment = PricingSubjectRepo::get_pricing_adjustment(&*state.db, user_id, scope)
        .await
        .map_err(map_db_read_error)?;
    Ok(SubjectPricingResponse {
        settings: adjustment.as_ref().map(api_settings).transpose()?,
    })
}

async fn agent_pricing_response(
    state: &AppState,
    user_id: &str,
    agent: &db::Agent,
) -> ApiResult<AgentPricing> {
    let preview = pricing_auto::preview_agent_price(&state.db, user_id, agent)
        .await
        .map_err(|_| ApiError::internal("agent pricing could not be resolved"))?;
    let (status, catalog_row, candidates, desired, source) = match &preview.resolution {
        None => (
            PricingResolutionStatus::NoModel,
            None,
            Vec::new(),
            None,
            if preview.agent_adjustment.is_some() {
                PricingAdjustmentSource::Agent
            } else if preview.provider_adjustment.is_some() {
                PricingAdjustmentSource::Provider
            } else {
                PricingAdjustmentSource::Default
            },
        ),
        Some(resolution) => {
            let (status, row, candidates) = match &resolution.catalog {
                pricing_auto::CatalogMatch::Found(row) => {
                    (PricingResolutionStatus::Priced, Some(row), Vec::new())
                }
                pricing_auto::CatalogMatch::Ambiguous(providers) => {
                    (PricingResolutionStatus::Ambiguous, None, providers.clone())
                }
                pricing_auto::CatalogMatch::NotFound => {
                    (PricingResolutionStatus::NotInCatalog, None, Vec::new())
                }
                pricing_auto::CatalogMatch::CatalogAbsent => {
                    (PricingResolutionStatus::CatalogAbsent, None, Vec::new())
                }
            };
            // A fixed price does not need a catalog row to be priced.
            let status = if resolution.desired.is_some() {
                PricingResolutionStatus::Priced
            } else {
                status
            };
            (
                status,
                row,
                candidates,
                resolution.desired.as_ref(),
                match resolution.adjustment.source {
                    pricing_auto::AdjustmentSource::Agent => PricingAdjustmentSource::Agent,
                    pricing_auto::AdjustmentSource::Provider => PricingAdjustmentSource::Provider,
                    pricing_auto::AdjustmentSource::Default => PricingAdjustmentSource::Default,
                },
            )
        }
    };
    Ok(AgentPricing {
        agent_id: agent.id.clone(),
        runtime_model: preview.runtime_model.clone(),
        settings: preview
            .agent_adjustment
            .as_ref()
            .map(api_settings)
            .transpose()?,
        provider_settings: preview
            .provider_adjustment
            .as_ref()
            .map(api_settings)
            .transpose()?,
        source,
        status,
        catalog_provider_id: catalog_row.and_then(|row| row.catalog_provider_id.clone()),
        catalog_model_id: catalog_row.and_then(|row| row.catalog_model_id.clone()),
        catalog_rates: catalog_row
            .map(|row| api_db_rate_buckets(row.rates))
            .transpose()?,
        effective_rates: desired
            .map(|desired| api_db_rate_buckets(desired.rates()))
            .transpose()?,
        candidate_providers: candidates,
    })
}

fn api_db_rate_buckets(rates: db::RateBuckets) -> ApiResult<ApiRateBuckets> {
    Ok(ApiRateBuckets {
        input: api_rate_amount(rates.input)?,
        output: api_rate_amount(rates.output)?,
        cache_read: api_rate_amount(rates.cache_read)?,
        cache_write: api_rate_amount(rates.cache_write)?,
    })
}

fn api_settings(adjustment: &PricingAdjustment) -> ApiResult<PricingSettings> {
    Ok(PricingSettings {
        mode: match adjustment.mode {
            PricingAdjustmentMode::List => PricingMode::List,
            PricingAdjustmentMode::Discount => PricingMode::Discount,
            PricingAdjustmentMode::Fixed => PricingMode::Fixed,
        },
        discount_percent: adjustment.discount_bps.map(discount_percent_text),
        fixed_rates: (adjustment.mode == PricingAdjustmentMode::Fixed)
            .then(|| api_db_rate_buckets(adjustment.fixed_rates))
            .transpose()?,
        catalog_provider_id: adjustment.catalog_provider_id.clone(),
        catalog_model_id: adjustment.catalog_model_id.clone(),
        version: adjustment.version,
    })
}

/// `2050` basis points → `"20.5"`.
fn discount_percent_text(bps: i64) -> String {
    let whole = bps / 100;
    let fraction = bps % 100;
    match fraction {
        0 => whole.to_string(),
        f if f % 10 == 0 => format!("{whole}.{}", f / 10),
        f => format!("{whole}.{f:02}"),
    }
}

/// `"20.5"` → `2050` basis points; 0–100 with at most two decimals.
fn parse_discount_percent(value: &str) -> ApiResult<i64> {
    let invalid = || {
        ApiError::validation("discount_percent must be 0 to 100 with at most two decimal places")
    };
    let value = value.trim();
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || fraction.len() > 2
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || whole.len() > 3
    {
        return Err(invalid());
    }
    let whole: i64 = whole.parse().map_err(|_| invalid())?;
    let fraction: i64 = format!("{fraction:0<2}").parse().map_err(|_| invalid())?;
    let bps = whole * 100 + fraction;
    if !(0..=10_000).contains(&bps) {
        return Err(invalid());
    }
    Ok(bps)
}

fn db_fixed_rates(rates: &ApiRateBuckets) -> ApiResult<db::RateBuckets> {
    let rates = service_rate_buckets(rates)?;
    Ok(db::RateBuckets::new(
        rates.input.map(NanoUsdPerMillion::as_nano_usd_per_million),
        rates.output.map(NanoUsdPerMillion::as_nano_usd_per_million),
        rates
            .cache_read
            .map(NanoUsdPerMillion::as_nano_usd_per_million),
        rates
            .cache_write
            .map(NanoUsdPerMillion::as_nano_usd_per_million),
    ))
}

async fn upsert_adjustment(
    state: &AppState,
    user_id: &str,
    scope: PricingAdjustmentScope,
    request: UpdatePricingSettingsRequest,
) -> ApiResult<()> {
    if request.expected_version < 0 {
        return Err(ApiError::validation(
            "expected_version must be zero or greater",
        ));
    }
    let catalog_provider_id = request
        .catalog_provider_id
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let catalog_model_id = request
        .catalog_model_id
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    for (value, field) in [
        (catalog_provider_id.as_deref(), "catalog_provider_id"),
        (catalog_model_id.as_deref(), "catalog_model_id"),
    ] {
        if let Some(value) = value {
            validate_required_identifier(value, field)?;
        }
    }
    if catalog_model_id.is_some() && !matches!(scope, PricingAdjustmentScope::Agent(_)) {
        return Err(ApiError::validation(
            "catalog_model_id can only be pinned on an agent",
        ));
    }
    if catalog_model_id.is_some() && catalog_provider_id.is_none() {
        return Err(ApiError::validation(
            "catalog_model_id requires catalog_provider_id",
        ));
    }
    if let Some(provider_id) = catalog_provider_id.as_deref() {
        let listed =
            pricing_auto::catalog_lists(&state.db, provider_id, catalog_model_id.as_deref())
                .await
                .map_err(|_| ApiError::internal("pricing catalog is temporarily unavailable"))?;
        if !listed {
            return Err(ApiError::validation(
                "the pinned models.dev provider/model is not in the current catalog",
            ));
        }
    }
    let (mode, discount_bps, fixed_rates) = match request.mode {
        PricingMode::List => {
            if request.discount_percent.is_some() || request.fixed_rates.is_some() {
                return Err(ApiError::validation(
                    "list pricing takes neither discount_percent nor fixed_rates",
                ));
            }
            (
                PricingAdjustmentMode::List,
                None,
                db::RateBuckets::default(),
            )
        }
        PricingMode::Discount => {
            if request.fixed_rates.is_some() {
                return Err(ApiError::validation(
                    "discount pricing does not take fixed_rates",
                ));
            }
            let percent = request.discount_percent.as_deref().ok_or_else(|| {
                ApiError::validation("discount pricing requires discount_percent")
            })?;
            (
                PricingAdjustmentMode::Discount,
                Some(parse_discount_percent(percent)?),
                db::RateBuckets::default(),
            )
        }
        PricingMode::Fixed => {
            if request.discount_percent.is_some() {
                return Err(ApiError::validation(
                    "fixed pricing does not take discount_percent",
                ));
            }
            let rates = request
                .fixed_rates
                .as_ref()
                .ok_or_else(|| ApiError::validation("fixed pricing requires fixed_rates"))?;
            let rates = db_fixed_rates(rates)?;
            if [
                rates.input,
                rates.output,
                rates.cache_read,
                rates.cache_write,
            ]
            .iter()
            .all(Option::is_none)
            {
                return Err(ApiError::validation(
                    "fixed pricing requires at least one rate",
                ));
            }
            (PricingAdjustmentMode::Fixed, None, rates)
        }
    };
    PricingSubjectRepo::upsert_pricing_adjustment(
        &*state.db,
        db::UpsertPricingAdjustment {
            owner_user_id: user_id.to_owned(),
            scope,
            mode,
            discount_bps,
            fixed_rates,
            catalog_provider_id,
            catalog_model_id,
            expected_version: request.expected_version,
            now: now_rfc3339(),
        },
    )
    .await
    .map_err(map_db_mutation_error)?;
    Ok(())
}

async fn delete_adjustment(
    state: &AppState,
    user_id: &str,
    scope: &PricingAdjustmentScope,
    version: i64,
) -> ApiResult<()> {
    PricingSubjectRepo::delete_pricing_adjustment(&*state.db, user_id, scope, version)
        .await
        .map_err(map_db_mutation_error)
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
        use pricing_auto::canonical_endpoint_class;
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
        let identity = pricing_auto::cli_identity("shell", Some(&detected));
        let persisted = format!("{identity:?}");
        assert!(!persisted.contains("sentinel"));
        assert!(!persisted.contains("example.test"));
        assert!(identity
            .runtime_fingerprint
            .as_deref()
            .is_some_and(|value| value.starts_with("sha256:")));

        let digest = pricing::pricing_subject_revision_digest(&identity);
        assert!(!digest.contains("sentinel"));
        assert!(!digest.contains("example.test"));
    }
}
