use api_types::{
    AgentProviderId, CancelProviderAuthorizationRequest, ProviderAuthorizationCallbackQuery,
    ProviderAuthorizationOperationResponse, ProviderAuthorizationState, ProviderCredentialMethod,
    StartProviderAuthorizationRequest,
};
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap},
    response::Redirect,
    Json,
};
use db::ProviderAuthorizationOperation;

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::AuthenticatedUser,
    state::AppState,
};

pub async fn start_provider_authorization(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    headers: HeaderMap,
    Json(request): Json<StartProviderAuthorizationRequest>,
) -> ApiResult<Json<ProviderAuthorizationOperationResponse>> {
    let operation = state
        .provider_authorization_service
        .start(user.user_id, request, request_origin(&headers))
        .await?;
    Ok(Json(operation_response(operation)?))
}

/// The origin this request arrived from: the browser-asserted `Origin` header
/// when present, otherwise the `Host` the client dialed (honoring
/// `X-Forwarded-Proto` behind a reverse proxy).
fn request_origin(headers: &HeaderMap) -> Option<String> {
    if let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    {
        return Some(origin.to_owned());
    }
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())?;
    let proto = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("http");
    Some(format!("{proto}://{host}"))
}

pub async fn get_provider_authorization(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<ProviderAuthorizationOperationResponse>> {
    let operation = state
        .provider_authorization_service
        .get(&id, &user.user_id)
        .await?;
    Ok(Json(operation_response(operation)?))
}

pub async fn cancel_provider_authorization(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<CancelProviderAuthorizationRequest>,
) -> ApiResult<Json<ProviderAuthorizationOperationResponse>> {
    let operation = state
        .provider_authorization_service
        .cancel(&id, &user.user_id, request.expected_version)
        .await?;
    Ok(Json(operation_response(operation)?))
}

pub async fn provider_authorization_callback(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Query(query): Query<ProviderAuthorizationCallbackQuery>,
) -> ApiResult<Redirect> {
    let callback_state = query
        .state
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("provider authorization callback is missing state"))?;
    let operation = state
        .provider_authorization_service
        .callback(
            &provider,
            callback_state,
            query.code.as_deref(),
            query.error.as_deref(),
        )
        .await?;
    Ok(Redirect::to(&format!(
        "{}/agents?provider={}&status={}&authorization={}",
        operation.redirect_origin, operation.provider, operation.status, operation.id
    )))
}

pub(crate) fn operation_response(
    operation: ProviderAuthorizationOperation,
) -> ApiResult<ProviderAuthorizationOperationResponse> {
    Ok(ProviderAuthorizationOperationResponse {
        id: operation.id,
        provider: parse_provider(&operation.provider)?,
        method: parse_method(&operation.method)?,
        state: parse_state(&operation.status)?,
        authorization_url: operation.authorization_url,
        user_code: operation.user_code,
        expires_at: operation.expires_at,
        poll_interval_seconds: operation.poll_interval_seconds.max(1) as u32,
        credential_handle_id: operation.credential_handle_id,
        error_code: operation.error_code,
        error_message: operation.error_message,
        version: operation.version,
        created_at: operation.created_at,
        updated_at: operation.updated_at,
        completed_at: operation.completed_at,
    })
}

fn parse_provider(value: &str) -> ApiResult<AgentProviderId> {
    match value {
        "openai" => Ok(AgentProviderId::OpenAi),
        "xai" => Ok(AgentProviderId::XAi),
        "gemini" => Ok(AgentProviderId::Gemini),
        "openrouter" => Ok(AgentProviderId::OpenRouter),
        "openai_compatible" => Ok(AgentProviderId::OpenAiCompatible),
        _ => Err(ApiError::internal("stored provider is invalid")),
    }
}

fn parse_method(value: &str) -> ApiResult<ProviderCredentialMethod> {
    match value {
        "api_key" => Ok(ProviderCredentialMethod::ApiKey),
        "browser_oauth" => Ok(ProviderCredentialMethod::BrowserOauth),
        "device_oauth" => Ok(ProviderCredentialMethod::DeviceOauth),
        _ => Err(ApiError::internal("stored credential method is invalid")),
    }
}

fn parse_state(value: &str) -> ApiResult<ProviderAuthorizationState> {
    match value {
        "starting" => Ok(ProviderAuthorizationState::Starting),
        "awaiting_browser" => Ok(ProviderAuthorizationState::AwaitingBrowser),
        "awaiting_device" => Ok(ProviderAuthorizationState::AwaitingDevice),
        "polling" => Ok(ProviderAuthorizationState::Polling),
        "exchanging" => Ok(ProviderAuthorizationState::Exchanging),
        "verifying" => Ok(ProviderAuthorizationState::Verifying),
        "publishing" => Ok(ProviderAuthorizationState::Publishing),
        "succeeded" => Ok(ProviderAuthorizationState::Succeeded),
        "denied" => Ok(ProviderAuthorizationState::Denied),
        "expired" => Ok(ProviderAuthorizationState::Expired),
        "cancelled" => Ok(ProviderAuthorizationState::Cancelled),
        "failed" => Ok(ProviderAuthorizationState::Failed),
        _ => Err(ApiError::internal("stored authorization state is invalid")),
    }
}
