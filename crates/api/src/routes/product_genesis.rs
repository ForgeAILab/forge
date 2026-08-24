//! Product Genesis routes on the account's existing Main Agent Chat.
//!
//! Genesis is an account-owned lifecycle, not another chat resource.  The
//! route derives the Main Chat from the authenticated account/binding, then
//! uses the normal AgentChatService to admit the visible discovery turn.  It
//! never accepts a caller-supplied chat or account identity as authority.

use api_types::{
    ApplyProductGenesisGuidedSetupRequest, CancelProductGenesisRequest,
    ProductGenesisActiveResponse, ProductGenesisStartResponse, StartProductGenesisRequest,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use db::new_uuid_v4;
use services::{
    MainGenesisCommandService, MainGenesisStartCommandInput, MainGenesisStartPrincipal,
    MainGenesisStartRequest, ProductGenesisService,
};

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::AuthenticatedUser,
    state::AppState,
};

/// Start Product Genesis in the existing global Main Agent Chat.
pub async fn start_product_genesis(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Json(request): Json<StartProductGenesisRequest>,
) -> ApiResult<(StatusCode, Json<ProductGenesisStartResponse>)> {
    let start = MainGenesisCommandService::new(state.db.clone())
        .start(MainGenesisStartCommandInput {
            principal: MainGenesisStartPrincipal::User {
                user_id: user.user_id,
            },
            request: MainGenesisStartRequest {
                maturity: request.maturity,
                initial_idea: request.initial_idea,
                preferred_project_agent_identity_id: request.preferred_project_agent_identity_id,
            },
            idempotency_key: request.idempotency_key,
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            policy_result: "allowed".to_owned(),
            requested_permission: "propose_discovery".to_owned(),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(ProductGenesisStartResponse {
            main_chat_id: start.main_chat_id,
            session: start.session,
            admitted_turn_id: Some(start.admitted_turn_id),
        }),
    ))
}

/// Return the authenticated account's active Genesis session, if any.
pub async fn get_active_product_genesis(
    State(state): State<AppState>,
    user: AuthenticatedUser,
) -> ApiResult<Json<ProductGenesisActiveResponse>> {
    let genesis = ProductGenesisService::for_sqlite(state.db.clone());
    Ok(Json(ProductGenesisActiveResponse {
        session: genesis.active(&user.user_id).await?,
    }))
}

/// Read one Genesis session from the authenticated account's history.
///
/// A session identifier is only a lookup key: ownership is checked against
/// the authenticated account before the durable record is returned.
pub async fn get_product_genesis(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(session_id): Path<String>,
) -> ApiResult<Json<api_types::ProductGenesisSession>> {
    let genesis = ProductGenesisService::for_sqlite(state.db.clone());
    let session = genesis.get(&session_id).await?;
    if session.account_id != user.user_id {
        return Err(ApiError::not_found("product_genesis_session", session_id));
    }
    Ok(Json(session))
}

/// Cancel an active Genesis session with optimistic concurrency.
pub async fn cancel_product_genesis(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(session_id): Path<String>,
    Json(request): Json<CancelProductGenesisRequest>,
) -> ApiResult<Json<api_types::ProductGenesisSession>> {
    let genesis = ProductGenesisService::for_sqlite(state.db.clone());
    let current = genesis.get(&session_id).await?;
    if current.account_id != user.user_id {
        return Err(ApiError::not_found("product_genesis_session", session_id));
    }
    let session = genesis
        .cancel(&current.id, request.expected_version, request.reason)
        .await?;
    Ok(Json(session))
}

/// Apply guided setup (maturity and/or preferred agent) to an active Genesis session at most once.
pub async fn apply_product_genesis_guided_setup(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(session_id): Path<String>,
    Json(request): Json<ApplyProductGenesisGuidedSetupRequest>,
) -> ApiResult<Json<api_types::ProductGenesisSession>> {
    let genesis = ProductGenesisService::for_sqlite(state.db.clone());
    let current = genesis.get(&session_id).await?;
    if current.account_id != user.user_id {
        return Err(ApiError::not_found("product_genesis_session", session_id));
    }
    if let Some(identity_id) = request.preferred_project_agent_identity_id.as_deref() {
        services::resolve_requested_genesis_project_agent(&state.db, &current, identity_id).await?;
    }
    let session = genesis
        .apply_guided_setup(
            &current.id,
            request.expected_version,
            request.maturity,
            request.preferred_project_agent_identity_id,
            request.provenance,
        )
        .await?;
    Ok(Json(session))
}
