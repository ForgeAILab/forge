//! Product Genesis routes on the account's existing Main Agent Chat.
//!
//! Genesis is an account-owned lifecycle, not another chat resource.  The
//! route derives the Main Chat from the authenticated account/binding, then
//! uses the normal AgentChatService to admit the visible discovery turn.  It
//! never accepts a caller-supplied chat or account identity as authority.

use api_types::{
    ApplyProductGenesisGuidedSetupRequest, CancelProductGenesisRequest,
    ProductGenesisActiveResponse, ProductGenesisStartResponse, ProductMaturity,
    StartProductGenesisRequest,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use db::AccountMainAgentBindingRepo;
use services::{GenesisPromptContext, ProductGenesisService, SendAgentChatMessageInput};

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
    let genesis = ProductGenesisService::for_sqlite(state.db.clone());
    let main_chat_id =
        if AccountMainAgentBindingRepo::get_active_main_binding(&*state.db, &user.user_id)
            .await?
            .is_some()
        {
            Some(
                state
                    .agent_chat_service
                    .ensure_main_chat(&user.user_id)
                    .await?
                    .id,
            )
        } else {
            // Passing None intentionally produces the explicit setup-required
            // result without creating a Genesis row or admitting a turn.
            None
        };
    let maturity = request.maturity.unwrap_or(ProductMaturity::Mvp);
    let initial_idea = request.initial_idea.clone();
    let start = genesis
        .start(
            &user.user_id,
            main_chat_id.as_deref(),
            maturity,
            initial_idea.clone(),
            request.preferred_project_agent_identity_id,
            GenesisPromptContext {
                initial_idea,
                ..GenesisPromptContext::default()
            },
        )
        .await?;

    // The visible typed turn is admitted through the same Main Chat service
    // as every other user message.  The protocol text is bounded and already
    // guarded by the regular message admission path; no second chat/thread is
    // created.  If admission fails, cancel the just-created lifecycle so a
    // durable session cannot claim that a turn was admitted.
    let admitted = match state
        .agent_chat_service
        .send_message(SendAgentChatMessageInput {
            actor_user_id: user.user_id.clone(),
            chat_id: start.session.main_chat_id.clone(),
            content: start.prompt,
            dedupe_key: Some(format!("product-genesis:{}", start.session.id)),
        })
        .await
    {
        Ok(admitted) => admitted,
        Err(error) => {
            let _ = genesis
                .cancel(
                    &start.session.id,
                    start.session.version,
                    Some("Main Chat turn admission failed".to_owned()),
                )
                .await;
            return Err(error.into());
        }
    };
    let session = genesis
        .record_source_message(
            &start.session.id,
            start.session.version,
            &admitted.message.id,
        )
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(ProductGenesisStartResponse {
            main_chat_id: session.main_chat_id.clone(),
            session,
            admitted_turn_id: Some(admitted.turn_job.id),
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
