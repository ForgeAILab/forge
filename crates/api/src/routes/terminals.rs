use api_types::{
    CreateTerminalSessionRequest, CreateTerminalSessionResponse, ResizeTerminalSessionRequest,
    TerminalAvailability, TerminalClientFrame, TerminalServerFrame, TerminalSessionResponse,
    TERMINAL_ATTACH_TOKEN_INVALID,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{header, HeaderMap, StatusCode},
    response::Response,
    Json,
};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use serde::Deserialize;

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::AuthenticatedUser,
    state::AppState,
};

#[derive(Debug, Deserialize)]
pub struct ListTerminalSessionsQuery {
    pub include_ended: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct TerminalWsQuery {
    pub attach_token: Option<String>,
    pub token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TerminateTerminalSessionRequest {
    pub reason: Option<String>,
}

pub async fn create_terminal_session(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<CreateTerminalSessionRequest>,
) -> ApiResult<(StatusCode, Json<CreateTerminalSessionResponse>)> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    let (session, attach) = state
        .terminal_service
        .create_session(&id, &user.user_id, request.rows, request.cols)
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(CreateTerminalSessionResponse { session, attach }),
    ))
}

pub async fn list_terminal_sessions(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Query(query): Query<ListTerminalSessionsQuery>,
) -> ApiResult<Json<Vec<TerminalSessionResponse>>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let sessions = state
        .terminal_service
        .list_sessions(&id, query.include_ended.unwrap_or(false))
        .await?;
    Ok(Json(sessions))
}

pub async fn terminal_availability(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<TerminalAvailability>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    Ok(Json(
        state
            .terminal_service
            .availability(&id, &user.user_id)
            .await?,
    ))
}

pub async fn get_terminal_session(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<TerminalSessionResponse>> {
    let session = require_terminal_session_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    Ok(Json(session))
}

pub async fn issue_terminal_attach_token(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<api_types::TerminalAttachTokenResponse>> {
    require_terminal_session_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    Ok(Json(
        state
            .terminal_service
            .issue_attach_token(&id, &user.user_id)
            .await?,
    ))
}

pub async fn resize_terminal_session(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<ResizeTerminalSessionRequest>,
) -> ApiResult<Json<TerminalSessionResponse>> {
    require_terminal_session_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    Ok(Json(
        state
            .terminal_service
            .resize_session(&id, &user.user_id, request.rows, request.cols)
            .await?,
    ))
}

pub async fn terminate_terminal_session(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    body: Option<Json<TerminateTerminalSessionRequest>>,
) -> ApiResult<Json<TerminalSessionResponse>> {
    require_terminal_session_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    let reason = body.and_then(|Json(body)| body.reason);
    Ok(Json(
        state
            .terminal_service
            .terminate_session(&id, &user.user_id, reason)
            .await?,
    ))
}

pub async fn terminal_ws(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<TerminalWsQuery>,
    ws: WebSocketUpgrade,
) -> ApiResult<Response> {
    if query.token.is_some() {
        return Err(ApiError::forbidden_with_code(
            TERMINAL_ATTACH_TOKEN_INVALID,
            "terminal websocket requires attach_token, not token",
        ));
    }
    if query.attach_token.is_none() && headers.contains_key(header::AUTHORIZATION) {
        return Err(ApiError::forbidden_with_code(
            TERMINAL_ATTACH_TOKEN_INVALID,
            "terminal websocket does not accept Authorization without attach_token",
        ));
    }
    let attach_token = query.attach_token.ok_or_else(|| {
        ApiError::forbidden_with_code(
            TERMINAL_ATTACH_TOKEN_INVALID,
            "terminal websocket attach_token is required",
        )
    })?;
    let user_id = state
        .terminal_service
        .consume_attach_token(&id, &attach_token)
        .await?;

    require_terminal_session_access(
        &state,
        &id,
        &user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    Ok(ws.on_upgrade(move |socket| run_terminal_socket(state, id, user_id, socket)))
}

async fn run_terminal_socket(
    state: AppState,
    session_id: String,
    user_id: String,
    socket: WebSocket,
) {
    let mut terminal_rx = state.terminal_service.attach_client(&session_id).await;
    let (mut sink, mut stream) = socket.split();

    loop {
        tokio::select! {
            server_frame = terminal_rx.recv() => {
                let Some(frame) = server_frame else {
                    break;
                };
                if !send_server_frame(&mut sink, frame).await {
                    break;
                }
            }
            client_message = stream.next() => {
                match client_message {
                    Some(Ok(Message::Text(text))) => {
                        if !handle_client_text_frame(&state, &session_id, &user_id, text.as_str(), &mut sink).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        if sink.send(Message::Pong(bytes)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Binary(_))) => {
                        if !send_server_frame(
                            &mut sink,
                            TerminalServerFrame::Error {
                                code: "invalid_frame".to_owned(),
                                message: "terminal websocket frames must be text JSON".to_owned(),
                            },
                        )
                        .await
                        {
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        tracing::warn!(%error, session_id = %session_id, "terminal websocket read failed");
                        break;
                    }
                }
            }
        }
    }

    state
        .terminal_service
        .detach_closed_clients(&session_id)
        .await;
}

async fn handle_client_text_frame(
    state: &AppState,
    session_id: &str,
    user_id: &str,
    text: &str,
    sink: &mut SplitSink<WebSocket, Message>,
) -> bool {
    let frame = match serde_json::from_str::<TerminalClientFrame>(text) {
        Ok(frame) => frame,
        Err(error) => {
            return send_server_frame(
                sink,
                TerminalServerFrame::Error {
                    code: "invalid_frame".to_owned(),
                    message: format!("invalid terminal client frame: {error}"),
                },
            )
            .await;
        }
    };

    let result = match frame {
        TerminalClientFrame::Input { data } => state
            .terminal_service
            .handle_terminal_input(session_id, &data)
            .await
            .map(|_| None),
        TerminalClientFrame::Resize { rows, cols } => state
            .terminal_service
            .resize_session(session_id, user_id, rows, cols)
            .await
            .map(|_| None),
        TerminalClientFrame::Ping {} => Ok(Some(TerminalServerFrame::Pong {})),
    };

    match result {
        Ok(Some(frame)) => send_server_frame(sink, frame).await,
        Ok(None) => true,
        Err(error) => {
            send_server_frame(
                sink,
                TerminalServerFrame::Error {
                    code: "terminal_error".to_owned(),
                    message: error.to_string(),
                },
            )
            .await
        }
    }
}

async fn send_server_frame(
    sink: &mut SplitSink<WebSocket, Message>,
    frame: TerminalServerFrame,
) -> bool {
    match serde_json::to_string(&frame) {
        Ok(json) => sink.send(Message::Text(json.into())).await.is_ok(),
        Err(error) => {
            tracing::error!(%error, "failed to serialize terminal server frame");
            false
        }
    }
}

async fn require_terminal_session_access(
    state: &AppState,
    session_id: &str,
    user_id: &str,
    access: crate::routes::authorization::ProjectAccess,
) -> ApiResult<TerminalSessionResponse> {
    let session = state.terminal_service.get_session(session_id).await?;
    crate::routes::authorization::require_task_access(state, &session.task_id, user_id, access)
        .await
        .map_err(|error| {
            error.mask_not_found(crate::errors::ApiError::not_found_with_code(
                api_types::TERMINAL_NOT_FOUND,
                "terminal_session",
                session_id.to_owned(),
            ))
        })?;
    Ok(session)
}
