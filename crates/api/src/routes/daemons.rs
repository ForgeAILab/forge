use std::time::Duration;

use api_types::{
    DaemonErrorPayload, DaemonFrame, DaemonRegisterRequest, DaemonRegisterResponse,
    DaemonReportRequest, DaemonResponse, PaginatedResponse, DAEMON_HEARTBEAT_INTERVAL_SECS,
    INVALID_FRAME,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{header, HeaderMap},
    response::Response,
    Json,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use services::daemon_service::{DetectedCliInput, RuntimeReportInput};
use tokio::sync::mpsc;

use db::UserRepo;

use crate::{
    errors::{ApiError, ApiResult},
    routes::{auth::RequireAdmin, daemon_response, page_request, paginated, ListParams},
    state::AppState,
};

pub async fn register_daemon(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<DaemonRegisterRequest>,
) -> ApiResult<Json<DaemonRegisterResponse>> {
    let owner_id = registration_owner_id(&state, &headers).await?;
    let visibility = owner_id.as_ref().map(|_| "account".to_owned());

    let registration = state
        .daemon_service
        .register(services::DaemonRegisterInput {
            machine_id: request.machine_id,
            hostname: request.hostname,
            os: request.os,
            arch: request.arch,
            agent_version: request.agent_version,
            labels: request.labels.unwrap_or_else(|| serde_json::json!({})),
            runtimes: request
                .runtimes
                .unwrap_or_default()
                .into_iter()
                .map(runtime_report_input)
                .collect(),
            owner_id,
            visibility,
        })
        .await?;

    Ok(Json(DaemonRegisterResponse {
        daemon_id: registration.daemon_id,
        registration_token: registration.plaintext_token,
    }))
}

pub async fn report_daemon(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<DaemonReportRequest>,
) -> ApiResult<Json<DaemonResponse>> {
    let token = bearer_token(&headers)?;
    state
        .daemon_service
        .authenticate(&id, token)
        .await
        .map_err(|_| ApiError::unauthorized("invalid daemon bearer token"))?;

    let daemon = state
        .daemon_service
        .ingest_report(
            &id,
            services::DaemonReportInput {
                detected_clis: request
                    .detected_clis
                    .into_iter()
                    .map(|detected_cli| DetectedCliInput {
                        kind: detected_cli.kind,
                        availability: detected_cli.availability,
                        config_path: detected_cli.config_path,
                        version: detected_cli.version,
                        path: detected_cli.path,
                    })
                    .collect(),
                runtimes: request
                    .runtimes
                    .unwrap_or_default()
                    .into_iter()
                    .map(runtime_report_input)
                    .collect(),
                labels: request.labels,
                active_execution_ids: request.active_execution_ids,
            },
        )
        .await?;

    Ok(Json(daemon_response(daemon)))
}

#[derive(Debug, Deserialize)]
pub struct ConnectQuery {
    pub token: Option<String>,
}

pub async fn connect_daemon(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ConnectQuery>,
    ws: WebSocketUpgrade,
) -> ApiResult<Response> {
    let token = connect_token(&headers, query.token)?;
    state
        .daemon_service
        .authenticate(&id, &token)
        .await
        .map_err(|_| ApiError::unauthorized("invalid daemon bearer token"))?;

    Ok(ws.on_upgrade(move |socket| run_command_socket(state, id, socket)))
}

async fn run_command_socket(state: AppState, daemon_id: String, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let (connection, mut outbound_rx) =
        services::daemon_transport::DaemonConnection::new(daemon_id.clone());
    let outbound_tx = connection.outbound.clone();
    let connection_id = connection.id();
    let mut stale_rx = connection.stale_receiver();

    if state
        .daemon_connections
        .register(daemon_id.clone(), connection.clone())
        .is_some()
    {
        tracing::info!(
            daemon_id = %daemon_id,
            connection_id,
            "replaced stale connection"
        );
    }

    if let Err(error) = state.daemon_service.mark_connected(&daemon_id).await {
        tracing::warn!(
            daemon_id = %daemon_id,
            connection_id,
            error = %error,
            "failed to mark daemon command socket connected"
        );
        if state
            .daemon_connections
            .is_current(&daemon_id, connection_id)
        {
            state.daemon_connections.unregister(&daemon_id);
        }
        return;
    }

    tracing::info!(
        daemon_id = %daemon_id,
        connection_id,
        "daemon command socket connected"
    );

    let writer_daemon_id = daemon_id.clone();
    let writer = tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            let json = match serde_json::to_string(&frame) {
                Ok(json) => json,
                Err(error) => {
                    tracing::error!(
                        daemon_id = %writer_daemon_id,
                        error = %error,
                        "failed to serialize daemon frame"
                    );
                    continue;
                }
            };

            if let Err(error) = sink.send(Message::Text(json.into())).await {
                tracing::warn!(
                    daemon_id = %writer_daemon_id,
                    error = %error,
                    "daemon command socket writer stopped"
                );
                break;
            }
        }
    });

    loop {
        let idle_timeout =
            tokio::time::sleep(Duration::from_secs(DAEMON_HEARTBEAT_INTERVAL_SECS * 3));
        tokio::pin!(idle_timeout);

        tokio::select! {
            _ = &mut idle_timeout => {
                tracing::warn!(
                    daemon_id = %daemon_id,
                    connection_id,
                    "daemon command socket heartbeat timed out"
                );
                break;
            }
            changed = stale_rx.changed() => {
                if changed.is_err() || *stale_rx.borrow() {
                    tracing::info!(
                        daemon_id = %daemon_id,
                        connection_id,
                        "daemon command socket marked stale"
                    );
                    break;
                }
            }
            message = stream.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        if !handle_daemon_text_frame(
                            &state,
                            &daemon_id,
                            connection_id,
                            &outbound_tx,
                            text.as_str(),
                        )
                        .await
                        {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        tracing::info!(
                            daemon_id = %daemon_id,
                            connection_id,
                            "daemon command socket closed by peer"
                        );
                        break;
                    }
                    Some(Ok(Message::Binary(_))) => {
                        tracing::warn!(
                            daemon_id = %daemon_id,
                            connection_id,
                            "daemon command socket received non-text frame"
                        );
                        if send_invalid_frame(&outbound_tx, None, "daemon frames must be text JSON", None)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                    Some(Err(error)) => {
                        tracing::warn!(
                            daemon_id = %daemon_id,
                            connection_id,
                            error = %error,
                            "daemon command socket read failed"
                        );
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    let should_unregister = !connection.is_stale()
        && state
            .daemon_connections
            .is_current(&daemon_id, connection_id);
    if should_unregister {
        state.daemon_connections.unregister(&daemon_id);
        if let Err(error) = state.daemon_service.mark_disconnected(&daemon_id).await {
            tracing::warn!(
                daemon_id = %daemon_id,
                connection_id,
                error = %error,
                "failed to mark daemon command socket disconnected"
            );
        }
    }

    drop(outbound_tx);
    drop(connection);
    let _ = writer.await;

    tracing::info!(
        daemon_id = %daemon_id,
        connection_id,
        "daemon command socket disconnected"
    );
}

async fn handle_daemon_text_frame(
    state: &AppState,
    daemon_id: &str,
    connection_id: u64,
    outbound_tx: &mpsc::Sender<DaemonFrame>,
    text: &str,
) -> bool {
    // A reconnect retains the durable daemon id but receives a new
    // authenticated connection incarnation. Stop processing a replaced
    // socket before a delayed response/notification can reach the current
    // connection's pending requests or execution lease handler.
    if !state
        .daemon_connections
        .is_current(daemon_id, connection_id)
    {
        return false;
    }

    let frame = match serde_json::from_str::<DaemonFrame>(text) {
        Ok(frame) => frame,
        Err(error) => {
            tracing::warn!(
                daemon_id = %daemon_id,
                error = %error,
                "received malformed daemon frame"
            );
            return send_invalid_frame(
                outbound_tx,
                frame_id_from_text(text),
                "invalid daemon frame",
                Some(error.to_string()),
            )
            .await
            .is_ok();
        }
    };

    if let DaemonFrame::Heartbeat { seq } = &frame {
        if outbound_tx
            .send(DaemonFrame::Heartbeat { seq: *seq })
            .await
            .is_err()
        {
            return false;
        }
        if let Err(error) = state.daemon_service.touch_connection(daemon_id).await {
            tracing::warn!(
                daemon_id = %daemon_id,
                error = %error,
                "failed to touch daemon command socket heartbeat"
            );
        }
    }

    state
        .daemon_connections
        .dispatch_incoming_for_connection(daemon_id, connection_id, frame)
}

async fn send_invalid_frame(
    outbound_tx: &mpsc::Sender<DaemonFrame>,
    id: Option<String>,
    message: &str,
    detail: Option<String>,
) -> Result<(), mpsc::error::SendError<DaemonFrame>> {
    outbound_tx
        .send(DaemonFrame::Error {
            id,
            error: DaemonErrorPayload {
                code: INVALID_FRAME.to_owned(),
                message: message.to_owned(),
                details: detail.map(|error| serde_json::json!({ "error": error })),
            },
        })
        .await
}

fn frame_id_from_text(text: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| {
            value
                .get("id")
                .and_then(|id| id.as_str())
                .map(str::to_owned)
        })
}

pub async fn list_daemons(
    State(state): State<AppState>,
    _admin: RequireAdmin,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<PaginatedResponse<DaemonResponse>>> {
    let page = state.daemon_service.list(page_request(&params)?).await?;
    Ok(Json(paginated(page, daemon_response)))
}

pub async fn get_daemon(
    State(state): State<AppState>,
    _admin: RequireAdmin,
    Path(id): Path<String>,
) -> ApiResult<Json<DaemonResponse>> {
    let daemon = state
        .daemon_service
        .get(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("daemon", id))?;
    Ok(Json(daemon_response(daemon)))
}

fn runtime_report_input(runtime: api_types::RuntimeReport) -> RuntimeReportInput {
    RuntimeReportInput {
        kind: runtime.kind,
        workspace_root: runtime.workspace_root,
        status: runtime.status,
    }
}

async fn registration_owner_id(state: &AppState, headers: &HeaderMap) -> ApiResult<Option<String>> {
    let Some(token) = optional_bearer_token(headers)? else {
        return Ok(None);
    };

    let result = if token.starts_with("fg_") {
        state.auth_service.verify_pat(token).await
    } else {
        state.auth_service.verify_token(token)
    };

    let (user_id, _email, _is_admin) = result.map_err(|code| {
        ApiError::unauthorized_with_code(auth_error_code(&code), "Authentication failed")
    })?;

    if UserRepo::get_user_by_id(&*state.db, &user_id)
        .await?
        .is_none()
    {
        // Registration is auth-exempt; stale or synthetic JWTs must not create dangling owner FKs.
        return Ok(None);
    }

    Ok(Some(user_id))
}

fn optional_bearer_token(headers: &HeaderMap) -> ApiResult<Option<&str>> {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| ApiError::unauthorized("invalid Authorization header"))?;
    value
        .strip_prefix("Bearer ")
        .filter(|token| !token.trim().is_empty())
        .map(Some)
        .ok_or_else(|| ApiError::unauthorized("invalid Authorization bearer token"))
}

fn bearer_token(headers: &HeaderMap) -> ApiResult<&str> {
    let value = headers
        .get(header::AUTHORIZATION)
        .ok_or_else(|| ApiError::unauthorized("missing Authorization bearer token"))?
        .to_str()
        .map_err(|_| ApiError::unauthorized("invalid Authorization header"))?;
    value
        .strip_prefix("Bearer ")
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| ApiError::unauthorized("invalid Authorization bearer token"))
}

fn connect_token(headers: &HeaderMap, query_token: Option<String>) -> ApiResult<String> {
    if let Some(token) = optional_bearer_token(headers)? {
        return Ok(token.to_owned());
    }

    query_token
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
        .ok_or_else(|| ApiError::unauthorized("missing daemon bearer token"))
}

fn auth_error_code(code: &str) -> &'static str {
    match code {
        "token_expired" => "token_expired",
        "invalid_token" => "invalid_token",
        _ => "invalid_token",
    }
}
