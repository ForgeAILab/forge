use std::convert::Infallible;

use axum::{
    extract::{rejection::JsonRejection, Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{rpc::dispatch_with_context, AppState};

#[derive(Debug, Clone, Default)]
pub(crate) struct McpContext {
    pub(crate) project_id: Option<String>,
    pub(crate) user_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpQuery {
    pub project_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpRequest {
    #[serde(default = "jsonrpc_version")]
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    pub id: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct McpResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<McpError>,
    pub id: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct McpError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

fn is_notification(request: &McpRequest) -> bool {
    request.id.is_none()
}

#[derive(Debug, Clone)]
pub struct McpUser {
    pub user_id: String,
}

pub async fn mcp_handler(
    State(state): State<AppState>,
    Query(query): Query<McpQuery>,
    headers: HeaderMap,
    mcp_user: Option<axum::Extension<McpUser>>,
    request: Result<Json<McpRequest>, JsonRejection>,
) -> Response {
    let request = match request {
        Ok(Json(request)) => request,
        Err(error) => {
            let syntax_error = matches!(&error, JsonRejection::JsonSyntaxError(_));
            return Json(error_response(
                Value::Null,
                if syntax_error { -32700 } else { -32600 },
                if syntax_error {
                    "parse error"
                } else {
                    "invalid request"
                },
                None,
            ))
            .into_response();
        }
    };

    if request.jsonrpc != "2.0" {
        let id = request.id.unwrap_or(Value::Null);
        return Json(error_response(
            id,
            -32600,
            "invalid request",
            Some(json!({ "details": "jsonrpc must be \"2.0\"" })),
        ))
        .into_response();
    }

    let notification = is_notification(&request);
    let id = request.id.unwrap_or(Value::Null);
    let context = McpContext {
        project_id: scoped_project_id(query.project_id, &headers),
        user_id: mcp_user.map(|u| u.user_id.clone()),
    };
    let result = dispatch_with_context(&state, &context, &request.method, request.params).await;

    if notification {
        return StatusCode::ACCEPTED.into_response();
    }

    Json(match result {
        Ok(result) => success_response(id, result),
        Err(error) if error.is_protocol() => error.into_response(id),
        Err(error) => error.into_tool_response(id),
    })
    .into_response()
}

fn scoped_project_id(query_project_id: Option<String>, headers: &HeaderMap) -> Option<String> {
    query_project_id
        .or_else(|| {
            headers
                .get("x-forge-project-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        })
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

pub async fn mcp_sse_handler(
    State(_state): State<AppState>,
    Query(_query): Query<McpQuery>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    // MCP Streamable HTTP transport: GET opens a server-to-client SSE stream.
    // We have no server-initiated messages, so we serve an empty stream and rely
    // on KeepAlive pings to keep the connection open.
    Sse::new(futures_util::stream::pending::<Result<Event, Infallible>>())
        .keep_alive(KeepAlive::default())
}

pub fn mcp_router(state: AppState) -> Router {
    Router::new()
        .route("/mcp", post(mcp_handler).get(mcp_sse_handler))
        .with_state(state)
}

pub(crate) fn success_response(id: Value, result: Value) -> McpResponse {
    McpResponse {
        jsonrpc: jsonrpc_version(),
        result: Some(result),
        error: None,
        id,
    }
}

pub(crate) fn error_response(
    id: Value,
    code: i64,
    message: impl Into<String>,
    data: Option<Value>,
) -> McpResponse {
    McpResponse {
        jsonrpc: jsonrpc_version(),
        result: None,
        error: Some(McpError {
            code,
            message: message.into(),
            data,
        }),
        id,
    }
}

fn jsonrpc_version() -> String {
    "2.0".to_owned()
}
