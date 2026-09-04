//! REST resources for Main Agent inquiries (`inquiry.run`).
//!
//! An inquiry is an ephemeral, read-only sub-agent run: no repo, no task
//! flow, no state machine, and the only user verb is cancel. This module is
//! deliberately thin -- all authorization (an inquiry is reachable only
//! through the chat that dispatched it) lives in
//! `services::agent_inquiry_service::AgentInquiryService`.

use api_types::{
    AgentInquiryListResponse, AgentInquiryResponse, AgentInquiryStatus, AgentInquiryTokenUsage,
    CancelAgentInquiryRequest,
};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use db::AgentInquiryStatus as DbAgentInquiryStatus;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    errors::ApiResult,
    routes::{
        auth::AuthenticatedUser,
        executions::{read_log_page, LogsQuery},
    },
    state::AppState,
};

const DEFAULT_LIST_LIMIT: i64 = 50;
const MAX_LIST_LIMIT: i64 = 200;

#[derive(Debug, Clone, Deserialize)]
pub struct ListAgentInquiriesQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

pub async fn list_agent_chat_inquiries(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(chat_id): Path<String>,
    Query(query): Query<ListAgentInquiriesQuery>,
) -> ApiResult<Json<AgentInquiryListResponse>> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    let page = state
        .agent_inquiry_service
        .list_for_chat(&user.user_id, &chat_id, limit, query.cursor.as_deref())
        .await?;
    Ok(Json(AgentInquiryListResponse {
        has_more: page.next_cursor.is_some(),
        items: page.items.into_iter().map(inquiry_response).collect(),
        next_cursor: page.next_cursor,
    }))
}

pub async fn get_agent_inquiry(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(inquiry_id): Path<String>,
) -> ApiResult<Json<AgentInquiryResponse>> {
    let inquiry = state
        .agent_inquiry_service
        .get(&user.user_id, &inquiry_id)
        .await?;
    Ok(Json(inquiry_response(inquiry)))
}

pub async fn cancel_agent_inquiry(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(inquiry_id): Path<String>,
    Json(request): Json<CancelAgentInquiryRequest>,
) -> ApiResult<Json<AgentInquiryResponse>> {
    let inquiry = state
        .agent_inquiry_service
        .cancel(&user.user_id, &inquiry_id, request.expected_version)
        .await?;
    Ok(Json(inquiry_response(inquiry)))
}

/// One page of a sub-agent's durable activity log: the reasoning, tool calls
/// with their bounded results, and reply deltas the runtime emitted while the
/// inquiry ran, in the same Forge JSONL shape as `/executions/{id}/logs` and
/// an Agent Chat turn's log. An inquiry that has not written anything yet
/// reads as an empty page rather than a 404.
pub async fn get_agent_inquiry_logs(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(inquiry_id): Path<String>,
    Query(params): Query<LogsQuery>,
) -> ApiResult<Json<Value>> {
    // Authorized exactly like the record itself: an inquiry is reachable only
    // through the chat that dispatched it.
    let inquiry = state
        .agent_inquiry_service
        .get(&user.user_id, &inquiry_id)
        .await?;
    let page = read_log_page(&state.agent_chat_turn_logs.path_for(&inquiry.id), &params).await?;
    Ok(Json(page.unwrap_or_else(
        || json!({"items": [], "has_more": false, "next_sequence": null}),
    )))
}

fn inquiry_response(inquiry: db::AgentInquiry) -> AgentInquiryResponse {
    AgentInquiryResponse {
        id: inquiry.id,
        chat_id: inquiry.chat_id,
        title: inquiry.title,
        question: inquiry.question,
        status: match inquiry.status {
            DbAgentInquiryStatus::Running => AgentInquiryStatus::Running,
            DbAgentInquiryStatus::Succeeded => AgentInquiryStatus::Succeeded,
            DbAgentInquiryStatus::Failed => AgentInquiryStatus::Failed,
            DbAgentInquiryStatus::Cancelled => AgentInquiryStatus::Cancelled,
        },
        findings: inquiry.findings,
        findings_path: inquiry.findings_path,
        error: inquiry.error,
        token_usage: AgentInquiryTokenUsage {
            input_tokens: inquiry.input_tokens,
            output_tokens: inquiry.output_tokens,
            cache_read_tokens: inquiry.cache_read_tokens,
            cache_write_tokens: inquiry.cache_write_tokens,
        },
        duration_ms: inquiry.duration_ms,
        version: inquiry.version,
        created_at: inquiry.created_at,
        started_at: inquiry.started_at,
        finished_at: inquiry.finished_at,
    }
}
