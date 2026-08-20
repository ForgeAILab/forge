use api_types::{
    AgentDetailResponse, AttentionItem, AttentionListResponse, AttentionMutationRequest,
    AttentionSnoozeRequest, MissionControlHomeResponse, MissionControlQuery,
};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use db::{PageRequest, SortBy, SortOrder};
use services::{attention_service, AttentionService};

use crate::{errors::ApiResult, routes::auth::AuthenticatedUser, state::AppState};

/// Mission Control handlers are intentionally kept in a separate module so
/// the router can be registered as one bounded read/mutation surface by the
/// application owner.  The service is cheap and stateless; constructing it
/// per request also keeps this slice independent of AppState wiring.
pub async fn home(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Query(query): Query<MissionControlQuery>,
) -> ApiResult<Json<MissionControlHomeResponse>> {
    let service = AttentionService::new(state.db);
    Ok(Json(
        service
            .mission_control_home(
                &user.user_id,
                query.project_id.as_deref(),
                query.limit.unwrap_or(20),
            )
            .await?,
    ))
}

pub async fn list_attention(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Query(query): Query<MissionControlQuery>,
) -> ApiResult<Json<AttentionListResponse>> {
    let service = AttentionService::new(state.db);
    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let (page, consumer_health) = service
        .list_for_user(
            &user.user_id,
            query.project_id.as_deref(),
            query.status.as_deref(),
            query.include_snoozed.unwrap_or(false),
            PageRequest {
                cursor: query.cursor,
                limit,
                include_total: query.include_total.unwrap_or(false),
                sort_by: SortBy::Priority,
                sort_order: SortOrder::Desc,
            },
        )
        .await?;
    let items = page
        .items
        .into_iter()
        .filter_map(attention_service::attention_item_lenient)
        .collect::<Vec<_>>();
    let has_more = page.next_cursor.is_some();
    Ok(Json(AttentionListResponse {
        items,
        next_cursor: page.next_cursor,
        has_more,
        total_count: page.total_count.and_then(|count| u64::try_from(count).ok()),
        consumer_health,
    }))
}

pub async fn acknowledge(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(body): Json<AttentionMutationRequest>,
) -> ApiResult<Json<AttentionItem>> {
    let service = AttentionService::new(state.db);
    Ok(Json(attention_service::attention_item(
        service
            .acknowledge(&user.user_id, &id, body.expected_version)
            .await?,
    )?))
}

pub async fn snooze(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(body): Json<AttentionSnoozeRequest>,
) -> ApiResult<Json<AttentionItem>> {
    let service = AttentionService::new(state.db);
    Ok(Json(attention_service::attention_item(
        service
            .snooze(
                &user.user_id,
                &id,
                body.expected_version,
                &body.snoozed_until,
            )
            .await?,
    )?))
}

pub async fn resolve(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(body): Json<AttentionMutationRequest>,
) -> ApiResult<Json<AttentionItem>> {
    let service = AttentionService::new(state.db);
    Ok(Json(attention_service::attention_item(
        service
            .resolve(&user.user_id, &id, body.expected_version)
            .await?,
    )?))
}

pub async fn agent_detail(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
    Query(query): Query<MissionControlQuery>,
) -> ApiResult<Json<AgentDetailResponse>> {
    let service = AttentionService::new(state.db);
    Ok(Json(
        service
            .agent_detail(&user.user_id, &identity_id, query.limit.unwrap_or(20))
            .await?,
    ))
}
