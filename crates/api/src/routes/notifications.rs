use api_types::{NotificationResponse, PaginatedResponse, UnreadCountResponse};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use db::{NotificationListQuery, NotificationRepo, PageRequest, SortBy, SortOrder};
use serde::Deserialize;

use crate::{
    errors::{ApiError, ApiResult},
    routes::paginated,
    state::AppState,
};

#[derive(Debug, Clone, Deserialize)]
pub struct ListNotificationsParams {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
    pub include_total: Option<bool>,
    pub project_id: Option<String>,
    pub read: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CountParams {
    pub project_id: Option<String>,
}

pub async fn list_notifications(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Query(params): Query<ListNotificationsParams>,
) -> ApiResult<Json<PaginatedResponse<NotificationResponse>>> {
    if let Some(project_id) = params.project_id.as_deref() {
        crate::routes::authorization::require_project_access(
            &state,
            project_id,
            &user.user_id,
            crate::routes::authorization::ProjectAccess::Read,
        )
        .await?;
    }
    let page = state
        .db
        .list_visible_notifications(
            &user.user_id,
            NotificationListQuery {
                project_id: params.project_id,
                read: params.read,
                page: PageRequest {
                    cursor: params.cursor,
                    limit: params.limit.unwrap_or(20).clamp(1, 100),
                    include_total: params.include_total.unwrap_or(false),
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Desc,
                },
            },
        )
        .await?;

    Ok(Json(paginated(page, notification_response)))
}

pub async fn get_unread_count(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Query(params): Query<CountParams>,
) -> ApiResult<Json<UnreadCountResponse>> {
    if let Some(project_id) = params.project_id.as_deref() {
        crate::routes::authorization::require_project_access(
            &state,
            project_id,
            &user.user_id,
            crate::routes::authorization::ProjectAccess::Read,
        )
        .await?;
    }
    let count = state
        .db
        .visible_notification_unread_count(&user.user_id, params.project_id.as_deref())
        .await?;
    Ok(Json(UnreadCountResponse { count }))
}

pub async fn mark_read(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<NotificationResponse>> {
    let notification = state
        .db
        .get_notification(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("notification", id.clone()))?;
    crate::routes::authorization::require_project_access(
        &state,
        &notification.project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await
    .map_err(|error| error.mask_not_found(ApiError::not_found("notification", id.clone())))?;
    let item = NotificationRepo::mark_read(&*state.db, &id).await?;
    Ok(Json(notification_response(item)))
}

pub async fn mark_all_read(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Query(params): Query<CountParams>,
) -> ApiResult<StatusCode> {
    if let Some(project_id) = params.project_id.as_deref() {
        crate::routes::authorization::require_project_access(
            &state,
            project_id,
            &user.user_id,
            crate::routes::authorization::ProjectAccess::Write,
        )
        .await?;
    }
    state
        .db
        .mark_visible_notifications_read(&user.user_id, params.project_id.as_deref())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_notification(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    let notification = state
        .db
        .get_notification(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("notification", id.clone()))?;
    crate::routes::authorization::require_project_access(
        &state,
        &notification.project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await
    .map_err(|error| error.mask_not_found(ApiError::not_found("notification", id.clone())))?;
    NotificationRepo::delete(&*state.db, &id).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn notification_response(item: db::Notification) -> NotificationResponse {
    NotificationResponse {
        id: item.id,
        project_id: item.project_id,
        task_id: item.task_id,
        event_type: item.event_type,
        title: item.title,
        body: item.body,
        read: item.read,
        created_at: item.created_at,
    }
}
