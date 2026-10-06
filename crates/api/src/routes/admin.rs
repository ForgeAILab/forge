use api_types::{
    AdminUserListResponse, AdminUserResponse, MemoryBackfillResponse, MemoryBackfillTypeResponse,
    SettingListResponse, SettingResponse, UpdateAdminRequest, UpsertSettingRequest,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use db::{PageRequest, SortBy, SortOrder, SystemSettingRepo, User, UserRepo};
use serde::Deserialize;

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::RequireAdmin,
    state::AppState,
};

#[derive(Debug, Deserialize)]
pub struct ListUsersParams {
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

pub async fn list_users(
    _admin: RequireAdmin,
    State(state): State<AppState>,
    Query(params): Query<ListUsersParams>,
) -> ApiResult<Json<AdminUserListResponse>> {
    let page_request = PageRequest {
        cursor: params.cursor,
        limit: params.limit.unwrap_or(50),
        include_total: false,
        sort_by: SortBy::CreatedAt,
        sort_order: SortOrder::Asc,
    };
    let page = UserRepo::list_users(&*state.db, page_request).await?;
    let has_more = page.next_cursor.is_some();

    Ok(Json(AdminUserListResponse {
        items: page.items.into_iter().map(admin_user_response).collect(),
        next_cursor: page.next_cursor,
        has_more,
    }))
}

pub async fn update_user_admin(
    RequireAdmin(requester): RequireAdmin,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateAdminRequest>,
) -> ApiResult<Json<AdminUserResponse>> {
    if !body.is_admin && id == requester.user_id {
        let count = UserRepo::count_admins(&*state.db).await?;
        if count <= 1 {
            return Err(ApiError::conflict_with_code(
                "last_admin",
                "Cannot revoke the last admin",
            ));
        }
    }

    UserRepo::set_admin(&*state.db, &id, body.is_admin).await?;
    let user = UserRepo::get_user_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("user", id.clone()))?;

    Ok(Json(admin_user_response(user)))
}

pub async fn delete_user(
    _admin: RequireAdmin,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    let user = UserRepo::get_user_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("user", id.clone()))?;

    if user.is_admin {
        let count = UserRepo::count_admins(&*state.db).await?;
        if count <= 1 {
            return Err(ApiError::conflict_with_code(
                "last_admin",
                "Cannot delete the last admin",
            ));
        }
    }

    if !UserRepo::delete_user(&*state.db, &id).await? {
        return Err(ApiError::not_found("user", id));
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_settings(
    _admin: RequireAdmin,
    State(state): State<AppState>,
) -> ApiResult<Json<SettingListResponse>> {
    let items = SystemSettingRepo::list_settings(&*state.db)
        .await?
        .into_iter()
        .filter(|(key, _)| !is_protected_setting_key(key))
        .map(|(key, value)| SettingResponse { key, value })
        .collect();

    Ok(Json(SettingListResponse { items }))
}

pub async fn upsert_setting(
    _admin: RequireAdmin,
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<UpsertSettingRequest>,
) -> ApiResult<Json<SettingResponse>> {
    if is_protected_setting_key(&key) {
        return Err(ApiError::forbidden_with_code(
            "protected_setting",
            "This setting key is protected",
        ));
    }

    let value = body.value;
    SystemSettingRepo::set_setting(&*state.db, &key, &value, &db::now_rfc3339()).await?;

    Ok(Json(SettingResponse { key, value }))
}

pub async fn delete_setting(
    _admin: RequireAdmin,
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> ApiResult<StatusCode> {
    if is_protected_setting_key(&key) {
        return Err(ApiError::forbidden_with_code(
            "protected_setting",
            "This setting key is protected",
        ));
    }

    SystemSettingRepo::delete_setting(&*state.db, &key).await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn backfill_memory(
    _admin: RequireAdmin,
    State(state): State<AppState>,
) -> ApiResult<Json<MemoryBackfillResponse>> {
    let summary = state.memory_service.backfill_sources().await?;
    Ok(Json(MemoryBackfillResponse {
        indexed: summary.indexed,
        skipped: summary.skipped,
        items: summary
            .items
            .into_iter()
            .map(|result| MemoryBackfillTypeResponse {
                source_type: result.source_type.to_string(),
                indexed: result.indexed,
                skipped: result.skipped,
            })
            .collect(),
    }))
}

fn admin_user_response(user: User) -> AdminUserResponse {
    AdminUserResponse {
        id: user.id,
        email: user.email,
        display_name: user.display_name,
        is_admin: user.is_admin,
        created_at: user.created_at,
    }
}

fn is_protected_setting_key(key: &str) -> bool {
    matches!(key, "bootstrap_completed" | db::MAPPING_REVISION_KEY)
}
