use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;

use api_types::{
    AddMemberRequest, ProjectMemberResponse, UpdateMemberRoleRequest, UserSearchResult,
};
use db::UserRepo;
use services::{ProjectMemberService, ServiceError};

use crate::{
    errors::{ApiError, ApiResult},
    state::AppState,
};

use super::auth::AuthenticatedUser;

#[derive(Debug, Deserialize)]
pub struct UserSearchParams {
    pub q: String,
    pub limit: Option<i64>,
}

pub async fn search_users(
    State(state): State<AppState>,
    _user: AuthenticatedUser,
    Query(params): Query<UserSearchParams>,
) -> ApiResult<Json<Vec<UserSearchResult>>> {
    let q = params.q.trim().to_string();
    if q.is_empty() {
        return Ok(Json(vec![]));
    }
    let limit = params.limit.unwrap_or(10).clamp(1, 50);
    let users = UserRepo::search_users(&*state.db, &q, limit).await?;
    let results = users
        .into_iter()
        .map(|u| UserSearchResult {
            id: u.id,
            email: u.email,
            display_name: u.display_name,
        })
        .collect();
    Ok(Json(results))
}

pub async fn list_members(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<Json<Vec<ProjectMemberResponse>>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let service = ProjectMemberService::new(state.db.clone());
    let members = service
        .list_members(&project_id, &user.user_id)
        .await
        .map_err(map_member_service_error)?;

    let mut responses = Vec::with_capacity(members.len());
    for member in members {
        let user = UserRepo::get_user_by_id(&*state.db, &member.user_id).await?;
        responses.push(ProjectMemberResponse {
            id: member.id,
            user_id: member.user_id,
            email: user.as_ref().map(|u| u.email.clone()).unwrap_or_default(),
            display_name: user.and_then(|u| u.display_name),
            role: member.role,
            created_at: member.created_at,
        });
    }

    Ok(Json(responses))
}

pub async fn add_member(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(body): Json<AddMemberRequest>,
) -> ApiResult<(StatusCode, Json<ProjectMemberResponse>)> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Admin,
    )
    .await?;
    let valid_roles = ["owner", "admin", "member", "viewer"];
    if !valid_roles.contains(&body.role.as_str()) {
        return Err(ApiError::bad_request(format!(
            "Invalid role: {}. Must be one of: owner, admin, member, viewer",
            body.role
        )));
    }

    // Verify the target user exists
    let target_user = UserRepo::get_user_by_id(&*state.db, &body.user_id)
        .await?
        .ok_or_else(|| ApiError::not_found("user", &body.user_id))?;

    let service = ProjectMemberService::new(state.db.clone());
    let member = service
        .add_member(&project_id, &user.user_id, &body.user_id, &body.role)
        .await
        .map_err(|e| match e {
            ServiceError::InvalidOperation { message } if message == "insufficient_role" => {
                ApiError::forbidden_with_code(
                    "insufficient_role",
                    "project owner or admin role is required",
                )
            }
            ServiceError::InvalidOperation { message } if message == "not_owner" => {
                ApiError::forbidden_with_code("not_owner", "owner role is required")
            }
            ServiceError::Conflict(message) if message == "last_owner" => {
                ApiError::conflict("last_owner", "cannot remove or demote the last owner")
            }
            ServiceError::Db(db::DbError::Check(msg)) if msg.contains("already exists") => {
                ApiError::conflict("member_exists", "User is already a member of this project")
            }
            other => ApiError::from(other),
        })?;

    Ok((
        StatusCode::CREATED,
        Json(ProjectMemberResponse {
            id: member.id,
            user_id: member.user_id,
            email: target_user.email,
            display_name: target_user.display_name,
            role: member.role,
            created_at: member.created_at,
        }),
    ))
}

pub async fn update_member_role(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, user_id)): Path<(String, String)>,
    Json(body): Json<UpdateMemberRoleRequest>,
) -> ApiResult<Json<ProjectMemberResponse>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Admin,
    )
    .await?;
    let valid_roles = ["owner", "admin", "member", "viewer"];
    if !valid_roles.contains(&body.role.as_str()) {
        return Err(ApiError::bad_request(format!(
            "Invalid role: {}. Must be one of: owner, admin, member, viewer",
            body.role
        )));
    }

    let service = ProjectMemberService::new(state.db.clone());
    let member = service
        .update_role(&project_id, &user.user_id, &user_id, &body.role)
        .await
        .map_err(map_member_service_error)?;

    let target_user = UserRepo::get_user_by_id(&*state.db, &member.user_id).await?;

    Ok(Json(ProjectMemberResponse {
        id: member.id,
        user_id: member.user_id,
        email: target_user
            .as_ref()
            .map(|u| u.email.clone())
            .unwrap_or_default(),
        display_name: target_user.and_then(|u| u.display_name),
        role: member.role,
        created_at: member.created_at,
    }))
}

pub async fn remove_member(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, user_id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Admin,
    )
    .await?;
    let service = ProjectMemberService::new(state.db.clone());
    service
        .remove_member(&project_id, &user.user_id, &user_id)
        .await
        .map_err(map_member_service_error)?;

    Ok(StatusCode::NO_CONTENT)
}

fn map_member_service_error(error: ServiceError) -> ApiError {
    match error {
        ServiceError::InvalidOperation { message } if message == "insufficient_role" => {
            ApiError::forbidden_with_code(
                "insufficient_role",
                "project owner or admin role is required",
            )
        }
        ServiceError::InvalidOperation { message } if message == "not_owner" => {
            ApiError::forbidden_with_code("not_owner", "owner role is required")
        }
        ServiceError::Conflict(message) if message == "last_owner" => {
            ApiError::conflict("last_owner", "cannot remove or demote the last owner")
        }
        ServiceError::NotFound {
            entity: "project",
            id,
        } => ApiError::not_found("project", id),
        ServiceError::Db(db::DbError::NotFound) => ApiError::not_found("member", "unknown"),
        other => ApiError::from(other),
    }
}
