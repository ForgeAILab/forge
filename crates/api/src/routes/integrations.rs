use api_types::{
    CreateIntegrationRequest, IntegrationResponse, PatchIntegrationRequest, SyncTriggerResponse,
};
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use db::{
    CreateProjectIntegration, DbError, IntegrationRepo, ProjectRepo, UpdateProjectIntegration,
};
use serde_json::{json, Value};
use services::IntegrationService;

use crate::{
    errors::{ApiError, ApiResult},
    middleware,
    state::AppState,
};

fn integration_response(model: db::ProjectIntegration) -> IntegrationResponse {
    IntegrationResponse {
        id: model.id,
        project_id: model.project_id,
        platform: model.platform.to_string(),
        base_url: model.base_url,
        owner: model.owner,
        repo: model.repo,
        token_secret_ref: model.token_secret_ref,
        poll_interval_secs: model.poll_interval_secs,
        sync_filter: serde_json::from_str(&model.sync_filter).unwrap_or_else(|_| json!({})),
        default_task_state: model.default_task_state,
        default_assignee_type: model.default_assignee_type,
        default_assignee_id: model.default_assignee_id,
        enabled: model.enabled,
        last_polled_at: model.last_polled_at,
        created_at: model.created_at,
        updated_at: model.updated_at,
    }
}

pub async fn create_integration(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(body): Json<CreateIntegrationRequest>,
) -> ApiResult<(StatusCode, Json<IntegrationResponse>)> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Admin,
    )
    .await?;
    let platform = body
        .platform
        .parse::<db::IntegrationPlatform>()
        .map_err(|_| ApiError::bad_request("invalid platform"))?;

    ProjectRepo::get_by_id(&*state.db, &project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.clone()))?;

    let now = db::now_rfc3339();
    let model = CreateProjectIntegration {
        id: db::new_uuid_v4(),
        project_id,
        platform,
        base_url: body.base_url,
        owner: body.owner,
        repo: body.repo,
        token_secret_ref: body.token_secret_ref,
        poll_interval_secs: body.poll_interval_secs.unwrap_or(300),
        sync_filter: body.sync_filter.unwrap_or_else(|| json!({})).to_string(),
        default_task_state: body.default_task_state,
        default_assignee_type: body.default_assignee_type,
        default_assignee_id: body.default_assignee_id,
        enabled: body.enabled.unwrap_or(true),
        last_polled_at: None,
        created_at: now.clone(),
        updated_at: now,
    };

    let result = IntegrationRepo::create_integration(&*state.db, model)
        .await
        .map_err(integration_create_error)?;

    Ok((StatusCode::CREATED, Json(integration_response(result))))
}

pub async fn get_integration(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<Json<Option<IntegrationResponse>>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let row = IntegrationRepo::get_by_project_id(&*state.db, &project_id).await?;

    Ok(Json(row.map(integration_response)))
}

pub async fn update_integration(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(body): Json<PatchIntegrationRequest>,
) -> ApiResult<Json<IntegrationResponse>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Admin,
    )
    .await?;
    let existing = IntegrationRepo::get_by_project_id(&*state.db, &project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("integration", project_id.clone()))?;
    let platform = body
        .platform
        .as_deref()
        .map(|platform| platform.parse::<db::IntegrationPlatform>())
        .transpose()
        .map_err(|_| ApiError::bad_request("invalid platform"))?;

    let update = UpdateProjectIntegration {
        id: existing.id.clone(),
        updated_at: db::now_rfc3339(),
        project_id: None,
        platform,
        base_url: body.base_url,
        owner: body.owner,
        repo: body.repo,
        token_secret_ref: body.token_secret_ref,
        poll_interval_secs: body.poll_interval_secs,
        sync_filter: body.sync_filter.as_ref().map(Value::to_string),
        default_task_state: body.default_task_state.map(Some),
        default_assignee_type: body.default_assignee_type.map(Some),
        default_assignee_id: body.default_assignee_id.map(Some),
        enabled: body.enabled,
        last_polled_at: None,
    };

    IntegrationRepo::update_integration(&*state.db, update).await?;
    let updated = IntegrationRepo::get_by_project_id(&*state.db, &project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("integration", project_id))?;

    Ok(Json(integration_response(updated)))
}

pub async fn delete_integration(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<StatusCode> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Admin,
    )
    .await?;
    let existing = IntegrationRepo::get_by_project_id(&*state.db, &project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("integration", project_id))?;

    IntegrationRepo::delete_integration(&*state.db, &existing.id).await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn trigger_sync(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<Response> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Admin,
    )
    .await?;
    let existing = IntegrationRepo::get_by_project_id(&*state.db, &project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("integration", project_id))?;

    if !existing.enabled {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(api_types::ErrorResponse {
                code: "unprocessable_entity".to_owned(),
                message: "integration is disabled".to_owned(),
                details: None,
                request_id: middleware::current_request_id()
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            }),
        )
            .into_response());
    }

    let service = IntegrationService::new(
        state.db.clone(),
        state.event_bus.clone(),
        state.task_service.clone(),
    );
    let result = service.sync_integration(&existing).await?;
    let now = db::now_rfc3339();
    IntegrationRepo::update_last_polled_at(&*state.db, &existing.id, &now, &now).await?;

    Ok(Json(SyncTriggerResponse {
        imported: result.imported,
        skipped: result.skipped,
        errors: result.errors,
    })
    .into_response())
}

fn integration_create_error(error: DbError) -> ApiError {
    if is_unique_constraint(&error) {
        ApiError::conflict_with_code("integration_exists", "project already has an integration")
    } else {
        error.into()
    }
}

fn is_unique_constraint(error: &DbError) -> bool {
    match error {
        DbError::Sqlx(sqlx::Error::Database(database_error)) => {
            database_error
                .message()
                .to_ascii_lowercase()
                .contains("unique constraint failed")
                || matches!(database_error.code().as_deref(), Some("1555" | "2067"))
        }
        _ => false,
    }
}
