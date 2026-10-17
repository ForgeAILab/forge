use api_types::{CreateExternalLinkRequest, ExternalLinkResponse};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use db::{now_rfc3339, CreateTaskExternalLink, ExternalLinkRepo, IntegrationRepo, TaskRepo};

use crate::{
    errors::{ApiError, ApiResult},
    state::AppState,
};

pub async fn list_external_links(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<ExternalLinkResponse>>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    TaskRepo::get_by_id(&*state.db, &id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", id.clone()))?;

    let links = ExternalLinkRepo::list_by_task_id(&*state.db, &id).await?;
    Ok(Json(
        links.into_iter().map(external_link_response).collect(),
    ))
}

pub async fn create_external_link(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<CreateExternalLinkRequest>,
) -> ApiResult<(StatusCode, Json<ExternalLinkResponse>)> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    let task = TaskRepo::get_by_id(&*state.db, &id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", id.clone()))?;
    let integration = IntegrationRepo::get_by_project_id(&*state.db, &task.project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project has no integration", task.project_id))?;

    let platform = integration.platform.to_string();
    let remote_owner = integration.owner.clone();
    let remote_repo = integration.repo.clone();
    let global_id = compute_global_id(&integration, request.remote_issue_number);
    let remote_url = compute_remote_url(&integration, request.remote_issue_number);

    if ExternalLinkRepo::get_by_global_id(&*state.db, &global_id)
        .await?
        .is_some()
    {
        return Err(ApiError::conflict_with_code(
            "duplicate_external_link",
            "external link already exists",
        ));
    }

    let now = now_rfc3339();
    let link = ExternalLinkRepo::create_link(
        &*state.db,
        CreateTaskExternalLink {
            id: db::new_uuid_v4(),
            task_id: task.id,
            integration_id: integration.id,
            platform,
            remote_owner,
            remote_repo,
            remote_issue_number: request.remote_issue_number,
            remote_url,
            global_id,
            synced_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await?;

    Ok((StatusCode::CREATED, Json(external_link_response(link))))
}

pub async fn delete_external_link(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path((id, link_id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    let link = ExternalLinkRepo::get_by_id(&*state.db, &link_id)
        .await?
        .ok_or_else(|| ApiError::not_found("external link", link_id.clone()))?;
    if link.task_id != id {
        return Err(ApiError::not_found("external link", link_id));
    }

    ExternalLinkRepo::delete_link(&*state.db, &link.id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) fn compute_global_id(integration: &db::ProjectIntegration, issue_number: i64) -> String {
    let platform = integration.platform.to_string();
    let owner = &integration.owner;
    let repo = &integration.repo;
    let host = url::Url::parse(&integration.base_url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_string()))
        .unwrap_or_default();
    let is_well_known = matches!(host.as_str(), "api.github.com" | "github.com");
    if is_well_known {
        format!("{platform}:{owner}/{repo}#{issue_number}")
    } else {
        format!("{platform}:{host}:{owner}/{repo}#{issue_number}")
    }
}

pub(crate) fn compute_remote_url(
    integration: &db::ProjectIntegration,
    issue_number: i64,
) -> String {
    let owner = &integration.owner;
    let repo = &integration.repo;
    match &integration.platform {
        db::IntegrationPlatform::Github => {
            format!("https://github.com/{owner}/{repo}/issues/{issue_number}")
        }
        db::IntegrationPlatform::Gitea => {
            let base = integration.base_url.trim_end_matches('/');
            format!("{base}/{owner}/{repo}/issues/{issue_number}")
        }
    }
}

fn external_link_response(link: db::TaskExternalLink) -> ExternalLinkResponse {
    ExternalLinkResponse {
        id: link.id,
        task_id: link.task_id,
        integration_id: link.integration_id,
        platform: link.platform,
        remote_owner: link.remote_owner,
        remote_repo: link.remote_repo,
        remote_issue_number: link.remote_issue_number,
        remote_url: link.remote_url,
        global_id: link.global_id,
        synced_at: link.synced_at,
        created_at: link.created_at,
        updated_at: link.updated_at,
    }
}
