use api_types::{
    CreateRepoRequest, PaginatedResponse, RepoResponse, RepoSyncResponse, UpdateRepoRequest,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use db::{
    new_uuid_v4, now_rfc3339, CreatePrProviderConfig, CreateRepo, PrProviderConfigRepo,
    ProjectRepo, RepoRepo, UpdateProject, UpdateRepo, WorkMode,
};

use crate::{
    errors::{ApiError, ApiResult},
    path_input::canonical_directory,
    routes::{page_request, paginated, repo_response, ListParams},
    state::AppState,
};

pub async fn create_repo(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Json(request): Json<CreateRepoRequest>,
) -> ApiResult<Json<RepoResponse>> {
    let project = ProjectRepo::get_by_id(&*state.db, &project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.clone()))?;
    if project.primary_repo_id.is_some() {
        return Err(ApiError::conflict_with_code(
            "project_already_has_primary_repo",
            format!("project {project_id} already has a primary repo"),
        ));
    }
    let local_path = normalize_optional_local_path(request.local_path)?;
    let name = request
        .name
        .unwrap_or_else(|| repo_name_from_remote_url(&request.remote_url));
    let now = now_rfc3339();
    let repo = RepoRepo::create(
        &*state.db,
        CreateRepo {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            name,
            local_path,
            remote_url: request.remote_url,
            work_mode: request
                .work_mode
                .map(work_mode_domain)
                .unwrap_or(WorkMode::DirectMerge),
            default_branch: request.default_branch.unwrap_or_else(|| "main".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await?;
    if let Some(pr_provider) = &request.pr_provider {
        let pr_config = request.pr_provider_config.as_ref();
        let now = now_rfc3339();
        PrProviderConfigRepo::create(
            &*state.db,
            CreatePrProviderConfig {
                id: new_uuid_v4(),
                repo_id: repo.id.clone(),
                provider_type: pr_provider.clone(),
                base_url: pr_config.and_then(|c| c.base_url.clone()),
                polling_interval_seconds: pr_config
                    .and_then(|c| c.polling_interval_seconds)
                    .unwrap_or(300),
                token_secret_ref: pr_config.and_then(|c| c.token.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await?;
    }
    ProjectRepo::update_at_version(
        &*state.db,
        UpdateProject {
            id: project_id,
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo.id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await?;
    Ok(Json(repo_response(repo)))
}

pub async fn list_repos(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<PaginatedResponse<RepoResponse>>> {
    let page = RepoRepo::list_by_project(&*state.db, &project_id, page_request(&params)?).await?;
    Ok(Json(paginated(page, repo_response)))
}

pub async fn get_repo(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<RepoResponse>> {
    let repo = RepoRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("repo", id))?;
    Ok(Json(repo_response(repo)))
}

pub async fn update_repo(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<UpdateRepoRequest>,
) -> ApiResult<Json<RepoResponse>> {
    let local_path = normalize_update_local_path(request.local_path)?;
    let repo = RepoRepo::update(
        &*state.db,
        UpdateRepo {
            id,
            name: request.name,
            local_path,
            remote_url: request.remote_url,
            work_mode: request.work_mode.map(work_mode_domain),
            default_branch: request.default_branch,
            updated_at: now_rfc3339(),
        },
    )
    .await?;
    Ok(Json(repo_response(repo)))
}

pub async fn delete_repo(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    RepoRepo::delete(&*state.db, &id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn sync_repo(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<RepoSyncResponse>> {
    let repo = RepoRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("repo", id.clone()))?;
    let local_path = repo.local_path.ok_or_else(|| {
        ApiError::bad_request_with_code(
            "repo.no_local_path",
            "repository has no local path to sync",
        )
    })?;
    let path = std::path::PathBuf::from(&local_path);
    if !git::is_git_repo(&path).await {
        return Err(ApiError::bad_request_with_code(
            "repo.not_a_git_repo",
            format!("{local_path} is not a git repository"),
        ));
    }
    let pull_output = git::pull_ff_only(&path).await.map_err(git_sync_error)?;
    let push_output = git::push(&path).await.map_err(git_sync_error)?;
    Ok(Json(RepoSyncResponse {
        pull_output,
        push_output,
    }))
}

fn git_sync_error(error: git::GitError) -> ApiError {
    match error {
        git::GitError::CommandFailed { stderr, stdout, .. } => {
            let detail = if !stderr.trim().is_empty() {
                stderr
            } else {
                stdout
            };
            ApiError::bad_request_with_code("repo.sync_failed", detail.trim().to_string())
        }
        other => ApiError::internal(format!("git sync failed: {other}")),
    }
}

fn normalize_optional_local_path(local_path: Option<String>) -> ApiResult<Option<String>> {
    local_path
        .map(|path| canonical_directory(&path).map(|path| path.to_string_lossy().into_owned()))
        .transpose()
}

fn normalize_update_local_path(
    local_path: Option<Option<String>>,
) -> ApiResult<Option<Option<String>>> {
    local_path.map(normalize_optional_local_path).transpose()
}

fn repo_name_from_remote_url(remote_url: &str) -> String {
    let segment = remote_url
        .trim()
        .trim_end_matches('/')
        .rsplit(['/', ':'])
        .next()
        .unwrap_or(remote_url);
    segment.strip_suffix(".git").unwrap_or(segment).to_owned()
}

fn work_mode_domain(work_mode: api_types::WorkMode) -> WorkMode {
    match work_mode {
        api_types::WorkMode::DirectMerge => WorkMode::DirectMerge,
        api_types::WorkMode::PullRequest => WorkMode::PullRequest,
    }
}
