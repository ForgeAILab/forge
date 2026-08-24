use std::{collections::HashSet, path::PathBuf};

use api_types::{
    parse_project_hooks_json, AgentTokenBreakdown as ApiAgentTokenBreakdown, CiStepAnalytics,
    CreateProjectFromCharterApprovalRequest, CreateProjectFromCharterApprovalResponse,
    CreateProjectRequest, ModelTokenBreakdown as ApiModelTokenBreakdown, PaginatedResponse,
    ProjectAnalyticsResponse, ProjectHookRunResponse, ProjectHookRunStatus,
    ProjectHookRunsResponse, ProjectResponse, ProjectSettings, ReviewConfig,
    ReviewSummaryAnalytics, StateKind, TestLifecycleHookRequest, TokenUsageAnalytics,
    UpdateProjectRequest, UpdateProjectWorkflowRequest, WorkflowDefinition,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use db::{
    new_uuid_v4, now_rfc3339, AgentProfileRepo, AgentRepo, AgentTokenBreakdown, CiStepStats,
    CreateProject, ModelTokenBreakdown, PageRequest, ProjectAnalyticsRepo, ProjectHookRun,
    ProjectHookRunRepo, ProjectRepo, ProjectReviewSummary, ProjectTokenStats, RepoRepo, SortBy,
    SortOrder, UpdateProject,
};
use events::{event_timestamp, EventContext, ForgeEvent};
use serde::Deserialize;
use services::{
    create_project_from_charter_approval as materialize_project_from_charter_approval,
    workflow::{
        default_workflow::default_workflow, engine::WorkflowEngine, validation::validate_workflow,
    },
    CreateProjectAuthorization, CreateProjectFromCharterApprovalInput, ServiceError,
};

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::AuthenticatedUser,
    routes::{page_request, project_response, ListParams},
    state::AppState,
};

const DEFAULT_REVIEW_CONFIG_KEY: &str = "default_review_config";

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum CreateProjectBody {
    FromCharterApproval(CreateProjectFromCharterApprovalRequest),
    Direct(CreateProjectRequest),
}

#[derive(Debug, Deserialize)]
pub struct AnalyticsQuery {
    pub from: Option<String>,
    pub to: Option<String>,
}

pub async fn create_project(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Json(request): Json<CreateProjectBody>,
) -> ApiResult<Response> {
    match request {
        CreateProjectBody::FromCharterApproval(request) => {
            create_project_from_charter_approval(state, user, request).await
        }
        CreateProjectBody::Direct(request) => create_direct_project(state, user, request).await,
    }
}

async fn create_direct_project(
    state: AppState,
    user: AuthenticatedUser,
    request: CreateProjectRequest,
) -> ApiResult<Response> {
    let now = now_rfc3339();
    let mut settings = request.settings.unwrap_or_else(|| serde_json::json!({}));
    apply_default_review_config(&mut settings, request.default_review_config.as_ref())?;
    let workflow_definition = serde_json::to_string(&default_workflow())
        .map_err(|error| ApiError::internal(format!("serialize default workflow: {error}")))?;
    let workflow = WorkflowEngine::resolve_workflow(&workflow_definition);
    validate_project_settings(&state.db, &settings, &workflow, None, None).await?;
    let settings = serialize_settings(&settings)?;
    let (project_agent_identity_id, project_agent_profile_id) = match (
        request.project_agent_identity_id,
        request.project_agent_profile_id,
    ) {
        (Some(identity_id), Some(profile_id)) => {
            let identity = AgentRepo::get_by_id(&*state.db, &identity_id)
                .await?
                .filter(|agent| agent.owner_id.as_deref() == Some(user.user_id.as_str()))
                .ok_or_else(|| ApiError::not_found("agent", identity_id.clone()))?;
            let profile = AgentProfileRepo::get_profile(&*state.db, &profile_id)
                .await?
                .filter(|profile| profile.identity_id == identity.id)
                .ok_or_else(|| ApiError::not_found("agent_profile", profile_id.clone()))?;
            (Some(identity.id), Some(profile.id))
        }
        (None, None) => (None, None),
        _ => {
            return Err(ApiError::bad_request(
                "project_agent_identity_id and project_agent_profile_id must be provided together",
            ));
        }
    };
    let project = ProjectRepo::create_with_agent_binding(
        &*state.db,
        CreateProject {
            id: new_uuid_v4(),
            name: request.name,
            settings,
            workflow_definition,
            primary_repo_id: None,
            owner_id: Some(user.user_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        project_agent_identity_id,
        project_agent_profile_id,
    )
    .await?;

    // Auto-create owner membership (best-effort; may fail if user row doesn't exist yet)
    let _ = db::ProjectMemberRepo::add_member(
        &*state.db,
        db::CreateProjectMember {
            id: new_uuid_v4(),
            project_id: project.id.clone(),
            user_id: user.user_id,
            role: "owner".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await;

    state.event_bus.publish(ForgeEvent {
        event_type: "project.created".to_owned(),
        entity_id: project.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ProjectCreated {
            name: project.name.clone(),
        },
    });

    let mut response = project_response(project)?;
    response.execution_setup = Some(
        services::load_project_execution_setup(&state.db, &response.id)
            .await
            .map_err(|_| ApiError::internal("Project execution setup projection is unavailable"))?,
    );
    Ok((StatusCode::OK, Json(response)).into_response())
}

async fn create_project_from_charter_approval(
    state: AppState,
    user: AuthenticatedUser,
    request: CreateProjectFromCharterApprovalRequest,
) -> ApiResult<Response> {
    // The service derives the account scope/principal, input digest, and
    // durable receipt from this authenticated user. The route never accepts
    // a caller-provided digest or receipt and contains no approval SQL.
    let created = materialize_project_from_charter_approval(
        state.db.clone(),
        CreateProjectFromCharterApprovalInput {
            approval_id: request.approval_id,
            idempotency_key: request.idempotency_key,
            account_id: user.user_id.clone(),
            authorization: CreateProjectAuthorization::from_api(&request.authorization),
            // The shared service allocates the command correlation id for a
            // direct authenticated-user command.
            correlation_id: String::new(),
            causation_depth: 1,
            command_receipt: None,
            action_execution: None,
        },
    )
    .await?;
    let execution_setup = services::load_project_execution_setup(&state.db, &created.project.id)
        .await
        .map_err(|_| ApiError::internal("Project execution setup projection is unavailable"))?;
    let response = CreateProjectFromCharterApprovalResponse {
        project_id: created.project.id,
        project_agent_binding_id: created.project_agent_binding_id,
        project_chat_id: created.project_chat_id,
        charter_id: created.charter_id,
        charter_revision_id: created.charter_revision_id,
        handoff_id: created.handoff_id,
        target_message_id: created.target_message_id,
        target_turn_id: created.target_turn_id,
        execution_setup,
    };
    Ok((StatusCode::CREATED, Json(response)).into_response())
}

pub async fn list_projects(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<PaginatedResponse<ProjectResponse>>> {
    let page = ProjectRepo::list(&*state.db, page_request(&params)?).await?;
    let has_more = page.next_cursor.is_some();
    let next_cursor = page.next_cursor;
    let total_count = page.total_count.and_then(|count| u64::try_from(count).ok());
    // Owner visibility is canonical even if a legacy/direct-create row was
    // left without its best-effort membership row.  Membership additionally
    // grants visibility to collaborators; owner_id = NULL denotes a public
    // system Project.
    let mut visible_items = Vec::new();
    for project in page.items {
        if project_is_visible(&state, &project, &user.user_id).await? {
            visible_items.push(project);
        }
    }
    let response = PaginatedResponse {
        items: visible_items
            .into_iter()
            .map(project_response)
            .collect::<ApiResult<Vec<_>>>()?,
        next_cursor,
        has_more,
        total_count,
    };
    Ok(Json(response))
}

pub async fn get_project(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<ProjectResponse>> {
    let project = ProjectRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", id.clone()))?;
    if !project_is_visible(&state, &project, &user.user_id).await? {
        return Err(ApiError::not_found("project", id));
    }
    Ok(Json(project_response(project)?))
}

pub async fn list_project_hook_runs(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<ProjectHookRunsResponse>> {
    require_project_visible(&state, &id, &user.user_id).await?;
    let page = ProjectHookRunRepo::list_for_project(
        &*state.db,
        &id,
        PageRequest {
            cursor: params.cursor,
            limit: params.limit.unwrap_or(20).clamp(1, 100),
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await?;
    Ok(Json(ProjectHookRunsResponse {
        items: page
            .items
            .into_iter()
            .map(project_hook_run_response)
            .collect(),
        next_cursor: page.next_cursor,
    }))
}

pub async fn get_project_analytics(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<ProjectAnalyticsResponse>, ApiError> {
    ProjectRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", id.clone()))?;

    let from = params.from.as_deref();
    let to = params.to.as_deref();

    let ci_steps = ProjectAnalyticsRepo::get_project_ci_analytics(&*state.db, &id, from, to)
        .await?
        .into_iter()
        .map(
            |CiStepStats {
                 command,
                 total_runs,
                 pass_count,
                 fail_count,
                 avg_duration_ms,
                 p50_duration_ms,
                 p95_duration_ms,
                 last_run_at,
             }| CiStepAnalytics {
                command,
                total_runs,
                pass_count,
                fail_count,
                success_rate: if total_runs > 0 {
                    pass_count as f64 / total_runs as f64
                } else {
                    0.0
                },
                avg_duration_ms,
                p50_duration_ms,
                p95_duration_ms,
                last_run_at,
            },
        )
        .collect();

    let token_usage =
        ProjectAnalyticsRepo::get_project_token_analytics(&*state.db, &id, from, to).await?;
    let ProjectTokenStats {
        total_input_tokens,
        total_output_tokens,
        total_cache_read_tokens,
        total_cache_write_tokens,
        total_cost_usd,
        execution_count,
        by_model,
        by_agent,
    } = token_usage;
    let token_usage = TokenUsageAnalytics {
        total_input_tokens,
        total_output_tokens,
        total_cache_read_tokens,
        total_cache_write_tokens,
        total_cost_usd,
        execution_count,
        by_model: by_model
            .into_iter()
            .map(
                |ModelTokenBreakdown {
                     provider,
                     model,
                     input_tokens,
                     output_tokens,
                     cache_read_tokens,
                     cache_write_tokens,
                     cost_usd,
                     execution_count,
                 }| ApiModelTokenBreakdown {
                    provider,
                    model,
                    input_tokens,
                    output_tokens,
                    cache_read_tokens,
                    cache_write_tokens,
                    cost_usd,
                    execution_count,
                },
            )
            .collect(),
        by_agent: by_agent
            .into_iter()
            .map(
                |AgentTokenBreakdown {
                     agent_id,
                     agent_name,
                     executor_type,
                     model,
                     input_tokens,
                     output_tokens,
                     cache_read_tokens,
                     cache_write_tokens,
                     cost_usd,
                     execution_count,
                     success_rate,
                     avg_duration_ms,
                 }| ApiAgentTokenBreakdown {
                    agent_id,
                    agent_name,
                    executor_type,
                    model,
                    input_tokens,
                    output_tokens,
                    cache_read_tokens,
                    cache_write_tokens,
                    cost_usd,
                    execution_count,
                    success_rate,
                    avg_duration_ms,
                },
            )
            .collect(),
    };

    let review_summary =
        ProjectAnalyticsRepo::get_project_review_summary(&*state.db, &id, from, to).await?;
    let review_summary = review_summary_analytics(review_summary);

    Ok(Json(ProjectAnalyticsResponse {
        ci_steps,
        token_usage,
        review_summary,
    }))
}

pub async fn get_project_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<WorkflowDefinition>> {
    let project = ProjectRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", id))?;
    Ok(Json(WorkflowEngine::resolve_workflow(
        &project.workflow_definition,
    )))
}

pub async fn update_project_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<UpdateProjectWorkflowRequest>,
) -> ApiResult<Json<WorkflowDefinition>> {
    let UpdateProjectWorkflowRequest {
        template_name,
        definition,
    } = request;
    let (definition, workflow_template_name) = if let Some(template_name) = template_name {
        let template = state
            .workflow_template_service
            .get_template(&template_name)
            .await
            .map_err(|error| workflow_template_service_error(&template_name, error))?;
        (template.definition, Some(template_name))
    } else if let Some(definition) = definition {
        (definition, None)
    } else {
        return Err(ApiError::bad_request(
            "either template_name or definition must be provided",
        ));
    };
    validate_workflow(&definition).map_err(workflow_validation_error)?;

    let project = ProjectRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", id.clone()))?;
    let old_workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    validate_workflow_update_safety(&state.db, &id, &old_workflow, &definition).await?;

    let workflow_definition = serde_json::to_string(&definition)?;
    let updated_at = now_rfc3339();
    let result = sqlx::query(
        "UPDATE project SET workflow_definition = ?, workflow_template_name = ?, updated_at = ? WHERE id = ?",
    )
    .bind(&workflow_definition)
    .bind(&workflow_template_name)
    .bind(&updated_at)
    .bind(&id)
    .execute(state.db.pool())
    .await
    .map_err(db::DbError::from)?;

    if result.rows_affected() == 0 {
        return Err(ApiError::not_found("project", id));
    }

    Ok(Json(definition))
}

pub async fn delete_project(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    let staged_repositories = stage_managed_project_repositories(&state, &id).await?;
    if let Err(error) = ProjectRepo::delete(&*state.db, &id).await {
        restore_staged_repositories(&staged_repositories).await;
        return Err(error.into());
    }
    for staged in &staged_repositories {
        if let Err(error) = tokio::fs::remove_dir_all(&staged.staged).await {
            tracing::error!(
                project_id = %id,
                path = %staged.staged.display(),
                error = %error,
                "Project database teardown committed but its staged managed repository could not be removed"
            );
            return Err(ApiError::internal(
                "Project was deleted, but its staged managed repository cleanup failed",
            ));
        }
    }
    state.event_bus.publish(ForgeEvent {
        event_type: "project.deleted".to_owned(),
        entity_id: id,
        timestamp: event_timestamp(),
        context: EventContext::ProjectDeleted {},
    });
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug)]
struct StagedRepositoryRemoval {
    original: PathBuf,
    staged: PathBuf,
}

async fn stage_managed_project_repositories(
    state: &AppState,
    project_id: &str,
) -> ApiResult<Vec<StagedRepositoryRemoval>> {
    let managed_root = state.effective_config.workspace.root.join("repos");
    let Ok(canonical_root) = tokio::fs::canonicalize(&managed_root).await else {
        return Ok(Vec::new());
    };
    let mut seen = HashSet::new();
    let mut staged = Vec::new();
    let mut cursor = None;
    loop {
        let repos = match RepoRepo::list_by_project(
            &*state.db,
            project_id,
            PageRequest {
                cursor,
                limit: 500,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Asc,
            },
        )
        .await
        {
            Ok(repos) => repos,
            Err(error) => {
                restore_staged_repositories(&staged).await;
                return Err(error.into());
            }
        };
        for local_path in repos.items.into_iter().filter_map(|repo| repo.local_path) {
            let original = PathBuf::from(local_path);
            let Ok(canonical) = tokio::fs::canonicalize(&original).await else {
                continue;
            };
            // Only repositories provisioned as direct children of Forge's managed
            // repos root are Project-owned filesystem state. Explicitly linked
            // repositories elsewhere on disk are never removed.
            if canonical.parent() != Some(canonical_root.as_path())
                || !seen.insert(canonical.clone())
            {
                continue;
            }
            let tombstone = canonical_root.join(format!(
                ".forge-project-delete-{}-{}",
                project_id,
                new_uuid_v4()
            ));
            if let Err(error) = tokio::fs::rename(&canonical, &tombstone).await {
                restore_staged_repositories(&staged).await;
                return Err(ApiError::internal(format!(
                    "stage managed Project repository for deletion: {error}"
                )));
            }
            staged.push(StagedRepositoryRemoval {
                original: canonical,
                staged: tombstone,
            });
        }
        let Some(next_cursor) = repos.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(staged)
}

async fn restore_staged_repositories(staged: &[StagedRepositoryRemoval]) {
    for repository in staged.iter().rev() {
        if let Err(error) = tokio::fs::rename(&repository.staged, &repository.original).await {
            tracing::error!(
                path = %repository.staged.display(),
                restore_path = %repository.original.display(),
                error = %error,
                "failed to restore a managed repository after Project teardown rolled back"
            );
        }
    }
}

pub async fn pause_project(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ProjectResponse>> {
    let project = ProjectRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", id.clone()))?;
    if project.paused_at.is_some() {
        return Ok(Json(project_response(project)?));
    }

    let paused_at = now_rfc3339();
    ProjectRepo::set_paused_at(&*state.db, &id, Some(paused_at.clone())).await?;
    let project = ProjectRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", id.clone()))?;
    tracing::info!(project_id = %project.id, project_name = %project.name, "project paused");
    state.event_bus.publish(ForgeEvent {
        event_type: "project.paused".to_owned(),
        entity_id: project.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ProjectPaused { paused_at },
    });

    Ok(Json(project_response(project)?))
}

pub async fn resume_project(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ProjectResponse>> {
    let project = ProjectRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", id.clone()))?;
    if project.paused_at.is_none() {
        return Ok(Json(project_response(project)?));
    }

    ProjectRepo::set_paused_at(&*state.db, &id, None).await?;
    let project = ProjectRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", id.clone()))?;
    tracing::info!(project_id = %project.id, project_name = %project.name, "project resumed");
    state.event_bus.publish(ForgeEvent {
        event_type: "project.resumed".to_owned(),
        entity_id: project.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ProjectResumed {},
    });

    Ok(Json(project_response(project)?))
}

pub async fn update_project(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<UpdateProjectRequest>,
) -> ApiResult<Json<ProjectResponse>> {
    let UpdateProjectRequest {
        name,
        settings,
        default_review_config,
        primary_repo_id,
        paused,
        project_hooks,
        version,
    } = request;
    let project_hooks_json = match project_hooks {
        Some(rules) => {
            let serialized = serde_json::to_string(&rules).map_err(|error| {
                ApiError::bad_request(format!("invalid project hooks: {error}"))
            })?;
            parse_project_hooks_json(&serialized).map_err(ApiError::bad_request)?;
            Some(serialized)
        }
        None => None,
    };
    let settings = update_settings(
        &state.db,
        &id,
        &user.user_id,
        settings,
        default_review_config.as_ref(),
    )
    .await?;
    let project = ProjectRepo::update_at_version(
        &*state.db,
        UpdateProject {
            id,
            name,
            settings,
            primary_repo_id: primary_repo_id.map(Some),
            paused_at: paused.map(|paused: bool| paused.then(now_rfc3339)),
            updated_at: now_rfc3339(),
        },
        version,
        project_hooks_json,
    )
    .await?;
    state.event_bus.publish(ForgeEvent {
        event_type: "project.updated".to_owned(),
        entity_id: project.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ProjectUpdated {},
    });

    Ok(Json(project_response(project)?))
}

async fn require_project_visible(
    state: &AppState,
    project_id: &str,
    user_id: &str,
) -> ApiResult<db::Project> {
    let project = ProjectRepo::get_by_id(&*state.db, project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    if !project_is_visible(state, &project, user_id).await? {
        return Err(ApiError::not_found("project", project_id.to_owned()));
    }
    Ok(project)
}

async fn project_is_visible(
    state: &AppState,
    project: &db::Project,
    user_id: &str,
) -> ApiResult<bool> {
    if project.owner_id.is_none() || project.owner_id.as_deref() == Some(user_id) {
        return Ok(true);
    }
    Ok(
        db::ProjectMemberRepo::get_member(&*state.db, &project.id, user_id)
            .await?
            .is_some(),
    )
}

fn project_hook_run_response(run: ProjectHookRun) -> ProjectHookRunResponse {
    ProjectHookRunResponse {
        id: run.id,
        project_id: run.project_id,
        rule_id: run.rule_id,
        trigger_type: run.trigger_type,
        dedupe_key: run.dedupe_key,
        status: match run.status {
            db::ProjectHookRunStatus::Queued => ProjectHookRunStatus::Queued,
            db::ProjectHookRunStatus::Running => ProjectHookRunStatus::Running,
            db::ProjectHookRunStatus::Dispatched => ProjectHookRunStatus::Dispatched,
            db::ProjectHookRunStatus::Skipped => ProjectHookRunStatus::Skipped,
            db::ProjectHookRunStatus::Failed => ProjectHookRunStatus::Failed,
            db::ProjectHookRunStatus::Completed => ProjectHookRunStatus::Completed,
        },
        source_task_id: run.source_task_id,
        source_execution_id: run.source_execution_id,
        automation_task_id: run.automation_task_id,
        execution_id: run.execution_id,
        agent_id: run.agent_id,
        reason: run.reason,
        created_at: run.created_at,
        updated_at: run.updated_at,
        completed_at: run.completed_at,
    }
}

pub async fn test_project_lifecycle_hook(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<TestLifecycleHookRequest>,
) -> ApiResult<Json<api_types::LifecycleHookTestResponse>> {
    let response = state
        .task_service
        .test_lifecycle_hook(&id, &request.task_id, request.event, request.hook_index)
        .await
        .map_err(|error| match error {
            ServiceError::InvalidOperation { message } => ApiError::bad_request(message),
            other => ApiError::from(other),
        })?;
    Ok(Json(response))
}

async fn validate_workflow_update_safety(
    db: &db::SqliteDb,
    project_id: &str,
    old_workflow: &WorkflowDefinition,
    new_workflow: &WorkflowDefinition,
) -> ApiResult<()> {
    let new_non_terminal_states: HashSet<&str> = new_workflow
        .states
        .iter()
        .filter(|state| state.kind != StateKind::Terminal)
        .map(|state| state.name.as_str())
        .collect();
    let old_terminal_states: HashSet<&str> = old_workflow
        .states
        .iter()
        .filter(|state| state.kind == StateKind::Terminal)
        .map(|state| state.name.as_str())
        .collect();
    let statuses = sqlx::query_as::<_, (String, i64)>(
        "SELECT status, COUNT(*) FROM task WHERE project_id = ? AND deleted_at IS NULL GROUP BY status",
    )
    .bind(project_id)
    .fetch_all(db.pool())
    .await
    .map_err(db::DbError::from)?;

    for (status, count) in statuses {
        if !new_non_terminal_states.contains(status.as_str())
            && !old_terminal_states.contains(status.as_str())
        {
            return Err(ApiError::conflict_with_code(
                "workflow_state_in_use",
                format!("cannot remove state {status}: {count} active tasks in this state"),
            ));
        }
    }

    Ok(())
}

fn workflow_validation_error(error: ServiceError) -> ApiError {
    match error {
        ServiceError::InvalidOperation { message } => ApiError::bad_request(message),
        other => ApiError::from(other),
    }
}

fn workflow_template_service_error(name: &str, error: ServiceError) -> ApiError {
    match error {
        ServiceError::NotFound { .. } => ApiError::not_found("workflow_template", name),
        ServiceError::InvalidOperation { message } => ApiError::bad_request(message),
        other => ApiError::from(other),
    }
}

async fn update_settings(
    db: &db::SqliteDb,
    project_id: &str,
    user_id: &str,
    settings: Option<serde_json::Value>,
    default_review_config: Option<&ReviewConfig>,
) -> ApiResult<Option<String>> {
    if settings.is_none() && default_review_config.is_none() {
        return Ok(None);
    }

    let project = ProjectRepo::get_by_id(db, project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;

    let mut settings = match settings {
        Some(settings) => settings,
        None => serde_json::from_str(&project.settings)
            .map_err(|error| ApiError::bad_request(format!("invalid settings: {error}")))?,
    };
    apply_default_review_config(&mut settings, default_review_config)?;
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    validate_project_settings(db, &settings, &workflow, Some(project_id), Some(user_id)).await?;
    Ok(Some(serialize_settings(&settings)?))
}

async fn validate_project_settings(
    db: &db::SqliteDb,
    settings: &serde_json::Value,
    workflow: &WorkflowDefinition,
    project_id: Option<&str>,
    user_id: Option<&str>,
) -> ApiResult<()> {
    let settings: ProjectSettings = serde_json::from_value(settings.clone())
        .map_err(|error| ApiError::bad_request(format!("invalid settings: {error}")))?;
    let role_names: HashSet<&str> = workflow
        .roles
        .iter()
        .map(|role| role.name.as_str())
        .collect();

    for assignment in &settings.default_role_assignments {
        if !role_names.contains(assignment.role_name.as_str()) {
            return Err(ApiError::bad_request(format!(
                "unknown role: {}",
                assignment.role_name
            )));
        }

        match assignment.assignee_type.as_str() {
            "agent" => {
                if option_is_blank(assignment.assignee_id.as_ref()) {
                    return Err(ApiError::bad_request(format!(
                        "default role assignment for role '{}' requires assignee_id",
                        assignment.role_name
                    )));
                }
                if let (Some(project_id), Some(user_id), Some(assignee_id)) =
                    (project_id, user_id, assignment.assignee_id.as_ref())
                {
                    let usable_agents = db
                        .list_agents_usable_in_project(project_id, user_id)
                        .await
                        .map_err(ApiError::from)?;
                    let is_usable = usable_agents
                        .into_iter()
                        .any(|agent| agent.id == *assignee_id);
                    if !is_usable {
                        return Err(ApiError::bad_request("agent not usable in this project"));
                    }
                }
            }
            "user" => {
                if option_is_blank(assignment.assignee_id.as_ref()) {
                    return Err(ApiError::bad_request(format!(
                        "default role assignment for role '{}' requires assignee_id",
                        assignment.role_name
                    )));
                }
                if is_legacy_manual_default_assignee(assignment.assignee_id.as_deref()) {
                    continue;
                }
                if let (Some(project_id), Some(assignee_id)) =
                    (project_id, assignment.assignee_id.as_ref())
                {
                    let member =
                        db::ProjectMemberRepo::get_member(db, project_id, assignee_id).await?;
                    if member.is_none() {
                        return Err(ApiError::bad_request("assignee must be a project member"));
                    }
                }
            }
            _ => {
                return Err(ApiError::bad_request(format!(
                    "default role assignment for role '{}' must use assignee_type 'agent' or 'user'",
                    assignment.role_name
                )));
            }
        }
    }

    for (name, value) in [
        ("review", settings.retry_budgets.review),
        ("merge_fix", settings.retry_budgets.merge_fix),
    ] {
        if value.is_some_and(|value| value < 0) {
            return Err(ApiError::bad_request(format!(
                "retry_budgets.{name} must be 0 or greater"
            )));
        }
    }

    for (event, hooks) in &settings.lifecycle_hooks {
        for hook in hooks {
            if let api_types::LifecycleHookDef::Script { blocking, .. } = hook {
                if *blocking && *event != api_types::LifecycleEvent::BeforeWork {
                    return Err(ApiError::bad_request(
                        "blocking lifecycle hooks are only supported for before_work",
                    ));
                }
            }
        }
    }

    Ok(())
}

fn option_is_blank(value: Option<&String>) -> bool {
    value.map(|value| value.trim().is_empty()).unwrap_or(true)
}

fn is_legacy_manual_default_assignee(assignee_id: Option<&str>) -> bool {
    assignee_id == Some("human")
}

fn apply_default_review_config(
    settings: &mut serde_json::Value,
    default_review_config: Option<&ReviewConfig>,
) -> ApiResult<()> {
    let Some(default_review_config) = default_review_config else {
        return Ok(());
    };
    let settings = settings.as_object_mut().ok_or_else(|| {
        ApiError::bad_request("settings must be a JSON object when default_review_config is set")
    })?;
    let value = serde_json::to_value(default_review_config).map_err(|error| {
        ApiError::bad_request(format!("invalid default_review_config: {error}"))
    })?;
    settings.insert(DEFAULT_REVIEW_CONFIG_KEY.to_owned(), value);
    Ok(())
}

fn serialize_settings(settings: &serde_json::Value) -> ApiResult<String> {
    serde_json::to_string(settings)
        .map_err(|error| ApiError::bad_request(format!("invalid settings: {error}")))
}

fn review_summary_analytics(summary: ProjectReviewSummary) -> ReviewSummaryAnalytics {
    ReviewSummaryAnalytics {
        total_reviews: summary.total_reviews,
        passed: summary.passed,
        failed: summary.failed,
        cancelled: summary.cancelled,
        avg_duration_ms: summary.avg_duration_ms,
        pass_rate: summary.pass_rate,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        is_legacy_manual_default_assignee, restore_staged_repositories, StagedRepositoryRemoval,
    };

    #[test]
    fn recognizes_legacy_manual_default_assignee() {
        assert!(is_legacy_manual_default_assignee(Some("human")));
        assert!(!is_legacy_manual_default_assignee(Some("user-123")));
        assert!(!is_legacy_manual_default_assignee(None));
    }

    #[tokio::test]
    async fn staged_repository_restore_reverses_each_rename() {
        let root = std::env::temp_dir().join(format!(
            "forge-project-delete-restore-test-{}",
            uuid::Uuid::new_v4()
        ));
        let original = root.join("repo");
        let staged = root.join(".forge-project-delete-test");
        tokio::fs::create_dir_all(&staged)
            .await
            .expect("staged repository fixture");
        tokio::fs::write(staged.join("tracked"), b"preserved")
            .await
            .expect("repository content");

        restore_staged_repositories(&[StagedRepositoryRemoval {
            original: original.clone(),
            staged: staged.clone(),
        }])
        .await;

        assert!(original.join("tracked").is_file());
        assert!(!staged.exists());
        tokio::fs::remove_dir_all(&root)
            .await
            .expect("remove temporary managed repo root");
    }
}
