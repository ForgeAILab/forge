use std::{collections::HashSet, path::Path as FsPath};

use api_types::{
    parse_project_hooks_json, CiStepAnalytics, CostCoverage,
    CreateProjectFromCharterApprovalRequest, CreateProjectFromCharterApprovalResponse,
    CreateProjectRequest, OutcomeCostMetric, OutcomeCostScope, OutcomeEligibility,
    OutcomeIneligibilityReason, OutcomeKind, PaginatedResponse, ProjectAnalyticsResponse,
    ProjectHookRunResponse, ProjectHookRunStatus, ProjectHookRunsResponse, ProjectResponse,
    ProjectSettings, ReviewConfig, ReviewSummaryAnalytics, StateKind, TestLifecycleHookRequest,
    UpdateProjectRequest, UpdateProjectWorkflowRequest, WorkflowDefinition,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use db::{
    new_uuid_v4, now_rfc3339, AgentProfileRepo, AgentRepo, CiStepStats, CreateProject, PageRequest,
    ProjectAnalyticsRepo, ProjectDeletionPaths, ProjectHookRun, ProjectHookRunRepo, ProjectRepo,
    ProjectReviewSummary, SortBy, SortOrder, UpdateProject, UsageAnalyticsRepo,
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
    routes::{page_request, project_agents::require_project_admin, project_response, ListParams},
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
    let page = ProjectRepo::list_visible(&*state.db, &user.user_id, page_request(&params)?).await?;
    let has_more = page.next_cursor.is_some();
    let next_cursor = page.next_cursor;
    let total_count = page.total_count.and_then(|count| u64::try_from(count).ok());
    let response = PaginatedResponse {
        items: page
            .items
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
    let project = require_project_visible(&state, &id, &user.user_id).await?;
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
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<ProjectAnalyticsResponse>, ApiError> {
    require_project_visible(&state, &id, &user.user_id).await?;

    let from = params.from.as_deref();
    let to = params.to.as_deref();
    validate_analytics_window(from, to)?;

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
        UsageAnalyticsRepo::get_project_usage_analytics(&*state.db, &id, from, to).await?;

    let review_summary =
        ProjectAnalyticsRepo::get_project_review_summary(&*state.db, &id, from, to).await?;
    let review_summary = review_summary_analytics(review_summary);
    let released_milestones =
        UsageAnalyticsRepo::count_project_released_milestones(&*state.db, &id, from, to).await?;
    let outcome_economics =
        released_milestone_outcome(&id, from, to, &token_usage.cost, released_milestones)?;

    Ok(Json(ProjectAnalyticsResponse {
        window: api_types::AnalyticsWindow {
            from: params.from,
            to: params.to,
        },
        ci_steps,
        token_usage,
        review_summary,
        outcome_economics,
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
    // Workflow edits change the role/capability decision for every parked
    // Task. Persist the edit, clear stale deterministic refusals, and bump
    // each affected Task version in one DB transaction so neither a crash nor
    // a stale dispatcher can restore the old refusal.
    ProjectRepo::update_workflow(
        &*state.db,
        &id,
        &workflow_definition,
        workflow_template_name.as_deref(),
        project.version,
        &updated_at,
    )
    .await?;

    Ok(Json(definition))
}

/// `?force=true` is the caller's explicit decision to destroy in-flight agent
/// work. Without it, a Project holding a running Execution or an active
/// Workspace lease is refused with `409 project_in_use`.
#[derive(Debug, Default, serde::Deserialize)]
pub struct DeleteProjectQuery {
    #[serde(default)]
    pub force: bool,
}

pub async fn delete_project(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Query(query): Query<DeleteProjectQuery>,
) -> ApiResult<StatusCode> {
    // Authorization must precede both the in-use counts and filesystem
    // cleanup. `require_project_admin` intentionally returns a project-scoped
    // 404 for callers who are not members, avoiding an existence/count oracle.
    require_project_admin(&state, &id, &user.user_id).await?;

    const MAX_FORCE_DELETE_ATTEMPTS: usize = 3;
    let mut attempt = 0;
    let late_project_paths = loop {
        // Force retries are bounded so a continuously-admitted execution or
        // lease produces a truthful 409 instead of an unbounded cancellation
        // loop.
        if query.force {
            // Force is an active cancellation request, not merely permission
            // to cascade away a still-live execution row. Provider failures
            // leave the Project intact and are surfaced to the caller.
            state.task_service.prepare_project_deletion(&id).await?;
        } else {
            // Run the guarded DB decision before touching any path. The final
            // delete below repeats this check to close the admission race.
            ProjectRepo::ensure_deletable(&*state.db, &id).await?;
        }

        if query.force {
            // Re-check immediately before the final DB boundary. A new
            // execution or lease admitted after the first cancellation pass
            // is handled by this pass or by the final in-use CAS below.
            state.task_service.prepare_project_deletion(&id).await?;
        }

        // The service has already terminalized/revoked force-mode activity.
        // The DB method repeats the in-use guard under BEGIN IMMEDIATE for
        // both modes, so a new execution or lease cannot be silently deleted.
        match ProjectRepo::delete_with_workspace_paths(&*state.db, &id).await {
            Ok(project_paths) => break project_paths,
            Err(db::DbError::ProjectInUse { .. })
                if query.force && attempt + 1 < MAX_FORCE_DELETE_ATTEMPTS =>
            {
                attempt += 1;
                continue;
            }
            Err(db::DbError::ProjectInUse {
                project_id,
                running_executions,
                active_leases,
            }) if query.force => {
                return Err(ApiError::conflict_with_code_and_details(
                    "project_in_use",
                    format!(
                        "project {project_id} remains in use after force-delete cancellation; deletion was not performed"
                    ),
                    serde_json::json!({
                        "project_id": project_id,
                        "running_executions": running_executions,
                        "active_leases": active_leases,
                        "force_cancellation_incomplete": true,
                    }),
                ));
            }
            Err(error) => {
                return Err(error.into());
            }
        }
    };
    // No Project-owned filesystem path is mutated before the authoritative DB
    // commit. The final transaction returns exact workspace/repository paths
    // (and Task IDs for orphan worktree directories) so this pass can clean
    // only the deleted Project's confined targets after commit.
    cleanup_project_owned_paths(&state, &id, &late_project_paths).await;
    state.event_bus.publish(ForgeEvent {
        event_type: "project.deleted".to_owned(),
        entity_id: id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ProjectDeleted {},
    });
    // The authoritative DB deletion already committed. Returning a 500 here
    // would invite an unsafe retry even though the Project no longer exists;
    // failed filesystem cleanup is recorded in the error log for recovery.
    Ok(StatusCode::NO_CONTENT)
}

async fn cleanup_project_owned_paths(
    state: &AppState,
    project_id: &str,
    project_paths: &ProjectDeletionPaths,
) {
    let workspace_root = state.effective_config.workspace.root.clone();
    for task_id in &project_paths.task_ids {
        // A new Task may reuse the captured task ID after the Project delete
        // commits. The ownership check and quarantine rename share the DB
        // write lock, so a creator cannot claim the ID between the check and
        // reservation.
        match remove_confined_direct_child_if_unowned(
            state,
            "SELECT 1 FROM task WHERE id = ? LIMIT 1",
            task_id,
            &workspace_root,
            &workspace_root.join(task_id),
            "Task workspace",
        )
        .await
        {
            Ok(true) => {
                tracing::warn!(
                    project_id = %project_id,
                    task_id,
                    "skipping Project task-path cleanup because the Task ID is live again"
                );
                continue;
            }
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    project_id = %project_id,
                    task_id,
                    ?error,
                    "skipping Project task-path cleanup because ownership could not be rechecked"
                );
                continue;
            }
        }
    }
    for path in &project_paths.workspace_paths {
        // Worktree paths are not globally unique in SQLite. A different live
        // Workspace may have claimed this path while the deleted Project's
        // cleanup was pending. The live-row check and quarantine rename are
        // serialized under BEGIN IMMEDIATE, so never remove it after a claim
        // wins.
        match remove_confined_direct_child_if_unowned(
            state,
            "SELECT 1 FROM workspace WHERE worktree_path = ? LIMIT 1",
            path,
            &workspace_root,
            FsPath::new(path),
            "Task workspace",
        )
        .await
        {
            Ok(true) => {
                tracing::warn!(
                    project_id = %project_id,
                    path,
                    "skipping Project workspace cleanup because the path is live again"
                );
                continue;
            }
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    project_id = %project_id,
                    path,
                    ?error,
                    "skipping Project workspace cleanup because ownership could not be rechecked"
                );
                continue;
            }
        }
    }

    let managed_root = workspace_root.join("repos");
    for repository in &project_paths.repository_paths {
        if let Some(local_path) = repository.local_path.as_deref() {
            // `repo.local_path` is not unique. Another Project can attach the
            // same directory after this deletion commits; the live-row check
            // and quarantine rename are serialized under BEGIN IMMEDIATE.
            match remove_confined_direct_child_if_unowned(
                state,
                "SELECT 1 FROM repo WHERE local_path = ? LIMIT 1",
                local_path,
                &managed_root,
                FsPath::new(local_path),
                "managed Project repository",
            )
            .await
            {
                Ok(true) => {
                    tracing::warn!(
                        project_id = %project_id,
                        repo_id = %repository.id,
                        path = local_path,
                        "skipping managed repository cleanup because the path is live again"
                    );
                    continue;
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::error!(
                        project_id = %project_id,
                        repo_id = %repository.id,
                        path = local_path,
                        ?error,
                        "skipping managed repository cleanup because ownership could not be rechecked"
                    );
                    continue;
                }
            }
        }
    }

    let cache_root = workspace_root.join(".repos");
    for repository in &project_paths.repository_paths {
        // Cache directories are keyed by repository ID. Do not remove a
        // cache that has already been reintroduced under a live Repo row; the
        // ownership check and quarantine rename share the DB write lock.
        match remove_confined_direct_child_if_unowned(
            state,
            "SELECT 1 FROM repo WHERE id = ? LIMIT 1",
            &repository.id,
            &cache_root,
            &cache_root.join(&repository.id),
            "managed repository cache",
        )
        .await
        {
            Ok(true) => {
                tracing::warn!(
                    project_id = %project_id,
                    repo_id = %repository.id,
                    "skipping repository-cache cleanup because the Repo ID is live again"
                );
                continue;
            }
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    project_id = %project_id,
                    repo_id = %repository.id,
                    ?error,
                    "skipping repository-cache cleanup because ownership could not be rechecked"
                );
                continue;
            }
        }
    }

    let project_root = state.effective_config.forge.data_dir.join("projects");
    // A Project ID can be explicitly reused after deletion. Preserve a live
    // replacement's Agent workspace rather than deleting it as stale cleanup;
    // the check and quarantine rename share the DB write lock.
    match remove_confined_direct_child_if_unowned(
        state,
        "SELECT 1 FROM project WHERE id = ? LIMIT 1",
        project_id,
        &project_root,
        &project_root.join(project_id),
        "Project Agent workspace",
    )
    .await
    {
        Ok(true) => {
            tracing::warn!(
                project_id = %project_id,
                "skipping Project Agent workspace cleanup because the Project ID is live again"
            );
        }
        Ok(false) => {}
        Err(error) => {
            tracing::error!(
                project_id = %project_id,
                ?error,
                "skipping Project Agent workspace cleanup because ownership could not be rechecked"
            );
        }
    }
}

/// Re-check a captured filesystem target against live authoritative rows and
/// reserve it while holding SQLite's write lock. Paths are not globally unique,
/// so a later Project/Task/Workspace may have claimed the same path after the
/// deletion transaction captured it. The ownership check and an O(1) rename to
/// a unique quarantine child share `BEGIN IMMEDIATE`, preventing a creator
/// from claiming the target between the check and reservation. The recursive
/// removal runs only after the transaction commits, so SQLite's writer lock is
/// not held for the duration of filesystem cleanup. Any read/lock/rename or
/// removal failure is returned so callers can skip cleanup rather than risk
/// deleting another owner's path.
async fn remove_confined_direct_child_if_unowned(
    state: &AppState,
    ownership_query: &str,
    ownership_value: &str,
    root: &std::path::Path,
    candidate: &std::path::Path,
    kind: &str,
) -> ApiResult<bool> {
    let mut transaction = db::begin_immediate(state.db.pool())
        .await
        .map_err(|error| ApiError::internal(format!("lock Project cleanup ownership: {error}")))?;
    let live = sqlx::query_scalar::<_, i64>(ownership_query)
        .bind(ownership_value)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|error| ApiError::internal(format!("recheck Project cleanup ownership: {error}")))?
        .is_some();
    if live {
        transaction.commit().await.map_err(|error| {
            ApiError::internal(format!("release Project cleanup lock: {error}"))
        })?;
        return Ok(true);
    }

    let quarantine = quarantine_confined_direct_child(root, candidate, kind).await?;
    if let Err(error) = transaction.commit().await {
        if let Some(quarantine) = quarantine {
            // The ownership reservation did not commit. Preserve the bytes:
            // restore the original name when it is still free, otherwise
            // leave the unique quarantine in place for operator recovery.
            let candidate_is_free = match tokio::fs::symlink_metadata(candidate).await {
                Err(candidate_error) if candidate_error.kind() == std::io::ErrorKind::NotFound => {
                    true
                }
                Ok(_) => false,
                Err(candidate_error) => {
                    tracing::error!(
                        path = %candidate.display(),
                        error = %candidate_error,
                        "failed to inspect Project cleanup target after lock commit failure"
                    );
                    false
                }
            };
            if candidate_is_free {
                if let Err(restore_error) = tokio::fs::rename(&quarantine, candidate).await {
                    tracing::error!(
                        quarantine = %quarantine.display(),
                        path = %candidate.display(),
                        error = %restore_error,
                        "failed to restore Project cleanup quarantine after lock commit failure"
                    );
                }
            } else {
                tracing::error!(
                    quarantine = %quarantine.display(),
                    path = %candidate.display(),
                    "preserving Project cleanup quarantine because the original path is occupied"
                );
            }
        }
        return Err(ApiError::internal(format!(
            "commit Project cleanup lock: {error}"
        )));
    }
    if let Some(quarantine) = quarantine {
        tokio::fs::remove_dir_all(&quarantine)
            .await
            .map_err(|error| {
                ApiError::internal(format!("remove {kind} cleanup quarantine: {error}"))
            })?;
    }
    Ok(false)
}

/// Atomically move an eligible direct child to a unique quarantine sibling.
/// The caller must hold the DB ownership lock while invoking this function;
/// the rename is deliberately separate from recursive removal so the lock is
/// held only for the short path reservation.
async fn quarantine_confined_direct_child(
    root: &std::path::Path,
    candidate: &std::path::Path,
    kind: &str,
) -> ApiResult<Option<std::path::PathBuf>> {
    let canonical_root = match tokio::fs::canonicalize(root).await {
        Ok(root) => root,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ApiError::internal(format!(
                "resolve Project-owned root after deletion: {error}"
            )));
        }
    };
    let metadata = match tokio::fs::symlink_metadata(candidate).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ApiError::internal(format!(
                "inspect {kind} after Project deletion: {error}"
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Ok(None);
    }
    let canonical = tokio::fs::canonicalize(candidate).await.map_err(|error| {
        ApiError::internal(format!("resolve {kind} after Project deletion: {error}"))
    })?;
    if canonical.parent() != Some(canonical_root.as_path()) {
        return Ok(None);
    }
    let metadata = match tokio::fs::symlink_metadata(candidate).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ApiError::internal(format!(
                "re-inspect {kind} after Project deletion: {error}"
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Ok(None);
    }
    let quarantine = canonical_root.join(format!(".forge-project-delete-{}", new_uuid_v4()));
    tokio::fs::rename(candidate, &quarantine)
        .await
        .map_err(|error| ApiError::internal(format!("quarantine {kind}: {error}")))?;
    Ok(Some(quarantine))
}

/// Remove a directory only when it is an existing, non-symlink direct child
/// of the supplied Forge-controlled root. This runs after the DB commit, so
/// there is no rollback rename and no live worktree is exposed to an
/// execution while a deletion is still provisional.
#[cfg(test)]
async fn remove_confined_direct_child(
    root: &std::path::Path,
    candidate: &std::path::Path,
    kind: &str,
) -> ApiResult<()> {
    let canonical_root = match tokio::fs::canonicalize(root).await {
        Ok(root) => root,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(ApiError::internal(format!(
                "resolve Project-owned root after deletion: {error}"
            )));
        }
    };
    let metadata = match tokio::fs::symlink_metadata(candidate).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(ApiError::internal(format!(
                "inspect {kind} after Project deletion: {error}"
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Ok(());
    }
    let canonical = tokio::fs::canonicalize(candidate).await.map_err(|error| {
        ApiError::internal(format!("resolve {kind} after Project deletion: {error}"))
    })?;
    if canonical.parent() != Some(canonical_root.as_path()) {
        return Ok(());
    }
    // Resolve only for the confinement check, then remove through the
    // original direct-child path.  `remove_dir_all` does not follow a
    // top-level symlink, so a concurrent replacement cannot redirect this
    // cleanup to a different canonical target between these two operations.
    let metadata = match tokio::fs::symlink_metadata(candidate).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(ApiError::internal(format!(
                "re-inspect {kind} after Project deletion: {error}"
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Ok(());
    }
    tokio::fs::remove_dir_all(candidate).await.map_err(|error| {
        ApiError::internal(format!("remove {kind} after Project deletion: {error}"))
    })
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

pub(crate) async fn require_project_visible(
    state: &AppState,
    project_id: &str,
    user_id: &str,
) -> ApiResult<db::Project> {
    ProjectRepo::get_visible_by_id(&*state.db, project_id, user_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))
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

pub(crate) fn validate_analytics_window(from: Option<&str>, to: Option<&str>) -> ApiResult<()> {
    let parsed_from = from
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()
        .map_err(|_| ApiError::bad_request("from must be a valid RFC3339 timestamp"))?;
    let parsed_to = to
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()
        .map_err(|_| ApiError::bad_request("to must be a valid RFC3339 timestamp"))?;
    if let (Some(from), Some(to)) = (parsed_from, parsed_to) {
        if from >= to {
            return Err(ApiError::bad_request("from must be before to"));
        }
    }
    Ok(())
}

fn parse_money_nanos(decimal: &str) -> Option<i128> {
    let (whole, fraction) = decimal.split_once('.').unwrap_or((decimal, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 9
    {
        return None;
    }
    let whole = whole.parse::<i128>().ok()?.checked_mul(1_000_000_000)?;
    let mut fractional = fraction.to_owned();
    while fractional.len() < 9 {
        fractional.push('0');
    }
    let fractional = if fractional.is_empty() {
        0
    } else {
        fractional.parse::<i128>().ok()?
    };
    whole.checked_add(fractional)
}

fn money_from_nanos(nanos: i128) -> Option<api_types::MoneyAmount> {
    if nanos < 0 {
        return None;
    }
    let whole = nanos / 1_000_000_000;
    let fractional = nanos % 1_000_000_000;
    let decimal = if fractional == 0 {
        whole.to_string()
    } else {
        let mut fractional = format!("{fractional:09}");
        while fractional.ends_with('0') {
            fractional.pop();
        }
        format!("{whole}.{fractional}")
    };
    Some(api_types::MoneyAmount {
        currency: "USD".to_owned(),
        decimal,
    })
}

fn divide_money_nanos(numerator: i128, denominator: i64) -> Option<i128> {
    let denominator = i128::from(denominator);
    if denominator <= 0 {
        return None;
    }
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let round_up =
        remainder > denominator / 2 || (denominator % 2 == 0 && remainder == denominator / 2);
    quotient.checked_add(i128::from(round_up))
}

fn released_milestone_outcome(
    project_id: &str,
    from: Option<&str>,
    to: Option<&str>,
    cost: &api_types::CostSummary,
    denominator: i64,
) -> ApiResult<OutcomeCostMetric> {
    let scope = OutcomeCostScope {
        project_id: project_id.to_owned(),
        from: from.map(str::to_owned),
        to: to.map(str::to_owned),
    };
    let base = |eligibility, reason, numerator, amount_per_outcome| OutcomeCostMetric {
        outcome_kind: OutcomeKind::ReleasedMilestone,
        numerator,
        denominator,
        amount_per_outcome,
        scope: scope.clone(),
        eligibility,
        ineligibility_reason: reason,
    };

    if denominator == 0 {
        return Ok(base(
            OutcomeEligibility::NoOutcomes,
            Some(OutcomeIneligibilityReason::NoReleasedMilestones),
            None,
            None,
        ));
    }

    let (eligibility, reason) = match cost.coverage {
        CostCoverage::Complete => (OutcomeEligibility::Eligible, None),
        CostCoverage::NoUsage => (
            OutcomeEligibility::IncompleteCost,
            Some(OutcomeIneligibilityReason::NoUsageCost),
        ),
        CostCoverage::Pending => (
            OutcomeEligibility::PendingCost,
            Some(OutcomeIneligibilityReason::CostPending),
        ),
        CostCoverage::Partial => (
            OutcomeEligibility::IncompleteCost,
            Some(OutcomeIneligibilityReason::CostPartial),
        ),
        CostCoverage::Unavailable => (
            OutcomeEligibility::IncompleteCost,
            Some(OutcomeIneligibilityReason::CostUnavailable),
        ),
    };
    if eligibility != OutcomeEligibility::Eligible {
        return Ok(base(eligibility, reason, None, None));
    }

    let total = cost
        .complete_total
        .as_ref()
        .and_then(|amount| parse_money_nanos(&amount.decimal))
        .ok_or_else(|| ApiError::internal("complete analytics cost is not a valid USD amount"))?;
    let amount_per_outcome = divide_money_nanos(total, denominator)
        .and_then(money_from_nanos)
        .ok_or_else(|| ApiError::internal("analytics outcome cost overflow"))?;
    let numerator =
        money_from_nanos(total).ok_or_else(|| ApiError::internal("analytics cost is negative"))?;
    Ok(base(
        OutcomeEligibility::Eligible,
        None,
        Some(numerator),
        Some(amount_per_outcome),
    ))
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
    use api_types::{
        CostCoverage, CostKind, CostSummary, MoneyAmount, OutcomeEligibility,
        OutcomeIneligibilityReason, TokenCounters, UsageCostCoverage,
    };

    use super::{
        is_legacy_manual_default_assignee, released_milestone_outcome,
        remove_confined_direct_child, validate_analytics_window,
    };

    fn cost_summary(coverage: CostCoverage, complete_total: Option<&str>) -> CostSummary {
        CostSummary {
            kind: if complete_total.is_some() {
                CostKind::ProviderReported
            } else {
                CostKind::Unknown
            },
            coverage,
            provider_reported: complete_total.map(|decimal| MoneyAmount {
                currency: "USD".to_owned(),
                decimal: decimal.to_owned(),
            }),
            estimated: None,
            known_subtotal: None,
            complete_total: complete_total.map(|decimal| MoneyAmount {
                currency: "USD".to_owned(),
                decimal: decimal.to_owned(),
            }),
            usage_coverage: UsageCostCoverage {
                total_runs_or_turns: 0,
                pending_runs_or_turns: 0,
                no_provider_call_runs_or_turns: 0,
                fully_metered_runs_or_turns: 0,
                fully_costed_runs_or_turns: 0,
                partially_costed_runs_or_turns: 0,
                unavailable_cost_runs_or_turns: 0,
                total_provider_attempts: 0,
                settled_provider_attempts: 0,
                pending_provider_attempts: 0,
                unsettled_provider_attempts: 0,
                metered_provider_attempts: 0,
                unmetered_provider_attempts: 0,
                costed_provider_attempts: 0,
                unpriced_provider_attempts: 0,
                priced_tokens: TokenCounters {
                    input_tokens: 0,
                    output_tokens: 0,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                },
                unpriced_tokens: TokenCounters {
                    input_tokens: 0,
                    output_tokens: 0,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                },
                reasons: Vec::new(),
            },
            sources: Vec::new(),
        }
    }

    #[test]
    fn recognizes_legacy_manual_default_assignee() {
        assert!(is_legacy_manual_default_assignee(Some("human")));
        assert!(!is_legacy_manual_default_assignee(Some("user-123")));
        assert!(!is_legacy_manual_default_assignee(None));
    }

    #[test]
    fn analytics_window_rejects_invalid_and_non_increasing_ranges() {
        assert!(validate_analytics_window(None, None).is_ok());
        assert!(validate_analytics_window(Some("not-a-timestamp"), None).is_err());
        assert!(validate_analytics_window(
            Some("2026-09-08T00:00:01Z"),
            Some("2026-09-08T00:00:01Z")
        )
        .is_err());
        assert!(validate_analytics_window(
            Some("2026-09-08T00:00:02Z"),
            Some("2026-09-08T00:00:01Z")
        )
        .is_err());
        assert!(validate_analytics_window(
            Some("2026-09-08T05:30:00+05:30"),
            Some("2026-09-08T00:00:01Z")
        )
        .is_ok());
    }

    #[test]
    fn released_milestone_outcome_uses_exact_coverage_eligibility() {
        let cases = [
            (
                CostCoverage::NoUsage,
                OutcomeEligibility::IncompleteCost,
                OutcomeIneligibilityReason::NoUsageCost,
            ),
            (
                CostCoverage::Pending,
                OutcomeEligibility::PendingCost,
                OutcomeIneligibilityReason::CostPending,
            ),
            (
                CostCoverage::Partial,
                OutcomeEligibility::IncompleteCost,
                OutcomeIneligibilityReason::CostPartial,
            ),
            (
                CostCoverage::Unavailable,
                OutcomeEligibility::IncompleteCost,
                OutcomeIneligibilityReason::CostUnavailable,
            ),
        ];
        for (coverage, eligibility, reason) in cases {
            let outcome =
                released_milestone_outcome("project", None, None, &cost_summary(coverage, None), 2)
                    .expect("ineligible outcome is representable");
            assert_eq!(outcome.eligibility, eligibility);
            assert_eq!(outcome.ineligibility_reason, Some(reason));
            assert_eq!(outcome.denominator, 2);
            assert!(outcome.numerator.is_none());
            assert!(outcome.amount_per_outcome.is_none());
        }

        let no_outcomes = released_milestone_outcome(
            "project",
            None,
            None,
            &cost_summary(CostCoverage::Complete, Some("3")),
            0,
        )
        .expect("no outcomes is representable");
        assert_eq!(no_outcomes.eligibility, OutcomeEligibility::NoOutcomes);
        assert_eq!(
            no_outcomes.ineligibility_reason,
            Some(OutcomeIneligibilityReason::NoReleasedMilestones)
        );

        let eligible = released_milestone_outcome(
            "project",
            None,
            None,
            &cost_summary(CostCoverage::Complete, Some("3")),
            2,
        )
        .expect("complete outcome is representable");
        assert_eq!(eligible.eligibility, OutcomeEligibility::Eligible);
        assert_eq!(eligible.numerator.unwrap().decimal, "3");
        assert_eq!(eligible.amount_per_outcome.unwrap().decimal, "1.5");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn post_commit_cleanup_never_follows_a_symlink_to_a_sibling() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!(
            "forge-project-delete-symlink-test-{}",
            uuid::Uuid::new_v4()
        ));
        let other_project = root.join("other-project-workspace");
        let candidate = root.join("task-owned-by-deleted-project");
        tokio::fs::create_dir_all(&other_project)
            .await
            .expect("sibling workspace fixture");
        tokio::fs::write(other_project.join("tracked"), b"preserved")
            .await
            .expect("sibling workspace content");
        symlink(&other_project, &candidate).expect("symlink fixture");
        let canonical_root = tokio::fs::canonicalize(&root)
            .await
            .expect("canonical workspace root");
        remove_confined_direct_child(&canonical_root, &candidate, "Task workspace")
            .await
            .expect("symlink candidate is safely ignored");

        assert!(candidate
            .symlink_metadata()
            .expect("candidate symlink remains")
            .file_type()
            .is_symlink());
        assert!(other_project.join("tracked").is_file());
        tokio::fs::remove_dir_all(&root)
            .await
            .expect("remove temporary workspace root");
    }
}
