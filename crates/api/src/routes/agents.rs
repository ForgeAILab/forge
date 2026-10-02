use api_types::{
    AgentAvailabilityResponse, AgentResponse, CreateAgentRequest, DiscoveredDaemonResponse,
    DiscoveredOptionsResponse, DuplicateAgentRequest, PaginatedResponse, UpdateAgentRequest,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use db::{
    new_uuid_v4, now_rfc3339, Agent, AgentListQuery, AgentRepo, AgentTaskListQuery, DaemonRepo,
    ExecutionRepo, TaskRepo, UpdateAgent,
};
use events::{event_timestamp, EventContext, ForgeEvent};
use executors::{DiscoverContext, ExecutorKind};
use services::agent_service::{compute_effective_status, resolve_daemon_for_agent};

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::AuthenticatedUser,
    routes::{
        agent_response, page_request, parse_csv, serialize_json, task_page_request,
        task_response_light, ListParams,
    },
    state::AppState,
};

pub async fn register_agent(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Json(request): Json<CreateAgentRequest>,
) -> ApiResult<Json<AgentResponse>> {
    if !user.is_admin && request.daemon_id.is_some() {
        return Err(ApiError::forbidden_with_code(
            "admin_required",
            "Admin access required to pin an agent to a daemon",
        ));
    }

    // A harness agent may reference one of the caller's provider entries; the
    // capability matrix decides whether that entry can drive this executor.
    let credential_ref = if let Some(credential_id) = request.credential_id.as_deref() {
        let entry = state
            .embedded_agent_service
            .require_owned_entry(&user.user_id, credential_id)
            .await?;
        services::provider_authorization::runtime_supported(
            &entry.provider,
            &entry.credential_method,
            &request.executor_type,
        )
        .map_err(ApiError::bad_request)?;
        Some(entry.id)
    } else {
        None
    };

    validate_agent_config_json(request.config_json.as_ref())?;
    let agent = state
        .agent_service
        .register(
            request.name,
            request.description,
            request.executor_type,
            request.model,
            request.reasoning_effort,
            request.permission_policy,
            request.prompt_template,
            serialize_json(request.capabilities)?.unwrap_or_else(|| "[]".to_owned()),
            serialize_json(request.config_json)?.unwrap_or_else(|| "{}".to_owned()),
            credential_ref,
            request.daemon_id,
            request.max_concurrent_tasks,
            request.heartbeat_interval_seconds,
            request.max_missed_heartbeats,
            request.is_default.unwrap_or(false),
            Some(user.user_id.clone()),
            Some("account".to_owned()),
        )
        .await?;
    let response = build_agent_response_for_user(&state, agent, Some(0), &user).await?;
    Ok(Json(response))
}

pub async fn list_agents(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<PaginatedResponse<AgentResponse>>> {
    let statuses = parse_csv::<db::AgentStatus>(params.status.as_ref(), "status")?;
    let status = statuses.into_iter().next();
    let capabilities = params
        .capabilities
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|item| !item.trim().is_empty())
        .map(|item| item.trim().to_owned())
        .collect();
    let page = AgentRepo::list_visible(
        &*state.db,
        &user.user_id,
        AgentListQuery {
            status,
            executor_type: params.executor_type.clone(),
            capabilities,
            page: page_request(&params)?,
        },
    )
    .await?;
    let has_more = page.next_cursor.is_some();
    let items = build_agent_responses_for_user(&state, page.items, &user).await?;
    Ok(Json(PaginatedResponse {
        items,
        next_cursor: page.next_cursor,
        has_more,
        total_count: page.total_count.and_then(|count| u64::try_from(count).ok()),
    }))
}

pub async fn get_agent(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<AgentResponse>> {
    let agent = AgentRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent", id.clone()))?;
    require_agent_visible(&agent, &user, &id)?;
    let active_assigned_task_count =
        AgentRepo::count_active_assigned_tasks(&*state.db, &agent.id).await?;
    Ok(Json(
        build_agent_response_for_user(&state, agent, Some(active_assigned_task_count), &user)
            .await?,
    ))
}

pub async fn list_agent_tasks(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<PaginatedResponse<api_types::TaskResponse>>> {
    let agent = AgentRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent", id.clone()))?;
    require_agent_visible(&agent, &user, &id)?;
    let page = TaskRepo::list_by_executing_agent(
        &*state.db,
        AgentTaskListQuery {
            agent_id: id,
            include_archived: params.include_archived.unwrap_or(false),
            include_cancelled: params.include_cancelled.unwrap_or(false),
            include_deleted: false,
            page: task_page_request(&params)?,
        },
    )
    .await?;
    let has_more = page.next_cursor.is_some();
    let mut items = Vec::with_capacity(page.items.len());
    for task in page.items {
        items.push(task_response_light(&state.db, &state.workspace_backend_router, task).await?);
    }
    Ok(Json(PaginatedResponse {
        items,
        next_cursor: page.next_cursor,
        has_more,
        total_count: page.total_count.and_then(|count| u64::try_from(count).ok()),
    }))
}

/// Rejects an agent-level run time limit every claim would then refuse:
/// `hard_deadline_seconds` is a positive whole number of seconds, or absent /
/// null for no limit.
fn validate_agent_config_json(config: Option<&serde_json::Value>) -> Result<(), ApiError> {
    let Some(value) = config.and_then(|config| config.get("hard_deadline_seconds")) else {
        return Ok(());
    };
    if value.is_null()
        || value
            .as_u64()
            .is_some_and(|seconds| seconds > 0 && u32::try_from(seconds).is_ok())
    {
        return Ok(());
    }
    Err(ApiError::bad_request(
        "config_json.hard_deadline_seconds must be a positive whole number of seconds, or omitted for no limit",
    ))
}

pub async fn update_agent(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<UpdateAgentRequest>,
) -> ApiResult<Json<AgentResponse>> {
    let existing = AgentRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent", id.clone()))?;
    require_agent_manageable(&existing, &user, &id)?;
    validate_agent_config_json(request.config_json.as_ref())?;
    if !user.is_admin && request.daemon_id.is_some() {
        return Err(ApiError::forbidden_with_code(
            "admin_required",
            "Admin access required to pin an agent to a daemon",
        ));
    }

    let agent = AgentRepo::update(
        &*state.db,
        UpdateAgent {
            id,
            expected_version: request.version,
            name: request.name,
            description: request.description,
            model: request.model,
            reasoning_effort: request.reasoning_effort,
            permission_policy: request.permission_policy,
            capabilities_json: request
                .capabilities
                .map(|capabilities| serialize_json(Some(capabilities)))
                .transpose()?
                .flatten(),
            config_json: serialize_json(request.config_json)?,
            daemon_id: request.daemon_id,
            max_concurrent_tasks: request.max_concurrent_tasks,
            heartbeat_interval_seconds: None,
            max_missed_heartbeats: None,
            status: None,
            last_heartbeat_at: None,
            is_default: request.is_default,
            paused: request.paused,
            prompt_template: request.prompt_template,
            updated_at: now_rfc3339(),
        },
    )
    .await?;
    let active_assigned_task_count =
        AgentRepo::count_active_assigned_tasks(&*state.db, &agent.id).await?;
    Ok(Json(
        build_agent_response_for_user(&state, agent, Some(active_assigned_task_count), &user)
            .await?,
    ))
}

pub async fn archive_agent(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    let agent = AgentRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent", id.clone()))?;
    require_agent_manageable(&agent, &user, &id)?;
    state.agent_service.archive(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn pause_agent(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<AgentResponse>> {
    let agent = AgentRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent", id.clone()))?;
    require_agent_manageable(&agent, &user, &id)?;
    if agent.paused {
        let response = build_agent_response_for_user(
            &state,
            agent,
            Some(AgentRepo::count_active_assigned_tasks(&*state.db, &id).await?),
            &user,
        )
        .await?;
        return Ok(Json(response));
    }

    AgentRepo::set_paused(&*state.db, &id, true).await?;
    let agent = AgentRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent", id.clone()))?;
    tracing::info!(agent_id = %agent.id, agent_name = %agent.name, "agent paused");
    state.event_bus.publish(ForgeEvent {
        event_type: "agent.paused".to_owned(),
        entity_id: agent.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::AgentPaused {},
    });
    let active_assigned_task_count =
        AgentRepo::count_active_assigned_tasks(&*state.db, &agent.id).await?;
    let response =
        build_agent_response_for_user(&state, agent, Some(active_assigned_task_count), &user)
            .await?;
    Ok(Json(response))
}

pub async fn resume_agent(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<AgentResponse>> {
    let agent = AgentRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent", id.clone()))?;
    require_agent_manageable(&agent, &user, &id)?;
    if !agent.paused {
        let response = build_agent_response_for_user(
            &state,
            agent,
            Some(AgentRepo::count_active_assigned_tasks(&*state.db, &id).await?),
            &user,
        )
        .await?;
        return Ok(Json(response));
    }

    AgentRepo::set_paused(&*state.db, &id, false).await?;
    let agent = AgentRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent", id.clone()))?;
    tracing::info!(agent_id = %agent.id, agent_name = %agent.name, "agent resumed");
    state.event_bus.publish(ForgeEvent {
        event_type: "agent.resumed".to_owned(),
        entity_id: agent.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::AgentResumed {},
    });
    let active_assigned_task_count =
        AgentRepo::count_active_assigned_tasks(&*state.db, &agent.id).await?;
    let response =
        build_agent_response_for_user(&state, agent, Some(active_assigned_task_count), &user)
            .await?;
    Ok(Json(response))
}

pub async fn duplicate_agent(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<DuplicateAgentRequest>,
) -> ApiResult<Json<AgentResponse>> {
    let existing = AgentRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent", id.clone()))?;
    require_agent_manageable(&existing, &user, &id)?;
    let agent =
        AgentRepo::duplicate_agent(&*state.db, &id, new_uuid_v4(), request.name, now_rfc3339())
            .await?;
    let active_assigned_task_count =
        AgentRepo::count_active_assigned_tasks(&*state.db, &agent.id).await?;
    Ok(Json(
        build_agent_response_for_user(&state, agent, Some(active_assigned_task_count), &user)
            .await?,
    ))
}

pub async fn agent_availability(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<AgentAvailabilityResponse>> {
    let agent = AgentRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent", id.clone()))?;
    require_agent_visible(&agent, &user, &id)?;
    let active_assigned_task_count =
        AgentRepo::count_active_assigned_tasks(&*state.db, &agent.id).await?;
    let effective_status =
        compute_effective_status(&state.db, &agent, Some(&state.daemon_connections)).await?;
    let resolved_daemon = resolve_daemon_for_agent(&state.db, &agent).await.ok();
    let available = effective_status.as_str() == "active" || effective_status.as_str() == "busy";
    let reason = if available {
        None
    } else {
        Some(match effective_status.as_str() {
            "daemon_unavailable" => format!(
                "No daemon with authenticated {} executor found",
                agent.executor_type
            ),
            api_types::DAEMON_UPGRADE_REQUIRED => {
                api_types::DAEMON_UPGRADE_REQUIRED_MESSAGE.to_owned()
            }
            "daemon_offline" => "Pinned daemon is offline".to_owned(),
            "deactivated" => "Pinned daemon does not have this executor authenticated".to_owned(),
            "connection_degraded" => "Embedded provider connection is degraded".to_owned(),
            "connection_unavailable" => "Embedded provider connection is unavailable".to_owned(),
            status => format!("Agent is not available: {status}"),
        })
    };
    Ok(Json(AgentAvailabilityResponse {
        available,
        effective_status: effective_status.as_str().to_owned(),
        resolved_daemon_id: if user.is_admin {
            resolved_daemon.map(|daemon| daemon.id)
        } else {
            None
        },
        active_assigned_task_count,
        running_execution_count: AgentRepo::count_running_executions(&*state.db, &agent.id).await?,
        max_concurrent_tasks: agent.max_concurrent_tasks,
        reason,
    }))
}

pub async fn agent_discovered_options(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Query(params): Query<DiscoveryParams>,
) -> ApiResult<Json<DiscoveredOptionsResponse>> {
    let agent = AgentRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent", id.clone()))?;
    require_agent_visible(&agent, &user, &id)?;
    let kind = parse_executor_kind(&agent.executor_type)?;
    let adapter = state.adapter_registry.get(&kind).ok_or_else(|| {
        ApiError::bad_request(format!(
            "No adapter registered for executor type: {}",
            agent.executor_type
        ))
    })?;
    let daemons = if user.is_admin {
        DaemonRepo::list_available_for_executor(&*state.db, &agent.executor_type).await?
    } else {
        Vec::new()
    };
    let discovered = if daemons.is_empty() {
        Default::default()
    } else {
        adapter
            .discover_options(DiscoverContext {
                project_path: params.project_id,
            })
            .await
            .map_err(|error| ApiError::bad_request(error.to_string()))?
    };
    Ok(Json(DiscoveredOptionsResponse {
        models: discovered.models,
        permission_policies: discovered.permission_policies,
        cli_specific: discovered.cli_specific,
        available_daemons: daemons
            .into_iter()
            .map(|daemon| DiscoveredDaemonResponse {
                id: daemon.id,
                name: daemon.hostname,
                status: daemon.status.to_string(),
            })
            .collect(),
        warning: None,
    }))
}

async fn build_agent_response(
    state: &AppState,
    agent: Agent,
    active_assigned_task_count: Option<i64>,
    usage: api_types::UsageAggregate,
) -> ApiResult<AgentResponse> {
    let effective_status =
        compute_effective_status(&state.db, &agent, Some(&state.daemon_connections))
            .await?
            .as_str()
            .to_owned();
    let stats = ExecutionRepo::stats_by_agent(&*state.db, &agent.id).await?;
    // Derived here rather than threaded through every caller: this is the
    // quantity `max_concurrent_tasks` actually bounds, so it should always be
    // present wherever the cap is.
    let running_execution_count =
        AgentRepo::count_running_executions(&*state.db, &agent.id).await?;
    Ok(agent_response(
        agent,
        active_assigned_task_count,
        Some(running_execution_count),
        Some(effective_status),
        stats,
        usage,
    ))
}

async fn build_agent_response_for_user(
    state: &AppState,
    agent: Agent,
    active_assigned_task_count: Option<i64>,
    user: &AuthenticatedUser,
) -> ApiResult<AgentResponse> {
    let runnable_on = services::environment_surfaces::runnable_on(
        &state.db,
        &agent,
        &state.adapter_registry,
        &state.daemon_connections,
        user.is_admin,
    )
    .await?;
    let usage = state.usage_ledger_index.agent(&agent.id).await?;
    let response = build_agent_response(state, agent, active_assigned_task_count, usage).await?;
    Ok(agent_response_for_user(response, runnable_on, user))
}

fn agent_response_for_user(
    mut response: AgentResponse,
    runnable_on: api_types::AgentRunnableOn,
    user: &AuthenticatedUser,
) -> AgentResponse {
    response.runnable_on = runnable_on;
    if !user.is_admin {
        response.daemon_id = None;
    }
    response
}

/// Share incremental usage and execution statistics across either Agent page.
pub(super) async fn build_agent_responses_for_user(
    state: &AppState,
    agents: Vec<Agent>,
    user: &AuthenticatedUser,
) -> ApiResult<Vec<AgentResponse>> {
    let ids = agents
        .iter()
        .map(|agent| agent.id.clone())
        .collect::<Vec<_>>();
    let mut runnable = services::environment_surfaces::runnable_on_for_agents(
        &state.db,
        &agents,
        &state.adapter_registry,
        &state.daemon_connections,
        user.is_admin,
    )
    .await?;
    let mut usage = state.usage_ledger_index.agents(&ids).await?;
    let mut stats = state.usage_ledger_index.agent_execution_stats(&ids).await?;
    let mut assignments = state.db.agent_list_active_assignments(&ids).await?;
    let running = stats
        .iter()
        .map(|(id, value)| (id.clone(), value.1))
        .collect();
    let mut effective = services::agent_service::compute_effective_status_for_agents(
        &state.db,
        &agents,
        &running,
        &state.daemon_connections,
    )
    .await?;
    let mut responses = Vec::with_capacity(agents.len());
    for agent in agents {
        let aggregate = match usage.remove(&agent.id) {
            Some(value) => value,
            None => {
                services::usage_projection::usage_aggregate_for_agent(&state.db, &agent.id).await?
            }
        };
        let live = match stats.remove(&agent.id) {
            Some(value) => value,
            None => (
                db::ExecutionRepo::stats_by_agent(&*state.db, &agent.id).await?,
                db::AgentRepo::count_running_executions(&*state.db, &agent.id).await?,
            ),
        };
        let effective_status = effective
            .remove(&agent.id)
            .expect("requested Agent")
            .as_str()
            .to_owned();
        let assigned = assignments.remove(&agent.id).unwrap_or(0);
        let response = agent_response(
            agent,
            Some(assigned),
            Some(live.1),
            Some(effective_status),
            live.0,
            aggregate,
        );
        let runnable_on = runnable.remove(&response.id).expect("requested Agent");
        responses.push(agent_response_for_user(response, runnable_on, user));
    }
    Ok(responses)
}

#[derive(Debug, serde::Deserialize)]
pub struct DiscoveryParams {
    pub project_id: Option<String>,
}

fn parse_executor_kind(value: &str) -> ApiResult<ExecutorKind> {
    value
        .parse()
        .map_err(|_| ApiError::bad_request(format!("invalid executor_type: {value}")))
}

fn can_view_agent(agent: &Agent, user: &AuthenticatedUser) -> bool {
    agent.visibility == "global" || agent.owner_id.as_deref() == Some(&user.user_id)
}

fn can_manage_agent(agent: &Agent, user: &AuthenticatedUser) -> bool {
    agent.owner_id.as_deref() == Some(&user.user_id)
        || (user.is_admin && agent.owner_id.is_none() && agent.visibility == "global")
}

fn require_agent_visible(agent: &Agent, user: &AuthenticatedUser, id: &str) -> ApiResult<()> {
    if can_view_agent(agent, user) {
        Ok(())
    } else {
        Err(ApiError::not_found("agent", id.to_owned()))
    }
}

fn require_agent_manageable(agent: &Agent, user: &AuthenticatedUser, id: &str) -> ApiResult<()> {
    if can_manage_agent(agent, user) {
        Ok(())
    } else {
        Err(ApiError::not_found("agent", id.to_owned()))
    }
}
