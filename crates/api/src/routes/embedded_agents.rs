use api_types::{
    AgentConnectionHealthResponse, AgentProfileResponse, AgentSessionResponse,
    CanonicalScopeRequest, ConnectEmbeddedProfileRequest, ConnectedEmbeddedAgentResponse,
    ConnectedEmbeddedProfileResponse, CreateAgentSessionRequest, CreateEmbeddedAgentRequest,
    CredentialHandleResponse, EffectivePermissionsResponse, ProtectedInteractionAnswerRequest,
    ProtectedInteractionAnswerValue, ProtectedInteractionCancelRequest,
    ProtectedInteractionSummaryResponse, SessionVersionRequest, SteerAgentSessionRequest,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use db::{
    now_rfc3339, Agent, AgentProfile, AgentProfileRepo, AgentRepo, AgentSession, CredentialHandle,
    ExecutionRepo, SelectAgentProfile,
};
use forge_agent_host::{AgentHostError, InteractionAnswer, InteractionAnswerValue};
use services::{
    agent_service::compute_effective_status,
    embedded_agent_service::{
        ConnectEmbeddedProfile, CreateEmbeddedAgent, CreateScopedSession, RequestedCanonicalScope,
    },
};

use crate::{
    errors::{ApiError, ApiResult},
    routes::{agent_response, auth::AuthenticatedUser, redact_sensitive_config},
    state::AppState,
};

pub async fn create_embedded_agent(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Json(request): Json<CreateEmbeddedAgentRequest>,
) -> ApiResult<Json<ConnectedEmbeddedAgentResponse>> {
    let connected = state
        .embedded_agent_service
        .create_agent_from_entry(CreateEmbeddedAgent {
            owner_user_id: user.user_id.clone(),
            name: request.name,
            description: request.description,
            credential_id: request.credential_id,
            model: request.model,
            system_prompt: request.system_prompt,
            account_permission_ceiling: request
                .account_permission_ceiling
                .unwrap_or_else(default_account_permissions),
            tool_policy: request
                .tool_policy
                .unwrap_or_else(default_profile_tool_policy),
            context_tokens: request.context_tokens,
            max_input_tokens: request.max_input_tokens,
            max_output_tokens: request.max_output_tokens,
        })
        .await?;
    let agent = response_for_agent(&state, connected.agent).await?;
    Ok(Json(ConnectedEmbeddedAgentResponse {
        agent,
        credential_handle: credential_response(connected.credential_handle),
        profile: profile_response(connected.profile),
        health: health_response(connected.health),
        session: session_response(connected.session),
    }))
}

pub async fn connect_embedded_profile(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
    Json(request): Json<ConnectEmbeddedProfileRequest>,
) -> ApiResult<Json<ConnectedEmbeddedProfileResponse>> {
    let (agent, credential, profile, health) = state
        .embedded_agent_service
        .connect_profile(ConnectEmbeddedProfile {
            owner_user_id: user.user_id,
            identity_id,
            expected_identity_version: request.version,
            credential_id: request.credential_id,
            model: request.model,
            system_prompt: request.system_prompt,
            permission_policy: request.permission_policy,
            tool_policy: request
                .tool_policy
                .unwrap_or_else(default_profile_tool_policy),
            context_tokens: request.context_tokens,
            max_input_tokens: request.max_input_tokens,
            max_output_tokens: request.max_output_tokens,
        })
        .await?;
    Ok(Json(ConnectedEmbeddedProfileResponse {
        agent: response_for_agent(&state, agent).await?,
        profile: profile_response(profile),
        credential_handle: credential_response(credential),
        health: health_response(health),
    }))
}

pub async fn list_profiles(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
) -> ApiResult<Json<Vec<AgentProfileResponse>>> {
    require_owned_identity(&state, &identity_id, &user.user_id).await?;
    let profiles = AgentProfileRepo::list_profiles(&*state.db, &identity_id).await?;
    Ok(Json(profiles.into_iter().map(profile_response).collect()))
}

pub async fn select_profile(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((identity_id, profile_id)): Path<(String, String)>,
    Json(request): Json<SessionVersionRequest>,
) -> ApiResult<Json<api_types::AgentResponse>> {
    require_owned_identity(&state, &identity_id, &user.user_id).await?;
    let agent = AgentProfileRepo::select_profile(
        &*state.db,
        SelectAgentProfile {
            identity_id,
            profile_id,
            expected_version: request.version,
            updated_at: now_rfc3339(),
        },
    )
    .await?;
    Ok(Json(response_for_agent(&state, agent).await?))
}

pub async fn create_session(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
    Json(request): Json<CreateAgentSessionRequest>,
) -> ApiResult<Json<AgentSessionResponse>> {
    let session = state
        .embedded_agent_service
        .create_or_resume_session(CreateScopedSession {
            actor_user_id: user.user_id,
            identity_id,
            profile_id: request.profile_id,
            scope: requested_scope(request.scope),
        })
        .await?;
    Ok(Json(session_response(session)))
}

pub async fn list_sessions(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
) -> ApiResult<Json<Vec<AgentSessionResponse>>> {
    let sessions = state
        .embedded_agent_service
        .list_sessions(&user.user_id, &identity_id)
        .await?;
    Ok(Json(sessions.into_iter().map(session_response).collect()))
}

/// List only redaction-safe pending interactions for an authenticated
/// owner's session.  The broker performs the owner/session join; no owner or
/// identity authority is accepted from the request body or query string.
pub async fn list_session_interactions(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(session_id): Path<String>,
) -> ApiResult<Json<Vec<ProtectedInteractionSummaryResponse>>> {
    let summaries = state
        .embedded_agent_service
        .interaction_broker()
        .list_pending_for_owner(&user.user_id, &session_id)
        .await
        .map_err(protected_interaction_error)?;
    Ok(Json(
        summaries
            .into_iter()
            .map(protected_interaction_response)
            .collect(),
    ))
}

pub async fn answer_session_interaction(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((session_id, interaction_id)): Path<(String, String)>,
    Json(request): Json<ProtectedInteractionAnswerRequest>,
) -> ApiResult<Json<ProtectedInteractionSummaryResponse>> {
    let answer = InteractionAnswer::new(
        interaction_id,
        request.expected_version,
        request
            .values
            .into_iter()
            .map(runtime_interaction_answer)
            .collect(),
    );
    let summary = state
        .embedded_agent_service
        .interaction_broker()
        .answer_for_session(&user.user_id, &session_id, answer)
        .await
        .map_err(protected_interaction_error)?;
    Ok(Json(protected_interaction_response(summary)))
}

pub async fn cancel_session_interaction(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((session_id, interaction_id)): Path<(String, String)>,
    Json(request): Json<ProtectedInteractionCancelRequest>,
) -> ApiResult<Json<ProtectedInteractionSummaryResponse>> {
    let summary = state
        .embedded_agent_service
        .interaction_broker()
        .cancel_for_session(
            &user.user_id,
            &session_id,
            &interaction_id,
            request.expected_version,
        )
        .await
        .map_err(protected_interaction_error)?;
    Ok(Json(protected_interaction_response(summary)))
}

pub async fn rotate_session(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(session_id): Path<String>,
    Json(request): Json<SessionVersionRequest>,
) -> ApiResult<Json<AgentSessionResponse>> {
    let session = state
        .embedded_agent_service
        .rotate_session(&user.user_id, &session_id, request.version)
        .await?;
    Ok(Json(session_response(session)))
}

pub async fn suspend_session(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(session_id): Path<String>,
    Json(request): Json<SessionVersionRequest>,
) -> ApiResult<Json<AgentSessionResponse>> {
    set_session_status(state, user, session_id, request.version, "suspended").await
}

pub async fn resume_session(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(session_id): Path<String>,
    Json(request): Json<SessionVersionRequest>,
) -> ApiResult<Json<AgentSessionResponse>> {
    set_session_status(state, user, session_id, request.version, "ready").await
}

pub async fn cancel_session_turn(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(session_id): Path<String>,
) -> ApiResult<StatusCode> {
    state
        .embedded_agent_service
        .cancel_session_turn(&user.user_id, &session_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn steer_session_turn(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(session_id): Path<String>,
    Json(request): Json<SteerAgentSessionRequest>,
) -> ApiResult<StatusCode> {
    state
        .embedded_agent_service
        .steer_session_turn(&user.user_id, &session_id, request.content)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn effective_permissions(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
    Json(scope): Json<CanonicalScopeRequest>,
) -> ApiResult<Json<EffectivePermissionsResponse>> {
    let permissions = state
        .embedded_agent_service
        .effective_permissions(&user.user_id, &identity_id, &requested_scope(scope))
        .await?;
    Ok(Json(EffectivePermissionsResponse {
        allowed: permissions.allowed.into_iter().collect(),
        denied: permissions.denied.into_iter().collect(),
        requires_approval: permissions.requires_approval.into_iter().collect(),
    }))
}

async fn set_session_status(
    state: AppState,
    user: AuthenticatedUser,
    session_id: String,
    version: i64,
    status: &'static str,
) -> ApiResult<Json<AgentSessionResponse>> {
    let session = state
        .embedded_agent_service
        .set_session_status(&user.user_id, &session_id, version, status)
        .await?;
    Ok(Json(session_response(session)))
}

async fn response_for_agent(state: &AppState, agent: Agent) -> ApiResult<api_types::AgentResponse> {
    let stats = ExecutionRepo::stats_by_agent(&*state.db, &agent.id).await?;
    let active_task_count = AgentRepo::count_active_tasks(&*state.db, &agent.id).await?;
    let effective_status = compute_effective_status(&state.db, &agent)
        .await?
        .as_str()
        .to_owned();
    Ok(agent_response(
        agent,
        Some(active_task_count),
        Some(effective_status),
        stats,
    ))
}

async fn require_owned_identity(
    state: &AppState,
    identity_id: &str,
    user_id: &str,
) -> ApiResult<Agent> {
    AgentRepo::get_by_id(&*state.db, identity_id)
        .await?
        .filter(|agent| agent.owner_id.as_deref() == Some(user_id))
        .ok_or_else(|| ApiError::not_found("agent", identity_id.to_owned()))
}

fn requested_scope(scope: CanonicalScopeRequest) -> RequestedCanonicalScope {
    match scope {
        CanonicalScopeRequest::Account => RequestedCanonicalScope::Account,
        CanonicalScopeRequest::Project { project_id } => {
            RequestedCanonicalScope::Project { project_id }
        }
        CanonicalScopeRequest::AgentChat { chat_id } => {
            RequestedCanonicalScope::AgentChat { chat_id }
        }
        CanonicalScopeRequest::Task { task_id, role } => {
            RequestedCanonicalScope::Task { task_id, role }
        }
    }
}

pub(crate) fn credential_response(handle: CredentialHandle) -> CredentialHandleResponse {
    CredentialHandleResponse {
        id: handle.id,
        provider: handle.provider,
        label: handle.label,
        credential_method: handle.credential_method,
        status: handle.status,
        version: handle.version,
        created_at: handle.created_at,
        updated_at: handle.updated_at,
    }
}

fn protected_interaction_response(
    summary: forge_agent_host::ProtectedInteractionSummary,
) -> ProtectedInteractionSummaryResponse {
    ProtectedInteractionSummaryResponse {
        id: summary.id,
        session_id: summary.session_id,
        interaction_kind: summary.interaction_kind,
        prompt_redacted: summary.prompt_redacted,
        status: summary.status,
        expires_at: summary.expires_at,
        version: summary.version,
        created_at: summary.created_at,
        updated_at: summary.updated_at,
    }
}

fn runtime_interaction_answer(value: ProtectedInteractionAnswerValue) -> InteractionAnswerValue {
    match value {
        ProtectedInteractionAnswerValue::Choice {
            question_id,
            choice_id,
        } => InteractionAnswerValue::Choice {
            question_id,
            choice_id,
        },
        ProtectedInteractionAnswerValue::FreeForm { question_id, value } => {
            InteractionAnswerValue::FreeForm { question_id, value }
        }
    }
}

fn protected_interaction_error(error: AgentHostError) -> ApiError {
    match error {
        AgentHostError::SessionNotFound => ApiError::not_found_with_code(
            "protected_interaction.not_found",
            "agent_session",
            "unavailable",
        ),
        AgentHostError::Authority(message) if message.contains("answer is invalid") => {
            ApiError::bad_request_with_code(
                "protected_interaction.invalid",
                "protected interaction answer is invalid",
            )
        }
        AgentHostError::Authority(_) => ApiError::conflict_with_code(
            "protected_interaction.version_conflict",
            "protected interaction is no longer pending or its version changed",
        ),
        AgentHostError::VersionConflict => ApiError::conflict_with_code(
            "protected_interaction.version_conflict",
            "protected interaction is no longer pending or its version changed",
        ),
        AgentHostError::ProtectedPersistence => {
            ApiError::internal("protected interaction persistence failed")
        }
        AgentHostError::Configuration(_) | AgentHostError::Unsupported(_) => {
            ApiError::bad_request_with_code(
                "protected_interaction.unavailable",
                "protected interaction is unavailable",
            )
        }
        AgentHostError::CredentialNotFound
        | AgentHostError::Runtime(_)
        | AgentHostError::TurnLimitReached { .. }
        | AgentHostError::StructuredOutcome(_) => {
            ApiError::internal("protected interaction is unavailable")
        }
    }
}

fn profile_response(profile: AgentProfile) -> AgentProfileResponse {
    AgentProfileResponse {
        id: profile.id,
        identity_id: profile.identity_id,
        backend_kind: profile.backend_kind,
        executor_type: profile.executor_type,
        provider: profile.provider,
        model: profile.model,
        reasoning_effort: profile.reasoning_effort,
        permission_policy: redact_profile_text(profile.permission_policy),
        system_prompt: redact_profile_text(profile.prompt_template),
        capabilities: redact_profile_value(parse_json(&profile.capabilities_json)),
        tool_policy: redact_profile_value(parse_json(&profile.tool_policy_json)),
        config: redact_profile_value(redact_sensitive_config(parse_json(&profile.config_json))),
        credential_handle_id: profile.credential_ref,
        version: profile.version,
        created_at: profile.created_at,
    }
}

fn redact_profile_text(value: Option<String>) -> Option<String> {
    value.map(|value| {
        if contains_protected_runtime_marker(&value) {
            "[redacted]".to_owned()
        } else {
            value
        }
    })
}

fn redact_profile_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(value) if contains_protected_runtime_marker(&value) => {
            serde_json::Value::String("[redacted]".to_owned())
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(redact_profile_value).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| {
                    let redacted = if is_sensitive_profile_key(&key) {
                        serde_json::Value::String("[redacted]".to_owned())
                    } else {
                        redact_profile_value(value)
                    };
                    (key, redacted)
                })
                .collect(),
        ),
        value => value,
    }
}

fn is_sensitive_profile_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace('-', "_");
    [
        "api_key",
        "token",
        "secret",
        "password",
        "authorization",
        "credential",
        "private_key",
    ]
    .iter()
    .any(|candidate| normalized == *candidate || normalized.contains(candidate))
}

fn contains_protected_runtime_marker(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let compact: String = lower
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect();
    let has_bearer_marker = lower
        .split(|character: char| !character.is_ascii_alphabetic())
        .any(|word| word == "bearer");
    let has_github_token_marker = ["ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_"]
        .iter()
        .any(|marker| lower.contains(marker));
    let has_pem_marker = lower.contains("-----begin")
        && (lower.contains("private key") || lower.contains("openssh"));
    has_bearer_marker
        || compact.contains("bearer")
        || compact.contains("apikey")
        || lower.contains("sk-")
        || has_github_token_marker
        || has_pem_marker
}

fn health_response(health: db::AgentConnectionHealth) -> AgentConnectionHealthResponse {
    AgentConnectionHealthResponse {
        profile_id: health.profile_id,
        status: health.status,
        capabilities: parse_json(&health.capability_status_json),
        checked_at: health.checked_at,
        error_code: health.error_code,
        updated_at: health.updated_at,
    }
}

fn session_response(session: AgentSession) -> AgentSessionResponse {
    AgentSessionResponse {
        id: session.id,
        identity_id: session.identity_id,
        profile_id: session.profile_id,
        context_scope_id: session.context_scope_id,
        backend_kind: session.backend_kind,
        status: session.status,
        capabilities: parse_json(&session.capabilities_json),
        connection_status: session.connection_status,
        predecessor_session_id: session.predecessor_session_id,
        replaced_by_session_id: session.replaced_by_session_id,
        last_activity_at: session.last_activity_at,
        version: session.version,
        created_at: session.created_at,
        updated_at: session.updated_at,
    }
}

fn parse_json(value: &str) -> serde_json::Value {
    serde_json::from_str(value).unwrap_or(serde_json::Value::Null)
}

fn default_account_permissions() -> serde_json::Value {
    default_profile_tool_policy()
}

fn default_profile_tool_policy() -> serde_json::Value {
    // A profile is a capability ceiling, not a scope grant. Keep the reusable
    // profile broad enough for later Project/Agent Chat/Task admission; the account
    // ceiling, membership/participation, workflow admission, and canonical
    // scope are still intersected server-side and therefore remain decisive.
    serde_json::json!({
        "allowed": [
            "read_account",
            "read_project",
            "read_agent_chat",
            "read_task",
            "read_memory",
            "propose_task",
            "propose_discovery",
            "propose_project",
            "propose_handoff",
            "propose_message",
            "propose_review",
            "propose_commitment",
            "propose_memory",
            "propose_decision",
            "propose_session",
            "task_read",
            "task_write"
        ]
    })
}
