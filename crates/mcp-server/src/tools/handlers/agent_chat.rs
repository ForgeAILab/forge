//! Agent profiles, sessions, bindings, chats, and handoff handlers.

use super::*;

pub(crate) async fn forge_list_agent_profiles(
    state: &AppState,
    params: ListAgentProfilesParams,
    _context: &McpContext,
) -> Result<Value, McpToolError> {
    let profiles = AgentProfileRepo::list_profiles(&*state.db, &params.identity_id).await?;
    Ok(json!({
        "items": profiles.into_iter().map(agent_profile_value).collect::<Vec<_>>(),
    }))
}

pub(crate) async fn forge_list_agent_sessions(
    state: &AppState,
    params: ListAgentSessionsParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let sessions = AgentSessionRepo::list_agent_sessions(&*state.db, &params.identity_id).await?;
    let user_id = authenticated_user(context)?;
    let mut visible = Vec::new();
    for session in sessions {
        if super::registry::require_session_scope(state, &session, user_id)
            .await
            .is_ok()
        {
            visible.push(agent_session_value(session));
        }
    }
    Ok(json!({
        "items": visible,
    }))
}

pub(crate) async fn forge_get_agent_session(
    state: &AppState,
    params: GetAgentSessionParams,
    _context: &McpContext,
) -> Result<Value, McpToolError> {
    let session = AgentSessionRepo::get_agent_session(&*state.db, &params.session_id)
        .await?
        .ok_or_else(|| McpToolError::not_found("agent_session", params.session_id.clone()))?;
    Ok(agent_session_value(session))
}

pub(crate) async fn forge_get_main_agent(
    state: &AppState,
    _params: GetMainAgentParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user_id = authenticated_user(context)?;
    let binding = AccountMainAgentBindingRepo::get_active_main_binding(&*state.db, user_id)
        .await?
        .ok_or_else(|| McpToolError::not_found("main_agent_binding", user_id.to_owned()))?;
    let chat_id = AgentChatRepo::get_main_chat(&*state.db, user_id)
        .await?
        .map(|chat| chat.id)
        .unwrap_or_default();
    Ok(serialize_public(main_binding_response(binding, chat_id)))
}

pub(crate) async fn forge_set_main_agent(
    state: &AppState,
    params: BindMainAgentParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user_id = authenticated_user(context)?;
    // Effect-time ownership re-check on the decoded identity. A missing and
    // a foreign identity are the same answer.
    let identity = AgentRepo::get_by_id(&*state.db, &params.identity_id)
        .await?
        .filter(|identity| identity.owner_id.as_deref() == Some(user_id))
        .ok_or_else(|| McpToolError::not_found("agent_identity", params.identity_id.clone()))?;
    let now = now_rfc3339();
    let replacement = CreateAccountMainAgentBinding {
        id: new_uuid_v4(),
        account_id: user_id.to_owned(),
        identity_id: params.identity_id,
        // Bind-time snapshot; turns re-resolve the agent's current profile.
        profile_id: identity.profile_id,
        autonomy_policy_json: policy_json(params.autonomy_policy),
        tool_policy_revision: "default".to_owned(),
        created_at: now.clone(),
        updated_at: now,
    };
    let binding =
        match AccountMainAgentBindingRepo::get_active_main_binding(&*state.db, user_id).await? {
            Some(_) => {
                AccountMainAgentBindingRepo::replace_main_binding(
                    &*state.db,
                    ReplaceAccountMainAgentBinding {
                        account_id: user_id.to_owned(),
                        expected_version: params.expected_version,
                        replacement,
                        replacement_reason: Some("mcp_replace".to_owned()),
                    },
                )
                .await?
            }
            None if params.expected_version == 0 => {
                AccountMainAgentBindingRepo::create_main_binding(&*state.db, replacement).await?
            }
            None => {
                return Err(McpToolError::new(
                    -32009,
                    "main agent binding does not exist; expected_version must be 0",
                ));
            }
        };
    let chat_id = AgentChatRepo::get_main_chat(&*state.db, user_id)
        .await?
        .map(|chat| chat.id)
        .unwrap_or_default();
    Ok(serialize_public(main_binding_response(binding, chat_id)))
}

pub(crate) async fn forge_get_project_agent(
    state: &AppState,
    params: GetProjectAgentParams,
    _context: &McpContext,
) -> Result<Value, McpToolError> {
    let binding =
        ProjectAgentBindingRepo::get_active_project_binding(&*state.db, &params.project_id)
            .await?
            .ok_or_else(|| setup_required("Project Agent setup is required"))?;
    let chat_id = AgentChatRepo::get_project_chat(&*state.db, &params.project_id)
        .await?
        .map(|chat| chat.id)
        .unwrap_or_default();
    Ok(serialize_public(project_binding_response(
        binding, chat_id,
    )?))
}

pub(crate) async fn forge_set_project_agent(
    state: &AppState,
    params: BindProjectAgentParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user_id = authenticated_user(context)?;
    if params.wake_budget < 0 {
        return Err(invalid_field_error(
            "wake_budget",
            "must be non-negative",
            Some(json!({ "type": "integer", "minimum": 0 })),
        ));
    }
    let binding = state
        .agent_chat_service
        .set_project_binding(SetProjectAgentBindingInput {
            actor_user_id: user_id.to_owned(),
            project_id: params.project_id.clone(),
            identity_id: Some(params.identity_id),
            state: "active".to_owned(),
            autonomy_policy_json: policy_json(params.autonomy_policy),
            permission_ceiling_json: policy_json(params.permission_ceiling),
            subscriptions_json: serde_json::to_string(&params.subscriptions)
                .map_err(|error| McpToolError::new(-32603, error.to_string()))?,
            wake_budget: params.wake_budget,
            expected_version: (params.expected_version > 0).then_some(params.expected_version),
            replacement_reason: Some("mcp_replace".to_owned()),
        })
        .await?;
    let chat_id = AgentChatRepo::get_project_chat(&*state.db, &params.project_id)
        .await?
        .map(|chat| chat.id)
        .unwrap_or_default();
    Ok(serialize_public(project_binding_response(
        binding, chat_id,
    )?))
}

pub(crate) async fn forge_list_agent_chats(
    state: &AppState,
    params: ListAgentChatsParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user_id = authenticated_user(context)?;
    let _ = (params.cursor, params.limit);
    let chats = AgentChatRepo::list_agent_chats(&*state.db, user_id)
        .await?
        .into_iter()
        .filter(|chat| {
            context.project_id.as_deref().is_none_or(|project_id| {
                chat.kind == "project" && chat.project_id.as_deref() == Some(project_id)
            })
        })
        .collect::<Vec<_>>();
    Ok(serialize_public(AgentChatListResponse {
        items: chats.into_iter().map(chat_response).collect(),
        next_cursor: None,
        has_more: false,
    }))
}

pub(crate) async fn forge_get_agent_chat(
    state: &AppState,
    params: GetAgentChatParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user_id = authenticated_user(context)?;
    let chat = state
        .agent_chat_service
        .get_authorized_chat(user_id, &params.chat_id)
        .await?;
    let main_binding = if chat.kind == "account_main" {
        AccountMainAgentBindingRepo::get_active_main_binding(&*state.db, user_id)
            .await?
            .map(|binding| main_binding_response(binding, chat.id.clone()))
    } else {
        None
    };
    let project_binding = if chat.kind == "project" {
        let project_id = chat.project_id.as_deref().unwrap_or_default();
        ProjectAgentBindingRepo::get_active_project_binding(&*state.db, project_id)
            .await?
            .map(|binding| project_binding_response(binding, chat.id.clone()))
            .transpose()?
    } else {
        None
    };
    Ok(serialize_public(AgentChatDetailResponse {
        chat: chat_response(chat),
        main_binding,
        project_binding,
    }))
}

pub(crate) async fn forge_list_agent_chat_messages(
    state: &AppState,
    params: ListAgentChatMessagesParams,
    _context: &McpContext,
) -> Result<Value, McpToolError> {
    let page = AgentChatMessageRepo::list_agent_chat_messages(
        &*state.db,
        AgentChatMessageListQuery {
            chat_id: params.chat_id,
            before_sequence: params.before_sequence,
            page: PageRequest {
                cursor: params.cursor,
                limit: params.limit.unwrap_or(50).clamp(1, 100),
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Asc,
            },
        },
    )
    .await?;
    let mut items = Vec::with_capacity(page.items.len());
    for message in page.items {
        items.push(message_response(state, message).await?);
    }
    Ok(serialize_public(AgentChatMessageListResponse {
        items,
        next_cursor: page.next_cursor.clone(),
        has_more: page.next_cursor.is_some(),
    }))
}

pub(crate) async fn forge_send_agent_chat_message(
    state: &AppState,
    params: SendAgentChatMessageParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user_id = authenticated_user(context)?;
    if params
        .dedupe_key
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(invalid_field_error(
            "dedupe_key",
            "must be non-empty when provided",
            Some(json!({ "type": "string", "non_empty": true })),
        ));
    }
    let admitted = state
        .agent_chat_service
        .send_message(services_agents::SendAgentChatMessageInput {
            actor_user_id: user_id.to_owned(),
            chat_id: params.chat_id,
            content: params.content,
            dedupe_key: params.dedupe_key,
        })
        .await?;
    Ok(serialize_public(api_types::SendAgentChatMessageResponse {
        message: message_response(state, admitted.message).await?,
        turn_job: Some(turn_response(admitted.turn_job)),
    }))
}

pub(crate) async fn forge_list_agent_handoffs(
    state: &AppState,
    params: ListAgentHandoffsParams,
    _context: &McpContext,
) -> Result<Value, McpToolError> {
    let project_id = params.project_id;
    let _ = (params.cursor, params.limit);
    let chat = AgentChatRepo::get_project_chat(&*state.db, &project_id)
        .await?
        .ok_or_else(|| McpToolError::not_found("agent_chat", project_id.clone()))?;
    let handoffs = AgentHandoffRepo::list_agent_handoffs(&*state.db, &chat.id).await?;
    Ok(Value::Array(
        handoffs
            .into_iter()
            .map(|handoff| serialize_public(handoff_response(handoff, project_id.clone())))
            .collect(),
    ))
}

pub(crate) async fn forge_get_agent_handoff(
    state: &AppState,
    params: GetAgentHandoffParams,
    _context: &McpContext,
) -> Result<Value, McpToolError> {
    let handoff = AgentHandoffRepo::get_agent_handoff(&*state.db, &params.handoff_id)
        .await?
        .ok_or_else(|| McpToolError::not_found("agent_handoff", params.handoff_id.clone()))?;
    Ok(serialize_public(handoff_response(
        handoff,
        params.project_id,
    )))
}

pub(crate) async fn forge_create_agent_handoff(
    state: &AppState,
    params: CreateAgentHandoffParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user_id = authenticated_user(context)?;
    let project_id = params.project_id.clone();
    if params.dedupe_key.trim().is_empty() {
        return Err(invalid_field_error(
            "dedupe_key",
            "must be a non-empty string",
            Some(json!({ "type": "string", "non_empty": true })),
        ));
    }
    let source = state.agent_chat_service.ensure_main_chat(user_id).await?;
    let outcome = state
        .agent_chat_service
        .create_handoff(services_agents::CreateAgentHandoffInput {
            actor_user_id: user_id.to_owned(),
            source_chat_id: source.id,
            source_message_id: params.source_message_id,
            source_turn_job_id: params.source_turn_job_id,
            target_project_id: project_id.clone(),
            content: params.content,
            source_revisions_json: "[]".to_owned(),
            dedupe_key: params.dedupe_key,
        })
        .await?;
    Ok(serialize_public(handoff_response(
        outcome.handoff,
        project_id,
    )))
}

fn main_binding_response(
    binding: AccountMainAgentBinding,
    chat_id: String,
) -> MainAgentBindingResponse {
    MainAgentBindingResponse {
        id: binding.id,
        account_id: binding.account_id,
        identity_id: binding.identity_id,
        profile_id: binding.profile_id,
        chat_id,
        state: binding_state(&binding.state),
        autonomy_policy: parse_json(&binding.autonomy_policy_json),
        tool_policy_revision: Some(binding.tool_policy_revision),
        version: binding.version,
        created_at: binding.created_at,
        updated_at: binding.updated_at,
    }
}

fn project_binding_response(
    binding: ProjectAgentBinding,
    chat_id: String,
) -> Result<ProjectAgentBindingResponse, McpToolError> {
    Ok(ProjectAgentBindingResponse {
        id: binding.id,
        project_id: binding.project_id,
        identity_id: binding.identity_id,
        profile_id: binding.profile_id,
        chat_id,
        state: binding_state(&binding.state),
        permission_ceiling: parse_json(&binding.permission_ceiling_json),
        autonomy_policy: parse_json(&binding.autonomy_policy_json),
        subscriptions: serde_json::from_str(&binding.subscriptions_json).unwrap_or_default(),
        wake_budget: binding.wake_budget,
        version: binding.version,
        created_at: binding.created_at,
        updated_at: binding.updated_at,
    })
}

fn chat_response(chat: AgentChat) -> ApiAgentChatResponse {
    ApiAgentChatResponse {
        id: chat.id,
        kind: if chat.kind == "project" {
            AgentChatKind::Project
        } else {
            AgentChatKind::Main
        },
        account_id: chat.account_id.unwrap_or_default(),
        project_id: chat.project_id,
        title: if chat.kind == "project" {
            "Project Agent".to_owned()
        } else {
            "Main Agent".to_owned()
        },
        status: chat_status(&chat.status),
        message_count: chat.message_count,
        pending_turn_count: 0,
        last_message_at: chat.last_message_at,
        version: chat.version,
        created_at: chat.created_at,
        updated_at: chat.updated_at,
    }
}

async fn message_response(
    state: &AppState,
    message: AgentChatMessage,
) -> Result<AgentChatMessageResponse, McpToolError> {
    // Chat usage is keyed by the immutable response message ID in both the
    // legacy migration and the runtime ledger.  Resolve it from the ledger
    // only after the caller has passed the chat authorization guard above;
    // never expose the historical token JSON as a fallback.
    let usage_source_id = message.source_id.as_deref().unwrap_or(&message.id);
    let mut usage =
        services::usage_projection::usage_breakdowns_for_source(&state.db, usage_source_id).await?;
    // V135's legacy chat import keyed rows by the immutable response message
    // ID, while live chat admissions key the turn's usage by its source/job
    // ID. Support both immutable forms without consulting token JSON.
    if usage.is_empty() && usage_source_id != message.id {
        usage =
            services::usage_projection::usage_breakdowns_for_source(&state.db, &message.id).await?;
    }
    Ok(AgentChatMessageResponse {
        id: message.id,
        chat_id: message.chat_id,
        author_type: match message.author_type {
            DbMessageAuthorType::User => AgentChatMessageAuthorType::User,
            DbMessageAuthorType::Agent => AgentChatMessageAuthorType::Agent,
            DbMessageAuthorType::Handoff => AgentChatMessageAuthorType::Handoff,
            DbMessageAuthorType::System => AgentChatMessageAuthorType::System,
        },
        author_id: message.author_id,
        content: message.content,
        content_guard: parse_json(&message.content_guard_json),
        sensitivity: message.sensitivity,
        status: match message.status {
            DbMessageStatus::Complete => AgentChatMessageStatus::Complete,
            DbMessageStatus::Failed => AgentChatMessageStatus::Failed,
            DbMessageStatus::Cancelled => AgentChatMessageStatus::Cancelled,
        },
        outcome: message.outcome,
        model: message.model,
        profile_id: message.profile_id,
        session_id: message.session_id,
        context_manifest_id: message.context_manifest_id,
        usage,
        duration_ms: message.duration_ms,
        error: message.error,
        correlation_id: message.correlation_id,
        causation_id: message.causation_id,
        handoff_id: message.handoff_id,
        source_chat_id: None,
        source_message_id: message.source_message_id,
        sequence: message.sequence,
        created_at: message.created_at,
    })
}

fn turn_response(job: AgentChatTurnJob) -> AgentChatTurnJobResponse {
    let error = job.error_message.clone().or_else(|| job.error_code.clone());
    let retry_action = job.retry_action();
    AgentChatTurnJobResponse {
        id: job.id,
        chat_id: job.chat_id,
        input_message_id: job.triggering_message_id,
        responder_identity_id: job.responder_identity_id,
        responder_profile_id: job.profile_id,
        status: match job.status {
            AgentChatTurnState::Queued => AgentChatTurnStatus::Queued,
            AgentChatTurnState::Leased => AgentChatTurnStatus::Leased,
            AgentChatTurnState::AwaitingInput => AgentChatTurnStatus::AwaitingInput,
            AgentChatTurnState::RetryWait => AgentChatTurnStatus::RetryWait,
            AgentChatTurnState::Succeeded => AgentChatTurnStatus::Succeeded,
            AgentChatTurnState::Failed => AgentChatTurnStatus::Failed,
            AgentChatTurnState::Cancelled => AgentChatTurnStatus::Cancelled,
        },
        pending_interaction_id: job.pending_interaction_id,
        attempt_count: job.attempt_count,
        max_attempts: job.max_attempts,
        lease_expires_at: job.leased_until,
        next_attempt_at: job.next_attempt_at,
        response_message_id: job.response_message_id,
        error_code: job.error_code,
        error_message: job.error_message,
        failure_class: job.failure_class,
        retry_decision: job.retry_decision,
        pre_provider_failure_count: job.pre_provider_failure_count,
        retry_action,
        error,
        correlation_id: job.correlation_id,
        version: job.version,
        created_at: job.created_at,
        updated_at: job.updated_at,
    }
}

fn handoff_response(handoff: AgentHandoff, target_project_id: String) -> AgentHandoffResponse {
    AgentHandoffResponse {
        id: handoff.id,
        source_chat_id: handoff.source_chat_id,
        source_message_id: handoff.source_message_id,
        source_turn_job_id: handoff.source_turn_job_id,
        target_project_id,
        target_chat_id: handoff.target_chat_id,
        author_identity_id: handoff.author_identity_id,
        content: handoff.content,
        content_guard: parse_json(&handoff.content_guard_json),
        sensitivity: "internal".to_owned(),
        status: match handoff.status {
            DbHandoffStatus::Pending => AgentHandoffStatus::Pending,
            DbHandoffStatus::Delivered => AgentHandoffStatus::Delivered,
            DbHandoffStatus::Failed => AgentHandoffStatus::Failed,
            DbHandoffStatus::Cancelled => AgentHandoffStatus::Cancelled,
        },
        target_message_id: handoff.target_message_id,
        target_turn_job_id: handoff.target_turn_job_id,
        dedupe_key: handoff.dedupe_key,
        correlation_id: handoff.correlation_id,
        causation_id: handoff.causation_id,
        error: handoff.error_code,
        created_at: handoff.created_at,
        updated_at: handoff.updated_at,
        delivered_at: None,
    }
}

fn binding_state(value: &str) -> AgentBindingState {
    match value {
        "setup_required" | "agent_setup_required" => AgentBindingState::SetupRequired,
        "paused" | "suspended" => AgentBindingState::Paused,
        "replaced" => AgentBindingState::Replaced,
        "revoked" => AgentBindingState::Revoked,
        _ => AgentBindingState::Active,
    }
}

fn chat_status(value: &str) -> AgentChatStatus {
    match value {
        "agent_setup_required" | "setup_required" => AgentChatStatus::SetupRequired,
        "archived" => AgentChatStatus::Archived,
        _ => AgentChatStatus::Ready,
    }
}

fn parse_json(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or_else(|_| json!({}))
}
