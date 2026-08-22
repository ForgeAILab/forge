use std::str::FromStr;

use api_types::{
    ActionExecutionResponse, AgentActionResponse, AnswerQuestionRequest, ApproveActionRequest,
    AskQuestionRequest, CommitmentEvidenceResponse, CommitmentResponse, CompleteCommitmentRequest,
    CoordinationListQuery, CreateCommitmentRequest, ExecuteActionRequest,
    ExecuteOrchestrationActionRequest, InboxItemResponse, ProposeActionRequest, QuestionResponse,
    TaskProposalCommandResponse, TaskProposalRequest, TransferCommitmentRequest,
    UpdateCommitmentRequest, UpdateInboxItemRequest,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use db::{
    AccountMainAgentBindingRepo, Agent, AgentAction, AgentActionApprovalDecision,
    AgentActionExecution, AgentActionListQuery, AgentActionStatus, AgentCommitment,
    AgentCommitmentEvidence, AgentCommitmentListQuery, AgentCommitmentStatus, AgentInboxItem,
    AgentInboxListQuery, AgentInboxStatus, AgentQuestion, AgentQuestionListQuery,
    AgentQuestionStatus, AgentRepo, ProjectAgentBindingRepo, TaskRepo, TaskRoleAssignmentRepo,
};
use forge_agent_host::{is_project_orchestration_operation, MAIN_PROJECT_CREATE_OPERATION};
use serde_json::Value;
use services::{
    ApproveActionInput, AskQuestionInput, CommitmentEvidenceInput, CompleteCommitmentInput,
    CreateCommitmentInput, DirectTaskProposalInput, ExecuteActionInput,
    ExecuteMainOrchestrationActionInput, ExecuteProjectOrchestrationActionInput,
    MainOrchestrationActionService, ProjectOrchestrationActionService, ProposeActionInput,
    TaskProposalPayload, TransferCommitmentInput, UpdateCommitmentInput,
};

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::AuthenticatedUser,
    state::AppState,
};

pub async fn list_commitments(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
    Query(query): Query<CoordinationListQuery>,
) -> ApiResult<Json<Vec<CommitmentResponse>>> {
    require_owned_identity(&state, &identity_id, &user.user_id).await?;
    if let (Some(scope_type), Some(scope_id)) =
        (query.scope_type.as_deref(), query.scope_id.as_deref())
    {
        authorize_scope_member(&state, scope_type, scope_id, &user.user_id).await?;
    }
    let commitments = state
        .commitment_service
        .list(AgentCommitmentListQuery {
            owner_identity_id: Some(identity_id),
            scope_type: query.scope_type.clone(),
            scope_id: query.scope_id.clone(),
            status: parse_commitment_status(query.status.as_deref())?,
            limit: bounded_limit(query.limit),
        })
        .await?;
    let mut visible = Vec::with_capacity(commitments.len());
    for commitment in commitments {
        if authorize_commitment_read(&state, &commitment, &user.user_id)
            .await
            .is_ok()
        {
            visible.push(commitment_response(commitment));
        }
    }
    Ok(Json(visible))
}

pub async fn create_commitment(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
    Json(request): Json<CreateCommitmentRequest>,
) -> ApiResult<(StatusCode, Json<CommitmentResponse>)> {
    require_identity_scope(
        &state,
        &identity_id,
        &request.scope_type,
        &request.scope_id,
        &user.user_id,
    )
    .await?;
    let status = parse_commitment_status(request.status.as_deref())?
        .unwrap_or(AgentCommitmentStatus::Proposed);
    if matches!(
        status,
        AgentCommitmentStatus::Completed | AgentCommitmentStatus::Cancelled
    ) {
        return Err(ApiError::bad_request(
            "a new commitment must begin in a non-terminal state",
        ));
    }
    let commitment = state
        .commitment_service
        .create(CreateCommitmentInput {
            id: None,
            owner_identity_id: identity_id,
            scope_type: request.scope_type,
            scope_id: request.scope_id,
            title: request.title,
            description: request.description,
            status,
            due_at: request.due_at,
            correlation_id: request.correlation_id,
            originating_action_id: request.originating_action_id,
            originating_task_id: request.originating_task_id,
            evidence_required: request.evidence_required.unwrap_or(true),
        })
        .await?;
    Ok((StatusCode::CREATED, Json(commitment_response(commitment))))
}

pub async fn get_commitment(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<CommitmentResponse>> {
    let commitment = state.commitment_service.get(&id).await?;
    authorize_commitment_read(&state, &commitment, &user.user_id).await?;
    Ok(Json(commitment_response(commitment)))
}

pub async fn update_commitment(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<UpdateCommitmentRequest>,
) -> ApiResult<Json<CommitmentResponse>> {
    let commitment = state.commitment_service.get(&id).await?;
    authorize_commitment_mutation(&state, &commitment, &user.user_id).await?;
    let status = parse_commitment_status(request.status.as_deref())?;
    let updated = state
        .commitment_service
        .update(UpdateCommitmentInput {
            id,
            expected_version: request.expected_version,
            status,
            due_at: request.due_at,
            description: request.description,
            blocked_reason: request.blocked_reason,
            cancellation_reason: request.cancellation_reason,
            actor_type: "user".to_owned(),
            actor_id: user.user_id,
            reason: request.reason,
            evidence_id: request.evidence_id,
            dedupe_key: request.dedupe_key,
        })
        .await?;
    Ok(Json(commitment_response(updated)))
}

pub async fn complete_commitment(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<CompleteCommitmentRequest>,
) -> ApiResult<Json<CommitmentResponse>> {
    let commitment = state.commitment_service.get(&id).await?;
    authorize_commitment_mutation(&state, &commitment, &user.user_id).await?;
    let evidence = CommitmentEvidenceInput {
        id: None,
        commitment_id: id.clone(),
        evidence_type: request.evidence_type,
        evidence_id: request.evidence_id,
        scope_type: commitment.scope_type.clone(),
        scope_id: commitment.scope_id.clone(),
        description: request.description,
        metadata_json: request.metadata.to_string(),
        authorized_by_type: "user".to_owned(),
        authorized_by_id: user.user_id.clone(),
        dedupe_key: request.dedupe_key.clone(),
    };
    let completed = state
        .commitment_service
        .complete(CompleteCommitmentInput {
            id,
            expected_version: request.expected_version,
            evidence,
            actor_type: "user".to_owned(),
            actor_id: user.user_id,
            reason: request.reason,
            dedupe_key: request.dedupe_key,
        })
        .await?;
    Ok(Json(commitment_response(completed)))
}

pub async fn transfer_commitment(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<TransferCommitmentRequest>,
) -> ApiResult<Json<CommitmentResponse>> {
    let commitment = state.commitment_service.get(&id).await?;
    authorize_commitment_mutation(&state, &commitment, &user.user_id).await?;
    require_owned_identity(&state, &request.to_identity_id, &user.user_id).await?;
    require_identity_scope(
        &state,
        &request.to_identity_id,
        &commitment.scope_type,
        &commitment.scope_id,
        &user.user_id,
    )
    .await?;
    let transferred = state
        .commitment_service
        .transfer(TransferCommitmentInput {
            id,
            expected_version: request.expected_version,
            to_identity_id: request.to_identity_id,
            reason: request.reason,
            actor_type: "user".to_owned(),
            actor_id: user.user_id,
            dedupe_key: request.dedupe_key,
        })
        .await?;
    Ok(Json(commitment_response(transferred)))
}

pub async fn cancel_commitment(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<UpdateCommitmentRequest>,
) -> ApiResult<Json<CommitmentResponse>> {
    let commitment = state.commitment_service.get(&id).await?;
    authorize_commitment_mutation(&state, &commitment, &user.user_id).await?;
    let cancelled = state
        .commitment_service
        .cancel(
            id,
            request.expected_version,
            request
                .reason
                .or(request.cancellation_reason.flatten())
                .ok_or_else(|| ApiError::bad_request("cancellation requires a reason"))?,
            "user".to_owned(),
            user.user_id,
            request.dedupe_key,
        )
        .await?;
    Ok(Json(commitment_response(cancelled)))
}

pub async fn list_commitment_evidence(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<CommitmentEvidenceResponse>>> {
    let commitment = state.commitment_service.get(&id).await?;
    authorize_commitment_read(&state, &commitment, &user.user_id).await?;
    Ok(Json(
        state
            .commitment_service
            .evidence(&id)
            .await?
            .into_iter()
            .map(evidence_response)
            .collect(),
    ))
}

pub async fn list_inbox(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
    Query(query): Query<CoordinationListQuery>,
) -> ApiResult<Json<Vec<InboxItemResponse>>> {
    require_owned_identity(&state, &identity_id, &user.user_id).await?;
    if let (Some(scope_type), Some(scope_id)) =
        (query.scope_type.as_deref(), query.scope_id.as_deref())
    {
        authorize_scope_member(&state, scope_type, scope_id, &user.user_id).await?;
    }
    let items = state
        .agent_inbox_service
        .list(AgentInboxListQuery {
            recipient_identity_id: identity_id,
            status: parse_inbox_status(query.status.as_deref())?,
            scope_type: query.scope_type.clone(),
            scope_id: query.scope_id.clone(),
            limit: bounded_limit(query.limit),
        })
        .await?;
    let mut visible = Vec::with_capacity(items.len());
    for item in items {
        if authorize_scope_member(&state, &item.scope_type, &item.scope_id, &user.user_id)
            .await
            .is_ok()
        {
            visible.push(inbox_response(item));
        }
    }
    Ok(Json(visible))
}

pub async fn get_inbox_item(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<InboxItemResponse>> {
    let item = state.agent_inbox_service.get(&id).await?;
    require_owned_identity(&state, &item.recipient_identity_id, &user.user_id).await?;
    authorize_scope_member(&state, &item.scope_type, &item.scope_id, &user.user_id).await?;
    Ok(Json(inbox_response(item)))
}

pub async fn update_inbox_item(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<UpdateInboxItemRequest>,
) -> ApiResult<Json<InboxItemResponse>> {
    let item = state.agent_inbox_service.get(&id).await?;
    require_owned_identity(&state, &item.recipient_identity_id, &user.user_id).await?;
    authorize_scope_member(&state, &item.scope_type, &item.scope_id, &user.user_id).await?;
    let updated = state
        .agent_inbox_service
        .set_status(
            id,
            request.expected_version,
            parse_inbox_status(Some(&request.status))?
                .ok_or_else(|| ApiError::bad_request("inbox status is required"))?,
        )
        .await?;
    Ok(Json(inbox_response(updated)))
}

pub async fn list_questions(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
    Query(query): Query<CoordinationListQuery>,
) -> ApiResult<Json<Vec<QuestionResponse>>> {
    require_owned_identity(&state, &identity_id, &user.user_id).await?;
    if let (Some(scope_type), Some(scope_id)) =
        (query.scope_type.as_deref(), query.scope_id.as_deref())
    {
        authorize_scope_member(&state, scope_type, scope_id, &user.user_id).await?;
    }
    let questions = state
        .agent_inbox_service
        .list_questions(AgentQuestionListQuery {
            recipient_identity_id: identity_id,
            status: parse_question_status(query.status.as_deref())?,
            scope_type: query.scope_type.clone(),
            scope_id: query.scope_id.clone(),
            limit: bounded_limit(query.limit),
        })
        .await?;
    let mut visible = Vec::with_capacity(questions.len());
    for question in questions {
        if authorize_question_read(&state, &question, &user.user_id)
            .await
            .is_ok()
        {
            visible.push(question_response(question));
        }
    }
    Ok(Json(visible))
}

pub async fn ask_question(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
    Json(request): Json<AskQuestionRequest>,
) -> ApiResult<(StatusCode, Json<QuestionResponse>)> {
    require_identity_scope(
        &state,
        &identity_id,
        &request.scope_type,
        &request.scope_id,
        &user.user_id,
    )
    .await?;
    let question = state
        .agent_inbox_service
        .ask_question(AskQuestionInput {
            id: None,
            inbox_item_id: None,
            recipient_identity_id: identity_id,
            scope_type: request.scope_type,
            scope_id: request.scope_id,
            question: request.question,
            context_json: request.context.to_string(),
            asked_by_type: "user".to_owned(),
            asked_by_id: user.user_id,
            due_at: request.due_at,
            correlation_id: request.correlation_id,
            inbox_title: request.inbox_title,
            inbox_dedupe_key: request.inbox_dedupe_key,
        })
        .await?;
    Ok((StatusCode::CREATED, Json(question_response(question))))
}

pub async fn get_question(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<QuestionResponse>> {
    let question = state.agent_inbox_service.get_question(&id).await?;
    authorize_question_read(&state, &question, &user.user_id).await?;
    Ok(Json(question_response(question)))
}

pub async fn answer_question(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<AnswerQuestionRequest>,
) -> ApiResult<Json<QuestionResponse>> {
    let question = state.agent_inbox_service.get_question(&id).await?;
    authorize_question_mutation(&state, &question, &user.user_id).await?;
    let answered = state
        .agent_inbox_service
        .answer_question(
            id,
            request.expected_version,
            request.answer,
            "user".to_owned(),
            user.user_id,
        )
        .await?;
    Ok(Json(question_response(answered)))
}

pub async fn list_actions(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
    Query(query): Query<CoordinationListQuery>,
) -> ApiResult<Json<Vec<AgentActionResponse>>> {
    require_owned_identity(&state, &identity_id, &user.user_id).await?;
    if let (Some(scope_type), Some(scope_id)) =
        (query.scope_type.as_deref(), query.scope_id.as_deref())
    {
        authorize_scope_member(&state, scope_type, scope_id, &user.user_id).await?;
    }
    let actions = state
        .agent_action_service
        .list(AgentActionListQuery {
            actor_identity_id: Some(identity_id),
            scope_type: query.scope_type.clone(),
            scope_id: query.scope_id.clone(),
            status: parse_action_status(query.status.as_deref())?,
            limit: bounded_limit(query.limit),
        })
        .await?;
    let mut visible = Vec::with_capacity(actions.len());
    for action in actions {
        if authorize_action_read(&state, &action, &user.user_id)
            .await
            .is_ok()
        {
            visible.push(action_response(action));
        }
    }
    Ok(Json(visible))
}

pub async fn propose_action(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
    Json(request): Json<ProposeActionRequest>,
) -> ApiResult<(StatusCode, Json<AgentActionResponse>)> {
    require_identity_scope(
        &state,
        &identity_id,
        &request.scope_type,
        &request.scope_id,
        &user.user_id,
    )
    .await?;
    let requested_permission = requested_permission_for_operation(&request.operation)?;
    validate_action_target(
        &request.operation,
        &request.scope_type,
        &request.scope_id,
        request.target_type.as_deref(),
        request.target_id.as_deref(),
    )?;
    let action = state
        .agent_action_service
        .propose(ProposeActionInput {
            id: None,
            actor_identity_id: identity_id,
            scope_type: request.scope_type,
            scope_id: request.scope_id,
            operation: request.operation,
            payload_json: request.payload.to_string(),
            dedupe_key: request.dedupe_key,
            correlation_id: request.correlation_id,
            causation_id: request.causation_id,
            causation_depth: request.causation_depth.unwrap_or(0),
            requested_permission: requested_permission.to_owned(),
            policy_reason: None,
            target_type: request.target_type,
            target_id: request.target_id,
        })
        .await?;
    Ok((StatusCode::CREATED, Json(action_response(action))))
}

pub async fn propose_task(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(identity_id): Path<String>,
    Json(request): Json<TaskProposalRequest>,
) -> ApiResult<(StatusCode, Json<TaskProposalCommandResponse>)> {
    // The command service owns active-binding authorization and receipt-first
    // replay.  Keep only stable route authentication here so a response-loss
    // retry can still resolve its frozen receipt after a binding is paused or
    // rotated.
    require_owned_identity(&state, &identity_id, &user.user_id).await?;
    authorize_scope_member(&state, "project", &request.project_id, &user.user_id).await?;
    let task_type = request.task_type.map(|task_type| {
        match task_type {
            api_types::TaskType::Task => "task",
            api_types::TaskType::PlanningTask => "planning_task",
            api_types::TaskType::SubTask => "sub_task",
            api_types::TaskType::Discovery => "discovery",
        }
        .to_owned()
    });
    let payload = serde_json::json!({
        "title": request.title,
        "description": request.description,
        "parent_task_id": request.parent_task_id,
        "priority": request.priority,
        "task_type": task_type,
        "task_state_config": request.task_state_config,
        "merge_config": request.merge_config,
        "role_assignments": request.role_assignments,
        "governance": request.governance,
    });
    let payload_json = payload.to_string();
    let payload: TaskProposalPayload = serde_json::from_value(payload).map_err(|error| {
        ApiError::bad_request(format!("invalid task proposal payload: {error}"))
    })?;
    let (policy_result, policy_reason) = state
        .agent_action_service
        .evaluate_direct_command_policy(
            &identity_id,
            "project",
            &request.project_id,
            "propose_task",
            "task.propose",
            Some(&payload_json),
        )
        .await?;
    let result = state
        .task_service
        .execute_task_proposal_direct(DirectTaskProposalInput {
            actor_identity_id: identity_id,
            executor_type: "user".to_owned(),
            executor_id: user.user_id,
            source_scope_type: "project".to_owned(),
            source_scope_id: request.project_id.clone(),
            project_id: request.project_id,
            payload,
            idempotency_key: request.dedupe_key,
            correlation_id: request.correlation_id,
            causation_id: request.causation_id,
            causation_depth: request.causation_depth.unwrap_or(0),
            policy_result: "allowed".to_owned(),
            preflight_policy_result: Some(policy_result.to_string()),
            preflight_policy_reason: policy_reason,
            policy_revision: None,
            policy_digest: None,
            requested_permission: "propose_task".to_owned(),
        })
        .await?;
    let status = if result.replayed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    let response = TaskProposalCommandResponse {
        operation: "task.propose".to_owned(),
        status: "succeeded".to_owned(),
        materialized: true,
        domain_committed: true,
        receipt_id: result.receipt.id,
        event_id: result.receipt.event_id,
        input_digest: result.receipt.input_digest,
        policy_result: result.receipt.policy_result,
        correlation_id: result.receipt.correlation_id,
        replayed: result.replayed,
        requires_user_authorization: false,
        task: crate::routes::task_response(&state.db, result.task).await?,
    };
    Ok((status, Json(response)))
}

pub async fn get_action(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<AgentActionResponse>> {
    let action = state.agent_action_service.get(&id).await?;
    authorize_action_read(&state, &action, &user.user_id).await?;
    Ok(Json(action_response(action)))
}

pub async fn approve_action(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<ApproveActionRequest>,
) -> ApiResult<Json<AgentActionResponse>> {
    let action = state.agent_action_service.get(&id).await?;
    authorize_action_read(&state, &action, &user.user_id).await?;
    require_owned_identity(&state, &request.approver_identity_id, &user.user_id).await?;
    let decision = AgentActionApprovalDecision::from_str(&request.decision)
        .map_err(|_| ApiError::bad_request("decision must be approved or denied"))?;
    state
        .agent_action_service
        .approve(ApproveActionInput {
            action_id: id.clone(),
            expected_version: request.expected_version,
            approver_identity_id: request.approver_identity_id,
            decision,
            reason: request.reason,
        })
        .await?;
    Ok(Json(action_response(
        state.agent_action_service.get(&id).await?,
    )))
}

pub async fn execute_action(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<ExecuteActionRequest>,
) -> ApiResult<Json<ActionExecutionResponse>> {
    let action = state.agent_action_service.get(&id).await?;
    authorize_action_mutation(&state, &action, &user.user_id).await?;
    let execution = state
        .agent_action_service
        .execute(ExecuteActionInput {
            action_id: id,
            expected_version: request.expected_version,
            attempt: request.attempt.unwrap_or(1),
            result_json: request.result.map(|value| value.to_string()),
            error: request.error,
            executed_by_type: "user".to_owned(),
            executed_by_id: user.user_id,
            idempotency_key: request.idempotency_key,
        })
        .await?;
    Ok(Json(execution_response(execution)))
}

/// Execute a Main Agent Charter/Project proposal through its typed domain
/// materializer. The generic `/execute` endpoint intentionally refuses these
/// operations so a caller cannot manufacture a successful result envelope.
pub async fn execute_orchestration_action(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Json(request): Json<ExecuteOrchestrationActionRequest>,
) -> ApiResult<Json<ActionExecutionResponse>> {
    let action = state.agent_action_service.get(&id).await?;
    authorize_action_mutation(&state, &action, &user.user_id).await?;
    let execution = if action.operation == MAIN_PROJECT_CREATE_OPERATION {
        MainOrchestrationActionService::new(state.db.clone())
            .execute(ExecuteMainOrchestrationActionInput {
                action_id: id,
                expected_version: request.expected_version,
                executed_by_type: "user".to_owned(),
                executed_by_id: user.user_id,
                idempotency_key: request.idempotency_key,
            })
            .await?
    } else if is_project_orchestration_operation(&action.operation) {
        ProjectOrchestrationActionService::new(state.db.clone())
            .execute(ExecuteProjectOrchestrationActionInput {
                action_id: id,
                expected_version: request.expected_version,
                executed_by_type: "user".to_owned(),
                executed_by_id: user.user_id,
                idempotency_key: request.idempotency_key,
            })
            .await?
    } else {
        return Err(ApiError::bad_request(
            "action is not a typed orchestration proposal",
        ));
    };
    Ok(Json(execution_response(execution)))
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

async fn require_identity_scope(
    state: &AppState,
    identity_id: &str,
    scope_type: &str,
    scope_id: &str,
    user_id: &str,
) -> ApiResult<()> {
    require_owned_identity(state, identity_id, user_id).await?;
    match scope_type {
        "account" => {
            if scope_id != user_id {
                return Err(ApiError::not_found("account", scope_id.to_owned()));
            }
        }
        "project" => {
            crate::routes::project_agents::require_project_member(state, scope_id, user_id).await?;
            let binding = ProjectAgentBindingRepo::get_active_project_binding(&*state.db, scope_id)
                .await?
                .filter(|binding| {
                    binding.state == "active" && binding.identity_id.as_deref() == Some(identity_id)
                })
                .ok_or_else(|| ApiError::not_found("project_agent_binding", identity_id))?;
            let _ = binding;
        }
        "agent_chat" => {
            let chat = state
                .agent_chat_service
                .get_authorized_chat(user_id, scope_id)
                .await?;
            match chat.kind.as_str() {
                "account_main" => {
                    let binding =
                        AccountMainAgentBindingRepo::get_active_main_binding(&*state.db, user_id)
                            .await?
                            .filter(|binding| binding.identity_id == identity_id)
                            .ok_or_else(|| {
                                ApiError::not_found("main_agent_binding", identity_id)
                            })?;
                    let _ = binding;
                }
                "project" => {
                    let project_id = chat
                        .project_id
                        .as_deref()
                        .ok_or_else(|| ApiError::not_found("agent_chat", scope_id.to_owned()))?;
                    let binding =
                        ProjectAgentBindingRepo::get_active_project_binding(&*state.db, project_id)
                            .await?
                            .filter(|binding| {
                                binding.state == "active"
                                    && binding.identity_id.as_deref() == Some(identity_id)
                            })
                            .ok_or_else(|| {
                                ApiError::not_found("project_agent_binding", identity_id)
                            })?;
                    let _ = binding;
                }
                _ => return Err(ApiError::not_found("agent_chat", scope_id.to_owned())),
            }
        }
        "task" => {
            let task = TaskRepo::get_by_id(&*state.db, scope_id, false)
                .await?
                .ok_or_else(|| ApiError::not_found("task", scope_id.to_owned()))?;
            crate::routes::project_agents::require_project_member(state, &task.project_id, user_id)
                .await?;
            let assigned_directly = task.assignee_type.as_deref() == Some("agent")
                && task.assignee_id.as_deref() == Some(identity_id);
            let assigned_role = TaskRoleAssignmentRepo::list_by_task(&*state.db, scope_id)
                .await?
                .into_iter()
                .any(|assignment| {
                    assignment
                        .assignee_type
                        .as_ref()
                        .is_some_and(|kind| kind.to_string() == "agent")
                        && assignment.assignee_id.as_deref() == Some(identity_id)
                });
            if !assigned_directly && !assigned_role {
                return Err(ApiError::not_found("task_assignment", scope_id.to_owned()));
            }
        }
        "agent" => {
            if scope_id != identity_id {
                return Err(ApiError::not_found("agent", scope_id.to_owned()));
            }
        }
        _ => return Err(ApiError::bad_request("unsupported canonical scope type")),
    }
    Ok(())
}

async fn authorize_commitment_read(
    state: &AppState,
    commitment: &AgentCommitment,
    user_id: &str,
) -> ApiResult<()> {
    let owns_identity = AgentRepo::get_by_id(&*state.db, &commitment.owner_identity_id)
        .await?
        .is_some_and(|agent| agent.owner_id.as_deref() == Some(user_id));
    if owns_identity && commitment.scope_type == "account" && commitment.scope_id == user_id {
        return Ok(());
    }
    authorize_scope_member(state, &commitment.scope_type, &commitment.scope_id, user_id).await
}

async fn authorize_commitment_mutation(
    state: &AppState,
    commitment: &AgentCommitment,
    user_id: &str,
) -> ApiResult<()> {
    authorize_commitment_read(state, commitment, user_id).await?;
    let owns_identity = AgentRepo::get_by_id(&*state.db, &commitment.owner_identity_id)
        .await?
        .is_some_and(|agent| agent.owner_id.as_deref() == Some(user_id));
    if owns_identity {
        return Ok(());
    }

    // Shared scope membership is sufficient for reads, but mutating another
    // identity's obligation is reserved for the canonical scope owner/admin.
    match commitment.scope_type.as_str() {
        "project" => {
            require_coordination_project_admin(state, &commitment.scope_id, user_id).await?;
            Ok(())
        }
        "task" => {
            let task = TaskRepo::get_by_id(&*state.db, &commitment.scope_id, false)
                .await?
                .ok_or_else(|| ApiError::not_found("task", commitment.scope_id.clone()))?;
            require_coordination_project_admin(state, &task.project_id, user_id).await?;
            Ok(())
        }
        "agent_chat" => {
            authorize_agent_chat_mutation(state, &commitment.scope_id, user_id).await?;
            Ok(())
        }
        _ => Err(ApiError::forbidden_with_code(
            "coordination_mutation_forbidden",
            "only the commitment owner or canonical scope owner may mutate this commitment",
        )),
    }
}

async fn authorize_question_read(
    state: &AppState,
    question: &AgentQuestion,
    user_id: &str,
) -> ApiResult<()> {
    let owns_identity = AgentRepo::get_by_id(&*state.db, &question.recipient_identity_id)
        .await?
        .is_some_and(|agent| agent.owner_id.as_deref() == Some(user_id));
    if owns_identity && question.scope_type == "account" && question.scope_id == user_id {
        return Ok(());
    }
    authorize_scope_member(state, &question.scope_type, &question.scope_id, user_id).await
}

async fn authorize_question_mutation(
    state: &AppState,
    question: &AgentQuestion,
    user_id: &str,
) -> ApiResult<()> {
    authorize_question_read(state, question, user_id).await?;
    let owns_identity = AgentRepo::get_by_id(&*state.db, &question.recipient_identity_id)
        .await?
        .is_some_and(|agent| agent.owner_id.as_deref() == Some(user_id));
    if owns_identity {
        return Ok(());
    }
    match question.scope_type.as_str() {
        "project" => {
            require_coordination_project_admin(state, &question.scope_id, user_id).await?;
            Ok(())
        }
        "task" => {
            let task = TaskRepo::get_by_id(&*state.db, &question.scope_id, false)
                .await?
                .ok_or_else(|| ApiError::not_found("task", question.scope_id.clone()))?;
            require_coordination_project_admin(state, &task.project_id, user_id).await?;
            Ok(())
        }
        "agent_chat" => {
            authorize_agent_chat_mutation(state, &question.scope_id, user_id).await?;
            Ok(())
        }
        _ => Err(ApiError::forbidden_with_code(
            "coordination_mutation_forbidden",
            "only the question recipient owner or canonical scope owner may answer",
        )),
    }
}

async fn authorize_action_read(
    state: &AppState,
    action: &AgentAction,
    user_id: &str,
) -> ApiResult<()> {
    let owns_identity = AgentRepo::get_by_id(&*state.db, &action.actor_identity_id)
        .await?
        .is_some_and(|agent| agent.owner_id.as_deref() == Some(user_id));
    if owns_identity && action.scope_type == "account" && action.scope_id == user_id {
        return Ok(());
    }
    authorize_scope_member(state, &action.scope_type, &action.scope_id, user_id).await
}

async fn authorize_action_mutation(
    state: &AppState,
    action: &AgentAction,
    user_id: &str,
) -> ApiResult<()> {
    authorize_action_read(state, action, user_id).await?;
    let owns_identity = AgentRepo::get_by_id(&*state.db, &action.actor_identity_id)
        .await?
        .is_some_and(|agent| agent.owner_id.as_deref() == Some(user_id));
    if owns_identity {
        return Ok(());
    }
    match action.scope_type.as_str() {
        "project" => {
            require_coordination_project_admin(state, &action.scope_id, user_id).await?;
            Ok(())
        }
        "task" => {
            let task = TaskRepo::get_by_id(&*state.db, &action.scope_id, false)
                .await?
                .ok_or_else(|| ApiError::not_found("task", action.scope_id.clone()))?;
            require_coordination_project_admin(state, &task.project_id, user_id).await?;
            Ok(())
        }
        "agent_chat" => {
            authorize_agent_chat_mutation(state, &action.scope_id, user_id).await?;
            Ok(())
        }
        _ => Err(ApiError::forbidden_with_code(
            "coordination_mutation_forbidden",
            "only the action owner or canonical scope owner may execute this action",
        )),
    }
}

async fn authorize_scope_member(
    state: &AppState,
    scope_type: &str,
    scope_id: &str,
    user_id: &str,
) -> ApiResult<()> {
    match scope_type {
        "account" if scope_id == user_id => Ok(()),
        "project" => {
            crate::routes::project_agents::require_project_member(state, scope_id, user_id)
                .await
                .map(|_| ())
        }
        "agent_chat" => state
            .agent_chat_service
            .get_authorized_chat(user_id, scope_id)
            .await
            .map(|_| ())
            .map_err(Into::into),
        "task" => {
            let task = TaskRepo::get_by_id(&*state.db, scope_id, false)
                .await?
                .ok_or_else(|| ApiError::not_found("task", scope_id.to_owned()))?;
            crate::routes::project_agents::require_project_member(state, &task.project_id, user_id)
                .await
                .map(|_| ())
        }
        "agent" => require_owned_identity(state, scope_id, user_id)
            .await
            .map(|_| ()),
        _ => Err(ApiError::not_found("scope", scope_id.to_owned())),
    }
}

async fn authorize_agent_chat_mutation(
    state: &AppState,
    chat_id: &str,
    user_id: &str,
) -> ApiResult<()> {
    let chat = state
        .agent_chat_service
        .get_authorized_chat(user_id, chat_id)
        .await?;
    if let Some(project_id) = chat.project_id.as_deref() {
        require_coordination_project_admin(state, project_id, user_id).await
    } else if chat.account_id.as_deref() == Some(user_id) {
        Ok(())
    } else {
        Err(ApiError::forbidden_with_code(
            "coordination_mutation_forbidden",
            "only the canonical agent chat owner may mutate this item",
        ))
    }
}

async fn require_coordination_project_admin(
    state: &AppState,
    project_id: &str,
    user_id: &str,
) -> ApiResult<()> {
    let member =
        crate::routes::project_agents::require_project_member(state, project_id, user_id).await?;
    if member.role == "owner" || member.role == "admin" {
        return Ok(());
    }
    Err(ApiError::forbidden_with_code(
        "coordination_mutation_forbidden",
        "only the coordination owner or canonical scope owner may mutate this record",
    ))
}

fn requested_permission_for_operation(operation: &str) -> ApiResult<&'static str> {
    match operation {
        "task.propose" => Ok("propose_task"),
        "memory.publish" => Ok("propose_memory_publication"),
        "commitment.update" => Ok("propose_commitment"),
        "message.send" => Ok("propose_message"),
        "review.request" => Ok("propose_review"),
        _ => Err(ApiError::bad_request(
            "unsupported action operation; use a typed Forge operation",
        )),
    }
}

fn validate_action_target(
    operation: &str,
    scope_type: &str,
    scope_id: &str,
    target_type: Option<&str>,
    target_id: Option<&str>,
) -> ApiResult<()> {
    if operation == "task.propose"
        && (scope_type != "project"
            || target_type != Some("project")
            || target_id != Some(scope_id))
    {
        return Err(ApiError::bad_request(
            "task proposals must target their admitted Project scope",
        ));
    }
    Ok(())
}

fn parse_commitment_status(value: Option<&str>) -> ApiResult<Option<AgentCommitmentStatus>> {
    value
        .map(|value| {
            AgentCommitmentStatus::from_str(value)
                .map_err(|_| ApiError::bad_request("invalid commitment status"))
        })
        .transpose()
}

fn parse_inbox_status(value: Option<&str>) -> ApiResult<Option<AgentInboxStatus>> {
    value
        .map(|value| {
            AgentInboxStatus::from_str(value)
                .map_err(|_| ApiError::bad_request("invalid inbox status"))
        })
        .transpose()
}

fn parse_question_status(value: Option<&str>) -> ApiResult<Option<AgentQuestionStatus>> {
    value
        .map(|value| {
            AgentQuestionStatus::from_str(value)
                .map_err(|_| ApiError::bad_request("invalid question status"))
        })
        .transpose()
}

fn parse_action_status(value: Option<&str>) -> ApiResult<Option<AgentActionStatus>> {
    value
        .map(|value| {
            AgentActionStatus::from_str(value)
                .map_err(|_| ApiError::bad_request("invalid action status"))
        })
        .transpose()
}

fn bounded_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(50).clamp(1, 100)
}

fn commitment_response(value: AgentCommitment) -> CommitmentResponse {
    CommitmentResponse {
        id: value.id,
        owner_identity_id: value.owner_identity_id,
        scope_type: value.scope_type,
        scope_id: value.scope_id,
        title: value.title,
        description: value.description,
        status: value.status.to_string(),
        due_at: value.due_at,
        correlation_id: value.correlation_id,
        originating_action_id: value.originating_action_id,
        originating_task_id: value.originating_task_id,
        evidence_required: value.evidence_required,
        cancellation_reason: value.cancellation_reason,
        blocked_reason: value.blocked_reason,
        completed_at: value.completed_at,
        cancelled_at: value.cancelled_at,
        version: value.version,
        created_at: value.created_at,
        updated_at: value.updated_at,
    }
}

fn evidence_response(value: AgentCommitmentEvidence) -> CommitmentEvidenceResponse {
    CommitmentEvidenceResponse {
        id: value.id,
        commitment_id: value.commitment_id,
        evidence_type: value.evidence_type,
        evidence_id: value.evidence_id,
        scope_type: value.scope_type,
        scope_id: value.scope_id,
        description: value.description,
        metadata: parse_json(&value.metadata_json),
        authorized_by_type: value.authorized_by_type,
        authorized_by_id: value.authorized_by_id,
        dedupe_key: value.dedupe_key,
        created_at: value.created_at,
    }
}

fn inbox_response(value: AgentInboxItem) -> InboxItemResponse {
    InboxItemResponse {
        id: value.id,
        recipient_identity_id: value.recipient_identity_id,
        scope_type: value.scope_type,
        scope_id: value.scope_id,
        kind: value.kind.to_string(),
        status: value.status.to_string(),
        title: value.title,
        body: value.body,
        payload: parse_json(&value.payload_json),
        source_type: value.source_type,
        source_id: value.source_id,
        correlation_id: value.correlation_id,
        causation_id: value.causation_id,
        dedupe_key: value.dedupe_key,
        read_at: value.read_at,
        acknowledged_at: value.acknowledged_at,
        version: value.version,
        created_at: value.created_at,
        updated_at: value.updated_at,
    }
}

fn question_response(value: AgentQuestion) -> QuestionResponse {
    QuestionResponse {
        id: value.id,
        recipient_identity_id: value.recipient_identity_id,
        scope_type: value.scope_type,
        scope_id: value.scope_id,
        status: value.status.to_string(),
        question: value.question,
        context: parse_json(&value.context_json),
        answer: value.answer,
        asked_by_type: value.asked_by_type,
        asked_by_id: value.asked_by_id,
        answered_by_type: value.answered_by_type,
        answered_by_id: value.answered_by_id,
        inbox_item_id: value.inbox_item_id,
        due_at: value.due_at,
        correlation_id: value.correlation_id,
        version: value.version,
        answered_at: value.answered_at,
        created_at: value.created_at,
        updated_at: value.updated_at,
    }
}

fn action_response(value: AgentAction) -> AgentActionResponse {
    let materialized = action_materialized(
        &value.operation,
        &value.status,
        value.target_type.as_deref(),
        value.target_id.as_deref(),
        value.outcome_json.as_deref(),
    );
    AgentActionResponse {
        id: value.id,
        actor_identity_id: value.actor_identity_id,
        scope_type: value.scope_type,
        scope_id: value.scope_id,
        operation: value.operation,
        payload_hash: value.payload_hash,
        dedupe_key: value.dedupe_key,
        correlation_id: value.correlation_id,
        causation_id: value.causation_id,
        causation_depth: value.causation_depth,
        requested_permission: value.requested_permission,
        policy_result: value.policy_result.to_string(),
        policy_reason: value.policy_reason,
        status: value.status.to_string(),
        target_type: value.target_type,
        target_id: value.target_id,
        outcome: value.outcome_json.as_deref().map(parse_json),
        materialized,
        version: value.version,
        created_at: value.created_at,
        updated_at: value.updated_at,
    }
}

/// Materialization is a derived public projection, not a second authority
/// flag. It becomes true only after a typed executor has transitioned the
/// action to `executed`, retained its server-derived target, and persisted the
/// typed outcome that proves which domain operation completed.
fn action_materialized(
    operation: &str,
    status: &AgentActionStatus,
    target_type: Option<&str>,
    target_id: Option<&str>,
    outcome_json: Option<&str>,
) -> bool {
    if *status != AgentActionStatus::Executed
        || target_type.is_none_or(str::is_empty)
        || target_id.is_none_or(str::is_empty)
    {
        return false;
    }
    let Some(outcome) = outcome_json
        .and_then(|value| serde_json::from_str::<Value>(value).ok())
        .and_then(|value| value.as_object().cloned())
    else {
        return false;
    };

    if operation == "task.propose" {
        return outcome
            .get("task_id")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty());
    }
    if operation == MAIN_PROJECT_CREATE_OPERATION || is_project_orchestration_operation(operation) {
        return outcome
            .get("operation")
            .and_then(Value::as_str)
            .is_some_and(|value| value == operation);
    }
    false
}

fn execution_response(value: AgentActionExecution) -> ActionExecutionResponse {
    ActionExecutionResponse {
        id: value.id,
        action_id: value.action_id,
        attempt: value.attempt,
        status: value.status.to_string(),
        result: value.result_json.as_deref().map(parse_json),
        error: value.error,
        executed_by_type: value.executed_by_type,
        executed_by_id: value.executed_by_id,
        idempotency_key: value.idempotency_key,
        created_at: value.created_at,
        completed_at: value.completed_at,
    }
}

fn parse_json(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_materialized_is_false_until_typed_outcome_is_persisted() {
        assert!(!action_materialized(
            "task.propose",
            &AgentActionStatus::Proposed,
            Some("project"),
            Some("project-1"),
            None,
        ));
        assert!(!action_materialized(
            "task.propose",
            &AgentActionStatus::Executed,
            Some("project"),
            Some("project-1"),
            None,
        ));
        assert!(!action_materialized(
            "task.propose",
            &AgentActionStatus::Executed,
            None,
            Some("project-1"),
            Some(r#"{"task_id":"task-1"}"#),
        ));
        assert!(!action_materialized(
            "task.propose",
            &AgentActionStatus::Executed,
            Some("project"),
            Some("project-1"),
            Some(r#"{"task_id":""}"#),
        ));
        assert!(action_materialized(
            "task.propose",
            &AgentActionStatus::Executed,
            Some("project"),
            Some("project-1"),
            Some(r#"{"task_id":"task-1"}"#),
        ));
        assert!(action_materialized(
            forge_agent_host::PROJECT_DOCUMENT_OPERATION,
            &AgentActionStatus::Executed,
            Some("project"),
            Some("project-1"),
            Some(r#"{"operation":"project.document","domain_committed":true}"#),
        ));
        assert!(!action_materialized(
            forge_agent_host::PROJECT_DOCUMENT_OPERATION,
            &AgentActionStatus::Executed,
            Some("project"),
            Some("project-1"),
            Some(r#"{"operation":"project.decision","domain_committed":true}"#),
        ));
        assert!(!action_materialized(
            "ordinary.action",
            &AgentActionStatus::Executed,
            Some("project"),
            Some("project-1"),
            Some(r#"{"task_id":"task-1"}"#),
        ));
    }
}
