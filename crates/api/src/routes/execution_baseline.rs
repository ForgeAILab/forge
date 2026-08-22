//! Project execution-baseline proposal, approval, and activation routes.
//!
//! A baseline is an immutable, digest-addressed execution contract.  This
//! module is a thin HTTP adapter: it authenticates the request, maps the
//! public envelope into the shared command service, and projects the committed
//! result.  Domain validation, lifecycle transitions, idempotency, receipts,
//! events, and Task-governance promotion live below the route.

use api_types::{
    ApproveExecutionBaselineRequest, AuthorizationProvenance, CreateExecutionBaselineRequest,
    ExecutionBaselineResponse, ExecutionBaselineWriteOperation,
    SaveExecutionBaselineRevisionRequest,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use services::{
    ActivateExecutionBaselineCommand, ApproveExecutionBaselineCommand,
    ExecutionBaselineCommandService, ExecutionBaselineQueryService, ProjectCommandAuthorization,
    ProposeExecutionBaselineForApprovalCommand, SaveExecutionBaselineDraftCommand,
    EXECUTION_BASELINE_ACTIVATE_COMMAND, EXECUTION_BASELINE_APPROVE_COMMAND,
    EXECUTION_BASELINE_PROPOSE_COMMAND, EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
};

use crate::{
    errors::{ApiError, ApiResult},
    routes::{auth::AuthenticatedUser, scoped_idempotency_key},
    state::AppState,
};

pub async fn get_execution_baseline(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<Json<ExecutionBaselineResponse>> {
    let response = ExecutionBaselineQueryService::new(state.db.clone())
        .get(&project_id, &user.user_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

pub async fn create_execution_baseline(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<CreateExecutionBaselineRequest>,
) -> ApiResult<(StatusCode, Json<ExecutionBaselineResponse>)> {
    if request.operation != ExecutionBaselineWriteOperation::SaveDraft {
        return Err(ApiError::bad_request(
            "the collection execution-baseline endpoint only accepts operation=save_draft",
        ));
    }
    validate_idempotency_key(&request.mutation.idempotency_key)?;
    let storage_key = scoped_idempotency_key(
        EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
        &project_id,
        &user.user_id,
        &request.mutation.idempotency_key,
    );
    let query = ExecutionBaselineQueryService::new(state.db.clone());
    let replay = query
        .has_command_receipt(
            "user",
            &user.user_id,
            &project_id,
            EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
            &storage_key,
        )
        .await
        .map_err(ApiError::from)?;
    let authorization = baseline_authorization(
        &request.mutation.authorization,
        &user.user_id,
        EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
        &storage_key,
    )?;
    let outcome = ExecutionBaselineCommandService::new(state.db.clone())
        .save_draft(SaveExecutionBaselineDraftCommand {
            project_id: project_id.clone(),
            baseline_id: None,
            base_revision_id: request.base_revision_id,
            expected_baseline_version: Some(request.mutation.expected_version),
            content: request.content,
            rendered_view: request.rendered_view,
            render_version: request.render_version,
            content_digest: request.content_digest,
            render_digest: request.render_digest,
            provenance: request.provenance,
            idempotency_key: storage_key,
            authorization,
            action: None,
        })
        .await
        .map_err(ApiError::from)?;
    Ok((
        if replay {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(
            query
                .response_for_command(&project_id, outcome)
                .await
                .map_err(ApiError::from)?,
        ),
    ))
}

pub async fn save_execution_baseline_revision(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, baseline_id)): Path<(String, String)>,
    Json(request): Json<SaveExecutionBaselineRevisionRequest>,
) -> ApiResult<(StatusCode, Json<ExecutionBaselineResponse>)> {
    let operation = request.operation;
    let operation_name = baseline_write_operation_name(operation);
    validate_idempotency_key(&request.mutation.idempotency_key)?;
    let storage_key = scoped_idempotency_key(
        operation_name,
        &project_id,
        &user.user_id,
        &request.mutation.idempotency_key,
    );
    let query = ExecutionBaselineQueryService::new(state.db.clone());
    let replay = query
        .has_command_receipt(
            "user",
            &user.user_id,
            &project_id,
            operation_name,
            &storage_key,
        )
        .await
        .map_err(ApiError::from)?;
    let authorization = baseline_authorization(
        &request.mutation.authorization,
        &user.user_id,
        operation_name,
        &storage_key,
    )?;
    let service = ExecutionBaselineCommandService::new(state.db.clone());
    let outcome = match operation {
        ExecutionBaselineWriteOperation::SaveDraft => {
            service
                .save_draft(SaveExecutionBaselineDraftCommand {
                    project_id: project_id.clone(),
                    baseline_id: Some(baseline_id.clone()),
                    base_revision_id: request.base_revision_id.clone(),
                    expected_baseline_version: Some(request.mutation.expected_version),
                    content: request.content.clone(),
                    rendered_view: request.rendered_view.clone(),
                    render_version: request.render_version.clone(),
                    content_digest: request.content_digest.clone(),
                    render_digest: request.render_digest.clone(),
                    provenance: request.provenance.clone(),
                    idempotency_key: storage_key.clone(),
                    authorization: authorization.clone(),
                    action: None,
                })
                .await
        }
        ExecutionBaselineWriteOperation::ProposeForApproval => {
            service
                .propose_for_approval(ProposeExecutionBaselineForApprovalCommand {
                    project_id: project_id.clone(),
                    baseline_id: baseline_id.clone(),
                    base_revision_id: request.base_revision_id.clone(),
                    expected_baseline_version: request.mutation.expected_version,
                    content: request.content.clone(),
                    rendered_view: request.rendered_view.clone(),
                    render_version: request.render_version.clone(),
                    content_digest: request.content_digest.clone(),
                    render_digest: request.render_digest.clone(),
                    provenance: request.provenance.clone(),
                    idempotency_key: storage_key,
                    authorization,
                    action: None,
                })
                .await
        }
    }
    .map_err(ApiError::from)?;

    Ok((
        if replay {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(
            query
                .response_for_command(&project_id, outcome)
                .await
                .map_err(ApiError::from)?,
        ),
    ))
}

pub async fn approve_execution_baseline(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, baseline_id, revision_id)): Path<(String, String, String)>,
    Json(request): Json<ApproveExecutionBaselineRequest>,
) -> ApiResult<(StatusCode, Json<ExecutionBaselineResponse>)> {
    if request.revision_id != revision_id {
        return Err(ApiError::conflict_with_code(
            "idempotency_conflict",
            "approval target does not match the request path",
        ));
    }
    validate_idempotency_key(&request.mutation.idempotency_key)?;
    let storage_key = scoped_idempotency_key(
        EXECUTION_BASELINE_APPROVE_COMMAND,
        &project_id,
        &user.user_id,
        &request.mutation.idempotency_key,
    );
    let query = ExecutionBaselineQueryService::new(state.db.clone());
    let replay = query
        .has_command_receipt(
            "user",
            &user.user_id,
            &project_id,
            EXECUTION_BASELINE_APPROVE_COMMAND,
            &storage_key,
        )
        .await
        .map_err(ApiError::from)?;
    let authorization = baseline_authorization(
        &request.mutation.authorization,
        &user.user_id,
        EXECUTION_BASELINE_APPROVE_COMMAND,
        &storage_key,
    )?;
    let outcome = ExecutionBaselineCommandService::new(state.db.clone())
        .approve(ApproveExecutionBaselineCommand {
            project_id: project_id.clone(),
            baseline_id,
            revision_id,
            expected_baseline_version: request.mutation.expected_version,
            expected_project_version: request.expected_project_version,
            content_digest: request.content_digest,
            render_digest: request.render_digest,
            idempotency_key: storage_key,
            authorization,
            action: None,
        })
        .await
        .map_err(ApiError::from)?;

    Ok((
        if replay {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(
            query
                .response_for_command(&project_id, outcome)
                .await
                .map_err(ApiError::from)?,
        ),
    ))
}

pub async fn activate_execution_baseline(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, baseline_id)): Path<(String, String)>,
    Json(request): Json<api_types::ActivateExecutionBaselineRequest>,
) -> ApiResult<(StatusCode, Json<ExecutionBaselineResponse>)> {
    if request.baseline_id != baseline_id {
        return Err(ApiError::conflict_with_code(
            "idempotency_conflict",
            "activation target does not match the request path",
        ));
    }
    validate_idempotency_key(&request.mutation.idempotency_key)?;
    let storage_key = scoped_idempotency_key(
        EXECUTION_BASELINE_ACTIVATE_COMMAND,
        &project_id,
        &user.user_id,
        &request.mutation.idempotency_key,
    );
    let authorization = baseline_authorization(
        &request.mutation.authorization,
        &user.user_id,
        EXECUTION_BASELINE_ACTIVATE_COMMAND,
        &storage_key,
    )?;
    let outcome = ExecutionBaselineCommandService::new(state.db.clone())
        .activate(ActivateExecutionBaselineCommand {
            project_id: project_id.clone(),
            baseline_id,
            revision_id: request.revision_id,
            approval_id: request.approval_id,
            expected_baseline_version: request.expected_baseline_version,
            expected_project_version: request.mutation.expected_version,
            content_digest: request.content_digest,
            render_digest: request.render_digest,
            idempotency_key: storage_key,
            authorization,
            action: None,
        })
        .await
        .map_err(ApiError::from)?;

    Ok((
        StatusCode::OK,
        Json(
            ExecutionBaselineQueryService::new(state.db.clone())
                .response_for_command(&project_id, outcome)
                .await
                .map_err(ApiError::from)?,
        ),
    ))
}

fn validate_idempotency_key(key: &str) -> ApiResult<()> {
    if key.trim().is_empty() {
        return Err(ApiError::bad_request("idempotency_key is required"));
    }
    Ok(())
}

fn baseline_write_operation_name(operation: ExecutionBaselineWriteOperation) -> &'static str {
    match operation {
        ExecutionBaselineWriteOperation::SaveDraft => EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
        ExecutionBaselineWriteOperation::ProposeForApproval => EXECUTION_BASELINE_PROPOSE_COMMAND,
    }
}

fn baseline_authorization(
    authorization: &AuthorizationProvenance,
    user_id: &str,
    operation: &str,
    correlation_id: &str,
) -> ApiResult<ProjectCommandAuthorization> {
    Ok(ProjectCommandAuthorization {
        principal_type: "user".to_owned(),
        principal_id: user_id.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: None,
        policy_digest: None,
        requested_permission: Some(operation.to_owned()),
        correlation_id: correlation_id.to_owned(),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: authorization.event_id.clone(),
        authorization_basis: authorization.authorization_basis.clone(),
        authorization_action: authorization.action.clone(),
        authorization_occurred_at: authorization.occurred_at.clone(),
        authorization_json: serde_json::to_string(authorization)
            .map_err(|error| ApiError::bad_request(error.to_string()))?,
    })
}
