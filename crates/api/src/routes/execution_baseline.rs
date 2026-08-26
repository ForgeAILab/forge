//! Project execution-baseline proposal, approval, and activation routes.
//!
//! A baseline is an immutable, digest-addressed execution contract.  This
//! module is a thin HTTP adapter: it authenticates the request, maps the
//! public envelope into the shared command service, and projects the committed
//! result.  Domain validation, lifecycle transitions, idempotency, receipts,
//! events, and Task-governance promotion live below the route.

use api_types::{
    ApproveAndActivateExecutionBaselineRequest, ApproveAndActivateExecutionBaselineResponse,
    ApproveExecutionBaselineRequest, AuthorizationProvenance, CreateExecutionBaselineRequest,
    ExecutionBaselineLifecycle, ExecutionBaselineResponse, ExecutionBaselineWriteOperation,
    SaveExecutionBaselineRevisionRequest,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use services::{
    ActivateExecutionBaselineCommand, ApproveAndActivateExecutionBaselineCommand,
    ApproveExecutionBaselineCommand, ExecutionBaselineCommandService,
    ExecutionBaselineQueryService, ProjectCommandAuthorization,
    ProposeExecutionBaselineForApprovalCommand, SaveExecutionBaselineDraftCommand,
    EXECUTION_BASELINE_ACTIVATE_COMMAND, EXECUTION_BASELINE_APPROVE_AND_ACTIVATE_COMMAND,
    EXECUTION_BASELINE_APPROVE_COMMAND, EXECUTION_BASELINE_PROPOSE_COMMAND,
    EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
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

/// "Approve plan and start work" (D18, F13). This is the ONLY route for that
/// gesture: one atomic command commits approval, activation, governance
/// promotion, receipt, and events together, so a lost response can only ever
/// be satisfied by replaying the frozen receipt -- never by re-deriving a
/// different outcome from mutable current state. The response is built
/// receipt-first (8.3.2): the identity fields below always come from the
/// committed command outcome, and the full `projection` is a best-effort
/// second read that never turns a successful commit into a reported failure
/// if it cannot be assembled. Before surfacing a conflict, this also checks
/// whether the exact requested revision is already the Project's active
/// baseline -- the same check the web performs, kept here too because a
/// resent request with a changed digest lands on `idempotency_conflict`
/// rather than the plain replay path.
pub async fn approve_and_activate_execution_baseline(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, baseline_id, revision_id)): Path<(String, String, String)>,
    Json(request): Json<ApproveAndActivateExecutionBaselineRequest>,
) -> ApiResult<(
    StatusCode,
    Json<ApproveAndActivateExecutionBaselineResponse>,
)> {
    if request.revision_id != revision_id {
        return Err(ApiError::conflict_with_code(
            "idempotency_conflict",
            "approval target does not match the request path",
        ));
    }
    validate_idempotency_key(&request.mutation.idempotency_key)?;
    let storage_key = scoped_idempotency_key(
        EXECUTION_BASELINE_APPROVE_AND_ACTIVATE_COMMAND,
        &project_id,
        &user.user_id,
        &request.mutation.idempotency_key,
    );
    let authorization = baseline_authorization(
        &request.mutation.authorization,
        &user.user_id,
        EXECUTION_BASELINE_APPROVE_AND_ACTIVATE_COMMAND,
        &storage_key,
    )?;
    let command_outcome = ExecutionBaselineCommandService::new(state.db.clone())
        .approve_and_activate(ApproveAndActivateExecutionBaselineCommand {
            project_id: project_id.clone(),
            baseline_id: baseline_id.clone(),
            revision_id: revision_id.clone(),
            expected_baseline_version: request.expected_baseline_version,
            expected_project_version: request.mutation.expected_version,
            content_digest: request.content_digest.clone(),
            render_digest: request.render_digest.clone(),
            idempotency_key: storage_key,
            authorization,
            action: None,
        })
        .await;

    let outcome = match command_outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            if let Some(response) = already_active_baseline_response(
                &state,
                &user.user_id,
                &project_id,
                &baseline_id,
                &revision_id,
                &request.content_digest,
                &request.render_digest,
            )
            .await
            {
                return Ok((StatusCode::OK, Json(response)));
            }
            return Err(ApiError::from(error));
        }
    };

    let query = ExecutionBaselineQueryService::new(state.db.clone());
    let projection = query
        .response_for_command(&project_id, outcome.clone())
        .await
        .ok();
    let refresh_required = projection.is_none();
    Ok((
        StatusCode::OK,
        Json(ApproveAndActivateExecutionBaselineResponse {
            baseline_id: outcome.baseline_id,
            revision_id: outcome.revision_id.unwrap_or_else(|| revision_id.clone()),
            approval_id: outcome.approval_id.unwrap_or_default(),
            content_digest: outcome
                .content_digest
                .unwrap_or_else(|| request.content_digest.clone()),
            render_digest: outcome
                .render_digest
                .unwrap_or_else(|| request.render_digest.clone()),
            projection,
            refresh_required,
        }),
    ))
}

/// Best-effort receipt-first fallback for a command call that returned an
/// error (typically a version or idempotency conflict). If the exact
/// requested revision is already the Project's active baseline with matching
/// digests, that is success evidence, not a reason to fail the request --
/// this is what lets a retried "Approve plan and start work" click render
/// success instead of the stale-baseline failure F13 reported. Any failure
/// reading current state here is swallowed: the caller falls back to
/// reporting the original command error.
async fn already_active_baseline_response(
    state: &AppState,
    user_id: &str,
    project_id: &str,
    baseline_id: &str,
    revision_id: &str,
    content_digest: &str,
    render_digest: &str,
) -> Option<ApproveAndActivateExecutionBaselineResponse> {
    let projection = ExecutionBaselineQueryService::new(state.db.clone())
        .get(project_id, user_id)
        .await
        .ok()?;
    let is_exact_active = projection.baseline.id == baseline_id
        && projection.baseline.lifecycle == ExecutionBaselineLifecycle::Active
        && projection
            .current_revision
            .as_ref()
            .is_some_and(|revision| {
                revision.id == revision_id
                    && revision.content_digest == content_digest
                    && revision.render_digest == render_digest
            });
    if !is_exact_active {
        return None;
    }
    let approval_id = projection
        .approval
        .as_ref()
        .filter(|approval| approval.revision_id == revision_id)
        .map(|approval| approval.id.clone())
        .unwrap_or_default();
    Some(ApproveAndActivateExecutionBaselineResponse {
        baseline_id: baseline_id.to_owned(),
        revision_id: revision_id.to_owned(),
        approval_id,
        content_digest: content_digest.to_owned(),
        render_digest: render_digest.to_owned(),
        projection: Some(projection),
        refresh_required: false,
    })
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
