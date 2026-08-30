//! Project-scoped Project Document and Decision Log resources.
//!
//! The route layer performs the visibility check before every orchestration
//! query.  IDs supplied by a caller are therefore lookup keys only; they are
//! never treated as authority to cross a Project boundary.

#[cfg(test)]
use api_types::ProjectDocumentContent;
use api_types::{
    ApproveDecisionCandidateRequest, ApproveProjectDocumentRequest, AuthorizationProvenance,
    CreateDecisionCandidateRequest, CreateProjectDocumentRequest, DecisionCandidate,
    DecisionCandidateContext, DecisionCandidateListResponse, DecisionClass, DecisionEditorState,
    DecisionRecord, DecisionRecordListResponse, DecisionRecordState, DocumentRevisionLifecycle,
    PrincipalKind, PrincipalRef, ProjectDocument, ProjectDocumentApproval,
    ProjectDocumentApprovalPolicy, ProjectDocumentKind, ProjectDocumentListResponse,
    ProjectDocumentRevision, ProjectDocumentRevisionDiffResponse,
    ProjectDocumentRevisionListResponse, ProjectDocumentState, RejectDecisionCandidateRequest,
    RevisionProvenance, SaveProjectDocumentRevisionRequest,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use db::{
    ProjectDecisionCandidateRecord, ProjectDecisionRecord, ProjectDocumentRecord,
    ProjectDocumentRevisionRecord, ProjectMemberRepo, ProjectOrchestrationRepo, ProjectRepo,
};
use serde::Deserialize;
use serde_json::Value;
use sqlx::Row;

use crate::{
    errors::{ApiError, ApiResult},
    routes::{auth::AuthenticatedUser, client_idempotency_key},
    state::AppState,
};
use services::{
    ProjectArtifactCommandService, ProjectCommandAuthorization, ProjectDocumentApprovalCommand,
    ProjectDocumentRevisionCommand,
};

const DOCUMENT_CREATE_ACTION: &str = "project.document.create";
const DOCUMENT_REVISION_SAVE_ACTION: &str = "project.document.revision.save";
const DOCUMENT_APPROVE_ACTION: &str = "project.document.approve";
const DECISION_CANDIDATE_CREATE_ACTION: &str = "project.decision.candidate.create";
const DECISION_CANDIDATE_APPROVE_ACTION: &str = "project.decision.candidate.approve";
const DECISION_CANDIDATE_REJECT_ACTION: &str = "project.decision.candidate.reject";

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ProjectArtifactListQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

/// List all typed Project Documents visible to a Project member.
pub async fn list_project_documents(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Query(query): Query<ProjectArtifactListQuery>,
) -> ApiResult<Json<ProjectDocumentListResponse>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let limit = bounded_limit(query.limit);
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let rows = if let Some((updated_at, id)) = cursor.as_ref() {
        sqlx::query(
            "SELECT id FROM project_document
             WHERE project_id = ?
               AND (updated_at < ? OR (updated_at = ? AND id < ?))
             ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(&project_id)
        .bind(updated_at)
        .bind(updated_at)
        .bind(id)
        .bind(limit + 1)
        .fetch_all(state.db.pool())
        .await?
    } else {
        sqlx::query(
            "SELECT id FROM project_document
             WHERE project_id = ?
             ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(&project_id)
        .bind(limit + 1)
        .fetch_all(state.db.pool())
        .await?
    };
    let mut documents = Vec::with_capacity(rows.len().min(limit as usize));
    for row in rows.into_iter().take(limit as usize) {
        let id: String = row.try_get("id")?;
        let document = ProjectOrchestrationRepo::get_project_document(&*state.db, &id)
            .await?
            .ok_or_else(|| ApiError::not_found("project_document", id.clone()))?;
        if document.project_id == project_id {
            documents.push(document_to_api(document)?);
        }
    }
    let has_more = documents.len() == limit as usize
        && has_more_documents(&state, &project_id, &documents).await?;
    let next_cursor = documents
        .last()
        .map(|document| encode_cursor(&document.updated_at, &document.id));
    Ok(Json(ProjectDocumentListResponse {
        items: documents,
        next_cursor: next_cursor.filter(|_| has_more),
        has_more,
    }))
}

pub async fn get_project_document(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, document_id)): Path<(String, String)>,
) -> ApiResult<Json<ProjectDocument>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let document = scoped_document(&state, &project_id, &document_id).await?;
    Ok(Json(document_to_api(document)?))
}

pub async fn create_project_document(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<CreateProjectDocumentRequest>,
) -> ApiResult<(StatusCode, Json<ProjectDocument>)> {
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        DOCUMENT_CREATE_ACTION,
    )?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    let title = required_text(&request.title, "title")?;
    let authorization_json = serde_json::to_string(&request.mutation.authorization)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let document = ProjectArtifactCommandService::new(state.db.clone())
        .create_document(
            services::ProjectDocumentCreateCommand {
                project_id,
                kind: document_kind_name(request.kind).to_owned(),
                title,
                approval_policy: approval_policy_name(request.approval_policy).to_owned(),
                expected_project_version: request.mutation.expected_version,
                idempotency_key: request.mutation.idempotency_key.clone(),
                authorization: ProjectCommandAuthorization {
                    principal_type: "user".to_owned(),
                    principal_id: user.user_id.clone(),
                    policy_result: "allowed".to_owned(),
                    policy_revision: None,
                    policy_digest: None,
                    requested_permission: Some(DOCUMENT_CREATE_ACTION.to_owned()),
                    correlation_id: request.mutation.authorization.event_id.clone(),
                    causation_id: None,
                    causation_depth: 0,
                    authorization_event_id: request.mutation.authorization.event_id.clone(),
                    authorization_basis: request.mutation.authorization.authorization_basis.clone(),
                    authorization_action: request.mutation.authorization.action.clone(),
                    authorization_occurred_at: request.mutation.authorization.occurred_at.clone(),
                    authorization_json,
                },
            },
            None,
        )
        .await?;
    Ok((StatusCode::CREATED, Json(document_to_api(document)?)))
}

pub async fn list_project_document_revisions(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, document_id)): Path<(String, String)>,
    Query(query): Query<ProjectArtifactListQuery>,
) -> ApiResult<Json<ProjectDocumentRevisionListResponse>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let document = scoped_document(&state, &project_id, &document_id).await?;
    let limit = bounded_limit(query.limit);
    let cursor = match decode_cursor(query.cursor.as_deref())? {
        Some((revision, id)) => Some((
            revision
                .parse::<i64>()
                .map_err(|_| ApiError::bad_request("invalid cursor"))?,
            id,
        )),
        None => None,
    };
    let mut statement = String::from(
        "SELECT * FROM project_document_revision
         WHERE document_id = ?",
    );
    if cursor.is_some() {
        statement.push_str(" AND (revision < ? OR (revision = ? AND id < ?))");
    }
    statement.push_str(" ORDER BY revision DESC, id DESC LIMIT ?");
    let mut revision_query = sqlx::query(&statement).bind(&document.id);
    if let Some((revision, id)) = cursor {
        revision_query = revision_query.bind(revision).bind(revision).bind(id);
    }
    let rows = revision_query
        .bind(limit + 1)
        .fetch_all(state.db.pool())
        .await?;
    let has_more = rows.len() > limit as usize;
    let revisions = rows
        .into_iter()
        .take(limit as usize)
        .map(document_revision_record_from_row)
        .map(|record| record.and_then(|record| revision_to_api(&document, record)))
        .collect::<ApiResult<Vec<_>>>()?;
    let next_cursor = revisions
        .last()
        .map(|revision| encode_cursor(&revision.revision_number.to_string(), &revision.id));
    Ok(Json(ProjectDocumentRevisionListResponse {
        items: revisions,
        next_cursor: next_cursor.filter(|_| has_more),
        has_more,
    }))
}

pub async fn get_project_document_revision(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, document_id, revision_id)): Path<(String, String, String)>,
) -> ApiResult<Json<ProjectDocumentRevision>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let document = scoped_document(&state, &project_id, &document_id).await?;
    let revision =
        ProjectOrchestrationRepo::get_project_document_revision(&*state.db, &revision_id)
            .await?
            .filter(|revision| revision.document_id == document.id)
            .ok_or_else(|| ApiError::not_found("project_document_revision", revision_id))?;
    Ok(Json(revision_to_api(&document, revision)?))
}

pub async fn get_project_document_revision_diff(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, document_id, revision_id)): Path<(String, String, String)>,
    Query(query): Query<RevisionDiffQuery>,
) -> ApiResult<Json<ProjectDocumentRevisionDiffResponse>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let document = scoped_document(&state, &project_id, &document_id).await?;
    let target = ProjectOrchestrationRepo::get_project_document_revision(&*state.db, &revision_id)
        .await?
        .filter(|revision| revision.document_id == document.id)
        .ok_or_else(|| ApiError::not_found("project_document_revision", revision_id.clone()))?;
    let base = match query.base_revision_id {
        Some(base_id) => Some(
            ProjectOrchestrationRepo::get_project_document_revision(&*state.db, &base_id)
                .await?
                .filter(|revision| revision.document_id == document.id)
                .ok_or_else(|| ApiError::not_found("project_document_revision", base_id))?,
        ),
        None => match target.base_revision_id.as_deref() {
            Some(base_id) => Some(
                ProjectOrchestrationRepo::get_project_document_revision(&*state.db, base_id)
                    .await?
                    .filter(|revision| revision.document_id == document.id)
                    .ok_or_else(|| {
                        ApiError::not_found("project_document_revision", base_id.to_owned())
                    })?,
            ),
            None => None,
        },
    };
    Ok(Json(ProjectDocumentRevisionDiffResponse {
        document_id: document.id,
        base_revision_id: base.as_ref().map(|revision| revision.id.clone()),
        revision_id: target.id,
        diff: services::diff_project_document_views(
            base.as_ref()
                .map(|revision| revision.rendered_view.as_str()),
            &target.rendered_view,
        ),
    }))
}

pub async fn save_project_document_revision(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, document_id)): Path<(String, String)>,
    Json(request): Json<SaveProjectDocumentRevisionRequest>,
) -> ApiResult<(StatusCode, Json<ProjectDocumentRevision>)> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        DOCUMENT_REVISION_SAVE_ACTION,
    )?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    if request.provenance.author.kind != PrincipalKind::User
        || request.provenance.author.id != user.user_id
    {
        return Err(ApiError::forbidden_with_code(
            "provenance.invalid",
            "HTTP Project Document revisions must be authored by the authenticated user",
        ));
    }
    let authorization_json = serde_json::to_string(&request.mutation.authorization)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let revision = ProjectArtifactCommandService::new(state.db.clone())
        .save_document_revision(
            ProjectDocumentRevisionCommand {
                project_id: project_id.clone(),
                document_id: document_id.clone(),
                kind: None,
                title: None,
                approval_policy: None,
                base_revision_id: request.base_revision_id.clone(),
                lifecycle: request.lifecycle,
                content: request.content.clone(),
                change_summary: request.change_summary.clone(),
                provenance: request.provenance.clone(),
                expected_document_version: request.mutation.expected_version,
                expected_digest: request.mutation.expected_digest.clone(),
                idempotency_key: request.mutation.idempotency_key.clone(),
                authorization: ProjectCommandAuthorization {
                    principal_type: "user".to_owned(),
                    principal_id: user.user_id.clone(),
                    policy_result: "allowed".to_owned(),
                    policy_revision: None,
                    policy_digest: None,
                    requested_permission: Some(DOCUMENT_REVISION_SAVE_ACTION.to_owned()),
                    correlation_id: request.mutation.authorization.event_id.clone(),
                    causation_id: None,
                    causation_depth: 0,
                    authorization_event_id: request.mutation.authorization.event_id.clone(),
                    authorization_basis: request.mutation.authorization.authorization_basis.clone(),
                    authorization_action: request.mutation.authorization.action.clone(),
                    authorization_occurred_at: request.mutation.authorization.occurred_at.clone(),
                    authorization_json,
                },
            },
            None,
        )
        .await?;
    let document = scoped_document(&state, &project_id, &document_id).await?;
    Ok((
        StatusCode::CREATED,
        Json(revision_to_api(&document, revision)?),
    ))
}

pub async fn approve_project_document(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, document_id)): Path<(String, String)>,
    Json(request): Json<ApproveProjectDocumentRequest>,
) -> ApiResult<(StatusCode, Json<ProjectDocumentApproval>)> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    if request.document_id != document_id {
        return Err(ApiError::bad_request(
            "the approval document_id must match the path",
        ));
    }
    let authorization_json = serde_json::to_string(&request.mutation.authorization)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let command = ProjectDocumentApprovalCommand {
        project_id: project_id.clone(),
        document_id: document_id.clone(),
        revision_id: request.revision_id.clone(),
        content_digest: request.content_digest.clone(),
        rendered_digest: request.render_digest.clone(),
        expected_document_version: request.mutation.expected_version,
        idempotency_key: request.mutation.idempotency_key.clone(),
        authorization: ProjectCommandAuthorization {
            principal_type: "user".to_owned(),
            principal_id: user.user_id.clone(),
            policy_result: "allowed".to_owned(),
            policy_revision: None,
            policy_digest: None,
            requested_permission: Some(DOCUMENT_APPROVE_ACTION.to_owned()),
            correlation_id: request.mutation.authorization.event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            authorization_event_id: request.mutation.authorization.event_id.clone(),
            authorization_basis: request.mutation.authorization.authorization_basis.clone(),
            authorization_action: request.mutation.authorization.action.clone(),
            authorization_occurred_at: request.mutation.authorization.occurred_at.clone(),
            authorization_json,
        },
    };
    let service = ProjectArtifactCommandService::new(state.db.clone());
    if let Some(approval) = service
        .replay_document_approval_if_present(&command)
        .await?
    {
        return Ok((
            StatusCode::OK,
            Json(approval_to_api(
                approval,
                request.mutation.expected_version,
            )?),
        ));
    }
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        DOCUMENT_APPROVE_ACTION,
    )?;
    let (approval, replayed) = service.approve_document_with_status(command, None).await?;
    Ok((
        if replayed {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(approval_to_api(
            approval,
            request.mutation.expected_version,
        )?),
    ))
}

pub async fn list_decision_candidates(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Query(query): Query<ProjectArtifactListQuery>,
) -> ApiResult<Json<DecisionCandidateListResponse>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let limit = bounded_limit(query.limit);
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let mut statement = String::from(
        "SELECT * FROM project_decision_candidate
         WHERE project_id = ?",
    );
    if cursor.is_some() {
        statement.push_str(" AND (created_at < ? OR (created_at = ? AND id < ?))");
    }
    statement.push_str(" ORDER BY created_at DESC, id DESC LIMIT ?");
    let mut candidate_query = sqlx::query(&statement).bind(&project_id);
    if let Some((created_at, id)) = cursor {
        candidate_query = candidate_query
            .bind(created_at.clone())
            .bind(created_at)
            .bind(id);
    }
    let rows = candidate_query
        .bind(limit + 1)
        .fetch_all(state.db.pool())
        .await?;
    let has_more = rows.len() > limit as usize;
    let candidates = rows
        .into_iter()
        .take(limit as usize)
        .map(candidate_record_from_row)
        .map(|record| record.and_then(candidate_to_api))
        .collect::<ApiResult<Vec<_>>>()?;
    let next_cursor = candidates
        .last()
        .map(|candidate| encode_cursor(&candidate.created_at, &candidate.id))
        .filter(|_| has_more);
    Ok(Json(DecisionCandidateListResponse {
        items: candidates,
        next_cursor,
        has_more,
    }))
}

pub async fn create_decision_candidate(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<CreateDecisionCandidateRequest>,
) -> ApiResult<(StatusCode, Json<DecisionCandidate>)> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        DECISION_CANDIDATE_CREATE_ACTION,
    )?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    let question = required_text(&request.question, "question")?;
    let authorization = decision_authorization(
        &request.mutation.authorization,
        &user.user_id,
        DECISION_CANDIDATE_CREATE_ACTION,
    )?;
    let record = services::ProjectDecisionCommandService::new(state.db.clone())
        .create_candidate(
            services::ProjectDecisionCandidateCommand {
                project_id,
                question,
                context: request.context,
                options: request.options,
                selected_outcome: request.selected_outcome,
                rationale: request.rationale,
                decision_class: request.decision_class,
                source_refs: request.source_refs,
                expected_project_version: request.mutation.expected_version,
                reconciliation_reason: None,
                idempotency_key: request.mutation.idempotency_key,
                authorization,
            },
            None,
        )
        .await?;
    Ok((StatusCode::CREATED, Json(candidate_to_api(record)?)))
}

pub async fn get_decision_candidate(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, candidate_id)): Path<(String, String)>,
) -> ApiResult<Json<DecisionCandidate>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let candidate =
        ProjectOrchestrationRepo::get_project_decision_candidate(&*state.db, &candidate_id)
            .await?
            .filter(|candidate| candidate.project_id == project_id)
            .ok_or_else(|| ApiError::not_found("decision_candidate", candidate_id))?;
    Ok(Json(candidate_to_api(candidate)?))
}

pub async fn approve_decision_candidate(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, candidate_id)): Path<(String, String)>,
    Json(request): Json<ApproveDecisionCandidateRequest>,
) -> ApiResult<(StatusCode, Json<DecisionRecord>)> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        DECISION_CANDIDATE_APPROVE_ACTION,
    )?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    let authorization = decision_authorization(
        &request.mutation.authorization,
        &user.user_id,
        DECISION_CANDIDATE_APPROVE_ACTION,
    )?;
    let record = services::ProjectDecisionCommandService::new(state.db.clone())
        .approve_candidate(
            services::ProjectDecisionApprovalCommand {
                project_id,
                candidate_id,
                expected_project_version: request.mutation.expected_version,
                idempotency_key: request.mutation.idempotency_key,
                authorization,
            },
            None,
        )
        .await?;
    Ok((StatusCode::CREATED, Json(decision_to_api(record)?)))
}

pub async fn reject_decision_candidate(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, candidate_id)): Path<(String, String)>,
    Json(request): Json<RejectDecisionCandidateRequest>,
) -> ApiResult<(StatusCode, Json<DecisionCandidate>)> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        DECISION_CANDIDATE_REJECT_ACTION,
    )?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    let reason = required_text(&request.reason, "reason")?;
    let authorization = decision_authorization(
        &request.mutation.authorization,
        &user.user_id,
        DECISION_CANDIDATE_REJECT_ACTION,
    )?;
    let candidate = services::ProjectDecisionCommandService::new(state.db.clone())
        .reject_candidate(
            services::ProjectDecisionRejectionCommand {
                project_id,
                candidate_id,
                reason,
                expected_project_version: request.mutation.expected_version,
                idempotency_key: request.mutation.idempotency_key,
                authorization,
            },
            None,
        )
        .await?;
    Ok((StatusCode::OK, Json(candidate_to_api(candidate)?)))
}

pub async fn list_decisions(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Query(query): Query<ProjectArtifactListQuery>,
) -> ApiResult<Json<DecisionRecordListResponse>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let limit = bounded_limit(query.limit);
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let mut statement = String::from(
        "SELECT * FROM project_decision
         WHERE project_id = ?",
    );
    if cursor.is_some() {
        statement.push_str(" AND (created_at < ? OR (created_at = ? AND id < ?))");
    }
    statement.push_str(" ORDER BY created_at DESC, id DESC LIMIT ?");
    let mut decision_query = sqlx::query(&statement).bind(&project_id);
    if let Some((created_at, id)) = cursor {
        decision_query = decision_query
            .bind(created_at.clone())
            .bind(created_at)
            .bind(id);
    }
    let rows = decision_query
        .bind(limit + 1)
        .fetch_all(state.db.pool())
        .await?;
    let has_more = rows.len() > limit as usize;
    let mut records = rows
        .into_iter()
        .take(limit as usize)
        .map(decision_record_from_row)
        .collect::<ApiResult<Vec<_>>>()?;
    for record in &mut records {
        // A replacement can fall on a later keyset page.  Derive effective
        // state from the full Project-scoped append-only log rather than only
        // the current page, otherwise an old record would briefly appear
        // active while its replacement is paginated out.
        let replacement_state: Option<String> = sqlx::query_scalar(
            "SELECT state FROM project_decision
             WHERE project_id = ? AND supersedes_decision_id = ?
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(&project_id)
        .bind(&record.id)
        .fetch_optional(state.db.pool())
        .await?;
        record.state = effective_decision_state(&record.state, replacement_state.as_deref());
    }
    let records = records
        .into_iter()
        .map(decision_to_api)
        .collect::<ApiResult<Vec<_>>>()?;
    let next_cursor = records
        .last()
        .map(|record| encode_cursor(&record.created_at, &record.id))
        .filter(|_| has_more);
    Ok(Json(DecisionRecordListResponse {
        items: records,
        next_cursor: next_cursor.filter(|_| has_more),
        has_more,
    }))
}

pub async fn get_decision(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, decision_id)): Path<(String, String)>,
) -> ApiResult<Json<DecisionRecord>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let mut record = get_decision_record(&state, &project_id, &decision_id).await?;
    let replaced_state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM project_decision
         WHERE project_id = ? AND supersedes_decision_id = ?
         ORDER BY created_at DESC, id DESC LIMIT 1",
    )
    .bind(&project_id)
    .bind(&decision_id)
    .fetch_optional(state.db.pool())
    .await?;
    if record.state == "active" {
        record.state = effective_decision_state(&record.state, replaced_state.as_deref());
    }
    Ok(Json(decision_to_api(record)?))
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RevisionDiffQuery {
    pub base_revision_id: Option<String>,
}

async fn require_project_access(
    state: &AppState,
    project_id: &str,
    user_id: &str,
) -> ApiResult<()> {
    let project = ProjectRepo::get_by_id(&*state.db, project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    if project.owner_id.as_deref() == Some(user_id) || project.owner_id.is_none() {
        return Ok(());
    }
    ProjectMemberRepo::get_member(&*state.db, project_id, user_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    Ok(())
}

async fn scoped_document(
    state: &AppState,
    project_id: &str,
    document_id: &str,
) -> ApiResult<ProjectDocumentRecord> {
    let document = ProjectOrchestrationRepo::get_project_document(&*state.db, document_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project_document", document_id.to_owned()))?;
    if document.project_id != project_id {
        return Err(ApiError::not_found(
            "project_document",
            document_id.to_owned(),
        ));
    }
    Ok(document)
}

async fn has_more_documents(
    state: &AppState,
    project_id: &str,
    documents: &[ProjectDocument],
) -> ApiResult<bool> {
    let Some(last) = documents.last() else {
        return Ok(false);
    };
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_document
         WHERE project_id = ? AND (updated_at < ? OR (updated_at = ? AND id < ?))",
    )
    .bind(project_id)
    .bind(&last.updated_at)
    .bind(&last.updated_at)
    .bind(&last.id)
    .fetch_one(state.db.pool())
    .await?;
    Ok(count > 0)
}

fn bounded_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(20).clamp(1, 100)
}

fn encode_cursor(timestamp: &str, id: &str) -> String {
    hex::encode(format!("{timestamp}\0{id}"))
}

fn decode_cursor(value: Option<&str>) -> ApiResult<Option<(String, String)>> {
    let Some(value) = value else { return Ok(None) };
    let bytes = hex::decode(value).map_err(|_| ApiError::bad_request("invalid cursor"))?;
    let decoded = String::from_utf8(bytes).map_err(|_| ApiError::bad_request("invalid cursor"))?;
    let (timestamp, id) = decoded
        .split_once('\0')
        .ok_or_else(|| ApiError::bad_request("invalid cursor"))?;
    if timestamp.is_empty() || id.is_empty() {
        return Err(ApiError::bad_request("invalid cursor"));
    }
    Ok(Some((timestamp.to_owned(), id.to_owned())))
}

fn validate_authorization(
    authorization: &AuthorizationProvenance,
    user_id: &str,
    expected_action: &str,
) -> ApiResult<()> {
    if authorization.principal.kind != PrincipalKind::User
        || authorization.principal.id != user_id
        || authorization.action != expected_action
        || authorization.authorization_basis.trim().is_empty()
        || authorization.event_id.trim().is_empty()
        || authorization.occurred_at.trim().is_empty()
    {
        return Err(ApiError::forbidden_with_code(
            "authorization.invalid",
            "the mutation requires an explicit authenticated Project-scoped user authorization event",
        ));
    }
    Ok(())
}

fn decision_authorization(
    authorization: &AuthorizationProvenance,
    user_id: &str,
    action: &str,
) -> ApiResult<ProjectCommandAuthorization> {
    Ok(ProjectCommandAuthorization {
        principal_type: "user".to_owned(),
        principal_id: user_id.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: None,
        policy_digest: None,
        requested_permission: Some(action.to_owned()),
        correlation_id: authorization.event_id.clone(),
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

fn require_idempotency_key(value: &str) -> ApiResult<()> {
    if value.trim().is_empty() {
        return Err(ApiError::bad_request(
            "mutation.idempotency_key is required",
        ));
    }
    Ok(())
}

fn required_text(value: &str, field: &str) -> ApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ApiError::bad_request(format!("{field} is required")));
    }
    Ok(value.to_owned())
}

fn document_kind_name(kind: ProjectDocumentKind) -> &'static str {
    services::document_kind_name(kind)
}

fn approval_policy_name(policy: ProjectDocumentApprovalPolicy) -> &'static str {
    match policy {
        ProjectDocumentApprovalPolicy::None => "none",
        ProjectDocumentApprovalPolicy::ProjectAgent => "project_agent",
        ProjectDocumentApprovalPolicy::User => "user",
        ProjectDocumentApprovalPolicy::UserOrProjectAgent => "user_or_project_agent",
    }
}

fn document_to_api(record: ProjectDocumentRecord) -> ApiResult<ProjectDocument> {
    Ok(ProjectDocument {
        id: record.id,
        project_id: record.project_id,
        kind: services::parse_document_kind(&record.kind)
            .ok_or_else(|| ApiError::internal("invalid persisted Project Document kind"))?,
        title: record.title,
        state: if record.lifecycle == "archived" {
            ProjectDocumentState::Archived
        } else {
            ProjectDocumentState::Active
        },
        approval_required: record.approval_policy != "none",
        current_draft_revision_id: record.current_draft_revision_id,
        current_approved_revision_id: record.current_approved_revision_id,
        version: record.version,
        created_at: record.created_at,
        updated_at: record.updated_at,
    })
}

fn revision_to_api(
    document: &ProjectDocumentRecord,
    record: ProjectDocumentRevisionRecord,
) -> ApiResult<ProjectDocumentRevision> {
    let content = serde_json::from_str(&record.content_json)
        .map_err(|_| ApiError::internal("invalid persisted Project Document content"))?;
    let source_refs = serde_json::from_str(&record.source_refs_json)
        .map_err(|_| ApiError::internal("invalid persisted Project Document provenance"))?;
    Ok(ProjectDocumentRevision {
        id: record.id,
        document_id: record.document_id,
        project_id: document.project_id.clone(),
        revision_number: record.revision,
        // The numeric revision is a display/order value only.  References
        // must use the immutable revision UUID so a revision can never be
        // mistaken for another Document's ordinal.
        base_revision_id: record.base_revision_id,
        lifecycle: parse_revision_lifecycle(&record.lifecycle)?,
        schema_version: record.schema_version,
        content,
        rendered_view: record.rendered_view,
        render_version: record.render_version,
        content_digest: record.content_digest,
        render_digest: record.rendered_digest,
        provenance: RevisionProvenance {
            author: PrincipalRef {
                kind: parse_principal_kind_strict(&record.author_type)?,
                id: record.author_id.ok_or_else(|| {
                    ApiError::internal("Project Document revision is missing its author")
                })?,
                display_name: None,
            },
            profile_revision: None,
            operating_skill_revision: None,
            source_refs,
            change_summary: record.change_summary,
            material_diff: None,
        },
        approved_at: (record.lifecycle == "approved").then(|| record.created_at.clone()),
        superseded_by_revision_id: (record.lifecycle == "superseded")
            .then(|| document.current_approved_revision_id.clone())
            .flatten(),
        created_at: record.created_at,
    })
}

fn approval_to_api(
    record: db::ProjectDocumentApprovalRecord,
    expected_version: i64,
) -> ApiResult<ProjectDocumentApproval> {
    let principal = PrincipalRef {
        kind: parse_principal_kind_strict(&record.principal_type)?,
        id: record.principal_id.clone(),
        display_name: None,
    };
    Ok(ProjectDocumentApproval {
        id: record.id,
        document_id: record.document_id,
        revision_id: record.revision_id,
        content_digest: record.content_digest,
        render_digest: record.rendered_digest,
        expected_document_version: expected_version,
        approved_by: principal.clone(),
        authorization: AuthorizationProvenance {
            principal,
            authorization_basis: record.authorization_basis,
            action: record.authorization_action,
            event_id: record.explicit_event,
            occurred_at: record.authorization_occurred_at,
        },
        approved_at: record.created_at,
        idempotency_key: client_idempotency_key(&record.idempotency_key),
    })
}

fn candidate_to_api(record: ProjectDecisionCandidateRecord) -> ApiResult<DecisionCandidate> {
    let mut context: Value = serde_json::from_str(&record.context_json)
        .map_err(|_| ApiError::internal("invalid persisted decision candidate context"))?;
    let options = serde_json::from_str(&record.options_json)
        .map_err(|_| ApiError::internal("invalid persisted decision candidate options"))?;
    let decision_class = context
        .get("decision_class")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::internal("decision candidate is missing its decision class"))?
        .to_owned();
    let rejection_reason = context
        .get("rejection_reason")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if let Some(object) = context.as_object_mut() {
        object.remove("decision_class");
        object.remove("rejection_reason");
    }
    serde_json::from_value::<DecisionCandidateContext>(context)
        .map_err(|_| ApiError::internal("invalid persisted decision candidate context"))?;
    let principal_type = record
        .principal_type
        .ok_or_else(|| ApiError::internal("decision candidate is missing its principal type"))?;
    let principal_id = record
        .principal_id
        .ok_or_else(|| ApiError::internal("decision candidate is missing its principal"))?;
    Ok(DecisionCandidate {
        id: record.id,
        project_id: record.project_id,
        editor_state: parse_candidate_state(&record.lifecycle)?,
        question: record.question,
        options,
        selected_outcome: record.selected_outcome,
        rationale: record.rationale,
        proposed_by: PrincipalRef {
            kind: parse_principal_kind_strict(&principal_type)?,
            id: principal_id,
            display_name: None,
        },
        decision_class: parse_decision_class(&decision_class)?,
        rejection_reason,
        effective_decision_id: record.effective_decision_id,
        created_at: record.created_at,
        updated_at: record.updated_at,
    })
}

fn decision_to_api(record: ProjectDecisionRecord) -> ApiResult<DecisionRecord> {
    let options = serde_json::from_str(&record.options_json)
        .map_err(|_| ApiError::internal("invalid persisted Decision options"))?;
    let affected: Value = serde_json::from_str(&record.affected_records_json)
        .map_err(|_| ApiError::internal("invalid persisted Decision affected records"))?;
    let affected_artifact_refs = affected
        .get("artifact_refs")
        .cloned()
        .ok_or_else(|| ApiError::internal("Decision is missing affected artifact references"))
        .and_then(|value| {
            serde_json::from_value(value)
                .map_err(|_| ApiError::internal("invalid Decision affected artifact references"))
        })?;
    let affected_task_ids = affected
        .get("task_ids")
        .cloned()
        .ok_or_else(|| ApiError::internal("Decision is missing affected Task IDs"))
        .and_then(|value| {
            serde_json::from_value(value)
                .map_err(|_| ApiError::internal("invalid Decision affected Task IDs"))
        })?;
    let affected_milestone_ids = affected
        .get("milestone_ids")
        .cloned()
        .ok_or_else(|| ApiError::internal("Decision is missing affected milestone IDs"))
        .and_then(|value| {
            serde_json::from_value(value)
                .map_err(|_| ApiError::internal("invalid Decision affected milestone IDs"))
        })?;
    let provenance = serde_json::from_str(&record.source_refs_json)
        .map_err(|_| ApiError::internal("invalid persisted Decision provenance"))?;
    serde_json::from_str::<Value>(&record.context_json)
        .map_err(|_| ApiError::internal("invalid persisted Decision context"))?;
    Ok(DecisionRecord {
        id: record.id,
        project_id: record.project_id,
        state: parse_decision_state(&record.state)?,
        question: record.question,
        context: Some(record.context_json),
        options,
        selected_outcome: record.selected_outcome,
        rationale: record.rationale,
        decision_maker: PrincipalRef {
            kind: parse_principal_kind_strict(&record.principal_type)?,
            id: record.principal_id,
            display_name: None,
        },
        decision_class: parse_decision_class(&record.decision_class)?,
        authority_basis: Some(record.authority_basis),
        affected_artifact_refs,
        affected_task_ids,
        affected_milestone_ids,
        supersedes_id: record.supersedes_decision_id,
        provenance,
        created_at: record.created_at.clone(),
        effective_at: record.created_at,
    })
}

async fn get_decision_record(
    state: &AppState,
    project_id: &str,
    decision_id: &str,
) -> ApiResult<ProjectDecisionRecord> {
    let row = sqlx::query("SELECT * FROM project_decision WHERE id = ? AND project_id = ?")
        .bind(decision_id)
        .bind(project_id)
        .fetch_optional(state.db.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("decision", decision_id.to_owned()))?;
    decision_record_from_row(row)
}

fn decision_record_from_row(row: sqlx::sqlite::SqliteRow) -> ApiResult<ProjectDecisionRecord> {
    Ok(ProjectDecisionRecord {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        state: row.try_get("state")?,
        decision_class: row.try_get("decision_class")?,
        question: row.try_get("question")?,
        context_json: row.try_get("context_json")?,
        options_json: row.try_get("options_json")?,
        selected_outcome: row.try_get("selected_outcome")?,
        rationale: row.try_get("rationale")?,
        principal_type: row.try_get("principal_type")?,
        principal_id: row.try_get("principal_id")?,
        authority_basis: row.try_get("authority_basis")?,
        authorization_action: row.try_get("authorization_action")?,
        explicit_event: row.try_get("explicit_event")?,
        authorization_occurred_at: row.try_get("authorization_occurred_at")?,
        charter_revision_id: row.try_get("charter_revision_id")?,
        source_refs_json: row.try_get("source_refs_json")?,
        affected_records_json: row.try_get("affected_records_json")?,
        supersedes_decision_id: row.try_get("supersedes_decision_id")?,
        created_at: row.try_get("created_at")?,
    })
}

fn document_revision_record_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> ApiResult<ProjectDocumentRevisionRecord> {
    Ok(ProjectDocumentRevisionRecord {
        id: row.try_get("id")?,
        document_id: row.try_get("document_id")?,
        revision: row.try_get("revision")?,
        base_revision: row.try_get("base_revision")?,
        base_revision_id: row.try_get("base_revision_id")?,
        lifecycle: row.try_get("lifecycle")?,
        schema_version: row.try_get("schema_version")?,
        render_version: row.try_get("render_version")?,
        content_json: row.try_get("content_json")?,
        rendered_view: row.try_get("rendered_view")?,
        change_summary: row.try_get("change_summary")?,
        author_type: row.try_get("author_type")?,
        author_id: row.try_get("author_id")?,
        source_refs_json: row.try_get("source_refs_json")?,
        content_digest: row.try_get("content_digest")?,
        rendered_digest: row.try_get("rendered_digest")?,
        created_at: row.try_get("created_at")?,
    })
}

fn candidate_record_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> ApiResult<ProjectDecisionCandidateRecord> {
    Ok(ProjectDecisionCandidateRecord {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        lifecycle: row.try_get("lifecycle")?,
        question: row.try_get("question")?,
        context_json: row.try_get("context_json")?,
        options_json: row.try_get("options_json")?,
        selected_outcome: row.try_get("selected_outcome")?,
        rationale: row.try_get("rationale")?,
        principal_type: row.try_get("principal_type")?,
        principal_id: row.try_get("principal_id")?,
        source_refs_json: row.try_get("source_refs_json")?,
        expected_project_version: row.try_get("expected_project_version")?,
        effective_decision_id: row.try_get("effective_decision_id")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn parse_candidate_state(value: &str) -> ApiResult<DecisionEditorState> {
    match value {
        "draft" => Ok(DecisionEditorState::Draft),
        "proposed" => Ok(DecisionEditorState::Proposed),
        "approved" => Ok(DecisionEditorState::Approved),
        "rejected" => Ok(DecisionEditorState::Rejected),
        _ => Err(ApiError::internal(
            "invalid persisted decision candidate state",
        )),
    }
}

fn parse_revision_lifecycle(value: &str) -> ApiResult<DocumentRevisionLifecycle> {
    match value {
        "draft" => Ok(DocumentRevisionLifecycle::Draft),
        "proposed" => Ok(DocumentRevisionLifecycle::Proposed),
        "approved" => Ok(DocumentRevisionLifecycle::Approved),
        "rejected" => Ok(DocumentRevisionLifecycle::Rejected),
        "withdrawn" => Ok(DocumentRevisionLifecycle::Withdrawn),
        "superseded" => Ok(DocumentRevisionLifecycle::Superseded),
        _ => Err(ApiError::internal(
            "invalid persisted Project Document revision state",
        )),
    }
}

fn parse_decision_state(value: &str) -> ApiResult<DecisionRecordState> {
    match value {
        "active" => Ok(DecisionRecordState::Active),
        "superseded" => Ok(DecisionRecordState::Superseded),
        "invalidated" => Ok(DecisionRecordState::Invalidated),
        _ => Err(ApiError::internal("invalid persisted Decision Log state")),
    }
}

fn effective_decision_state(current: &str, replacement_state: Option<&str>) -> String {
    if current != "active" {
        return current.to_owned();
    }
    match replacement_state {
        Some("invalidated") => "invalidated".to_owned(),
        Some(_) => "superseded".to_owned(),
        None => "active".to_owned(),
    }
}

fn parse_decision_class(value: &str) -> ApiResult<DecisionClass> {
    match value {
        "user_scope" => Ok(DecisionClass::UserScope),
        "project_implementation" => Ok(DecisionClass::ProjectImplementation),
        "policy" => Ok(DecisionClass::Policy),
        "waiver" => Ok(DecisionClass::Waiver),
        _ => Err(ApiError::internal("invalid persisted Decision Log class")),
    }
}

fn parse_principal_kind_strict(value: &str) -> ApiResult<PrincipalKind> {
    match value {
        "user" => Ok(PrincipalKind::User),
        "agent" => Ok(PrincipalKind::Agent),
        "worker" => Ok(PrincipalKind::Worker),
        "reviewer" => Ok(PrincipalKind::Reviewer),
        "service" => Ok(PrincipalKind::Service),
        "system" => Ok(PrincipalKind::System),
        _ => Err(ApiError::internal("invalid persisted principal kind")),
    }
}

fn map_sql_error(error: sqlx::Error) -> ApiError {
    tracing::error!(error = %error, "Project artifact mutation failed");
    ApiError::internal("Project artifact mutation failed")
}

impl From<sqlx::Error> for ApiError {
    fn from(error: sqlx::Error) -> Self {
        map_sql_error(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_are_opaque_and_round_trip() {
        let cursor = encode_cursor("2026-08-13T12:00:00Z", "revision-17");
        assert_ne!(cursor, "2026-08-13T12:00:00Z\0revision-17");
        assert_eq!(
            decode_cursor(Some(&cursor)).unwrap(),
            Some(("2026-08-13T12:00:00Z".to_owned(), "revision-17".to_owned()))
        );
        assert!(decode_cursor(Some("not-a-cursor")).is_err());
    }

    #[test]
    fn revision_cursor_uses_exact_immutable_id_and_numeric_revision() {
        let cursor = encode_cursor("12", "revision-12");
        let (revision, id) = decode_cursor(Some(&cursor)).unwrap().unwrap();
        assert_eq!(revision.parse::<i64>().unwrap(), 12);
        assert_eq!(id, "revision-12");
    }

    #[test]
    fn empty_idempotency_keys_are_rejected() {
        assert!(require_idempotency_key(" \t").is_err());
        assert!(require_idempotency_key("mutation-1").is_ok());
    }

    #[test]
    fn artifact_queries_reject_unknown_fields() {
        assert!(
            serde_json::from_value::<ProjectArtifactListQuery>(serde_json::json!({
                "limit": 20,
                "unexpected": "must fail",
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<RevisionDiffQuery>(serde_json::json!({
                "base_revision_id": "revision-1",
                "unexpected": "must fail",
            }))
            .is_err()
        );
    }

    #[test]
    fn revision_response_preserves_exact_base_revision_id() {
        let document = ProjectDocumentRecord {
            id: "document-1".to_owned(),
            project_id: "project-1".to_owned(),
            kind: "research".to_owned(),
            title: "Research".to_owned(),
            lifecycle: "draft".to_owned(),
            approval_policy: "user".to_owned(),
            current_draft_revision_id: Some("revision-2".to_owned()),
            current_approved_revision_id: None,
            version: 2,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
            updated_at: "2026-08-13T00:00:00Z".to_owned(),
        };
        let revision = ProjectDocumentRevisionRecord {
            id: "revision-2".to_owned(),
            document_id: document.id.clone(),
            revision: 2,
            base_revision: 1,
            base_revision_id: Some("immutable-base-uuid".to_owned()),
            lifecycle: "draft".to_owned(),
            schema_version: services::PROJECT_DOCUMENT_SCHEMA_VERSION.to_owned(),
            render_version: services::PROJECT_DOCUMENT_RENDER_VERSION.to_owned(),
            content_json: serde_json::to_string(&ProjectDocumentContent::Research(
                api_types::ResearchDocumentContent {
                    question: "question".to_owned(),
                    decision_informed: "decision".to_owned(),
                    scope: "scope".to_owned(),
                    stopping_condition: "stop".to_owned(),
                    sources: Vec::new(),
                    findings: Vec::new(),
                    evidence: Vec::new(),
                    inferences: Vec::new(),
                    alternatives: Vec::new(),
                    recommendation: None,
                    uncertainty: Vec::new(),
                    unresolved_questions: Vec::new(),
                    affected_artifact_ids: Vec::new(),
                    affected_decision_ids: Vec::new(),
                },
            ))
            .unwrap(),
            rendered_view: "# Research".to_owned(),
            change_summary: "update".to_owned(),
            author_type: "user".to_owned(),
            author_id: Some("user-1".to_owned()),
            source_refs_json: "[]".to_owned(),
            content_digest: "content".to_owned(),
            rendered_digest: "render".to_owned(),
            created_at: "2026-08-13T00:01:00Z".to_owned(),
        };
        let response = revision_to_api(&document, revision).unwrap();
        assert_eq!(
            response.base_revision_id.as_deref(),
            Some("immutable-base-uuid")
        );
    }

    #[test]
    fn effective_decision_state_derives_append_only_replacement() {
        assert_eq!(effective_decision_state("active", None), "active");
        assert_eq!(
            effective_decision_state("active", Some("active")),
            "superseded"
        );
        assert_eq!(
            effective_decision_state("active", Some("invalidated")),
            "invalidated"
        );
        assert_eq!(
            effective_decision_state("superseded", Some("active")),
            "superseded"
        );
    }

    #[test]
    fn candidate_mapper_fails_closed_on_corrupt_authority_json() {
        let record = ProjectDecisionCandidateRecord {
            id: "candidate-1".to_owned(),
            project_id: "project-1".to_owned(),
            lifecycle: "proposed".to_owned(),
            question: "Which option?".to_owned(),
            context_json: r#"{"decision_class":"project_implementation"}"#.to_owned(),
            options_json: "not-json".to_owned(),
            selected_outcome: None,
            rationale: None,
            principal_type: Some("agent".to_owned()),
            principal_id: Some("agent-1".to_owned()),
            source_refs_json: "[]".to_owned(),
            expected_project_version: 1,
            effective_decision_id: None,
            version: 1,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
            updated_at: "2026-08-13T00:00:00Z".to_owned(),
        };
        assert!(candidate_to_api(record).is_err());
    }

    #[test]
    fn decision_mapper_fails_closed_on_corrupt_affected_records() {
        let record = ProjectDecisionRecord {
            id: "decision-1".to_owned(),
            project_id: "project-1".to_owned(),
            state: "active".to_owned(),
            decision_class: "project_implementation".to_owned(),
            question: "Which option?".to_owned(),
            context_json: "{}".to_owned(),
            options_json: "[]".to_owned(),
            selected_outcome: "one".to_owned(),
            rationale: "because".to_owned(),
            principal_type: "agent".to_owned(),
            principal_id: "agent-1".to_owned(),
            authority_basis: "baseline".to_owned(),
            authorization_action: "project.decision.record_effective".to_owned(),
            explicit_event: "event-1".to_owned(),
            authorization_occurred_at: "2026-08-13T00:00:00Z".to_owned(),
            charter_revision_id: None,
            source_refs_json: "[]".to_owned(),
            affected_records_json: "{}".to_owned(),
            supersedes_decision_id: None,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
        };
        assert!(decision_to_api(record).is_err());
    }
}
