//! Project-scoped Project Document and Decision Log resources.
//!
//! The route layer performs the visibility check before every orchestration
//! query.  IDs supplied by a caller are therefore lookup keys only; they are
//! never treated as authority to cross a Project boundary.

use api_types::{
    ApproveDecisionCandidateRequest, ApproveProjectDocumentRequest, AuthorizationProvenance,
    CreateDecisionCandidateRequest, CreateProjectDocumentRequest, DecisionCandidate,
    DecisionCandidateContext, DecisionCandidateListResponse, DecisionClass, DecisionEditorState,
    DecisionRecord, DecisionRecordListResponse, DecisionRecordState, DocumentRevisionLifecycle,
    PrincipalKind, PrincipalRef, ProjectDocument, ProjectDocumentApproval,
    ProjectDocumentApprovalPolicy, ProjectDocumentContent, ProjectDocumentKind,
    ProjectDocumentListResponse, ProjectDocumentRevision, ProjectDocumentRevisionDiffResponse,
    ProjectDocumentRevisionListResponse, ProjectDocumentState, RejectDecisionCandidateRequest,
    RevisionProvenance, SaveProjectDocumentRevisionRequest,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use db::{
    new_uuid_v4, now_rfc3339, CreateDomainEvent, DomainEventRepo, ProjectDecisionCandidateRecord,
    ProjectDecisionRecord, ProjectDocumentRecord, ProjectDocumentRevisionRecord, ProjectMemberRepo,
    ProjectOrchestrationRepo, ProjectRepo,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, Transaction};

use crate::{
    errors::{ApiError, ApiResult},
    routes::{auth::AuthenticatedUser, client_idempotency_key, scoped_idempotency_key},
    state::AppState,
};

const DOCUMENT_CREATE_ACTION: &str = "project.document.create";
const DOCUMENT_REVISION_SAVE_ACTION: &str = "project.document.revision.save";
const DOCUMENT_APPROVE_ACTION: &str = "project.document.approve";
const DECISION_CANDIDATE_CREATE_ACTION: &str = "project.decision.candidate.create";
const DECISION_CANDIDATE_APPROVE_ACTION: &str = "project.decision.candidate.approve";
const DECISION_CANDIDATE_REJECT_ACTION: &str = "project.decision.candidate.reject";
const DOCUMENT_SCHEMA_VERSION: &str = services::PROJECT_DOCUMENT_SCHEMA_VERSION;
const DOCUMENT_RENDER_VERSION: &str = services::PROJECT_DOCUMENT_RENDER_VERSION;

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
    require_project_access(&state, &project_id, &user.user_id).await?;
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        DOCUMENT_CREATE_ACTION,
    )?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    let title = required_text(&request.title, "title")?;
    let dedupe_key = format!(
        "project.document.create:{project_id}:{}",
        request.mutation.idempotency_key
    );
    if let Some(event) = DomainEventRepo::get_event_by_dedupe(&*state.db, &dedupe_key).await? {
        let payload: Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted document event is invalid"))?;
        let document_id = payload
            .get("document_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::internal("persisted document event is missing its document")
            })?;
        let document = scoped_document(&state, &project_id, document_id).await?;
        if payload.get("kind").and_then(Value::as_str) != Some(document.kind.as_str())
            || payload.get("title").and_then(Value::as_str) != Some(document.title.as_str())
            || payload.get("approval_policy").and_then(Value::as_str)
                != Some(document.approval_policy.as_str())
            || payload
                .get("expected_project_version")
                .and_then(Value::as_i64)
                != Some(request.mutation.expected_version)
            || payload.get("principal_id").and_then(Value::as_str) != Some(user.user_id.as_str())
            || payload
                .get("authorization_event_id")
                .and_then(Value::as_str)
                != Some(request.mutation.authorization.event_id.as_str())
            || payload.get("authorization_basis").and_then(Value::as_str)
                != Some(request.mutation.authorization.authorization_basis.as_str())
        {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Project Document",
            ));
        }
        return Ok((StatusCode::OK, Json(document_to_api(document)?)));
    }
    // The document row and its replay ledger entry are one authoritative
    // mutation.  A separate event transaction would leave an un-replayable
    // document if the process stopped between the two commits.
    let document_id = new_uuid_v4();
    let now = now_rfc3339();
    let kind = document_kind_name(request.kind).to_owned();
    let approval_policy = approval_policy_name(request.approval_policy).to_owned();
    let event_id = new_uuid_v4();
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    // Lock the Project version before allocating the Document identity.  The
    // expected version is the Project mutation token for creation, not a
    // caller-supplied hint which can be ignored while another Project write
    // races this one.
    let locked = sqlx::query("UPDATE project SET version = version WHERE id = ? AND version = ?")
        .bind(&project_id)
        .bind(request.mutation.expected_version)
        .execute(&mut *tx)
        .await
        .map_err(map_sql_error)?;
    if locked.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before the Document was created",
        ));
    }
    let inserted = sqlx::query(
        "INSERT INTO project_document (
            id, project_id, kind, title, lifecycle, approval_policy,
            current_draft_revision_id, current_approved_revision_id,
            version, created_at, updated_at
         ) VALUES (?, ?, ?, ?, 'draft', ?, NULL, NULL, 1, ?, ?)",
    )
    .bind(&document_id)
    .bind(&project_id)
    .bind(&kind)
    .bind(&title)
    .bind(&approval_policy)
    .bind(&now)
    .bind(&now)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    if inserted.rows_affected() != 1 {
        return Err(ApiError::internal("Project Document was not created"));
    }
    let project_updated = sqlx::query(
        "UPDATE project SET version = version + 1, updated_at = ?
         WHERE id = ? AND version = ?",
    )
    .bind(&now)
    .bind(&project_id)
    .bind(request.mutation.expected_version)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    if project_updated.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before the Document was created",
        ));
    }
    let event = DomainEventRepo::append_event_in_tx(
        &*state.db,
        &mut tx,
        &CreateDomainEvent {
            id: event_id.clone(),
            event_type: "project.document.created".to_owned(),
            entity_type: "project_document".to_owned(),
            entity_id: document_id.clone(),
            actor_type: principal_kind_name(request.mutation.authorization.principal.kind)
                .to_owned(),
            actor_id: Some(request.mutation.authorization.principal.id.clone()),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: request.mutation.authorization.event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(dedupe_key),
            payload_json: json!({
                "project_id": project_id,
                "document_id": document_id,
                "kind": kind,
                "title": title,
                "approval_policy": approval_policy,
                "expected_project_version": request.mutation.expected_version,
                "principal_id": user.user_id,
                "authorization_event_id": request.mutation.authorization.event_id,
                "authorization_basis": request.mutation.authorization.authorization_basis,
            })
            .to_string(),
            created_at: now.clone(),
        },
    )
    .await
    .map_err(map_event_error)?;
    // append_event_in_tx returns the existing event for an idempotent replay.
    // If a concurrent request won the dedupe race, discard this transaction
    // so it cannot create a second document for the first request's event.
    if event.id != event_id {
        tx.rollback().await?;
        let payload: Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted document event is invalid"))?;
        let same_request = payload.get("project_id").and_then(Value::as_str)
            == Some(project_id.as_str())
            && payload.get("kind").and_then(Value::as_str) == Some(kind.as_str())
            && payload.get("title").and_then(Value::as_str) == Some(title.as_str())
            && payload.get("approval_policy").and_then(Value::as_str)
                == Some(approval_policy.as_str())
            && payload
                .get("expected_project_version")
                .and_then(Value::as_i64)
                == Some(request.mutation.expected_version)
            && payload.get("principal_id").and_then(Value::as_str) == Some(user.user_id.as_str())
            && payload
                .get("authorization_event_id")
                .and_then(Value::as_str)
                == Some(request.mutation.authorization.event_id.as_str())
            && payload.get("authorization_basis").and_then(Value::as_str)
                == Some(request.mutation.authorization.authorization_basis.as_str());
        if !same_request {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Project Document",
            ));
        }
        let existing_id = payload
            .get("document_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::internal("persisted document event is missing its document")
            })?;
        let existing = scoped_document(&state, &project_id, existing_id).await?;
        return Ok((StatusCode::OK, Json(document_to_api(existing)?)));
    }
    tx.commit().await?;
    let document = ProjectOrchestrationRepo::get_project_document(&*state.db, &document_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project_document", document_id.clone()))?;
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
    let document = scoped_document(&state, &project_id, &document_id).await?;
    if !matches!(
        request.lifecycle,
        DocumentRevisionLifecycle::Draft | DocumentRevisionLifecycle::Proposed
    ) {
        return Err(ApiError::bad_request(
            "a new Project Document revision must be draft or proposed",
        ));
    }
    validate_content_kind(document_kind(&document)?, &request.content)?;
    let rendered_view = services::render_project_document(
        &document.title,
        document_kind(&document)?,
        &request.content,
    );
    let content_digest = services::document_content_digest(&request.content);
    let render_digest = services::document_render_digest(DOCUMENT_RENDER_VERSION, &rendered_view);
    // A Document whose policy is `none` has no approval gate.  Persisting its
    // accepted revision as merely a draft would leave the artifact unusable
    // forever because the approval endpoint correctly rejects that policy.
    // The save therefore promotes that revision atomically; user/agent-gated
    // policies retain the requested draft/proposed lifecycle.
    let effective_lifecycle = if document.approval_policy == "none" {
        "approved"
    } else {
        document_revision_lifecycle_name(request.lifecycle)
    };
    let source_refs_json = serde_json::to_string(&request.provenance.source_refs)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let dedupe_key = format!(
        "project.document.revision:{project_id}:{document_id}:{}",
        request.mutation.idempotency_key
    );
    if let Some(event) = DomainEventRepo::get_event_by_dedupe(&*state.db, &dedupe_key).await? {
        let payload: Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted document revision event is invalid"))?;
        let same_request = payload.get("project_id").and_then(Value::as_str)
            == Some(project_id.as_str())
            && payload.get("document_id").and_then(Value::as_str) == Some(document_id.as_str())
            && payload.get("content_digest").and_then(Value::as_str)
                == Some(content_digest.as_str())
            && payload.get("render_digest").and_then(Value::as_str) == Some(render_digest.as_str())
            && payload.get("base_revision_id").and_then(Value::as_str)
                == request.base_revision_id.as_deref()
            && payload.get("lifecycle").and_then(Value::as_str) == Some(effective_lifecycle)
            && payload
                .get("expected_document_version")
                .and_then(Value::as_i64)
                == Some(request.mutation.expected_version)
            && payload.get("expected_digest").and_then(Value::as_str)
                == request.mutation.expected_digest.as_deref()
            && payload.get("change_summary").and_then(Value::as_str)
                == Some(request.change_summary.as_str())
            && payload.get("source_refs_json").and_then(Value::as_str)
                == Some(source_refs_json.as_str())
            && payload.get("principal_id").and_then(Value::as_str) == Some(user.user_id.as_str())
            && payload
                .get("authorization_event_id")
                .and_then(Value::as_str)
                == Some(request.mutation.authorization.event_id.as_str())
            && payload.get("authorization_basis").and_then(Value::as_str)
                == Some(request.mutation.authorization.authorization_basis.as_str());
        if !same_request {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Project Document revision",
            ));
        }
        let revision_id = payload
            .get("revision_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::internal("persisted document revision event is missing its revision")
            })?;
        let revision =
            ProjectOrchestrationRepo::get_project_document_revision(&*state.db, revision_id)
                .await?
                .filter(|revision| revision.document_id == document.id)
                .ok_or_else(|| {
                    ApiError::not_found("project_document_revision", revision_id.to_owned())
                })?;
        return Ok((StatusCode::OK, Json(revision_to_api(&document, revision)?)));
    }
    let content_json = serde_json::to_string(&request.content)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let base_revision_id = request.base_revision_id.clone();
    let revision_id = new_uuid_v4();
    let created_at = now_rfc3339();
    let event_id = new_uuid_v4();
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let document_row = sqlx::query(
        "SELECT version, current_draft_revision_id
         FROM project_document WHERE id = ? AND project_id = ?",
    )
    .bind(&document_id)
    .bind(&project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::not_found("project_document", document_id.clone()))?;
    let document_version: i64 = document_row.try_get("version")?;
    if document_version != request.mutation.expected_version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project Document changed before this revision was saved",
        ));
    }
    let current_draft_revision_id: Option<String> =
        document_row.try_get("current_draft_revision_id")?;
    let base_revision = if let Some(base_id) = base_revision_id.as_deref() {
        let base = sqlx::query(
            "SELECT revision FROM project_document_revision
             WHERE id = ? AND document_id = ?",
        )
        .bind(base_id)
        .bind(&document_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| ApiError::not_found("project_document_revision", base_id.to_owned()))?;
        if current_draft_revision_id.as_deref() != Some(base_id) {
            return Err(ApiError::conflict_with_code(
                "version_conflict",
                "the revision base is not the current draft",
            ));
        }
        base.try_get("revision")?
    } else {
        if current_draft_revision_id.is_some() {
            return Err(ApiError::conflict_with_code(
                "version_conflict",
                "a revision base is required after the first draft",
            ));
        }
        0
    };
    if let Some(expected_digest) = request.mutation.expected_digest.as_deref() {
        let actual = match current_draft_revision_id.as_deref() {
            Some(current_id) => {
                sqlx::query_scalar::<_, String>(
                    "SELECT content_digest FROM project_document_revision WHERE id = ?",
                )
                .bind(current_id)
                .fetch_optional(&mut *tx)
                .await?
            }
            None => None,
        };
        if actual.as_deref() != Some(expected_digest) {
            return Err(ApiError::conflict_with_code(
                "digest_conflict",
                "the current draft digest changed before this revision was saved",
            ));
        }
    }
    let revision: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(revision), 0) + 1
         FROM project_document_revision WHERE document_id = ?",
    )
    .bind(&document_id)
    .fetch_one(&mut *tx)
    .await?;
    if effective_lifecycle == "approved" {
        sqlx::query(
            "UPDATE project_document_revision SET lifecycle = 'superseded'
             WHERE document_id = ? AND lifecycle = 'approved'",
        )
        .bind(&document_id)
        .execute(&mut *tx)
        .await
        .map_err(map_sql_error)?;
    }
    sqlx::query(
        "INSERT INTO project_document_revision (
            id, document_id, revision, base_revision, base_revision_id, lifecycle,
            schema_version, render_version, content_json, rendered_view,
            change_summary, author_type, author_id, source_refs_json,
            content_digest, rendered_digest, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&revision_id)
    .bind(&document_id)
    .bind(revision)
    .bind(base_revision)
    .bind(base_revision_id.as_deref())
    .bind(effective_lifecycle)
    .bind(DOCUMENT_SCHEMA_VERSION)
    .bind(DOCUMENT_RENDER_VERSION)
    .bind(&content_json)
    .bind(&rendered_view)
    .bind(&request.change_summary)
    // HTTP revisions are always user-authored.  Agent/worker provenance is
    // materialized through a separate Project-Agent service path.
    .bind(principal_kind_name(PrincipalKind::User))
    .bind(Some(user.user_id.as_str()))
    .bind(&source_refs_json)
    .bind(&content_digest)
    .bind(&render_digest)
    .bind(&created_at)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    let updated = sqlx::query(
        "UPDATE project_document
         SET current_draft_revision_id = ?,
             current_approved_revision_id = CASE WHEN ? = 'approved' THEN ? ELSE current_approved_revision_id END,
             lifecycle = CASE WHEN ? = 'approved' THEN 'approved' ELSE lifecycle END,
             version = version + 1, updated_at = ?
         WHERE id = ? AND project_id = ? AND version = ?",
    )
    .bind(&revision_id)
    .bind(effective_lifecycle)
    .bind(&revision_id)
    .bind(effective_lifecycle)
    .bind(&created_at)
    .bind(&document_id)
    .bind(&project_id)
    .bind(request.mutation.expected_version)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    if updated.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project Document changed before this revision was saved",
        ));
    }
    let event = DomainEventRepo::append_event_in_tx(
        &*state.db,
        &mut tx,
        &CreateDomainEvent {
            id: event_id.clone(),
            event_type: "project.document.revision_created".to_owned(),
            entity_type: "project_document_revision".to_owned(),
            entity_id: revision_id.clone(),
            actor_type: "user".to_owned(),
            actor_id: Some(user.user_id.clone()),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: request.mutation.authorization.event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(dedupe_key),
            payload_json: json!({
                "project_id": project_id,
                "document_id": document_id,
                "revision_id": revision_id,
                "content_digest": content_digest,
                "render_digest": render_digest,
                "base_revision_id": base_revision_id,
                "lifecycle": effective_lifecycle,
                "auto_approved": effective_lifecycle == "approved"
                    && document.approval_policy == "none",
                "expected_document_version": request.mutation.expected_version,
                "expected_digest": request.mutation.expected_digest,
                "change_summary": request.change_summary,
                "source_refs_json": source_refs_json,
                "principal_id": user.user_id,
                "authorization_event_id": request.mutation.authorization.event_id,
                "authorization_basis": request.mutation.authorization.authorization_basis,
            })
            .to_string(),
            created_at: created_at.clone(),
        },
    )
    .await
    .map_err(map_event_error)?;
    if event.id != event_id {
        tx.rollback().await?;
        let payload: Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted document revision event is invalid"))?;
        let same_request = payload.get("project_id").and_then(Value::as_str)
            == Some(project_id.as_str())
            && payload.get("document_id").and_then(Value::as_str) == Some(document_id.as_str())
            && payload.get("content_digest").and_then(Value::as_str)
                == Some(content_digest.as_str())
            && payload.get("render_digest").and_then(Value::as_str) == Some(render_digest.as_str())
            && payload.get("base_revision_id").and_then(Value::as_str)
                == base_revision_id.as_deref()
            && payload.get("lifecycle").and_then(Value::as_str) == Some(effective_lifecycle)
            && payload
                .get("expected_document_version")
                .and_then(Value::as_i64)
                == Some(request.mutation.expected_version)
            && payload.get("expected_digest").and_then(Value::as_str)
                == request.mutation.expected_digest.as_deref()
            && payload.get("change_summary").and_then(Value::as_str)
                == Some(request.change_summary.as_str())
            && payload.get("source_refs_json").and_then(Value::as_str)
                == Some(source_refs_json.as_str())
            && payload.get("principal_id").and_then(Value::as_str) == Some(user.user_id.as_str())
            && payload
                .get("authorization_event_id")
                .and_then(Value::as_str)
                == Some(request.mutation.authorization.event_id.as_str())
            && payload.get("authorization_basis").and_then(Value::as_str)
                == Some(request.mutation.authorization.authorization_basis.as_str());
        if !same_request {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Project Document revision",
            ));
        }
        let existing_id = payload
            .get("revision_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::internal("persisted document revision event is missing its revision")
            })?;
        let existing =
            ProjectOrchestrationRepo::get_project_document_revision(&*state.db, existing_id)
                .await?
                .filter(|revision| revision.document_id == document.id)
                .ok_or_else(|| {
                    ApiError::not_found("project_document_revision", existing_id.to_owned())
                })?;
        return Ok((StatusCode::OK, Json(revision_to_api(&document, existing)?)));
    }
    tx.commit().await?;
    let record = ProjectOrchestrationRepo::get_project_document_revision(&*state.db, &revision_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project_document_revision", revision_id.clone()))?;
    Ok((
        StatusCode::CREATED,
        Json(revision_to_api(&document, record)?),
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
    let revision_id = request.revision_id.clone();
    let document = scoped_document(&state, &project_id, &document_id).await?;
    if document.approval_policy == "none" {
        return Err(ApiError::conflict_with_code(
            "document.approval_not_required",
            "this Project Document does not have an approval policy",
        ));
    }
    if document.approval_policy == "project_agent" {
        return Err(ApiError::forbidden_with_code(
            "document.approval_policy",
            "this Project Document requires the bound Project Agent",
        ));
    }
    let expected_document_version = request.mutation.expected_version;
    let content_digest = request.content_digest.clone();
    let render_digest = request.render_digest.clone();
    let idempotency_key = request.mutation.idempotency_key.clone();
    let storage_idempotency_key = scoped_idempotency_key(
        "document-approval",
        &project_id,
        &user.user_id,
        &idempotency_key,
    );
    let authorization = request.mutation.authorization.clone();
    let principal_id = user.user_id.clone();
    let now = now_rfc3339();
    let dedupe_key =
        format!("project.document.approve:{project_id}:{document_id}:{idempotency_key}");
    let replay = DomainEventRepo::get_event_by_dedupe(&*state.db, &dedupe_key).await?;
    if let Some(event) = replay.as_ref() {
        let payload: Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted document approval event is invalid"))?;
        let same_request = document_approval_replay_matches(
            &payload,
            &project_id,
            &document_id,
            &revision_id,
            &content_digest,
            &render_digest,
            expected_document_version,
            &principal_id,
            &authorization,
        );
        if !same_request {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Document approval",
            ));
        }
    }
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    if let Some(row) =
        sqlx::query("SELECT * FROM project_document_approval WHERE idempotency_key = ?")
            .bind(&storage_idempotency_key)
            .fetch_optional(&mut *tx)
            .await?
    {
        let record = document_approval_record_from_row(row)?;
        if record.document_id != document_id
            || record.revision_id != revision_id
            || record.content_digest != content_digest
            || record.rendered_digest != render_digest
            || record.principal_type != "user"
            || record.principal_id != principal_id
            || record.authorization_basis != authorization.authorization_basis
            || record.authorization_action != authorization.action
            || record.explicit_event != authorization.event_id
            || record.authorization_occurred_at != authorization.occurred_at
        {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Document approval",
            ));
        }
        // A new approval and its event are committed together.  If a row is
        // visible without the corresponding replay event, fail closed rather
        // than inventing a compatibility repair for an invalid state.
        if replay.is_none() {
            return Err(ApiError::internal(
                "persisted document approval is missing its replay event",
            ));
        }
        tx.commit().await?;
        return Ok((
            StatusCode::OK,
            Json(approval_to_api(record, expected_document_version)?),
        ));
    }
    if replay.is_some() {
        return Err(ApiError::internal(
            "persisted document approval event is missing its approval",
        ));
    }
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        DOCUMENT_APPROVE_ACTION,
    )?;
    let document_row = sqlx::query(
        "SELECT version FROM project_document
         WHERE id = ? AND project_id = ?",
    )
    .bind(&document_id)
    .bind(&project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::not_found("project_document", document_id.clone()))?;
    let document_version: i64 = document_row.try_get("version")?;
    if document_version != expected_document_version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project Document changed before approval",
        ));
    }
    let target = sqlx::query(
        "SELECT lifecycle FROM project_document_revision
         WHERE id = ? AND document_id = ? AND content_digest = ?
           AND rendered_digest = ?",
    )
    .bind(&revision_id)
    .bind(&document_id)
    .bind(&content_digest)
    .bind(&render_digest)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| {
        ApiError::conflict_with_code(
            "digest_conflict",
            "the approval digests do not match the target revision",
        )
    })?;
    let target_lifecycle: String = target.try_get("lifecycle")?;
    if matches!(
        target_lifecycle.as_str(),
        "rejected" | "withdrawn" | "superseded"
    ) {
        return Err(ApiError::conflict_with_code(
            "document_revision.inactive",
            "the target revision is no longer approvable",
        ));
    }
    sqlx::query(
        "UPDATE project_document_revision SET lifecycle = 'superseded'
         WHERE document_id = ? AND lifecycle = 'approved' AND id != ?",
    )
    .bind(&document_id)
    .bind(&revision_id)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    let approved = sqlx::query(
        "UPDATE project_document_revision SET lifecycle = 'approved'
         WHERE id = ? AND document_id = ? AND lifecycle != 'approved'",
    )
    .bind(&revision_id)
    .bind(&document_id)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    if approved.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "document_revision.conflict",
            "the target revision changed before approval",
        ));
    }
    let updated = sqlx::query(
        "UPDATE project_document
         SET current_approved_revision_id = ?, lifecycle = 'approved',
             version = version + 1, updated_at = ?
         WHERE id = ? AND project_id = ? AND version = ?",
    )
    .bind(&revision_id)
    .bind(&now)
    .bind(&document_id)
    .bind(&project_id)
    .bind(expected_document_version)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    if updated.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project Document changed before approval",
        ));
    }
    let approval_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_document_approval (
            id, document_id, revision_id, principal_type, principal_id,
            authorization_basis, authorization_action, explicit_event,
            authorization_occurred_at, content_digest, rendered_digest,
            lifecycle, idempotency_key, version, created_at, updated_at
         ) VALUES (?, ?, ?, 'user', ?, ?, ?, ?, ?, ?, ?, 'active', ?, 1, ?, ?)",
    )
    .bind(&approval_id)
    .bind(&document_id)
    .bind(&revision_id)
    .bind(&principal_id)
    .bind(&authorization.authorization_basis)
    .bind(&authorization.action)
    .bind(&authorization.event_id)
    .bind(&authorization.occurred_at)
    .bind(&content_digest)
    .bind(&render_digest)
    .bind(&storage_idempotency_key)
    .bind(&now)
    .bind(&now)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    let event_id = new_uuid_v4();
    let event = DomainEventRepo::append_event_in_tx(
        &*state.db,
        &mut tx,
        &CreateDomainEvent {
            id: event_id.clone(),
            event_type: "project.document.approved".to_owned(),
            entity_type: "project_document_approval".to_owned(),
            entity_id: approval_id.clone(),
            actor_type: "user".to_owned(),
            actor_id: Some(principal_id.clone()),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: authorization.event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(dedupe_key),
            payload_json: json!({
                "project_id": project_id,
                "document_id": document_id,
                "revision_id": revision_id,
                "approval_id": approval_id,
                "content_digest": content_digest,
                "render_digest": render_digest,
                "expected_document_version": expected_document_version,
                "principal_id": principal_id,
                "authorization_principal": authorization.principal,
                "authorization_basis": authorization.authorization_basis,
                "authorization_action": authorization.action,
                "authorization_event_id": authorization.event_id,
                "authorization_occurred_at": authorization.occurred_at,
            })
            .to_string(),
            created_at: now.clone(),
        },
    )
    .await
    .map_err(map_event_error)?;
    if event.id != event_id {
        tx.rollback().await?;
        let payload: Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted document approval event is invalid"))?;
        let same_request = document_approval_replay_matches(
            &payload,
            &project_id,
            &document_id,
            &revision_id,
            &content_digest,
            &render_digest,
            expected_document_version,
            &principal_id,
            &authorization,
        );
        if !same_request {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Document approval",
            ));
        }
        let existing_id = payload
            .get("approval_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::internal("persisted approval event is missing its approval")
            })?;
        let existing = sqlx::query("SELECT * FROM project_document_approval WHERE id = ?")
            .bind(existing_id)
            .fetch_optional(state.db.pool())
            .await?
            .ok_or_else(|| {
                ApiError::not_found("project_document_approval", existing_id.to_owned())
            })?;
        return Ok((
            StatusCode::OK,
            Json(approval_to_api(
                document_approval_record_from_row(existing)?,
                expected_document_version,
            )?),
        ));
    }
    let row = sqlx::query("SELECT * FROM project_document_approval WHERE id = ?")
        .bind(&approval_id)
        .fetch_one(&mut *tx)
        .await?;
    let record = document_approval_record_from_row(row)?;
    tx.commit().await?;
    Ok((
        StatusCode::CREATED,
        Json(approval_to_api(record, expected_document_version)?),
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
    let mut context = serde_json::to_value(&request.context)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    if !context.is_object() {
        context = json!({ "summary": context });
    }
    context["decision_class"] =
        Value::String(decision_class_name(request.decision_class).to_owned());
    let context_json = context.to_string();
    let options_json = serde_json::to_string(&request.options)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let source_refs_json = serde_json::to_string(&request.source_refs)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let dedupe_key = format!(
        "project.decision.candidate.create:{project_id}:{}",
        request.mutation.idempotency_key
    );
    if let Some(event) = DomainEventRepo::get_event_by_dedupe(&*state.db, &dedupe_key).await? {
        let payload: Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted decision candidate event is invalid"))?;
        let same_request = payload.get("project_id").and_then(Value::as_str)
            == Some(project_id.as_str())
            && payload.get("question").and_then(Value::as_str) == Some(question.as_str())
            && payload.get("decision_class").and_then(Value::as_str)
                == Some(decision_class_name(request.decision_class))
            && payload.get("context_json").and_then(Value::as_str) == Some(context_json.as_str())
            && payload.get("options_json").and_then(Value::as_str) == Some(options_json.as_str())
            && payload
                .get("expected_project_version")
                .and_then(Value::as_i64)
                == Some(request.mutation.expected_version)
            && payload
                .get("selected_outcome")
                .cloned()
                .unwrap_or(Value::Null)
                == request
                    .selected_outcome
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            && payload.get("rationale").cloned().unwrap_or(Value::Null)
                == request
                    .rationale
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            && payload.get("source_refs_json").and_then(Value::as_str)
                == Some(source_refs_json.as_str())
            && payload.get("principal_id").and_then(Value::as_str) == Some(user.user_id.as_str())
            && payload
                .get("authorization_event_id")
                .and_then(Value::as_str)
                == Some(request.mutation.authorization.event_id.as_str())
            && payload.get("authorization_basis").and_then(Value::as_str)
                == Some(request.mutation.authorization.authorization_basis.as_str());
        if !same_request {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Decision candidate",
            ));
        }
        let candidate_id = payload
            .get("candidate_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::internal("persisted decision candidate event is missing its candidate")
            })?;
        let candidate =
            ProjectOrchestrationRepo::get_project_decision_candidate(&*state.db, candidate_id)
                .await?
                .filter(|candidate| candidate.project_id == project_id)
                .ok_or_else(|| {
                    ApiError::not_found("decision_candidate", candidate_id.to_owned())
                })?;
        return Ok((StatusCode::OK, Json(candidate_to_api(candidate)?)));
    }
    let now = now_rfc3339();
    let candidate_id = new_uuid_v4();
    let event_id = new_uuid_v4();
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let project = sqlx::query("SELECT version FROM project WHERE id = ?")
        .bind(&project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.clone()))?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != request.mutation.expected_version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before the Decision candidate was created",
        ));
    }
    validate_decision_context_in_tx(&mut tx, &project_id, &context).await?;
    sqlx::query(
        "INSERT INTO project_decision_candidate (
            id, project_id, lifecycle, question, context_json, options_json,
            selected_outcome, rationale, principal_type, principal_id,
            source_refs_json, expected_project_version, effective_decision_id,
            version, created_at, updated_at
         ) VALUES (?, ?, 'proposed', ?, ?, ?, ?, ?, 'user', ?, ?, ?, NULL, 1, ?, ?)",
    )
    .bind(&candidate_id)
    .bind(&project_id)
    .bind(&question)
    .bind(&context_json)
    .bind(&options_json)
    .bind(request.selected_outcome.as_deref())
    .bind(request.rationale.as_deref())
    .bind(user.user_id.as_str())
    .bind(&source_refs_json)
    .bind(request.mutation.expected_version)
    .bind(&now)
    .bind(&now)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    let project_updated = sqlx::query(
        "UPDATE project SET version = version + 1, updated_at = ?
         WHERE id = ? AND version = ?",
    )
    .bind(&now)
    .bind(&project_id)
    .bind(request.mutation.expected_version)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    if project_updated.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before the Decision candidate was created",
        ));
    }
    let event = DomainEventRepo::append_event_in_tx(
        &*state.db,
        &mut tx,
        &CreateDomainEvent {
            id: event_id.clone(),
            event_type: "project.decision.candidate_created".to_owned(),
            entity_type: "project_decision_candidate".to_owned(),
            entity_id: candidate_id.clone(),
            actor_type: "user".to_owned(),
            actor_id: Some(user.user_id.clone()),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: request.mutation.authorization.event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(dedupe_key),
            payload_json: json!({
                "project_id": project_id,
                "candidate_id": candidate_id,
                "question": question,
                "decision_class": decision_class_name(request.decision_class),
                "context_json": context_json,
                "options_json": options_json,
                "selected_outcome": request.selected_outcome,
                "rationale": request.rationale,
                "source_refs_json": source_refs_json,
                "expected_project_version": request.mutation.expected_version,
                "principal_id": user.user_id,
                "authorization_event_id": request.mutation.authorization.event_id,
                "authorization_basis": request.mutation.authorization.authorization_basis,
            })
            .to_string(),
            created_at: now.clone(),
        },
    )
    .await
    .map_err(map_event_error)?;
    if event.id != event_id {
        tx.rollback().await?;
        let payload: Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted decision candidate event is invalid"))?;
        let same_request = payload.get("project_id").and_then(Value::as_str)
            == Some(project_id.as_str())
            && payload.get("question").and_then(Value::as_str) == Some(question.as_str())
            && payload.get("decision_class").and_then(Value::as_str)
                == Some(decision_class_name(request.decision_class))
            && payload.get("context_json").and_then(Value::as_str) == Some(context_json.as_str())
            && payload.get("options_json").and_then(Value::as_str) == Some(options_json.as_str())
            && payload.get("source_refs_json").and_then(Value::as_str)
                == Some(source_refs_json.as_str())
            && payload
                .get("expected_project_version")
                .and_then(Value::as_i64)
                == Some(request.mutation.expected_version)
            && payload
                .get("selected_outcome")
                .cloned()
                .unwrap_or(Value::Null)
                == request
                    .selected_outcome
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            && payload.get("rationale").cloned().unwrap_or(Value::Null)
                == request
                    .rationale
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            && payload.get("principal_id").and_then(Value::as_str) == Some(user.user_id.as_str())
            && payload
                .get("authorization_event_id")
                .and_then(Value::as_str)
                == Some(request.mutation.authorization.event_id.as_str())
            && payload.get("authorization_basis").and_then(Value::as_str)
                == Some(request.mutation.authorization.authorization_basis.as_str());
        if !same_request {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Decision candidate",
            ));
        }
        let existing_id = payload
            .get("candidate_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::internal("persisted decision event is missing its candidate")
            })?;
        let existing =
            ProjectOrchestrationRepo::get_project_decision_candidate(&*state.db, existing_id)
                .await?
                .filter(|candidate| candidate.project_id == project_id)
                .ok_or_else(|| ApiError::not_found("decision_candidate", existing_id.to_owned()))?;
        return Ok((StatusCode::OK, Json(candidate_to_api(existing)?)));
    }
    tx.commit().await?;
    let record =
        ProjectOrchestrationRepo::get_project_decision_candidate(&*state.db, &candidate_id)
            .await?
            .ok_or_else(|| ApiError::not_found("decision_candidate", candidate_id.clone()))?;
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
    let dedupe_key = format!(
        "project.decision.approve:{project_id}:{candidate_id}:{}",
        request.mutation.idempotency_key
    );
    if let Some(event) = DomainEventRepo::get_event_by_dedupe(&*state.db, &dedupe_key).await? {
        let payload: Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted decision approval event is invalid"))?;
        let same_request = payload.get("project_id").and_then(Value::as_str)
            == Some(project_id.as_str())
            && payload.get("candidate_id").and_then(Value::as_str) == Some(candidate_id.as_str())
            && payload
                .get("expected_project_version")
                .and_then(Value::as_i64)
                == Some(request.mutation.expected_version)
            && payload.get("authorization_basis").and_then(Value::as_str)
                == Some(request.mutation.authorization.authorization_basis.as_str())
            && payload
                .get("authorization_event_id")
                .and_then(Value::as_str)
                == Some(request.mutation.authorization.event_id.as_str())
            && payload.get("principal_id").and_then(Value::as_str) == Some(user.user_id.as_str());
        if !same_request {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Decision approval",
            ));
        }
        let decision_id = payload
            .get("decision_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::internal("persisted decision approval event is missing its Decision")
            })?;
        let record = get_decision_record(&state, &project_id, decision_id).await?;
        return Ok((StatusCode::OK, Json(decision_to_api(record)?)));
    }
    let now = now_rfc3339();
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let project =
        sqlx::query("SELECT version, current_charter_revision_id FROM project WHERE id = ?")
            .bind(&project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| ApiError::not_found("project", project_id.clone()))?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != request.mutation.expected_version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before the decision was approved",
        ));
    }
    let candidate = sqlx::query(
        "SELECT * FROM project_decision_candidate
         WHERE id = ? AND project_id = ?",
    )
    .bind(&candidate_id)
    .bind(&project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::not_found("decision_candidate", candidate_id.clone()))?;
    let lifecycle: String = candidate.try_get("lifecycle")?;
    if !matches!(lifecycle.as_str(), "draft" | "proposed") {
        return Err(ApiError::conflict_with_code(
            "decision_candidate.inactive",
            "the decision candidate is no longer awaiting approval",
        ));
    }
    let question: String = candidate.try_get("question")?;
    let context_json: String = candidate.try_get("context_json")?;
    let options_json: String = candidate.try_get("options_json")?;
    let selected_outcome: String = candidate
        .try_get::<Option<String>, _>("selected_outcome")?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                "decision_candidate.incomplete",
                "selected_outcome is required",
            )
        })?;
    let rationale: String = candidate
        .try_get::<Option<String>, _>("rationale")?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ApiError::conflict_with_code("decision_candidate.incomplete", "rationale is required")
        })?;
    let context_value: Value = serde_json::from_str(&context_json)
        .map_err(|_| ApiError::internal("persisted decision candidate context is invalid"))?;
    let decision_class = context_value
        .get("decision_class")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::internal("decision candidate is missing its decision class"))?
        .to_owned();
    if !matches!(
        decision_class.as_str(),
        "user_scope" | "project_implementation" | "policy" | "waiver"
    ) {
        return Err(ApiError::internal(
            "decision candidate has an invalid decision class",
        ));
    }
    validate_decision_context_in_tx(&mut tx, &project_id, &context_value).await?;
    let supersedes_decision_id = context_value
        .get("supersedes_decision_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    let invalidates_decision_id = context_value
        .get("invalidates_decision_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    if supersedes_decision_id.is_some() && invalidates_decision_id.is_some() {
        return Err(ApiError::bad_request(
            "a Decision candidate may supersede or invalidate one Decision, not both",
        ));
    }
    let invalidates = invalidates_decision_id.is_some();
    let target_decision_id = supersedes_decision_id
        .clone()
        .or(invalidates_decision_id.clone());
    if let Some(target_id) = target_decision_id.as_deref() {
        let target_exists: Option<String> =
            sqlx::query_scalar("SELECT id FROM project_decision WHERE id = ? AND project_id = ?")
                .bind(target_id)
                .bind(&project_id)
                .fetch_optional(&mut *tx)
                .await?;
        if target_exists.is_none() {
            return Err(ApiError::not_found("decision", target_id.to_owned()));
        }
    }
    let source_refs_json: String = candidate.try_get("source_refs_json")?;
    let affected_records_json = json!({
        "artifact_refs": context_value
            .get("affected_artifact_refs")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "task_ids": context_value
            .get("affected_task_ids")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "milestone_ids": context_value
            .get("affected_milestone_ids")
            .cloned()
            .unwrap_or_else(|| json!([])),
    })
    .to_string();
    let charter_revision_id = context_value
        .get("governing_charter_revision_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or(project.try_get("current_charter_revision_id")?);
    let baseline_revision_id = context_value
        .get("governing_baseline_revision_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let decision_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_decision (
            id, project_id, state, decision_class, question, context_json,
            options_json, selected_outcome, rationale, principal_type,
            principal_id, authority_basis, charter_revision_id,
            baseline_revision_id, source_refs_json, affected_records_json,
            supersedes_decision_id, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'user', ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&decision_id)
    .bind(&project_id)
    .bind(if invalidates { "invalidated" } else { "active" })
    .bind(&decision_class)
    .bind(&question)
    .bind(&context_json)
    .bind(&options_json)
    .bind(&selected_outcome)
    .bind(&rationale)
    .bind(&user.user_id)
    .bind(&request.mutation.authorization.authorization_basis)
    .bind(charter_revision_id.as_deref())
    .bind(baseline_revision_id.as_deref())
    .bind(&source_refs_json)
    .bind(&affected_records_json)
    .bind(target_decision_id.as_deref())
    .bind(&now)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    let candidate_update = sqlx::query(
        "UPDATE project_decision_candidate
         SET lifecycle = 'approved', effective_decision_id = ?,
             version = version + 1, updated_at = ?
         WHERE id = ? AND project_id = ? AND lifecycle IN ('draft', 'proposed')",
    )
    .bind(&decision_id)
    .bind(&now)
    .bind(&candidate_id)
    .bind(&project_id)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    if candidate_update.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "decision_candidate.conflict",
            "the decision candidate changed before approval",
        ));
    }
    let project_update = sqlx::query(
        "UPDATE project SET version = version + 1, updated_at = ?
         WHERE id = ? AND version = ?",
    )
    .bind(&now)
    .bind(&project_id)
    .bind(project_version)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    if project_update.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before approval",
        ));
    }
    let event_id = new_uuid_v4();
    let event = DomainEventRepo::append_event_in_tx(
        &*state.db,
        &mut tx,
        &CreateDomainEvent {
            id: event_id.clone(),
            event_type: "project.decision.approved".to_owned(),
            entity_type: "project_decision".to_owned(),
            entity_id: decision_id.clone(),
            actor_type: "user".to_owned(),
            actor_id: Some(user.user_id.clone()),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: request.mutation.authorization.event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(dedupe_key),
            payload_json: json!({
                "project_id": project_id,
                "candidate_id": candidate_id,
                "decision_id": decision_id,
                "expected_project_version": request.mutation.expected_version,
                "principal_id": user.user_id,
                "authorization_event_id": request.mutation.authorization.event_id,
                "authorization_basis": request.mutation.authorization.authorization_basis,
            })
            .to_string(),
            created_at: now,
        },
    )
    .await
    .map_err(map_event_error)?;
    if event.id != event_id {
        tx.rollback().await?;
        let payload: Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted decision approval event is invalid"))?;
        let same_request = payload.get("project_id").and_then(Value::as_str)
            == Some(project_id.as_str())
            && payload.get("candidate_id").and_then(Value::as_str) == Some(candidate_id.as_str())
            && payload
                .get("expected_project_version")
                .and_then(Value::as_i64)
                == Some(request.mutation.expected_version)
            && payload.get("authorization_basis").and_then(Value::as_str)
                == Some(request.mutation.authorization.authorization_basis.as_str())
            && payload
                .get("authorization_event_id")
                .and_then(Value::as_str)
                == Some(request.mutation.authorization.event_id.as_str())
            && payload.get("principal_id").and_then(Value::as_str) == Some(user.user_id.as_str());
        if !same_request {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Decision approval",
            ));
        }
        let decision_id = payload
            .get("decision_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::internal("persisted decision approval event is missing its Decision")
            })?;
        let record = get_decision_record(&state, &project_id, decision_id).await?;
        return Ok((StatusCode::OK, Json(decision_to_api(record)?)));
    }
    tx.commit().await?;
    let record = get_decision_record(&state, &project_id, &decision_id).await?;
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
    let dedupe_key = format!(
        "project.decision.reject:{project_id}:{candidate_id}:{}",
        request.mutation.idempotency_key
    );
    if let Some(event) = DomainEventRepo::get_event_by_dedupe(&*state.db, &dedupe_key).await? {
        let payload: Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted decision rejection event is invalid"))?;
        let same_request = payload.get("project_id").and_then(Value::as_str)
            == Some(project_id.as_str())
            && payload.get("candidate_id").and_then(Value::as_str) == Some(candidate_id.as_str())
            && payload.get("reason").and_then(Value::as_str) == Some(reason.as_str())
            && payload
                .get("expected_project_version")
                .and_then(Value::as_i64)
                == Some(request.mutation.expected_version)
            && payload.get("principal_id").and_then(Value::as_str) == Some(user.user_id.as_str())
            && payload
                .get("authorization_event_id")
                .and_then(Value::as_str)
                == Some(request.mutation.authorization.event_id.as_str())
            && payload.get("authorization_basis").and_then(Value::as_str)
                == Some(request.mutation.authorization.authorization_basis.as_str());
        if !same_request {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Decision rejection",
            ));
        }
        let candidate =
            ProjectOrchestrationRepo::get_project_decision_candidate(&*state.db, &candidate_id)
                .await?
                .filter(|candidate| candidate.project_id == project_id)
                .ok_or_else(|| ApiError::not_found("decision_candidate", candidate_id.clone()))?;
        return Ok((StatusCode::OK, Json(candidate_to_api(candidate)?)));
    }
    let now = now_rfc3339();
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let project = sqlx::query("SELECT version FROM project WHERE id = ?")
        .bind(&project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.clone()))?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != request.mutation.expected_version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before rejection",
        ));
    }
    let candidate = sqlx::query(
        "SELECT context_json FROM project_decision_candidate
         WHERE id = ? AND project_id = ? AND lifecycle IN ('draft', 'proposed')",
    )
    .bind(&candidate_id)
    .bind(&project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::not_found("decision_candidate", candidate_id.clone()))?;
    let mut context: Value = serde_json::from_str(&candidate.try_get::<String, _>("context_json")?)
        .map_err(|_| ApiError::internal("persisted decision candidate context is invalid"))?;
    if !context.is_object() {
        context = json!({ "summary": context });
    }
    context["rejection_reason"] = Value::String(reason.clone());
    let updated = sqlx::query(
        "UPDATE project_decision_candidate
         SET lifecycle = 'rejected', context_json = ?, version = version + 1, updated_at = ?
         WHERE id = ? AND project_id = ? AND lifecycle IN ('draft', 'proposed')",
    )
    .bind(context.to_string())
    .bind(&now)
    .bind(&candidate_id)
    .bind(&project_id)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    if updated.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "decision_candidate.conflict",
            "the candidate changed before rejection",
        ));
    }
    let project_update = sqlx::query(
        "UPDATE project SET version = version + 1, updated_at = ? WHERE id = ? AND version = ?",
    )
    .bind(&now)
    .bind(&project_id)
    .bind(project_version)
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    if project_update.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before rejection",
        ));
    }
    let event_id = new_uuid_v4();
    let event = DomainEventRepo::append_event_in_tx(
        &*state.db,
        &mut tx,
        &CreateDomainEvent {
            id: event_id.clone(),
            event_type: "project.decision.candidate_rejected".to_owned(),
            entity_type: "project_decision_candidate".to_owned(),
            entity_id: candidate_id.clone(),
            actor_type: "user".to_owned(),
            actor_id: Some(user.user_id.clone()),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: request.mutation.authorization.event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(dedupe_key),
            payload_json: json!({
                "project_id": project_id,
                "candidate_id": candidate_id,
                "reason": reason,
                "expected_project_version": request.mutation.expected_version,
                "principal_id": user.user_id,
                "authorization_event_id": request.mutation.authorization.event_id,
                "authorization_basis": request.mutation.authorization.authorization_basis,
            })
            .to_string(),
            created_at: now,
        },
    )
    .await
    .map_err(map_event_error)?;
    if event.id != event_id {
        tx.rollback().await?;
        let payload: Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted decision rejection event is invalid"))?;
        let same_request = payload.get("project_id").and_then(Value::as_str)
            == Some(project_id.as_str())
            && payload.get("candidate_id").and_then(Value::as_str) == Some(candidate_id.as_str())
            && payload.get("reason").and_then(Value::as_str) == Some(reason.as_str())
            && payload
                .get("expected_project_version")
                .and_then(Value::as_i64)
                == Some(request.mutation.expected_version)
            && payload.get("principal_id").and_then(Value::as_str) == Some(user.user_id.as_str())
            && payload
                .get("authorization_event_id")
                .and_then(Value::as_str)
                == Some(request.mutation.authorization.event_id.as_str())
            && payload.get("authorization_basis").and_then(Value::as_str)
                == Some(request.mutation.authorization.authorization_basis.as_str());
        if !same_request {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different Decision rejection",
            ));
        }
        let winner_id = payload
            .get("candidate_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::internal("persisted decision rejection event is missing its candidate")
            })?;
        let candidate =
            ProjectOrchestrationRepo::get_project_decision_candidate(&*state.db, winner_id)
                .await?
                .filter(|candidate| candidate.project_id == project_id)
                .ok_or_else(|| ApiError::not_found("decision_candidate", winner_id.to_owned()))?;
        return Ok((StatusCode::OK, Json(candidate_to_api(candidate)?)));
    }
    tx.commit().await?;
    let candidate =
        ProjectOrchestrationRepo::get_project_decision_candidate(&*state.db, &candidate_id)
            .await?
            .ok_or_else(|| ApiError::not_found("decision_candidate", candidate_id))?;
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

/// Validate every Decision reference while the candidate/effective Decision
/// transaction is open.  IDs are never looked up first and then disclosed:
/// an unknown or another-Project reference receives the same typed not-found
/// result, so a caller cannot probe another Project through this API.
async fn validate_decision_context_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    project_id: &str,
    context: &Value,
) -> ApiResult<()> {
    let mut typed_context = context.clone();
    if let Some(object) = typed_context.as_object_mut() {
        // The persisted candidate envelope adds this routing discriminator;
        // it is not part of the closed context payload itself.
        object.remove("decision_class");
        object.remove("rejection_reason");
    }
    let typed_context: DecisionCandidateContext = serde_json::from_value(typed_context)
        .map_err(|_| ApiError::bad_request("decision context contains an invalid reference"))?;

    for artifact in &typed_context.affected_artifact_refs {
        let row = sqlx::query(
            "SELECT render_version, rendered_digest
             FROM project_document_revision AS r
             JOIN project_document AS d ON d.id = r.document_id
             WHERE d.project_id = ? AND d.id = ? AND r.id = ? AND r.content_digest = ?
             UNION ALL
             SELECT r.render_version, r.rendered_digest
             FROM project_charter_revision AS r
             JOIN project_charter AS c ON c.id = r.charter_id
             WHERE c.project_id = ? AND c.id = ? AND r.id = ? AND r.content_digest = ?
             UNION ALL
             SELECT r.render_version, r.rendered_digest
             FROM project_execution_baseline_revision AS r
             JOIN project_execution_baseline AS b ON b.id = r.baseline_id
             WHERE b.project_id = ? AND b.id = ? AND r.id = ? AND r.content_digest = ?
             UNION ALL
             SELECT r.render_version, r.rendered_digest
             FROM project_milestone_revision AS r
             JOIN project_milestone AS m ON m.id = r.milestone_id
             WHERE m.project_id = ? AND m.id = ? AND r.id = ? AND r.content_digest = ?
             LIMIT 1",
        )
        .bind(project_id)
        .bind(&artifact.artifact_id)
        .bind(&artifact.revision_id)
        .bind(&artifact.content_digest)
        .bind(project_id)
        .bind(&artifact.artifact_id)
        .bind(&artifact.revision_id)
        .bind(&artifact.content_digest)
        .bind(project_id)
        .bind(&artifact.artifact_id)
        .bind(&artifact.revision_id)
        .bind(&artifact.content_digest)
        .bind(project_id)
        .bind(&artifact.artifact_id)
        .bind(&artifact.revision_id)
        .bind(&artifact.content_digest)
        .fetch_optional(&mut **tx)
        .await?;
        let Some(row) = row else {
            return Err(ApiError::not_found(
                "decision_reference",
                artifact.revision_id.clone(),
            ));
        };
        let render_version: String = row.try_get("render_version")?;
        let render_digest: String = row.try_get("rendered_digest")?;
        if artifact
            .render_version
            .as_deref()
            .is_some_and(|value| value != render_version)
            || artifact
                .render_digest
                .as_deref()
                .is_some_and(|value| value != render_digest)
        {
            return Err(ApiError::conflict_with_code(
                "decision_reference.digest_conflict",
                "a Decision artifact reference is stale",
            ));
        }
    }

    for task_id in &typed_context.affected_task_ids {
        let exists: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM task WHERE id = ? AND project_id = ? LIMIT 1")
                .bind(task_id)
                .bind(project_id)
                .fetch_optional(&mut **tx)
                .await?;
        if exists.is_none() {
            return Err(ApiError::not_found("decision_reference", task_id.clone()));
        }
    }
    for milestone_id in &typed_context.affected_milestone_ids {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_milestone WHERE id = ? AND project_id = ? LIMIT 1",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(&mut **tx)
        .await?;
        if exists.is_none() {
            return Err(ApiError::not_found(
                "decision_reference",
                milestone_id.clone(),
            ));
        }
    }
    if let Some(charter_revision_id) = typed_context.governing_charter_revision_id.as_deref() {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM project_charter_revision AS r
             JOIN project_charter AS c ON c.id = r.charter_id
             WHERE r.id = ? AND c.project_id = ? LIMIT 1",
        )
        .bind(charter_revision_id)
        .bind(project_id)
        .fetch_optional(&mut **tx)
        .await?;
        if exists.is_none() {
            return Err(ApiError::not_found(
                "decision_reference",
                charter_revision_id.to_owned(),
            ));
        }
    }
    if let Some(baseline_revision_id) = typed_context.governing_baseline_revision_id.as_deref() {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM project_execution_baseline_revision AS r
             JOIN project_execution_baseline AS b ON b.id = r.baseline_id
             WHERE r.id = ? AND b.project_id = ? LIMIT 1",
        )
        .bind(baseline_revision_id)
        .bind(project_id)
        .fetch_optional(&mut **tx)
        .await?;
        if exists.is_none() {
            return Err(ApiError::not_found(
                "decision_reference",
                baseline_revision_id.to_owned(),
            ));
        }
    }
    Ok(())
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

fn document_kind(document: &ProjectDocumentRecord) -> ApiResult<ProjectDocumentKind> {
    parse_document_kind(&document.kind)
        .ok_or_else(|| ApiError::internal("invalid persisted Project Document kind"))
}

fn validate_content_kind(
    kind: ProjectDocumentKind,
    content: &ProjectDocumentContent,
) -> ApiResult<()> {
    let matches = matches!(
        (kind, content),
        (
            ProjectDocumentKind::Research,
            ProjectDocumentContent::Research(_)
        ) | (
            ProjectDocumentKind::DeliveryBrief,
            ProjectDocumentContent::DeliveryBrief(_)
        ) | (
            ProjectDocumentKind::ProductSpec,
            ProjectDocumentContent::ProductSpec(_)
        ) | (
            ProjectDocumentKind::Design,
            ProjectDocumentContent::Design(_)
        ) | (
            ProjectDocumentKind::Architecture,
            ProjectDocumentContent::Architecture(_)
        ) | (
            ProjectDocumentKind::ExecutionPlan,
            ProjectDocumentContent::ExecutionPlan(_)
        )
    );
    if matches {
        Ok(())
    } else {
        Err(ApiError::bad_request(
            "document content kind does not match the Project Document kind",
        ))
    }
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

fn document_revision_lifecycle_name(lifecycle: DocumentRevisionLifecycle) -> &'static str {
    match lifecycle {
        DocumentRevisionLifecycle::Draft => "draft",
        DocumentRevisionLifecycle::Proposed => "proposed",
        DocumentRevisionLifecycle::Approved => "approved",
        DocumentRevisionLifecycle::Rejected => "rejected",
        DocumentRevisionLifecycle::Withdrawn => "withdrawn",
        DocumentRevisionLifecycle::Superseded => "superseded",
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

#[allow(clippy::too_many_arguments)]
fn document_approval_replay_matches(
    payload: &Value,
    project_id: &str,
    document_id: &str,
    revision_id: &str,
    content_digest: &str,
    render_digest: &str,
    expected_document_version: i64,
    principal_id: &str,
    authorization: &AuthorizationProvenance,
) -> bool {
    payload.get("project_id").and_then(Value::as_str) == Some(project_id)
        && payload.get("document_id").and_then(Value::as_str) == Some(document_id)
        && payload.get("revision_id").and_then(Value::as_str) == Some(revision_id)
        && payload.get("content_digest").and_then(Value::as_str) == Some(content_digest)
        && payload.get("render_digest").and_then(Value::as_str) == Some(render_digest)
        && payload
            .get("expected_document_version")
            .and_then(Value::as_i64)
            == Some(expected_document_version)
        && payload.get("principal_id").and_then(Value::as_str) == Some(principal_id)
        && payload.get("authorization_principal")
            == serde_json::to_value(&authorization.principal).ok().as_ref()
        && payload.get("authorization_basis").and_then(Value::as_str)
            == Some(authorization.authorization_basis.as_str())
        && payload.get("authorization_action").and_then(Value::as_str)
            == Some(authorization.action.as_str())
        && payload
            .get("authorization_event_id")
            .and_then(Value::as_str)
            == Some(authorization.event_id.as_str())
        && payload
            .get("authorization_occurred_at")
            .and_then(Value::as_str)
            == Some(authorization.occurred_at.as_str())
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
        baseline_revision_id: row.try_get("baseline_revision_id")?,
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

fn document_approval_record_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> ApiResult<db::ProjectDocumentApprovalRecord> {
    Ok(db::ProjectDocumentApprovalRecord {
        id: row.try_get("id")?,
        document_id: row.try_get("document_id")?,
        revision_id: row.try_get("revision_id")?,
        principal_type: row.try_get("principal_type")?,
        principal_id: row.try_get("principal_id")?,
        authorization_basis: row.try_get("authorization_basis")?,
        authorization_action: row.try_get("authorization_action")?,
        explicit_event: row.try_get("explicit_event")?,
        authorization_occurred_at: row.try_get("authorization_occurred_at")?,
        content_digest: row.try_get("content_digest")?,
        rendered_digest: row.try_get("rendered_digest")?,
        lifecycle: row.try_get("lifecycle")?,
        idempotency_key: row.try_get("idempotency_key")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
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

fn decision_class_name(value: DecisionClass) -> &'static str {
    match value {
        DecisionClass::UserScope => "user_scope",
        DecisionClass::ProjectImplementation => "project_implementation",
        DecisionClass::Policy => "policy",
        DecisionClass::Waiver => "waiver",
    }
}

fn parse_document_kind(value: &str) -> Option<ProjectDocumentKind> {
    services::parse_document_kind(value)
}

fn principal_kind_name(kind: PrincipalKind) -> &'static str {
    match kind {
        PrincipalKind::User => "user",
        PrincipalKind::Agent => "agent",
        PrincipalKind::Worker => "worker",
        PrincipalKind::Reviewer => "reviewer",
        PrincipalKind::Service => "service",
        PrincipalKind::System => "system",
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

fn map_event_error(error: db::DbError) -> ApiError {
    match error {
        db::DbError::Check(message) if message.contains("dedupe key") => {
            ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different mutation",
            )
        }
        other => other.into(),
    }
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
    fn document_content_must_match_document_kind() {
        let content = ProjectDocumentContent::Research(api_types::ResearchDocumentContent {
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
        });
        assert!(validate_content_kind(ProjectDocumentKind::Research, &content).is_ok());
        assert!(validate_content_kind(ProjectDocumentKind::Architecture, &content).is_err());
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
            schema_version: DOCUMENT_SCHEMA_VERSION.to_owned(),
            render_version: DOCUMENT_RENDER_VERSION.to_owned(),
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
            baseline_revision_id: None,
            source_refs_json: "[]".to_owned(),
            affected_records_json: "{}".to_owned(),
            supersedes_decision_id: None,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
        };
        assert!(decision_to_api(record).is_err());
    }

    #[test]
    fn document_approval_replay_requires_the_complete_authorization_provenance() {
        let authorization = AuthorizationProvenance {
            principal: PrincipalRef {
                kind: PrincipalKind::User,
                id: "user-1".to_owned(),
                display_name: None,
            },
            authorization_basis: "explicit_user_approval".to_owned(),
            action: DOCUMENT_APPROVE_ACTION.to_owned(),
            event_id: "event-1".to_owned(),
            occurred_at: "2026-08-13T00:00:00Z".to_owned(),
        };
        let payload = serde_json::json!({
            "project_id": "project-1",
            "document_id": "document-1",
            "revision_id": "revision-1",
            "content_digest": "content-1",
            "render_digest": "render-1",
            "expected_document_version": 2,
            "principal_id": "user-1",
            "authorization_principal": authorization.principal,
            "authorization_basis": authorization.authorization_basis,
            "authorization_action": authorization.action,
            "authorization_event_id": authorization.event_id,
            "authorization_occurred_at": authorization.occurred_at,
        });
        assert!(document_approval_replay_matches(
            &payload,
            "project-1",
            "document-1",
            "revision-1",
            "content-1",
            "render-1",
            2,
            "user-1",
            &authorization,
        ));
        for field in [
            "authorization_basis",
            "authorization_action",
            "authorization_event_id",
            "authorization_occurred_at",
        ] {
            let mut tampered = payload.clone();
            tampered[field] = Value::String("tampered".to_owned());
            assert!(!document_approval_replay_matches(
                &tampered,
                "project-1",
                "document-1",
                "revision-1",
                "content-1",
                "render-1",
                2,
                "user-1",
                &authorization,
            ));
        }
    }
}
