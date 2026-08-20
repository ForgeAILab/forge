//! Project execution-baseline proposal, approval, and activation routes.
//!
//! A baseline is an immutable, digest-addressed execution contract.  The
//! route owns the public authorization and idempotency boundary; the database
//! owns the immutable rows and the Task-governance trigger.  In particular,
//! activation and promotion of pre-planned Tasks are one SQLite transaction,
//! so a restart cannot leave an approved baseline with half-promoted Tasks.

use api_types::{
    ApproveExecutionBaselineRequest, ArtifactRef, AuthorizationProvenance,
    CreateExecutionBaselineRequest, ExecutionBaseline, ExecutionBaselineApproval,
    ExecutionBaselineContent, ExecutionBaselineLifecycle, ExecutionBaselineResponse,
    ExecutionBaselineRevision, PrincipalKind, PrincipalRef, RevisionProvenance,
    SaveExecutionBaselineRevisionRequest,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use db::{
    new_uuid_v4, now_rfc3339, ProjectExecutionBaselineRecord, ProjectMemberRepo,
    ProjectOrchestrationRepo, ProjectRepo,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use services::{
    baseline_column_json, render_execution_baseline, validate_execution_baseline_policy,
    EXECUTION_BASELINE_RENDER_VERSION, EXECUTION_BASELINE_SCHEMA_VERSION,
};
use sqlx::{Row, Sqlite, Transaction};

use crate::{
    errors::{ApiError, ApiResult},
    routes::{auth::AuthenticatedUser, client_idempotency_key, scoped_idempotency_key},
    state::AppState,
};

const CREATE_ACTION: &str = "project.execution_baseline.propose";
const REVISE_ACTION: &str = "project.execution_baseline.revise";
const APPROVE_ACTION: &str = "project.execution_baseline.approve";
const ACTIVATE_ACTION: &str = "project.execution_baseline.activate";
const MANIFEST_SCHEMA: &str = "forge.execution-baseline-manifest/v1";
const MAX_AUTHORIZATION_CLOCK_SKEW_SECONDS: i64 = 48 * 60 * 60;

/// The normalized V076 columns are query projections.  This manifest keeps
/// the exact closed content, review view, and provenance that the user saw;
/// `api_revision` rejects a row if any of those authoritative values are
/// missing or no longer reproduce its persisted digests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BaselineManifest {
    schema: String,
    content: ExecutionBaselineContent,
    rendered_view: String,
    provenance: RevisionProvenance,
}

pub async fn get_execution_baseline(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<Json<ExecutionBaselineResponse>> {
    let _project = authorized_project(&state, &user.user_id, &project_id).await?;
    let baseline_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM project_execution_baseline
         WHERE project_id = ?
         ORDER BY CASE lifecycle WHEN 'active' THEN 0 WHEN 'approved' THEN 1
                                WHEN 'proposed' THEN 2 WHEN 'draft' THEN 3 ELSE 4 END,
                  updated_at DESC, id DESC LIMIT 1",
    )
    .bind(&project_id)
    .fetch_optional(state.db.pool())
    .await?;
    let Some(baseline_id) = baseline_id else {
        return Err(ApiError::not_found("execution_baseline", project_id));
    };
    Ok(Json(
        load_response(&state, &project_id, &baseline_id).await?,
    ))
}

pub async fn create_execution_baseline(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<CreateExecutionBaselineRequest>,
) -> ApiResult<(StatusCode, Json<ExecutionBaselineResponse>)> {
    let project = authorized_project(&state, &user.user_id, &project_id).await?;
    validate_user_authorization(
        &request.mutation.authorization,
        &user.user_id,
        CREATE_ACTION,
    )?;
    let actor_type = "user";
    let actor_id = user.user_id.clone();
    validate_idempotency_key(&request.mutation.idempotency_key)?;

    // The shell primary key is server-minted (matching the Charter
    // precedent): clients and agents never supply a baseline id, so the
    // replay identity below deliberately excludes it.
    let input = json!({
        "project_id": project_id,
        "expected_project_version": request.mutation.expected_version,
        "authorization": request.mutation.authorization,
    });
    if let Some(result) = replay_event(
        &state,
        &event_key("create", &request.mutation.idempotency_key),
        &input,
    )
    .await?
    {
        let baseline_id = result
            .get("baseline_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::internal("execution-baseline replay is missing its result"))?;
        return Ok((
            StatusCode::OK,
            Json(load_response(&state, &project_id, baseline_id).await?),
        ));
    }
    if request.mutation.expected_version != project.version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before the execution baseline was proposed",
        ));
    }
    let now = now_rfc3339();
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let current_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
        .bind(&project_id)
        .fetch_one(&mut *tx)
        .await?;
    if current_version != request.mutation.expected_version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before the execution baseline was proposed",
        ));
    }
    let baseline_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_execution_baseline
             (id, project_id, current_revision_id, lifecycle, version, created_at, updated_at)
         VALUES (?, ?, NULL, 'draft', 1, ?, ?)",
    )
    .bind(&baseline_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    let advanced = sqlx::query(
        "UPDATE project SET version = version + 1, updated_at = ?
         WHERE id = ? AND version = ?",
    )
    .bind(&now)
    .bind(&project_id)
    .bind(request.mutation.expected_version)
    .execute(&mut *tx)
    .await?;
    if advanced.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before the execution baseline was proposed",
        ));
    }
    append_event(
        &mut tx,
        "project.execution_baseline.proposed",
        "execution_baseline",
        &baseline_id,
        actor_type,
        Some(&actor_id),
        &project_id,
        &request.mutation.idempotency_key,
        &event_key("create", &request.mutation.idempotency_key),
        json!({
            "input": input,
            "result": {"baseline_id": baseline_id},
        }),
        &now,
    )
    .await?;
    tx.commit().await?;

    Ok((
        StatusCode::CREATED,
        Json(load_response(&state, &project_id, &baseline_id).await?),
    ))
}

pub async fn save_execution_baseline_revision(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, baseline_id)): Path<(String, String)>,
    Json(request): Json<SaveExecutionBaselineRevisionRequest>,
) -> ApiResult<(StatusCode, Json<ExecutionBaselineResponse>)> {
    let project = authorized_project(&state, &user.user_id, &project_id).await?;
    validate_user_authorization(
        &request.mutation.authorization,
        &user.user_id,
        REVISE_ACTION,
    )?;
    let actor_type = "user";
    let actor_id = user.user_id.clone();
    validate_idempotency_key(&request.mutation.idempotency_key)?;
    let rendered = render_execution_baseline(&request.content)
        .map_err(|error| ApiError::bad_request(format!("invalid execution baseline: {error}")))?;
    if request.render_version != EXECUTION_BASELINE_RENDER_VERSION
        || request.rendered_view != rendered.rendered_view
        || request.content_digest != rendered.content_digest
        || request.render_digest != rendered.render_digest
    {
        return Err(ApiError::conflict_with_code(
            "baseline_digest_conflict",
            "the execution baseline review view or digest does not match Forge's canonical renderer",
        ));
    }
    validate_baseline_content(&state, &project, &request.content).await?;

    let input = json!({
        "project_id": project_id,
        "baseline_id": baseline_id,
        "expected_baseline_version": request.mutation.expected_version,
        "expected_digest": request.mutation.expected_digest,
        "base_revision_id": request.base_revision_id,
        "content": request.content,
        "rendered_view": request.rendered_view,
        "render_version": request.render_version,
        "content_digest": request.content_digest,
        "render_digest": request.render_digest,
        "provenance": request.provenance,
        "authorization": request.mutation.authorization,
    });
    if let Some(result) = replay_event(
        &state,
        &event_key("revision", &request.mutation.idempotency_key),
        &input,
    )
    .await?
    {
        let replay_baseline_id = result
            .get("baseline_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::internal("execution-baseline replay is missing its result"))?;
        return Ok((
            StatusCode::OK,
            Json(load_response(&state, &project_id, replay_baseline_id).await?),
        ));
    }

    let baseline = sqlx::query(
        "SELECT project_id, lifecycle, version, current_revision_id
         FROM project_execution_baseline WHERE id = ?",
    )
    .bind(&baseline_id)
    .fetch_optional(state.db.pool())
    .await?
    .ok_or_else(|| ApiError::not_found("execution_baseline", baseline_id.clone()))?;
    let baseline_project: String = baseline.try_get("project_id")?;
    if baseline_project != project_id {
        return Err(ApiError::not_found("execution_baseline", baseline_id));
    }
    let baseline_version: i64 = baseline.try_get("version")?;
    let baseline_lifecycle: String = baseline.try_get("lifecycle")?;
    let current_revision_id: Option<String> = baseline.try_get("current_revision_id")?;
    if baseline_version != request.mutation.expected_version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the execution baseline changed before this revision was saved",
        ));
    }

    let (base_revision, base_revision_id) = if let Some(base_id) =
        request.base_revision_id.as_deref()
    {
        let base = sqlx::query(
            "SELECT baseline_id, revision, content_digest
             FROM project_execution_baseline_revision WHERE id = ?",
        )
        .bind(base_id)
        .fetch_optional(state.db.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("execution_baseline_revision", base_id.to_owned()))?;
        let base_baseline: String = base.try_get("baseline_id")?;
        if base_baseline != baseline_id {
            return Err(ApiError::conflict_with_code(
                "base_revision_conflict",
                "the base revision belongs to a different execution baseline",
            ));
        }
        let base_digest: String = base.try_get("content_digest")?;
        if request.mutation.expected_digest.as_deref() != Some(base_digest.as_str()) {
            return Err(ApiError::conflict_with_code(
                "digest_conflict",
                "the execution baseline base digest is stale",
            ));
        }
        if current_revision_id.as_deref() != Some(base_id) {
            return Err(ApiError::conflict_with_code(
                "base_revision_conflict",
                "the execution baseline base revision is not its current revision",
            ));
        }
        (base.try_get("revision")?, Some(base_id.to_owned()))
    } else {
        if request.mutation.expected_digest.is_some() || current_revision_id.is_some() {
            return Err(ApiError::conflict_with_code(
                "digest_conflict",
                "an existing execution baseline revision requires an exact base_revision_id and digest",
            ));
        }
        (0, None)
    };

    let columns = baseline_column_json(&request.content)
        .map_err(|error| ApiError::bad_request(format!("invalid execution baseline: {error}")))?;
    let manifest = BaselineManifest {
        schema: MANIFEST_SCHEMA.to_owned(),
        content: request.content.clone(),
        rendered_view: request.rendered_view.clone(),
        provenance: request.provenance.clone(),
    };
    let manifest_json = serde_json::to_string(&manifest).map_err(|error| {
        ApiError::internal(format!("cannot persist baseline manifest: {error}"))
    })?;
    let milestone_ids_json =
        serde_json::to_string(&request.content.milestone_ids).map_err(|error| {
            ApiError::internal(format!("cannot persist milestone references: {error}"))
        })?;
    let milestone_definition_revision_ids_json = serde_json::to_string(
        &request.content.milestone_definition_revision_ids,
    )
    .map_err(|error| {
        ApiError::internal(format!(
            "cannot persist milestone definition references: {error}"
        ))
    })?;
    let now = now_rfc3339();
    let revision_id = new_uuid_v4();
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let tx_baseline = sqlx::query(
        "SELECT project_id, lifecycle, version, current_revision_id
         FROM project_execution_baseline WHERE id = ?",
    )
    .bind(&baseline_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::not_found("execution_baseline", baseline_id.clone()))?;
    let tx_project: String = tx_baseline.try_get("project_id")?;
    let tx_version: i64 = tx_baseline.try_get("version")?;
    if tx_project != project_id || tx_version != request.mutation.expected_version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the execution baseline changed before this revision was saved",
        ));
    }
    let revision_number: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(revision), 0) + 1
         FROM project_execution_baseline_revision WHERE baseline_id = ?",
    )
    .bind(&baseline_id)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO project_execution_baseline_revision (
            id, baseline_id, revision, base_revision, base_revision_id, lifecycle,
            charter_revision_id, document_revisions_json, plan_items_json,
            milestone_id, milestone_ids_json, milestone_definition_revision_ids_json,
            primary_milestone_id, release_policy_json,
            release_policy_revision, release_policy_digest, acceptance_matrix_json,
            capability_classes_json, risk_classes_json, adaptive_envelope_json,
            elevated_operations_json, exclusions_json, rollback_recovery_json,
            schema_version, render_version, rendered_view, content_digest,
            rendered_digest, source_refs_json, created_at
         ) VALUES (?, ?, ?, ?, ?, 'proposed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&revision_id)
    .bind(&baseline_id)
    .bind(revision_number)
    .bind(base_revision)
    .bind(base_revision_id.as_deref())
    .bind(&request.content.charter_revision.revision_id)
    .bind(&columns.document_revisions_json)
    .bind(&columns.plan_items_json)
    .bind(columns.milestone_id.as_deref())
    .bind(&milestone_ids_json)
    .bind(&milestone_definition_revision_ids_json)
    .bind(columns.primary_milestone_id.as_deref())
    .bind(&columns.release_policy_json)
    .bind(&request.content.release_policy_revision)
    .bind(&request.content.release_policy_digest)
    .bind(&columns.acceptance_matrix_json)
    .bind(&columns.capability_classes_json)
    .bind(&columns.risk_classes_json)
    .bind(&columns.adaptive_envelope_json)
    .bind(&columns.elevated_operations_json)
    .bind(&columns.exclusions_json)
    .bind(&columns.rollback_recovery_json)
    .bind(EXECUTION_BASELINE_SCHEMA_VERSION)
    .bind(&request.render_version)
    .bind(&request.rendered_view)
    .bind(&request.content_digest)
    .bind(&request.render_digest)
    .bind(&manifest_json)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    // Proposed revisions on a draft/proposed baseline are its visible draft;
    // an active baseline keeps its approved pointer until the proposal is
    // separately approved, so a draft cannot become authoritative by save.
    let advanced = sqlx::query(
        "UPDATE project_execution_baseline
         SET current_revision_id = CASE
                 WHEN lifecycle IN ('draft', 'proposed') THEN ?
                 ELSE current_revision_id
             END,
             version = version + 1, updated_at = ?
         WHERE id = ? AND version = ?",
    )
    .bind(&revision_id)
    .bind(&now)
    .bind(&baseline_id)
    .bind(request.mutation.expected_version)
    .execute(&mut *tx)
    .await?;
    if advanced.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the execution baseline changed before this revision was saved",
        ));
    }
    append_event(
        &mut tx,
        "project.execution_baseline.revised",
        "execution_baseline",
        &baseline_id,
        actor_type,
        Some(&actor_id),
        &project_id,
        &request.mutation.idempotency_key,
        &event_key("revision", &request.mutation.idempotency_key),
        json!({
            "input": input,
            "result": {"baseline_id": baseline_id, "revision_id": revision_id},
        }),
        &now,
    )
    .await?;
    tx.commit().await?;

    // Keep the pre-transaction read variables used above honest in debug
    // builds: all authoritative checks were repeated inside the transaction.
    let _ = (baseline_lifecycle, project.version);
    Ok((
        StatusCode::CREATED,
        Json(load_response(&state, &project_id, &baseline_id).await?),
    ))
}

pub async fn approve_execution_baseline(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, baseline_id, revision_id)): Path<(String, String, String)>,
    Json(request): Json<ApproveExecutionBaselineRequest>,
) -> ApiResult<(StatusCode, Json<ExecutionBaselineResponse>)> {
    let project = authorized_project(&state, &user.user_id, &project_id).await?;
    validate_idempotency_key(&request.mutation.idempotency_key)?;
    let storage_idempotency_key = scoped_idempotency_key(
        "baseline-approval",
        &project_id,
        &user.user_id,
        &request.mutation.idempotency_key,
    );
    if request.revision_id != revision_id {
        return Err(ApiError::conflict_with_code(
            "idempotency_conflict",
            "approval idempotency key target does not match the request path",
        ));
    }
    let input = json!({
        "project_id": project_id,
        "baseline_id": baseline_id,
        "revision_id": revision_id,
        "expected_baseline_version": request.mutation.expected_version,
        "expected_project_version": request.expected_project_version,
        "content_digest": request.content_digest,
        "render_digest": request.render_digest,
        "authorization": request.mutation.authorization,
    });
    if let Some(result) = replay_event(
        &state,
        &event_key("approval", &storage_idempotency_key),
        &input,
    )
    .await?
    {
        let replay_baseline_id = result
            .get("baseline_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::internal("execution-baseline replay is missing its result"))?;
        return Ok((
            StatusCode::OK,
            Json(load_response(&state, &project_id, replay_baseline_id).await?),
        ));
    }
    // Only a new mutation reaches current-auth validation. The immutable
    // replay above compares the complete envelope (including
    // principal/action/time) and must return either the stored result or an
    // idempotency conflict even when the replayed envelope is no longer a
    // currently valid authorization request.
    validate_user_authorization(
        &request.mutation.authorization,
        &user.user_id,
        APPROVE_ACTION,
    )?;
    if request.expected_project_version != project.version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed while the user reviewed this baseline",
        ));
    }

    if let Some(existing) = sqlx::query(
        "SELECT baseline_id, revision_id, expected_project_version,
                principal_type, principal_id, authorization_basis,
                authorization_action, authorization_occurred_at, explicit_event,
                content_digest, rendered_digest
         FROM project_execution_baseline_approval WHERE idempotency_key = ?",
    )
    .bind(&storage_idempotency_key)
    .fetch_optional(state.db.pool())
    .await?
    {
        let exact = existing.try_get::<String, _>("baseline_id")? == baseline_id
            && existing.try_get::<String, _>("revision_id")? == revision_id
            && existing.try_get::<i64, _>("expected_project_version")?
                == request.expected_project_version
            && existing.try_get::<String, _>("principal_type")? == "user"
            && request.mutation.authorization.principal.kind == PrincipalKind::User
            && existing.try_get::<String, _>("principal_id")?
                == request.mutation.authorization.principal.id
            && existing.try_get::<String, _>("authorization_basis")?
                == request.mutation.authorization.authorization_basis
            && existing.try_get::<String, _>("authorization_action")?
                == request.mutation.authorization.action
            && existing.try_get::<String, _>("authorization_occurred_at")?
                == request.mutation.authorization.occurred_at
            && existing.try_get::<String, _>("explicit_event")?
                == request.mutation.authorization.event_id
            && existing.try_get::<String, _>("content_digest")? == request.content_digest
            && existing.try_get::<String, _>("rendered_digest")? == request.render_digest;
        if !exact {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "approval idempotency key was already used for different input",
            ));
        }
        return Ok((
            StatusCode::OK,
            Json(load_response(&state, &project_id, &baseline_id).await?),
        ));
    }

    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let baseline = sqlx::query(
        "SELECT project_id, version, lifecycle FROM project_execution_baseline WHERE id = ?",
    )
    .bind(&baseline_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::not_found("execution_baseline", baseline_id.clone()))?;
    if baseline.try_get::<String, _>("project_id")? != project_id
        || baseline.try_get::<i64, _>("version")? != request.mutation.expected_version
    {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the execution baseline changed before approval",
        ));
    }
    let revision = sqlx::query(
        "SELECT lifecycle, content_digest, rendered_digest
         FROM project_execution_baseline_revision
         WHERE id = ? AND baseline_id = ?",
    )
    .bind(&revision_id)
    .bind(&baseline_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::not_found("execution_baseline_revision", revision_id.clone()))?;
    let revision_lifecycle: String = revision.try_get("lifecycle")?;
    if matches!(revision_lifecycle.as_str(), "superseded" | "revoked")
        || revision.try_get::<String, _>("content_digest")? != request.content_digest
        || revision.try_get::<String, _>("rendered_digest")? != request.render_digest
    {
        return Err(ApiError::conflict_with_code(
            "baseline_digest_conflict",
            "approval does not target the exact proposed baseline revision",
        ));
    }
    let persisted_manifest =
        validate_persisted_manifest_in_tx(&mut tx, &project_id, &baseline_id, &revision_id).await?;
    let persisted_rendered =
        render_execution_baseline(&persisted_manifest.content).map_err(|error| {
            ApiError::internal(format!("persisted execution baseline is invalid: {error}"))
        })?;
    if persisted_rendered.content_digest != request.content_digest
        || persisted_rendered.render_digest != request.render_digest
    {
        return Err(ApiError::conflict_with_code(
            "baseline_digest_conflict",
            "approval does not target the exact persisted baseline review target",
        ));
    }
    let tx_project_version: i64 = sqlx::query_scalar(
        "SELECT version FROM project WHERE id = ? AND charter_status = 'charter_backed'",
    )
    .bind(&project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::not_found("project", project_id.clone()))?;
    if tx_project_version != request.expected_project_version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed while the user reviewed this baseline",
        ));
    }
    if sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM project_execution_baseline_approval
         WHERE baseline_id = ? AND revision_id = ?
           AND principal_type = 'user'
           AND authorization_action = ?
           AND length(trim(authorization_basis)) > 0
           AND length(trim(authorization_occurred_at)) > 0
           AND length(trim(explicit_event)) > 0
           AND content_digest = ? AND rendered_digest = ?
           AND lifecycle IN ('active', 'consumed')",
    )
    .bind(&baseline_id)
    .bind(&revision_id)
    .bind(APPROVE_ACTION)
    .bind(&request.content_digest)
    .bind(&request.render_digest)
    .fetch_one(&mut *tx)
    .await?
        > 0
    {
        return Err(ApiError::conflict_with_code(
            "baseline_approval_conflict",
            "this exact baseline revision already has an authoritative approval receipt",
        ));
    }
    let now = now_rfc3339();
    let baseline_lifecycle: String = baseline.try_get("lifecycle")?;
    if baseline_lifecycle != "active" {
        sqlx::query(
            "UPDATE project_execution_baseline_revision SET lifecycle = 'superseded'
             WHERE baseline_id = ? AND lifecycle = 'approved' AND id != ?",
        )
        .bind(&baseline_id)
        .bind(&revision_id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "UPDATE project_execution_baseline_revision SET lifecycle = 'approved'
         WHERE id = ? AND baseline_id = ? AND lifecycle IN ('proposed', 'approved')",
    )
    .bind(&revision_id)
    .bind(&baseline_id)
    .execute(&mut *tx)
    .await?;
    let promoted = if baseline_lifecycle == "active" {
        sqlx::query(
            "UPDATE project_execution_baseline
             SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND lifecycle = 'active'",
        )
        .bind(&now)
        .bind(&baseline_id)
        .bind(request.mutation.expected_version)
        .execute(&mut *tx)
        .await?
    } else {
        sqlx::query(
            "UPDATE project_execution_baseline
             SET lifecycle = 'approved', current_revision_id = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND lifecycle IN ('draft', 'proposed', 'approved')",
        )
        .bind(&revision_id)
        .bind(&now)
        .bind(&baseline_id)
        .bind(request.mutation.expected_version)
        .execute(&mut *tx)
        .await?
    };
    if promoted.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the execution baseline changed before approval",
        ));
    }
    if baseline_lifecycle != "active" {
        sqlx::query(
            "UPDATE project_execution_baseline_approval SET lifecycle = 'revoked', updated_at = ?
             WHERE baseline_id = ? AND lifecycle = 'active' AND revision_id != ?",
        )
        .bind(&now)
        .bind(&baseline_id)
        .bind(&revision_id)
        .execute(&mut *tx)
        .await?;
    }
    let approval_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_execution_baseline_approval (
             id, baseline_id, revision_id, expected_project_version,
             principal_type, principal_id, authorization_basis,
             authorization_action, authorization_occurred_at, explicit_event,
             content_digest, rendered_digest, lifecycle, idempotency_key,
             created_at, updated_at
         ) VALUES (?, ?, ?, ?, 'user', ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)",
    )
    .bind(&approval_id)
    .bind(&baseline_id)
    .bind(&revision_id)
    .bind(request.expected_project_version)
    .bind(&user.user_id)
    .bind(&request.mutation.authorization.authorization_basis)
    .bind(&request.mutation.authorization.action)
    .bind(&request.mutation.authorization.occurred_at)
    .bind(&request.mutation.authorization.event_id)
    .bind(&request.content_digest)
    .bind(&request.render_digest)
    .bind(&storage_idempotency_key)
    .bind(&now)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    append_event(
        &mut tx,
        "project.execution_baseline.approved",
        "execution_baseline",
        &baseline_id,
        "user",
        Some(&user.user_id),
        &project_id,
        &request.mutation.idempotency_key,
        &event_key("approval", &storage_idempotency_key),
        json!({
            "input": input,
            "result": {"baseline_id": baseline_id, "revision_id": revision_id, "approval_id": approval_id},
        }),
        &now,
    )
    .await?;
    tx.commit().await?;

    Ok((
        StatusCode::CREATED,
        Json(load_response(&state, &project_id, &baseline_id).await?),
    ))
}

pub async fn activate_execution_baseline(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, baseline_id)): Path<(String, String)>,
    Json(request): Json<api_types::ActivateExecutionBaselineRequest>,
) -> ApiResult<Json<ExecutionBaselineResponse>> {
    let project = authorized_project(&state, &user.user_id, &project_id).await?;
    validate_idempotency_key(&request.mutation.idempotency_key)?;
    if request.baseline_id != baseline_id {
        return Err(ApiError::conflict_with_code(
            "idempotency_conflict",
            "activation idempotency key target does not match the request path",
        ));
    }
    let input = json!({
        "project_id": project_id,
        "baseline_id": baseline_id,
        "revision_id": request.revision_id,
        "approval_id": request.approval_id,
        "expected_project_version": request.mutation.expected_version,
        "content_digest": request.content_digest,
        "render_digest": request.render_digest,
        "authorization": request.mutation.authorization,
    });
    if let Some(result) = replay_event(
        &state,
        &event_key("activation", &request.mutation.idempotency_key),
        &input,
    )
    .await?
    {
        let replay_baseline_id = result
            .get("baseline_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::internal("execution-baseline replay is missing its result"))?;
        return Ok(Json(
            load_response(&state, &project_id, replay_baseline_id).await?,
        ));
    }
    // Current authorization is evaluated only for a new activation. Replay
    // identity is the immutable full input envelope; altered authority or
    // target must be reported as an idempotency conflict rather than being
    // re-authorized as a new mutation.
    validate_user_authorization(
        &request.mutation.authorization,
        &user.user_id,
        ACTIVATE_ACTION,
    )?;
    if request.mutation.expected_version != project.version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before baseline activation",
        ));
    }

    let now = now_rfc3339();
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let approval = sqlx::query(
        "SELECT a.baseline_id, a.revision_id, a.expected_project_version,
                a.principal_type, a.principal_id, a.content_digest,
                a.rendered_digest, a.authorization_basis,
                a.authorization_action, a.authorization_occurred_at,
                a.explicit_event, a.lifecycle, b.project_id, b.version,
                b.lifecycle AS baseline_lifecycle, r.lifecycle AS revision_lifecycle,
                r.content_digest AS revision_content_digest,
                r.rendered_digest AS revision_rendered_digest
         FROM project_execution_baseline_approval a
         JOIN project_execution_baseline b ON b.id = a.baseline_id
         JOIN project_execution_baseline_revision r
           ON r.id = a.revision_id AND r.baseline_id = a.baseline_id
         WHERE a.id = ?",
    )
    .bind(&request.approval_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| {
        ApiError::not_found("execution_baseline_approval", request.approval_id.clone())
    })?;
    let exact = approval.try_get::<String, _>("baseline_id")? == baseline_id
        && approval.try_get::<String, _>("revision_id")? == request.revision_id
        && approval.try_get::<String, _>("project_id")? == project_id
        && approval.try_get::<i64, _>("expected_project_version")?
            == request.mutation.expected_version
        && approval.try_get::<String, _>("principal_type")? == "user"
        && approval.try_get::<String, _>("principal_id")? == user.user_id
        // The receipt's approval envelope is a separate user action from
        // this activation envelope.  Verify all durable receipt fields so a
        // tampered/legacy partial receipt can never authorize activation.
        && approval.try_get::<String, _>("authorization_action")? == APPROVE_ACTION
        && !approval
            .try_get::<String, _>("authorization_basis")?
            .trim()
            .is_empty()
        && !approval
            .try_get::<String, _>("authorization_occurred_at")?
            .trim()
            .is_empty()
        && valid_authorization_timestamp(
            &approval.try_get::<String, _>("authorization_occurred_at")?,
        )
        && !approval
            .try_get::<String, _>("explicit_event")?
            .trim()
            .is_empty()
        && approval.try_get::<String, _>("content_digest")? == request.content_digest
        && approval.try_get::<String, _>("rendered_digest")? == request.render_digest
        && approval.try_get::<String, _>("revision_content_digest")? == request.content_digest
        && approval.try_get::<String, _>("revision_rendered_digest")? == request.render_digest
        && matches!(
            approval
                .try_get::<String, _>("baseline_lifecycle")?
                .as_str(),
            "approved" | "active"
        )
        && approval.try_get::<String, _>("revision_lifecycle")? == "approved"
        && approval.try_get::<String, _>("lifecycle")? == "active";
    if !exact {
        return Err(ApiError::conflict_with_code(
            "baseline_approval_conflict",
            "activation requires the exact active user approval receipt",
        ));
    }
    let persisted_manifest =
        validate_persisted_manifest_in_tx(&mut tx, &project_id, &baseline_id, &request.revision_id)
            .await?;
    let persisted_rendered =
        render_execution_baseline(&persisted_manifest.content).map_err(|error| {
            ApiError::internal(format!("persisted execution baseline is invalid: {error}"))
        })?;
    if persisted_rendered.content_digest != request.content_digest
        || persisted_rendered.render_digest != request.render_digest
    {
        return Err(ApiError::conflict_with_code(
            "baseline_digest_conflict",
            "activation does not target the exact persisted baseline review target",
        ));
    }
    let current_project_version: i64 = sqlx::query_scalar(
        "SELECT version FROM project WHERE id = ? AND charter_status = 'charter_backed'",
    )
    .bind(&project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::not_found("project", project_id.clone()))?;
    if current_project_version != request.mutation.expected_version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before baseline activation",
        ));
    }
    let baseline_version: i64 = approval.try_get("version")?;
    let prior_active: Option<String> = sqlx::query_scalar(
        "SELECT id FROM project_execution_baseline
         WHERE project_id = ? AND lifecycle = 'active' AND id != ? LIMIT 1",
    )
    .bind(&project_id)
    .bind(&baseline_id)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(prior_active) = prior_active {
        sqlx::query(
            "UPDATE project_execution_baseline
             SET lifecycle = 'superseded', version = version + 1, updated_at = ?
             WHERE id = ? AND lifecycle = 'active'",
        )
        .bind(&now)
        .bind(&prior_active)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE project_task_governance SET runnable = 0, version = version + 1, updated_at = ?
             WHERE project_id = ? AND baseline_id = ? AND runnable = 1",
        )
        .bind(&now)
        .bind(&project_id)
        .bind(&prior_active)
        .execute(&mut *tx)
        .await?;
    }
    // A baseline may already be active while this exact approved successor is
    // being activated.  Retire the old runnable projection in the same
    // transaction; otherwise an old governance row could retain `runnable=1`
    // even though the baseline current pointer moved to the successor.  The
    // execution gate still checks the pointer, but keeping the projection
    // stale creates an unsafe race for alternate readers/dispatchers.
    sqlx::query(
        "UPDATE project_task_governance
         SET runnable = 0, version = version + 1, updated_at = ?
         WHERE project_id = ? AND baseline_id = ?
           AND baseline_revision_id != ? AND runnable = 1",
    )
    .bind(&now)
    .bind(&project_id)
    .bind(&baseline_id)
    .bind(&request.revision_id)
    .execute(&mut *tx)
    .await?;
    let active = sqlx::query(
        "UPDATE project_execution_baseline
         SET lifecycle = 'active', current_revision_id = ?,
             version = version + 1, updated_at = ?
         WHERE id = ? AND project_id = ? AND version = ?
           AND lifecycle IN ('approved', 'active', 'proposed')",
    )
    .bind(&request.revision_id)
    .bind(&now)
    .bind(&baseline_id)
    .bind(&project_id)
    .bind(baseline_version)
    .execute(&mut *tx)
    .await?;
    if active.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the execution baseline changed before activation",
        ));
    }
    activate_manifest_milestones_in_tx(&mut tx, &project_id, &persisted_manifest.content, &now)
        .await?;
    sqlx::query(
        "UPDATE project_execution_baseline_revision SET lifecycle = 'superseded'
         WHERE baseline_id = ? AND lifecycle = 'approved' AND id != ?",
    )
    .bind(&baseline_id)
    .bind(&request.revision_id)
    .execute(&mut *tx)
    .await?;
    // An activated baseline names the Project's primary milestone (or covers
    // exactly one). A Project with active milestones but no explicit primary
    // pointer refuses every agent-turn admission, so activation fills a NULL
    // pointer here with the same validity predicate the explicit
    // `project.milestone.primary.set` operation applies: the milestone must
    // be this Project's and active. An existing pointer is never overridden.
    let manifest_primary_milestone_id = persisted_manifest
        .content
        .primary_milestone_id
        .clone()
        .or_else(
            || match persisted_manifest.content.milestone_ids.as_slice() {
                [only] => Some(only.clone()),
                _ => None,
            },
        );
    let project_update = sqlx::query(
        "UPDATE project
         SET primary_milestone_id = COALESCE(
                 primary_milestone_id,
                 (SELECT id FROM project_milestone
                  WHERE id = ? AND project_id = ? AND lifecycle = 'active')),
             version = version + 1, updated_at = ?
         WHERE id = ? AND version = ?",
    )
    .bind(manifest_primary_milestone_id.as_deref())
    .bind(&project_id)
    .bind(&now)
    .bind(&project_id)
    .bind(request.mutation.expected_version)
    .execute(&mut *tx)
    .await?;
    if project_update.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before baseline activation",
        ));
    }
    // This promotion is deliberately in the same transaction as the
    // authoritative baseline/project transition.  It is repairable by the
    // gate even if a future migration adds another derived projection.
    // Besides flipping rows already bound to this exact revision, the shared
    // pass re-binds preplanned Tasks whose plan item the activated baseline
    // covers, so work proposed before activation becomes runnable too.
    db::promote_baseline_task_governance_in_tx(
        &mut tx,
        &project_id,
        &baseline_id,
        &request.revision_id,
        &now,
    )
    .await?;
    let consumed = sqlx::query(
        "UPDATE project_execution_baseline_approval SET lifecycle = 'consumed', updated_at = ?
         WHERE id = ? AND lifecycle = 'active'",
    )
    .bind(&now)
    .bind(&request.approval_id)
    .execute(&mut *tx)
    .await?;
    if consumed.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "baseline_approval_conflict",
            "the approval was consumed concurrently",
        ));
    }
    append_event(
        &mut tx,
        "project.execution_baseline.activated",
        "execution_baseline",
        &baseline_id,
        "user",
        Some(&user.user_id),
        &project_id,
        &request.mutation.idempotency_key,
        &event_key("activation", &request.mutation.idempotency_key),
        json!({
            "input": input,
            "result": {"baseline_id": baseline_id, "revision_id": request.revision_id},
        }),
        &now,
    )
    .await?;
    tx.commit().await?;
    Ok(Json(
        load_response(&state, &project_id, &baseline_id).await?,
    ))
}

async fn authorized_project(
    state: &AppState,
    user_id: &str,
    project_id: &str,
) -> ApiResult<db::Project> {
    let project = ProjectRepo::get_by_id(&*state.db, project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    let member_exists = if project.owner_id.as_deref() == Some(user_id) {
        true
    } else {
        ProjectMemberRepo::get_member(&*state.db, project_id, user_id)
            .await?
            .is_some()
    };
    if !project_access_allowed(project.owner_id.as_deref(), user_id, member_exists) {
        return Err(ApiError::not_found("project", project_id.to_owned()));
    }
    Ok(project)
}

fn project_access_allowed(owner_id: Option<&str>, user_id: &str, member_exists: bool) -> bool {
    owner_id.is_none() || owner_id == Some(user_id) || member_exists
}

fn validate_user_authorization(
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
        || !valid_authorization_timestamp(&authorization.occurred_at)
    {
        return Err(ApiError::forbidden_with_code(
            "authorization.invalid",
            "this execution-baseline mutation requires an explicit authenticated user authorization event",
        ));
    }
    Ok(())
}

fn valid_authorization_timestamp(value: &str) -> bool {
    let Ok(timestamp) = DateTime::parse_from_rfc3339(value) else {
        return false;
    };
    let elapsed = Utc::now().signed_duration_since(timestamp.with_timezone(&Utc));
    elapsed.num_seconds().abs() <= MAX_AUTHORIZATION_CLOCK_SKEW_SECONDS
}

fn validate_idempotency_key(key: &str) -> ApiResult<()> {
    if key.trim().is_empty() {
        return Err(ApiError::bad_request("idempotency_key is required"));
    }
    Ok(())
}

fn event_key(kind: &str, idempotency_key: &str) -> String {
    format!("execution-baseline:{kind}:{idempotency_key}")
}

async fn replay_event(
    state: &AppState,
    dedupe_key: &str,
    input: &Value,
) -> ApiResult<Option<Value>> {
    let Some(row) = sqlx::query("SELECT payload_json FROM domain_event WHERE dedupe_key = ?")
        .bind(dedupe_key)
        .fetch_optional(state.db.pool())
        .await?
    else {
        return Ok(None);
    };
    let payload: Value =
        serde_json::from_str(&row.try_get::<String, _>("payload_json")?).map_err(|error| {
            ApiError::internal(format!("persisted idempotency event is invalid: {error}"))
        })?;
    if payload.get("input") != Some(input) {
        return Err(ApiError::conflict_with_code(
            "idempotency_conflict",
            "idempotency key was already used for different input",
        ));
    }
    Ok(Some(payload.get("result").cloned().ok_or_else(|| {
        ApiError::internal("persisted idempotency event is missing its result")
    })?))
}

#[allow(clippy::too_many_arguments)]
async fn append_event(
    tx: &mut Transaction<'_, Sqlite>,
    event_type: &str,
    entity_type: &str,
    entity_id: &str,
    actor_type: &str,
    actor_id: Option<&str>,
    scope_id: &str,
    correlation_id: &str,
    dedupe_key: &str,
    payload: Value,
    created_at: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO domain_event (
             id, event_type, entity_type, entity_id, actor_type, actor_id,
             scope_type, scope_id, correlation_id, causation_id, causation_depth,
             dedupe_key, payload_json, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, 'project', ?, ?, NULL, 0, ?, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(event_type)
    .bind(entity_type)
    .bind(entity_id)
    .bind(actor_type)
    .bind(actor_id)
    .bind(scope_id)
    .bind(correlation_id)
    .bind(dedupe_key)
    .bind(
        serde_json::to_string(&payload)
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?,
    )
    .bind(created_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Validate the ordered milestone/definition pin before any normalized
/// projection or lifecycle transition is written.  A pair is positional:
/// `milestone_ids[i]` is governed by
/// `milestone_definition_revision_ids[i]`.  Set-membership checks are not
/// sufficient because they allow a valid definition to be attached to the
/// wrong milestone.
fn validate_milestone_definition_pairs(content: &ExecutionBaselineContent) -> Result<(), String> {
    if content.milestone_ids.is_empty() {
        return Err("execution baseline must include at least one milestone".to_owned());
    }
    if content
        .milestone_ids
        .iter()
        .any(|milestone_id| milestone_id.trim().is_empty())
    {
        return Err("milestone_ids must contain non-empty identifiers".to_owned());
    }
    if content.milestone_definition_revision_ids.len() != content.milestone_ids.len() {
        return Err(
            "milestone_ids and milestone_definition_revision_ids must have the same length"
                .to_owned(),
        );
    }
    if content
        .milestone_definition_revision_ids
        .iter()
        .any(|definition_id| definition_id.trim().is_empty())
    {
        return Err(
            "milestone_definition_revision_ids must contain non-empty identifiers".to_owned(),
        );
    }
    if content.milestone_ids.iter().enumerate().any(|(index, id)| {
        content
            .milestone_ids
            .iter()
            .take(index)
            .any(|prior| prior == id)
    }) {
        return Err("milestone_ids must not contain duplicates".to_owned());
    }
    if content
        .milestone_definition_revision_ids
        .iter()
        .enumerate()
        .any(|(index, id)| {
            content
                .milestone_definition_revision_ids
                .iter()
                .take(index)
                .any(|prior| prior == id)
        })
    {
        return Err("milestone_definition_revision_ids must not contain duplicates".to_owned());
    }
    if let Some(primary) = content.primary_milestone_id.as_deref() {
        if !content.milestone_ids.iter().any(|id| id == primary) {
            return Err("primary_milestone_id must be included in milestone_ids".to_owned());
        }
    }
    Ok(())
}

async fn validate_baseline_content(
    state: &AppState,
    project: &db::Project,
    content: &ExecutionBaselineContent,
) -> ApiResult<()> {
    let current_charter = project
        .current_charter_revision_id
        .as_deref()
        .ok_or_else(|| {
            ApiError::conflict_with_code("charter_required", "Project has no approved Charter")
        })?;
    if project.charter_status != "charter_backed"
        || content.charter_revision.revision_id != current_charter
    {
        return Err(ApiError::conflict_with_code(
            "charter_revision_conflict",
            "the execution baseline must reference the current approved Project Charter revision",
        ));
    }
    validate_artifact_ref(state, project, &content.charter_revision, "charter").await?;
    if content.plan_item_ids.is_empty()
        || content.milestone_ids.is_empty()
        || content.release_policy_revision.trim().is_empty()
        || content.release_policy_digest.trim().is_empty()
        || content.capability_classes.is_empty()
        || content.risk_classes.is_empty()
    {
        return Err(ApiError::bad_request(
            "execution baseline requires plan items, milestones, release policy, capability classes, and risk classes",
        ));
    }
    validate_execution_baseline_policy(content)
        .map_err(|error| ApiError::conflict_with_code("release_policy_conflict", error))?;
    validate_milestone_definition_pairs(content).map_err(ApiError::bad_request)?;
    for document in &content.document_revisions {
        validate_artifact_ref(state, project, document, "document").await?;
    }
    for (milestone_id, definition_id) in content
        .milestone_ids
        .iter()
        .zip(&content.milestone_definition_revision_ids)
    {
        let definition = sqlx::query(
            "SELECT m.current_definition_revision_id, r.lifecycle, r.charter_revision_id
             FROM project_milestone m
             JOIN project_milestone_revision r ON r.milestone_id = m.id
             WHERE m.id = ? AND m.project_id = ? AND r.id = ? LIMIT 1",
        )
        .bind(milestone_id)
        .bind(&project.id)
        .bind(definition_id)
        .fetch_optional(state.db.pool())
        .await?;
        let Some(definition) = definition else {
            return Err(ApiError::conflict_with_code(
                "milestone_conflict",
                "every baseline milestone must reference an owned definition revision",
            ));
        };
        let current_definition_id: Option<String> =
            definition.try_get("current_definition_revision_id")?;
        let lifecycle: String = definition.try_get("lifecycle")?;
        let charter_revision_id: Option<String> = definition.try_get("charter_revision_id")?;
        if current_definition_id.as_deref() != Some(definition_id.as_str())
            || !matches!(lifecycle.as_str(), "proposed" | "approved")
            || charter_revision_id.as_deref() != Some(content.charter_revision.revision_id.as_str())
        {
            return Err(ApiError::conflict_with_code(
                "milestone_definition_conflict",
                "every baseline milestone must reference its current definition tied to the Charter",
            ));
        }
    }
    Ok(())
}

async fn validate_artifact_ref(
    state: &AppState,
    project: &db::Project,
    reference: &ArtifactRef,
    kind: &str,
) -> ApiResult<()> {
    let (sql, error_code, label) = match kind {
        "charter" => (
            "SELECT c.id AS artifact_id, r.content_digest, r.render_version,
                    r.rendered_digest, r.lifecycle
             FROM project_charter_revision r
             JOIN project_charter c ON c.id = r.charter_id
             WHERE r.id = ? AND c.project_id = ?",
            "charter_revision_conflict",
            "Charter",
        ),
        "document" => (
            "SELECT d.id AS artifact_id, r.content_digest, r.render_version,
                    r.rendered_digest, r.lifecycle
             FROM project_document_revision r
             JOIN project_document d ON d.id = r.document_id
             WHERE r.id = ? AND d.project_id = ?",
            "document_revision_conflict",
            "Document",
        ),
        _ => return Err(ApiError::internal("unknown baseline artifact kind")),
    };
    let row = sqlx::query(sql)
        .bind(&reference.revision_id)
        .bind(&project.id)
        .fetch_optional(state.db.pool())
        .await?
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                error_code,
                format!("{label} revision is not owned by this Project"),
            )
        })?;
    let artifact_id: String = row.try_get("artifact_id")?;
    let content_digest: String = row.try_get("content_digest")?;
    let lifecycle: String = row.try_get("lifecycle")?;
    let render_version: String = row.try_get("render_version")?;
    let render_digest: String = row.try_get("rendered_digest")?;
    let exact = artifact_id == reference.artifact_id
        && content_digest == reference.content_digest
        && lifecycle == "approved"
        && reference.render_version.as_deref() == Some(render_version.as_str())
        && reference.render_digest.as_deref() == Some(render_digest.as_str());
    if !exact {
        return Err(ApiError::conflict_with_code(
            error_code,
            format!("the {label} ArtifactRef does not match its approved persisted revision"),
        ));
    }
    Ok(())
}

/// Revalidate the complete persisted baseline manifest while the approval or
/// activation transaction still owns its SQLite snapshot.  Pool-level checks
/// performed while saving a proposal are useful for fast feedback, but they
/// cannot prove that Charter, Document, and milestone inputs stayed current
/// until the authority receipt is written.
async fn validate_persisted_manifest_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    project_id: &str,
    baseline_id: &str,
    revision_id: &str,
) -> ApiResult<BaselineManifest> {
    let row = sqlx::query(
        "SELECT r.schema_version, r.render_version, r.rendered_view,
                r.content_digest, r.rendered_digest, r.source_refs_json,
                r.lifecycle
         FROM project_execution_baseline_revision r
         WHERE r.id = ? AND r.baseline_id = ?",
    )
    .bind(revision_id)
    .bind(baseline_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| ApiError::not_found("execution_baseline_revision", revision_id.to_owned()))?;
    let manifest: BaselineManifest =
        serde_json::from_str(&row.try_get::<String, _>("source_refs_json")?).map_err(|error| {
            ApiError::internal(format!(
                "persisted execution baseline manifest is invalid: {error}"
            ))
        })?;
    if manifest.schema != MANIFEST_SCHEMA {
        return Err(ApiError::internal(
            "persisted execution baseline manifest has an unknown schema",
        ));
    }
    if matches!(
        row.try_get::<String, _>("lifecycle")?.as_str(),
        "superseded" | "revoked"
    ) {
        return Err(ApiError::conflict_with_code(
            "baseline_revision_conflict",
            "the execution baseline revision is no longer approvable",
        ));
    }
    let rendered = render_execution_baseline(&manifest.content).map_err(|error| {
        ApiError::internal(format!("persisted execution baseline is invalid: {error}"))
    })?;
    if row.try_get::<String, _>("schema_version")? != EXECUTION_BASELINE_SCHEMA_VERSION
        || row.try_get::<String, _>("render_version")? != EXECUTION_BASELINE_RENDER_VERSION
        || row.try_get::<String, _>("rendered_view")? != manifest.rendered_view
        || manifest.rendered_view != rendered.rendered_view
        || row.try_get::<String, _>("content_digest")? != rendered.content_digest
        || row.try_get::<String, _>("rendered_digest")? != rendered.render_digest
    {
        return Err(ApiError::internal(
            "persisted execution baseline does not reproduce its approved review digests",
        ));
    }
    let project = sqlx::query(
        "SELECT charter_status, charter_setup_required, current_charter_revision_id
         FROM project WHERE id = ?",
    )
    .bind(project_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    let charter_status: String = project.try_get("charter_status")?;
    let setup_required: i64 = project.try_get("charter_setup_required")?;
    let current_charter: Option<String> = project.try_get("current_charter_revision_id")?;
    if charter_status != "charter_backed"
        || setup_required != 0
        || current_charter.as_deref()
            != Some(manifest.content.charter_revision.revision_id.as_str())
    {
        return Err(ApiError::conflict_with_code(
            "charter_revision_conflict",
            "the current approved Project Charter no longer matches the baseline manifest",
        ));
    }
    validate_artifact_ref_in_tx(
        tx,
        project_id,
        &manifest.content.charter_revision,
        "charter",
    )
    .await?;
    for document in &manifest.content.document_revisions {
        validate_artifact_ref_in_tx(tx, project_id, document, "document").await?;
    }
    validate_milestone_definition_pairs(&manifest.content).map_err(|message| {
        ApiError::conflict_with_code("milestone_definition_conflict", message)
    })?;
    for (milestone_id, definition_id) in manifest
        .content
        .milestone_ids
        .iter()
        .zip(&manifest.content.milestone_definition_revision_ids)
    {
        let definition = sqlx::query(
            "SELECT m.current_definition_revision_id, r.lifecycle, r.charter_revision_id
             FROM project_milestone m
             JOIN project_milestone_revision r ON r.milestone_id = m.id
             WHERE m.id = ? AND m.project_id = ? AND r.id = ? LIMIT 1",
        )
        .bind(milestone_id)
        .bind(project_id)
        .bind(definition_id)
        .fetch_optional(&mut **tx)
        .await?;
        let Some(definition) = definition else {
            return Err(ApiError::conflict_with_code(
                "milestone_definition_conflict",
                "the baseline manifest references a missing or cross-Project milestone definition",
            ));
        };
        let current_definition_id: Option<String> =
            definition.try_get("current_definition_revision_id")?;
        let definition_lifecycle: String = definition.try_get("lifecycle")?;
        let definition_charter_revision_id: Option<String> =
            definition.try_get("charter_revision_id")?;
        if current_definition_id.as_deref() != Some(definition_id.as_str())
            || !matches!(definition_lifecycle.as_str(), "proposed" | "approved")
            || definition_charter_revision_id.as_deref()
                != Some(manifest.content.charter_revision.revision_id.as_str())
        {
            return Err(ApiError::conflict_with_code(
                "milestone_definition_conflict",
                "the baseline manifest no longer references the current Charter-bound milestone definition",
            ));
        }
    }
    validate_execution_baseline_policy(&manifest.content)
        .map_err(|error| ApiError::conflict_with_code("release_policy_conflict", error))?;
    Ok(manifest)
}

async fn validate_artifact_ref_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    project_id: &str,
    reference: &ArtifactRef,
    kind: &str,
) -> ApiResult<()> {
    let (sql, error_code, label) = match kind {
        "charter" => (
            "SELECT c.id AS artifact_id, r.content_digest, r.render_version,
                    r.rendered_digest, r.lifecycle
             FROM project_charter_revision r
             JOIN project_charter c ON c.id = r.charter_id
             WHERE r.id = ? AND c.project_id = ?",
            "charter_revision_conflict",
            "Charter",
        ),
        "document" => (
            "SELECT d.id AS artifact_id, r.content_digest, r.render_version,
                    r.rendered_digest, r.lifecycle
             FROM project_document_revision r
             JOIN project_document d ON d.id = r.document_id
             WHERE r.id = ? AND d.project_id = ?",
            "document_revision_conflict",
            "Document",
        ),
        _ => return Err(ApiError::internal("unknown baseline artifact kind")),
    };
    let row = sqlx::query(sql)
        .bind(&reference.revision_id)
        .bind(project_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                error_code,
                format!("{label} revision is not owned by this Project"),
            )
        })?;
    let artifact_id: String = row.try_get("artifact_id")?;
    let content_digest: String = row.try_get("content_digest")?;
    let lifecycle: String = row.try_get("lifecycle")?;
    let render_version: String = row.try_get("render_version")?;
    let render_digest: String = row.try_get("rendered_digest")?;
    let exact = artifact_id == reference.artifact_id
        && content_digest == reference.content_digest
        && lifecycle == "approved"
        && reference.render_version.as_deref() == Some(render_version.as_str())
        && reference.render_digest.as_deref() == Some(render_digest.as_str());
    if !exact {
        return Err(ApiError::conflict_with_code(
            error_code,
            format!("the {label} ArtifactRef no longer matches its approved persisted revision"),
        ));
    }
    Ok(())
}

/// Activate the exact milestone definitions covered by the approved baseline
/// in the same transaction.  A baseline cannot make a milestone runnable by
/// merely naming it: its current definition must be present and approved.
async fn activate_manifest_milestones_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    project_id: &str,
    content: &ExecutionBaselineContent,
    now: &str,
) -> ApiResult<()> {
    validate_milestone_definition_pairs(content).map_err(|message| {
        ApiError::conflict_with_code("milestone_definition_conflict", message)
    })?;
    for (milestone_id, expected_definition_id) in content
        .milestone_ids
        .iter()
        .zip(&content.milestone_definition_revision_ids)
    {
        let row = sqlx::query(
            "SELECT lifecycle, current_definition_revision_id
             FROM project_milestone WHERE id = ? AND project_id = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                "milestone_conflict",
                "every activated baseline milestone must belong to the same Project",
            )
        })?;
        let definition_id: Option<String> = row.try_get("current_definition_revision_id")?;
        let Some(definition_id) = definition_id else {
            return Err(ApiError::conflict_with_code(
                "milestone_definition_conflict",
                "every activated baseline milestone must have a current definition revision",
            ));
        };
        if definition_id != *expected_definition_id {
            return Err(ApiError::conflict_with_code(
                "milestone_definition_conflict",
                "the milestone current definition changed after the baseline was proposed",
            ));
        }
        let definition_lifecycle: String = sqlx::query_scalar(
            "SELECT lifecycle FROM project_milestone_revision
             WHERE id = ? AND milestone_id = ?",
        )
        .bind(&definition_id)
        .bind(milestone_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                "milestone_definition_conflict",
                "the baseline milestone definition revision is missing",
            )
        })?;
        if definition_lifecycle == "proposed" {
            sqlx::query(
                "UPDATE project_milestone_revision SET lifecycle = 'approved'
                 WHERE id = ? AND milestone_id = ? AND lifecycle = 'proposed'",
            )
            .bind(&definition_id)
            .bind(milestone_id)
            .execute(&mut **tx)
            .await?;
        } else if definition_lifecycle != "approved" {
            return Err(ApiError::conflict_with_code(
                "milestone_definition_conflict",
                "the baseline milestone definition revision is not approvable",
            ));
        }
        sqlx::query(
            "UPDATE project_milestone
             SET lifecycle = CASE WHEN lifecycle = 'planned' THEN 'active' ELSE lifecycle END,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND lifecycle IN ('planned', 'active')",
        )
        .bind(now)
        .bind(milestone_id)
        .bind(project_id)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

async fn load_response(
    state: &AppState,
    project_id: &str,
    baseline_id: &str,
) -> ApiResult<ExecutionBaselineResponse> {
    let baseline =
        ProjectOrchestrationRepo::get_project_execution_baseline(&*state.db, baseline_id)
            .await?
            .filter(|baseline| baseline.project_id == project_id)
            .ok_or_else(|| ApiError::not_found("execution_baseline", baseline_id.to_owned()))?;
    let revision_id = match baseline.current_revision_id.as_deref() {
        Some(id) => Some(id.to_owned()),
        None => {
            sqlx::query_scalar::<_, String>(
                "SELECT id FROM project_execution_baseline_revision
             WHERE baseline_id = ? ORDER BY revision DESC, id DESC LIMIT 1",
            )
            .bind(baseline_id)
            .fetch_optional(state.db.pool())
            .await?
        }
    };
    let current_revision = match revision_id {
        Some(id) => Some(api_revision(state, project_id, baseline_id, &id).await?),
        None => None,
    };
    let latest_revision_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM project_execution_baseline_revision
         WHERE baseline_id = ? ORDER BY revision DESC, id DESC LIMIT 1",
    )
    .bind(baseline_id)
    .fetch_optional(state.db.pool())
    .await?;
    let proposed_revision = match latest_revision_id {
        Some(id)
            if current_revision
                .as_ref()
                .is_none_or(|revision| revision.id != id) =>
        {
            Some(api_revision(state, project_id, baseline_id, &id).await?)
        }
        _ => None,
    };
    let mut approval = None;
    for revision in [proposed_revision.as_ref(), current_revision.as_ref()]
        .into_iter()
        .flatten()
    {
        let approval_id = sqlx::query_scalar::<_, String>(
            "SELECT id FROM project_execution_baseline_approval
             WHERE baseline_id = ? AND revision_id = ?
               AND lifecycle IN ('active', 'consumed')
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(baseline_id)
        .bind(&revision.id)
        .fetch_optional(state.db.pool())
        .await?;
        if let Some(id) = approval_id {
            approval = Some(api_approval(state, &id, project_id).await?);
            break;
        }
    }
    Ok(ExecutionBaselineResponse {
        baseline: api_baseline(baseline)?,
        current_revision,
        proposed_revision,
        approval,
    })
}

fn api_baseline(record: ProjectExecutionBaselineRecord) -> ApiResult<ExecutionBaseline> {
    Ok(ExecutionBaseline {
        id: record.id,
        project_id: record.project_id,
        current_revision_id: record.current_revision_id,
        lifecycle: baseline_lifecycle(&record.lifecycle)?,
        version: record.version,
        created_at: record.created_at,
        updated_at: record.updated_at,
    })
}

async fn api_revision(
    state: &AppState,
    project_id: &str,
    baseline_id: &str,
    revision_id: &str,
) -> ApiResult<ExecutionBaselineRevision> {
    let row = sqlx::query(
        "SELECT r.id, r.baseline_id, r.revision, r.base_revision_id, r.lifecycle,
                r.schema_version, r.render_version, r.rendered_view, r.content_digest,
                r.rendered_digest, r.source_refs_json, r.created_at,
                b.lifecycle AS baseline_lifecycle,
                b.current_revision_id AS baseline_current_revision_id,
                b.updated_at AS baseline_updated_at
         FROM project_execution_baseline_revision r
         JOIN project_execution_baseline b ON b.id = r.baseline_id
         WHERE r.id = ? AND r.baseline_id = ?",
    )
    .bind(revision_id)
    .bind(baseline_id)
    .fetch_optional(state.db.pool())
    .await?
    .ok_or_else(|| ApiError::not_found("execution_baseline_revision", revision_id.to_owned()))?;
    let manifest: BaselineManifest =
        serde_json::from_str(&row.try_get::<String, _>("source_refs_json")?).map_err(|error| {
            ApiError::internal(format!(
                "persisted execution baseline manifest is invalid: {error}"
            ))
        })?;
    if manifest.schema != MANIFEST_SCHEMA {
        return Err(ApiError::internal(
            "persisted execution baseline manifest has an unknown schema",
        ));
    }
    let rendered = render_execution_baseline(&manifest.content).map_err(|error| {
        ApiError::internal(format!("persisted execution baseline is invalid: {error}"))
    })?;
    let persisted_render_version: String = row.try_get("render_version")?;
    let persisted_view: String = row.try_get("rendered_view")?;
    let persisted_content_digest: String = row.try_get("content_digest")?;
    let persisted_render_digest: String = row.try_get("rendered_digest")?;
    if row.try_get::<String, _>("schema_version")? != EXECUTION_BASELINE_SCHEMA_VERSION
        || persisted_render_version != EXECUTION_BASELINE_RENDER_VERSION
        || persisted_view != manifest.rendered_view
        || manifest.rendered_view != rendered.rendered_view
        || persisted_content_digest != rendered.content_digest
        || persisted_render_digest != rendered.render_digest
    {
        return Err(ApiError::internal(
            "persisted execution baseline does not reproduce its approved review digests",
        ));
    }
    let activated_at = if row.try_get::<String, _>("baseline_lifecycle")? == "active"
        && row
            .try_get::<Option<String>, _>("baseline_current_revision_id")?
            .as_deref()
            == Some(revision_id)
    {
        Some(row.try_get::<String, _>("baseline_updated_at")?)
    } else {
        None
    };
    Ok(ExecutionBaselineRevision {
        id: row.try_get("id")?,
        baseline_id: row.try_get("baseline_id")?,
        project_id: project_id.to_owned(),
        revision_number: row.try_get("revision")?,
        base_revision_id: row.try_get("base_revision_id")?,
        lifecycle: baseline_lifecycle(&row.try_get::<String, _>("lifecycle")?)?,
        schema_version: EXECUTION_BASELINE_SCHEMA_VERSION.to_owned(),
        content: manifest.content,
        rendered_view: manifest.rendered_view,
        render_version: persisted_render_version,
        content_digest: persisted_content_digest,
        render_digest: persisted_render_digest,
        provenance: manifest.provenance,
        created_at: row.try_get("created_at")?,
        activated_at,
    })
}

async fn api_approval(
    state: &AppState,
    approval_id: &str,
    project_id: &str,
) -> ApiResult<ExecutionBaselineApproval> {
    let row = sqlx::query(
        "SELECT a.id, a.baseline_id, a.revision_id, a.expected_project_version,
                a.principal_type, a.principal_id, a.authorization_basis,
                a.authorization_action, a.authorization_occurred_at,
                a.explicit_event, a.content_digest, a.rendered_digest,
                a.created_at, a.idempotency_key, b.project_id
         FROM project_execution_baseline_approval a
         JOIN project_execution_baseline b ON b.id = a.baseline_id
         WHERE a.id = ?",
    )
    .bind(approval_id)
    .fetch_optional(state.db.pool())
    .await?
    .ok_or_else(|| ApiError::not_found("execution_baseline_approval", approval_id.to_owned()))?;
    if row.try_get::<String, _>("project_id")? != project_id {
        return Err(ApiError::not_found(
            "execution_baseline_approval",
            approval_id.to_owned(),
        ));
    }
    let principal_type: String = row.try_get("principal_type")?;
    let principal_id: String = row.try_get("principal_id")?;
    let principal = PrincipalRef {
        kind: parse_principal_kind(&principal_type),
        id: principal_id.clone(),
        display_name: None,
    };
    Ok(ExecutionBaselineApproval {
        id: row.try_get("id")?,
        baseline_id: row.try_get("baseline_id")?,
        revision_id: row.try_get("revision_id")?,
        content_digest: row.try_get("content_digest")?,
        render_digest: row.try_get("rendered_digest")?,
        expected_project_version: row.try_get("expected_project_version")?,
        approved_by: principal.clone(),
        authorization: AuthorizationProvenance {
            principal,
            authorization_basis: row.try_get("authorization_basis")?,
            action: row.try_get("authorization_action")?,
            event_id: row.try_get("explicit_event")?,
            occurred_at: row.try_get("authorization_occurred_at")?,
        },
        approved_at: row.try_get("created_at")?,
        idempotency_key: client_idempotency_key(&row.try_get::<String, _>("idempotency_key")?),
    })
}

fn baseline_lifecycle(value: &str) -> ApiResult<ExecutionBaselineLifecycle> {
    match value {
        "draft" => Ok(ExecutionBaselineLifecycle::Draft),
        "proposed" => Ok(ExecutionBaselineLifecycle::Proposed),
        "approved" => Ok(ExecutionBaselineLifecycle::Approved),
        "active" => Ok(ExecutionBaselineLifecycle::Active),
        "superseded" => Ok(ExecutionBaselineLifecycle::Superseded),
        "revoked" => Ok(ExecutionBaselineLifecycle::Revoked),
        _ => Err(ApiError::internal(format!(
            "unknown execution baseline lifecycle: {value}"
        ))),
    }
}

fn parse_principal_kind(value: &str) -> PrincipalKind {
    match value {
        "user" => PrincipalKind::User,
        "agent" => PrincipalKind::Agent,
        "worker" => PrincipalKind::Worker,
        "reviewer" => PrincipalKind::Reviewer,
        "service" => PrincipalKind::Service,
        _ => PrincipalKind::System,
    }
}

#[cfg(test)]
mod tests {
    use super::{project_access_allowed, valid_authorization_timestamp};
    use chrono::{Duration, Utc};

    #[test]
    fn project_owner_access_does_not_require_membership_row() {
        assert!(project_access_allowed(Some("owner"), "owner", false));
        assert!(!project_access_allowed(Some("owner"), "other", false));
        assert!(project_access_allowed(Some("owner"), "member", true));
    }

    #[test]
    fn authorization_timestamp_is_rfc3339_and_bounded() {
        assert!(valid_authorization_timestamp(&Utc::now().to_rfc3339()));
        assert!(!valid_authorization_timestamp("not-a-timestamp"));
        assert!(!valid_authorization_timestamp(
            &(Utc::now() - Duration::days(3)).to_rfc3339()
        ));
    }
}
