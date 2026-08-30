//! Project-local milestone, readiness, and immutable release routes.
//!
//! Every handler authorizes the Project before touching milestone, evidence,
//! readiness, or release rows.  The service owns the transaction and pure
//! digest/release rules; these handlers only bind the authenticated user to
//! the frozen request envelope.

use api_types::{
    canonical_digest_with_schema, AuthorizationProvenance, CreateMilestoneRequest,
    EvaluateMilestoneReadinessRequest, MilestoneDefinitionLifecycle, MilestoneDefinitionRevision,
    MilestoneDefinitionRevisionListResponse, MilestoneLifecycle, PrincipalKind, PrincipalRef,
    ProjectMilestone, ProjectMilestoneListResponse, ProjectRelease, ProjectReleaseListResponse,
    ReadinessSnapshot, ReadinessSnapshotListResponse, RecordMilestoneCheckRequest,
    ReleaseMilestoneRequest, SaveMilestoneRevisionRequest, SetPrimaryMilestoneRequest,
    TransitionMilestoneRequest, TransitionMilestoneRevisionRequest, ValidationResult,
    WaiveMilestoneCheckRequest,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use db::{
    new_uuid_v4, now_rfc3339, CreateDomainEvent, DomainEventRepo, ProjectMemberRepo, ProjectRepo,
};
use serde::Deserialize;
use serde_json::json;
use services::MilestoneRuntime;
use sqlx::{Row, Sqlite, Transaction};
use uuid::Uuid;

use crate::{
    errors::{ApiError, ApiResult},
    routes::{auth::AuthenticatedUser, scoped_idempotency_key},
    state::AppState,
};

const PRIMARY_ACTION: &str = "project.milestone.primary.set";
const CREATE_ACTION: &str = "project.milestone.create";
const REVISION_ACTION: &str = "project.milestone.revision.save";
const READINESS_ACTION: &str = "project.milestone.readiness";
const REVISION_TRANSITION_ACTION: &str = "project.milestone.revision.transition";
const MILESTONE_TRANSITION_ACTION: &str = "project.milestone.transition";
const CHECK_RESULT_ACTION: &str = "project.milestone.check.record";
const CHECK_WAIVE_ACTION: &str = "project.milestone.check.waive";
const MAX_AUTHORIZATION_CLOCK_SKEW_SECONDS: i64 = 48 * 60 * 60;
const MAX_AUTHORIZATION_TIMESTAMP_LEN: usize = 64;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MilestoneListQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

pub async fn create_milestone(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<CreateMilestoneRequest>,
) -> ApiResult<(StatusCode, Json<ProjectMilestone>)> {
    ensure_project_access(&state, &project_id, &user).await?;
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        CREATE_ACTION,
    )?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    let revision = services::ProjectMilestoneCommandService::new(state.db.clone())
        .define_milestone(
            services::ProjectMilestoneDefinitionCommand {
                project_id: project_id.clone(),
                milestone_id: None,
                display_label: request.display_label,
                lifecycle: request.lifecycle,
                content: request.content,
                rendered_view: request.rendered_view,
                render_version: request.render_version,
                change_summary: request.change_summary,
                provenance: request.provenance,
                base_revision_id: None,
                expected_project_version: request.mutation.expected_version,
                expected_milestone_version: 1,
                idempotency_key: request.mutation.idempotency_key,
                authorization: milestone_authorization(
                    &request.mutation.authorization,
                    &user.user_id,
                    CREATE_ACTION,
                )?,
            },
            None,
        )
        .await?;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let milestone = runtime
        .get(&project_id, &revision.milestone_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("milestone", revision.milestone_id.clone()))?;
    Ok((StatusCode::CREATED, Json(milestone)))
}

pub async fn list_milestone_revisions(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
) -> ApiResult<Json<MilestoneDefinitionRevisionListResponse>> {
    list_milestone_revisions_with_query(
        State(state),
        user,
        Path((project_id, milestone_id)),
        Query(MilestoneListQuery::default()),
    )
    .await
}

pub async fn list_milestone_revisions_with_query(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
    Query(query): Query<MilestoneListQuery>,
) -> ApiResult<Json<MilestoneDefinitionRevisionListResponse>> {
    ensure_project_access(&state, &project_id, &user).await?;
    let limit = bounded_limit(query.limit);
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let rows = if let Some((revision, id)) = cursor.as_ref() {
        let revision = revision
            .parse::<i64>()
            .map_err(|_| ApiError::bad_request("invalid cursor"))?;
        sqlx::query(
            "SELECT r.id
             FROM project_milestone_revision r
             JOIN project_milestone m ON m.id = r.milestone_id
             WHERE r.milestone_id = ? AND m.project_id = ?
               AND (r.revision > ? OR (r.revision = ? AND r.id > ?))
             ORDER BY r.revision ASC, r.id ASC LIMIT ?",
        )
        .bind(&milestone_id)
        .bind(&project_id)
        .bind(revision)
        .bind(revision)
        .bind(id)
        .bind(limit + 1)
        .fetch_all(state.db.pool())
        .await?
    } else {
        sqlx::query(
            "SELECT r.id
             FROM project_milestone_revision r
             JOIN project_milestone m ON m.id = r.milestone_id
             WHERE r.milestone_id = ? AND m.project_id = ?
             ORDER BY r.revision ASC, r.id ASC LIMIT ?",
        )
        .bind(&milestone_id)
        .bind(&project_id)
        .bind(limit + 1)
        .fetch_all(state.db.pool())
        .await?
    };
    let has_more = rows.len() > limit as usize;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let mut items = Vec::with_capacity(rows.len().min(limit as usize));
    for row in rows.into_iter().take(limit as usize) {
        let revision_id: String = row.try_get("id")?;
        let revision = runtime
            .definition_revision(&project_id, &milestone_id, &revision_id)
            .await
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError::not_found("milestone_definition_revision", revision_id))?;
        items.push(revision);
    }
    let next_cursor = items
        .last()
        .map(|item| encode_cursor(&item.revision_number.to_string(), &item.id));
    Ok(Json(MilestoneDefinitionRevisionListResponse {
        items,
        next_cursor: next_cursor.filter(|_| has_more),
        has_more,
    }))
}

pub async fn get_milestone_revision(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id, revision_id)): Path<(String, String, String)>,
) -> ApiResult<Json<MilestoneDefinitionRevision>> {
    ensure_project_access(&state, &project_id, &user).await?;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let revision = runtime
        .definition_revision(&project_id, &milestone_id, &revision_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("milestone_definition_revision", revision_id))?;
    Ok(Json(revision))
}

pub async fn transition_milestone_revision(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id, revision_id)): Path<(String, String, String)>,
    Json(request): Json<TransitionMilestoneRevisionRequest>,
) -> ApiResult<Json<MilestoneDefinitionRevision>> {
    ensure_project_access(&state, &project_id, &user).await?;
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        REVISION_TRANSITION_ACTION,
    )?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    let transition_dedupe = format!(
        "milestone.definition.transitioned:{project_id}:{}",
        request.mutation.idempotency_key
    );
    if let Some(event) =
        DomainEventRepo::get_event_by_dedupe(&*state.db, &transition_dedupe).await?
    {
        let payload: serde_json::Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted milestone transition event is invalid"))?;
        if payload
            .get("revision_id")
            .and_then(serde_json::Value::as_str)
            != Some(revision_id.as_str())
            || payload.get("lifecycle").and_then(serde_json::Value::as_str)
                != Some(milestone_definition_lifecycle_name(request.lifecycle))
            || payload
                .get("expected_milestone_version")
                .and_then(serde_json::Value::as_i64)
                != Some(request.mutation.expected_version)
            || payload
                .get("principal_id")
                .and_then(serde_json::Value::as_str)
                != Some(user.user_id.as_str())
        {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different milestone transition",
            ));
        }
        let runtime = MilestoneRuntime::new(state.db.clone());
        let revision = runtime
            .definition_revision(&project_id, &milestone_id, &revision_id)
            .await
            .map_err(ApiError::from)?
            .ok_or_else(|| {
                ApiError::not_found("milestone_definition_revision", revision_id.clone())
            })?;
        return Ok(Json(revision));
    }
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let locked = sqlx::query(
        "UPDATE project_milestone SET version = version
         WHERE id = ? AND project_id = ? AND version = ?",
    )
    .bind(&milestone_id)
    .bind(&project_id)
    .bind(request.mutation.expected_version)
    .execute(&mut *tx)
    .await?;
    if locked.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the milestone changed before its definition transition",
        ));
    }
    let target = sqlx::query(
        "SELECT r.* FROM project_milestone_revision r
         JOIN project_milestone m ON m.id = r.milestone_id
         WHERE r.id = ? AND r.milestone_id = ? AND m.project_id = ?",
    )
    .bind(&revision_id)
    .bind(&milestone_id)
    .bind(&project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::not_found("milestone_definition_revision", revision_id.clone()))?;
    let from_name: String = target.try_get("lifecycle")?;
    let from = parse_definition_lifecycle(&from_name)?;
    services::validate_definition_transition(from, request.lifecycle)
        .map_err(|error| ApiError::conflict_with_code("invalid_transition", error.to_string()))?;
    let acceptance_checks_json: String = target.try_get("acceptance_checks_json")?;
    let acceptance_checks: Vec<api_types::MilestoneAcceptanceCheck> =
        serde_json::from_str(&acceptance_checks_json)
            .map_err(|_| ApiError::internal("persisted milestone acceptance checks are invalid"))?;
    let evidence_requirements_json: String = target.try_get("evidence_requirements_json")?;
    let evidence_requirements: Vec<api_types::AcceptanceEvidenceRequirement> =
        serde_json::from_str(&evidence_requirements_json).map_err(|_| {
            ApiError::internal("persisted milestone evidence requirements are invalid")
        })?;
    let transitioned = sqlx::query(
        "UPDATE project_milestone_revision SET lifecycle = ?
         WHERE id = ? AND milestone_id = ? AND lifecycle = ?",
    )
    .bind(milestone_definition_lifecycle_name(request.lifecycle))
    .bind(&revision_id)
    .bind(&milestone_id)
    .bind(&from_name)
    .execute(&mut *tx)
    .await?;
    if transitioned.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the milestone definition changed before its transition",
        ));
    }
    let updated_at = now_rfc3339();
    if matches!(
        request.lifecycle,
        MilestoneDefinitionLifecycle::Proposed | MilestoneDefinitionLifecycle::Approved
    ) {
        materialize_check_definitions_in_tx(
            &mut tx,
            &project_id,
            &milestone_id,
            &revision_id,
            &acceptance_checks,
            &evidence_requirements,
            &updated_at,
        )
        .await?;
    }
    if request.lifecycle == MilestoneDefinitionLifecycle::Approved {
        sqlx::query(
            "UPDATE project_milestone_revision
             SET lifecycle = 'superseded'
             WHERE milestone_id = ? AND id != ? AND lifecycle = 'approved'",
        )
        .bind(&milestone_id)
        .bind(&revision_id)
        .execute(&mut *tx)
        .await?;
    }
    let advanced = if matches!(
        request.lifecycle,
        MilestoneDefinitionLifecycle::Proposed | MilestoneDefinitionLifecycle::Approved
    ) {
        sqlx::query(
            "UPDATE project_milestone
             SET current_definition_revision_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(&revision_id)
        .bind(&updated_at)
        .bind(&milestone_id)
        .bind(&project_id)
        .bind(request.mutation.expected_version)
        .execute(&mut *tx)
        .await?
        .rows_affected()
    } else {
        sqlx::query(
            "UPDATE project_milestone SET version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(&updated_at)
        .bind(&milestone_id)
        .bind(&project_id)
        .bind(request.mutation.expected_version)
        .execute(&mut *tx)
        .await?
        .rows_affected()
    };
    if advanced != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the milestone changed before its definition transition",
        ));
    }
    let event_id = new_uuid_v4();
    let event_inserted = append_milestone_event_in_tx(
        &state,
        &mut tx,
        "milestone.definition.transitioned",
        &project_id,
        &milestone_id,
        &user.user_id,
        &event_id,
        &request.mutation.authorization,
        &request.mutation.idempotency_key,
        json!({
            "revision_id": revision_id,
            "lifecycle": milestone_definition_lifecycle_name(request.lifecycle),
            "expected_milestone_version": request.mutation.expected_version,
            "principal_id": user.user_id,
        }),
        &updated_at,
    )
    .await?;
    if !event_inserted {
        tx.rollback().await?;
        return Err(ApiError::conflict_with_code(
            "idempotency_in_progress",
            "another request committed this definition transition; retry with the same key",
        ));
    }
    tx.commit().await?;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let revision = runtime
        .definition_revision(&project_id, &milestone_id, &revision_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("milestone_definition_revision", revision_id))?;
    Ok(Json(revision))
}

pub async fn save_milestone_revision(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
    Json(request): Json<SaveMilestoneRevisionRequest>,
) -> ApiResult<(axum::http::StatusCode, Json<MilestoneDefinitionRevision>)> {
    ensure_project_access(&state, &project_id, &user).await?;
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        REVISION_ACTION,
    )?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    if request.base_revision_id.trim().is_empty() {
        return Err(ApiError::bad_request(
            "a milestone revision requires a UUID base",
        ));
    }
    let revision = services::ProjectMilestoneCommandService::new(state.db.clone())
        .revise_milestone(
            services::ProjectMilestoneDefinitionCommand {
                project_id: project_id.clone(),
                milestone_id: Some(milestone_id.clone()),
                display_label: None,
                lifecycle: request.lifecycle,
                content: request.content,
                rendered_view: request.rendered_view,
                render_version: request.render_version,
                change_summary: request.change_summary,
                provenance: request.provenance,
                base_revision_id: Some(request.base_revision_id),
                expected_project_version: 0,
                expected_milestone_version: request.mutation.expected_version,
                idempotency_key: request.mutation.idempotency_key,
                authorization: milestone_authorization(
                    &request.mutation.authorization,
                    &user.user_id,
                    REVISION_ACTION,
                )?,
            },
            None,
        )
        .await?;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let revision = runtime
        .definition_revision(&project_id, &milestone_id, &revision.id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("milestone_definition_revision", revision.id))?;
    Ok((StatusCode::CREATED, Json(revision)))
}

pub async fn list_milestones(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<Json<ProjectMilestoneListResponse>> {
    list_milestones_with_query(
        State(state),
        user,
        Path(project_id),
        Query(MilestoneListQuery::default()),
    )
    .await
}

pub async fn list_milestones_with_query(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Query(query): Query<MilestoneListQuery>,
) -> ApiResult<Json<ProjectMilestoneListResponse>> {
    ensure_project_access(&state, &project_id, &user).await?;
    let limit = bounded_limit(query.limit);
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let rows = if let Some((sequence, id)) = cursor.as_ref() {
        let sequence = sequence
            .parse::<i64>()
            .map_err(|_| ApiError::bad_request("invalid cursor"))?;
        sqlx::query(
            "SELECT id
             FROM project_milestone
             WHERE project_id = ?
               AND (milestone_sequence > ? OR (milestone_sequence = ? AND id > ?))
             ORDER BY milestone_sequence ASC, id ASC LIMIT ?",
        )
        .bind(&project_id)
        .bind(sequence)
        .bind(sequence)
        .bind(id)
        .bind(limit + 1)
        .fetch_all(state.db.pool())
        .await?
    } else {
        sqlx::query(
            "SELECT id FROM project_milestone
             WHERE project_id = ?
             ORDER BY milestone_sequence ASC, id ASC LIMIT ?",
        )
        .bind(&project_id)
        .bind(limit + 1)
        .fetch_all(state.db.pool())
        .await?
    };
    let has_more = rows.len() > limit as usize;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let mut items = Vec::with_capacity(rows.len().min(limit as usize));
    for row in rows.into_iter().take(limit as usize) {
        let milestone_id: String = row.try_get("id")?;
        // `get` already tolerates an unset definition pointer by falling
        // back to the latest revision; a `None` here means the milestone has
        // no revision at all, which `MilestoneRuntime` already warned about.
        // Skip it rather than failing the whole page for one corrupt row.
        if let Some(milestone) = runtime.get(&project_id, &milestone_id).await? {
            items.push(milestone);
        }
    }
    let next_cursor = items
        .last()
        .map(|item| encode_cursor(&item.milestone_sequence.to_string(), &item.id));
    Ok(Json(ProjectMilestoneListResponse {
        items,
        next_cursor: next_cursor.filter(|_| has_more),
        has_more,
    }))
}

pub async fn get_milestone(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
) -> ApiResult<Json<ProjectMilestone>> {
    ensure_project_access(&state, &project_id, &user).await?;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let milestone = runtime
        .get(&project_id, &milestone_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("milestone", milestone_id))?;
    Ok(Json(milestone))
}

/// Transition the mutable milestone instance.  This is intentionally not
/// folded into definition revision approval: a definition may be proposed or
/// approved while its milestone remains planned/active, and cancellation is a
/// terminal instance decision.
pub async fn transition_milestone(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
    Json(request): Json<TransitionMilestoneRequest>,
) -> ApiResult<Json<ProjectMilestone>> {
    ensure_project_access(&state, &project_id, &user).await?;
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        MILESTONE_TRANSITION_ACTION,
    )?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    let dedupe = format!(
        "milestone.lifecycle.transitioned:{project_id}:{}",
        request.mutation.idempotency_key
    );
    if let Some(event) = DomainEventRepo::get_event_by_dedupe(&*state.db, &dedupe).await? {
        let payload: serde_json::Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| ApiError::internal("persisted milestone lifecycle event is invalid"))?;
        if payload
            .get("milestone_id")
            .and_then(serde_json::Value::as_str)
            != Some(milestone_id.as_str())
            || payload.get("lifecycle").and_then(serde_json::Value::as_str)
                != Some(milestone_lifecycle_name(request.lifecycle))
            || payload
                .get("expected_milestone_version")
                .and_then(serde_json::Value::as_i64)
                != Some(request.mutation.expected_version)
            || payload
                .get("principal_id")
                .and_then(serde_json::Value::as_str)
                != Some(user.user_id.as_str())
        {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the idempotency key was already used for a different milestone transition",
            ));
        }
        let runtime = MilestoneRuntime::new(state.db.clone());
        let milestone = runtime
            .get(&project_id, &milestone_id)
            .await
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError::not_found("milestone", milestone_id.clone()))?;
        return Ok(Json(milestone));
    }
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let locked = sqlx::query(
        "UPDATE project_milestone SET version = version
         WHERE id = ? AND project_id = ? AND version = ?",
    )
    .bind(&milestone_id)
    .bind(&project_id)
    .bind(request.mutation.expected_version)
    .execute(&mut *tx)
    .await?;
    if locked.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the milestone changed before its lifecycle transition",
        ));
    }
    let row =
        sqlx::query("SELECT lifecycle FROM project_milestone WHERE id = ? AND project_id = ?")
            .bind(&milestone_id)
            .bind(&project_id)
            .fetch_one(&mut *tx)
            .await?;
    let current_name: String = row.try_get("lifecycle")?;
    let current = parse_milestone_lifecycle(&current_name)?;
    services::validate_milestone_transition(current, request.lifecycle)
        .map_err(|error| ApiError::conflict_with_code("invalid_transition", error.to_string()))?;
    if request.lifecycle == MilestoneLifecycle::Cancelled {
        let primary: Option<String> =
            sqlx::query_scalar("SELECT primary_milestone_id FROM project WHERE id = ?")
                .bind(&project_id)
                .fetch_one(&mut *tx)
                .await?;
        if primary.as_deref() == Some(milestone_id.as_str()) {
            return Err(ApiError::conflict_with_code(
                "primary_milestone_required",
                "choose another active primary milestone before cancelling this milestone",
            ));
        }
    }
    let updated_at = now_rfc3339();
    let updated = sqlx::query(
        "UPDATE project_milestone SET lifecycle = ?, version = version + 1, updated_at = ?
         WHERE id = ? AND project_id = ? AND version = ? AND lifecycle = ?",
    )
    .bind(milestone_lifecycle_name(request.lifecycle))
    .bind(&updated_at)
    .bind(&milestone_id)
    .bind(&project_id)
    .bind(request.mutation.expected_version)
    .bind(&current_name)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the milestone changed before its lifecycle transition",
        ));
    }
    // The primary pointer is an explicit Project invariant. Delivery keeps
    // pointing at the emphasized milestone; only removal from the intended
    // outcome set may repair it.
    repair_primary_pointer_in_tx(&mut tx, &project_id, &milestone_id).await?;
    let event_id = new_uuid_v4();
    let event_inserted = append_milestone_event_in_tx(
        &state,
        &mut tx,
        "milestone.lifecycle.transitioned",
        &project_id,
        &milestone_id,
        &user.user_id,
        &event_id,
        &request.mutation.authorization,
        &request.mutation.idempotency_key,
        json!({
            "milestone_id": milestone_id,
            "lifecycle": milestone_lifecycle_name(request.lifecycle),
            "expected_milestone_version": request.mutation.expected_version,
            "principal_id": user.user_id,
        }),
        &updated_at,
    )
    .await?;
    if !event_inserted {
        tx.rollback().await?;
        return Err(ApiError::conflict_with_code(
            "idempotency_in_progress",
            "another request committed this milestone lifecycle transition; retry with the same key",
        ));
    }
    tx.commit().await?;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let milestone = runtime
        .get(&project_id, &milestone_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("milestone", milestone_id))?;
    Ok(Json(milestone))
}

async fn replay_milestone_check_result(
    state: &AppState,
    project_id: &str,
    milestone_id: &str,
    check_id: &str,
    user_id: &str,
    storage_idempotency_key: &str,
    request: &RecordMilestoneCheckRequest,
) -> ApiResult<Option<ValidationResult>> {
    let Some(existing) =
        sqlx::query("SELECT * FROM project_milestone_check_result WHERE idempotency_key = ?")
            .bind(storage_idempotency_key)
            .fetch_optional(state.db.pool())
            .await?
    else {
        return Ok(None);
    };

    let existing_manifest: serde_json::Value =
        serde_json::from_str(&existing.try_get::<String, _>("source_manifest_json")?)
            .map_err(|_| ApiError::internal("persisted manual check result is invalid"))?;
    let existing_result = existing_manifest
        .get("result")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ApiError::internal("persisted manual check result text is missing"))?;
    let existing_definition = existing_manifest
        .get("check_definition_revision_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            ApiError::internal("persisted manual check definition reference is missing")
        })?;
    let existing_governing_revision_ids = existing_manifest
        .get("governing_revision_ids")
        .cloned()
        .map(serde_json::from_value::<Vec<String>>)
        .transpose()
        .map_err(|_| ApiError::internal("persisted manual check governing revisions are invalid"))?
        .ok_or_else(|| {
            ApiError::internal("persisted manual check governing revisions are missing")
        })?;
    let existing_charter_revision_id =
        existing.try_get::<Option<String>, _>("governing_charter_revision_id")?;
    let requested_charter_revision_id = request.governing_revision_ids.first().map(String::as_str);

    let mismatch = request.check_id != check_id
        || request.mutation.authorization.principal.kind != PrincipalKind::User
        || request.mutation.authorization.principal.id != user_id
        || existing.try_get::<String, _>("project_id")? != project_id
        || existing.try_get::<String, _>("milestone_id")? != milestone_id
        || existing.try_get::<String, _>("check_id")? != check_id
        || existing.try_get::<String, _>("source_kind")? != "manual"
        || existing.try_get::<String, _>("definition_revision_id")?
            != request.definition_revision_id
        || existing.try_get::<String, _>("outcome")? != check_result_outcome(request.status)
        || existing.try_get::<String, _>("principal_type")? != "user"
        || existing.try_get::<String, _>("principal_id")? != user_id
        || existing.try_get::<String, _>("input_digest")? != request.input_digest
        || existing.try_get::<i64, _>("expected_version")? != request.mutation.expected_version
        || existing.try_get::<String, _>("authorization_basis")?
            != request.mutation.authorization.authorization_basis
        || existing.try_get::<String, _>("authorization_action")?
            != request.mutation.authorization.action
        || existing.try_get::<String, _>("explicit_event")?
            != request.mutation.authorization.event_id
        || existing.try_get::<String, _>("authorization_occurred_at")?
            != request.mutation.authorization.occurred_at
        || existing_result != request.result
        || existing_definition != request.definition_revision_id
        || existing_governing_revision_ids != request.governing_revision_ids
        || existing_charter_revision_id.as_deref() != requested_charter_revision_id;
    if mismatch {
        return Err(ApiError::conflict_with_code(
            "idempotency_conflict",
            "the manual check idempotency key was already used for a different result",
        ));
    }

    Ok(Some(validation_result_from_row(&existing, project_id)?))
}

pub async fn record_milestone_check(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id, check_id)): Path<(String, String, String)>,
    Json(request): Json<RecordMilestoneCheckRequest>,
) -> ApiResult<Json<ValidationResult>> {
    require_idempotency_key(&request.mutation.idempotency_key)?;
    ensure_project_access(&state, &project_id, &user).await?;
    let storage_idempotency_key = scoped_idempotency_key(
        "milestone-check",
        &project_id,
        &user.user_id,
        &request.mutation.idempotency_key,
    );
    if let Some(replay) = replay_milestone_check_result(
        &state,
        &project_id,
        &milestone_id,
        &check_id,
        &user.user_id,
        &storage_idempotency_key,
        &request,
    )
    .await?
    {
        return Ok(Json(replay));
    }
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        CHECK_RESULT_ACTION,
    )?;
    if request.check_id != check_id {
        return Err(ApiError::bad_request(
            "check_id in path and request must match",
        ));
    }
    if request.input_digest.trim().is_empty() || request.result.trim().is_empty() {
        return Err(ApiError::bad_request(
            "manual check input_digest and result are required",
        ));
    }
    if !matches!(
        request.status,
        api_types::AcceptanceCheckResultStatus::Pass
            | api_types::AcceptanceCheckResultStatus::Fail
            | api_types::AcceptanceCheckResultStatus::Blocked
            | api_types::AcceptanceCheckResultStatus::Stale
            | api_types::AcceptanceCheckResultStatus::Unavailable
    ) {
        return Err(ApiError::bad_request(
            "manual check result must be pass, fail, blocked, stale, or unavailable",
        ));
    }
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let check = sqlx::query(
        "SELECT c.version, c.definition_revision_id, c.source_kind,
                m.current_definition_revision_id, r.author_id
         FROM project_milestone_check c
         JOIN project_milestone m ON m.id = c.milestone_id AND m.project_id = c.project_id
         LEFT JOIN project_milestone_revision r ON r.id = c.definition_revision_id
         WHERE c.id = ? AND c.project_id = ? AND c.milestone_id = ?",
    )
    .bind(&check_id)
    .bind(&project_id)
    .bind(&milestone_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::not_found("milestone_check", check_id.clone()))?;
    let check_version: i64 = check.try_get("version")?;
    let check_definition_revision_id: String = check.try_get("definition_revision_id")?;
    let current_definition_revision_id: String = check
        .try_get::<Option<String>, _>("current_definition_revision_id")?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                "definition_required",
                "manual acceptance requires the milestone's current definition revision",
            )
        })?;
    let source_kind: String = check.try_get("source_kind")?;
    let author_id: String = check
        .try_get::<Option<String>, _>("author_id")?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                "definition_author_required",
                "manual acceptance requires an authored definition revision",
            )
        })?;
    if source_kind != "manual" {
        return Err(ApiError::bad_request(
            "only manual acceptance checks accept this endpoint",
        ));
    }
    if request.mutation.expected_version != check_version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the acceptance check changed before manual attestation",
        ));
    }
    if request.definition_revision_id != check_definition_revision_id {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the acceptance check belongs to a different immutable definition revision",
        ));
    }
    if check_definition_revision_id != current_definition_revision_id {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the acceptance check belongs to a superseded milestone definition revision",
        ));
    }
    let governance =
        sqlx::query("SELECT p.current_charter_revision_id FROM project p WHERE p.id = ? LIMIT 1")
            .bind(&project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| ApiError::not_found("project", &project_id))?;
    let governing_charter_revision_id: String = governance
        .try_get::<Option<String>, _>("current_charter_revision_id")?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                "charter_required",
                "manual acceptance requires the current approved Project Charter",
            )
        })?;
    let derived_governing_revision_ids = vec![governing_charter_revision_id.clone()];
    if request.governing_revision_ids != derived_governing_revision_ids {
        return Err(ApiError::conflict_with_code(
            "governing_revision_conflict",
            "manual acceptance governing revisions must match the current approved Charter",
        ));
    }
    if author_id == user.user_id {
        return Err(ApiError::forbidden_with_code(
            "self_attestation_denied",
            "the definition author cannot attest its own manual check",
        ));
    }
    let outcome = check_result_outcome(request.status);
    let result_id = new_uuid_v4();
    let created_at = now_rfc3339();
    let input_digest = request.input_digest.clone();
    let source_manifest = serde_json::json!({
        "result": request.result.clone(),
        "governing_revision_ids": request.governing_revision_ids.clone(),
        "check_definition_revision_id": check_definition_revision_id.clone(),
    });
    let inserted = sqlx::query(
        "INSERT INTO project_milestone_check_result (
            id, project_id, milestone_id, check_id, definition_revision_id,
            outcome, source_kind, source_manifest_json, input_digest,
            governing_charter_revision_id,
            principal_type, principal_id, authorization_basis, authorization_action,
            expected_version, explicit_event, authorization_occurred_at,
            idempotency_key, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, 'manual', ?, ?, ?, 'user', ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&result_id)
    .bind(&project_id)
    .bind(&milestone_id)
    .bind(&check_id)
    .bind(&check_definition_revision_id)
    .bind(outcome)
    .bind(source_manifest.to_string())
    .bind(&input_digest)
    .bind(&governing_charter_revision_id)
    .bind(&user.user_id)
    .bind(&request.mutation.authorization.authorization_basis)
    .bind(&request.mutation.authorization.action)
    .bind(request.mutation.expected_version)
    .bind(&request.mutation.authorization.event_id)
    .bind(&request.mutation.authorization.occurred_at)
    .bind(&storage_idempotency_key)
    .bind(&created_at)
    .execute(&mut *tx)
    .await;
    if let Err(error) = inserted {
        if error.to_string().to_ascii_lowercase().contains("unique") {
            return Err(ApiError::conflict_with_code(
                "idempotency_in_progress",
                "another manual check result is being committed; retry with the same key",
            ));
        }
        return Err(error.into());
    }
    let updated = sqlx::query(
        "UPDATE project_milestone_check SET current_result_id = ?, version = version + 1,
         updated_at = ? WHERE id = ? AND version = ?",
    )
    .bind(&result_id)
    .bind(&created_at)
    .bind(&check_id)
    .bind(check_version)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the acceptance check changed before its result was committed",
        ));
    }
    let event = DomainEventRepo::append_event_in_tx(
        &*state.db,
        &mut tx,
        &CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "milestone.check.recorded".to_owned(),
            entity_type: "milestone_check_result".to_owned(),
            entity_id: result_id.clone(),
            actor_type: "user".to_owned(),
            actor_id: Some(user.user_id.clone()),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: request.mutation.authorization.event_id.clone(),
            causation_id: Some(request.mutation.authorization.event_id.clone()),
            causation_depth: 0,
            dedupe_key: Some(format!(
                "milestone.check.recorded:{project_id}:{}",
                request.mutation.idempotency_key
            )),
            payload_json: serde_json::json!({
                "project_id": project_id,
                "milestone_id": milestone_id,
                "check_id": check_id,
                "result_id": result_id,
                "status": outcome,
                "authorization": request.mutation.authorization.clone(),
                "governing_revision_ids": request.governing_revision_ids.clone(),
            })
            .to_string(),
            created_at: created_at.clone(),
        },
    )
    .await?;
    if event.entity_id != result_id {
        tx.rollback().await?;
        return Err(ApiError::conflict_with_code(
            "idempotency_in_progress",
            "another manual check result committed; retry with the same key",
        ));
    }
    tx.commit().await?;
    let status = request.status;
    let result_text = request.result.clone();
    let governing_revision_ids = request.governing_revision_ids.clone();
    let authorization = request.mutation.authorization.clone();
    let event_id = authorization.event_id.clone();
    let result = ValidationResult {
        id: result_id.clone(),
        project_id,
        check_id: check_id.clone(),
        status,
        result: result_text.clone(),
        principal: user_principal(&user.user_id),
        authorization: authorization.clone(),
        input_digest: input_digest.clone(),
        governing_revision_ids: governing_revision_ids.clone(),
        expected_version: check_version,
        event_id,
        evaluated_at: created_at.clone(),
        result_digest: canonical_digest_with_schema(
            services::MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
            &serde_json::json!({
                "id": result_id,
                "check_id": check_id,
                "status": status,
                "result": result_text,
                "input_digest": input_digest,
                "governing_revision_ids": governing_revision_ids,
                "authorization": authorization,
                "evaluated_at": created_at,
                "source_manifest": source_manifest,
            }),
        )
        .map_err(|error| ApiError::internal(error.to_string()))?,
    };
    Ok(Json(result))
}

async fn replay_milestone_check_waiver(
    state: &AppState,
    project_id: &str,
    milestone_id: &str,
    check_id: &str,
    user_id: &str,
    request: &WaiveMilestoneCheckRequest,
    waiver_dedupe: &str,
) -> ApiResult<Option<serde_json::Value>> {
    let Some(event) = DomainEventRepo::get_event_by_dedupe(&*state.db, waiver_dedupe).await? else {
        return Ok(None);
    };
    let mut payload: serde_json::Value = serde_json::from_str(&event.payload_json)
        .map_err(|_| ApiError::internal("persisted milestone waiver event is invalid"))?;
    // Older persisted waiver events did not include the response timestamp;
    // the domain-event timestamp is the same value emitted by the mutation.
    if payload.get("created_at").is_none() {
        payload["created_at"] = json!(event.created_at.clone());
    }
    let waiver_id = payload
        .get("waiver_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ApiError::internal("persisted milestone waiver id is missing"))?;
    let payload_policy_revision = payload
        .get("governing_policy_revision")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ApiError::internal("persisted milestone waiver policy revision is missing")
        })?;
    let payload_policy_digest = payload
        .get("governing_policy_digest")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ApiError::internal("persisted milestone waiver policy digest is missing"))?;
    let decision = sqlx::query(
        "SELECT state, decision_class, rationale, selected_outcome,
                principal_type, principal_id, authority_basis,
                authorization_action, explicit_event,
                authorization_occurred_at, charter_revision_id,
                affected_records_json
         FROM project_decision WHERE id = ? AND project_id = ?",
    )
    .bind(waiver_id)
    .bind(project_id)
    .fetch_optional(state.db.pool())
    .await?;
    let decision_matches = if let Some(decision) = decision {
        let affected: serde_json::Value =
            serde_json::from_str(&decision.try_get::<String, _>("affected_records_json")?)
                .map_err(|_| ApiError::internal("persisted waiver decision records are invalid"))?;
        let decision_state = decision.try_get::<String, _>("state")?;
        matches!(
            decision_state.as_str(),
            "active" | "superseded" | "invalidated"
        ) && decision.try_get::<String, _>("decision_class")? == "waiver"
            && decision.try_get::<String, _>("rationale")? == request.reason
            && decision.try_get::<String, _>("selected_outcome")? == "waived"
            && decision.try_get::<String, _>("principal_type")? == "user"
            && decision.try_get::<String, _>("principal_id")? == user_id
            && decision.try_get::<String, _>("authority_basis")?
                == request.mutation.authorization.authorization_basis
            && decision.try_get::<String, _>("authorization_action")?
                == request.mutation.authorization.action
            && decision.try_get::<String, _>("explicit_event")?
                == request.mutation.authorization.event_id
            && decision.try_get::<String, _>("authorization_occurred_at")?
                == request.mutation.authorization.occurred_at
            && decision.try_get::<String, _>("charter_revision_id")?
                == request
                    .governing_revision_ids
                    .first()
                    .cloned()
                    .unwrap_or_default()
            && affected
                .get("milestone_id")
                .and_then(serde_json::Value::as_str)
                == Some(milestone_id)
            && affected.get("check_id").and_then(serde_json::Value::as_str) == Some(check_id)
            && affected
                .get("definition_revision_id")
                .and_then(serde_json::Value::as_str)
                == Some(request.definition_revision_id.as_str())
            && affected
                .get("input_digest")
                .and_then(serde_json::Value::as_str)
                == Some(request.input_digest.as_str())
            && affected
                .get("expected_version")
                .and_then(serde_json::Value::as_i64)
                == Some(request.mutation.expected_version)
            && affected
                .get("governing_revision_ids")
                .cloned()
                .and_then(|value| serde_json::from_value::<Vec<String>>(value).ok())
                .as_ref()
                == Some(&request.governing_revision_ids)
            && affected
                .get("governing_policy_revision")
                .and_then(serde_json::Value::as_str)
                == Some(payload_policy_revision)
            && affected
                .get("governing_policy_digest")
                .and_then(serde_json::Value::as_str)
                == Some(payload_policy_digest)
    } else {
        false
    };
    let same_request = request.check_id == check_id
        && request.mutation.authorization.principal.kind == PrincipalKind::User
        && request.mutation.authorization.principal.id == user_id
        && payload
            .get("project_id")
            .and_then(serde_json::Value::as_str)
            == Some(project_id)
        && payload
            .get("milestone_id")
            .and_then(serde_json::Value::as_str)
            == Some(milestone_id)
        && payload.get("check_id").and_then(serde_json::Value::as_str) == Some(check_id)
        && payload
            .get("definition_revision_id")
            .and_then(serde_json::Value::as_str)
            == Some(request.definition_revision_id.as_str())
        && payload.get("reason").and_then(serde_json::Value::as_str)
            == Some(request.reason.as_str())
        && payload
            .get("input_digest")
            .and_then(serde_json::Value::as_str)
            == Some(request.input_digest.as_str())
        && payload
            .get("expected_version")
            .and_then(serde_json::Value::as_i64)
            == Some(request.mutation.expected_version)
        && payload
            .get("principal_id")
            .and_then(serde_json::Value::as_str)
            == Some(user_id)
        && payload
            .get("authorization")
            .cloned()
            .and_then(|value| serde_json::from_value::<AuthorizationProvenance>(value).ok())
            .as_ref()
            == Some(&request.mutation.authorization)
        && payload
            .get("governing_revision_ids")
            .cloned()
            .and_then(|value| serde_json::from_value::<Vec<String>>(value).ok())
            .as_ref()
            == Some(&request.governing_revision_ids)
        && payload
            .get("governing_policy_revision")
            .and_then(serde_json::Value::as_str)
            == Some(payload_policy_revision)
        && payload
            .get("governing_policy_digest")
            .and_then(serde_json::Value::as_str)
            == Some(payload_policy_digest);
    if !same_request || !decision_matches {
        return Err(ApiError::conflict_with_code(
            "idempotency_conflict",
            "the waiver idempotency key was already used for a different waiver",
        ));
    }
    Ok(Some(payload))
}

pub async fn waive_milestone_check(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id, check_id)): Path<(String, String, String)>,
    Json(request): Json<WaiveMilestoneCheckRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    require_idempotency_key(&request.mutation.idempotency_key)?;
    let waiver_dedupe = format!(
        "milestone.check.waived:{project_id}:{}",
        request.mutation.idempotency_key
    );
    // Resolve the immutable waiver receipt before current Project access,
    // authority, check, or governance validation. Replays must return the
    // persisted decision (or conflict) even after mutable state and
    // authorization age.
    if let Some(replay) = replay_milestone_check_waiver(
        &state,
        &project_id,
        &milestone_id,
        &check_id,
        &user.user_id,
        &request,
        &waiver_dedupe,
    )
    .await?
    {
        return Ok(Json(replay));
    }
    ensure_project_access(&state, &project_id, &user).await?;
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        CHECK_WAIVE_ACTION,
    )?;
    if request.check_id != check_id
        || request.reason.trim().is_empty()
        || request.input_digest.trim().is_empty()
    {
        return Err(ApiError::bad_request(
            "check_id must match the path and waiver reason is required",
        ));
    }
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let check_row = sqlx::query(
        "SELECT c.version, m.current_definition_revision_id, r.author_id
         FROM project_milestone_check c
         JOIN project_milestone m
           ON m.id = c.milestone_id AND m.project_id = c.project_id
         JOIN project_milestone_revision r ON r.id = c.definition_revision_id
         WHERE c.id = ? AND c.project_id = ? AND c.milestone_id = ?
           AND c.definition_revision_id = ? AND c.version = ?",
    )
    .bind(&check_id)
    .bind(&project_id)
    .bind(&milestone_id)
    .bind(&request.definition_revision_id)
    .bind(request.mutation.expected_version)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(check_row) = check_row else {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the acceptance check target/version is stale or cross-Project",
        ));
    };
    let author_id: String = check_row
        .try_get::<Option<String>, _>("author_id")?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                "definition_author_required",
                "waivers require an authored definition revision",
            )
        })?;
    let current_definition_revision_id: String = check_row
        .try_get::<Option<String>, _>("current_definition_revision_id")?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                "definition_required",
                "waivers require the milestone's current definition revision",
            )
        })?;
    if current_definition_revision_id != request.definition_revision_id {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the acceptance check belongs to a superseded milestone definition revision",
        ));
    }
    if author_id == user.user_id {
        return Err(ApiError::forbidden_with_code(
            "self_waiver_denied",
            "the definition author cannot waive its own acceptance check",
        ));
    }
    let governance =
        sqlx::query("SELECT p.current_charter_revision_id FROM project p WHERE p.id = ? LIMIT 1")
            .bind(&project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| ApiError::not_found("project", &project_id))?;
    let waiver_charter_revision_id: String = governance
        .try_get::<Option<String>, _>("current_charter_revision_id")?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                "charter_required",
                "waivers require the current approved Project Charter",
            )
        })?;
    let derived_governing_revision_ids = vec![waiver_charter_revision_id.clone()];
    if request.governing_revision_ids != derived_governing_revision_ids {
        return Err(ApiError::conflict_with_code(
            "governing_revision_conflict",
            "waiver governing revisions must match the current approved Charter",
        ));
    }
    let waiver_id = new_uuid_v4();
    let created_at = now_rfc3339();
    let event = DomainEventRepo::append_event_in_tx(
        &*state.db,
        &mut tx,
        &CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "milestone.check.waived".to_owned(),
            entity_type: "milestone_check_waiver".to_owned(),
            entity_id: waiver_id.clone(),
            actor_type: "user".to_owned(),
            actor_id: Some(user.user_id.clone()),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: request.mutation.authorization.event_id.clone(),
            causation_id: Some(request.mutation.authorization.event_id.clone()),
            causation_depth: 0,
            dedupe_key: Some(format!(
                "milestone.check.waived:{project_id}:{}",
                request.mutation.idempotency_key
            )),
            payload_json: serde_json::json!({
                "waiver_id": waiver_id,
                "project_id": project_id,
                "milestone_id": milestone_id,
                "check_id": check_id,
                "definition_revision_id": request.definition_revision_id.clone(),
                "reason": request.reason.clone(),
                "input_digest": request.input_digest.clone(),
                "expected_version": request.mutation.expected_version,
                "principal_id": user.user_id.clone(),
                "authorization": request.mutation.authorization.clone(),
                "governing_revision_ids": request.governing_revision_ids.clone(),
                "created_at": created_at.clone(),
            })
            .to_string(),
            created_at: created_at.clone(),
        },
    )
    .await?;
    if event.entity_id != waiver_id {
        tx.rollback().await?;
        return Err(ApiError::conflict_with_code(
            "idempotency_in_progress",
            "another waiver committed; retry with the same key",
        ));
    }
    // A waiver is an immutable effective Project Decision.  Readiness uses
    // only these user-authored decisions and never a caller-supplied waiver id.
    sqlx::query(
        "INSERT INTO project_decision (
            id, project_id, state, decision_class, question, context_json,
            selected_outcome, rationale, principal_type, principal_id,
            authority_basis, authorization_action, explicit_event,
            authorization_occurred_at, charter_revision_id,
            source_refs_json, affected_records_json, created_at
         ) VALUES (?, ?, 'active', 'waiver', ?, '{}', 'waived', ?, 'user', ?, ?, ?, ?, ?, ?, '[]', ?, ?)",
    )
    .bind(&waiver_id)
    .bind(&project_id)
    .bind(format!("Waive milestone check {check_id}"))
    .bind(&request.reason)
    .bind(&user.user_id)
    .bind(&request.mutation.authorization.authorization_basis)
    .bind(&request.mutation.authorization.action)
    .bind(&request.mutation.authorization.event_id)
    .bind(&request.mutation.authorization.occurred_at)
    .bind(&waiver_charter_revision_id)
    .bind(
        serde_json::json!({
            "milestone_id": milestone_id,
            "check_id": check_id,
            "definition_revision_id": request.definition_revision_id.clone(),
            "input_digest": request.input_digest.clone(),
            "expected_version": request.mutation.expected_version,
            "governing_revision_ids": request.governing_revision_ids.clone(),
        })
        .to_string(),
    )
    .bind(&created_at)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(serde_json::json!({
        "waiver_id": waiver_id,
        "project_id": project_id,
        "milestone_id": milestone_id,
        "check_id": check_id,
        "definition_revision_id": request.definition_revision_id,
        "reason": request.reason,
        "input_digest": request.input_digest,
        "expected_version": request.mutation.expected_version,
        "principal_id": user.user_id,
        "authorization": request.mutation.authorization,
        "governing_revision_ids": request.governing_revision_ids,
        "created_at": created_at,
    })))
}

pub async fn evaluate_readiness(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
    Json(request): Json<EvaluateMilestoneReadinessRequest>,
) -> ApiResult<Json<ReadinessSnapshot>> {
    ensure_project_access(&state, &project_id, &user).await?;
    if request.milestone_id != milestone_id {
        return Err(ApiError::bad_request(
            "milestone_id in the path and request must match",
        ));
    }
    require_idempotency_key(&request.mutation.idempotency_key)?;
    let record = services::ProjectMilestoneCommandService::new(state.db.clone())
        .request_readiness(
            services::ProjectReadinessRequestCommand {
                project_id: project_id.clone(),
                milestone_id: milestone_id.clone(),
                expected_milestone_version: request.mutation.expected_version,
                idempotency_key: request.mutation.idempotency_key,
                authenticated_user_id: Some(user.user_id.clone()),
                authorization: readiness_authorization(
                    &request.mutation.authorization,
                    &user.user_id,
                    READINESS_ACTION,
                )?,
            },
            None,
        )
        .await?;
    let snapshot_id = record.id.clone();
    let runtime = MilestoneRuntime::new(state.db.clone());
    let snapshot = runtime
        .get_readiness(&project_id, &milestone_id, &snapshot_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("readiness_snapshot", snapshot_id))?;
    Ok(Json(snapshot))
}

pub async fn list_readiness_snapshots(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
    Query(query): Query<MilestoneListQuery>,
) -> ApiResult<Json<ReadinessSnapshotListResponse>> {
    ensure_project_access(&state, &project_id, &user).await?;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let mut items = runtime
        .list_readiness(&project_id, &milestone_id)
        .await
        .map_err(ApiError::from)?;
    let cursor = decode_cursor(query.cursor.as_deref())?;
    if let Some((created_at, id)) = cursor {
        items.retain(|item| {
            item.computed_at > created_at || (item.computed_at == created_at && item.id > id)
        });
    }
    let limit = bounded_limit(query.limit) as usize;
    let has_more = items.len() > limit;
    items.truncate(limit);
    let next_cursor = items
        .last()
        .map(|item| encode_cursor(&item.computed_at, &item.id));
    Ok(Json(ReadinessSnapshotListResponse {
        items,
        next_cursor: next_cursor.filter(|_| has_more),
        has_more,
    }))
}

pub async fn get_readiness_snapshot(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id, snapshot_id)): Path<(String, String, String)>,
) -> ApiResult<Json<ReadinessSnapshot>> {
    ensure_project_access(&state, &project_id, &user).await?;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let snapshot = runtime
        .get_readiness(&project_id, &milestone_id, &snapshot_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("readiness_snapshot", snapshot_id))?;
    Ok(Json(snapshot))
}

pub async fn release_milestone(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
    Json(request): Json<ReleaseMilestoneRequest>,
) -> ApiResult<Json<ProjectRelease>> {
    ensure_project_access(&state, &project_id, &user).await?;
    if request.milestone_id != milestone_id {
        return Err(ApiError::bad_request(
            "milestone_id in the path and request must match",
        ));
    }
    let runtime = MilestoneRuntime::new(state.db.clone());
    let release = runtime
        .release(
            &project_id,
            &user_principal(&user.user_id),
            &request.mutation.authorization,
            &milestone_id,
            request.mutation.expected_version,
            &request.readiness_snapshot_id,
            &request.readiness_digest,
            &request.mutation.idempotency_key,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(release))
}

pub async fn get_release(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, release_id)): Path<(String, String)>,
) -> ApiResult<Json<ProjectRelease>> {
    ensure_project_access(&state, &project_id, &user).await?;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let release = runtime
        .get_release(&project_id, &release_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("project_release", release_id))?;
    Ok(Json(release))
}

pub async fn list_releases(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
    Query(query): Query<MilestoneListQuery>,
) -> ApiResult<Json<ProjectReleaseListResponse>> {
    ensure_project_access(&state, &project_id, &user).await?;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let mut items = runtime
        .list_releases(&project_id, &milestone_id)
        .await
        .map_err(ApiError::from)?;
    let cursor = decode_cursor(query.cursor.as_deref())?;
    if let Some((revision, id)) = cursor {
        let revision = revision
            .parse::<i64>()
            .map_err(|_| ApiError::bad_request("invalid cursor"))?;
        items.retain(|item| item.version > revision || (item.version == revision && item.id > id));
    }
    let limit = bounded_limit(query.limit) as usize;
    let has_more = items.len() > limit;
    items.truncate(limit);
    let next_cursor = items
        .last()
        .map(|item| encode_cursor(&item.version.to_string(), &item.id));
    Ok(Json(ProjectReleaseListResponse {
        items,
        next_cursor: next_cursor.filter(|_| has_more),
        has_more,
    }))
}

pub async fn set_primary_milestone(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<SetPrimaryMilestoneRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    ensure_project_access(&state, &project_id, &user).await?;
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        PRIMARY_ACTION,
    )?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    let updated = services::ProjectMilestoneCommandService::new(state.db.clone())
        .set_primary_milestone(
            services::ProjectPrimaryMilestoneCommand {
                project_id: project_id.clone(),
                primary_milestone_id: request.primary_milestone_id,
                expected_project_version: request.mutation.expected_version,
                idempotency_key: request.mutation.idempotency_key,
                authorization: milestone_authorization(
                    &request.mutation.authorization,
                    &user.user_id,
                    PRIMARY_ACTION,
                )?,
            },
            None,
        )
        .await?;
    Ok(Json(serde_json::json!({
        "project_id": project_id,
        "primary_milestone_id": updated.primary_milestone_id,
        "version": updated.version,
    })))
}

async fn materialize_check_definitions_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    project_id: &str,
    milestone_id: &str,
    definition_revision_id: &str,
    checks: &[api_types::MilestoneAcceptanceCheck],
    evidence_requirements: &[api_types::AcceptanceEvidenceRequirement],
    updated_at: &str,
) -> ApiResult<()> {
    let mut evidence_by_id = std::collections::HashMap::new();
    for requirement in evidence_requirements {
        if requirement.id.trim().is_empty()
            || requirement.description.trim().is_empty()
            || evidence_by_id
                .insert(requirement.id.as_str(), requirement)
                .is_some()
        {
            return Err(ApiError::bad_request(
                "milestone evidence requirements require unique stable ids and descriptions",
            ));
        }
        if requirement.evidence_kind.as_deref().is_some_and(|kind| {
            !matches!(
                kind,
                "screenshot" | "walkthrough_video" | "log" | "report" | "other"
            )
        }) {
            return Err(ApiError::bad_request(
                "milestone evidence_kind must be one of: screenshot, walkthrough_video, log, report, other",
            ));
        }
        if requirement.required && !checks.iter().any(|check| check.id == requirement.id) {
            return Err(ApiError::bad_request(format!(
                "required evidence '{}' must reference an acceptance check with the same stable id",
                requirement.id
            )));
        }
    }

    let mut seen = std::collections::HashSet::new();
    for check in checks {
        if !seen.insert(check.id.as_str()) {
            return Err(ApiError::bad_request(
                "milestone acceptance check IDs must be unique within a revision",
            ));
        }
        if check.id.trim().is_empty() || check.description.trim().is_empty() {
            return Err(ApiError::bad_request(
                "milestone acceptance checks require stable ids and descriptions",
            ));
        }
        let evidence_required = evidence_by_id
            .get(check.id.as_str())
            .is_some_and(|requirement| requirement.required);
        if check.required && !evidence_required {
            return Err(ApiError::bad_request(format!(
                "required acceptance check '{}' requires a required evidence requirement with the same stable id",
                check.id
            )));
        }
        // Only check kinds with an authoritative server projection are
        // admitted into a release-gating definition.  A check result cannot
        // be supplied by the caller for document/media/git sources, and
        // silently accepting those definitions would make every such gate
        // permanently or accidentally missing.  Policy waivers remain a
        // valid definition kind because their authority is the immutable
        // user Decision written by the waiver route, and `task_validation`
        // joins them because `project.validation` writes its result with a
        // server-derived, receipt-backed provenance.
        if !matches!(
            check.source_kind,
            api_types::AcceptanceCheckSourceKind::Manual
                | api_types::AcceptanceCheckSourceKind::PolicyWaiver
                | api_types::AcceptanceCheckSourceKind::TaskValidation
        ) {
            return Err(ApiError::bad_request(
                "this acceptance check source kind is not currently admitted without an authoritative projection",
            ));
        }
        let source_kind = acceptance_source_kind_name(check.source_kind);
        let existing: Option<(String, String, String)> = sqlx::query_as(
            "SELECT project_id, milestone_id, definition_revision_id
             FROM project_milestone_check WHERE id = ?",
        )
        .bind(&check.id)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some((existing_project, existing_milestone, existing_revision)) = existing {
            if existing_project != project_id || existing_milestone != milestone_id {
                return Err(ApiError::bad_request(
                    "milestone acceptance check belongs to another Project or milestone",
                ));
            }
            if existing_revision == definition_revision_id {
                continue;
            }
            sqlx::query(
                "UPDATE project_milestone_check
                 SET definition_revision_id = ?, check_key = ?, description = ?, required = ?,
                     source_kind = ?, expected_result = ?, evidence_required = ?,
                     version = version + 1, current_result_id = NULL, updated_at = ?
                 WHERE id = ? AND project_id = ? AND milestone_id = ?",
            )
            .bind(definition_revision_id)
            .bind(&check.id)
            .bind(&check.description)
            .bind(check.required)
            .bind(source_kind)
            .bind(&check.expected_result)
            .bind(evidence_required)
            .bind(updated_at)
            .bind(&check.id)
            .bind(project_id)
            .bind(milestone_id)
            .execute(&mut **tx)
            .await?;
        } else {
            sqlx::query(
                "INSERT INTO project_milestone_check (
                    id, project_id, milestone_id, definition_revision_id, check_key,
                    description, required, source_kind, expected_result,
                    evidence_required, version, current_result_id, created_at, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?)",
            )
            .bind(&check.id)
            .bind(project_id)
            .bind(milestone_id)
            .bind(definition_revision_id)
            .bind(&check.id)
            .bind(&check.description)
            .bind(check.required)
            .bind(source_kind)
            .bind(&check.expected_result)
            .bind(evidence_required)
            .bind(updated_at)
            .bind(updated_at)
            .execute(&mut **tx)
            .await?;
        }
    }
    Ok(())
}

fn acceptance_source_kind_name(value: api_types::AcceptanceCheckSourceKind) -> &'static str {
    match value {
        api_types::AcceptanceCheckSourceKind::TaskValidation => "task_validation",
        api_types::AcceptanceCheckSourceKind::DocumentApproval => "document_approval",
        api_types::AcceptanceCheckSourceKind::Manual => "manual",
        api_types::AcceptanceCheckSourceKind::PolicyWaiver => "policy_waiver",
        api_types::AcceptanceCheckSourceKind::MediaEvidence => "media_evidence",
        api_types::AcceptanceCheckSourceKind::GitRef => "git_ref",
    }
}

#[allow(clippy::too_many_arguments)]
async fn append_milestone_event_in_tx(
    state: &AppState,
    tx: &mut Transaction<'_, Sqlite>,
    event_type: &str,
    project_id: &str,
    milestone_id: &str,
    actor_id: &str,
    event_id: &str,
    authorization: &AuthorizationProvenance,
    idempotency_key: &str,
    payload: serde_json::Value,
    created_at: &str,
) -> ApiResult<bool> {
    let event = DomainEventRepo::append_event_in_tx(
        &*state.db,
        tx,
        &CreateDomainEvent {
            id: event_id.to_owned(),
            event_type: event_type.to_owned(),
            entity_type: "milestone".to_owned(),
            entity_id: milestone_id.to_owned(),
            actor_type: "user".to_owned(),
            actor_id: Some(actor_id.to_owned()),
            scope_type: "project".to_owned(),
            scope_id: project_id.to_owned(),
            correlation_id: Uuid::new_v4().to_string(),
            causation_id: Some(authorization.event_id.clone()),
            causation_depth: 0,
            dedupe_key: Some(format!("{event_type}:{project_id}:{idempotency_key}")),
            payload_json: payload.to_string(),
            created_at: created_at.to_owned(),
        },
    )
    .await
    .map_err(ApiError::from)?;
    Ok(event.id == event_id)
}

async fn repair_primary_pointer_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    project_id: &str,
    changed_milestone_id: &str,
) -> ApiResult<()> {
    let primary: Option<String> =
        sqlx::query_scalar("SELECT primary_milestone_id FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_one(&mut **tx)
            .await?;
    if primary.as_deref() != Some(changed_milestone_id) {
        return Ok(());
    }
    let lifecycle: String = sqlx::query_scalar(
        "SELECT lifecycle FROM project_milestone WHERE id = ? AND project_id = ?",
    )
    .bind(changed_milestone_id)
    .bind(project_id)
    .fetch_one(&mut **tx)
    .await?;
    if matches!(
        lifecycle.as_str(),
        "planned" | "active" | "ready_for_release" | "released"
    ) {
        return Ok(());
    }
    let replacement: Option<String> = sqlx::query_scalar(
        "SELECT id FROM project_milestone
         WHERE project_id = ? AND lifecycle = 'active' AND id != ?
         ORDER BY milestone_sequence ASC, id ASC LIMIT 1",
    )
    .bind(project_id)
    .bind(changed_milestone_id)
    .fetch_optional(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE project SET primary_milestone_id = ?, version = version + 1,
         updated_at = ? WHERE id = ? AND primary_milestone_id = ?",
    )
    .bind(replacement)
    .bind(now_rfc3339())
    .bind(project_id)
    .bind(changed_milestone_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn milestone_definition_lifecycle_name(value: MilestoneDefinitionLifecycle) -> &'static str {
    match value {
        MilestoneDefinitionLifecycle::Draft => "draft",
        MilestoneDefinitionLifecycle::Proposed => "proposed",
        MilestoneDefinitionLifecycle::Approved => "approved",
        MilestoneDefinitionLifecycle::Superseded => "superseded",
    }
}

fn parse_definition_lifecycle(value: &str) -> ApiResult<MilestoneDefinitionLifecycle> {
    match value {
        "draft" => Ok(MilestoneDefinitionLifecycle::Draft),
        "proposed" => Ok(MilestoneDefinitionLifecycle::Proposed),
        "approved" => Ok(MilestoneDefinitionLifecycle::Approved),
        "superseded" => Ok(MilestoneDefinitionLifecycle::Superseded),
        _ => Err(ApiError::internal(
            "persisted milestone definition lifecycle is invalid",
        )),
    }
}

fn milestone_lifecycle_name(value: MilestoneLifecycle) -> &'static str {
    match value {
        MilestoneLifecycle::Planned => "planned",
        MilestoneLifecycle::Active => "active",
        MilestoneLifecycle::ReadyForRelease => "ready_for_release",
        MilestoneLifecycle::Released => "released",
        MilestoneLifecycle::Cancelled => "cancelled",
    }
}

fn check_result_outcome(value: api_types::AcceptanceCheckResultStatus) -> &'static str {
    match value {
        api_types::AcceptanceCheckResultStatus::Pass => "passed",
        api_types::AcceptanceCheckResultStatus::Fail => "failed",
        api_types::AcceptanceCheckResultStatus::Pending
        | api_types::AcceptanceCheckResultStatus::Blocked
        | api_types::AcceptanceCheckResultStatus::Unavailable => "missing",
        api_types::AcceptanceCheckResultStatus::Stale => "stale",
        api_types::AcceptanceCheckResultStatus::Waived => "waived",
    }
}

fn validation_result_from_row(
    row: &sqlx::sqlite::SqliteRow,
    project_id: &str,
) -> ApiResult<ValidationResult> {
    let id: String = row.try_get("id")?;
    let check_id: String = row.try_get("check_id")?;
    if row.try_get::<String, _>("source_kind")? != "manual" {
        return Err(ApiError::internal(
            "persisted manual check has an unsupported source kind",
        ));
    }
    let outcome: String = row.try_get("outcome")?;
    let evaluated_at: String = row.try_get("created_at")?;
    let status = match outcome.as_str() {
        "passed" => api_types::AcceptanceCheckResultStatus::Pass,
        "failed" => api_types::AcceptanceCheckResultStatus::Fail,
        "stale" => api_types::AcceptanceCheckResultStatus::Stale,
        "waived" => {
            return Err(ApiError::internal(
                "persisted manual check cannot have a waived outcome",
            ));
        }
        "missing" => api_types::AcceptanceCheckResultStatus::Blocked,
        "blocked" => api_types::AcceptanceCheckResultStatus::Blocked,
        "pending" => api_types::AcceptanceCheckResultStatus::Pending,
        "unavailable" => api_types::AcceptanceCheckResultStatus::Unavailable,
        other => {
            return Err(ApiError::internal(format!(
                "persisted manual check has unknown outcome {other}"
            )));
        }
    };
    let source_manifest: serde_json::Value =
        serde_json::from_str(&row.try_get::<String, _>("source_manifest_json")?)
            .map_err(|_| ApiError::internal("persisted manual check result is invalid"))?;
    let result = source_manifest
        .get("result")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ApiError::internal("persisted manual check result is missing"))?
        .to_owned();
    let governing_value = source_manifest
        .get("governing_revision_ids")
        .cloned()
        .ok_or_else(|| {
            ApiError::internal("persisted manual check governing revisions are missing")
        })?;
    let governing_revision_ids: Vec<String> =
        serde_json::from_value(governing_value).map_err(|_| {
            ApiError::internal("persisted manual check governing revisions are invalid")
        })?;
    if governing_revision_ids.is_empty()
        || governing_revision_ids
            .iter()
            .any(|value| value.trim().is_empty())
    {
        return Err(ApiError::internal(
            "persisted manual check governing revisions are incomplete",
        ));
    }
    let persisted_charter_revision_id: Option<String> =
        row.try_get("governing_charter_revision_id")?;
    if governing_revision_ids.first().map(String::as_str)
        != persisted_charter_revision_id.as_deref()
    {
        return Err(ApiError::internal(
            "persisted manual check governing columns disagree with its manifest",
        ));
    }
    if source_manifest
        .get("check_definition_revision_id")
        .and_then(serde_json::Value::as_str)
        != Some(row.try_get::<String, _>("definition_revision_id")?.as_str())
        || source_manifest
            .get("governing_policy_revision")
            .and_then(serde_json::Value::as_str)
            .is_none_or(|value| value.trim().is_empty())
        || source_manifest
            .get("governing_policy_digest")
            .and_then(serde_json::Value::as_str)
            .is_none_or(|value| value.trim().is_empty())
    {
        return Err(ApiError::internal(
            "persisted manual check authority manifest is incomplete",
        ));
    }
    let principal = PrincipalRef {
        kind: match row.try_get::<String, _>("principal_type")?.as_str() {
            "user" => PrincipalKind::User,
            "agent" => PrincipalKind::Agent,
            "worker" => PrincipalKind::Worker,
            "reviewer" => PrincipalKind::Reviewer,
            "service" => PrincipalKind::Service,
            "system" => PrincipalKind::System,
            other => {
                return Err(ApiError::internal(format!(
                    "unknown persisted principal kind {other}"
                )));
            }
        },
        id: row.try_get("principal_id")?,
        display_name: None,
    };
    let authorization_action: String = row.try_get("authorization_action")?;
    let authorization_occurred_at: String = row.try_get("authorization_occurred_at")?;
    let authorization = AuthorizationProvenance {
        principal: principal.clone(),
        authorization_basis: row.try_get("authorization_basis")?,
        action: authorization_action,
        event_id: row.try_get("explicit_event")?,
        occurred_at: authorization_occurred_at,
    };
    if principal.kind != PrincipalKind::User
        || principal.id.trim().is_empty()
        || row.try_get::<String, _>("input_digest")?.trim().is_empty()
        || row.try_get::<i64, _>("expected_version")? <= 0
    {
        return Err(ApiError::internal(
            "persisted manual check target provenance is invalid",
        ));
    }
    if authorization.action != CHECK_RESULT_ACTION
        || !well_formed_authorization_timestamp(&authorization.occurred_at)
        || authorization.authorization_basis.trim().is_empty()
        || authorization.event_id.trim().is_empty()
    {
        return Err(ApiError::internal(
            "persisted manual check authorization provenance is invalid",
        ));
    }
    if authorization.event_id != row.try_get::<String, _>("explicit_event")? {
        return Err(ApiError::internal(
            "persisted manual check authorization event disagrees with its result",
        ));
    }
    let result_digest = canonical_digest_with_schema(
        services::MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
        &json!({
            "id": id,
            "check_id": check_id,
            "status": status,
            "result": result.clone(),
            "input_digest": row.try_get::<String, _>("input_digest")?,
            "evaluated_at": evaluated_at.clone(),
            "governing_revision_ids": governing_revision_ids.clone(),
            "authorization": authorization.clone(),
            "source_manifest": source_manifest,
        }),
    )
    .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(ValidationResult {
        id,
        project_id: project_id.to_owned(),
        check_id,
        status,
        result,
        principal,
        authorization,
        input_digest: row.try_get("input_digest")?,
        governing_revision_ids,
        expected_version: row.try_get("expected_version")?,
        event_id: row.try_get("explicit_event")?,
        evaluated_at,
        result_digest,
    })
}

fn parse_milestone_lifecycle(value: &str) -> ApiResult<MilestoneLifecycle> {
    match value {
        "planned" => Ok(MilestoneLifecycle::Planned),
        "active" => Ok(MilestoneLifecycle::Active),
        "ready_for_release" => Ok(MilestoneLifecycle::ReadyForRelease),
        "released" => Ok(MilestoneLifecycle::Released),
        "cancelled" => Ok(MilestoneLifecycle::Cancelled),
        _ => Err(ApiError::internal(
            "persisted milestone lifecycle is invalid",
        )),
    }
}

fn bounded_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(20).clamp(1, 100)
}

fn encode_cursor(primary: &str, id: &str) -> String {
    hex::encode(format!("{primary}\0{id}"))
}

fn decode_cursor(value: Option<&str>) -> ApiResult<Option<(String, String)>> {
    let Some(value) = value else { return Ok(None) };
    let bytes = hex::decode(value).map_err(|_| ApiError::bad_request("invalid cursor"))?;
    let decoded = String::from_utf8(bytes).map_err(|_| ApiError::bad_request("invalid cursor"))?;
    let (primary, id) = decoded
        .split_once('\0')
        .ok_or_else(|| ApiError::bad_request("invalid cursor"))?;
    if primary.is_empty() || id.is_empty() {
        return Err(ApiError::bad_request("invalid cursor"));
    }
    Ok(Some((primary.to_owned(), id.to_owned())))
}

fn require_idempotency_key(value: &str) -> ApiResult<()> {
    if value.trim().is_empty() {
        return Err(ApiError::bad_request(
            "mutation.idempotency_key is required",
        ));
    }
    Ok(())
}

async fn ensure_project_access(
    state: &AppState,
    project_id: &str,
    user: &AuthenticatedUser,
) -> ApiResult<db::Project> {
    let project = ProjectRepo::get_by_id(&*state.db, project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    if project.owner_id.as_deref() != Some(user.user_id.as_str())
        && ProjectMemberRepo::get_member(&*state.db, project_id, &user.user_id)
            .await?
            .is_none()
    {
        return Err(ApiError::not_found("project", project_id.to_owned()));
    }
    Ok(project)
}

fn user_principal(user_id: &str) -> PrincipalRef {
    PrincipalRef {
        kind: PrincipalKind::User,
        id: user_id.to_owned(),
        display_name: None,
    }
}

fn milestone_authorization(
    authorization: &AuthorizationProvenance,
    user_id: &str,
    action: &str,
) -> ApiResult<services::ProjectCommandAuthorization> {
    Ok(services::ProjectCommandAuthorization {
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

fn readiness_authorization(
    authorization: &AuthorizationProvenance,
    user_id: &str,
    action: &str,
) -> ApiResult<services::ProjectCommandAuthorization> {
    let mut command_authorization = milestone_authorization(authorization, user_id, action)?;
    // Readiness replay must compare the submitted authority envelope before
    // authenticating a new command.  Keep the submitted principal in the
    // digest for that comparison; `authenticated_user_id` on the command
    // still binds new REST requests to the JWT user.
    command_authorization.principal_type = match authorization.principal.kind {
        PrincipalKind::User => "user",
        PrincipalKind::Agent => "agent",
        PrincipalKind::Worker => "worker",
        PrincipalKind::Reviewer => "reviewer",
        PrincipalKind::Service => "service",
        PrincipalKind::System => "system",
    }
    .to_owned();
    command_authorization.principal_id = authorization.principal.id.clone();
    Ok(command_authorization)
}

fn validate_authorization(
    authorization: &AuthorizationProvenance,
    user_id: &str,
    expected_action: &str,
) -> ApiResult<()> {
    validate_authorization_shape(authorization, user_id, expected_action)?;
    if !valid_authorization_timestamp(&authorization.occurred_at) {
        return Err(ApiError::forbidden_with_code(
            "authorization.invalid",
            "the milestone action requires a recent authenticated user authorization event",
        ));
    }
    Ok(())
}

fn validate_authorization_shape(
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
        || !well_formed_authorization_timestamp(&authorization.occurred_at)
    {
        return Err(ApiError::forbidden_with_code(
            "authorization.invalid",
            "the milestone action requires an explicit authenticated user authorization event",
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

fn well_formed_authorization_timestamp(value: &str) -> bool {
    if value.len() > MAX_AUTHORIZATION_TIMESTAMP_LEN || value.trim() != value {
        return false;
    }
    DateTime::parse_from_rfc3339(value).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_timestamp_requires_rfc3339_and_recent_input() {
        let now = Utc::now().to_rfc3339();
        assert!(well_formed_authorization_timestamp(&now));
        assert!(valid_authorization_timestamp(&now));
        assert!(!well_formed_authorization_timestamp("not-a-timestamp"));
        assert!(!well_formed_authorization_timestamp(&format!(" {now}")));
        let old = (Utc::now() - chrono::Duration::hours(49)).to_rfc3339();
        assert!(well_formed_authorization_timestamp(&old));
        assert!(!valid_authorization_timestamp(&old));
    }
}
