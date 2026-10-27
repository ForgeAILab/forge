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
use db::{new_uuid_v4, now_rfc3339, CreateDomainEvent, DomainEventRepo};
use serde::Deserialize;
use serde_json::json;
use services::MilestoneRuntime;
use sqlx::{Sqlite, Transaction};
use uuid::Uuid;

use crate::{
    errors::{ApiError, ApiResult},
    routes::{auth::AuthenticatedUser, scoped_idempotency_key},
    state::AppState,
};

mod checks;
mod definitions;

pub use checks::{record_milestone_check, waive_milestone_check};
pub use definitions::{
    get_milestone_revision, list_milestone_revisions_with_query, save_milestone_revision,
    transition_milestone_revision,
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
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
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

pub async fn list_milestones_with_query(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Query(query): Query<MilestoneListQuery>,
) -> ApiResult<Json<ProjectMilestoneListResponse>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let limit = bounded_limit(query.limit);
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let cursor = cursor
        .map(|(sequence, id)| {
            sequence
                .parse::<i64>()
                .map(|sequence| (sequence, id))
                .map_err(|_| ApiError::bad_request("invalid cursor"))
        })
        .transpose()?;
    let rows = state
        .db
        .project_milestone_id_page(&project_id, cursor, limit)
        .await
        .map_err(ApiError::project_artifact_mutation)?;
    let has_more = rows.len() > limit as usize;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let mut items = Vec::with_capacity(rows.len().min(limit as usize));
    for row in rows.into_iter().take(limit as usize) {
        let milestone_id = row.id().map_err(ApiError::project_artifact_mutation)?;
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
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
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
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
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
    let locked = state
        .db
        .lock_milestone_version_in_tx(
            &mut tx,
            &milestone_id,
            &project_id,
            request.mutation.expected_version,
        )
        .await
        .map_err(ApiError::project_artifact_mutation)?;
    if locked != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the milestone changed before its lifecycle transition",
        ));
    }
    let current_name = state
        .db
        .milestone_lifecycle_in_tx(&mut tx, &milestone_id, &project_id)
        .await
        .map_err(ApiError::project_artifact_mutation)?;
    let current = parse_milestone_lifecycle(&current_name)?;
    services::validate_milestone_transition(current, request.lifecycle)
        .map_err(|error| ApiError::conflict_with_code("invalid_transition", error.to_string()))?;
    if request.lifecycle == MilestoneLifecycle::Cancelled {
        let primary = state
            .db
            .primary_milestone_id_in_tx(&mut tx, &project_id)
            .await
            .map_err(ApiError::project_artifact_mutation)?;
        if primary.as_deref() == Some(milestone_id.as_str()) {
            return Err(ApiError::conflict_with_code(
                "primary_milestone_required",
                "choose another active primary milestone before cancelling this milestone",
            ));
        }
    }
    let updated_at = now_rfc3339();
    state
        .db
        .transition_milestone_lifecycle_in_tx(
            &mut tx,
            db::TransitionMilestoneLifecycleInput {
                project_id: &project_id,
                milestone_id: &milestone_id,
                expected_version: request.mutation.expected_version,
                from_lifecycle: &current_name,
                lifecycle: milestone_lifecycle_name(request.lifecycle),
                updated_at: &updated_at,
            },
        )
        .await
        .map_err(|error| match error {
            db::DbError::VersionConflict => ApiError::conflict_with_code(
                "version_conflict",
                "the milestone changed before its lifecycle transition",
            ),
            error => ApiError::project_artifact_mutation(error),
        })?;
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

pub async fn evaluate_readiness(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
    Json(request): Json<EvaluateMilestoneReadinessRequest>,
) -> ApiResult<Json<ReadinessSnapshot>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
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
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
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
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
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
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
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
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
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
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
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
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
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

fn milestone_lifecycle_name(value: MilestoneLifecycle) -> &'static str {
    match value {
        MilestoneLifecycle::Planned => "planned",
        MilestoneLifecycle::Active => "active",
        MilestoneLifecycle::ReadyForRelease => "ready_for_release",
        MilestoneLifecycle::Released => "released",
        MilestoneLifecycle::Cancelled => "cancelled",
    }
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
mod tests;
