//! Project-scoped Charter reads, revisions, and explicit adoption approval.
//!
//! A legacy Project is deliberately not given a synthetic Charter.  The first
//! Project route which writes a revision creates (or claims) an unapproved,
//! Project-scoped draft.  Only the explicit user approval route below may set
//! the Project's current Charter pointers.

use api_types::{
    ApproveProjectCharterRequest, AuthorizationProvenance, CharterApprovalState,
    CharterApprovalType, CharterRevisionLifecycle, PrincipalKind, PrincipalRef,
    ProductAgentSelection, ProductGenesisCharterResponse, ProductMaturity, ProjectCharter,
    ProjectCharterApproval, ProjectCharterContent, ProjectCharterRevision, ProjectCharterState,
    ProjectMode, SaveProjectCharterRevisionRequest,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use db::{
    new_uuid_v4, now_rfc3339, AgentProfileRepo, AgentRepo, CreateDomainEvent, CreateProjectCharter,
    CreateProjectCharterRevision, CreateProjectCharterRevisionAtomically, DomainEventRepo,
    ProjectAgentBindingRepo, ProjectCharterApprovalRecord, ProjectCharterRecord,
    ProjectCharterRevisionRecord, ProjectMemberRepo, ProjectOrchestrationRepo, ProjectRepo,
};
use services::{
    evaluate_project_charter_readiness, project_agent_policy_digest, render_and_digest_charter,
    semantic_revision_diff, validate_charter_approval_candidate, CHARTER_READINESS_POLICY_VERSION,
    PROJECT_CHARTER_RENDER_VERSION, PROJECT_OPERATING_SKILL_KEY,
};
use sqlx::Row;

use crate::{
    errors::{ApiError, ApiResult},
    routes::{auth::AuthenticatedUser, client_idempotency_key, scoped_idempotency_key},
    state::AppState,
};

const CHARTER_SCHEMA_VERSION: &str = "forge.project-charter/v1";
const PROJECT_AGENT_POLICY_REVISION: &str = "forge.project-agent-policy/v1";
const REVISION_SAVE_ACTION: &str = "project_charter.revision.save";
const APPROVAL_ACTION: &str = "project_charter.approval";
const MAX_AUTHORIZATION_CLOCK_SKEW_SECONDS: i64 = 48 * 60 * 60;

pub async fn get_project_charter(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<Json<ProductGenesisCharterResponse>> {
    let project = authorized_project(&state, &user.user_id, &project_id, false).await?;
    let account_id = project.owner_id.as_deref().unwrap_or(&user.user_id);
    let selected_project_agent = selected_project_agent(&state, &project.id, account_id).await?;
    let Some(charter) = project_charter_for_project(&state, &project, account_id).await? else {
        return Ok(Json(ProductGenesisCharterResponse {
            charter: None,
            revisions: Vec::new(),
            current_draft_revision: None,
            current_approved_revision: None,
            approval: None,
            selected_project_agent,
        }));
    };

    Ok(Json(
        charter_projection(&state, charter, selected_project_agent).await?,
    ))
}

pub async fn save_project_charter_revision(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<SaveProjectCharterRevisionRequest>,
) -> ApiResult<(StatusCode, Json<ProjectCharterRevision>)> {
    validate_user_authorization(
        &request.mutation.authorization,
        &user.user_id,
        REVISION_SAVE_ACTION,
    )?;
    let project = authorized_project(&state, &user.user_id, &project_id, true).await?;
    let account_id = project.owner_id.as_deref().unwrap_or(&user.user_id);
    if request.charter_id.trim().is_empty() {
        return Err(ApiError::bad_request("charter_id is required"));
    }
    if request.provenance.author.kind != PrincipalKind::User
        || request.provenance.author.id != user.user_id
    {
        return Err(ApiError::forbidden_with_code(
            "authorization.invalid",
            "Project Charter revisions must identify the authenticated user as author",
        ));
    }

    let charter =
        project_charter_for_revision_request(&state, &project, account_id, &request).await?;
    // A stale first-revision retry may return the exact committed draft, but
    // version zero is never a compatibility sentinel.  The first revision
    // has an explicit expected Charter version of one.
    if request.mutation.expected_version == 1 && charter.version > 1 {
        if let Some(draft_id) = charter.current_draft_revision_id.as_deref() {
            if let Some(draft) =
                ProjectOrchestrationRepo::get_project_charter_revision(&*state.db, draft_id).await?
            {
                let rendered = render_and_digest_charter(&request.content);
                let source_refs_json = serde_json::to_string(&request.provenance.source_refs)
                    .map_err(|error| {
                        ApiError::internal(format!("serialize Charter provenance: {error}"))
                    })?;
                let change_summary =
                    semantic_revision_diff(None, &request.content).change_summary();
                let exact_replay = draft.charter_id == charter.id
                    && draft.author_type == "user"
                    && draft.author_id.as_deref() == Some(user.user_id.as_str())
                    && draft.base_revision == 0
                    && draft.base_revision_id.is_none()
                    && request.project_mode.as_str() == charter.project_mode
                    && request.maturity.as_str() == charter.maturity
                    && draft.content_digest == rendered.content_digest
                    && draft.rendered_digest == rendered.render_digest
                    && request.render_version == rendered.render_version
                    && request.rendered_view == rendered.rendered_view
                    && draft.source_refs_json == source_refs_json
                    && draft.change_summary == change_summary;
                if exact_replay {
                    let current_charter =
                        ProjectOrchestrationRepo::get_project_charter(&*state.db, &charter.id)
                            .await?
                            .ok_or_else(|| {
                                ApiError::not_found("project_charter", charter.id.clone())
                            })?;
                    let draft_created_at = draft.created_at.clone();
                    let mut response = api_revision(&current_charter, draft)?;
                    response.readiness = Some(evaluate_project_charter_readiness(
                        &response.content,
                        request.project_mode,
                        request.maturity,
                        CHARTER_READINESS_POLICY_VERSION,
                        &draft_created_at,
                    ));
                    return Ok((StatusCode::CREATED, Json(response)));
                }
            }
        }
    }
    let effective_expected_version = request.mutation.expected_version;
    if effective_expected_version <= 0 {
        return Err(ApiError::bad_request(
            "mutation.expected_version must be a positive Charter version",
        ));
    }
    if charter.version != effective_expected_version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project Charter changed before this revision was saved",
        ));
    }

    let previous = match request.base_revision_id.as_deref() {
        Some(base_id) => {
            let revision =
                ProjectOrchestrationRepo::get_project_charter_revision(&*state.db, base_id)
                    .await?
                    .filter(|revision| revision.charter_id == charter.id)
                    .ok_or_else(|| {
                        ApiError::not_found("project_charter_revision", base_id.to_owned())
                    })?;
            Some(api_revision(&charter, revision)?)
        }
        None => None,
    };
    if let Some(expected) = request.mutation.expected_digest.as_deref() {
        let actual = previous
            .as_ref()
            .map(|revision| revision.content_digest.as_str())
            .unwrap_or_default();
        if expected != actual {
            return Err(ApiError::conflict_with_code(
                "digest_conflict",
                "the Charter base digest changed before this revision was saved",
            ));
        }
    }
    if request.render_version != PROJECT_CHARTER_RENDER_VERSION {
        return Err(ApiError::bad_request(
            "render_version must name the current server Charter renderer",
        ));
    }
    if request.project_mode.as_str() != charter.project_mode
        || request.maturity.as_str() != charter.maturity
    {
        return Err(ApiError::conflict_with_code(
            "charter_scope_conflict",
            "Project Charter mode and maturity are immutable after draft creation",
        ));
    }

    let rendered = render_and_digest_charter(&request.content);
    if request.rendered_view != rendered.rendered_view {
        return Err(ApiError::conflict_with_code(
            "render_digest_conflict",
            "rendered_view does not match the canonical server rendering",
        ));
    }
    let created_at = now_rfc3339();
    let readiness = evaluate_project_charter_readiness(
        &request.content,
        request.project_mode,
        request.maturity,
        CHARTER_READINESS_POLICY_VERSION,
        &created_at,
    );
    let diff = semantic_revision_diff(
        previous.as_ref().map(|revision| &revision.content),
        &request.content,
    );
    let revision_input = CreateProjectCharterRevision {
        id: new_uuid_v4(),
        charter_id: charter.id.clone(),
        expected_charter_version: effective_expected_version,
        project_mode: request.project_mode.as_str().to_owned(),
        maturity: request.maturity.as_str().to_owned(),
        base_revision: previous
            .as_ref()
            .map(|revision| revision.revision_number)
            .unwrap_or(0),
        base_revision_id: request.base_revision_id.clone(),
        lifecycle: "proposed".to_owned(),
        schema_version: CHARTER_SCHEMA_VERSION.to_owned(),
        render_version: rendered.render_version,
        content_json: serde_json::to_string(&request.content)
            .map_err(|error| ApiError::bad_request(error.to_string()))?,
        rendered_view: rendered.rendered_view,
        change_summary: diff.change_summary(),
        author_type: "user".to_owned(),
        author_id: Some(user.user_id.clone()),
        source_message_id: None,
        source_turn_job_id: None,
        source_refs_json: serde_json::to_string(&request.provenance.source_refs)
            .map_err(|error| ApiError::bad_request(error.to_string()))?,
        content_digest: rendered.content_digest,
        rendered_digest: rendered.render_digest,
        created_at,
    };
    let record = if project.charter_status == "legacy_unverified" {
        ProjectOrchestrationRepo::create_project_charter_revision_atomically(
            &*state.db,
            CreateProjectCharterRevisionAtomically {
                project_id: Some(project.id.clone()),
                genesis_session_id: None,
                account_id: account_id.to_owned(),
                charter: CreateProjectCharter {
                    id: charter.id.clone(),
                    account_id: account_id.to_owned(),
                    genesis_session_id: None,
                    project_mode: charter.project_mode.clone(),
                    maturity: charter.maturity.clone(),
                    created_at: charter.created_at.clone(),
                    updated_at: charter.updated_at.clone(),
                },
                revision: revision_input,
            },
        )
        .await?
    } else {
        ProjectOrchestrationRepo::create_project_charter_revision(&*state.db, revision_input)
            .await?
    };
    let current_charter = ProjectOrchestrationRepo::get_project_charter(&*state.db, &charter.id)
        .await?
        .ok_or_else(|| ApiError::not_found("project_charter", charter.id.clone()))?;
    let mut response = api_revision(&current_charter, record)?;
    response.readiness = Some(readiness);
    Ok((StatusCode::CREATED, Json(response)))
}

pub async fn approve_project_charter_revision(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, revision_id)): Path<(String, String)>,
    Json(request): Json<ApproveProjectCharterRequest>,
) -> ApiResult<(StatusCode, Json<ProjectCharterApproval>)> {
    let project = authorized_project(&state, &user.user_id, &project_id, true).await?;
    let storage_idempotency_key = scoped_idempotency_key(
        "charter-approval",
        &project_id,
        &user.user_id,
        &request.mutation.idempotency_key,
    );
    if let Some(replay) = replay_project_charter_approval(
        &state,
        &user.user_id,
        &project_id,
        &revision_id,
        &storage_idempotency_key,
        &request,
    )
    .await?
    {
        return Ok((StatusCode::CREATED, Json(replay)));
    }
    validate_user_authorization(
        &request.mutation.authorization,
        &user.user_id,
        APPROVAL_ACTION,
    )?;
    if request.revision_id != revision_id {
        return Err(ApiError::not_found("project_charter_revision", revision_id));
    }
    let expected_project_version = request.expected_project_version.ok_or_else(|| {
        ApiError::bad_request("expected_project_version is required for a Project Charter approval")
    })?;
    if expected_project_version <= 0 || expected_project_version != project.version {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed while this Charter approval was being reviewed",
        ));
    }
    let account_id = project.owner_id.as_deref().unwrap_or(&user.user_id);
    let charter = project_charter_for_project(&state, &project, account_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project_charter", request.charter_id.clone()))?;
    if request.charter_id != charter.id {
        return Err(ApiError::not_found(
            "project_charter",
            request.charter_id.clone(),
        ));
    }
    let revision_record =
        ProjectOrchestrationRepo::get_project_charter_revision(&*state.db, &request.revision_id)
            .await?
            .filter(|revision| revision.charter_id == charter.id)
            .ok_or_else(|| {
                ApiError::not_found("project_charter_revision", request.revision_id.clone())
            })?;

    let charter_api = api_charter(charter.clone())?;
    let mut revision = api_revision(&charter, revision_record)?;
    let approval_at = now_rfc3339();
    revision.readiness = Some(evaluate_project_charter_readiness(
        &revision.content,
        revision.project_mode,
        revision.maturity,
        CHARTER_READINESS_POLICY_VERSION,
        &approval_at,
    ));
    validate_charter_approval_candidate(&charter_api, &revision, &request).map_err(|error| {
        ApiError::conflict_with_code("charter_approval_conflict", error.to_string())
    })?;

    let profile = AgentProfileRepo::get_profile(
        &*state.db,
        &request.selected_project_agent_profile_revision_id,
    )
    .await?
    .filter(|profile| profile.identity_id == request.selected_project_agent_identity_id)
    .ok_or_else(|| {
        ApiError::not_found(
            "agent_profile",
            request.selected_project_agent_profile_revision_id.clone(),
        )
    })?;
    let identity = AgentRepo::get_by_id(&*state.db, &request.selected_project_agent_identity_id)
        .await?
        .filter(|identity| identity.owner_id.as_deref() == Some(account_id) && !identity.paused)
        .ok_or_else(|| {
            ApiError::not_found(
                "agent_identity",
                request.selected_project_agent_identity_id.clone(),
            )
        })?;
    let current_operating_skill_revision =
        current_project_agent_operating_skill_revision(&state).await?;
    if request.selected_project_agent_operating_skill_revision != current_operating_skill_revision {
        return Err(ApiError::conflict_with_code(
            "operating_skill_conflict",
            "the selected Project Agent operating-skill revision is stale",
        ));
    }
    let policy_digest = project_agent_policy_digest(&profile.tool_policy_json);
    if request.selected_project_agent_policy_digest != policy_digest {
        return Err(ApiError::conflict_with_code(
            "policy_digest_conflict",
            "the selected Project Agent policy digest is stale",
        ));
    }

    let approval_id = approve_existing_project_charter(
        &state,
        &project,
        &charter,
        &revision,
        &request,
        &identity.id,
        &profile.id,
        &policy_digest,
        account_id,
        &user.user_id,
        &approval_at,
        &storage_idempotency_key,
    )
    .await?;
    let approval = ProjectOrchestrationRepo::get_project_charter_approval(&*state.db, &approval_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project_charter_approval", approval_id))?;
    Ok((StatusCode::CREATED, Json(api_approval(approval)?)))
}

/// Resolve an existing Project adoption/amendment receipt before checking the
/// current request's user authorization.  A reused key is either an exact
/// replay of the immutable target and full authorization envelope or a typed
/// idempotency conflict; it must not be reclassified as a fresh 403/validation
/// failure merely because the retry's timestamp or event is malformed/stale.
async fn replay_project_charter_approval(
    state: &AppState,
    user_id: &str,
    project_id: &str,
    revision_id: &str,
    storage_idempotency_key: &str,
    request: &ApproveProjectCharterRequest,
) -> ApiResult<Option<ProjectCharterApproval>> {
    let approval_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM project_charter_approval WHERE idempotency_key = ? LIMIT 1",
    )
    .bind(storage_idempotency_key)
    .fetch_optional(state.db.pool())
    .await?;
    let Some(approval_id) = approval_id else {
        return Ok(None);
    };
    let record = ProjectOrchestrationRepo::get_project_charter_approval(&*state.db, &approval_id)
        .await?
        .ok_or_else(|| {
            ApiError::internal("persisted Charter approval idempotency row is missing")
        })?;
    let target = sqlx::query(
        "SELECT c.project_id, c.account_id, p.version
         FROM project_charter c
         LEFT JOIN project p ON p.id = c.project_id
         WHERE c.id = ?",
    )
    .bind(&record.charter_id)
    .fetch_optional(state.db.pool())
    .await?;
    let Some(target) = target else {
        return Err(ApiError::conflict_with_code(
            "idempotency_conflict",
            "the approval idempotency key was already used for another target",
        ));
    };
    let target_project_id: Option<String> = target.try_get("project_id")?;
    let target_project_version: Option<i64> = target.try_get("version")?;
    let amendment_expected_project_version = sqlx::query_scalar::<_, i64>(
        "SELECT expected_project_version
         FROM project_charter_amendment WHERE approval_id = ?
         ORDER BY created_at DESC, id DESC LIMIT 1",
    )
    .bind(&record.id)
    .fetch_optional(state.db.pool())
    .await?;
    let expected_project_version_matches = request.expected_project_version.is_some()
        && match amendment_expected_project_version {
            Some(expected) => request.expected_project_version == Some(expected),
            // Adoption has no separate amendment row.  Its approval route
            // increments the Project exactly once, so the committed version
            // identifies the optimistic version consumed by that receipt.
            None => {
                target_project_version.and_then(|version| version.checked_sub(1))
                    == request.expected_project_version
            }
        };
    let same = matches!(
        record.approval_type.as_str(),
        "adoption" | "charter_amendment"
    ) && target_project_id.as_deref() == Some(project_id)
        && record.revision_id == request.revision_id
        && request.revision_id == revision_id
        && record.content_digest == request.content_digest
        && record.rendered_digest == request.render_digest
        && record.expected_charter_version == request.expected_charter_version
        && request.mutation.expected_version == request.expected_charter_version
        && request
            .mutation
            .expected_digest
            .as_deref()
            .is_none_or(|digest| digest == record.content_digest)
        && expected_project_version_matches
        && record.approved_name.as_deref() == Some(request.approved_project_name.as_str())
        && record.approved_slug == request.approved_project_slug
        && record.approved_project_mode == request.project_mode.as_str()
        && record.selected_identity_id.as_deref()
            == Some(request.selected_project_agent_identity_id.as_str())
        && record.selected_profile_id.as_deref()
            == Some(request.selected_project_agent_profile_revision_id.as_str())
        && record.selected_operating_skill_revision_id.as_deref()
            == Some(
                request
                    .selected_project_agent_operating_skill_revision
                    .as_str(),
            )
        && record.selected_policy_revision.as_deref() == Some(PROJECT_AGENT_POLICY_REVISION)
        && record.selected_policy_digest.as_deref()
            == Some(request.selected_project_agent_policy_digest.as_str())
        && record.approving_principal_type == "user"
        && record.approving_principal_id == user_id
        && request.mutation.authorization.principal.kind == PrincipalKind::User
        && request.mutation.authorization.principal.id == user_id
        && record.authorization_basis == request.mutation.authorization.authorization_basis
        && record.authorization_action == request.mutation.authorization.action
        && record.source_action == request.mutation.authorization.action
        && record.explicit_event == request.mutation.authorization.event_id
        && record.authorization_occurred_at == request.mutation.authorization.occurred_at
        && record.approval_event_id.as_deref()
            == Some(request.mutation.authorization.event_id.as_str());
    if !same {
        return Err(ApiError::conflict_with_code(
            "idempotency_conflict",
            "the approval idempotency key was already used for another target",
        ));
    }
    Ok(Some(api_approval(record)?))
}

async fn authorized_project(
    state: &AppState,
    user_id: &str,
    project_id: &str,
    require_owner_or_admin: bool,
) -> ApiResult<db::Project> {
    let project = ProjectRepo::get_by_id(&*state.db, project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    let is_owner = project.owner_id.as_deref() == Some(user_id);
    let member = ProjectMemberRepo::get_member(&*state.db, project_id, user_id).await?;
    if !is_owner && member.is_none() {
        return Err(ApiError::not_found("project", project_id.to_owned()));
    }
    if require_owner_or_admin
        && !is_owner
        && !member
            .as_ref()
            .is_some_and(|member| matches!(member.role.as_str(), "owner" | "admin"))
    {
        return Err(ApiError::forbidden_with_code(
            "project_owner_required",
            "Project owner or admin role is required for Charter mutation",
        ));
    }
    Ok(project)
}

async fn project_charter_for_project(
    state: &AppState,
    project: &db::Project,
    account_id: &str,
) -> ApiResult<Option<ProjectCharterRecord>> {
    let charter_id = if let Some(charter_id) = project.current_charter_id.as_deref() {
        Some(charter_id.to_owned())
    } else {
        sqlx::query_scalar::<_, String>(
            "SELECT id FROM project_charter
             WHERE project_id = ? AND account_id = ?
             ORDER BY updated_at DESC, id DESC LIMIT 1",
        )
        .bind(&project.id)
        .bind(account_id)
        .fetch_optional(state.db.pool())
        .await?
    };
    let Some(charter_id) = charter_id else {
        return Ok(None);
    };
    let charter = ProjectOrchestrationRepo::get_project_charter_for_account(
        &*state.db,
        &charter_id,
        account_id,
    )
    .await?
    .ok_or_else(|| ApiError::not_found("project_charter", charter_id.clone()))?;
    if charter.project_id.as_deref() != Some(project.id.as_str()) {
        return Err(ApiError::not_found("project_charter", charter_id));
    }
    Ok(Some(charter))
}

async fn project_charter_for_revision_request(
    state: &AppState,
    project: &db::Project,
    account_id: &str,
    request: &SaveProjectCharterRevisionRequest,
) -> ApiResult<ProjectCharterRecord> {
    if let Some(current_id) = project.current_charter_id.as_deref() {
        if current_id != request.charter_id {
            return Err(ApiError::not_found(
                "project_charter",
                request.charter_id.clone(),
            ));
        }
    }
    if let Some(existing) = ProjectOrchestrationRepo::get_project_charter_for_account(
        &*state.db,
        &request.charter_id,
        account_id,
    )
    .await?
    {
        if let Some(existing_project_id) = existing.project_id.as_deref() {
            if existing_project_id != project.id {
                return Err(ApiError::not_found(
                    "project_charter",
                    request.charter_id.clone(),
                ));
            }
        } else if existing.genesis_session_id.is_some() {
            return Err(ApiError::conflict_with_code(
                "charter_scope_conflict",
                "a Genesis-owned Charter cannot be adopted through a Project route",
            ));
        }
        return Ok(existing);
    }
    if request.mutation.expected_version != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the first Project adoption Charter revision requires expected_version 1",
        ));
    }
    let now = now_rfc3339();
    Ok(ProjectCharterRecord {
        id: request.charter_id.clone(),
        account_id: account_id.to_owned(),
        genesis_session_id: None,
        project_id: Some(project.id.clone()),
        current_draft_revision_id: None,
        current_approved_revision_id: None,
        project_mode: request.project_mode.as_str().to_owned(),
        maturity: request.maturity.as_str().to_owned(),
        lifecycle: "draft".to_owned(),
        version: 1,
        created_at: now.clone(),
        updated_at: now,
    })
}

async fn selected_project_agent(
    state: &AppState,
    project_id: &str,
    account_id: &str,
) -> ApiResult<Option<ProductAgentSelection>> {
    let Some(binding) =
        ProjectAgentBindingRepo::get_active_project_binding(&*state.db, project_id).await?
    else {
        return Ok(None);
    };
    let (Some(identity_id), Some(profile_id)) = (binding.identity_id, binding.profile_id) else {
        return Ok(None);
    };
    let Some(identity) = AgentRepo::get_by_id(&*state.db, &identity_id).await? else {
        return Ok(None);
    };
    if identity.owner_id.as_deref() != Some(account_id) || identity.paused {
        return Ok(None);
    }
    let Some(profile) = AgentProfileRepo::get_profile(&*state.db, &profile_id)
        .await?
        .filter(|profile| profile.identity_id == identity.id)
    else {
        return Ok(None);
    };
    let operating_skill_revision = current_project_agent_operating_skill_revision(state).await?;
    Ok(Some(ProductAgentSelection {
        identity_id: identity.id,
        display_name: Some(identity.name),
        profile_revision_id: profile.id,
        operating_skill_revision,
        policy_digest: project_agent_policy_digest(&profile.tool_policy_json),
    }))
}

async fn charter_projection(
    state: &AppState,
    charter_record: ProjectCharterRecord,
    selected_project_agent: Option<ProductAgentSelection>,
) -> ApiResult<ProductGenesisCharterResponse> {
    let records =
        ProjectOrchestrationRepo::list_project_charter_revisions(&*state.db, &charter_record.id)
            .await?;
    let revisions = records
        .into_iter()
        .map(|record| async { api_revision(&charter_record, record) })
        .collect::<Vec<_>>();
    let mut revisions = futures_util::future::try_join_all(revisions).await?;
    for revision in &mut revisions {
        revision.readiness = Some(evaluate_project_charter_readiness(
            &revision.content,
            revision.project_mode,
            revision.maturity,
            CHARTER_READINESS_POLICY_VERSION,
            &charter_record.updated_at,
        ));
    }
    let current_draft_revision = charter_record
        .current_draft_revision_id
        .as_ref()
        .and_then(|id| revisions.iter().find(|revision| &revision.id == id))
        .cloned();
    let current_approved_revision = charter_record
        .current_approved_revision_id
        .as_ref()
        .and_then(|id| revisions.iter().find(|revision| &revision.id == id))
        .cloned();
    let approval_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM project_charter_approval
         WHERE charter_id = ? ORDER BY created_at DESC, id DESC LIMIT 1",
    )
    .bind(&charter_record.id)
    .fetch_optional(state.db.pool())
    .await?;
    let approval = match approval_id {
        Some(id) => ProjectOrchestrationRepo::get_project_charter_approval(&*state.db, &id)
            .await?
            .map(api_approval)
            .transpose()?,
        None => None,
    };
    Ok(ProductGenesisCharterResponse {
        charter: Some(api_charter(charter_record)?),
        revisions,
        current_draft_revision,
        current_approved_revision,
        approval,
        selected_project_agent,
    })
}

#[allow(clippy::too_many_arguments)]
async fn approve_existing_project_charter(
    state: &AppState,
    project: &db::Project,
    charter: &ProjectCharterRecord,
    revision: &ProjectCharterRevision,
    request: &ApproveProjectCharterRequest,
    identity_id: &str,
    profile_id: &str,
    policy_digest: &str,
    account_id: &str,
    approving_user_id: &str,
    approval_at: &str,
    storage_idempotency_key: &str,
) -> ApiResult<String> {
    let expected_project_version = request.expected_project_version.ok_or_else(|| {
        ApiError::bad_request("expected_project_version is required for a Project Charter approval")
    })?;
    let mut tx = db::begin_immediate(state.db.pool()).await?;
    let approval_id = new_uuid_v4();
    let approval_type = if project.charter_status == "charter_backed" {
        "charter_amendment"
    } else {
        "adoption"
    };

    if let Some(existing) = sqlx::query(
        "SELECT id, charter_id, revision_id, content_digest, rendered_digest,
                expected_charter_version, approved_name, approved_slug,
                approved_project_mode, selected_identity_id, selected_profile_id,
                selected_operating_skill_revision_id, selected_policy_revision,
                selected_policy_digest, approving_principal_type, approving_principal_id,
                authorization_basis, authorization_action, explicit_event,
                authorization_occurred_at, source_action, approval_event_id
         FROM project_charter_approval WHERE idempotency_key = ?",
    )
    .bind(storage_idempotency_key)
    .fetch_optional(&mut *tx)
    .await?
    {
        let same = existing.try_get::<String, _>("charter_id")? == charter.id
            && existing.try_get::<String, _>("revision_id")? == revision.id
            && existing.try_get::<String, _>("content_digest")? == revision.content_digest
            && existing.try_get::<String, _>("rendered_digest")? == revision.render_digest
            && existing.try_get::<i64, _>("expected_charter_version")?
                == request.expected_charter_version
            && existing.try_get::<Option<String>, _>("approved_name")?
                == Some(request.approved_project_name.clone())
            && existing.try_get::<Option<String>, _>("approved_slug")?
                == request.approved_project_slug
            && existing.try_get::<String, _>("approved_project_mode")?
                == request.project_mode.as_str()
            && existing.try_get::<Option<String>, _>("selected_identity_id")?
                == Some(identity_id.to_owned())
            && existing.try_get::<Option<String>, _>("selected_profile_id")?
                == Some(profile_id.to_owned())
            && existing.try_get::<Option<String>, _>("selected_operating_skill_revision_id")?
                == Some(
                    request
                        .selected_project_agent_operating_skill_revision
                        .clone(),
                )
            && existing.try_get::<Option<String>, _>("selected_policy_revision")?
                == Some(PROJECT_AGENT_POLICY_REVISION.to_owned())
            && existing.try_get::<Option<String>, _>("selected_policy_digest")?
                == Some(policy_digest.to_owned())
            && existing.try_get::<String, _>("approving_principal_type")? == "user"
            && existing.try_get::<String, _>("approving_principal_id")? == approving_user_id
            && existing.try_get::<String, _>("authorization_basis")?
                == request.mutation.authorization.authorization_basis
            && existing.try_get::<String, _>("authorization_action")?
                == request.mutation.authorization.action
            && existing.try_get::<String, _>("explicit_event")?
                == request.mutation.authorization.event_id
            && existing.try_get::<String, _>("authorization_occurred_at")?
                == request.mutation.authorization.occurred_at
            && existing.try_get::<String, _>("source_action")?
                == request.mutation.authorization.action
            && existing.try_get::<Option<String>, _>("approval_event_id")?
                == Some(request.mutation.authorization.event_id.clone());
        if !same {
            return Err(ApiError::conflict_with_code(
                "idempotency_conflict",
                "the approval idempotency key was already used for another target",
            ));
        }
        let existing_id: String = existing.try_get("id")?;
        tx.commit().await?;
        return Ok(existing_id);
    }

    let target = sqlx::query(
        "SELECT c.account_id, c.project_id, c.genesis_session_id, c.version,
                c.current_approved_revision_id, c.project_mode, c.maturity,
                r.lifecycle, r.content_digest, r.rendered_digest
         FROM project_charter c
         JOIN project_charter_revision r ON r.charter_id = c.id
         WHERE c.id = ? AND r.id = ?",
    )
    .bind(&charter.id)
    .bind(&revision.id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::not_found("project_charter_revision", revision.id.clone()))?;
    let target_genesis_session_id: Option<String> = target.try_get("genesis_session_id")?;
    if target.try_get::<String, _>("account_id")? != account_id
        || target
            .try_get::<Option<String>, _>("project_id")?
            .as_deref()
            != Some(project.id.as_str())
        || (project.charter_status != "charter_backed" && target_genesis_session_id.is_some())
    {
        return Err(ApiError::not_found(
            "project_charter_revision",
            revision.id.clone(),
        ));
    }
    if target.try_get::<i64, _>("version")? != request.expected_charter_version
        || target.try_get::<String, _>("project_mode")? != request.project_mode.as_str()
        || target.try_get::<String, _>("maturity")? != revision.maturity.as_str()
        || target.try_get::<String, _>("content_digest")? != revision.content_digest
        || target.try_get::<String, _>("rendered_digest")? != revision.render_digest
        || !matches!(
            target.try_get::<String, _>("lifecycle")?.as_str(),
            "draft" | "proposed" | "approved"
        )
    {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project Charter approval target is stale",
        ));
    }

    let selected_ok: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM agent_profile p
         JOIN agent_identity i ON i.id = p.identity_id
         JOIN operating_skill_revision s ON s.id = ?
         JOIN operating_skill os
           ON os.id = s.operating_skill_id
          AND os.current_revision_id = s.id
         WHERE p.id = ? AND p.identity_id = ? AND i.owner_id = ?
           AND i.paused = 0 AND s.skill_key = ?
           AND os.skill_key = ? AND os.lifecycle = 'active' LIMIT 1",
    )
    .bind(&request.selected_project_agent_operating_skill_revision)
    .bind(profile_id)
    .bind(identity_id)
    .bind(account_id)
    .bind(PROJECT_OPERATING_SKILL_KEY)
    .bind(PROJECT_OPERATING_SKILL_KEY)
    .fetch_optional(&mut *tx)
    .await?;
    if selected_ok.is_none() {
        return Err(ApiError::conflict_with_code(
            "agent_selection_conflict",
            "the selected Project Agent is no longer eligible",
        ));
    }

    let name_taken: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM project WHERE owner_id = ? AND name = ? AND id != ? LIMIT 1",
    )
    .bind(project.owner_id.as_deref().unwrap_or(account_id))
    .bind(&request.approved_project_name)
    .bind(&project.id)
    .fetch_optional(&mut *tx)
    .await?;
    if name_taken.is_some() {
        return Err(ApiError::conflict_with_code(
            "project_name_conflict",
            "the approved Project name is already in use",
        ));
    }

    let previous_active: Option<String> = sqlx::query_scalar(
        "SELECT id FROM project_charter_approval
         WHERE charter_id = ? AND lifecycle = 'active' LIMIT 1",
    )
    .bind(&charter.id)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(previous_id) = previous_active {
        sqlx::query(
            "UPDATE project_charter_approval
             SET lifecycle = 'revoked', version = version + 1, updated_at = ?
             WHERE id = ? AND lifecycle = 'active'",
        )
        .bind(approval_at)
        .bind(&previous_id)
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?;
        sqlx::query(
            "INSERT INTO project_charter_approval_event (
                id, approval_id, lifecycle, principal_type, principal_id,
                authorization_basis, action, explicit_event, reason,
                idempotency_key, occurred_at, created_at
             ) VALUES (?, ?, 'revoked', 'user', ?, ?, ?, 'Superseded by newer Project approval', ?, ?, ?)",
        )
        .bind(new_uuid_v4())
        .bind(&previous_id)
        .bind(approving_user_id)
        .bind(&request.mutation.authorization.authorization_basis)
        .bind(&request.mutation.authorization.action)
        .bind(&request.mutation.authorization.event_id)
        .bind(format!("{}:revoke:{}", request.mutation.idempotency_key, previous_id))
        .bind(approval_at)
        .bind(approval_at)
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?;
    }

    let previous_approved_revision_id: Option<String> =
        target.try_get("current_approved_revision_id")?;
    let previous_approved_content =
        if let Some(previous_revision_id) = previous_approved_revision_id.as_deref() {
            let content_json: String = sqlx::query_scalar(
                "SELECT content_json FROM project_charter_revision
             WHERE id = ? AND charter_id = ? AND lifecycle = 'approved'",
            )
            .bind(previous_revision_id)
            .bind(&charter.id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(db::DbError::VersionConflict)?;
            Some(
                serde_json::from_str::<ProjectCharterContent>(&content_json).map_err(|error| {
                    ApiError::internal(format!(
                        "persisted approved Charter content is invalid: {error}"
                    ))
                })?,
            )
        } else {
            None
        };
    if let Some(previous_revision_id) = previous_approved_revision_id.as_deref() {
        if previous_revision_id != revision.id {
            sqlx::query(
                "UPDATE project_charter_revision SET lifecycle = 'superseded'
                 WHERE id = ? AND charter_id = ? AND lifecycle = 'approved'",
            )
            .bind(previous_revision_id)
            .bind(&charter.id)
            .execute(&mut *tx)
            .await
            .map_err(map_write_error)?;
        }
    }
    sqlx::query("UPDATE project_charter_revision SET lifecycle = 'approved' WHERE id = ?")
        .bind(&revision.id)
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?;
    sqlx::query(
        "UPDATE project_charter
         SET current_approved_revision_id = ?, lifecycle = 'attached',
             version = version + 1, updated_at = ?
         WHERE id = ? AND version = ? AND project_id = ?",
    )
    .bind(&revision.id)
    .bind(approval_at)
    .bind(&charter.id)
    .bind(request.expected_charter_version)
    .bind(&project.id)
    .execute(&mut *tx)
    .await
    .map_err(map_write_error)
    .and_then(|result| {
        if result.rows_affected() == 1 {
            Ok(result)
        } else {
            Err(db::DbError::VersionConflict)
        }
    })?;

    sqlx::query(
        "INSERT INTO project_charter_approval (
            id, approval_type, charter_id, revision_id, content_digest,
            rendered_digest, expected_charter_version, approved_name, approved_slug,
            selected_identity_id, selected_profile_id, selected_operating_skill_revision_id,
            selected_policy_revision, selected_policy_digest, approving_principal_type,
            approving_principal_id, authorization_basis, authorization_action, explicit_event,
            authorization_occurred_at, source_action,
            lifecycle, idempotency_key, version, created_at, updated_at,
            approved_project_mode, approval_event_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   'active', ?, 1, ?, ?, ?, NULL)",
    )
    .bind(&approval_id)
    .bind(approval_type)
    .bind(&charter.id)
    .bind(&revision.id)
    .bind(&revision.content_digest)
    .bind(&revision.render_digest)
    .bind(request.expected_charter_version)
    .bind(&request.approved_project_name)
    .bind(request.approved_project_slug.as_deref())
    .bind(identity_id)
    .bind(profile_id)
    .bind(&request.selected_project_agent_operating_skill_revision)
    .bind(PROJECT_AGENT_POLICY_REVISION)
    .bind(policy_digest)
    .bind("user")
    .bind(approving_user_id)
    .bind(&request.mutation.authorization.authorization_basis)
    .bind(&request.mutation.authorization.action)
    .bind(&request.mutation.authorization.event_id)
    .bind(&request.mutation.authorization.occurred_at)
    .bind(&request.mutation.authorization.action)
    .bind(storage_idempotency_key)
    .bind(approval_at)
    .bind(approval_at)
    .bind(request.project_mode.as_str())
    .execute(&mut *tx)
    .await
    .map_err(map_write_error)?;
    sqlx::query(
        "INSERT INTO project_charter_approval_event (
            id, approval_id, lifecycle, principal_type, principal_id,
            authorization_basis, action, explicit_event, idempotency_key,
            occurred_at, created_at
         ) VALUES (?, ?, 'active', 'user', ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&request.mutation.authorization.event_id)
    .bind(&approval_id)
    .bind(approving_user_id)
    .bind(&request.mutation.authorization.authorization_basis)
    .bind(&request.mutation.authorization.action)
    .bind(&request.mutation.authorization.event_id)
    .bind(format!("{storage_idempotency_key}:active"))
    .bind(&request.mutation.authorization.occurred_at)
    .bind(approval_at)
    .execute(&mut *tx)
    .await
    .map_err(map_write_error)?;
    // The approval receipt references the immutable active event, but the
    // event itself references the receipt.  Insert the nullable receipt row
    // first, then close the FK cycle only after the event exists.
    let linked = sqlx::query(
        "UPDATE project_charter_approval
         SET approval_event_id = ?
         WHERE id = ? AND lifecycle = 'active' AND approval_event_id IS NULL",
    )
    .bind(&request.mutation.authorization.event_id)
    .bind(&approval_id)
    .execute(&mut *tx)
    .await
    .map_err(map_write_error)?;
    if linked.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Charter approval receipt could not be linked to its active event",
        ));
    }

    if project.charter_status == "charter_backed" {
        let Some(base_revision_id) = previous_approved_revision_id.as_deref() else {
            return Err(ApiError::conflict_with_code(
                "version_conflict",
                "a Charter amendment requires the current approved revision",
            ));
        };
        let diff = semantic_revision_diff(previous_approved_content.as_ref(), &revision.content);
        let material_diff_json = serde_json::json!({
            "schema_version": diff.schema_version,
            "changed_sections": diff.changed_sections,
            "changes": diff.changes.iter().map(|change| serde_json::json!({
                "section": change.section,
                "field": change.field,
                "before": change.before,
                "after": change.after,
            })).collect::<Vec<_>>(),
        })
        .to_string();
        let affected_records_json = serde_json::json!({
            "project_id": project.id,
            "reconciliation_required": if diff.is_empty() { Vec::<&str>::new() } else {
                vec!["documents", "decisions", "tasks", "baselines", "milestones", "validations", "releases"]
            },
            "governing_charter_revision_id": revision.id,
        })
        .to_string();
        sqlx::query(
            "INSERT INTO project_charter_amendment (
                id, project_id, base_charter_revision_id, candidate_revision_id,
                lifecycle, rationale, material_diff_json, affected_records_json,
                requested_principal_type, requested_principal_id,
                expected_project_version, approval_id, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'approved', ?, ?, ?, 'user', ?, ?, ?, 1, ?, ?)",
        )
        .bind(new_uuid_v4())
        .bind(&project.id)
        .bind(base_revision_id)
        .bind(&revision.id)
        .bind(diff.change_summary())
        .bind(material_diff_json)
        .bind(affected_records_json)
        .bind(approving_user_id)
        .bind(expected_project_version)
        .bind(&approval_id)
        .bind(approval_at)
        .bind(approval_at)
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?;
    }

    let current_binding = sqlx::query(
        "SELECT id, state, version, autonomy_policy_json, permission_ceiling_json,
                subscriptions_json, wake_budget
         FROM project_agent_binding
         WHERE project_id = ? AND state IN ('active', 'agent_setup_required')
         ORDER BY created_at DESC, id DESC LIMIT 1",
    )
    .bind(&project.id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(db::DbError::NotFound)?;
    let binding_id: String = current_binding.try_get("id")?;
    let binding_state: String = current_binding.try_get("state")?;
    let binding_version: i64 = current_binding.try_get("version")?;
    if binding_state == "agent_setup_required" {
        let updated = sqlx::query(
            "UPDATE project_agent_binding
             SET identity_id = ?, profile_id = ?, state = 'active',
                 operating_skill_revision_id = ?, policy_revision = ?, policy_digest = ?,
                 charter_id = ?, charter_revision_id = ?, charter_setup_required = 0,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND state = 'agent_setup_required' AND version = ?",
        )
        .bind(identity_id)
        .bind(profile_id)
        .bind(&request.selected_project_agent_operating_skill_revision)
        .bind(PROJECT_AGENT_POLICY_REVISION)
        .bind(policy_digest)
        .bind(&charter.id)
        .bind(&revision.id)
        .bind(approval_at)
        .bind(&binding_id)
        .bind(binding_version)
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?;
        if updated.rows_affected() != 1 {
            return Err(ApiError::conflict_with_code(
                "version_conflict",
                "the Project Agent setup changed before Charter approval committed",
            ));
        }
    } else {
        // An active binding is immutable identity/profile history.  Approval
        // creates a replacement row and links the old row in the same
        // transaction, so a concurrent turn can retain its exact provenance.
        let replaced = sqlx::query(
            "UPDATE project_agent_binding
             SET state = 'replaced', replacement_reason = ?, version = version + 1,
                 updated_at = ?
             WHERE id = ? AND project_id = ? AND state = 'active' AND version = ?",
        )
        .bind(if project.charter_status == "charter_backed" {
            "Project Charter amendment"
        } else {
            "Project Charter adoption"
        })
        .bind(approval_at)
        .bind(&binding_id)
        .bind(&project.id)
        .bind(binding_version)
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?;
        if replaced.rows_affected() != 1 {
            return Err(ApiError::conflict_with_code(
                "version_conflict",
                "the active Project Agent binding changed before Charter approval committed",
            ));
        }
        let replacement_id = new_uuid_v4();
        sqlx::query(
            "INSERT INTO project_agent_binding (
                id, project_id, identity_id, profile_id, state,
                autonomy_policy_json, permission_ceiling_json, subscriptions_json,
                wake_budget, version, replaced_by_binding_id, replacement_reason,
                operating_skill_revision_id, policy_revision, policy_digest,
                charter_id, charter_revision_id, charter_setup_required,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, 0, ?, ?)",
        )
        .bind(&replacement_id)
        .bind(&project.id)
        .bind(identity_id)
        .bind(profile_id)
        .bind(current_binding.try_get::<String, _>("autonomy_policy_json")?)
        .bind(current_binding.try_get::<String, _>("permission_ceiling_json")?)
        .bind(current_binding.try_get::<String, _>("subscriptions_json")?)
        .bind(current_binding.try_get::<i64, _>("wake_budget")?)
        .bind(binding_version)
        .bind(&request.selected_project_agent_operating_skill_revision)
        .bind(PROJECT_AGENT_POLICY_REVISION)
        .bind(policy_digest)
        .bind(&charter.id)
        .bind(&revision.id)
        .bind(approval_at)
        .bind(approval_at)
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?;
        let linked = sqlx::query(
            "UPDATE project_agent_binding SET replaced_by_binding_id = ?
             WHERE id = ? AND state = 'replaced'",
        )
        .bind(&replacement_id)
        .bind(&binding_id)
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?;
        if linked.rows_affected() != 1 {
            return Err(ApiError::conflict_with_code(
                "version_conflict",
                "the replaced Project Agent binding could not be linked",
            ));
        }
    }
    sqlx::query(
        "UPDATE agent_chat SET status = 'ready', version = version + 1, updated_at = ?
         WHERE kind = 'project' AND project_id = ? AND status = 'agent_setup_required'",
    )
    .bind(approval_at)
    .bind(&project.id)
    .execute(&mut *tx)
    .await
    .map_err(map_write_error)?;

    let project_update = if project.charter_status == "legacy_unverified" {
        sqlx::query(
            "UPDATE project
             SET name = ?, charter_status = 'charter_backed', charter_setup_required = 0,
                 current_charter_id = ?, current_charter_revision_id = ?,
                 current_charter_version = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND charter_status = 'legacy_unverified'
               AND charter_setup_required = 1
               AND current_charter_id IS NULL AND current_charter_revision_id IS NULL",
        )
        .bind(&request.approved_project_name)
        .bind(&charter.id)
        .bind(&revision.id)
        .bind(request.expected_charter_version + 1)
        .bind(approval_at)
        .bind(&project.id)
        .bind(expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?
    } else if project.charter_status == "charter_backed" {
        sqlx::query(
            "UPDATE project
             SET current_charter_id = ?, current_charter_revision_id = ?,
                 current_charter_version = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND charter_status = 'charter_backed'
               AND current_charter_id = ? AND current_charter_revision_id = ?",
        )
        .bind(&charter.id)
        .bind(&revision.id)
        .bind(request.expected_charter_version + 1)
        .bind(approval_at)
        .bind(&project.id)
        .bind(expected_project_version)
        .bind(&charter.id)
        .bind(previous_approved_revision_id.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?
    } else {
        return Err(ApiError::conflict_with_code(
            "charter_scope_conflict",
            "the Project is not in an adoptable Charter state",
        ));
    };
    if project_update.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Project changed before Charter approval committed",
        ));
    }

    if project.charter_status == "charter_backed" {
        // A current Charter amendment invalidates every active execution
        // baseline bound to the old Charter revision.  Keep this derived
        // projection in the same transaction as the Charter CAS: running
        // executions are allowed to finish under their already-issued lease,
        // but no new claim/launch can use the superseded baseline.
        sqlx::query(
            "UPDATE project_execution_baseline
             SET lifecycle = 'superseded', version = version + 1, updated_at = ?
             WHERE project_id = ? AND lifecycle = 'active'
               AND EXISTS (
                   SELECT 1
                   FROM project_execution_baseline_revision r
                   WHERE r.id = project_execution_baseline.current_revision_id
                     AND r.baseline_id = project_execution_baseline.id
                     AND r.charter_revision_id != ?
               )",
        )
        .bind(approval_at)
        .bind(&project.id)
        .bind(&revision.id)
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?;
        sqlx::query(
            "UPDATE project_task_governance
             SET runnable = 0, version = version + 1, updated_at = ?
             WHERE project_id = ? AND runnable = 1
               AND baseline_id IN (
                   SELECT id FROM project_execution_baseline
                   WHERE project_id = ? AND lifecycle = 'superseded'
               )",
        )
        .bind(approval_at)
        .bind(&project.id)
        .bind(&project.id)
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?;
    }

    if project.charter_status == "legacy_unverified" {
        let project_chat_id: String = sqlx::query_scalar(
            "SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = ?",
        )
        .bind(&project.id)
        .fetch_one(&mut *tx)
        .await?;
        let sequence: i64 = sqlx::query_scalar(
            "UPDATE agent_chat
             SET message_count = message_count + 1, last_message_at = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? RETURNING message_count - 1",
        )
        .bind(approval_at)
        .bind(approval_at)
        .bind(&project_chat_id)
        .fetch_one(&mut *tx)
        .await?;
        let bootstrap_content = format!(
            "Project Charter adoption approved for Project {}. Charter {} revision {} is now authoritative.",
            project.id, charter.id, revision.id
        );
        sqlx::query(
            "INSERT INTO agent_chat_message (
                id, chat_id, sequence, author_type, author_id, content,
                content_guard_json, sensitivity, status, correlation_id,
                source_type, source_id, source_metadata_json, created_at
             ) VALUES (?, ?, ?, 'system', ?, ?, ?, 'internal', 'complete', ?,
                       'native', ?, ?, ?)",
        )
        .bind(new_uuid_v4())
        .bind(&project_chat_id)
        .bind(sequence)
        .bind(approving_user_id)
        .bind(bootstrap_content)
        .bind(
            serde_json::json!({
                "schema_version": "forge.project-charter-adoption/v1",
                "authority": "data_only",
                "project_id": project.id,
                "charter_id": charter.id,
                "revision_id": revision.id,
                "approval_id": approval_id,
                "content_digest": revision.content_digest,
                "render_digest": revision.render_digest,
                "explicit_event": request.mutation.authorization.event_id,
            })
            .to_string(),
        )
        .bind(&request.mutation.idempotency_key)
        .bind(&approval_id)
        .bind(
            serde_json::json!({
                "kind": "project_charter_adoption",
                "approval_id": approval_id,
                "charter_id": charter.id,
                "revision_id": revision.id,
            })
            .to_string(),
        )
        .bind(approval_at)
        .execute(&mut *tx)
        .await
        .map_err(map_write_error)?;
    }

    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.charter.approved".to_owned(),
        entity_type: "project_charter".to_owned(),
        entity_id: charter.id.clone(),
        actor_type: "user".to_owned(),
        actor_id: Some(approving_user_id.to_owned()),
        scope_type: "project".to_owned(),
        scope_id: project.id.clone(),
        correlation_id: request.mutation.idempotency_key.clone(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: Some(format!(
            "project-charter-approval:{}",
            request.mutation.idempotency_key
        )),
        payload_json: serde_json::json!({
            "project_id": project.id,
            "charter_id": charter.id,
            "revision_id": revision.id,
            "approval_id": approval_id,
            "approval_type": approval_type,
            "content_digest": revision.content_digest,
            "render_digest": revision.render_digest,
        })
        .to_string(),
        created_at: approval_at.to_owned(),
    };
    DomainEventRepo::append_event_in_tx(&*state.db, &mut tx, &event).await?;
    let consumed = sqlx::query(
        "UPDATE project_charter_approval
         SET lifecycle = 'consumed', consumed_project_id = ?, consumed_at = ?,
             version = version + 1, updated_at = ?
         WHERE id = ? AND lifecycle = 'active'",
    )
    .bind(&project.id)
    .bind(approval_at)
    .bind(approval_at)
    .bind(&approval_id)
    .execute(&mut *tx)
    .await
    .map_err(map_write_error)?;
    if consumed.rows_affected() != 1 {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the Charter approval receipt was consumed concurrently",
        ));
    }
    sqlx::query(
        "INSERT INTO project_charter_approval_event (
            id, approval_id, lifecycle, principal_type, principal_id,
            authorization_basis, action, explicit_event, reason,
            idempotency_key, occurred_at, created_at
         ) VALUES (?, ?, 'consumed', 'user', ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&approval_id)
    .bind(approving_user_id)
    .bind(&request.mutation.authorization.authorization_basis)
    .bind(&request.mutation.authorization.action)
    .bind(&request.mutation.authorization.event_id)
    .bind(if project.charter_status == "legacy_unverified" {
        "Project Charter adoption applied"
    } else {
        "Project Charter amendment applied"
    })
    .bind(format!("{}:consumed", request.mutation.idempotency_key))
    .bind(approval_at)
    .bind(approval_at)
    .execute(&mut *tx)
    .await
    .map_err(map_write_error)?;
    tx.commit().await?;
    Ok(approval_id)
}

fn api_charter(record: ProjectCharterRecord) -> ApiResult<ProjectCharter> {
    parse_charter_lifecycle(&record.lifecycle)?;
    Ok(ProjectCharter {
        id: record.id,
        genesis_session_id: record.genesis_session_id,
        project_id: record.project_id,
        state: if record.current_approved_revision_id.is_some() {
            ProjectCharterState::Approved
        } else {
            ProjectCharterState::CharterSetupRequired
        },
        project_mode: parse_project_mode(&record.project_mode)?,
        maturity: parse_maturity(&record.maturity)?,
        current_draft_revision_id: record.current_draft_revision_id,
        current_approved_revision_id: record.current_approved_revision_id,
        version: record.version,
        created_at: record.created_at,
        updated_at: record.updated_at,
    })
}

fn api_revision(
    charter: &ProjectCharterRecord,
    record: ProjectCharterRevisionRecord,
) -> ApiResult<ProjectCharterRevision> {
    let author_id = required_persisted("Charter revision author id", record.author_id)?;
    let source_refs = serde_json::from_str(&record.source_refs_json).map_err(|error| {
        ApiError::internal(format!("persisted Charter provenance is invalid: {error}"))
    })?;
    Ok(ProjectCharterRevision {
        id: record.id,
        charter_id: record.charter_id,
        revision_number: record.revision,
        base_revision_id: record.base_revision_id,
        lifecycle: parse_revision_lifecycle(&record.lifecycle)?,
        project_mode: parse_project_mode(&charter.project_mode)?,
        maturity: parse_maturity(&charter.maturity)?,
        schema_version: record.schema_version,
        content: serde_json::from_str(&record.content_json)
            .map_err(|error| ApiError::internal(error.to_string()))?,
        rendered_view: record.rendered_view,
        render_version: record.render_version,
        content_digest: record.content_digest,
        render_digest: record.rendered_digest,
        provenance: api_types::RevisionProvenance {
            author: PrincipalRef {
                kind: parse_principal_kind(&record.author_type)?,
                id: author_id,
                display_name: None,
            },
            profile_revision: None,
            operating_skill_revision: None,
            source_refs,
            change_summary: record.change_summary,
            material_diff: None,
        },
        readiness: None,
        approved_at: None,
        superseded_by_revision_id: None,
        created_at: record.created_at,
    })
}

fn api_approval(record: ProjectCharterApprovalRecord) -> ApiResult<ProjectCharterApproval> {
    let approved_name = required_persisted("Charter approval project name", record.approved_name)?;
    let selected_identity_id = required_persisted(
        "Charter approval Project Agent identity",
        record.selected_identity_id,
    )?;
    let selected_profile_id = required_persisted(
        "Charter approval Project Agent profile",
        record.selected_profile_id,
    )?;
    let selected_operating_skill_revision = required_persisted(
        "Charter approval operating-skill revision",
        record.selected_operating_skill_revision_id,
    )?;
    let selected_policy_revision = required_persisted(
        "Charter approval policy revision",
        record.selected_policy_revision,
    )?;
    if selected_policy_revision != PROJECT_AGENT_POLICY_REVISION {
        return Err(ApiError::internal(
            "persisted Charter approval policy revision is not the server contract",
        ));
    }
    let selected_policy_digest = required_persisted(
        "Charter approval policy digest",
        record.selected_policy_digest,
    )?;
    let approving_principal_id = required_text(
        "Charter approval principal id",
        record.approving_principal_id,
    )?;
    let authorization_basis = required_text(
        "Charter approval authorization basis",
        record.authorization_basis,
    )?;
    let authorization_action = required_text(
        "Charter approval authorization action",
        record.authorization_action,
    )?;
    let source_action = required_text("Charter approval source action", record.source_action)?;
    if source_action != authorization_action {
        return Err(ApiError::internal(
            "persisted Charter approval authorization and source actions differ",
        ));
    }
    let explicit_event = required_text("Charter approval explicit event", record.explicit_event)?;
    let occurred_at = required_text(
        "Charter approval authorization timestamp",
        record.authorization_occurred_at,
    )?;
    if DateTime::parse_from_rfc3339(&occurred_at).is_err() {
        return Err(ApiError::internal(
            "persisted Charter approval authorization timestamp is invalid",
        ));
    }
    let approval_event_id =
        required_persisted("Charter approval event id", record.approval_event_id)?;
    let idempotency_key = client_idempotency_key(&required_text(
        "Charter approval idempotency key",
        record.idempotency_key,
    )?);
    let approving_kind = parse_principal_kind(&record.approving_principal_type)?;
    Ok(ProjectCharterApproval {
        id: record.id,
        approval_type: match record.approval_type.as_str() {
            "project_creation" => CharterApprovalType::ProjectCreation,
            "charter_amendment" => CharterApprovalType::CharterAmendment,
            "adoption" => CharterApprovalType::Adoption,
            value => {
                return Err(ApiError::internal(format!(
                    "unknown approval type: {value}"
                )));
            }
        },
        charter_id: record.charter_id,
        charter_revision_id: record.revision_id,
        charter_content_digest: record.content_digest,
        charter_render_digest: record.rendered_digest,
        expected_charter_version: record.expected_charter_version,
        approved_project_name: approved_name,
        approved_project_slug: record.approved_slug,
        approved_project_mode: parse_project_mode(&record.approved_project_mode)?,
        selected_project_agent_identity_id: selected_identity_id,
        selected_project_agent_profile_revision_id: selected_profile_id,
        selected_project_agent_operating_skill_revision: selected_operating_skill_revision,
        selected_project_agent_policy_digest: selected_policy_digest,
        approved_by: PrincipalRef {
            kind: approving_kind,
            id: approving_principal_id.clone(),
            display_name: None,
        },
        authorization: AuthorizationProvenance {
            principal: PrincipalRef {
                kind: approving_kind,
                id: approving_principal_id,
                display_name: None,
            },
            authorization_basis,
            action: source_action,
            event_id: explicit_event,
            occurred_at,
        },
        approval_event_id,
        approved_at: record.created_at,
        state: match record.lifecycle.as_str() {
            "active" => CharterApprovalState::Active,
            "consumed" => CharterApprovalState::Consumed,
            "revoked" => CharterApprovalState::Revoked,
            value => {
                return Err(ApiError::internal(format!(
                    "unknown approval state: {value}"
                )));
            }
        },
        consumed_by_project_id: record.consumed_project_id,
        idempotency_key,
    })
}

fn validate_user_authorization(
    authorization: &AuthorizationProvenance,
    user_id: &str,
    expected_action: &str,
) -> ApiResult<()> {
    if authorization.principal.kind != PrincipalKind::User
        || authorization.principal.id != user_id
        || authorization.action != expected_action
        || authorization.event_id.trim().is_empty()
        || authorization.authorization_basis.trim().is_empty()
        || authorization.occurred_at.trim().is_empty()
        || !valid_authorization_timestamp(&authorization.occurred_at)
    {
        return Err(ApiError::forbidden_with_code(
            "authorization.invalid",
            "the mutation requires an explicit authenticated user authorization event",
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

fn parse_project_mode(value: &str) -> ApiResult<ProjectMode> {
    match value {
        "compact" => Ok(ProjectMode::Compact),
        "standard" => Ok(ProjectMode::Standard),
        value => Err(ApiError::internal(format!("unknown Project mode: {value}"))),
    }
}

fn parse_maturity(value: &str) -> ApiResult<ProductMaturity> {
    match value {
        "prototype" => Ok(ProductMaturity::Prototype),
        "mvp" => Ok(ProductMaturity::Mvp),
        "production" => Ok(ProductMaturity::Production),
        "critical" => Ok(ProductMaturity::Critical),
        value => Err(ApiError::internal(format!("unknown maturity: {value}"))),
    }
}

fn parse_revision_lifecycle(value: &str) -> ApiResult<CharterRevisionLifecycle> {
    match value {
        "draft" => Ok(CharterRevisionLifecycle::Draft),
        "proposed" => Ok(CharterRevisionLifecycle::Proposed),
        "approved" => Ok(CharterRevisionLifecycle::Approved),
        "rejected" => Ok(CharterRevisionLifecycle::Rejected),
        "withdrawn" => Ok(CharterRevisionLifecycle::Withdrawn),
        "superseded" => Ok(CharterRevisionLifecycle::Superseded),
        value => Err(ApiError::internal(format!(
            "unknown revision lifecycle: {value}"
        ))),
    }
}

fn parse_charter_lifecycle(value: &str) -> ApiResult<()> {
    match value {
        "draft" | "ready_for_approval" | "attached" | "superseded" | "cancelled" => Ok(()),
        value => Err(ApiError::internal(format!(
            "unknown Charter lifecycle: {value}"
        ))),
    }
}

fn parse_principal_kind(value: &str) -> ApiResult<PrincipalKind> {
    match value {
        "user" => Ok(PrincipalKind::User),
        "agent" => Ok(PrincipalKind::Agent),
        "worker" => Ok(PrincipalKind::Worker),
        "reviewer" => Ok(PrincipalKind::Reviewer),
        "service" => Ok(PrincipalKind::Service),
        value => Err(ApiError::internal(format!(
            "unknown principal kind: {value}"
        ))),
    }
}

fn required_persisted(field: &'static str, value: Option<String>) -> ApiResult<String> {
    let value = value.ok_or_else(|| ApiError::internal(format!("persisted {field} is missing")))?;
    if value.trim().is_empty() {
        return Err(ApiError::internal(format!("persisted {field} is empty")));
    }
    Ok(value)
}

fn required_text(field: &'static str, value: String) -> ApiResult<String> {
    if value.trim().is_empty() {
        return Err(ApiError::internal(format!("persisted {field} is empty")));
    }
    Ok(value)
}

async fn current_project_agent_operating_skill_revision(state: &AppState) -> ApiResult<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT revision.id
         FROM operating_skill AS skill
         JOIN operating_skill_revision AS revision
           ON revision.id = skill.current_revision_id
          AND revision.operating_skill_id = skill.id
          AND revision.skill_key = skill.skill_key
         WHERE skill.skill_key = ?
           AND skill.lifecycle = 'active'
           AND skill.current_revision_id IS NOT NULL
         LIMIT 1",
    )
    .bind(PROJECT_OPERATING_SKILL_KEY)
    .fetch_optional(state.db.pool())
    .await?
    .ok_or_else(|| {
        ApiError::conflict_with_code(
            "operating_skill_conflict",
            "the Project Agent operating skill has no current active revision",
        )
    })
}

fn map_write_error(error: sqlx::Error) -> db::DbError {
    if error.to_string().contains("UNIQUE") {
        db::DbError::VersionConflict
    } else {
        db::DbError::from(error)
    }
}
