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
    ProjectCharterApproval, ProjectCharterRevision, ProjectCharterState, ProjectMode,
    SaveProjectCharterRevisionRequest,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use chrono::DateTime;
use db::{
    AgentProfileRepo, AgentRepo, ProjectAgentBindingRepo, ProjectCharterApprovalRecord,
    ProjectCharterRecord, ProjectCharterRevisionRecord, ProjectMemberRepo,
    ProjectOrchestrationRepo, ProjectRepo,
};
use services::{
    evaluate_project_charter_readiness, project_agent_policy_digest, ProjectCharterApprovalCommand,
    ProjectCharterCommandService, ProjectCharterRevisionCommand, ProjectCommandAuthorization,
    CHARTER_READINESS_POLICY_VERSION, PROJECT_OPERATING_SKILL_KEY,
};

use crate::{
    errors::{ApiError, ApiResult},
    routes::{auth::AuthenticatedUser, client_idempotency_key, scoped_idempotency_key},
    state::AppState,
};

const PROJECT_AGENT_POLICY_REVISION: &str = "forge.project-agent-policy/v1";
const REVISION_SAVE_ACTION: &str = "project_charter.revision.save";
const APPROVAL_ACTION: &str = "project_charter.approval";

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
    validate_user_authorization_envelope(
        &request.mutation.authorization,
        &user.user_id,
        REVISION_SAVE_ACTION,
    )?;
    if request.charter_id.trim().is_empty() {
        return Err(ApiError::bad_request("charter_id is required"));
    }
    if request.mutation.expected_version <= 0 {
        return Err(ApiError::bad_request(
            "mutation.expected_version must be a positive Charter version",
        ));
    }
    if request.provenance.author.kind != PrincipalKind::User
        || request.provenance.author.id != user.user_id
    {
        return Err(ApiError::forbidden_with_code(
            "authorization.invalid",
            "Project Charter revisions must identify the authenticated user as author",
        ));
    }

    let storage_idempotency_key = scoped_idempotency_key(
        "charter-revision",
        &project_id,
        &user.user_id,
        &request.mutation.idempotency_key,
    );
    let authorization = ProjectCommandAuthorization {
        principal_type: "user".to_owned(),
        principal_id: user.user_id.clone(),
        policy_result: "allowed".to_owned(),
        policy_revision: Some(PROJECT_AGENT_POLICY_REVISION.to_owned()),
        policy_digest: None,
        requested_permission: Some("save_project_charter_revision".to_owned()),
        correlation_id: storage_idempotency_key.clone(),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: request.mutation.authorization.event_id.clone(),
        authorization_basis: request.mutation.authorization.authorization_basis.clone(),
        authorization_action: request.mutation.authorization.action.clone(),
        authorization_occurred_at: request.mutation.authorization.occurred_at.clone(),
        authorization_json: serde_json::to_string(&request.mutation.authorization)
            .map_err(|error| ApiError::bad_request(error.to_string()))?,
    };
    let outcome = ProjectCharterCommandService::new(state.db.clone())
        .save_revision(
            ProjectCharterRevisionCommand {
                project_id: project_id.clone(),
                charter_id: request.charter_id,
                base_revision_id: request.base_revision_id,
                expected_digest: request.mutation.expected_digest,
                project_mode: request.project_mode.as_str().to_owned(),
                maturity: request.maturity.as_str().to_owned(),
                content: request.content,
                rendered_view: Some(request.rendered_view),
                render_version: Some(request.render_version),
                provenance: request.provenance,
                expected_charter_version: request.mutation.expected_version,
                idempotency_key: storage_idempotency_key,
                authorization,
            },
            None,
        )
        .await
        .map_err(ApiError::from)?;
    let charter =
        ProjectOrchestrationRepo::get_project_charter(&*state.db, &outcome.revision.charter_id)
            .await?
            .ok_or_else(|| {
                ApiError::not_found("project_charter", outcome.revision.charter_id.clone())
            })?;
    let mut response = api_revision(&charter, outcome.revision)?;
    response.readiness = Some(outcome.readiness);
    Ok((StatusCode::CREATED, Json(response)))
}

pub async fn approve_project_charter_revision(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, revision_id)): Path<(String, String)>,
    Json(request): Json<ApproveProjectCharterRequest>,
) -> ApiResult<(StatusCode, Json<ProjectCharterApproval>)> {
    let storage_idempotency_key = scoped_idempotency_key(
        "charter-approval",
        &project_id,
        &user.user_id,
        &request.mutation.idempotency_key,
    );
    let expected_project_version = request.expected_project_version.ok_or_else(|| {
        ApiError::bad_request("expected_project_version is required for a Project Charter approval")
    })?;
    if request.revision_id != revision_id {
        return Err(ApiError::not_found("project_charter_revision", revision_id));
    }
    validate_user_authorization_envelope(
        &request.mutation.authorization,
        &user.user_id,
        APPROVAL_ACTION,
    )?;
    let authorization = ProjectCommandAuthorization {
        principal_type: "user".to_owned(),
        principal_id: user.user_id.clone(),
        policy_result: "allowed".to_owned(),
        policy_revision: Some(PROJECT_AGENT_POLICY_REVISION.to_owned()),
        policy_digest: Some(request.selected_project_agent_policy_digest.clone()),
        requested_permission: Some("approve_project_charter".to_owned()),
        correlation_id: storage_idempotency_key.clone(),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: request.mutation.authorization.event_id.clone(),
        authorization_basis: request.mutation.authorization.authorization_basis.clone(),
        authorization_action: request.mutation.authorization.action.clone(),
        authorization_occurred_at: request.mutation.authorization.occurred_at.clone(),
        authorization_json: serde_json::to_string(&request.mutation.authorization)
            .map_err(|error| ApiError::bad_request(error.to_string()))?,
    };
    let outcome = ProjectCharterCommandService::new(state.db.clone())
        .approve(
            ProjectCharterApprovalCommand {
                project_id,
                charter_id: request.charter_id,
                revision_id: request.revision_id,
                content_digest: request.content_digest,
                rendered_digest: request.render_digest,
                expected_charter_version: request.expected_charter_version,
                expected_project_version,
                approved_project_name: request.approved_project_name,
                approved_project_slug: request.approved_project_slug,
                project_mode: request.project_mode.as_str().to_owned(),
                selected_project_agent_identity_id: request.selected_project_agent_identity_id,
                selected_project_agent_profile_revision_id: request
                    .selected_project_agent_profile_revision_id,
                selected_project_agent_operating_skill_revision: request
                    .selected_project_agent_operating_skill_revision,
                selected_project_agent_policy_digest: request.selected_project_agent_policy_digest,
                idempotency_key: storage_idempotency_key,
                authorization,
            },
            None,
        )
        .await?;
    Ok((StatusCode::CREATED, Json(api_approval(outcome.approval)?)))
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

fn validate_user_authorization_envelope(
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
    {
        return Err(ApiError::forbidden_with_code(
            "authorization.invalid",
            "the mutation requires an explicit authenticated user authorization event",
        ));
    }
    Ok(())
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
