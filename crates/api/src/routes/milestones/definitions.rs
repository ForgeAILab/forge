//! Read, save, and transition milestone definition revisions.

use super::*;

pub async fn list_milestone_revisions_with_query(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
    Query(query): Query<MilestoneListQuery>,
) -> ApiResult<Json<MilestoneDefinitionRevisionListResponse>> {
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
        .map(|(revision, id)| {
            revision
                .parse::<i64>()
                .map(|revision| (revision, id))
                .map_err(|_| ApiError::bad_request("invalid cursor"))
        })
        .transpose()?;
    let rows = state
        .db
        .project_milestone_revision_id_page(&project_id, &milestone_id, cursor, limit)
        .await
        .map_err(ApiError::project_artifact_mutation)?;
    let has_more = rows.len() > limit as usize;
    let runtime = MilestoneRuntime::new(state.db.clone());
    let mut items = Vec::with_capacity(rows.len().min(limit as usize));
    for row in rows.into_iter().take(limit as usize) {
        let revision_id = row.id().map_err(ApiError::project_artifact_mutation)?;
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
            "the milestone changed before its definition transition",
        ));
    }
    let target = state
        .db
        .milestone_definition_transition_source_in_tx(
            &mut tx,
            &revision_id,
            &milestone_id,
            &project_id,
        )
        .await
        .map_err(ApiError::project_artifact_mutation)?
        .ok_or_else(|| ApiError::not_found("milestone_definition_revision", revision_id.clone()))?;
    let from_name: String = target
        .lifecycle()
        .map_err(ApiError::project_artifact_mutation)?;
    let from = parse_definition_lifecycle(&from_name)?;
    services::validate_definition_transition(from, request.lifecycle)
        .map_err(|error| ApiError::conflict_with_code("invalid_transition", error.to_string()))?;
    let acceptance_checks_json: String = target
        .acceptance_checks_json()
        .map_err(ApiError::project_artifact_mutation)?;
    let acceptance_checks: Vec<api_types::MilestoneAcceptanceCheck> =
        serde_json::from_str(&acceptance_checks_json)
            .map_err(|_| ApiError::internal("persisted milestone acceptance checks are invalid"))?;
    let evidence_requirements_json: String = target
        .evidence_requirements_json()
        .map_err(ApiError::project_artifact_mutation)?;
    let evidence_requirements: Vec<api_types::AcceptanceEvidenceRequirement> =
        serde_json::from_str(&evidence_requirements_json).map_err(|_| {
            ApiError::internal("persisted milestone evidence requirements are invalid")
        })?;
    let transitioned = state
        .db
        .transition_milestone_definition_lifecycle_in_tx(
            &mut tx,
            &revision_id,
            &milestone_id,
            &from_name,
            milestone_definition_lifecycle_name(request.lifecycle),
        )
        .await
        .map_err(ApiError::project_artifact_mutation)?;
    if transitioned != 1 {
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
            &state.db,
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
    let advanced = state
        .db
        .advance_milestone_definition_in_tx(
            &mut tx,
            &project_id,
            &milestone_id,
            &revision_id,
            request.mutation.expected_version,
            milestone_definition_lifecycle_name(request.lifecycle),
            &updated_at,
        )
        .await
        .map_err(ApiError::project_artifact_mutation)?;
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

#[allow(clippy::too_many_arguments)]
async fn materialize_check_definitions_in_tx(
    db: &db::SqliteDb,
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
        db.materialize_milestone_definition_check_in_tx(
            tx,
            db::MaterializeMilestoneCheckInput {
                project_id,
                milestone_id,
                definition_revision_id,
                check_id: &check.id,
                description: &check.description,
                required: check.required,
                source_kind,
                expected_result: &check.expected_result,
                evidence_required,
                updated_at,
            },
        )
        .await
        .map_err(|error| match error {
            db::DbError::VersionConflict => ApiError::bad_request(
                "milestone acceptance check belongs to another Project or milestone",
            ),
            error => ApiError::project_artifact_mutation(error),
        })?;
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
