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
