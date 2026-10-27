//! Record and waive manual milestone checks with idempotent replay.

use super::*;

async fn replay_milestone_check_result(
    state: &AppState,
    project_id: &str,
    milestone_id: &str,
    check_id: &str,
    user_id: &str,
    storage_idempotency_key: &str,
    request: &RecordMilestoneCheckRequest,
) -> ApiResult<Option<ValidationResult>> {
    let Some(existing) = state
        .db
        .milestone_manual_result_replay(storage_idempotency_key)
        .await
        .map_err(ApiError::project_artifact_mutation)?
    else {
        return Ok(None);
    };

    let existing_manifest: serde_json::Value = serde_json::from_str(
        &existing
            .source_manifest_json()
            .map_err(ApiError::project_artifact_mutation)?,
    )
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
    let existing_charter_revision_id = existing
        .governing_charter_revision_id()
        .map_err(ApiError::project_artifact_mutation)?;
    let requested_charter_revision_id = request.governing_revision_ids.first().map(String::as_str);

    let mismatch = request.check_id != check_id
        || request.mutation.authorization.principal.kind != PrincipalKind::User
        || request.mutation.authorization.principal.id != user_id
        || existing
            .project_id()
            .map_err(ApiError::project_artifact_mutation)?
            != project_id
        || existing
            .milestone_id()
            .map_err(ApiError::project_artifact_mutation)?
            != milestone_id
        || existing
            .check_id()
            .map_err(ApiError::project_artifact_mutation)?
            != check_id
        || existing
            .source_kind()
            .map_err(ApiError::project_artifact_mutation)?
            != "manual"
        || existing
            .definition_revision_id()
            .map_err(ApiError::project_artifact_mutation)?
            != request.definition_revision_id
        || existing
            .outcome()
            .map_err(ApiError::project_artifact_mutation)?
            != check_result_outcome(request.status)
        || existing
            .principal_type()
            .map_err(ApiError::project_artifact_mutation)?
            != "user"
        || existing
            .principal_id()
            .map_err(ApiError::project_artifact_mutation)?
            != user_id
        || existing
            .input_digest()
            .map_err(ApiError::project_artifact_mutation)?
            != request.input_digest
        || existing
            .expected_version()
            .map_err(ApiError::project_artifact_mutation)?
            != request.mutation.expected_version
        || existing
            .authorization_basis()
            .map_err(ApiError::project_artifact_mutation)?
            != request.mutation.authorization.authorization_basis
        || existing
            .authorization_action()
            .map_err(ApiError::project_artifact_mutation)?
            != request.mutation.authorization.action
        || existing
            .explicit_event()
            .map_err(ApiError::project_artifact_mutation)?
            != request.mutation.authorization.event_id
        || existing
            .authorization_occurred_at()
            .map_err(ApiError::project_artifact_mutation)?
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

    Ok(Some(validation_result_from_read(&existing, project_id)?))
}

pub async fn record_milestone_check(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id, check_id)): Path<(String, String, String)>,
    Json(request): Json<RecordMilestoneCheckRequest>,
) -> ApiResult<Json<ValidationResult>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    require_idempotency_key(&request.mutation.idempotency_key)?;
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
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
    let check = state
        .db
        .milestone_manual_check_target_in_tx(&mut tx, &check_id, &project_id, &milestone_id)
        .await
        .map_err(ApiError::project_artifact_mutation)?
        .ok_or_else(|| ApiError::not_found("milestone_check", check_id.clone()))?;
    let check_version: i64 = check
        .version()
        .map_err(ApiError::project_artifact_mutation)?;
    let check_definition_revision_id: String = check
        .definition_revision_id()
        .map_err(ApiError::project_artifact_mutation)?;
    let current_definition_revision_id: String = check
        .current_definition_revision_id()
        .map_err(ApiError::project_artifact_mutation)?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                "definition_required",
                "manual acceptance requires the milestone's current definition revision",
            )
        })?;
    let source_kind: String = check
        .source_kind()
        .map_err(ApiError::project_artifact_mutation)?;
    let author_id: String = check
        .author_id()
        .map_err(ApiError::project_artifact_mutation)?
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
    let governing_charter_revision_id: String = state
        .db
        .milestone_check_charter_in_tx(&mut tx, &project_id)
        .await
        .map_err(ApiError::project_artifact_mutation)?
        .ok_or_else(|| ApiError::not_found("project", &project_id))?
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
    let governing_policy_digest = state
        .db
        .milestone_check_charter_digest_in_tx(&mut tx, &project_id, &governing_charter_revision_id)
        .await
        .map_err(ApiError::project_artifact_mutation)?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ApiError::internal("governing Charter content digest is missing"))?;
    let outcome = check_result_outcome(request.status);
    let result_id = new_uuid_v4();
    let created_at = now_rfc3339();
    let input_digest = request.input_digest.clone();
    let source_manifest = serde_json::json!({
        "result": request.result.clone(),
        "governing_revision_ids": request.governing_revision_ids.clone(),
        "check_definition_revision_id": check_definition_revision_id.clone(),
        "governing_policy_revision": governing_charter_revision_id.clone(),
        "governing_policy_digest": governing_policy_digest,
    });
    state
        .db
        .record_manual_milestone_result_in_tx(
            &mut tx,
            &db::CreateProjectMilestoneCheckResult {
                id: result_id.clone(),
                project_id: project_id.clone(),
                milestone_id: milestone_id.clone(),
                check_id: check_id.clone(),
                definition_revision_id: check_definition_revision_id.clone(),
                outcome: outcome.to_owned(),
                source_kind: "manual".to_owned(),
                source_manifest_json: source_manifest.to_string(),
                input_digest: input_digest.clone(),
                governing_charter_revision_id: Some(governing_charter_revision_id.clone()),
                principal_type: "user".to_owned(),
                principal_id: user.user_id.clone(),
                authorization_basis: request.mutation.authorization.authorization_basis.clone(),
                authorization_action: request.mutation.authorization.action.clone(),
                expected_version: request.mutation.expected_version,
                explicit_event: request.mutation.authorization.event_id.clone(),
                authorization_occurred_at: request.mutation.authorization.occurred_at.clone(),
                idempotency_key: storage_idempotency_key.clone(),
                created_at: created_at.clone(),
            },
            check_version,
        )
        .await
        .map_err(|error| match error {
            db::DbError::IdempotencyConflict => ApiError::conflict_with_code(
                "idempotency_in_progress",
                "another manual check result is being committed; retry with the same key",
            ),
            db::DbError::VersionConflict => ApiError::conflict_with_code(
                "version_conflict",
                "the acceptance check changed before its result was committed",
            ),
            error => ApiError::project_artifact_mutation(error),
        })?;
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
    let decision = state
        .db
        .project_decision_command_source(waiver_id, project_id)
        .await
        .map_err(ApiError::project_artifact_mutation)?;
    let decision_matches = if let Some(decision) = decision {
        let affected: serde_json::Value = serde_json::from_str(
            &decision
                .affected_records_json()
                .map_err(ApiError::project_artifact_mutation)?,
        )
        .map_err(|_| ApiError::internal("persisted waiver decision records are invalid"))?;
        let decision_state = decision
            .state()
            .map_err(ApiError::project_artifact_mutation)?;
        matches!(
            decision_state.as_str(),
            "active" | "superseded" | "invalidated"
        ) && decision
            .decision_class()
            .map_err(ApiError::project_artifact_mutation)?
            == "waiver"
            && decision
                .rationale()
                .map_err(ApiError::project_artifact_mutation)?
                == request.reason
            && decision
                .selected_outcome()
                .map_err(ApiError::project_artifact_mutation)?
                == "waived"
            && decision
                .principal_type()
                .map_err(ApiError::project_artifact_mutation)?
                == "user"
            && decision
                .principal_id()
                .map_err(ApiError::project_artifact_mutation)?
                == user_id
            && decision
                .authority_basis()
                .map_err(ApiError::project_artifact_mutation)?
                == request.mutation.authorization.authorization_basis
            && decision
                .authorization_action()
                .map_err(ApiError::project_artifact_mutation)?
                == request.mutation.authorization.action
            && decision
                .explicit_event()
                .map_err(ApiError::project_artifact_mutation)?
                == request.mutation.authorization.event_id
            && decision
                .authorization_occurred_at()
                .map_err(ApiError::project_artifact_mutation)?
                == request.mutation.authorization.occurred_at
            && decision
                .required_charter_revision_id()
                .map_err(ApiError::project_artifact_mutation)?
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
            && affected.get("governing_policy_revision") == payload.get("governing_policy_revision")
            && affected.get("governing_policy_digest") == payload.get("governing_policy_digest")
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
        && stored_governing_policy_matches(
            &payload,
            request.governing_revision_ids.first().map(String::as_str),
        );
    if !same_request || !decision_matches {
        return Err(ApiError::conflict_with_code(
            "idempotency_conflict",
            "the waiver idempotency key was already used for a different waiver",
        ));
    }
    // Policy provenance is internal receipt metadata, not part of the response.
    if let Some(payload) = payload.as_object_mut() {
        payload.remove("governing_policy_revision");
        payload.remove("governing_policy_digest");
    }
    Ok(Some(payload))
}

pub async fn waive_milestone_check(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id, check_id)): Path<(String, String, String)>,
    Json(request): Json<WaiveMilestoneCheckRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
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
    let check_row = state
        .db
        .milestone_waiver_check_target_in_tx(
            &mut tx,
            &check_id,
            &project_id,
            &milestone_id,
            &request.definition_revision_id,
            request.mutation.expected_version,
        )
        .await
        .map_err(ApiError::project_artifact_mutation)?;
    let Some(check_row) = check_row else {
        return Err(ApiError::conflict_with_code(
            "version_conflict",
            "the acceptance check target/version is stale or cross-Project",
        ));
    };
    let author_id: String = check_row
        .author_id()
        .map_err(ApiError::project_artifact_mutation)?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ApiError::conflict_with_code(
                "definition_author_required",
                "waivers require an authored definition revision",
            )
        })?;
    let current_definition_revision_id: String = check_row
        .current_definition_revision_id()
        .map_err(ApiError::project_artifact_mutation)?
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
    let waiver_charter_revision_id: String = state
        .db
        .milestone_check_charter_in_tx(&mut tx, &project_id)
        .await
        .map_err(ApiError::project_artifact_mutation)?
        .ok_or_else(|| ApiError::not_found("project", &project_id))?
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
    let governing_policy_digest = state
        .db
        .milestone_check_charter_digest_in_tx(&mut tx, &project_id, &waiver_charter_revision_id)
        .await
        .map_err(ApiError::project_artifact_mutation)?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ApiError::internal("governing Charter content digest is missing"))?;
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
                "governing_policy_revision": waiver_charter_revision_id.clone(),
                "governing_policy_digest": governing_policy_digest.clone(),
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
    state
        .db
        .insert_milestone_waiver_decision_in_tx(
            &mut tx,
            db::InsertMilestoneWaiverDecisionInput {
                waiver_id: &waiver_id,
                project_id: &project_id,
                check_id: &check_id,
                reason: &request.reason,
                user_id: &user.user_id,
                authorization_basis: &request.mutation.authorization.authorization_basis,
                authorization_action: &request.mutation.authorization.action,
                explicit_event: &request.mutation.authorization.event_id,
                authorization_occurred_at: &request.mutation.authorization.occurred_at,
                charter_revision_id: &waiver_charter_revision_id,
                affected_records_json: &serde_json::json!({
                    "milestone_id": milestone_id,
                    "check_id": check_id,
                    "definition_revision_id": request.definition_revision_id.clone(),
                    "input_digest": request.input_digest.clone(),
                    "expected_version": request.mutation.expected_version,
                    "governing_revision_ids": request.governing_revision_ids.clone(),
                    "governing_policy_revision": waiver_charter_revision_id.clone(),
                    "governing_policy_digest": governing_policy_digest,
                })
                .to_string(),
                created_at: &created_at,
            },
        )
        .await
        .map_err(ApiError::project_artifact_mutation)?;
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

fn stored_governing_policy_matches(
    manifest: &serde_json::Value,
    charter_revision_id: Option<&str>,
) -> bool {
    match (
        manifest.get("governing_policy_revision"),
        manifest.get("governing_policy_digest"),
    ) {
        // Earlier writes omitted both fields. Their committed receipt remains authoritative.
        (None, None) => true,
        (Some(revision), Some(digest)) => {
            revision
                .as_str()
                .is_some_and(|value| !value.trim().is_empty() && Some(value) == charter_revision_id)
                && digest
                    .as_str()
                    .is_some_and(|value| !value.trim().is_empty())
        }
        _ => false,
    }
}

fn validation_result_from_read(
    row: &db::MilestoneManualResultRead,
    project_id: &str,
) -> ApiResult<ValidationResult> {
    let id: String = row.id().map_err(ApiError::project_artifact_mutation)?;
    let check_id: String = row
        .check_id()
        .map_err(ApiError::project_artifact_mutation)?;
    if row
        .source_kind()
        .map_err(ApiError::project_artifact_mutation)?
        != "manual"
    {
        return Err(ApiError::internal(
            "persisted manual check has an unsupported source kind",
        ));
    }
    let outcome: String = row.outcome().map_err(ApiError::project_artifact_mutation)?;
    let evaluated_at: String = row
        .created_at()
        .map_err(ApiError::project_artifact_mutation)?;
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
    let source_manifest: serde_json::Value = serde_json::from_str(
        &row.source_manifest_json()
            .map_err(ApiError::project_artifact_mutation)?,
    )
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
    let persisted_charter_revision_id: Option<String> = row
        .governing_charter_revision_id()
        .map_err(ApiError::project_artifact_mutation)?;
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
        != Some(
            row.definition_revision_id()
                .map_err(ApiError::project_artifact_mutation)?
                .as_str(),
        )
        || !stored_governing_policy_matches(
            &source_manifest,
            persisted_charter_revision_id.as_deref(),
        )
    {
        return Err(ApiError::internal(
            "persisted manual check authority manifest is incomplete",
        ));
    }
    let principal = PrincipalRef {
        kind: match row
            .principal_type()
            .map_err(ApiError::project_artifact_mutation)?
            .as_str()
        {
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
        id: row
            .principal_id()
            .map_err(ApiError::project_artifact_mutation)?,
        display_name: None,
    };
    let authorization_action: String = row
        .authorization_action()
        .map_err(ApiError::project_artifact_mutation)?;
    let authorization_occurred_at: String = row
        .authorization_occurred_at()
        .map_err(ApiError::project_artifact_mutation)?;
    let authorization = AuthorizationProvenance {
        principal: principal.clone(),
        authorization_basis: row
            .authorization_basis()
            .map_err(ApiError::project_artifact_mutation)?,
        action: authorization_action,
        event_id: row
            .explicit_event()
            .map_err(ApiError::project_artifact_mutation)?,
        occurred_at: authorization_occurred_at,
    };
    if principal.kind != PrincipalKind::User
        || principal.id.trim().is_empty()
        || row
            .input_digest()
            .map_err(ApiError::project_artifact_mutation)?
            .trim()
            .is_empty()
        || row
            .expected_version()
            .map_err(ApiError::project_artifact_mutation)?
            <= 0
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
    if authorization.event_id
        != row
            .explicit_event()
            .map_err(ApiError::project_artifact_mutation)?
    {
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
            "input_digest": row.input_digest().map_err(ApiError::project_artifact_mutation)?,
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
        input_digest: row
            .input_digest()
            .map_err(ApiError::project_artifact_mutation)?,
        governing_revision_ids,
        expected_version: row
            .expected_version()
            .map_err(ApiError::project_artifact_mutation)?,
        event_id: row
            .explicit_event()
            .map_err(ApiError::project_artifact_mutation)?,
        evaluated_at,
        result_digest,
    })
}
