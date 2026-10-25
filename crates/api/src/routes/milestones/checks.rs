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
