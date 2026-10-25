//! Agent Chat admission validation and provenance digests.

use super::*;

pub(super) fn turn_admission_semantics_match(
    input: &CreateAgentChatTurnJob,
    existing: &AgentChatTurnJob,
) -> bool {
    match (&input.admission_digest, &existing.admission_digest) {
        (Some(expected), Some(actual)) => {
            expected == actual
                && input.responder_binding_id == existing.responder_binding_id
                && input.responder_binding_version == existing.responder_binding_version
                && input.responder_identity_version == existing.responder_identity_version
                && input.profile_version == existing.profile_version
                && input.operating_skill_revision_id == existing.operating_skill_revision_id
                && input.policy_revision == existing.policy_revision
                && input.policy_digest == existing.policy_digest
                && input.permission_policy_digest == existing.permission_policy_digest
                && input.tool_policy_digest == existing.tool_policy_digest
                && input.canonical_scope_provenance_json == existing.canonical_scope_provenance_json
        }
        // Legacy rows predate the admission digest.  Retain their old
        // dedupe behavior, but never treat a newly prepared admission as an
        // authenticated replay of one of those rows.
        (None, None) => {
            input.chat_id == existing.chat_id
                && input.responder_identity_id
                    == existing
                        .responder_identity_id
                        .as_deref()
                        .unwrap_or_default()
                && input.profile_id == existing.profile_id.as_deref().unwrap_or_default()
                && input.canonical_scope_type == existing.canonical_scope_type
                && input.canonical_scope_id == existing.canonical_scope_id
                && input.dedupe_key == existing.dedupe_key
                && input.causation_id == existing.causation_id
                && input.causation_depth == existing.causation_depth
        }
        _ => false,
    }
}

/// Revalidate the resolver's expected-version contract while the admission
/// transaction holds SQLite's IMMEDIATE write lock.  This deliberately does
/// not require the historical binding to remain active after admission; it
/// only prevents a resolver snapshot from straddling a concurrent replacement
/// or Profile selection edit.
pub(super) async fn validate_agent_chat_turn_admission(
    transaction: &mut Transaction<'_, Sqlite>,
    turn: &CreateAgentChatTurnJob,
) -> Result<()> {
    let frozen = [
        turn.responder_binding_id.is_some(),
        turn.responder_binding_version.is_some(),
        turn.responder_identity_version.is_some(),
        turn.profile_version.is_some(),
        turn.operating_skill_revision_id.is_some(),
        turn.policy_revision.is_some(),
        turn.policy_digest.is_some(),
        turn.permission_policy_digest.is_some(),
        turn.tool_policy_digest.is_some(),
        turn.admission_digest.is_some(),
        turn.canonical_scope_provenance_json.is_some(),
    ];
    if frozen.iter().all(|present| !present) {
        // Pre-V088 producers remain processable through the conservative
        // legacy worker path.  New producers must populate the complete set.
        return Ok(());
    }
    if frozen.iter().any(|present| !present)
        || turn.canonical_scope_type != "agent_chat"
        || turn.canonical_scope_id != turn.chat_id
    {
        return Err(DbError::Check(
            "agent turn admission provenance is incomplete or out of scope".to_owned(),
        ));
    }

    let chat = sqlx::query(
        "SELECT kind, account_id, project_id, status
         FROM agent_chat WHERE id = ?",
    )
    .bind(&turn.chat_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    let kind: String = chat.try_get("kind")?;
    let account_id: Option<String> = chat.try_get("account_id")?;
    let project_id: Option<String> = chat.try_get("project_id")?;
    let status: String = chat.try_get("status")?;
    if status != "ready" {
        return Err(DbError::VersionConflict);
    }

    let (
        binding_identity_id,
        binding_policy_revision,
        binding_policy_digest,
        binding_permission_json,
        binding_skill_revision,
    ) = match kind.as_str() {
        "account_main" => {
            let account_id = account_id
                .ok_or_else(|| DbError::Check("Main Chat has no account scope".to_owned()))?;
            let row = sqlx::query(
                "SELECT identity_id, version, state, autonomy_policy_json,
                        tool_policy_revision
                 FROM account_main_agent_binding
                 WHERE id = ? AND account_id = ?",
            )
            .bind(turn.responder_binding_id.as_deref())
            .bind(account_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(DbError::VersionConflict)?;
            let state: String = row.try_get("state")?;
            let version: i64 = row.try_get("version")?;
            if state != "active" || Some(version) != turn.responder_binding_version {
                return Err(DbError::VersionConflict);
            }
            let autonomy_policy_json: String = row.try_get("autonomy_policy_json")?;
            let tool_policy_revision: String = row.try_get("tool_policy_revision")?;
            (
                row.try_get("identity_id")?,
                tool_policy_revision,
                admission_policy_digest(&autonomy_policy_json)?,
                None,
                None,
            )
        }
        "project" => {
            let project_id = project_id
                .ok_or_else(|| DbError::Check("Project Chat has no project scope".to_owned()))?;
            let row = sqlx::query(
                "SELECT identity_id, version, state, permission_ceiling_json,
                        operating_skill_revision_id, policy_revision, policy_digest
                 FROM project_agent_binding
                 WHERE id = ? AND project_id = ?",
            )
            .bind(turn.responder_binding_id.as_deref())
            .bind(project_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(DbError::VersionConflict)?;
            let state: String = row.try_get("state")?;
            let version: i64 = row.try_get("version")?;
            if state != "active" || Some(version) != turn.responder_binding_version {
                return Err(DbError::VersionConflict);
            }
            let permission_json: String = row.try_get("permission_ceiling_json")?;
            let policy_revision: String = row.try_get("policy_revision")?;
            let stored_policy_digest: String = row.try_get("policy_digest")?;
            let effective_policy_digest = if stored_policy_digest.trim().is_empty() {
                admission_policy_digest(&permission_json)?
            } else {
                stored_policy_digest
            };
            (
                row.try_get::<Option<String>, _>("identity_id")?
                    .ok_or_else(|| DbError::VersionConflict)?,
                policy_revision,
                effective_policy_digest,
                Some(permission_json),
                row.try_get("operating_skill_revision_id")?,
            )
        }
        _ => return Err(DbError::Check("unsupported Agent Chat kind".to_owned())),
    };

    if binding_identity_id != turn.responder_identity_id {
        return Err(DbError::VersionConflict);
    }
    let identity = sqlx::query(
        "SELECT version, selected_profile_id, account_permission_ceiling, paused
         FROM agent_identity WHERE id = ?",
    )
    .bind(&turn.responder_identity_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::VersionConflict)?;
    let identity_version: i64 = identity.try_get("version")?;
    let selected_profile_id: Option<String> = identity.try_get("selected_profile_id")?;
    let account_permission_json: String = identity.try_get("account_permission_ceiling")?;
    let paused: i64 = identity.try_get("paused")?;
    if paused != 0 {
        return Err(DbError::VersionConflict);
    }
    if Some(identity_version) != turn.responder_identity_version {
        return Err(DbError::VersionConflict);
    }
    let provenance: serde_json::Value = serde_json::from_str(
        turn.canonical_scope_provenance_json
            .as_deref()
            .ok_or_else(|| DbError::Check("missing turn provenance".to_owned()))?,
    )
    .map_err(|_| DbError::Check("turn provenance JSON is invalid".to_owned()))?;
    if provenance
        .get("identity_version")
        .and_then(serde_json::Value::as_i64)
        != Some(identity_version)
    {
        return Err(DbError::VersionConflict);
    }
    if turn.policy_revision.as_deref() != Some(binding_policy_revision.as_str())
        || turn.policy_digest.as_deref() != Some(binding_policy_digest.as_str())
    {
        return Err(DbError::VersionConflict);
    }
    let permission_json = binding_permission_json
        .as_deref()
        .unwrap_or(&account_permission_json);
    if turn.permission_policy_digest.as_deref()
        != Some(admission_policy_digest(permission_json)?.as_str())
    {
        return Err(DbError::VersionConflict);
    }
    let profile = sqlx::query(
        "SELECT identity_id, version, tool_policy_json
         FROM agent_profile WHERE id = ?",
    )
    .bind(&turn.profile_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::VersionConflict)?;
    let profile_identity_id: String = profile.try_get("identity_id")?;
    let profile_version: i64 = profile.try_get("version")?;
    let profile_tool_policy_json: String = profile.try_get("tool_policy_json")?;
    if profile_identity_id != turn.responder_identity_id
        || selected_profile_id.as_deref() != Some(turn.profile_id.as_str())
        || Some(profile_version) != turn.profile_version
        || turn.tool_policy_digest.as_deref()
            != Some(admission_policy_digest(&profile_tool_policy_json)?.as_str())
    {
        return Err(DbError::VersionConflict);
    }

    let expected_skill_revision = if kind == "account_main" {
        let genesis_active = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                 SELECT 1 FROM product_genesis_session
                 WHERE main_chat_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
             )",
        )
        .bind(&turn.chat_id)
        .fetch_one(&mut **transaction)
        .await?
            != 0;
        if genesis_active {
            sqlx::query_scalar::<_, String>(
                "SELECT current_revision_id FROM operating_skill
                 WHERE id = 'forge.main.project-discovery/v2' AND lifecycle = 'active'",
            )
            .fetch_optional(&mut **transaction)
            .await?
        } else {
            let revision = turn
                .operating_skill_revision_id
                .as_deref()
                .filter(|revision| supported_main_baseline_revision(revision))
                .ok_or(DbError::VersionConflict)?;
            Some(revision.to_owned())
        }
    } else {
        binding_skill_revision
    };
    if expected_skill_revision.as_deref() != turn.operating_skill_revision_id.as_deref() {
        return Err(DbError::VersionConflict);
    }

    Ok(())
}

/// The compiled Main baseline revisions admission will accept.
///
/// This must list every revision `operating_skills` still resolves a body
/// for. Bumping the baseline without adding it here rejects every new Main
/// Chat message with a bare version conflict, which is exactly as confusing
/// as it sounds -- see the paired test.
pub fn supported_main_baseline_revision(revision: &str) -> bool {
    matches!(
        revision,
        "forge.main.baseline/v1@1"
            | "forge.main.baseline/v1@2"
            | "forge.main.baseline/v1@3"
            | "forge.main.baseline/v1@4"
            | "forge.main.baseline/v1@5"
    )
}

pub(crate) fn admission_policy_digest(value: &str) -> Result<String> {
    let parsed: serde_json::Value = serde_json::from_str(value)
        .map_err(|_| DbError::Check("Agent Chat policy JSON is invalid".to_owned()))?;
    let envelope = serde_json::json!({
        "schema_version": "forge.agent-turn-policy/v1",
        "value": canonicalize_admission_json(&parsed),
    });
    let canonical = serde_json::to_string(&canonicalize_admission_json(&envelope))
        .map_err(|_| DbError::Check("Agent Chat policy JSON is invalid".to_owned()))?;
    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

pub(crate) fn handoff_content_digest_for_admission(
    content: &str,
    source_revisions_json: &str,
    source_message_id: Option<&str>,
    source_turn_job_id: Option<&str>,
) -> Result<String> {
    let value = serde_json::json!({
        "content": content,
        "source_message_id": source_message_id,
        "source_revisions_json": source_revisions_json,
        "source_turn_job_id": source_turn_job_id,
    });
    let envelope = serde_json::json!({
        "schema_version": "forge.agent-chat-content/v1",
        "value": value,
    });
    let canonical = serde_json::to_string(&canonicalize_admission_json(&envelope))
        .map_err(|_| DbError::Check("Agent Chat handoff content is invalid".to_owned()))?;
    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

pub(crate) fn handoff_admission_digest_for_provenance(
    dedupe_key: &str,
    content_digest: &str,
    causation_depth: i64,
    target_provenance: &serde_json::Value,
    source_provenance: Option<&serde_json::Value>,
) -> Result<String> {
    let value = serde_json::json!({
        "causation_depth": causation_depth,
        "causation_id": serde_json::Value::Null,
        "content_digest": content_digest,
        "dedupe_key": dedupe_key,
        "responder": target_provenance,
        "source_responder": source_provenance,
        "trigger": "main_project_handoff",
    });
    admission_digest_for_json(&value)
}

pub(crate) async fn validate_agent_chat_turn_job_id(
    transaction: &mut Transaction<'_, Sqlite>,
    turn_job_id: &str,
) -> Result<()> {
    let row = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
        .bind(turn_job_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::VersionConflict)?;
    let turn = map_agent_chat_turn_job(row)?;
    let input = CreateAgentChatTurnJob {
        id: turn.id,
        chat_id: turn.chat_id,
        triggering_message_id: turn.triggering_message_id,
        responder_identity_id: turn.responder_identity_id.unwrap_or_default(),
        profile_id: turn.profile_id.unwrap_or_default(),
        responder_binding_id: turn.responder_binding_id,
        responder_binding_version: turn.responder_binding_version,
        responder_identity_version: turn.responder_identity_version,
        profile_version: turn.profile_version,
        operating_skill_revision_id: turn.operating_skill_revision_id,
        policy_revision: turn.policy_revision,
        policy_digest: turn.policy_digest,
        permission_policy_digest: turn.permission_policy_digest,
        tool_policy_digest: turn.tool_policy_digest,
        admission_digest: turn.admission_digest,
        canonical_scope_provenance_json: turn.canonical_scope_provenance_json,
        canonical_scope_type: turn.canonical_scope_type,
        canonical_scope_id: turn.canonical_scope_id,
        dedupe_key: turn.dedupe_key,
        max_attempts: turn.max_attempts,
        correlation_id: turn.correlation_id,
        causation_id: turn.causation_id,
        causation_depth: turn.causation_depth,
        created_at: turn.created_at,
        updated_at: turn.updated_at,
    };
    validate_agent_chat_turn_admission(transaction, &input).await
}

#[derive(Debug, Deserialize)]
struct HandoffSourceResponderProvenance {
    chat_id: String,
    canonical_scope_type: String,
    canonical_scope_id: String,
    readiness: String,
    binding_id: Option<String>,
    binding_version: Option<i64>,
    identity_id: Option<String>,
    identity_version: Option<i64>,
    profile_id: Option<String>,
    profile_version: Option<i64>,
    operating_skill_revision: Option<String>,
    policy_revision: Option<String>,
    policy_digest: Option<String>,
    permission_policy_digest: Option<String>,
    tool_policy_digest: Option<String>,
}

pub(super) async fn validate_handoff_source_responder(
    transaction: &mut Transaction<'_, Sqlite>,
    source_chat_id: &str,
    provenance_json: &str,
    expected_author_identity_id: Option<&str>,
) -> Result<()> {
    let provenance: HandoffSourceResponderProvenance = serde_json::from_str(provenance_json)
        .map_err(|_| DbError::Check("handoff source responder provenance is invalid".to_owned()))?;
    if provenance.readiness != "ready"
        || provenance.chat_id != source_chat_id
        || provenance.canonical_scope_type != "agent_chat"
        || provenance.canonical_scope_id != source_chat_id
    {
        return Err(DbError::VersionConflict);
    }
    let identity_id = provenance
        .identity_id
        .ok_or_else(|| DbError::VersionConflict)?;
    if expected_author_identity_id != Some(identity_id.as_str()) {
        return Err(DbError::VersionConflict);
    }
    let profile_id = provenance
        .profile_id
        .ok_or_else(|| DbError::VersionConflict)?;
    let profile_version = provenance
        .profile_version
        .ok_or_else(|| DbError::VersionConflict)?;
    let turn = CreateAgentChatTurnJob {
        id: "handoff-source-responder-validation".to_owned(),
        chat_id: source_chat_id.to_owned(),
        triggering_message_id: "handoff-source-responder-validation".to_owned(),
        responder_identity_id: identity_id,
        profile_id,
        responder_binding_id: provenance.binding_id,
        responder_binding_version: provenance.binding_version,
        responder_identity_version: provenance.identity_version,
        profile_version: Some(profile_version),
        operating_skill_revision_id: provenance.operating_skill_revision,
        policy_revision: provenance.policy_revision,
        policy_digest: provenance.policy_digest,
        permission_policy_digest: provenance.permission_policy_digest,
        tool_policy_digest: provenance.tool_policy_digest,
        admission_digest: Some("handoff-source-responder-validation".to_owned()),
        canonical_scope_provenance_json: Some(provenance_json.to_owned()),
        canonical_scope_type: "agent_chat".to_owned(),
        canonical_scope_id: source_chat_id.to_owned(),
        dedupe_key: "handoff-source-responder-validation".to_owned(),
        max_attempts: 1,
        correlation_id: "handoff-source-responder-validation".to_owned(),
        causation_id: None,
        causation_depth: 0,
        created_at: "handoff-source-responder-validation".to_owned(),
        updated_at: "handoff-source-responder-validation".to_owned(),
    };
    validate_agent_chat_turn_admission(transaction, &turn).await
}
