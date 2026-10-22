use super::*;
/// Return the caller's handoff request after removing values that this
/// transaction fills in from durable rows.  Project, binding, handoff,
/// message, turn, chat, correlation, and creation-time values are transport
/// allocations; Charter, approval, policy, authorization, and semantic
/// content remain part of the canonical packet fingerprint.
fn normalize_handoff_request(value: &serde_json::Value) -> Result<serde_json::Value> {
    normalize_project_handoff_request(value).map_err(DbError::Check)
}

fn handoff_request_fingerprint(
    value: &serde_json::Value,
    input: &CreateProjectFromCharterApproval,
) -> Result<String> {
    // The create authorization is part of the immutable request identity, but
    // it is supplied as a typed input rather than trusted from handoff prose.
    // Include it in the normalized digest so a replay under a different
    // principal, action, event, or timestamp cannot reuse the receipt.
    let authorization = serde_json::json!({
        "principal_type": input.create_principal_type,
        "principal_id": input.create_principal_id,
        "authorization_basis": input.create_authorization_basis,
        "action": input.create_action,
        "event_id": input.create_event_id,
        "occurred_at": input.create_occurred_at,
    });
    project_handoff_request_fingerprint(value, &input.source_revisions_json, &authorization)
        .map_err(DbError::Check)
}

impl SqliteDb {
    pub async fn published_handoff_charter_approval_id(
        &self,
        dedupe_key: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT json_extract(source_revisions_json, '$.approval_id')
         FROM agent_handoff WHERE dedupe_key = ? LIMIT 1",
        )
        .bind(dedupe_key)
        .fetch_optional(self.pool())
        .await?)
    }
}

pub struct ProjectCreationOperatingSkillRow {
    row: SqliteRow,
}
impl ProjectCreationOperatingSkillRow {
    pub fn skill_key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("skill_key")?)
    }
    pub fn operating_skill_key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("operating_skill_key")?)
    }
    pub fn current_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("current_revision_id")?)
    }
}

impl SqliteDb {
    pub async fn project_creation_operating_skill_source(
        &self,
        skill_revision_id: &str,
    ) -> crate::Result<Option<ProjectCreationOperatingSkillRow>> {
        let rows = sqlx::query(
            "SELECT sr.skill_key, os.skill_key AS operating_skill_key, os.current_revision_id
         FROM operating_skill_revision sr
         JOIN operating_skill os ON os.id = sr.operating_skill_id
           AND os.skill_key = sr.skill_key
         WHERE sr.id = ? AND os.lifecycle = 'active'
         LIMIT 1",
        )
        .bind(skill_revision_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ProjectCreationOperatingSkillRow { row }))
    }
}

impl SqliteDb {
    pub async fn delivery_followup_governed_milestone_ids(
        &self,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT g.milestone_id FROM project_task_governance g
                 WHERE g.task_id = ? AND g.project_id = ? AND g.milestone_id IS NOT NULL",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_all(self.pool())
        .await?)
    }
}
#[allow(clippy::type_complexity)]
pub(super) async fn create_project_from_charter_approval(
    db: &SqliteDb,
    input: CreateProjectFromCharterApproval,
) -> Result<CreatedProjectFromCharterApproval> {
    let mut tx = crate::begin_immediate(db.pool()).await?;

    // Main supplies the canonical command identity with the composite
    // request. Resolve it while this transaction is still open so a
    // response-loss replay can validate the durable receipt before
    // entering the consumed approval path. `get_command_receipt_in_tx`
    // deliberately turns a changed principal or digest for the same
    // canonical key into IdempotencyConflict.
    let replayed_command_receipt = if let Some(receipt) = input.command_receipt.as_ref() {
        CommandReceiptRepo::get_command_receipt_in_tx(
            db,
            &mut tx,
            &receipt.principal_type,
            &receipt.principal_id,
            &receipt.scope_type,
            &receipt.scope_id,
            &receipt.operation,
            &receipt.idempotency_key,
            &receipt.input_digest,
        )
        .await?
    } else {
        None
    };

    // The create authorization is a second, explicit user action. Keep it
    // separate from the approval receipt and validate it again at the DB
    // boundary before any replay or mutation path can proceed.
    if input.create_principal_type != "user"
        || input.create_principal_id != input.account_id
        || input.create_action != "product_genesis.create_project_from_approval"
        || input.create_authorization_basis.trim().is_empty()
        || input.create_event_id.trim().is_empty()
        || input.create_occurred_at.trim().is_empty()
        || !valid_authorization_timestamp(&input.create_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }

    // Replay is intentionally resolved before checking the active receipt.
    // A consumed receipt is the durable idempotency record for the whole
    // composite operation, not an invitation to create a second Project.
    let approval_row = sqlx::query(
        "SELECT a.*, c.account_id, c.genesis_session_id, c.project_id AS charter_project_id,
                    c.current_approved_revision_id, a.approved_project_mode,
                    r.content_digest AS revision_content_digest,
                    r.rendered_digest AS revision_rendered_digest,
                    g.lifecycle AS genesis_lifecycle, g.project_id AS genesis_project_id,
                    g.handoff_id AS genesis_handoff_id, g.main_chat_id,
                    r.content_json AS revision_content_json
             FROM project_charter_approval a
             JOIN project_charter c ON c.id = a.charter_id
             JOIN project_charter_revision r ON r.id = a.revision_id
             LEFT JOIN product_genesis_session g ON g.id = c.genesis_session_id
             WHERE a.id = ?",
    )
    .bind(&input.approval_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;

    let approval = sqlx::query("SELECT * FROM project_charter_approval WHERE id = ?")
        .bind(&input.approval_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(DbError::from)
        .and_then(map_charter_approval)?;
    let account_id: String = approval_row.try_get("account_id")?;
    if account_id != input.account_id {
        return Err(DbError::VersionConflict);
    }
    let approval_event_id = approval
        .approval_event_id
        .as_deref()
        .ok_or(DbError::VersionConflict)?;
    let approval_event = sqlx::query(
        "SELECT principal_type, principal_id, authorization_basis,
                    action, explicit_event, occurred_at, lifecycle
             FROM project_charter_approval_event
             WHERE id = ? AND approval_id = ?",
    )
    .bind(approval_event_id)
    .bind(&approval.id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::VersionConflict)?;
    let event_lifecycle: String = approval_event.try_get("lifecycle")?;
    let event_principal_type: String = approval_event.try_get("principal_type")?;
    let event_principal_id: String = approval_event.try_get("principal_id")?;
    let event_authorization_basis: String = approval_event.try_get("authorization_basis")?;
    let event_action: String = approval_event.try_get("action")?;
    let event_explicit_event: String = approval_event.try_get("explicit_event")?;
    let event_occurred_at: String = approval_event.try_get("occurred_at")?;
    if event_lifecycle != "active"
        || event_principal_type != approval.approving_principal_type
        || event_principal_id != approval.approving_principal_id
        || event_authorization_basis != approval.authorization_basis
        || event_action != approval.authorization_action
        || event_explicit_event != approval.explicit_event
        || event_occurred_at != approval.authorization_occurred_at
    {
        return Err(DbError::VersionConflict);
    }
    if approval.approved_name.as_deref() != Some(input.project.name.as_str())
        || approval.selected_policy_digest.as_deref() != Some(input.policy_digest.as_str())
        || approval.selected_policy_revision.as_deref() != Some(input.policy_revision.as_str())
        || input.project.owner_id.as_deref() != Some(input.account_id.as_str())
    {
        return Err(DbError::VersionConflict);
    }
    let requested_mode = serde_json::from_str::<serde_json::Value>(&input.project.settings)
        .ok()
        .and_then(|settings| {
            settings
                .get("project_mode")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
    if requested_mode.as_deref() != Some(approval.approved_project_mode.as_str()) {
        return Err(DbError::VersionConflict);
    }
    let main_chat_id: String = approval_row.try_get("main_chat_id")?;
    if approval.lifecycle == "consumed" {
        if approval.consumed_project_id.is_none() {
            return Err(DbError::VersionConflict);
        }
        let replay_ids = if let Some(receipt) = replayed_command_receipt.as_ref() {
            let project_id = command_outcome_string(receipt, "project_id")?;
            let binding_id = command_outcome_string(receipt, "project_agent_binding_id")?;
            let project_chat_id = command_outcome_string(receipt, "project_chat_id")?;
            let handoff_id = command_outcome_string(receipt, "handoff_id")?;
            let target_message_id = command_outcome_string(receipt, "target_message_id")?;
            let target_turn_id = command_outcome_string(receipt, "target_turn_id")?;
            validate_command_outcome_identity(
                receipt,
                &[
                    ("charter_id", approval.charter_id.as_str()),
                    ("charter_revision_id", approval.revision_id.as_str()),
                ],
            )?;
            let valid_scope = (receipt.scope_type == "account"
                && receipt.scope_id == input.account_id)
                || (receipt.scope_type == "agent_chat" && receipt.scope_id == main_chat_id);
            if !valid_scope {
                return Err(DbError::IdempotencyConflict);
            }
            Some((
                project_id,
                binding_id,
                project_chat_id,
                handoff_id,
                target_message_id,
                target_turn_id,
            ))
        } else {
            None
        };
        let project_id = replay_ids
            .as_ref()
            .map(|ids| ids.0.as_str())
            .or(approval.consumed_project_id.as_deref())
            .ok_or(DbError::NotFound)?;
        if replay_ids
            .as_ref()
            .is_some_and(|ids| approval.consumed_project_id.as_deref() != Some(ids.0.as_str()))
        {
            return Err(DbError::IdempotencyConflict);
        }
        let project_row = sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
        ))
        .bind(project_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(DbError::from)?;
        let project = map_project(project_row)?;
        if (replayed_command_receipt.is_none() && project.id != input.project.id)
            || project.name != input.project.name
            || project.settings != input.project.settings
            || project.workflow_definition != input.project.workflow_definition
            || project.primary_repo_id != input.project.primary_repo_id
            || project.owner_id != input.project.owner_id
        {
            return Err(DbError::VersionConflict);
        }
        let (
            project_chat_id,
            binding_id,
            binding_identity_id,
            binding_profile_id,
            binding_skill_revision_id,
            binding_policy_revision,
            binding_policy_digest,
            binding_charter_id,
            binding_charter_revision_id,
            chat_status,
            binding_state,
        ): (
            String,
            String,
            String,
            String,
            Option<String>,
            String,
            String,
            Option<String>,
            Option<String>,
            String,
            String,
        ) = sqlx::query_as(
            "SELECT c.id, b.id, b.identity_id, b.profile_id,
                        b.operating_skill_revision_id, b.policy_revision, b.policy_digest,
                        b.charter_id, b.charter_revision_id, c.status, b.state
                 FROM agent_chat c
                 JOIN project_agent_binding b ON b.project_id = c.project_id
                   AND b.state IN ('active', 'replaced')
                 WHERE c.project_id = ? AND c.kind = 'project'
                   AND (? IS NULL OR c.id = ?) AND b.id = ?
                 LIMIT 1",
        )
        .bind(project_id)
        .bind(replay_ids.as_ref().map(|ids| ids.2.as_str()))
        .bind(replay_ids.as_ref().map(|ids| ids.2.as_str()))
        .bind(
            replay_ids
                .as_ref()
                .map(|ids| ids.1.as_str())
                .unwrap_or(input.project_agent_binding_id.as_str()),
        )
        .fetch_one(&mut *tx)
        .await?;
        if replay_ids
            .as_ref()
            .is_some_and(|ids| project_chat_id != ids.2 || binding_id != ids.1)
            || chat_status != "ready"
            || !matches!(binding_state.as_str(), "active" | "replaced")
            || binding_identity_id
                != approval
                    .selected_identity_id
                    .clone()
                    .ok_or(DbError::VersionConflict)?
            || binding_profile_id
                != approval
                    .selected_profile_id
                    .clone()
                    .ok_or(DbError::VersionConflict)?
            || binding_skill_revision_id != approval.selected_operating_skill_revision_id
            || binding_policy_revision
                != approval
                    .selected_policy_revision
                    .clone()
                    .ok_or(DbError::VersionConflict)?
            || binding_policy_digest != input.policy_digest
            || binding_charter_id.as_deref() != Some(approval.charter_id.as_str())
            || binding_charter_revision_id.as_deref() != Some(approval.revision_id.as_str())
        {
            return Err(DbError::VersionConflict);
        }
        let handoff = sqlx::query(
            "SELECT * FROM agent_handoff
                 WHERE id = ? AND target_chat_id = ? AND dedupe_key = ? LIMIT 1",
        )
        .bind(
            replay_ids
                .as_ref()
                .map(|ids| ids.3.as_str())
                .unwrap_or(input.handoff_id.as_str()),
        )
        .bind(&project_chat_id)
        .bind(&input.idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::VersionConflict)?;
        let handoff_id: String = handoff.try_get("id")?;
        let source_chat_id: String = handoff.try_get("source_chat_id")?;
        let stored_source_message_id: Option<String> = handoff.try_get("source_message_id")?;
        let stored_source_turn_id: Option<String> = handoff.try_get("source_turn_job_id")?;
        let target_message_id: String = handoff.try_get("target_message_id")?;
        let target_turn_id: String = handoff.try_get("target_turn_job_id")?;
        let stored_author_identity_id: Option<String> = handoff.try_get("author_identity_id")?;
        let stored_content: String = handoff.try_get("content")?;
        let stored_content_guard: String = handoff.try_get("content_guard_json")?;
        let stored_source_revisions: String = handoff.try_get("source_revisions_json")?;
        let stored_correlation_id: String = handoff.try_get("correlation_id")?;
        let stored_causation_id: Option<String> = handoff.try_get("causation_id")?;
        if replay_ids.as_ref().is_some_and(|ids| handoff_id != ids.3)
                || handoff_id != input.handoff_id && replayed_command_receipt.is_none()
                || source_chat_id != main_chat_id
                || stored_source_message_id != input.source_message_id
                || stored_source_turn_id != input.source_turn_id
                || replay_ids
                    .as_ref()
                    .map(|ids| target_message_id != ids.4 || target_turn_id != ids.5)
                    .unwrap_or(
                        target_message_id != input.target_message_id
                            || target_turn_id != input.target_turn_id,
                    )
                || stored_author_identity_id.as_deref()
                    != Some(
                        input
                            .source_identity_id
                            .as_deref()
                            .ok_or(DbError::VersionConflict)?,
                )
                || stored_content != input.handoff_content
                || stored_content_guard != input.content_guard_json
                // A contender can enter this transaction before the winning
                // receipt is visible and therefore carry a different
                // server-minted transport correlation.  Once the receipt is
                // resolved, the committed packet is authoritative; only a
                // fresh mutation may be compared with the candidate value.
                || (replayed_command_receipt.is_none()
                    && stored_correlation_id != input.correlation_id)
                || stored_causation_id != input.causation_id
        {
            return Err(DbError::VersionConflict);
        }
        let stored_source_value =
            serde_json::from_str::<serde_json::Value>(&stored_source_revisions)
                .map_err(|_| DbError::VersionConflict)?;
        let stored_source = stored_source_value
            .get("source")
            .ok_or(DbError::VersionConflict)?;
        let stored_project = stored_source_value
            .get("project")
            .ok_or(DbError::VersionConflict)?;
        let stored_target = stored_source_value
            .get("target")
            .ok_or(DbError::VersionConflict)?;
        let stored_request = stored_source_value
            .get("request")
            .ok_or(DbError::VersionConflict)?;
        if json_string(&stored_source_value, "correlation_id")
            != Some(stored_correlation_id.as_str())
            || json_string(stored_source, "identity_id") != input.source_identity_id.as_deref()
            || json_string(stored_source, "profile_revision_id")
                != input.source_profile_id.as_deref()
            || json_string(stored_source, "instruction_revision_id")
                != input.source_instruction_revision_id.as_deref()
            || json_string(stored_source, "message_id") != input.source_message_id.as_deref()
            || json_string(stored_source, "turn_id") != input.source_turn_id.as_deref()
            || json_string(stored_project, "id") != Some(project.id.as_str())
            || json_string(stored_project, "name") != Some(input.project.name.as_str())
            || json_string(stored_project, "mode") != Some(approval.approved_project_mode.as_str())
            || json_string(stored_target, "chat_id") != Some(project_chat_id.as_str())
            || json_string(stored_target, "binding_id") != Some(binding_id.as_str())
            || json_string(stored_target, "message_id") != Some(target_message_id.as_str())
            || json_string(stored_target, "turn_id") != Some(target_turn_id.as_str())
            || json_string(stored_request, "policy_revision")
                != Some(input.policy_revision.as_str())
            || json_string(stored_request, "policy_digest") != Some(input.policy_digest.as_str())
        {
            return Err(DbError::VersionConflict);
        }
        let stored_source_digest = stored_source_value
            .pointer("/request/source_revisions_digest")
            .and_then(serde_json::Value::as_str)
            .ok_or(DbError::VersionConflict)?;
        let request_value = serde_json::from_str::<serde_json::Value>(&input.source_revisions_json)
            .map_err(|_| DbError::VersionConflict)?;
        if stored_source_digest != handoff_request_fingerprint(&request_value, &input)? {
            return Err(DbError::VersionConflict);
        }

        let replayed = CreatedProjectFromCharterApproval {
            project,
            project_agent_binding_id: binding_id,
            project_chat_id,
            charter_id: approval.charter_id,
            charter_revision_id: approval.revision_id,
            handoff_id,
            target_message_id,
            target_turn_id,
        };
        if let Some(receipt) = replayed_command_receipt.as_ref() {
            let expected_outcome = serde_json::json!({
                "operation": receipt.operation,
                "project_id": replayed.project.id,
                "project_agent_binding_id": replayed.project_agent_binding_id,
                "project_chat_id": replayed.project_chat_id,
                "charter_id": replayed.charter_id,
                "charter_revision_id": replayed.charter_revision_id,
                "handoff_id": replayed.handoff_id,
                "target_message_id": replayed.target_message_id,
                "target_turn_id": replayed.target_turn_id,
            });
            let stored_outcome = serde_json::from_str::<serde_json::Value>(&receipt.outcome_json)
                .map_err(|_| DbError::IdempotencyConflict)?;
            if stored_outcome != expected_outcome {
                return Err(DbError::IdempotencyConflict);
            }
            // The receipt's execution is authoritative.  A response-loss
            // retry may carry a fresh execution shell (including the
            // pre-finalization placeholder outcome), so validate the
            // persisted provenance/linkage without treating that shell's
            // server-minted fields as caller identity.
            validate_replay_action_bundle(&mut tx, receipt, None).await?;
            let event = sqlx::query(
                "SELECT event_type, entity_type, entity_id, scope_type, scope_id,
                            payload_json
                     FROM domain_event WHERE id = ?",
            )
            .bind(&receipt.event_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            let event_type: String = event.try_get("event_type")?;
            let entity_type: String = event.try_get("entity_type")?;
            let entity_id: String = event.try_get("entity_id")?;
            let event_scope_type: String = event.try_get("scope_type")?;
            let event_scope_id: String = event.try_get("scope_id")?;
            if event_type != "project.created_from_charter_approval"
                || entity_type != "project"
                || entity_id != replayed.project.id
                || event_scope_type != "project"
                || event_scope_id != replayed.project.id
            {
                return Err(DbError::IdempotencyConflict);
            }
            let payload: serde_json::Value =
                serde_json::from_str(&event.try_get::<String, _>("payload_json")?)
                    .map_err(|_| DbError::IdempotencyConflict)?;
            if json_string(&payload, "project_id") != Some(replayed.project.id.as_str())
                || json_string(&payload, "charter_id") != Some(replayed.charter_id.as_str())
                || json_string(&payload, "charter_revision_id")
                    != Some(replayed.charter_revision_id.as_str())
                || json_string(&payload, "approval_id") != Some(approval.id.as_str())
                || json_string(&payload, "handoff_id") != Some(replayed.handoff_id.as_str())
                || json_string(&payload, "project_chat_id")
                    != Some(replayed.project_chat_id.as_str())
                || json_string(&payload, "project_agent_binding_id")
                    != Some(replayed.project_agent_binding_id.as_str())
            {
                return Err(DbError::IdempotencyConflict);
            }
        }
        tx.commit().await?;
        return Ok(replayed);
    }
    if approval.lifecycle != "active" {
        return Err(DbError::VersionConflict);
    }
    let genesis_lifecycle: Option<String> = approval_row.try_get("genesis_lifecycle")?;
    if genesis_lifecycle.as_deref() != Some("ready_for_project") {
        return Err(DbError::VersionConflict);
    }
    let revision_content_digest: String = approval_row.try_get("revision_content_digest")?;
    let revision_rendered_digest: String = approval_row.try_get("revision_rendered_digest")?;
    let current_approved_revision_id: Option<String> =
        approval_row.try_get("current_approved_revision_id")?;
    if approval.content_digest != revision_content_digest
        || approval.rendered_digest != revision_rendered_digest
        || current_approved_revision_id.as_deref() != Some(approval.revision_id.as_str())
        || approval.selected_identity_id.is_none()
        || approval.selected_profile_id.is_none()
        || approval.selected_operating_skill_revision_id.is_none()
        || approval.approval_event_id.is_none()
    {
        return Err(DbError::VersionConflict);
    }
    let genesis_session_id: String = approval_row
        .try_get::<Option<String>, _>("genesis_session_id")?
        .ok_or(DbError::VersionConflict)?;
    let author_identity_id = input
        .source_identity_id
        .clone()
        .ok_or(DbError::VersionConflict)?;
    let historical_author: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM agent_identity
             WHERE id = ? AND owner_id = ? LIMIT 1",
    )
    .bind(&author_identity_id)
    .bind(&input.account_id)
    .fetch_optional(&mut *tx)
    .await?;
    if historical_author.is_none() {
        return Err(DbError::VersionConflict);
    }
    if let Some(source_profile_id) = input.source_profile_id.as_deref() {
        let profile_ok: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM agent_profile p
                 JOIN agent_identity i ON i.id = p.identity_id
                 WHERE p.id = ? AND p.identity_id = ? AND i.owner_id = ? LIMIT 1",
        )
        .bind(source_profile_id)
        .bind(&author_identity_id)
        .bind(&input.account_id)
        .fetch_optional(&mut *tx)
        .await?;
        if profile_ok.is_none() {
            return Err(DbError::VersionConflict);
        }
    }
    if let Some(source_instruction_revision_id) = input.source_instruction_revision_id.as_deref() {
        let instruction_ok: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM agent_chat_instruction_revision
                 WHERE id = ? AND chat_id = ? LIMIT 1",
        )
        .bind(source_instruction_revision_id)
        .bind(&main_chat_id)
        .fetch_optional(&mut *tx)
        .await?;
        if instruction_ok.is_none() {
            return Err(DbError::VersionConflict);
        }
    }
    if let Some(source_message_id) = input.source_message_id.as_deref() {
        let message_ok: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM agent_chat_message
                 WHERE id = ? AND chat_id = ? LIMIT 1",
        )
        .bind(source_message_id)
        .bind(&main_chat_id)
        .fetch_optional(&mut *tx)
        .await?;
        if message_ok.is_none() {
            return Err(DbError::VersionConflict);
        }
    }
    if let Some(source_turn_id) = input.source_turn_id.as_deref() {
        let turn_ok: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM agent_chat_turn_job
                 WHERE id = ? AND chat_id = ? AND responder_identity_id = ?
                   AND (? IS NULL OR profile_id = ?)
                 LIMIT 1",
        )
        .bind(source_turn_id)
        .bind(&main_chat_id)
        .bind(&author_identity_id)
        .bind(input.source_profile_id.as_deref())
        .bind(input.source_profile_id.as_deref())
        .fetch_optional(&mut *tx)
        .await?;
        if turn_ok.is_none() {
            return Err(DbError::VersionConflict);
        }
        // A frozen Genesis source turn must still authenticate the
        // historical Main binding/Profile snapshot while this composite
        // holds its IMMEDIATE transaction lock.  Legacy pre-V088 source
        // turns take the conservative no-provenance path in the shared
        // validator; they are never silently rebound to a new identity.
        super::agent_chat::validate_agent_chat_turn_job_id(&mut tx, source_turn_id).await?;
    }
    let identity_id = approval
        .selected_identity_id
        .clone()
        .ok_or(DbError::VersionConflict)?;
    let profile_id = approval
        .selected_profile_id
        .clone()
        .ok_or(DbError::VersionConflict)?;
    let skill_revision_id = approval
        .selected_operating_skill_revision_id
        .clone()
        .ok_or(DbError::VersionConflict)?;
    let project_mode: String = approval_row.try_get("approved_project_mode")?;
    if !matches!(project_mode.as_str(), "compact" | "standard") {
        return Err(DbError::Check(
            "approved Project mode is invalid".to_owned(),
        ));
    }
    if approval.approved_name.as_deref() != Some(input.project.name.as_str())
        || approval.selected_policy_digest.as_deref() != Some(input.policy_digest.as_str())
        || approval.selected_policy_revision.as_deref() != Some(input.policy_revision.as_str())
        || input.project.owner_id.as_deref() != Some(input.account_id.as_str())
    {
        return Err(DbError::VersionConflict);
    }
    let requested_mode = serde_json::from_str::<serde_json::Value>(&input.project.settings)
        .ok()
        .and_then(|settings| {
            settings
                .get("project_mode")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
    if requested_mode.as_deref() != Some(project_mode.as_str()) {
        return Err(DbError::VersionConflict);
    }
    let name_taken: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM project
             WHERE owner_id = ? AND name = ? LIMIT 1",
    )
    .bind(&input.account_id)
    .bind(&input.project.name)
    .fetch_optional(&mut *tx)
    .await?;
    if name_taken.is_some() {
        return Err(DbError::VersionConflict);
    }

    let selected = sqlx::query(
        "SELECT p.tool_policy_json, i.paused, i.archived_at,
                    i.selected_profile_id, sr.id AS skill_revision_id,
                    sr.skill_key, sr.policy_digest, sr.content_digest,
                    s.current_revision_id, s.lifecycle
             FROM agent_profile p
             JOIN agent_identity i ON i.id = p.identity_id
             JOIN operating_skill_revision sr ON sr.id = ?
             JOIN operating_skill s ON s.id = sr.operating_skill_id
             WHERE p.id = ? AND p.identity_id = ? AND i.owner_id = ?
               AND i.selected_profile_id = p.id
             LIMIT 1",
    )
    .bind(&skill_revision_id)
    .bind(&profile_id)
    .bind(&identity_id)
    .bind(&input.account_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(selected) = selected else {
        return Err(DbError::VersionConflict);
    };
    let selected_paused: i64 = selected.try_get("paused")?;
    let selected_archived: Option<String> = selected.try_get("archived_at")?;
    let selected_profile_id: Option<String> = selected.try_get("selected_profile_id")?;
    let selected_skill_revision_id: String = selected.try_get("skill_revision_id")?;
    let selected_skill_key: String = selected.try_get("skill_key")?;
    let selected_skill_policy_digest: String = selected.try_get("policy_digest")?;
    let selected_skill_content_digest: String = selected.try_get("content_digest")?;
    let selected_skill_current_revision_id: Option<String> =
        selected.try_get("current_revision_id")?;
    let selected_skill_lifecycle: String = selected.try_get("lifecycle")?;
    let selected_tool_policy_json: String = selected.try_get("tool_policy_json")?;
    if selected_profile_id.as_deref() != Some(profile_id.as_str())
        || selected_skill_revision_id != skill_revision_id
        || selected_skill_current_revision_id.as_deref() != Some(skill_revision_id.as_str())
        || selected_skill_lifecycle != "active"
        || selected_paused != 0
        || selected_archived.is_some()
        || selected_skill_key != PROJECT_OPERATING_SKILL_KEY
        || selected_skill_policy_digest.trim().is_empty()
        || selected_skill_content_digest.trim().is_empty()
        || profile_policy_digest(&selected_tool_policy_json) != input.policy_digest
    {
        return Err(DbError::VersionConflict);
    }

    sqlx::query(
        "INSERT INTO project (
                id, name, settings, workflow_definition, workflow_template_name,
                primary_repo_id, owner_id, project_hooks_json, project_work_epoch,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, NULL, ?, ?, '[]', 0, ?, ?)",
    )
    .bind(&input.project.id)
    .bind(&input.project.name)
    .bind(&input.project.settings)
    .bind(&input.project.workflow_definition)
    .bind(input.project.primary_repo_id.as_deref())
    .bind(input.project.owner_id.as_deref())
    .bind(&input.project.created_at)
    .bind(&input.project.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    super::super::project::bootstrap_project_chat_and_binding_in_tx(
        &mut tx,
        &input.project.id,
        &input.project.created_at,
        &input.project.updated_at,
    )
    .await?;

    // The Project-create commit also schedules the durable provisioning
    // operation and its complete checkpoint set.  This is intentionally
    // before any response/follow-up work: a process stop after this
    // transaction commits still leaves a recoverable operation for the
    // next replay or background reconciler.
    sqlx::query(
        "INSERT INTO project_provisioning_operation (
                id, project_id, idempotency_key, status, current_checkpoint,
                attempt_count, max_attempts, retryable,
                last_error_code, last_error_message, created_at, updated_at,
                version
             ) VALUES (?, ?, ?, 'setup_required', 'preflight', 0, ?, 1,
                       NULL, NULL, ?, ?, 1)",
    )
    .bind(&input.provisioning_operation_id)
    .bind(&input.project.id)
    .bind(format!("project-provisioning:{}", input.project.id))
    .bind(input.max_attempts)
    .bind(&input.project.created_at)
    .bind(&input.project.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    for checkpoint in [
        "preflight",
        "repository_scaffolded",
        "repository_initialized",
        "repository_registered",
        "repository_linked",
        "roles_assigned",
    ] {
        sqlx::query(
            "INSERT INTO project_provisioning_checkpoint (
                    id, operation_id, checkpoint, status, attempt_count,
                    error_code, error_message, details_json, started_at,
                    completed_at, created_at, updated_at, version
                 ) VALUES (?, ?, ?, 'pending', 0, NULL, NULL, '{}', NULL,
                           NULL, ?, ?, 1)",
        )
        .bind(new_uuid_v4())
        .bind(&input.provisioning_operation_id)
        .bind(checkpoint)
        .bind(&input.project.created_at)
        .bind(&input.project.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
    }

    let (project_chat_id, setup_binding_id): (String, String) = sqlx::query_as(
        "SELECT c.id, b.id FROM agent_chat c
             JOIN project_agent_binding b ON b.project_id = c.project_id
               AND b.state = 'agent_setup_required'
             WHERE c.project_id = ? AND c.kind = 'project'",
    )
    .bind(&input.project.id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|error| DbError::Check(format!("Project setup binding is unavailable: {error}")))?;

    let replaced_setup = sqlx::query(
        "UPDATE project_agent_binding
             SET state = 'replaced', replacement_reason = 'Charter-backed Project creation',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND state = 'agent_setup_required'",
    )
    .bind(&input.project.updated_at)
    .bind(&setup_binding_id)
    .execute(&mut *tx)
    .await?;
    if replaced_setup.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let active_binding = sqlx::query(
        "INSERT INTO project_agent_binding (
                id, project_id, identity_id, profile_id, state,
                autonomy_policy_json, permission_ceiling_json, subscriptions_json,
                wake_budget, version, replaced_by_binding_id,
                operating_skill_revision_id, policy_revision, policy_digest,
                charter_id, charter_revision_id, charter_setup_required,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'active', '{}', ?, '[]', ?, 1, NULL,
                       ?, ?, ?, ?, ?, 1, ?, ?)",
    )
    .bind(&input.project_agent_binding_id)
    .bind(&input.project.id)
    .bind(&identity_id)
    .bind(&profile_id)
    .bind(PROJECT_AGENT_PERMISSION_CEILING)
    .bind(crate::DEFAULT_PROJECT_AGENT_WAKE_BUDGET)
    .bind(&skill_revision_id)
    .bind(
        approval
            .selected_policy_revision
            .as_deref()
            .ok_or(DbError::VersionConflict)?,
    )
    .bind(&input.policy_digest)
    .bind(&approval.charter_id)
    .bind(&approval.revision_id)
    .bind(&input.project.created_at)
    .bind(&input.project.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let linked_setup = sqlx::query(
        "UPDATE project_agent_binding SET replaced_by_binding_id = ?
             WHERE id = ? AND state = 'replaced'",
    )
    .bind(&input.project_agent_binding_id)
    .bind(&setup_binding_id)
    .execute(&mut *tx)
    .await?;
    if active_binding.rows_affected() != 1 || linked_setup.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let ready_chat = sqlx::query(
        "UPDATE agent_chat SET status = 'ready', version = version + 1, updated_at = ?
             WHERE id = ? AND status = 'agent_setup_required'",
    )
    .bind(&input.project.updated_at)
    .bind(&project_chat_id)
    .execute(&mut *tx)
    .await?;

    if ready_chat.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }

    // The target Project binding/chat now exist inside this same
    // transaction. Resolve the identity's current Profile and exact
    // policy/skill versions before admitting the handoff turn; the
    // approval's bind-time profile id is not treated as a Profile
    // snapshot. These reads are protected by the IMMEDIATE transaction
    // and are also checked against the values used to create the binding.
    let target_binding = sqlx::query(
        "SELECT version, identity_id, profile_id, operating_skill_revision_id,
                    policy_revision, policy_digest, permission_ceiling_json, state
             FROM project_agent_binding
             WHERE id = ? AND project_id = ?",
    )
    .bind(&input.project_agent_binding_id)
    .bind(&input.project.id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|error| DbError::Check(format!("Project target binding is unavailable: {error}")))?;
    let target_binding_version: i64 = target_binding.try_get("version")?;
    let target_binding_identity_id: Option<String> = target_binding.try_get("identity_id")?;
    let target_binding_profile_id: Option<String> = target_binding.try_get("profile_id")?;
    let target_binding_skill_revision: Option<String> =
        target_binding.try_get("operating_skill_revision_id")?;
    let target_binding_policy_revision: String = target_binding.try_get("policy_revision")?;
    let target_binding_policy_digest: String = target_binding.try_get("policy_digest")?;
    let target_permission_json: String = target_binding.try_get("permission_ceiling_json")?;
    let target_binding_state: String = target_binding.try_get("state")?;
    if target_binding_state != "active"
        || target_binding_identity_id.as_deref() != Some(identity_id.as_str())
        || target_binding_profile_id.as_deref() != Some(profile_id.as_str())
        || target_binding_skill_revision.as_deref() != Some(skill_revision_id.as_str())
        || target_binding_policy_revision != input.policy_revision
        || target_binding_policy_digest != input.policy_digest
    {
        return Err(DbError::VersionConflict);
    }
    let target_identity = sqlx::query(
        "SELECT version, selected_profile_id, paused
             FROM agent_identity WHERE id = ?",
    )
    .bind(&identity_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|error| DbError::Check(format!("Project target identity is unavailable: {error}")))?;
    let target_identity_version: i64 = target_identity.try_get("version")?;
    let target_selected_profile_id: Option<String> =
        target_identity.try_get("selected_profile_id")?;
    let target_paused: i64 = target_identity.try_get("paused")?;
    if target_paused != 0 || target_selected_profile_id.as_deref() != Some(profile_id.as_str()) {
        return Err(DbError::VersionConflict);
    }
    let target_profile = sqlx::query(
        "SELECT identity_id, version, tool_policy_json
             FROM agent_profile WHERE id = ?",
    )
    .bind(&profile_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|error| DbError::Check(format!("Project target Profile is unavailable: {error}")))?;
    let target_profile_identity_id: String = target_profile.try_get("identity_id")?;
    let target_profile_version: i64 = target_profile.try_get("version")?;
    let target_tool_policy_json: String = target_profile.try_get("tool_policy_json")?;
    if target_profile_identity_id != identity_id {
        return Err(DbError::VersionConflict);
    }
    let target_permission_policy_digest =
        super::agent_chat::admission_policy_digest(&target_permission_json)?;
    let target_tool_policy_digest =
        super::agent_chat::admission_policy_digest(&target_tool_policy_json)?;
    let target_provenance = serde_json::json!({
        "chat_id": project_chat_id,
        "canonical_scope_type": "agent_chat",
        "canonical_scope_id": project_chat_id,
        "readiness": "ready",
        "binding_id": input.project_agent_binding_id,
        "binding_version": target_binding_version,
        "identity_id": identity_id,
        "identity_version": target_identity_version,
        "profile_id": profile_id,
        "profile_version": target_profile_version,
        "operating_skill_revision": skill_revision_id,
        "policy_revision": input.policy_revision,
        "policy_digest": input.policy_digest,
        "permission_policy_digest": target_permission_policy_digest,
        "tool_policy_digest": target_tool_policy_digest,
    });
    let source_provenance = if let Some(source_turn_id) = input.source_turn_id.as_deref() {
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT canonical_scope_provenance_json
                 FROM agent_chat_turn_job WHERE id = ?",
        )
        .bind(source_turn_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| DbError::Check(format!("Main source turn is unavailable: {error}")))?
        .map(|json| {
            serde_json::from_str::<serde_json::Value>(&json).map_err(|_| DbError::VersionConflict)
        })
        .transpose()?
    } else {
        None
    };
    let admission_source_value =
        serde_json::from_str::<serde_json::Value>(&input.source_revisions_json).map_err(|_| {
            DbError::Check("handoff source_revisions_json must be valid JSON".to_owned())
        })?;
    let admission_source_revisions = normalize_handoff_request(&admission_source_value)?;
    let admission_source_revisions_json = serde_json::to_string(
        &canonicalize_project_handoff_json(&admission_source_revisions),
    )
    .map_err(|error| DbError::Check(format!("invalid handoff request: {error}")))?;
    let handoff_content_digest = super::agent_chat::handoff_content_digest_for_admission(
        &input.handoff_content,
        &admission_source_revisions_json,
        input.source_message_id.as_deref(),
        input.source_turn_id.as_deref(),
    )?;
    let admission_digest = super::agent_chat::handoff_admission_digest_for_provenance(
        &format!("handoff:{}", input.idempotency_key),
        &handoff_content_digest,
        input.causation_depth.saturating_add(1),
        &target_provenance,
        source_provenance.as_ref(),
    )?;
    let target_provenance_json = serde_json::to_string(&target_provenance)
        .map_err(|_| DbError::Check("target turn provenance is invalid".to_owned()))?;

    // Attach the already-approved Charter before setting the Project
    // pointer; the migration trigger deliberately requires this order.
    let attached_charter = sqlx::query(
        "UPDATE project_charter SET project_id = ?, lifecycle = 'attached', updated_at = ?
             WHERE id = ? AND project_id IS NULL",
    )
    .bind(&input.project.id)
    .bind(&input.project.updated_at)
    .bind(&approval.charter_id)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if attached_charter.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let backed_project = sqlx::query(
        "UPDATE project SET charter_status = 'charter_backed', charter_setup_required = 0,
                 current_charter_id = ?, current_charter_revision_id = ?,
                 current_charter_version = ?, version = version + 1, updated_at = ?
             WHERE id = ?",
    )
    .bind(&approval.charter_id)
    .bind(&approval.revision_id)
    .bind(approval.expected_charter_version + 1)
    .bind(&input.project.updated_at)
    .bind(&input.project.id)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if backed_project.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }

    // Compact Projects start with one explicit M001. Acceptance
    // statements already approved in the Charter become manual checks
    // with required, check-linked evidence; the Project Agent may refine
    // the immutable definition later. Approving the Charter approves the
    // work, so the milestone becomes active with its approved definition
    // rather than waiting on a second artifact.
    if project_mode == "compact" {
        let milestone_id = new_uuid_v4();
        let milestone_revision_id = new_uuid_v4();
        let revision_content_json: String = approval_row.try_get("revision_content_json")?;
        let revision_content: serde_json::Value = serde_json::from_str(&revision_content_json)
            .map_err(|error| {
                DbError::Check(format!("approved Charter content is invalid JSON: {error}"))
            })?;
        let acceptance_statements = revision_content
            .get("success")
            .and_then(serde_json::Value::as_object)
            .and_then(|success| success.get("acceptance_statements"))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                DbError::Check("approved Charter acceptance_statements must be an array".to_owned())
            })?;
        if acceptance_statements.is_empty() {
            return Err(DbError::Check(
                "approved Charter acceptance_statements must not be empty".to_owned(),
            ));
        }
        let mut acceptance_checks = Vec::new();
        let mut evidence_requirements = Vec::new();
        let mut acceptance_check_rows = Vec::new();
        for (index, statement) in acceptance_statements.iter().enumerate() {
            let description = statement
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    DbError::Check(format!(
                        "approved Charter acceptance statement {index} must be a non-empty string"
                    ))
                })?;
            let check_id = new_uuid_v4();
            acceptance_checks.push(serde_json::json!({
                "id": check_id.clone(),
                "description": description,
                "required": true,
                "source_kind": "manual",
                "expected_result": "passed",
            }));
            evidence_requirements.push(serde_json::json!({
                "id": check_id.clone(),
                "description": format!("Authoritative evidence for: {description}"),
                "required": true,
                "evidence_kind": null,
            }));
            acceptance_check_rows.push((
                check_id,
                format!("acceptance-{}", index + 1),
                description.to_owned(),
            ));
        }
        let milestone_outcome = format!("Initial outcome for {}", input.project.name);
        let milestone_content_json = serde_json::json!({
            "name": "M1 — Deliver outcome",
            "outcome": milestone_outcome,
            "included_scope": [],
            "excluded_scope": [],
            "charter_revision": approval.revision_id,
            "document_revisions": [],
            "task_ids": [],
            "dependencies": [],
            "risks": [],
            "acceptance_checks": acceptance_checks.clone(),
            "evidence_requirements": evidence_requirements.clone(),
            "known_issues": [],
        })
        .to_string();
        let milestone_rendered_view = format!("# M1 — Deliver outcome\n\n{}", milestone_outcome);
        let milestone_content_digest = sha256_hex(milestone_content_json.as_bytes());
        let milestone_rendered_digest = sha256_hex(milestone_rendered_view.as_bytes());
        sqlx::query(
                "INSERT INTO project_milestone (
                    id, project_id, milestone_sequence, milestone_key, display_label,
                    lifecycle, blocker_reason_json, stale_reason_json,
                    reconciliation_reason_json, version, created_at, updated_at
                 ) VALUES (?, ?, 1, 'M001', 'M1 — Deliver outcome', 'planned', '[]', '[]', '[]', 1, ?, ?)",
            )
            .bind(&milestone_id)
            .bind(&input.project.id)
            .bind(&input.project.created_at)
            .bind(&input.project.updated_at)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
        sqlx::query(
                "INSERT INTO project_milestone_revision (
                    id, milestone_id, revision, base_revision, lifecycle,
                    display_label, outcome, included_scope_json, excluded_scope_json,
                    charter_revision_id, document_revisions_json, task_selection_json,
                    dependencies_json, risks_json, acceptance_checks_json,
                    evidence_requirements_json, known_issues_json, change_summary,
                    schema_version, render_version, rendered_view, content_digest,
                    rendered_digest,
                    author_type, author_id, source_refs_json, created_at
                 ) VALUES (?, ?, 1, 0, 'approved', 'M1 — Deliver outcome', ?, '[]', '[]', ?, '[]', '[]',
                           '[]', '[]', ?, ?, '[]', 'Genesis baseline',
                           'forge.project-orchestration/v1', '1', ?, ?, ?, 'system',
                           'forge.project_creation', '[]', ?)",
            )
            .bind(&milestone_revision_id)
            .bind(&milestone_id)
            .bind(&milestone_outcome)
            .bind(&approval.revision_id)
            .bind(serde_json::to_string(&acceptance_checks).map_err(|error| {
                DbError::Check(format!("invalid compact milestone checks: {error}"))
            })?)
            .bind(serde_json::to_string(&evidence_requirements).map_err(|error| {
                DbError::Check(format!("invalid compact milestone evidence: {error}"))
            })?)
            .bind(&milestone_rendered_view)
            .bind(&milestone_content_digest)
            .bind(&milestone_rendered_digest)
            .bind(&input.project.created_at)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
        for (check_id, check_key, description) in acceptance_check_rows {
            sqlx::query(
                "INSERT INTO project_milestone_check (
                        id, project_id, milestone_id, definition_revision_id,
                        check_key, description, required, source_kind,
                        expected_result, evidence_required, version,
                        current_result_id, created_at, updated_at
                     ) VALUES (?, ?, ?, ?, ?, ?, 1, 'manual', 'passed', 1, 1, NULL, ?, ?)",
            )
            .bind(check_id)
            .bind(&input.project.id)
            .bind(&milestone_id)
            .bind(&milestone_revision_id)
            .bind(check_key)
            .bind(description)
            .bind(&input.project.created_at)
            .bind(&input.project.updated_at)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
        }
        let milestone_pointer = sqlx::query(
            // An approved definition is what makes a milestone active
            // work. Baseline activation used to perform this transition;
            // the approved Charter and its milestone definition are the
            // authority now, so the pointer advance carries it.
            "UPDATE project_milestone
                 SET current_definition_revision_id = ?,
                     lifecycle = CASE WHEN lifecycle = 'planned'
                                      THEN 'active' ELSE lifecycle END,
                     version = version + 1,
                     updated_at = ? WHERE id = ? AND project_id = ?",
        )
        .bind(&milestone_revision_id)
        .bind(&input.project.updated_at)
        .bind(&milestone_id)
        .bind(&input.project.id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if milestone_pointer.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let project_pointer = sqlx::query(
            "UPDATE project SET primary_milestone_id = ?, version = version + 1,
                     updated_at = ? WHERE id = ?",
        )
        .bind(&milestone_id)
        .bind(&input.project.updated_at)
        .bind(&input.project.id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if project_pointer.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
    }

    sqlx::query(
        "INSERT INTO project_member (id, project_id, user_id, role, created_at, updated_at)
             VALUES (?, ?, ?, 'owner', ?, ?)",
    )
    .bind(&input.member_id)
    .bind(&input.project.id)
    .bind(&input.account_id)
    .bind(&input.project.created_at)
    .bind(&input.project.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;

    let sequence: i64 = sqlx::query_scalar(
        "UPDATE agent_chat SET message_count = message_count + 1,
                    last_message_at = ?, version = version + 1, updated_at = ?
             WHERE id = ? RETURNING message_count - 1",
    )
    .bind(&input.project.updated_at)
    .bind(&input.project.updated_at)
    .bind(&project_chat_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|error| DbError::Check(format!("Project Chat sequence is unavailable: {error}")))?;
    let source_value = serde_json::from_str::<serde_json::Value>(&input.source_revisions_json)
        .map_err(|_| {
            DbError::Check("handoff source_revisions_json must be valid JSON".to_owned())
        })?;
    if !source_value.is_object() {
        return Err(DbError::Check(
            "handoff source_revisions_json must be a JSON object".to_owned(),
        ));
    }
    let handoff_payload_digest = handoff_request_fingerprint(&source_value, &input)?;
    let source_revisions_json: String = sqlx::query_scalar(
        "SELECT json_set(
                ?,
                '$.handoff_id', ?,
                '$.correlation_id', ?,
                '$.target.chat_id', ?,
                '$.target.binding_id', ?,
                '$.target.message_id', ?,
                '$.target.turn_id', ?,
                '$.project.id', ?,
                '$.approval_id', ?,
                '$.source.identity_id', ?,
                '$.source.profile_revision_id', ?,
                '$.source.instruction_revision_id', ?,
                '$.source.message_id', ?,
                '$.source.turn_id', ?,
                '$.request.policy_revision', ?,
                '$.request.policy_digest', ?,
                '$.request.source_revisions_digest', ?,
                '$.request.source_revisions_json', ?,
                '$.request.authorization.principal_type', ?,
                '$.request.authorization.principal_id', ?,
                '$.request.authorization.authorization_basis', ?,
                '$.request.authorization.action', ?,
                '$.request.authorization.event_id', ?,
                '$.request.authorization.occurred_at', ?,
                '$.delivery.delivered_at', ?
             )",
    )
    .bind(&input.source_revisions_json)
    .bind(&input.handoff_id)
    .bind(&input.correlation_id)
    .bind(&project_chat_id)
    .bind(&input.project_agent_binding_id)
    .bind(&input.target_message_id)
    .bind(&input.target_turn_id)
    .bind(&input.project.id)
    .bind(&input.approval_id)
    .bind(&author_identity_id)
    .bind(input.source_profile_id.as_deref())
    .bind(input.source_instruction_revision_id.as_deref())
    .bind(input.source_message_id.as_deref())
    .bind(input.source_turn_id.as_deref())
    .bind(&input.policy_revision)
    .bind(&input.policy_digest)
    .bind(&handoff_payload_digest)
    .bind(&input.source_revisions_json)
    .bind(&input.create_principal_type)
    .bind(&input.create_principal_id)
    .bind(&input.create_authorization_basis)
    .bind(&input.create_action)
    .bind(&input.create_event_id)
    .bind(&input.create_occurred_at)
    .bind(&input.project.updated_at)
    .fetch_one(&mut *tx)
    .await
    .map_err(|error| DbError::Check(format!("Project handoff packet cannot be frozen: {error}")))?;
    sqlx::query(
        "INSERT INTO agent_handoff (
                id, source_chat_id, target_chat_id, source_message_id,
                source_turn_job_id, target_message_id,
                target_turn_job_id, author_identity_id, content, content_guard_json,
                source_revisions_json, status, correlation_id, causation_id,
                dedupe_key, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'delivered', ?, ?, ?, ?, ?)",
    )
    .bind(&input.handoff_id)
    .bind(&main_chat_id)
    .bind(&project_chat_id)
    .bind(input.source_message_id.as_deref())
    .bind(input.source_turn_id.as_deref())
    .bind(&input.target_message_id)
    .bind(&input.target_turn_id)
    .bind(Some(&author_identity_id))
    .bind(&input.handoff_content)
    .bind(&input.content_guard_json)
    .bind(&source_revisions_json)
    .bind(&input.correlation_id)
    .bind(input.causation_id.as_deref())
    .bind(&input.idempotency_key)
    .bind(&input.project.created_at)
    .bind(&input.project.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    super::super::agent_chat_topic::enqueue_native_topic_rotation_in_tx(
        &mut tx,
        Some(&main_chat_id),
        "After handoff",
        "handoff",
        &input.project.created_at,
    )
    .await?;
    sqlx::query(
        "INSERT INTO agent_chat_message (
                id, chat_id, sequence, author_type, author_id, content,
                content_guard_json, sensitivity, status, outcome, profile_id,
                correlation_id,
                causation_id, handoff_id, source_type, source_id,
                source_metadata_json, created_at
             ) VALUES (?, ?, ?, 'handoff', ?, ?, ?, 'internal', 'complete',
                       'handoff_delivered', ?, ?, ?, ?,
                       'handoff', ?, ?, ?)",
    )
    .bind(&input.target_message_id)
    .bind(&project_chat_id)
    .bind(sequence)
    .bind(&author_identity_id)
    .bind(&input.handoff_content)
    .bind(&input.content_guard_json)
    .bind(input.source_profile_id.as_deref())
    .bind(&input.correlation_id)
    .bind(input.causation_id.as_deref())
    .bind(&input.handoff_id)
    .bind(&input.handoff_id)
    .bind(
        serde_json::json!({
            "source_chat_id": main_chat_id.clone(),
            "source_identity_id": author_identity_id.clone(),
            "source_profile_id": input.source_profile_id.clone(),
        })
        .to_string(),
    )
    .bind(&input.project.created_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    super::chat_read_events::append(
        db,
        &mut tx,
        super::chat_read_events::ChatReadEvent {
            event_type: "agent_chat.message.appended",
            entity_type: "agent_chat_message",
            entity_id: &input.target_message_id,
            chat_id: &project_chat_id,
            status: Some("complete"),
            dedupe_key: Some(format!("chat-message:{}", input.target_message_id)),
            created_at: &now_rfc3339(),
        },
    )
    .await?;
    sqlx::query(
        "INSERT INTO agent_chat_turn_job (
                id, chat_id, triggering_message_id, responder_identity_id, profile_id,
                responder_binding_id, responder_binding_version, responder_identity_version,
                profile_version, operating_skill_revision_id, policy_revision, policy_digest,
                permission_policy_digest, tool_policy_digest, admission_digest,
                canonical_scope_provenance_json,
                canonical_scope_type, canonical_scope_id, status, dedupe_key,
                max_attempts, correlation_id, causation_id, causation_depth,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       'agent_chat', ?, 'queued', ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.target_turn_id)
    .bind(&project_chat_id)
    .bind(&input.target_message_id)
    .bind(&identity_id)
    .bind(&profile_id)
    .bind(&input.project_agent_binding_id)
    .bind(target_binding_version)
    .bind(target_identity_version)
    .bind(target_profile_version)
    .bind(&skill_revision_id)
    .bind(&input.policy_revision)
    .bind(&input.policy_digest)
    .bind(&target_permission_policy_digest)
    .bind(&target_tool_policy_digest)
    .bind(&admission_digest)
    .bind(&target_provenance_json)
    .bind(&project_chat_id)
    .bind(format!("handoff:{}", input.idempotency_key))
    .bind(input.max_attempts)
    .bind(&input.correlation_id)
    .bind(Some(input.handoff_id.as_str()))
    .bind(input.causation_depth.saturating_add(1))
    .bind(&input.project.created_at)
    .bind(&input.project.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    sqlx::query(
        "INSERT INTO agent_handoff_delivery (
                handoff_id, delivery_sequence, status, target_message_id,
                target_turn_job_id, created_at
             ) VALUES (?, 1, 'delivered', ?, ?, ?)",
    )
    .bind(&input.handoff_id)
    .bind(&input.target_message_id)
    .bind(&input.target_turn_id)
    .bind(&input.project.created_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;

    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.created_from_charter_approval".to_owned(),
        entity_type: "project".to_owned(),
        entity_id: input.project.id.clone(),
        actor_type: input
            .command_receipt
            .as_ref()
            .map(|receipt| receipt.principal_type.clone())
            .unwrap_or_else(|| input.create_principal_type.clone()),
        actor_id: input
            .command_receipt
            .as_ref()
            .map(|receipt| receipt.principal_id.clone())
            .or_else(|| Some(input.create_principal_id.clone())),
        scope_type: "project".to_owned(),
        scope_id: input.project.id.clone(),
        correlation_id: input
            .command_receipt
            .as_ref()
            .map(|receipt| receipt.correlation_id.clone())
            .unwrap_or_else(|| input.correlation_id.clone()),
        causation_id: input
            .command_receipt
            .as_ref()
            .map(|receipt| receipt.causation_id.clone())
            .unwrap_or_else(|| input.causation_id.clone()),
        causation_depth: input
            .command_receipt
            .as_ref()
            .map_or(input.causation_depth, |receipt| receipt.causation_depth),
        dedupe_key: Some(format!("project-charter-create:{}", input.idempotency_key)),
        payload_json: serde_json::json!({
            "project_id": input.project.id,
            "charter_id": approval.charter_id,
            "charter_revision_id": approval.revision_id,
            "approval_id": approval.id,
            "handoff_id": input.handoff_id,
            "project_chat_id": project_chat_id,
            "project_agent_binding_id": input.project_agent_binding_id,
            "authorization": {
                "principal_type": input.create_principal_type,
                "principal_id": input.create_principal_id,
                "authorization_basis": input.create_authorization_basis,
                "action": input.create_action,
                "event_id": input.create_event_id,
                "occurred_at": input.create_occurred_at,
            },
        })
        .to_string(),
        created_at: input.project.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut tx, &event).await?;

    // Main's exact Project-create command uses this same transaction for
    // its action execution and frozen receipt.  REST retries leave both
    // options unset and retain the historical composite behavior.
    let mut command_receipt = input.command_receipt.clone();
    let mut action_execution = input.action_execution.clone();
    if let Some(receipt) = command_receipt.as_mut() {
        // The DB transaction is the first point at which the generated
        // Project Chat id is authoritative. Rebuild the complete frozen
        // result here, rather than patching a transport placeholder.
        receipt.outcome_json = serde_json::json!({
            "operation": receipt.operation,
            "project_id": input.project.id,
            "project_agent_binding_id": input.project_agent_binding_id,
            "project_chat_id": project_chat_id,
            "charter_id": approval.charter_id,
            "charter_revision_id": approval.revision_id,
            "handoff_id": input.handoff_id,
            "target_message_id": input.target_message_id,
            "target_turn_id": input.target_turn_id,
        })
        .to_string();
        if let Some(execution) = action_execution.as_mut() {
            execution.result_json = Some(receipt.outcome_json.clone());
            execution.action_outcome_json = Some(receipt.outcome_json.clone());
        }
    }
    finalize_command_in_tx(db, &mut tx, &event.id, command_receipt, action_execution).await?;

    let handed_off_genesis = sqlx::query(
        "UPDATE product_genesis_session
             SET project_id = ?, handoff_id = ?, charter_id = ?, charter_revision_id = ?,
                 charter_approval_id = ?, charter_version = ?, lifecycle = 'handed_off',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND lifecycle = 'ready_for_project'",
    )
    .bind(&input.project.id)
    .bind(&input.handoff_id)
    .bind(&approval.charter_id)
    .bind(&approval.revision_id)
    .bind(&approval.id)
    .bind(approval.expected_charter_version + 1)
    .bind(&input.project.updated_at)
    .bind(&genesis_session_id)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if handed_off_genesis.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    super::super::agent_chat_topic::enqueue_native_topic_rotation_in_tx(
        &mut tx,
        Some(&main_chat_id),
        "After Project handoff",
        "project.create",
        &input.project.updated_at,
    )
    .await?;
    let consumed_approval = sqlx::query(
        "UPDATE project_charter_approval
             SET lifecycle = 'consumed', consumed_project_id = ?, consumed_at = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND lifecycle = 'active'",
    )
    .bind(&input.project.id)
    .bind(&input.project.updated_at)
    .bind(&input.project.updated_at)
    .bind(&approval.id)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if consumed_approval.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "INSERT INTO project_charter_approval_event (
                id, approval_id, lifecycle, principal_type, principal_id,
                authorization_basis, action, explicit_event, reason,
                idempotency_key, occurred_at, created_at
             ) VALUES (?, ?, 'consumed', ?, ?, ?, ?, ?, 'Project created', ?, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&approval.id)
    .bind(&input.create_principal_type)
    .bind(&input.create_principal_id)
    .bind(&input.create_authorization_basis)
    .bind(&input.create_action)
    .bind(&input.create_event_id)
    .bind(format!("{}:consumed", input.idempotency_key))
    .bind(&input.create_occurred_at)
    .bind(&input.project.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;

    let admission_receipt_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_admission_receipt (
                id, project_id, source_kind, handoff_id,
                initial_charter_approval_id, initial_charter_id,
                initial_charter_revision_id, payload_digest,
                validation_schema_version, validated_at, created_at
             ) VALUES (?, ?, 'genesis_handoff', ?, ?, ?, ?, ?,
                       'forge.project-admission/v1', ?, ?)",
    )
    .bind(&admission_receipt_id)
    .bind(&input.project.id)
    .bind(&input.handoff_id)
    .bind(&approval.id)
    .bind(&approval.charter_id)
    .bind(&approval.revision_id)
    .bind(&handoff_payload_digest)
    .bind(&input.project.updated_at)
    .bind(&input.project.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let completed_binding = sqlx::query(
        "UPDATE project_agent_binding
             SET admission_receipt_id = ?, charter_approval_id = ?,
                 charter_setup_required = 0
             WHERE id = ? AND project_id = ? AND state = 'active'
               AND charter_setup_required = 1",
    )
    .bind(&admission_receipt_id)
    .bind(&approval.id)
    .bind(&input.project_agent_binding_id)
    .bind(&input.project.id)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if completed_binding.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }

    let project_row = sqlx::query(&format!(
        "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
    ))
    .bind(&input.project.id)
    .fetch_one(&mut *tx)
    .await
    .map_err(DbError::from)?;
    let project = map_project(project_row)?;
    tx.commit().await?;
    Ok(CreatedProjectFromCharterApproval {
        project,
        project_agent_binding_id: input.project_agent_binding_id,
        project_chat_id,
        charter_id: approval.charter_id,
        charter_revision_id: approval.revision_id,
        handoff_id: input.handoff_id,
        target_message_id: input.target_message_id,
        target_turn_id: input.target_turn_id,
    })
}
