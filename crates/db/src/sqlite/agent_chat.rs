mod admission;
mod bindings;
mod queries;
mod repositories;

pub use admission::supported_main_baseline_revision;
pub(crate) use admission::{
    admission_policy_digest, handoff_admission_digest_for_provenance,
    handoff_content_digest_for_admission, validate_agent_chat_turn_job_id,
};
use admission::{
    turn_admission_semantics_match, validate_agent_chat_turn_admission,
    validate_handoff_source_responder,
};
pub use queries::{
    ActionApproverChatScopeRow, AttentionChatScopeRow, ChatTurnProtectedInteractionRow,
    ClaimFifoChatTurnInTxInput, DirectProjectChatScopeRow, ExpiredChatTurnLeaseRow,
    FailChatTurnExpiredInteractionInTxInput, FailChatTurnMissingInteractionInTxInput,
    FailChatTurnWithoutInteractionInTxInput, GenesisDraftMainChatScopeRow,
    GenesisStartLeasedMainTurnRow, InsertGenesisChatInstructionInTxInput,
    InsertProductGenesisInstructionInTxInput, MainActionChatAccountRow,
    OrchestrationBoundProjectChatScopeRow, OrchestrationMainChatScopeRow,
    ParkedChatTurnInteractionRow, ProjectTurnAdmissionHandoffRow,
    RecoverExpiredChatTurnLeaseInTxInput, TaskProposalChatScopeRow, TopicRotationHistoryMessageRow,
    WakeChatCanonicalScopeRow,
};

#[cfg(test)]
#[path = "agent_chat/tests.rs"]
mod main_baseline_revision_tests;

use super::*;
use crate::RetryAgentChatTurn;
use sha2::{Digest, Sha256};

// Insertion order distinguishes retries/continuations even when timestamps tie.
// Only a failed or cancelled turn can offer a retry, so the supersession
// probes run for those rows alone; a chat's settled history costs nothing.
pub(super) const AGENT_CHAT_TURN_WITH_RETRY_STATE: &str = "SELECT t.*, CASE
    WHEN t.status IN ('failed', 'cancelled') THEN (
    EXISTS (
        SELECT 1 FROM agent_chat_turn_job newer
        WHERE newer.chat_id = t.chat_id
          AND newer.triggering_message_id = t.triggering_message_id
          AND newer.rowid > t.rowid
    ) OR EXISTS (
        SELECT 1 FROM agent_chat_message m
        WHERE m.chat_id = t.chat_id AND m.sequence > (
            SELECT original.sequence FROM agent_chat_message original
            WHERE original.id = t.triggering_message_id
        )
    )
) ELSE 0 END AS retry_superseded FROM agent_chat_turn_job t WHERE t.id = ?";

/// Admit a chat message and queued turn using a caller-owned transaction.
/// Wake disposition persistence uses this same primitive so a turn admission,
/// its message-admitted event, the disposition, and the source-event checkpoint
/// share one commit boundary.
pub(super) async fn admit_agent_chat_turn_in_tx(
    db: &SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    input: AdmitAgentChatTurn,
) -> Result<AdmittedAgentChatTurn> {
    if input.message.chat_id != input.turn.chat_id
        || input.message.id != input.turn.triggering_message_id
    {
        return Err(DbError::Check(
            "chat turn message and job scope must match".to_owned(),
        ));
    }
    if let Some(existing) = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE dedupe_key = ?")
        .bind(&input.turn.dedupe_key)
        .fetch_optional(&mut **transaction)
        .await?
    {
        let turn = map_agent_chat_turn_job(existing)?;
        if !turn_admission_semantics_match(&input.turn, &turn) {
            return Err(DbError::IdempotencyConflict);
        }
        let message = sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
            .bind(&turn.triggering_message_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(DbError::NotFound)
            .and_then(map_agent_chat_message)?;
        return Ok(AdmittedAgentChatTurn { message, turn });
    }

    // The resolver runs before this boundary, but writers can replace the
    // binding or selected Profile between those reads and the insert.  Hold
    // the IMMEDIATE transaction lock while checking every frozen identity
    // version so a new job is never admitted from a mixed snapshot.
    validate_agent_chat_turn_admission(transaction, &input.turn).await?;

    let sequence = allocate_chat_sequence(
        transaction,
        &input.message.chat_id,
        &input.message.created_at,
    )
    .await?;
    let mut message_input = input.message.clone();
    message_input.sequence = sequence;
    let message = insert_chat_message(transaction, &message_input).await?;

    // A direct user message intentionally supersedes a parked interaction;
    // autonomous/wake/handoff messages must leave a pending user question
    // intact and cannot silently cancel it.
    if matches!(&input.message.author_type, AgentChatMessageAuthorType::User) {
        let parked_rows = sqlx::query(
            "SELECT id, pending_interaction_id, version FROM agent_chat_turn_job
             WHERE chat_id = ? AND status = 'awaiting_input'",
        )
        .bind(&input.turn.chat_id)
        .fetch_all(&mut **transaction)
        .await?;

        for row in parked_rows {
            let pid: String = row.try_get("id")?;
            let pending_interaction_id: Option<String> = row.try_get("pending_interaction_id")?;
            let pver: i64 = row.try_get("version")?;
            if let Some(iid) = pending_interaction_id {
                let _ = sqlx::query(
                    "UPDATE protected_interaction SET status = 'cancelled', version = version + 1, updated_at = ?
                     WHERE id = ? AND status = 'pending'",
                )
                .bind(&input.turn.created_at)
                .bind(&iid)
                .execute(&mut **transaction)
                .await;
            }
            let _ = sqlx::query(
                "UPDATE agent_chat_turn_job
                 SET status = 'cancelled', pending_interaction_id = NULL,
                     error_code = 'superseded_by_user_message',
                     error_message = 'superseded by newer user message', failure_class_json = NULL, retry_decision = NULL,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ? AND status = 'awaiting_input'",
            )
            .bind(&input.turn.created_at)
            .bind(&pid)
            .bind(pver)
            .execute(&mut **transaction)
            .await;
        }
    }

    sqlx::query(
        "INSERT INTO agent_chat_turn_job (
            id, chat_id, triggering_message_id, responder_identity_id, profile_id,
            responder_binding_id, responder_binding_version, responder_identity_version,
            profile_version,
            operating_skill_revision_id, policy_revision, policy_digest,
            permission_policy_digest, tool_policy_digest, admission_digest,
            canonical_scope_provenance_json,
            canonical_scope_type, canonical_scope_id, status, dedupe_key,
            max_attempts, correlation_id, causation_id, causation_depth,
            created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.turn.id)
    .bind(&input.turn.chat_id)
    .bind(&input.turn.triggering_message_id)
    .bind(&input.turn.responder_identity_id)
    .bind(&input.turn.profile_id)
    .bind(input.turn.responder_binding_id.as_deref())
    .bind(input.turn.responder_binding_version)
    .bind(input.turn.responder_identity_version)
    .bind(input.turn.profile_version)
    .bind(input.turn.operating_skill_revision_id.as_deref())
    .bind(input.turn.policy_revision.as_deref())
    .bind(input.turn.policy_digest.as_deref())
    .bind(input.turn.permission_policy_digest.as_deref())
    .bind(input.turn.tool_policy_digest.as_deref())
    .bind(input.turn.admission_digest.as_deref())
    .bind(input.turn.canonical_scope_provenance_json.as_deref())
    .bind(&input.turn.canonical_scope_type)
    .bind(&input.turn.canonical_scope_id)
    .bind(&input.turn.dedupe_key)
    .bind(input.turn.max_attempts)
    .bind(&input.turn.correlation_id)
    .bind(input.turn.causation_id.as_deref())
    .bind(input.turn.causation_depth)
    .bind(&input.turn.created_at)
    .bind(&input.turn.updated_at)
    .execute(&mut **transaction)
    .await
    .map_err(map_chat_write_error)?;
    let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
        .bind(&input.turn.id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(DbError::from)
        .and_then(map_agent_chat_turn_job)?;
    append_agent_chat_event(
        db,
        transaction,
        "agent_chat.message.admitted",
        &message,
        input.turn.correlation_id.clone(),
        input.turn.causation_id.clone(),
        input.turn.causation_depth,
    )
    .await?;
    Ok(AdmittedAgentChatTurn { message, turn })
}

#[async_trait]
impl AgentChatTransactionRepo for SqliteDb {
    async fn admit_agent_chat_turn(
        &self,
        input: AdmitAgentChatTurn,
    ) -> Result<AdmittedAgentChatTurn> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let admitted = admit_agent_chat_turn_in_tx(self, &mut transaction, input).await?;
        transaction.commit().await?;
        Ok(admitted)
    }

    async fn admit_agent_chat_turn_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: AdmitAgentChatTurn,
    ) -> Result<AdmittedAgentChatTurn> {
        admit_agent_chat_turn_in_tx(self, transaction, input).await
    }

    async fn admit_agent_chat_continuation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateAgentChatTurnJob,
    ) -> Result<AgentChatTurnJob> {
        if let Some(existing) =
            sqlx::query("SELECT * FROM agent_chat_turn_job WHERE dedupe_key = ?")
                .bind(&input.dedupe_key)
                .fetch_optional(&mut **transaction)
                .await?
        {
            let turn = map_agent_chat_turn_job(existing)?;
            if !turn_admission_semantics_match(&input, &turn) {
                return Err(DbError::IdempotencyConflict);
            }
            return Ok(turn);
        }

        let trigger_chat_id =
            sqlx::query_scalar::<_, String>("SELECT chat_id FROM agent_chat_message WHERE id = ?")
                .bind(&input.triggering_message_id)
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or(DbError::NotFound)?;
        if trigger_chat_id != input.chat_id {
            return Err(DbError::Check(
                "chat continuation trigger does not belong to the canonical chat".to_owned(),
            ));
        }

        validate_agent_chat_turn_admission(transaction, &input).await?;
        sqlx::query(
            "INSERT INTO agent_chat_turn_job (
                id, chat_id, triggering_message_id, responder_identity_id, profile_id,
                responder_binding_id, responder_binding_version, responder_identity_version,
                profile_version, operating_skill_revision_id, policy_revision, policy_digest,
                permission_policy_digest, tool_policy_digest, admission_digest,
                canonical_scope_provenance_json, canonical_scope_type, canonical_scope_id,
                status, dedupe_key, max_attempts, correlation_id, causation_id,
                causation_depth, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       'queued', ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.chat_id)
        .bind(&input.triggering_message_id)
        .bind(&input.responder_identity_id)
        .bind(&input.profile_id)
        .bind(input.responder_binding_id.as_deref())
        .bind(input.responder_binding_version)
        .bind(input.responder_identity_version)
        .bind(input.profile_version)
        .bind(input.operating_skill_revision_id.as_deref())
        .bind(input.policy_revision.as_deref())
        .bind(input.policy_digest.as_deref())
        .bind(input.permission_policy_digest.as_deref())
        .bind(input.tool_policy_digest.as_deref())
        .bind(input.admission_digest.as_deref())
        .bind(input.canonical_scope_provenance_json.as_deref())
        .bind(&input.canonical_scope_type)
        .bind(&input.canonical_scope_id)
        .bind(&input.dedupe_key)
        .bind(input.max_attempts)
        .bind(&input.correlation_id)
        .bind(input.causation_id.as_deref())
        .bind(input.causation_depth)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(map_chat_write_error)?;

        sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)
    }

    async fn complete_agent_chat_turn(
        &self,
        input: CompleteAgentChatTurn,
    ) -> Result<CompletedAgentChatTurn> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let current_row = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let current = map_agent_chat_turn_job(current_row)?;
        if current.status == AgentChatTurnState::Succeeded {
            let response_id = current
                .response_message_id
                .clone()
                .ok_or(DbError::NotFound)?;
            let response = sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
                .bind(response_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(DbError::from)
                .and_then(map_agent_chat_message)?;
            transaction.commit().await?;
            return Ok(CompletedAgentChatTurn {
                response,
                turn: current,
            });
        }
        if current.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        if current.status != AgentChatTurnState::Leased
            || current.lease_owner.as_deref() != Some(input.lease_owner.as_str())
        {
            return Err(DbError::VersionConflict);
        }
        if matches!(
            current.status,
            AgentChatTurnState::Failed | AgentChatTurnState::Cancelled
        ) {
            return Err(DbError::InvalidTransition);
        }
        if input.response.chat_id != current.chat_id
            || input.response.id == current.triggering_message_id
        {
            return Err(DbError::Check(
                "response message must belong to turn chat and differ from trigger".to_owned(),
            ));
        }
        if let Some(existing) = sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
            .bind(&input.response.id)
            .fetch_optional(&mut *transaction)
            .await?
        {
            let response = map_agent_chat_message(existing)?;
            if response.chat_id != current.chat_id {
                return Err(DbError::Check("response message chat mismatch".to_owned()));
            }
            let updated = sqlx::query(
                "UPDATE agent_chat_turn_job
                 SET status = 'succeeded', response_message_id = ?,
                     lease_owner = NULL, leased_until = NULL,
                     next_attempt_at = NULL, error_code = NULL,
                     error_message = NULL, failure_class_json = NULL, retry_decision = NULL,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ?
                   AND status = 'leased' AND lease_owner = ?",
            )
            .bind(&response.id)
            .bind(&input.updated_at)
            .bind(&input.turn_job_id)
            .bind(input.expected_version)
            .bind(&input.lease_owner)
            .execute(&mut *transaction)
            .await?;
            if updated.rows_affected() == 0 {
                return Err(DbError::VersionConflict);
            }
            self.append_agent_chat_turn_status_in_tx(&mut transaction, &input.turn_job_id)
                .await?;
            let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
                .bind(&input.turn_job_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(DbError::from)
                .and_then(map_agent_chat_turn_job)?;
            transaction.commit().await?;
            return Ok(CompletedAgentChatTurn { response, turn });
        }

        let sequence = allocate_chat_sequence(
            &mut transaction,
            &current.chat_id,
            &input.response.created_at,
        )
        .await?;
        let mut response_input = input.response.clone();
        response_input.sequence = sequence;
        let response = insert_chat_message(&mut transaction, &response_input).await?;
        let updated = sqlx::query(
            "UPDATE agent_chat_turn_job
             SET status = 'succeeded', response_message_id = ?,
                 lease_owner = NULL, leased_until = NULL,
                 next_attempt_at = NULL, error_code = NULL,
                 error_message = NULL, failure_class_json = NULL, retry_decision = NULL,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?
               AND status = 'leased' AND lease_owner = ?",
        )
        .bind(&response.id)
        .bind(&input.updated_at)
        .bind(&input.turn_job_id)
        .bind(input.expected_version)
        .bind(&input.lease_owner)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        append_agent_chat_event(
            self,
            &mut transaction,
            "agent_chat.response.completed",
            &response,
            current.correlation_id.clone(),
            current.causation_id.clone(),
            current.causation_depth,
        )
        .await?;
        let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)?;
        transaction.commit().await?;
        Ok(CompletedAgentChatTurn { response, turn })
    }

    async fn complete_agent_chat_turn_with_usage(
        &self,
        input: CompleteAgentChatTurnWithUsage,
    ) -> Result<CompletedAgentChatTurn> {
        super::chat_ledger::complete_agent_chat_turn_with_usage(self, input).await
    }

    async fn complete_agent_chat_control_transfer(
        &self,
        input: CompleteAgentChatControlTransfer,
    ) -> Result<AgentChatTurnJob> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let current_row = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let current = map_agent_chat_turn_job(current_row)?;
        if current.status == AgentChatTurnState::Succeeded && current.response_message_id.is_none()
        {
            transaction.commit().await?;
            return Ok(current);
        }
        if current.version != input.expected_version
            || current.status != AgentChatTurnState::Leased
            || current.lease_owner.as_deref() != Some(input.lease_owner.as_str())
        {
            return Err(DbError::VersionConflict);
        }

        let outcome_json = sqlx::query_scalar::<_, String>(
            "SELECT outcome_json FROM command_receipt
             WHERE id = ? AND operation = 'genesis.start'",
        )
        .bind(&input.command_receipt_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let outcome: serde_json::Value = serde_json::from_str(&outcome_json)
            .map_err(|_| DbError::Check("Genesis start receipt outcome is invalid".to_owned()))?;
        if outcome
            .get("source_turn_id")
            .and_then(serde_json::Value::as_str)
            != Some(current.id.as_str())
            || outcome
                .get("admitted_turn_id")
                .and_then(serde_json::Value::as_str)
                != Some(input.continuation_turn_id.as_str())
            || outcome
                .get("session_id")
                .and_then(serde_json::Value::as_str)
                != Some(input.genesis_session_id.as_str())
            || outcome
                .get("control_transfer")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
        {
            return Err(DbError::Check(
                "Genesis start receipt does not authorize this turn control transfer".to_owned(),
            ));
        }
        let continuation_valid: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM agent_chat_turn_job
             WHERE id = ? AND chat_id = ? AND triggering_message_id = ?
               AND status IN ('queued', 'retry_wait', 'leased', 'awaiting_input', 'succeeded')",
        )
        .bind(&input.continuation_turn_id)
        .bind(&current.chat_id)
        .bind(&current.triggering_message_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if continuation_valid.is_none() {
            return Err(DbError::Check(
                "Genesis control transfer continuation is unavailable".to_owned(),
            ));
        }

        let updated = sqlx::query(
            "UPDATE agent_chat_turn_job
             SET status = 'succeeded', response_message_id = NULL,
                 lease_owner = NULL, leased_until = NULL, next_attempt_at = NULL,
                 error_code = NULL, error_message = NULL, failure_class_json = NULL, retry_decision = NULL,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND status = 'leased' AND lease_owner = ?",
        )
        .bind(&input.updated_at)
        .bind(&input.turn_job_id)
        .bind(input.expected_version)
        .bind(&input.lease_owner)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        self.append_agent_chat_turn_status_in_tx(&mut transaction, &input.turn_job_id)
            .await?;

        DomainEventRepo::append_event_in_tx(
            self,
            &mut transaction,
            &CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "agent_chat.turn.control_transferred".to_owned(),
                entity_type: "agent_chat_turn_job".to_owned(),
                entity_id: current.id.clone(),
                actor_type: "system".to_owned(),
                actor_id: None,
                scope_type: "agent_chat".to_owned(),
                scope_id: current.chat_id.clone(),
                correlation_id: current.correlation_id.clone(),
                causation_id: Some(input.command_receipt_id.clone()),
                causation_depth: current.causation_depth.saturating_add(1).min(16),
                dedupe_key: Some(format!("agent-chat-control-transfer:{}", current.id)),
                payload_json: serde_json::json!({
                    "operation": "genesis.start",
                    "source_turn_id": current.id,
                    "continuation_turn_id": input.continuation_turn_id,
                    "genesis_session_id": input.genesis_session_id,
                    "command_receipt_id": input.command_receipt_id,
                    "response_message_committed": false,
                })
                .to_string(),
                created_at: input.updated_at.clone(),
            },
        )
        .await?;
        let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)?;
        transaction.commit().await?;
        Ok(turn)
    }

    async fn complete_agent_chat_control_transfer_with_usage(
        &self,
        input: CompleteAgentChatControlTransferWithUsage,
    ) -> Result<AgentChatTurnJob> {
        super::chat_ledger::complete_agent_chat_control_transfer_with_usage(self, input).await
    }

    async fn fail_agent_chat_turn(&self, input: FailAgentChatTurn) -> Result<AgentChatTurnJob> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let error_code = bounded_event_text(&input.error_code, 128);
        let error_message = bounded_event_text(&input.error_message, 2048);
        let updated = sqlx::query(
            "UPDATE agent_chat_turn_job
             SET status = ?, lease_owner = NULL, leased_until = NULL,
                 attempt_count = ?, next_attempt_at = ?, error_code = ?,
                 error_message = ?, failure_class_json = ?, retry_decision = ?,
                 pre_provider_failure_count = ?, usage_limit_deferral_count = ?, usage_limit_first_deferred_at = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND status = 'leased' AND lease_owner = ?",
        )
        .bind(input.status.to_string())
        .bind(input.attempt_count)
        .bind(input.next_attempt_at.as_deref())
        .bind(&error_code)
        .bind(&error_message)
        .bind(
            serde_json::to_string(&input.failure_class)
                .map_err(|error| DbError::Check(error.to_string()))?,
        )
        .bind(input.retry_decision.as_str())
        .bind(input.pre_provider_failure_count)
        .bind(input.usage_limit_deferral_count)
        .bind(&input.usage_limit_first_deferred_at)
        .bind(&input.updated_at)
        .bind(&input.turn_job_id)
        .bind(input.expected_version)
        .bind(&input.lease_owner)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        let turn = sqlx::query(AGENT_CHAT_TURN_WITH_RETRY_STATE)
            .bind(&input.turn_job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)?;
        append_agent_chat_turn_failure_event(self, &mut transaction, &turn, &input).await?;
        transaction.commit().await?;
        Ok(turn)
    }

    async fn fail_agent_chat_turn_with_usage(
        &self,
        input: FailAgentChatTurnWithUsage,
    ) -> Result<AgentChatTurnJob> {
        super::chat_ledger::fail_agent_chat_turn_with_usage(self, input).await
    }

    async fn park_agent_chat_turn(&self, input: ParkAgentChatTurn) -> Result<AgentChatTurnJob> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let updated = sqlx::query(
            "UPDATE agent_chat_turn_job
             SET status = 'awaiting_input', pending_interaction_id = ?,
                 lease_owner = NULL, leased_until = NULL,
                 attempt_count = MAX(0, attempt_count - 1),
                 next_attempt_at = NULL, error_code = NULL,
                 error_message = NULL, failure_class_json = NULL, retry_decision = NULL, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND status = 'leased' AND lease_owner = ?",
        )
        .bind(&input.pending_interaction_id)
        .bind(&input.updated_at)
        .bind(&input.turn_job_id)
        .bind(input.expected_version)
        .bind(&input.lease_owner)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        self.append_agent_chat_turn_status_in_tx(&mut transaction, &input.turn_job_id)
            .await?;
        let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)?;
        append_agent_chat_turn_awaiting_event(self, &mut transaction, &turn).await?;
        transaction.commit().await?;
        Ok(turn)
    }

    async fn park_agent_chat_turn_with_usage(
        &self,
        input: ParkAgentChatTurnWithUsage,
    ) -> Result<AgentChatTurnJob> {
        super::chat_ledger::park_agent_chat_turn_with_usage(self, input).await
    }

    async fn retry_agent_chat_turn(&self, input: RetryAgentChatTurn) -> Result<AgentChatTurnJob> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let key = format!(
            "agent-chat-turn-retry:{}:{}",
            input.turn_job_id, input.idempotency_key
        );
        if let Some(result_id) = sqlx::query_scalar::<_, String>(
            "SELECT entity_id FROM domain_event WHERE dedupe_key = ?",
        )
        .bind(&key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let job = sqlx::query(AGENT_CHAT_TURN_WITH_RETRY_STATE)
                .bind(&result_id)
                .fetch_one(&mut *transaction)
                .await?;
            let job = map_agent_chat_turn_job(job)?;
            transaction.commit().await?;
            return Ok(job);
        }
        let current = sqlx::query(AGENT_CHAT_TURN_WITH_RETRY_STATE)
            .bind(&input.turn_job_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)
            .and_then(map_agent_chat_turn_job)?;
        if current.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        if current.retry_action().is_none() {
            return Err(DbError::TurnNotRetryable);
        }
        let busy: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_chat_turn_job WHERE chat_id = ?
             AND status IN ('queued', 'leased', 'retry_wait', 'awaiting_input')",
        )
        .bind(&current.chat_id)
        .fetch_one(&mut *transaction)
        .await?;
        if busy > 0 {
            return Err(DbError::ChatTurnLive);
        }
        let updated = sqlx::query("UPDATE agent_chat_turn_job SET version = version + 1, updated_at = ? WHERE id = ? AND version = ?")
            .bind(&input.updated_at).bind(&current.id).bind(input.expected_version)
            .execute(&mut *transaction).await?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let turn = AgentChatTransactionRepo::admit_agent_chat_continuation_in_tx(
            self,
            &mut transaction,
            input.new_turn,
        )
        .await?;
        DomainEventRepo::append_event_in_tx(
            self,
            &mut transaction,
            &CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "agent_chat.turn.retried".to_owned(),
                entity_type: "agent_chat_turn_job".to_owned(),
                entity_id: turn.id.clone(),
                actor_type: "user".to_owned(),
                actor_id: Some(input.actor_user_id),
                scope_type: "agent_chat".to_owned(),
                scope_id: turn.chat_id.clone(),
                correlation_id: turn.correlation_id.clone(),
                causation_id: turn.causation_id.clone(),
                causation_depth: turn.causation_depth,
                dedupe_key: Some(key),
                payload_json: serde_json::json!({
                    "turn_job_id": turn.id,
                    "chat_id": turn.chat_id,
                    "version": turn.version,
                    "status": "queued",
                    "source_turn_job_id": current.id,
                })
                .to_string(),
                created_at: input.updated_at.clone(),
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(turn)
    }

    async fn cancel_agent_chat_turn(&self, input: CancelAgentChatTurn) -> Result<AgentChatTurnJob> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let dedupe_key = format!(
            "agent-chat-turn-cancel:{}:{}",
            input.turn_job_id, input.idempotency_key
        );

        // Cancellation idempotency is durable in the same ledger that records
        // the state transition.  A replay returns the terminal job without
        // rechecking the caller's old optimistic version or appending another
        // event.
        if let Some(existing_entity_id) = sqlx::query_scalar::<_, String>(
            "SELECT entity_id FROM domain_event WHERE dedupe_key = ?",
        )
        .bind(&dedupe_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            if existing_entity_id != input.turn_job_id {
                return Err(DbError::Check(
                    "turn cancellation idempotency key belongs to another turn".to_owned(),
                ));
            }
            let turn = sqlx::query(AGENT_CHAT_TURN_WITH_RETRY_STATE)
                .bind(&input.turn_job_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(DbError::from)
                .and_then(map_agent_chat_turn_job)?;
            transaction.commit().await?;
            return Ok(turn);
        }

        let current = sqlx::query(AGENT_CHAT_TURN_WITH_RETRY_STATE)
            .bind(&input.turn_job_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)
            .and_then(map_agent_chat_turn_job)?;
        if current.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        if !matches!(
            current.status,
            AgentChatTurnState::Queued
                | AgentChatTurnState::Leased
                | AgentChatTurnState::RetryWait
                | AgentChatTurnState::AwaitingInput
        ) {
            return Err(DbError::VersionConflict);
        }

        let updated = sqlx::query(
            "UPDATE agent_chat_turn_job
             SET status = 'cancelled', lease_owner = NULL, leased_until = NULL,
                 next_attempt_at = NULL, error_code = 'cancelled_by_user',
                 error_message = 'cancelled by user', failure_class_json = NULL, retry_decision = NULL, version = version + 1,
                 updated_at = ?
             WHERE id = ? AND version = ?
               AND status IN ('queued', 'leased', 'retry_wait', 'awaiting_input')",
        )
        .bind(&input.updated_at)
        .bind(&input.turn_job_id)
        .bind(input.expected_version)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }

        let turn = sqlx::query(AGENT_CHAT_TURN_WITH_RETRY_STATE)
            .bind(&input.turn_job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)?;
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "agent_chat.turn.cancelled".to_owned(),
            entity_type: "agent_chat_turn_job".to_owned(),
            entity_id: turn.id.clone(),
            actor_type: "user".to_owned(),
            actor_id: Some(input.actor_user_id),
            scope_type: "agent_chat".to_owned(),
            scope_id: turn.chat_id.clone(),
            correlation_id: turn.correlation_id.clone(),
            causation_id: turn.causation_id.clone(),
            causation_depth: turn.causation_depth,
            dedupe_key: Some(dedupe_key),
            payload_json: serde_json::json!({
                "turn_job_id": turn.id,
                "chat_id": turn.chat_id,
                "status": turn.status.to_string(),
                "version": turn.version,
            })
            .to_string(),
            created_at: input.updated_at,
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        transaction.commit().await?;
        Ok(turn)
    }

    async fn cancel_agent_chat_turn_with_usage(
        &self,
        input: CancelAgentChatTurnWithUsage,
    ) -> Result<AgentChatTurnJob> {
        super::chat_ledger::cancel_agent_chat_turn_with_usage(self, input).await
    }

    async fn admit_agent_handoff(&self, input: AdmitAgentHandoff) -> Result<AdmittedAgentHandoff> {
        if input.handoff.source_chat_id == input.handoff.target_chat_id
            || input.target_message.chat_id != input.handoff.target_chat_id
            || input.target_turn.chat_id != input.handoff.target_chat_id
            || input.target_turn.triggering_message_id != input.target_message.id
        {
            return Err(DbError::Check(
                "handoff source/target and turn scope must match".to_owned(),
            ));
        }
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        if let Some(existing_row) = sqlx::query("SELECT * FROM agent_handoff WHERE dedupe_key = ?")
            .bind(&input.handoff.dedupe_key)
            .fetch_optional(&mut *transaction)
            .await?
        {
            let handoff = map_agent_handoff(existing_row)?;
            let target_message_id = handoff.target_message_id.clone().ok_or(DbError::NotFound)?;
            let target_turn_id = handoff
                .target_turn_job_id
                .clone()
                .ok_or(DbError::NotFound)?;
            let message = sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
                .bind(target_message_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(DbError::from)
                .and_then(map_agent_chat_message)?;
            let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
                .bind(target_turn_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(DbError::from)
                .and_then(map_agent_chat_turn_job)?;
            if !turn_admission_semantics_match(&input.target_turn, &turn) {
                return Err(DbError::IdempotencyConflict);
            }
            transaction.commit().await?;
            return Ok(AdmittedAgentHandoff {
                handoff,
                message,
                turn,
            });
        }

        if let Some(source_provenance) = input.source_responder_provenance_json.as_deref() {
            validate_handoff_source_responder(
                &mut transaction,
                &input.handoff.source_chat_id,
                source_provenance,
                input.handoff.author_identity_id.as_deref(),
            )
            .await?;
        }
        validate_agent_chat_turn_admission(&mut transaction, &input.target_turn).await?;

        let sequence = allocate_chat_sequence(
            &mut transaction,
            &input.handoff.target_chat_id,
            &input.target_message.created_at,
        )
        .await?;
        let mut target_message_input = input.target_message.clone();
        target_message_input.sequence = sequence;
        target_message_input.handoff_id = Some(input.handoff.id.clone());
        let handoff = sqlx::query(
            "INSERT INTO agent_handoff (
                id, source_chat_id, target_chat_id, source_message_id,
                source_turn_job_id, target_message_id, target_turn_job_id,
                author_identity_id, content, content_guard_json, source_revisions_json,
                status, correlation_id, causation_id, dedupe_key, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'delivered', ?, ?, ?, ?, ?)",
        )
        .bind(&input.handoff.id)
        .bind(&input.handoff.source_chat_id)
        .bind(&input.handoff.target_chat_id)
        .bind(input.handoff.source_message_id.as_deref())
        .bind(input.handoff.source_turn_job_id.as_deref())
        .bind(&target_message_input.id)
        .bind(&input.target_turn.id)
        .bind(input.handoff.author_identity_id.as_deref())
        .bind(&input.handoff.content)
        .bind(&input.handoff.content_guard_json)
        .bind(&input.handoff.source_revisions_json)
        .bind(&input.handoff.correlation_id)
        .bind(input.handoff.causation_id.as_deref())
        .bind(&input.handoff.dedupe_key)
        .bind(&input.handoff.created_at)
        .bind(&input.handoff.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_chat_write_error)?;
        if handoff.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        super::agent_chat_topic::enqueue_native_topic_rotation_in_tx(
            &mut transaction,
            Some(&input.handoff.source_chat_id),
            "After handoff",
            "handoff",
            &input.handoff.created_at,
        )
        .await?;
        let message = insert_chat_message(&mut transaction, &target_message_input).await?;
        sqlx::query(
            "INSERT INTO agent_chat_turn_job (
                id, chat_id, triggering_message_id, responder_identity_id, profile_id,
                responder_binding_id, responder_binding_version, responder_identity_version,
                profile_version,
                operating_skill_revision_id, policy_revision, policy_digest,
                permission_policy_digest, tool_policy_digest, admission_digest,
                canonical_scope_provenance_json,
                canonical_scope_type, canonical_scope_id, status, dedupe_key,
                max_attempts, correlation_id, causation_id, causation_depth,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.target_turn.id)
        .bind(&input.target_turn.chat_id)
        .bind(&input.target_turn.triggering_message_id)
        .bind(&input.target_turn.responder_identity_id)
        .bind(&input.target_turn.profile_id)
        .bind(input.target_turn.responder_binding_id.as_deref())
        .bind(input.target_turn.responder_binding_version)
        .bind(input.target_turn.responder_identity_version)
        .bind(input.target_turn.profile_version)
        .bind(input.target_turn.operating_skill_revision_id.as_deref())
        .bind(input.target_turn.policy_revision.as_deref())
        .bind(input.target_turn.policy_digest.as_deref())
        .bind(input.target_turn.permission_policy_digest.as_deref())
        .bind(input.target_turn.tool_policy_digest.as_deref())
        .bind(input.target_turn.admission_digest.as_deref())
        .bind(input.target_turn.canonical_scope_provenance_json.as_deref())
        .bind(&input.target_turn.canonical_scope_type)
        .bind(&input.target_turn.canonical_scope_id)
        .bind(&input.target_turn.dedupe_key)
        .bind(input.target_turn.max_attempts)
        .bind(&input.target_turn.correlation_id)
        .bind(input.target_turn.causation_id.as_deref())
        .bind(input.target_turn.causation_depth)
        .bind(&input.target_turn.created_at)
        .bind(&input.target_turn.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_chat_write_error)?;
        sqlx::query(
            "INSERT INTO agent_handoff_delivery (
                handoff_id, delivery_sequence, status, target_message_id,
                target_turn_job_id, created_at
             ) VALUES (?, 1, 'delivered', ?, ?, ?)",
        )
        .bind(&input.handoff.id)
        .bind(&message.id)
        .bind(&input.target_turn.id)
        .bind(&input.handoff.updated_at)
        .execute(&mut *transaction)
        .await?;
        append_agent_chat_event(
            self,
            &mut transaction,
            "agent_chat.message.admitted",
            &message,
            input.handoff.correlation_id.clone(),
            input.handoff.causation_id.clone(),
            input.target_turn.causation_depth,
        )
        .await?;
        let handoff = sqlx::query("SELECT * FROM agent_handoff WHERE id = ?")
            .bind(&input.handoff.id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_handoff)?;
        let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.target_turn.id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)?;
        transaction.commit().await?;
        Ok(AdmittedAgentHandoff {
            handoff,
            message,
            turn,
        })
    }
}

pub(super) async fn append_agent_chat_event(
    db: &SqliteDb,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event_type: &str,
    message: &AgentChatMessage,
    correlation_id: String,
    causation_id: Option<String>,
    causation_depth: i64,
) -> Result<DomainEvent> {
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: event_type.to_owned(),
        entity_type: "agent_chat_message".to_owned(),
        entity_id: message.id.clone(),
        actor_type: message.author_type.to_string(),
        actor_id: message.author_id.clone(),
        scope_type: "agent_chat".to_owned(),
        scope_id: message.chat_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!("agent-chat-event:{event_type}:{}", message.id)),
        payload_json: serde_json::json!({
            "message_id": message.id,
            "chat_id": message.chat_id,
            "sequence": message.sequence,
            "source_type": message.source_type,
        })
        .to_string(),
        created_at: message.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, transaction, &event).await
}

pub(super) async fn append_agent_chat_turn_failure_event(
    db: &SqliteDb,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    turn: &AgentChatTurnJob,
    input: &FailAgentChatTurn,
) -> Result<DomainEvent> {
    // Error details are operational metadata, not a transcript. Keep the
    // event useful for Attention/recovery while preventing adapter output or
    // protected content from becoming an unbounded durable payload.
    let error_code = bounded_event_text(&input.error_code, 128);
    let error_message = bounded_event_text(&input.error_message, 512);
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "agent_chat.turn.failed".to_owned(),
        entity_type: "agent_chat_turn_job".to_owned(),
        entity_id: turn.id.clone(),
        actor_type: "system".to_owned(),
        actor_id: None,
        scope_type: "agent_chat".to_owned(),
        scope_id: turn.chat_id.clone(),
        correlation_id: turn.correlation_id.clone(),
        causation_id: turn.causation_id.clone(),
        causation_depth: turn.causation_depth,
        dedupe_key: Some(format!(
            "agent-chat-event:agent_chat.turn.failed:{}:{}",
            turn.id, input.expected_version
        )),
        payload_json: serde_json::json!({
            "turn_job_id": turn.id,
            "chat_id": turn.chat_id,
            "responder_identity_id": turn.responder_identity_id,
            "status": turn.status.to_string(),
            "attempt_count": turn.attempt_count,
            "max_attempts": turn.max_attempts,
            "error_code": error_code,
            "error_message": error_message,
            "failure_class": turn.failure_class,
            "retry_decision": turn.retry_decision,
            "pre_provider_failure_count": turn.pre_provider_failure_count,
            "retry_action": turn.retry_action(),
            "next_attempt_at": turn.next_attempt_at,
            "version": turn.version,
        })
        .to_string(),
        created_at: input.updated_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, transaction, &event).await
}

pub(super) async fn append_agent_chat_turn_awaiting_event(
    db: &SqliteDb,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    turn: &AgentChatTurnJob,
) -> Result<DomainEvent> {
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "agent_chat.turn.awaiting_input".to_owned(),
        entity_type: "agent_chat_turn_job".to_owned(),
        entity_id: turn.id.clone(),
        actor_type: "system".to_owned(),
        actor_id: None,
        scope_type: "agent_chat".to_owned(),
        scope_id: turn.chat_id.clone(),
        correlation_id: turn.correlation_id.clone(),
        causation_id: turn.causation_id.clone(),
        causation_depth: turn.causation_depth,
        dedupe_key: Some(format!(
            "agent-chat-event:agent_chat.turn.awaiting_input:{}:{}",
            turn.id, turn.version
        )),
        payload_json: serde_json::json!({
            "turn_job_id": turn.id,
            "chat_id": turn.chat_id,
            "responder_identity_id": turn.responder_identity_id,
            "status": turn.status.to_string(),
            "pending_interaction_id": turn.pending_interaction_id,
            "attempt_count": turn.attempt_count,
            "max_attempts": turn.max_attempts,
            "version": turn.version,
        })
        .to_string(),
        created_at: turn.updated_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, transaction, &event).await
}

pub(super) fn bounded_event_text(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

pub(super) async fn allocate_chat_sequence(
    transaction: &mut Transaction<'_, Sqlite>,
    chat_id: &str,
    timestamp: &str,
) -> Result<i64> {
    let count = sqlx::query_scalar::<_, i64>(
        "UPDATE agent_chat
         SET message_count = message_count + 1,
             last_message_at = CASE
                 WHEN last_message_at IS NULL OR last_message_at < ? THEN ?
                 ELSE last_message_at END,
             version = version + 1, updated_at = ?
         WHERE id = ?
         RETURNING message_count",
    )
    .bind(timestamp)
    .bind(timestamp)
    .bind(timestamp)
    .bind(chat_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    Ok(count - 1)
}

pub(super) async fn insert_chat_message(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &CreateAgentChatMessage,
) -> Result<AgentChatMessage> {
    sqlx::query(
        "INSERT INTO agent_chat_message (
            id, chat_id, sequence, author_type, author_id, content,
            content_guard_json, sensitivity, status, outcome, model, profile_id,
            session_id, context_manifest_id, token_usage_json, duration_ms, error,
            correlation_id, causation_id, handoff_id, source_type, source_id,
            source_message_id, source_room_id, source_conversation_id,
            source_sequence, source_metadata_json, created_at
             ) VALUES (
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?
             )",
    )
    .bind(&input.id)
    .bind(&input.chat_id)
    .bind(input.sequence)
    .bind(input.author_type.to_string())
    .bind(input.author_id.as_deref())
    .bind(&input.content)
    .bind(&input.content_guard_json)
    .bind(&input.sensitivity)
    .bind(input.status.to_string())
    .bind(input.outcome.as_deref())
    .bind(input.model.as_deref())
    .bind(input.profile_id.as_deref())
    .bind(input.session_id.as_deref())
    .bind(input.context_manifest_id.as_deref())
    .bind(input.token_usage_json.as_deref())
    .bind(input.duration_ms)
    .bind(input.error.as_deref())
    .bind(&input.correlation_id)
    .bind(input.causation_id.as_deref())
    .bind(input.handoff_id.as_deref())
    .bind(&input.source_type)
    .bind(input.source_id.as_deref())
    .bind(input.source_message_id.as_deref())
    .bind(input.source_room_id.as_deref())
    .bind(input.source_conversation_id.as_deref())
    .bind(input.source_sequence)
    .bind(&input.source_metadata_json)
    .bind(&input.created_at)
    .execute(&mut **transaction)
    .await
    .map_err(map_chat_write_error)?;
    super::agent_chat_topic::enqueue_idle_topic_rotation_in_tx(
        transaction,
        &input.id,
        &input.chat_id,
        &input.author_type.to_string(),
        &input.created_at,
    )
    .await?;
    sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(DbError::from)
        .and_then(map_agent_chat_message)
}

fn map_account_main_binding(row: SqliteRow) -> Result<AccountMainAgentBinding> {
    Ok(AccountMainAgentBinding {
        id: row.try_get("id")?,
        account_id: row.try_get("account_id")?,
        identity_id: row.try_get("identity_id")?,
        profile_id: row.try_get("profile_id")?,
        state: row.try_get("state")?,
        autonomy_policy_json: row.try_get("autonomy_policy_json")?,
        tool_policy_revision: row.try_get("tool_policy_revision")?,
        version: row.try_get("version")?,
        replaced_by_binding_id: row.try_get("replaced_by_binding_id")?,
        replacement_reason: row.try_get("replacement_reason")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_project_agent_binding(row: SqliteRow) -> Result<ProjectAgentBinding> {
    Ok(ProjectAgentBinding {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        identity_id: row.try_get("identity_id")?,
        profile_id: row.try_get("profile_id")?,
        state: row.try_get("state")?,
        autonomy_policy_json: row.try_get("autonomy_policy_json")?,
        permission_ceiling_json: row.try_get("permission_ceiling_json")?,
        subscriptions_json: row.try_get("subscriptions_json")?,
        wake_budget: row.try_get("wake_budget")?,
        operating_skill_revision_id: row.try_get("operating_skill_revision_id")?,
        policy_revision: row.try_get("policy_revision")?,
        policy_digest: row.try_get("policy_digest")?,
        charter_id: row.try_get("charter_id")?,
        charter_revision_id: row.try_get("charter_revision_id")?,
        charter_setup_required: row.try_get::<i64, _>("charter_setup_required")? != 0,
        admission_receipt_id: row.try_get("admission_receipt_id")?,
        charter_approval_id: row.try_get("charter_approval_id")?,
        version: row.try_get("version")?,
        replaced_by_binding_id: row.try_get("replaced_by_binding_id")?,
        replacement_reason: row.try_get("replacement_reason")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_project_admission_receipt(row: SqliteRow) -> Result<ProjectAdmissionReceipt> {
    Ok(ProjectAdmissionReceipt {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        source_kind: row.try_get("source_kind")?,
        handoff_id: row.try_get("handoff_id")?,
        initial_charter_approval_id: row.try_get("initial_charter_approval_id")?,
        initial_charter_id: row.try_get("initial_charter_id")?,
        initial_charter_revision_id: row.try_get("initial_charter_revision_id")?,
        payload_digest: row.try_get("payload_digest")?,
        validation_schema_version: row.try_get("validation_schema_version")?,
        validated_at: row.try_get("validated_at")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_agent_chat(row: SqliteRow) -> Result<AgentChat> {
    Ok(AgentChat {
        id: row.try_get("id")?,
        kind: row.try_get("kind")?,
        account_id: row.try_get("account_id")?,
        project_id: row.try_get("project_id")?,
        status: row.try_get("status")?,
        instruction_revision: row.try_get("instruction_revision")?,
        message_count: row.try_get("message_count")?,
        last_message_at: row.try_get("last_message_at")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_agent_chat_source_ref(row: SqliteRow) -> Result<AgentChatSourceRef> {
    Ok(AgentChatSourceRef {
        chat_id: row.try_get("chat_id")?,
        source_type: row.try_get("source_type")?,
        source_id: row.try_get("source_id")?,
        source_scope_type: row.try_get("source_scope_type")?,
        source_scope_id: row.try_get("source_scope_id")?,
        source_revision: row.try_get("source_revision")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_agent_chat_instruction(row: SqliteRow) -> Result<AgentChatInstructionRevision> {
    Ok(AgentChatInstructionRevision {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        source_type: row.try_get("source_type")?,
        source_id: row.try_get("source_id")?,
        revision: row.try_get("revision")?,
        body: row.try_get("body")?,
        content_guard_json: row.try_get("content_guard_json")?,
        sensitivity: row.try_get("sensitivity")?,
        created_by_type: row.try_get("created_by_type")?,
        created_by_id: row.try_get("created_by_id")?,
        created_at: row.try_get("created_at")?,
    })
}

pub(super) fn map_agent_chat_message(row: SqliteRow) -> Result<AgentChatMessage> {
    Ok(AgentChatMessage {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        sequence: row.try_get("sequence")?,
        author_type: parse_enum(row.try_get::<String, _>("author_type")?)?,
        author_id: row.try_get("author_id")?,
        content: row.try_get("content")?,
        content_guard_json: row.try_get("content_guard_json")?,
        sensitivity: row.try_get("sensitivity")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        outcome: row.try_get("outcome")?,
        model: row.try_get("model")?,
        profile_id: row.try_get("profile_id")?,
        session_id: row.try_get("session_id")?,
        context_manifest_id: row.try_get("context_manifest_id")?,
        token_usage_json: row.try_get("token_usage_json")?,
        duration_ms: row.try_get("duration_ms")?,
        error: row.try_get("error")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        handoff_id: row.try_get("handoff_id")?,
        source_type: row.try_get("source_type")?,
        source_id: row.try_get("source_id")?,
        source_message_id: row.try_get("source_message_id")?,
        source_room_id: row.try_get("source_room_id")?,
        source_conversation_id: row.try_get("source_conversation_id")?,
        source_sequence: row.try_get("source_sequence")?,
        source_metadata_json: row.try_get("source_metadata_json")?,
        created_at: row.try_get("created_at")?,
    })
}

pub(super) fn map_agent_chat_turn_job(row: SqliteRow) -> Result<AgentChatTurnJob> {
    let failure_class = row
        .try_get::<Option<String>, _>("failure_class_json")?
        .map(|json| serde_json::from_str::<api_types::TurnFailure>(&json))
        .transpose();
    let retry_decision = row
        .try_get::<Option<String>, _>("retry_decision")?
        .map(|value| {
            serde_json::from_value::<api_types::TurnRetryDecision>(serde_json::Value::String(value))
        })
        .transpose();
    let (failure_class, retry_decision) = match (failure_class, retry_decision) {
        (Ok(class), Ok(decision)) => (class, decision),
        _ => {
            tracing::warn!(
                turn_id = row.get::<String, _>("id"),
                "unknown stored chat turn failure evidence; loading without class or decision"
            );
            (None, None)
        }
    };
    Ok(AgentChatTurnJob {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        triggering_message_id: row.try_get("triggering_message_id")?,
        responder_identity_id: row.try_get("responder_identity_id")?,
        profile_id: row.try_get("profile_id")?,
        responder_binding_id: row.try_get("responder_binding_id")?,
        responder_binding_version: row.try_get("responder_binding_version")?,
        responder_identity_version: row.try_get("responder_identity_version")?,
        profile_version: row.try_get("profile_version")?,
        operating_skill_revision_id: row.try_get("operating_skill_revision_id")?,
        policy_revision: row.try_get("policy_revision")?,
        policy_digest: row.try_get("policy_digest")?,
        permission_policy_digest: row.try_get("permission_policy_digest")?,
        tool_policy_digest: row.try_get("tool_policy_digest")?,
        admission_digest: row.try_get("admission_digest")?,
        canonical_scope_provenance_json: row.try_get("canonical_scope_provenance_json")?,
        canonical_scope_type: row.try_get("canonical_scope_type")?,
        canonical_scope_id: row.try_get("canonical_scope_id")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        pending_interaction_id: row.try_get("pending_interaction_id")?,
        dedupe_key: row.try_get("dedupe_key")?,
        lease_owner: row.try_get("lease_owner")?,
        leased_until: row.try_get("leased_until")?,
        attempt_count: row.try_get("attempt_count")?,
        max_attempts: row.try_get("max_attempts")?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        response_message_id: row.try_get("response_message_id")?,
        error_code: row.try_get("error_code")?,
        error_message: row.try_get("error_message")?,
        failure_class,
        retry_decision,
        retry_superseded: row.try_get("retry_superseded").unwrap_or(false),
        usage_limit_deferral_count: row.try_get("usage_limit_deferral_count")?,
        usage_limit_first_deferred_at: row.try_get("usage_limit_first_deferred_at")?,
        pre_provider_failure_count: row.try_get("pre_provider_failure_count")?,
        invocation_count: row.try_get("invocation_count")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        causation_depth: row.try_get("causation_depth")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Compute the same schema-versioned digest used by the services admission
/// resolver for a pre-canonicalized JSON value.  The Genesis composite lives
/// in `db`, so it uses this helper rather than importing service-layer types.
pub(crate) fn admission_digest_for_json(value: &serde_json::Value) -> Result<String> {
    let envelope = serde_json::json!({
        "schema_version": "forge.agent-turn-admission/v1",
        "value": value,
    });
    let canonical = serde_json::to_string(&canonicalize_admission_json(&envelope))
        .map_err(|_| DbError::Check("Agent Chat admission JSON is invalid".to_owned()))?;
    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

fn canonicalize_admission_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(values) => {
            let mut keys: Vec<_> = values.keys().collect();
            keys.sort_unstable();
            let mut result = serde_json::Map::new();
            for key in keys {
                result.insert(key.clone(), canonicalize_admission_json(&values[key]));
            }
            serde_json::Value::Object(result)
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonicalize_admission_json).collect())
        }
        scalar => scalar.clone(),
    }
}

fn map_agent_handoff(row: SqliteRow) -> Result<AgentHandoff> {
    Ok(AgentHandoff {
        id: row.try_get("id")?,
        source_chat_id: row.try_get("source_chat_id")?,
        target_chat_id: row.try_get("target_chat_id")?,
        source_message_id: row.try_get("source_message_id")?,
        source_turn_job_id: row.try_get("source_turn_job_id")?,
        target_message_id: row.try_get("target_message_id")?,
        target_turn_job_id: row.try_get("target_turn_job_id")?,
        author_identity_id: row.try_get("author_identity_id")?,
        content: row.try_get("content")?,
        content_guard_json: row.try_get("content_guard_json")?,
        source_revisions_json: row.try_get("source_revisions_json")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        error_code: row.try_get("error_code")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        dedupe_key: row.try_get("dedupe_key")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_binding_write_error(error: sqlx::Error) -> DbError {
    if error.to_string().to_ascii_lowercase().contains("unique") {
        DbError::Check("only one active Main/Project binding is allowed".to_owned())
    } else {
        error.into()
    }
}

fn map_chat_write_error(error: sqlx::Error) -> DbError {
    if error.to_string().to_ascii_lowercase().contains("unique") {
        DbError::Check("duplicate Agent Chat id, sequence, or deduplication key".to_owned())
    } else {
        error.into()
    }
}

impl SqliteDb {
    /// Physical chat deletion and its polymorphic scope edge are one write.
    /// Parent User/Project deletes call the same scope helper before FK cascade.
    pub async fn delete_agent_chat(&self, chat_id: &str) -> Result<bool> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        self.delete_agent_chat_scopes_in_tx(&mut tx, chat_id)
            .await?;
        let removed = sqlx::query("DELETE FROM agent_chat WHERE id=?")
            .bind(chat_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(removed != 0)
    }

    pub(super) async fn delete_agent_chat_scopes_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        chat_id: &str,
    ) -> Result<()> {
        sqlx::query(
            "DELETE FROM agent_context_scope
            WHERE scope_type='agent_chat' AND scope_id=?
              AND EXISTS(SELECT 1 FROM agent_chat WHERE id=?)",
        )
        .bind(chat_id)
        .bind(chat_id)
        .execute(&mut *connection)
        .await?;
        Ok(())
    }
}
