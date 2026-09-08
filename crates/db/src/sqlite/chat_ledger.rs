//! Atomic chat/inquiry state transitions with the typed usage ledger.
//!
//! Provider reports are prepared by the service before entering these
//! methods.  This module owns the last write boundary: the visible chat or
//! inquiry CAS, usage invocation settlement/event insertion, and the durable
//! domain event all commit (or roll back) together.

use super::*;
use std::collections::HashSet;

use crate::models::{
    AgentChatMessage, AgentChatTurnJob, AgentInquiry, AgentInquiryStatus, UsageInvocation,
    UsageInvocationLifecycle,
};
use crate::now_rfc3339;
use crate::repository::{
    CancelAgentChatTurnWithUsage, CancelAgentInquiryWithUsage,
    CompleteAgentChatControlTransferWithUsage, CompleteAgentChatTurnWithUsage,
    CompleteAgentInquiryWithUsage, CompletedAgentChatTurn, FailAgentChatTurn,
    FailAgentChatTurnWithUsage, ParkAgentChatTurnWithUsage,
};

/// Settle every observed report and close any provider attempt that finished
/// without a report.  Cancellation deliberately leaves started attempts in
/// `pending_settlement`; a late report can then use the standalone ledger
/// drain without racing the user-visible CAS.
async fn settle_source_in_tx(
    db: &SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    source_id: &str,
    settlements: &[UsageLedgerSettlement],
    cancellation: bool,
    now: &str,
) -> Result<()> {
    let rows = sqlx::query(
        "SELECT * FROM usage_invocation WHERE source_id = ? ORDER BY attempt_ordinal ASC, id ASC",
    )
    .bind(source_id)
    .fetch_all(&mut **transaction)
    .await?;
    let invocations = rows
        .into_iter()
        .map(super::pricing::map_invocation)
        .collect::<Result<Vec<_>>>()?;
    let mut settled_ids = HashSet::new();
    // Cancellation wins the domain race and deliberately leaves every
    // started obligation pending.  Any reports observed by the losing
    // provider task are drained after the CAS through the standalone ledger
    // path; accepting them here would turn a cancellation into a partial
    // terminal settlement and make the late drain non-authoritative.
    let settlements: &[UsageLedgerSettlement] = if cancellation { &[] } else { settlements };

    for settlement in settlements {
        let invocation = invocations
            .iter()
            .find(|invocation| invocation.id == settlement.invocation_id)
            .ok_or_else(|| DbError::Check("usage settlement source mismatch".to_owned()))?;
        if invocation.source_id != source_id {
            return Err(DbError::Check(
                "usage settlement source mismatch".to_owned(),
            ));
        }
        match invocation.lifecycle {
            UsageInvocationLifecycle::Started | UsageInvocationLifecycle::PendingSettlement => {
                UsageLedgerRepo::settle_usage_invocation_in_tx(
                    db,
                    transaction,
                    SettleUsageInvocation {
                        id: settlement.invocation_id.clone(),
                        expected_version: settlement.expected_version,
                        telemetry_state: settlement.telemetry_state,
                        terminal_reason: settlement.terminal_reason.clone(),
                        settled_at: settlement.settled_at.clone(),
                        updated_at: settlement.updated_at.clone(),
                    },
                )
                .await?;
                for event in &settlement.events {
                    if event.invocation_id != settlement.invocation_id
                        || event.source_id != source_id
                        || event.owner_user_id != invocation.owner_user_id
                        || event.project_id != invocation.project_id
                        || event.surface != invocation.surface
                        || event.candidate_key != invocation.candidate_key
                        || event.attempt_ordinal != invocation.attempt_ordinal
                    {
                        return Err(DbError::Check(
                            "usage event does not match settlement invocation".to_owned(),
                        ));
                    }
                    UsageLedgerRepo::append_usage_event_in_tx(db, transaction, event.clone())
                        .await?;
                }
                settled_ids.insert(invocation.id.clone());
            }
            // A terminal invocation means a prior composite committed both
            // halves.  A retry must not append a second event or turn a
            // successful settlement into an idempotency conflict.
            UsageInvocationLifecycle::Settled => {
                validate_settled_replay_in_tx(db, transaction, invocation, settlement).await?;
                settled_ids.insert(invocation.id.clone());
            }
            UsageInvocationLifecycle::Unsettled => {
                return Err(DbError::IdempotencyConflict);
            }
            UsageInvocationLifecycle::Admitted => {
                return Err(DbError::Check(
                    "admitted invocation cannot be settled before provider start".to_owned(),
                ));
            }
        }
    }

    for invocation in invocations {
        if settled_ids.contains(&invocation.id) {
            continue;
        }
        match invocation.lifecycle {
            UsageInvocationLifecycle::Started if cancellation => {
                UsageLedgerRepo::mark_usage_invocation_pending_settlement_in_tx(
                    db,
                    transaction,
                    MarkUsageInvocationPendingSettlement {
                        id: invocation.id,
                        expected_version: invocation.version,
                        updated_at: now.to_owned(),
                    },
                )
                .await?;
            }
            UsageInvocationLifecycle::PendingSettlement if cancellation => {}
            UsageInvocationLifecycle::Started | UsageInvocationLifecycle::PendingSettlement => {
                // The provider call returned without any observable report.
                // Preserve the fact that it ran but emit no synthetic usage
                // event: an unmetered invocation is not billable evidence.
                UsageLedgerRepo::settle_usage_invocation_in_tx(
                    db,
                    transaction,
                    SettleUsageInvocation {
                        id: invocation.id,
                        expected_version: invocation.version,
                        telemetry_state: UsageTelemetryState::Unmetered,
                        terminal_reason: Some("unmetered".to_owned()),
                        settled_at: now.to_owned(),
                        updated_at: now.to_owned(),
                    },
                )
                .await?;
            }
            UsageInvocationLifecycle::Admitted
            | UsageInvocationLifecycle::Settled
            | UsageInvocationLifecycle::Unsettled => {}
        }
    }
    Ok(())
}

async fn current_chat_turn(
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<AgentChatTurnJob> {
    sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
        .bind(id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)
        .and_then(super::agent_chat::map_agent_chat_turn_job)
}

async fn current_chat_response(
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<AgentChatMessage> {
    sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
        .bind(id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(DbError::from)
        .and_then(super::agent_chat::map_agent_chat_message)
}

/// Verify a replay against the already committed invocation and events.
///
/// A terminal chat/inquiry retry is allowed to be idempotent, but it must not
/// be allowed to smuggle a changed report under the same domain terminal
/// transition. Delegate event comparison to the ledger repository's existing
/// idempotency checks, while first requiring the exact event identity to be
/// present so a replay cannot append a new event to a settled invocation.
async fn validate_settled_replay_in_tx(
    db: &SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    invocation: &UsageInvocation,
    settlement: &UsageLedgerSettlement,
) -> Result<()> {
    if invocation.telemetry_state != settlement.telemetry_state
        || invocation.terminal_reason != settlement.terminal_reason
        || invocation.settled_at.as_deref() != Some(settlement.settled_at.as_str())
        || invocation.updated_at != settlement.updated_at
    {
        return Err(DbError::IdempotencyConflict);
    }

    let event_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM usage_event WHERE invocation_id = ?")
            .bind(&invocation.id)
            .fetch_one(&mut **transaction)
            .await?;
    if event_count
        != i64::try_from(settlement.events.len())
            .map_err(|_| DbError::Check("usage settlement contains too many events".to_owned()))?
    {
        return Err(DbError::IdempotencyConflict);
    }

    for event in &settlement.events {
        let event_exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM usage_event
             WHERE invocation_id = ? AND event_idempotency_key = ?
             LIMIT 1",
        )
        .bind(&invocation.id)
        .bind(&event.event_idempotency_key)
        .fetch_optional(&mut **transaction)
        .await?
        .is_some();
        if !event_exists {
            return Err(DbError::IdempotencyConflict);
        }

        // The repository performs a field-for-field idempotency comparison,
        // including counters, provider money, provenance, and timestamps.
        UsageLedgerRepo::append_usage_event_in_tx(db, transaction, event.clone()).await?;
    }
    Ok(())
}

pub(super) async fn complete_agent_chat_turn_with_usage(
    db: &SqliteDb,
    input: CompleteAgentChatTurnWithUsage,
) -> Result<CompletedAgentChatTurn> {
    let terminal = input.terminal;
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let current = current_chat_turn(&mut transaction, &terminal.turn_job_id).await?;
    if current.status == AgentChatTurnState::Succeeded {
        let response_id = current
            .response_message_id
            .clone()
            .ok_or(DbError::NotFound)?;
        if response_id != terminal.response.id {
            return Err(DbError::IdempotencyConflict);
        }
        let response = current_chat_response(&mut transaction, &response_id).await?;
        settle_source_in_tx(
            db,
            &mut transaction,
            &terminal.turn_job_id,
            &input.settlements,
            false,
            &terminal.updated_at,
        )
        .await?;
        transaction.commit().await?;
        return Ok(CompletedAgentChatTurn {
            response,
            turn: current,
        });
    }
    if current.version != terminal.expected_version
        || current.status != AgentChatTurnState::Leased
        || current.lease_owner.as_deref() != Some(terminal.lease_owner.as_str())
    {
        return Err(DbError::VersionConflict);
    }
    if terminal.response.chat_id != current.chat_id
        || terminal.response.id == current.triggering_message_id
    {
        return Err(DbError::Check(
            "response message must belong to turn chat and differ from trigger".to_owned(),
        ));
    }

    let (response, inserted) = if let Some(existing) =
        sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
            .bind(&terminal.response.id)
            .fetch_optional(&mut *transaction)
            .await?
    {
        let response = super::agent_chat::map_agent_chat_message(existing)?;
        if response.chat_id != current.chat_id {
            return Err(DbError::Check("response message chat mismatch".to_owned()));
        }
        (response, false)
    } else {
        let sequence = super::agent_chat::allocate_chat_sequence(
            &mut transaction,
            &current.chat_id,
            &terminal.response.created_at,
        )
        .await?;
        let mut response_input = terminal.response.clone();
        response_input.sequence = sequence;
        (
            super::agent_chat::insert_chat_message(&mut transaction, &response_input).await?,
            true,
        )
    };

    let updated = sqlx::query(
        "UPDATE agent_chat_turn_job
         SET status = 'succeeded', response_message_id = ?,
             lease_owner = NULL, leased_until = NULL,
             next_attempt_at = NULL, error_code = NULL,
             error_message = NULL, version = version + 1, updated_at = ?
         WHERE id = ? AND version = ? AND status = 'leased' AND lease_owner = ?",
    )
    .bind(&response.id)
    .bind(&terminal.updated_at)
    .bind(&terminal.turn_job_id)
    .bind(terminal.expected_version)
    .bind(&terminal.lease_owner)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    if inserted {
        super::agent_chat::append_agent_chat_event(
            db,
            &mut transaction,
            "agent_chat.response.completed",
            &response,
            current.correlation_id.clone(),
            current.causation_id.clone(),
            current.causation_depth,
        )
        .await?;
    }
    settle_source_in_tx(
        db,
        &mut transaction,
        &terminal.turn_job_id,
        &input.settlements,
        false,
        &terminal.updated_at,
    )
    .await?;
    let turn = current_chat_turn(&mut transaction, &terminal.turn_job_id).await?;
    transaction.commit().await?;
    Ok(CompletedAgentChatTurn { response, turn })
}

pub(super) async fn complete_agent_chat_control_transfer_with_usage(
    db: &SqliteDb,
    input: CompleteAgentChatControlTransferWithUsage,
) -> Result<AgentChatTurnJob> {
    let terminal = input.terminal;
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let current = current_chat_turn(&mut transaction, &terminal.turn_job_id).await?;
    if current.status == AgentChatTurnState::Succeeded && current.response_message_id.is_none() {
        // A Genesis handoff is a domain idempotency boundary, not merely a
        // terminal status.  Verify the durable command payload before
        // accepting a replay, so a manually replayed transfer cannot replace
        // its continuation/session identifiers while reusing the same turn.
        let transfer_key = format!("agent-chat-control-transfer:{}", current.id);
        if let Some(payload_json) = sqlx::query_scalar::<_, String>(
            "SELECT payload_json FROM domain_event WHERE dedupe_key = ?",
        )
        .bind(&transfer_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let payload: serde_json::Value =
                serde_json::from_str(&payload_json).map_err(|_| DbError::IdempotencyConflict)?;
            let fields_match = payload.get("operation").and_then(serde_json::Value::as_str)
                == Some("genesis.start")
                && payload
                    .get("source_turn_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(current.id.as_str())
                && payload
                    .get("continuation_turn_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(terminal.continuation_turn_id.as_str())
                && payload
                    .get("genesis_session_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(terminal.genesis_session_id.as_str())
                && payload
                    .get("command_receipt_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(terminal.command_receipt_id.as_str())
                && payload
                    .get("response_message_committed")
                    .and_then(serde_json::Value::as_bool)
                    == Some(false);
            if !fields_match {
                return Err(DbError::IdempotencyConflict);
            }
        }
        settle_source_in_tx(
            db,
            &mut transaction,
            &terminal.turn_job_id,
            &input.settlements,
            !input.provider_finished,
            &terminal.updated_at,
        )
        .await?;
        transaction.commit().await?;
        return Ok(current);
    }
    if current.version != terminal.expected_version
        || current.status != AgentChatTurnState::Leased
        || current.lease_owner.as_deref() != Some(terminal.lease_owner.as_str())
    {
        return Err(DbError::VersionConflict);
    }

    let outcome_json = sqlx::query_scalar::<_, String>(
        "SELECT outcome_json FROM command_receipt
         WHERE id = ? AND operation = 'genesis.start'",
    )
    .bind(&terminal.command_receipt_id)
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
            != Some(terminal.continuation_turn_id.as_str())
        || outcome
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            != Some(terminal.genesis_session_id.as_str())
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
    .bind(&terminal.continuation_turn_id)
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
             error_code = NULL, error_message = NULL,
             version = version + 1, updated_at = ?
         WHERE id = ? AND version = ? AND status = 'leased' AND lease_owner = ?",
    )
    .bind(&terminal.updated_at)
    .bind(&terminal.turn_job_id)
    .bind(terminal.expected_version)
    .bind(&terminal.lease_owner)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }

    DomainEventRepo::append_event_in_tx(
        db,
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
            causation_id: Some(terminal.command_receipt_id.clone()),
            causation_depth: current.causation_depth.saturating_add(1).min(16),
            dedupe_key: Some(format!("agent-chat-control-transfer:{}", current.id)),
            payload_json: serde_json::json!({
                "operation": "genesis.start",
                "source_turn_id": current.id,
                "continuation_turn_id": terminal.continuation_turn_id,
                "genesis_session_id": terminal.genesis_session_id,
                "command_receipt_id": terminal.command_receipt_id,
                "response_message_committed": false,
            })
            .to_string(),
            created_at: terminal.updated_at.clone(),
        },
    )
    .await?;
    settle_source_in_tx(
        db,
        &mut transaction,
        &terminal.turn_job_id,
        &input.settlements,
        !input.provider_finished,
        &terminal.updated_at,
    )
    .await?;
    let turn = current_chat_turn(&mut transaction, &terminal.turn_job_id).await?;
    transaction.commit().await?;
    Ok(turn)
}

pub(super) async fn fail_agent_chat_turn_with_usage(
    db: &SqliteDb,
    input: FailAgentChatTurnWithUsage,
) -> Result<AgentChatTurnJob> {
    let terminal = input.terminal;
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let current = current_chat_turn(&mut transaction, &terminal.turn_job_id).await?;
    let error_code = super::agent_chat::bounded_event_text(&terminal.error_code, 128);
    let error_message = super::agent_chat::bounded_event_text(&terminal.error_message, 2048);
    if current.status == terminal.status
        && current.lease_owner.is_none()
        && current.attempt_count == terminal.attempt_count
        && current.next_attempt_at == terminal.next_attempt_at
        && current.error_code.as_deref() == Some(error_code.as_str())
        && current.error_message.as_deref() == Some(error_message.as_str())
    {
        settle_source_in_tx(
            db,
            &mut transaction,
            &terminal.turn_job_id,
            &input.settlements,
            false,
            &terminal.updated_at,
        )
        .await?;
        transaction.commit().await?;
        return Ok(current);
    }
    if current.version != terminal.expected_version
        || current.status != AgentChatTurnState::Leased
        || current.lease_owner.as_deref() != Some(terminal.lease_owner.as_str())
    {
        return Err(DbError::VersionConflict);
    }
    let updated = sqlx::query(
        "UPDATE agent_chat_turn_job
         SET status = ?, lease_owner = NULL, leased_until = NULL,
             attempt_count = ?, next_attempt_at = ?, error_code = ?,
             error_message = ?, version = version + 1, updated_at = ?
         WHERE id = ? AND version = ? AND status = 'leased' AND lease_owner = ?",
    )
    .bind(terminal.status.to_string())
    .bind(terminal.attempt_count)
    .bind(terminal.next_attempt_at.as_deref())
    .bind(&error_code)
    .bind(&error_message)
    .bind(&terminal.updated_at)
    .bind(&terminal.turn_job_id)
    .bind(terminal.expected_version)
    .bind(&terminal.lease_owner)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let turn = current_chat_turn(&mut transaction, &terminal.turn_job_id).await?;
    super::agent_chat::append_agent_chat_turn_failure_event(
        db,
        &mut transaction,
        &turn,
        &FailAgentChatTurn {
            error_code,
            error_message,
            ..terminal.clone()
        },
    )
    .await?;
    settle_source_in_tx(
        db,
        &mut transaction,
        &terminal.turn_job_id,
        &input.settlements,
        false,
        &terminal.updated_at,
    )
    .await?;
    transaction.commit().await?;
    Ok(turn)
}

pub(super) async fn park_agent_chat_turn_with_usage(
    db: &SqliteDb,
    input: ParkAgentChatTurnWithUsage,
) -> Result<AgentChatTurnJob> {
    let terminal = input.terminal;
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let current = current_chat_turn(&mut transaction, &terminal.turn_job_id).await?;
    if current.status == AgentChatTurnState::AwaitingInput
        && current.lease_owner.is_none()
        && current.pending_interaction_id.as_deref()
            == Some(terminal.pending_interaction_id.as_str())
    {
        settle_source_in_tx(
            db,
            &mut transaction,
            &terminal.turn_job_id,
            &input.settlements,
            false,
            &terminal.updated_at,
        )
        .await?;
        transaction.commit().await?;
        return Ok(current);
    }
    if current.version != terminal.expected_version
        || current.status != AgentChatTurnState::Leased
        || current.lease_owner.as_deref() != Some(terminal.lease_owner.as_str())
    {
        return Err(DbError::VersionConflict);
    }
    let updated = sqlx::query(
        "UPDATE agent_chat_turn_job
         SET status = 'awaiting_input', pending_interaction_id = ?,
             lease_owner = NULL, leased_until = NULL,
             attempt_count = MAX(0, attempt_count - 1), next_attempt_at = NULL,
             error_code = NULL, error_message = NULL, version = version + 1,
             updated_at = ?
         WHERE id = ? AND version = ? AND status = 'leased' AND lease_owner = ?",
    )
    .bind(&terminal.pending_interaction_id)
    .bind(&terminal.updated_at)
    .bind(&terminal.turn_job_id)
    .bind(terminal.expected_version)
    .bind(&terminal.lease_owner)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let turn = current_chat_turn(&mut transaction, &terminal.turn_job_id).await?;
    super::agent_chat::append_agent_chat_turn_awaiting_event(db, &mut transaction, &turn).await?;
    settle_source_in_tx(
        db,
        &mut transaction,
        &terminal.turn_job_id,
        &input.settlements,
        false,
        &terminal.updated_at,
    )
    .await?;
    transaction.commit().await?;
    Ok(turn)
}

pub(super) async fn cancel_agent_chat_turn_with_usage(
    db: &SqliteDb,
    input: CancelAgentChatTurnWithUsage,
) -> Result<AgentChatTurnJob> {
    let terminal = input.terminal;
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let dedupe_key = format!(
        "agent-chat-turn-cancel:{}:{}",
        terminal.turn_job_id, terminal.idempotency_key
    );
    if let Some(existing_entity_id) =
        sqlx::query_scalar::<_, String>("SELECT entity_id FROM domain_event WHERE dedupe_key = ?")
            .bind(&dedupe_key)
            .fetch_optional(&mut *transaction)
            .await?
    {
        if existing_entity_id != terminal.turn_job_id {
            return Err(DbError::Check(
                "turn cancellation idempotency key belongs to another turn".to_owned(),
            ));
        }
        let turn = current_chat_turn(&mut transaction, &terminal.turn_job_id).await?;
        transaction.commit().await?;
        return Ok(turn);
    }
    let current = current_chat_turn(&mut transaction, &terminal.turn_job_id).await?;
    if current.version != terminal.expected_version {
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
             error_message = 'cancelled by user', version = version + 1,
             updated_at = ?
         WHERE id = ? AND version = ?
           AND status IN ('queued', 'leased', 'retry_wait', 'awaiting_input')",
    )
    .bind(&terminal.updated_at)
    .bind(&terminal.turn_job_id)
    .bind(terminal.expected_version)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    settle_source_in_tx(
        db,
        &mut transaction,
        &terminal.turn_job_id,
        &input.settlements,
        true,
        &terminal.updated_at,
    )
    .await?;
    let turn = current_chat_turn(&mut transaction, &terminal.turn_job_id).await?;
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "agent_chat.turn.cancelled".to_owned(),
        entity_type: "agent_chat_turn_job".to_owned(),
        entity_id: turn.id.clone(),
        actor_type: "user".to_owned(),
        actor_id: Some(terminal.actor_user_id),
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
        created_at: terminal.updated_at,
    };
    DomainEventRepo::append_event_in_tx(db, &mut transaction, &event).await?;
    transaction.commit().await?;
    Ok(turn)
}

async fn current_inquiry(
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<AgentInquiry> {
    sqlx::query("SELECT * FROM agent_inquiry WHERE id = ?")
        .bind(id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)
        .and_then(super::agent_inquiry::map_agent_inquiry)
}

async fn append_inquiry_event(
    db: &SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    inquiry: &AgentInquiry,
    status: AgentInquiryStatus,
    expected_version: i64,
    created_at: String,
) -> Result<DomainEvent> {
    let event_type = match status {
        AgentInquiryStatus::Succeeded => "agent_inquiry.completed",
        AgentInquiryStatus::Failed => "agent_inquiry.failed",
        AgentInquiryStatus::Cancelled => "agent_inquiry.cancelled",
        AgentInquiryStatus::Running => "agent_inquiry.started",
    };
    DomainEventRepo::append_event_in_tx(
        db,
        transaction,
        &CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: event_type.to_owned(),
            entity_type: "agent_inquiry".to_owned(),
            entity_id: inquiry.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "agent_chat".to_owned(),
            scope_id: inquiry.chat_id.clone(),
            correlation_id: inquiry
                .turn_job_id
                .clone()
                .unwrap_or_else(|| inquiry.id.clone()),
            causation_id: inquiry.turn_job_id.clone(),
            causation_depth: 0,
            dedupe_key: Some(format!(
                "agent-inquiry-event:{event_type}:{}:{expected_version}",
                inquiry.id
            )),
            payload_json: serde_json::json!({
                "inquiry_id": inquiry.id,
                "chat_id": inquiry.chat_id,
                "status": inquiry.status.to_string(),
                "version": inquiry.version,
            })
            .to_string(),
            created_at,
        },
    )
    .await
}

pub(super) async fn complete_agent_inquiry_with_usage(
    db: &SqliteDb,
    input: CompleteAgentInquiryWithUsage,
) -> Result<AgentInquiry> {
    let terminal = input.terminal;
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let current = current_inquiry(&mut transaction, &terminal.id).await?;
    if current.status == terminal.status {
        // Cancellation is authoritative in a completion race, so a late
        // completion is allowed to observe the already-cancelled record. For
        // all other terminal states, a retry must carry the exact same domain
        // payload; otherwise an idempotency key would hide a changed answer.
        if terminal.status != AgentInquiryStatus::Cancelled
            && (current.findings != terminal.findings
                || current.findings_path != terminal.findings_path
                || current.error != terminal.error
                || current.input_tokens != terminal.input_tokens
                || current.output_tokens != terminal.output_tokens
                || current.cache_read_tokens != terminal.cache_read_tokens
                || current.cache_write_tokens != terminal.cache_write_tokens
                || current.duration_ms != terminal.duration_ms)
        {
            return Err(DbError::IdempotencyConflict);
        }
        if !input.settlements.is_empty() || terminal.status != AgentInquiryStatus::Cancelled {
            settle_source_in_tx(
                db,
                &mut transaction,
                &terminal.id,
                &input.settlements,
                terminal.status == AgentInquiryStatus::Cancelled,
                &now_rfc3339(),
            )
            .await?;
        }
        transaction.commit().await?;
        return Ok(current);
    }
    if current.version != terminal.expected_version || current.status != AgentInquiryStatus::Running
    {
        return Err(DbError::VersionConflict);
    }
    let now = now_rfc3339();
    let updated = sqlx::query(
        "UPDATE agent_inquiry
         SET status = ?, findings = ?, findings_path = ?, error = ?,
             input_tokens = ?, output_tokens = ?, cache_read_tokens = ?,
             cache_write_tokens = ?, duration_ms = ?, version = version + 1,
             updated_at = ?, finished_at = ?
         WHERE id = ? AND version = ? AND status = 'running'",
    )
    .bind(terminal.status.to_string())
    .bind(terminal.findings.as_deref())
    .bind(terminal.findings_path.as_deref())
    .bind(terminal.error.as_deref())
    .bind(terminal.input_tokens)
    .bind(terminal.output_tokens)
    .bind(terminal.cache_read_tokens)
    .bind(terminal.cache_write_tokens)
    .bind(terminal.duration_ms)
    .bind(&now)
    .bind(&now)
    .bind(&terminal.id)
    .bind(terminal.expected_version)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let record = current_inquiry(&mut transaction, &terminal.id).await?;
    let cancellation = terminal.status == AgentInquiryStatus::Cancelled;
    append_inquiry_event(
        db,
        &mut transaction,
        &record,
        terminal.status,
        terminal.expected_version,
        now.clone(),
    )
    .await?;
    settle_source_in_tx(
        db,
        &mut transaction,
        &terminal.id,
        &input.settlements,
        cancellation,
        &now,
    )
    .await?;
    transaction.commit().await?;
    Ok(record)
}

pub(super) async fn cancel_agent_inquiry_with_usage(
    db: &SqliteDb,
    input: CancelAgentInquiryWithUsage,
) -> Result<AgentInquiry> {
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let current = current_inquiry(&mut transaction, &input.id).await?;
    if current.status == AgentInquiryStatus::Cancelled {
        transaction.commit().await?;
        return Ok(current);
    }
    if current.version != input.expected_version || current.status != AgentInquiryStatus::Running {
        return Err(DbError::VersionConflict);
    }
    let now = now_rfc3339();
    let updated = sqlx::query(
        "UPDATE agent_inquiry
         SET status = 'cancelled', version = version + 1, updated_at = ?,
             finished_at = ?, duration_ms = COALESCE(
                 duration_ms,
                 CAST((julianday(?) - julianday(started_at)) * 86400000.0 AS INTEGER)
             )
         WHERE id = ? AND version = ? AND status = 'running'",
    )
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .bind(&input.id)
    .bind(input.expected_version)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let record = current_inquiry(&mut transaction, &input.id).await?;
    append_inquiry_event(
        db,
        &mut transaction,
        &record,
        AgentInquiryStatus::Cancelled,
        input.expected_version,
        now.clone(),
    )
    .await?;
    settle_source_in_tx(
        db,
        &mut transaction,
        &input.id,
        &input.settlements,
        true,
        &now,
    )
    .await?;
    transaction.commit().await?;
    Ok(record)
}
