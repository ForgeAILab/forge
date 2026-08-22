use crate::{
    AgentActionExecution, AgentActionRepo, CommandReceiptRepo, CreateAgentActionExecution,
    CreateCommandReceipt, DbError, Result, SqliteDb,
};
use sqlx::Row;

pub(crate) async fn action_scope_resolves_to_command_scope(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    action_scope_type: &str,
    action_scope_id: &str,
    command_scope_type: &str,
    command_scope_id: &str,
) -> Result<bool> {
    if action_scope_type == command_scope_type && action_scope_id == command_scope_id {
        return Ok(true);
    }
    if action_scope_type != "agent_chat" {
        return Ok(false);
    }

    let resolved: Option<i64> = match command_scope_type {
        "account" => {
            sqlx::query_scalar(
                "SELECT 1 FROM agent_chat
                 WHERE id = ? AND kind = 'account_main' AND account_id = ?
                 LIMIT 1",
            )
            .bind(action_scope_id)
            .bind(command_scope_id)
            .fetch_optional(&mut **transaction)
            .await?
        }
        "project" => {
            sqlx::query_scalar(
                "SELECT 1 FROM agent_chat
                 WHERE id = ? AND kind = 'project' AND project_id = ?
                 LIMIT 1",
            )
            .bind(action_scope_id)
            .bind(command_scope_id)
            .fetch_optional(&mut **transaction)
            .await?
        }
        _ => None,
    };
    Ok(resolved.is_some())
}

/// A public Action may be a coarse, closed command family while its durable
/// command receipt names the exact lifecycle operation. Keep the exceptions
/// explicit: accepting arbitrary dotted prefixes would let a broad Action
/// operation authorize unrelated commands that happen to share a namespace.
fn action_operation_resolves_to_command(action_operation: &str, command_operation: &str) -> bool {
    action_operation == command_operation
        || matches!(
            (action_operation, command_operation),
            (
                "project.execution_baseline",
                "project.execution_baseline.save_draft"
                    | "project.execution_baseline.propose_for_approval"
            )
        )
}

/// Finalize a shared command while the domain repository still owns its
/// transaction. The domain event has already been appended, so this function
/// can bind the frozen receipt and optional Action execution to that exact
/// event before the single commit.
pub(crate) async fn finalize_command_in_tx(
    db: &SqliteDb,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event_id: &str,
    mut receipt: Option<CreateCommandReceipt>,
    action_execution: Option<CreateAgentActionExecution>,
) -> Result<Option<AgentActionExecution>> {
    if let (Some(receipt), Some(action_execution)) = (&receipt, &action_execution) {
        if receipt.agent_action_execution_id.as_deref() != Some(action_execution.id.as_str())
            || action_execution.executed_by_type != receipt.principal_type
            || action_execution.executed_by_id != receipt.principal_id
            || action_execution.idempotency_key != receipt.idempotency_key
            || action_execution.result_json.as_deref() != Some(receipt.outcome_json.as_str())
            || action_execution.action_outcome_json.as_deref()
                != Some(receipt.outcome_json.as_str())
        {
            return Err(DbError::IdempotencyConflict);
        }

        let action = sqlx::query(
            "SELECT operation, scope_type, scope_id, policy_result,
                    correlation_id, causation_id, causation_depth
             FROM agent_action WHERE id = ?",
        )
        .bind(&action_execution.action_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let action_operation: String = action.try_get("operation")?;
        let action_scope_type: String = action.try_get("scope_type")?;
        let action_scope_id: String = action.try_get("scope_id")?;
        let action_policy_result: String = action.try_get("policy_result")?;
        let action_correlation_id: String = action.try_get("correlation_id")?;
        let action_causation_id: Option<String> = action.try_get("causation_id")?;
        let action_causation_depth: i64 = action.try_get("causation_depth")?;
        let scope_matches = action_scope_resolves_to_command_scope(
            transaction,
            &action_scope_type,
            &action_scope_id,
            &receipt.scope_type,
            &receipt.scope_id,
        )
        .await?;
        if !action_operation_resolves_to_command(&action_operation, &receipt.operation)
            || !scope_matches
            || action_policy_result != receipt.policy_result
            || action_correlation_id != receipt.correlation_id
            || action_causation_id != receipt.causation_id
            || action_causation_depth != receipt.causation_depth
        {
            return Err(DbError::IdempotencyConflict);
        }
    }
    if let Some(receipt) = &receipt {
        if receipt.id.trim().is_empty()
            || receipt.principal_type.trim().is_empty()
            || receipt.principal_id.trim().is_empty()
            || receipt.operation.trim().is_empty()
            || receipt.idempotency_key.trim().is_empty()
            || receipt.input_digest.trim().is_empty()
            || receipt.policy_result.trim().is_empty()
            || receipt.correlation_id.trim().is_empty()
            || receipt.outcome_json.trim().is_empty()
            || serde_json::from_str::<serde_json::Value>(&receipt.outcome_json).is_err()
        {
            return Err(DbError::Check(
                "command receipt finalization is incomplete".to_owned(),
            ));
        }

        let event = sqlx::query(
            "SELECT actor_type, actor_id, correlation_id, causation_id, causation_depth
             FROM domain_event WHERE id = ?",
        )
        .bind(event_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let event_actor_type: String = event.try_get("actor_type")?;
        let event_actor_id: Option<String> = event.try_get("actor_id")?;
        let event_correlation_id: String = event.try_get("correlation_id")?;
        let event_causation_id: Option<String> = event.try_get("causation_id")?;
        let event_causation_depth: i64 = event.try_get("causation_depth")?;
        if event_actor_type != receipt.principal_type
            || event_actor_id.as_deref() != Some(receipt.principal_id.as_str())
            || event_correlation_id != receipt.correlation_id
            || event_causation_id != receipt.causation_id
            || event_causation_depth != receipt.causation_depth
        {
            return Err(DbError::IdempotencyConflict);
        }
    }
    let execution = if let Some(action_execution) = action_execution {
        Some(
            AgentActionRepo::record_action_execution_in_tx(db, transaction, action_execution)
                .await?,
        )
    } else {
        None
    };

    if let Some(mut receipt) = receipt.take() {
        receipt.event_id = event_id.to_owned();
        receipt.agent_action_execution_id = execution.as_ref().map(|value| value.id.clone());
        CommandReceiptRepo::create_command_receipt_in_tx(db, transaction, receipt).await?;
    }
    Ok(execution)
}

#[cfg(test)]
mod tests {
    use super::action_operation_resolves_to_command;

    #[test]
    fn baseline_action_family_resolves_only_to_agent_allowed_commands() {
        assert!(action_operation_resolves_to_command(
            "project.execution_baseline",
            "project.execution_baseline.save_draft"
        ));
        assert!(action_operation_resolves_to_command(
            "project.execution_baseline",
            "project.execution_baseline.propose_for_approval"
        ));
        assert!(!action_operation_resolves_to_command(
            "project.execution_baseline",
            "project.execution_baseline.approve"
        ));
        assert!(!action_operation_resolves_to_command(
            "project.execution_baseline",
            "project.execution_baseline.activate"
        ));
        assert!(!action_operation_resolves_to_command(
            "project.execution_baseline",
            "project.document.revise"
        ));
    }
}
