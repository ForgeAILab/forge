use super::*;
use crate::{CommandReceipt, CommandReceiptRepo, CreateCommandReceipt};

const COMMAND_RECEIPT_COLUMNS: &str = "id, principal_type, principal_id, scope_type, scope_id, operation, idempotency_key, input_digest, policy_result, correlation_id, causation_id, causation_depth, event_id, agent_action_execution_id, outcome_json, committed_at";

fn map_command_receipt(row: SqliteRow) -> Result<CommandReceipt> {
    Ok(CommandReceipt {
        id: row.try_get("id")?,
        principal_type: row.try_get("principal_type")?,
        principal_id: row.try_get("principal_id")?,
        scope_type: row.try_get("scope_type")?,
        scope_id: row.try_get("scope_id")?,
        operation: row.try_get("operation")?,
        idempotency_key: row.try_get("idempotency_key")?,
        input_digest: row.try_get("input_digest")?,
        policy_result: row.try_get("policy_result")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        causation_depth: row.try_get("causation_depth")?,
        event_id: row.try_get("event_id")?,
        agent_action_execution_id: row.try_get("agent_action_execution_id")?,
        outcome_json: row.try_get("outcome_json")?,
        committed_at: row.try_get("committed_at")?,
    })
}

fn is_command_receipt_replay_constraint(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(database_error) = error else {
        return false;
    };

    // SQLite reports the columns participating in the violated unique key.
    // Only the canonical replay key is an idempotency collision.  In
    // particular, the primary-key collision on `id` and foreign-key/check
    // failures must retain their database error category.
    database_error
        .message()
        .to_ascii_lowercase()
        .contains(
            "unique constraint failed: command_receipt.scope_type, command_receipt.scope_id, command_receipt.operation, command_receipt.idempotency_key",
        )
}

#[allow(clippy::too_many_arguments)]
async fn get_command_receipt_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    principal_type: &str,
    principal_id: &str,
    scope_type: &str,
    scope_id: &str,
    operation: &str,
    idempotency_key: &str,
    input_digest: &str,
) -> Result<Option<CommandReceipt>> {
    let Some(row) = sqlx::query(&format!(
        "SELECT {COMMAND_RECEIPT_COLUMNS}
         FROM command_receipt
         WHERE scope_type = ? AND scope_id = ?
           AND operation = ? AND idempotency_key = ?"
    ))
    .bind(scope_type)
    .bind(scope_id)
    .bind(operation)
    .bind(idempotency_key)
    .fetch_optional(&mut **transaction)
    .await?
    else {
        return Ok(None);
    };

    let receipt = map_command_receipt(row)?;
    if receipt.principal_type != principal_type
        || receipt.principal_id != principal_id
        || receipt.input_digest != input_digest
    {
        return Err(DbError::IdempotencyConflict);
    }
    Ok(Some(receipt))
}

async fn insert_command_receipt_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &CreateCommandReceipt,
) -> Result<CommandReceipt> {
    if let Some(existing) = get_command_receipt_in_tx(
        transaction,
        &input.principal_type,
        &input.principal_id,
        &input.scope_type,
        &input.scope_id,
        &input.operation,
        &input.idempotency_key,
        &input.input_digest,
    )
    .await?
    {
        return Ok(existing);
    }

    let insert = sqlx::query(
        "INSERT INTO command_receipt (
            id, principal_type, principal_id, scope_type, scope_id, operation,
            idempotency_key, input_digest, policy_result, correlation_id,
            causation_id, causation_depth, event_id, agent_action_execution_id,
            outcome_json, committed_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.principal_type)
    .bind(&input.principal_id)
    .bind(&input.scope_type)
    .bind(&input.scope_id)
    .bind(&input.operation)
    .bind(&input.idempotency_key)
    .bind(&input.input_digest)
    .bind(&input.policy_result)
    .bind(&input.correlation_id)
    .bind(input.causation_id.as_deref())
    .bind(input.causation_depth)
    .bind(&input.event_id)
    .bind(input.agent_action_execution_id.as_deref())
    .bind(&input.outcome_json)
    .bind(&input.committed_at)
    .execute(&mut **transaction)
    .await;

    if let Err(error) = insert {
        if is_command_receipt_replay_constraint(&error) {
            // BEGIN IMMEDIATE serializes writers, but resolving the row here
            // keeps this invariant correct if the insert path is ever reused
            // by a different transaction policy.
            return get_command_receipt_in_tx(
                transaction,
                &input.principal_type,
                &input.principal_id,
                &input.scope_type,
                &input.scope_id,
                &input.operation,
                &input.idempotency_key,
                &input.input_digest,
            )
            .await?
            .ok_or_else(|| check_error(error));
        }
        return Err(check_error(error));
    }

    sqlx::query(&format!(
        "SELECT {COMMAND_RECEIPT_COLUMNS} FROM command_receipt WHERE id = ?"
    ))
    .bind(&input.id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(DbError::from)
    .and_then(map_command_receipt)
}

#[async_trait]
impl CommandReceiptRepo for SqliteDb {
    async fn get_command_receipt_by_identity(
        &self,
        principal_type: &str,
        principal_id: &str,
        scope_type: &str,
        scope_id: &str,
        operation: &str,
        idempotency_key: &str,
    ) -> Result<Option<CommandReceipt>> {
        let row = sqlx::query(&format!(
            "SELECT {COMMAND_RECEIPT_COLUMNS}
             FROM command_receipt
             WHERE principal_type = ? AND principal_id = ?
               AND scope_type = ? AND scope_id = ?
               AND operation = ? AND idempotency_key = ?
             LIMIT 1"
        ))
        .bind(principal_type)
        .bind(principal_id)
        .bind(scope_type)
        .bind(scope_id)
        .bind(operation)
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(map_command_receipt).transpose()
    }

    async fn get_command_receipt(
        &self,
        principal_type: &str,
        principal_id: &str,
        scope_type: &str,
        scope_id: &str,
        operation: &str,
        idempotency_key: &str,
        input_digest: &str,
    ) -> Result<Option<CommandReceipt>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let receipt = get_command_receipt_in_tx(
            &mut transaction,
            principal_type,
            principal_id,
            scope_type,
            scope_id,
            operation,
            idempotency_key,
            input_digest,
        )
        .await?;
        transaction.commit().await?;
        Ok(receipt)
    }

    async fn get_command_receipt_by_agent_action_execution(
        &self,
        agent_action_execution_id: &str,
    ) -> Result<Option<CommandReceipt>> {
        let row = sqlx::query(&format!(
            "SELECT {COMMAND_RECEIPT_COLUMNS}
             FROM command_receipt
             WHERE agent_action_execution_id = ?
             LIMIT 1"
        ))
        .bind(agent_action_execution_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(map_command_receipt).transpose()
    }

    async fn create_command_receipt(&self, input: CreateCommandReceipt) -> Result<CommandReceipt> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let receipt = insert_command_receipt_in_tx(&mut transaction, &input).await?;
        transaction.commit().await?;
        Ok(receipt)
    }

    async fn get_command_receipt_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        principal_type: &str,
        principal_id: &str,
        scope_type: &str,
        scope_id: &str,
        operation: &str,
        idempotency_key: &str,
        input_digest: &str,
    ) -> Result<Option<CommandReceipt>> {
        get_command_receipt_in_tx(
            transaction,
            principal_type,
            principal_id,
            scope_type,
            scope_id,
            operation,
            idempotency_key,
            input_digest,
        )
        .await
    }

    async fn create_command_receipt_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateCommandReceipt,
    ) -> Result<CommandReceipt> {
        insert_command_receipt_in_tx(transaction, &input).await
    }
}
