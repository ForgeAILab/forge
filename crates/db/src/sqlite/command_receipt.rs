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

impl SqliteDb {
    pub async fn retained_owner_workspace_reset_outcome(
        &self,
        placement_id: &str,
        generation: i64,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT outcome_json FROM command_receipt WHERE operation = 'daemon.workspace.reset'
         AND principal_id = 'workspace-backend'
         AND json_extract(outcome_json, '$.metadata.placement_id') = ?
         AND json_extract(outcome_json, '$.metadata.generation') = ?
         AND json_extract(outcome_json, '$.metadata.status') = 'result'
         ORDER BY committed_at DESC, id DESC LIMIT 1",
        )
        .bind(placement_id)
        .bind(generation)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub(crate) fn attention_coordination_scope_visibility_predicate(
        user_id: &str,
        project_id: Option<&str>,
        scope_alias: &str,
    ) -> (String, Vec<String>) {
        match project_id {
            Some(project_id) => (
                format!(
                    "(({scope_alias}.scope_type = 'project' AND {scope_alias}.scope_id = ?
                       AND EXISTS (
                           SELECT 1 FROM project p
                           WHERE p.id = {scope_alias}.scope_id
                             AND (p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                                 SELECT 1 FROM project_member pm
                                 WHERE pm.project_id = p.id AND pm.user_id = ?
                             ))
                       ))
                      OR ({scope_alias}.scope_type = 'agent_chat' AND EXISTS (
                           SELECT 1 FROM agent_chat c
                           JOIN project p ON p.id = c.project_id
                           WHERE c.id = {scope_alias}.scope_id
                             AND c.kind = 'project'
                             AND c.project_id = ?
                             AND (p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                                 SELECT 1 FROM project_member pm
                                 WHERE pm.project_id = p.id AND pm.user_id = ?
                             ))
                       ))
                      OR ({scope_alias}.scope_type = 'task' AND EXISTS (
                           SELECT 1 FROM task t
                           JOIN project p ON p.id = t.project_id
                           WHERE t.id = {scope_alias}.scope_id
                             AND t.project_id = ?
                             AND (p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                                 SELECT 1 FROM project_member pm
                                 WHERE pm.project_id = p.id AND pm.user_id = ?
                             ))
                       )))"
                ),
                vec![
                    project_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    project_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    project_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                ],
            ),
            None => (
                format!(
                    "(({scope_alias}.scope_type = 'account' AND {scope_alias}.scope_id = ?)
                      OR ({scope_alias}.scope_type = 'project' AND EXISTS (
                           SELECT 1 FROM project p
                           WHERE p.id = {scope_alias}.scope_id
                             AND (p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                                 SELECT 1 FROM project_member pm
                                 WHERE pm.project_id = p.id AND pm.user_id = ?
                             ))
                       ))
                      OR ({scope_alias}.scope_type = 'agent_chat' AND EXISTS (
                           SELECT 1 FROM agent_chat c
                           LEFT JOIN project p ON p.id = c.project_id
                           WHERE c.id = {scope_alias}.scope_id
                             AND ((c.kind = 'account_main' AND c.account_id = ?)
                               OR (c.kind = 'project' AND p.id IS NOT NULL AND
                                   (p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                                       SELECT 1 FROM project_member pm
                                       WHERE pm.project_id = p.id AND pm.user_id = ?
                                   ))))
                       ))
                      OR ({scope_alias}.scope_type = 'task' AND EXISTS (
                           SELECT 1 FROM task t
                           JOIN project p ON p.id = t.project_id
                           WHERE t.id = {scope_alias}.scope_id
                             AND (p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                                 SELECT 1 FROM project_member pm
                                 WHERE pm.project_id = p.id AND pm.user_id = ?
                             ))
                       )))"
                ),
                vec![
                    user_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                ],
            ),
        }
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionDirectCoordinationReceiptsRow {
    pub id: crate::Result<String>,
    pub principal_type: crate::Result<String>,
    pub principal_id: crate::Result<String>,
    pub scope_type: crate::Result<String>,
    pub scope_id: crate::Result<String>,
    pub operation: crate::Result<String>,
    pub input_digest: crate::Result<String>,
    pub policy_result: crate::Result<String>,
    pub correlation_id: crate::Result<String>,
    pub outcome_json: crate::Result<Option<String>>,
    pub committed_at: crate::Result<String>,
}

impl SqliteDb {
    pub async fn attention_direct_coordination_receipts(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> crate::Result<Vec<AttentionDirectCoordinationReceiptsRow>> {
        let (predicate, values) =
            Self::attention_coordination_scope_visibility_predicate(user_id, project_id, "r");

        let direct_sql = format!(
            "SELECT r.id, r.principal_type, r.principal_id, r.scope_type, r.scope_id,
                    r.operation, r.input_digest, r.policy_result, r.correlation_id,
                    r.outcome_json, r.committed_at
             FROM command_receipt r
             WHERE r.agent_action_execution_id IS NULL
               AND {predicate}
             ORDER BY r.committed_at DESC, r.id DESC
             LIMIT ?"
        );
        let mut direct_query = sqlx::query(&direct_sql);
        for value in &values {
            direct_query = direct_query.bind(value);
        }
        let direct_rows = direct_query.bind(limit).fetch_all(self.pool()).await?;
        Ok(direct_rows
            .into_iter()
            .map(|row| AttentionDirectCoordinationReceiptsRow {
                id: row.try_get("id").map_err(crate::DbError::from),
                principal_type: row.try_get("principal_type").map_err(crate::DbError::from),
                principal_id: row.try_get("principal_id").map_err(crate::DbError::from),
                scope_type: row.try_get("scope_type").map_err(crate::DbError::from),
                scope_id: row.try_get("scope_id").map_err(crate::DbError::from),
                operation: row.try_get("operation").map_err(crate::DbError::from),
                input_digest: row.try_get("input_digest").map_err(crate::DbError::from),
                policy_result: row.try_get("policy_result").map_err(crate::DbError::from),
                correlation_id: row.try_get("correlation_id").map_err(crate::DbError::from),
                outcome_json: row.try_get("outcome_json").map_err(crate::DbError::from),
                committed_at: row.try_get("committed_at").map_err(crate::DbError::from),
            })
            .collect())
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct GenesisControlTransferReceiptRow {
    row: sqlx::sqlite::SqliteRow,
}
impl GenesisControlTransferReceiptRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn continuation_turn_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("continuation_turn_id")?)
    }
    pub fn genesis_session_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("genesis_session_id")?)
    }
}

impl SqliteDb {
    pub async fn genesis_control_transfer_receipt(
        &self,
        operation: &str,
        source_turn_id: &str,
    ) -> crate::Result<Option<GenesisControlTransferReceiptRow>> {
        let rows = sqlx::query(
            "SELECT id,
                    json_extract(outcome_json, '$.admitted_turn_id') AS continuation_turn_id,
                    json_extract(outcome_json, '$.session_id') AS genesis_session_id
             FROM command_receipt
             WHERE operation = ?
               AND json_extract(outcome_json, '$.source_turn_id') = ?
               AND json_extract(outcome_json, '$.control_transfer') = 1
             ORDER BY committed_at DESC, id DESC LIMIT 1",
        )
        .bind(operation)
        .bind(source_turn_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| GenesisControlTransferReceiptRow { row }))
    }
}

pub struct ArtifactCommandConflictingInputDigestInput<'a> {
    pub principal_type: &'a str,
    pub principal_id: &'a str,
    pub scope_type: &'a str,
    pub scope_id: &'a str,
    pub operation: &'a str,
    pub idempotency_key: &'a str,
}

impl SqliteDb {
    pub async fn artifact_command_conflicting_input_digest(
        &self,
        input: ArtifactCommandConflictingInputDigestInput<'_>,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT input_digest FROM command_receipt
             WHERE principal_type = ? AND principal_id = ? AND scope_type = ?
               AND scope_id = ? AND operation = ? AND idempotency_key = ?
             LIMIT 1",
        )
        .bind(input.principal_type)
        .bind(input.principal_id)
        .bind(input.scope_type)
        .bind(input.scope_id)
        .bind(input.operation)
        .bind(input.idempotency_key)
        .fetch_optional(self.pool())
        .await?)
    }
}

pub struct ArtifactReplayCommandInputDigestInput<'a> {
    pub principal_type: &'a str,
    pub principal_id: &'a str,
    pub scope_type: &'a str,
    pub scope_id: &'a str,
    pub operation: &'a str,
    pub idempotency_key: &'a str,
}

impl SqliteDb {
    pub async fn artifact_replay_command_input_digest(
        &self,
        input: ArtifactReplayCommandInputDigestInput<'_>,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT input_digest FROM command_receipt
             WHERE principal_type = ? AND principal_id = ? AND scope_type = ?
               AND scope_id = ? AND operation = ? AND idempotency_key = ?
             LIMIT 1",
        )
        .bind(input.principal_type)
        .bind(input.principal_id)
        .bind(input.scope_type)
        .bind(input.scope_id)
        .bind(input.operation)
        .bind(input.idempotency_key)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn milestone_replay_input_digest(
        &self,
        scope_type: &str,
        scope_id: &str,
        operation: &str,
        idempotency_key: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT input_digest FROM command_receipt
         WHERE scope_type = ? AND scope_id = ? AND operation = ?
           AND idempotency_key = ? LIMIT 1",
        )
        .bind(scope_type)
        .bind(scope_id)
        .bind(operation)
        .bind(idempotency_key)
        .fetch_optional(self.pool())
        .await?)
    }
}
