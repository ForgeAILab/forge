use super::*;

#[async_trait]
impl AgentActionRepo for SqliteDb {
    async fn create_action(&self, input: CreateAgentAction) -> Result<AgentAction> {
        sqlx::query(
            "INSERT INTO agent_action (
                id, actor_identity_id, scope_type, scope_id, operation, payload_json,
                payload_hash, dedupe_key, correlation_id, causation_id, causation_depth,
                requested_permission, policy_result, policy_reason, status, target_type,
                target_id, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
             ON CONFLICT(actor_identity_id, scope_type, scope_id, dedupe_key) DO NOTHING",
        )
        .bind(&input.id)
        .bind(&input.actor_identity_id)
        .bind(&input.scope_type)
        .bind(&input.scope_id)
        .bind(&input.operation)
        .bind(&input.payload_json)
        .bind(&input.payload_hash)
        .bind(&input.dedupe_key)
        .bind(&input.correlation_id)
        .bind(input.causation_id.as_deref())
        .bind(input.causation_depth)
        .bind(&input.requested_permission)
        .bind(input.policy_result.to_string())
        .bind(input.policy_reason.as_deref())
        .bind(input.status.to_string())
        .bind(input.target_type.as_deref())
        .bind(input.target_id.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await
        .map_err(check_error)?;
        sqlx::query(
            "SELECT * FROM agent_action
             WHERE actor_identity_id = ? AND scope_type = ? AND scope_id = ? AND dedupe_key = ?",
        )
        .bind(&input.actor_identity_id)
        .bind(&input.scope_type)
        .bind(&input.scope_id)
        .bind(&input.dedupe_key)
        .fetch_optional(&self.pool)
        .await?
        .map(map_agent_action)
        .transpose()?
        .ok_or(DbError::NotFound)
    }

    async fn get_action(&self, id: &str) -> Result<Option<AgentAction>> {
        sqlx::query("SELECT * FROM agent_action WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_action)
            .transpose()
    }

    async fn list_actions(&self, query: AgentActionListQuery) -> Result<Vec<AgentAction>> {
        let mut builder =
            sqlx::QueryBuilder::<Sqlite>::new("SELECT * FROM agent_action WHERE 1 = 1");
        if let Some(actor_identity_id) = &query.actor_identity_id {
            builder
                .push(" AND actor_identity_id = ")
                .push_bind(actor_identity_id);
        }
        if let Some(scope_type) = &query.scope_type {
            builder.push(" AND scope_type = ").push_bind(scope_type);
        }
        if let Some(scope_id) = &query.scope_id {
            builder.push(" AND scope_id = ").push_bind(scope_id);
        }
        if let Some(status) = &query.status {
            builder.push(" AND status = ").push_bind(status.to_string());
        }
        builder
            .push(" ORDER BY created_at DESC, id DESC LIMIT ")
            .push_bind(query.limit.clamp(1, 500));
        let rows = builder.build().fetch_all(&self.pool).await?;
        rows.into_iter().map(map_agent_action).collect()
    }

    async fn update_action(&self, input: UpdateAgentAction) -> Result<AgentAction> {
        let current = self.get_action(&input.id).await?.ok_or(DbError::NotFound)?;
        let policy_result = input
            .policy_result
            .unwrap_or_else(|| current.policy_result.clone());
        let policy_reason = input
            .policy_reason
            .unwrap_or_else(|| current.policy_reason.clone());
        let status = input.status.unwrap_or_else(|| current.status.clone());
        let outcome_json = input
            .outcome_json
            .unwrap_or_else(|| current.outcome_json.clone());
        let result = sqlx::query(
            "UPDATE agent_action SET policy_result = ?, policy_reason = ?, status = ?,
                outcome_json = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(policy_result.to_string())
        .bind(policy_reason.as_deref())
        .bind(status.to_string())
        .bind(outcome_json.as_deref())
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        self.get_action(&input.id).await?.ok_or(DbError::NotFound)
    }

    async fn record_action_approval(
        &self,
        input: CreateAgentActionApproval,
    ) -> Result<AgentActionApproval> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let action = sqlx::query("SELECT * FROM agent_action WHERE id = ?")
            .bind(&input.action_id)
            .fetch_optional(&mut *transaction)
            .await?
            .map(map_agent_action)
            .transpose()?
            .ok_or(DbError::NotFound)?;
        if action.version != input.expected_action_version {
            let existing = sqlx::query(
                "SELECT * FROM agent_action_approval
                 WHERE action_id = ? AND approver_identity_id = ?",
            )
            .bind(&input.action_id)
            .bind(&input.approver_identity_id)
            .fetch_optional(&mut *transaction)
            .await?
            .map(map_agent_action_approval)
            .transpose()?;
            if let Some(existing) = existing {
                transaction.commit().await?;
                return Ok(existing);
            }
            return Err(DbError::VersionConflict);
        }

        sqlx::query(
            "INSERT INTO agent_action_approval (
                id, action_id, approver_identity_id, decision, reason, created_at
             ) VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(action_id, approver_identity_id) DO NOTHING",
        )
        .bind(&input.id)
        .bind(&input.action_id)
        .bind(&input.approver_identity_id)
        .bind(input.decision.to_string())
        .bind(input.reason.as_deref())
        .bind(&input.created_at)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        let approval = map_agent_action_approval(
            sqlx::query(
                "SELECT * FROM agent_action_approval
             WHERE action_id = ? AND approver_identity_id = ?",
            )
            .bind(&input.action_id)
            .bind(&input.approver_identity_id)
            .fetch_one(&mut *transaction)
            .await?,
        )?;
        let result = sqlx::query(
            "UPDATE agent_action SET status = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(input.resulting_status.to_string())
        .bind(&input.updated_at)
        .bind(&input.action_id)
        .bind(input.expected_action_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        transaction.commit().await?;
        Ok(approval)
    }

    async fn list_action_approvals(&self, action_id: &str) -> Result<Vec<AgentActionApproval>> {
        sqlx::query(
            "SELECT * FROM agent_action_approval
             WHERE action_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(action_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_action_approval)
        .collect()
    }

    async fn record_action_execution(
        &self,
        input: CreateAgentActionExecution,
    ) -> Result<AgentActionExecution> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let execution = self
            .record_action_execution_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(execution)
    }

    async fn record_action_execution_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateAgentActionExecution,
    ) -> Result<AgentActionExecution> {
        if let Some(existing) = sqlx::query(
            "SELECT * FROM agent_action_execution
             WHERE action_id = ? AND idempotency_key = ?",
        )
        .bind(&input.action_id)
        .bind(&input.idempotency_key)
        .fetch_optional(&mut **transaction)
        .await?
        .map(map_agent_action_execution)
        .transpose()?
        {
            let action = sqlx::query("SELECT * FROM agent_action WHERE id = ?")
                .bind(&input.action_id)
                .fetch_optional(&mut **transaction)
                .await?
                .map(map_agent_action)
                .transpose()?
                .ok_or(DbError::NotFound)?;

            // The execution idempotency key is the replay identity, but the
            // key does not make a different execution payload equivalent.
            // Compare every persisted execution field and the action status
            // outcome that would be advanced by this completion.  IDs,
            // expected versions, and timestamps are transport/coordination
            // metadata and are intentionally not replay identity.
            if existing.action_id != input.action_id
                || existing.attempt != input.attempt
                || existing.status != input.status
                || existing.result_json != input.result_json
                || existing.error != input.error
                || existing.executed_by_type != input.executed_by_type
                || existing.executed_by_id != input.executed_by_id
                || existing.idempotency_key != input.idempotency_key
                || action.status != input.action_status
                || action.outcome_json != input.action_outcome_json
            {
                return Err(DbError::IdempotencyConflict);
            }

            return Ok(existing);
        }

        let action = sqlx::query("SELECT * FROM agent_action WHERE id = ?")
            .bind(&input.action_id)
            .fetch_optional(&mut **transaction)
            .await?
            .map(map_agent_action)
            .transpose()?
            .ok_or(DbError::NotFound)?;
        if action.version != input.expected_action_version {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO agent_action_execution (
                id, action_id, attempt, status, result_json, error, executed_by_type,
                executed_by_id, idempotency_key, created_at, completed_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.action_id)
        .bind(input.attempt)
        .bind(input.status.to_string())
        .bind(input.result_json.as_deref())
        .bind(input.error.as_deref())
        .bind(&input.executed_by_type)
        .bind(&input.executed_by_id)
        .bind(&input.idempotency_key)
        .bind(&input.created_at)
        .bind(input.completed_at.as_deref())
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        let result = sqlx::query(
            "UPDATE agent_action SET status = ?, outcome_json = ?, version = version + 1,
                updated_at = ? WHERE id = ? AND version = ?",
        )
        .bind(input.action_status.to_string())
        .bind(input.action_outcome_json.as_deref())
        .bind(&input.updated_at)
        .bind(&input.action_id)
        .bind(input.expected_action_version)
        .execute(&mut **transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        let execution = map_agent_action_execution(
            sqlx::query(
                "SELECT * FROM agent_action_execution
             WHERE action_id = ? AND idempotency_key = ?",
            )
            .bind(&input.action_id)
            .bind(&input.idempotency_key)
            .fetch_one(&mut **transaction)
            .await?,
        )?;
        Ok(execution)
    }

    async fn get_successful_action_execution(
        &self,
        action_id: &str,
    ) -> Result<Option<AgentActionExecution>> {
        sqlx::query(
            "SELECT * FROM agent_action_execution
             WHERE action_id = ? AND status = 'succeeded'
             ORDER BY attempt ASC, created_at ASC LIMIT 1",
        )
        .bind(action_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_agent_action_execution)
        .transpose()
    }

    async fn list_action_executions(&self, action_id: &str) -> Result<Vec<AgentActionExecution>> {
        sqlx::query(
            "SELECT * FROM agent_action_execution
             WHERE action_id = ? ORDER BY attempt ASC, created_at ASC, id ASC",
        )
        .bind(action_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_action_execution)
        .collect()
    }
}

fn map_agent_action(row: SqliteRow) -> Result<AgentAction> {
    Ok(AgentAction {
        id: row.try_get("id")?,
        actor_identity_id: row.try_get("actor_identity_id")?,
        scope_type: row.try_get("scope_type")?,
        scope_id: row.try_get("scope_id")?,
        operation: row.try_get("operation")?,
        payload_json: row.try_get("payload_json")?,
        payload_hash: row.try_get("payload_hash")?,
        dedupe_key: row.try_get("dedupe_key")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        causation_depth: row.try_get("causation_depth")?,
        requested_permission: row.try_get("requested_permission")?,
        policy_result: parse_enum(row.try_get("policy_result")?)?,
        policy_reason: row.try_get("policy_reason")?,
        status: parse_enum(row.try_get("status")?)?,
        target_type: row.try_get("target_type")?,
        target_id: row.try_get("target_id")?,
        outcome_json: row.try_get("outcome_json")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_agent_action_approval(row: SqliteRow) -> Result<AgentActionApproval> {
    Ok(AgentActionApproval {
        id: row.try_get("id")?,
        action_id: row.try_get("action_id")?,
        approver_identity_id: row.try_get("approver_identity_id")?,
        decision: parse_enum(row.try_get("decision")?)?,
        reason: row.try_get("reason")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_agent_action_execution(row: SqliteRow) -> Result<AgentActionExecution> {
    Ok(AgentActionExecution {
        id: row.try_get("id")?,
        action_id: row.try_get("action_id")?,
        attempt: row.try_get("attempt")?,
        status: parse_enum(row.try_get("status")?)?,
        result_json: row.try_get("result_json")?,
        error: row.try_get("error")?,
        executed_by_type: row.try_get("executed_by_type")?,
        executed_by_id: row.try_get("executed_by_id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        created_at: row.try_get("created_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}
