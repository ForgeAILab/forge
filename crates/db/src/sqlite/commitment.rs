use super::*;

#[async_trait]
impl AgentCommitmentRepo for SqliteDb {
    async fn create_commitment(&self, input: CreateAgentCommitment) -> Result<AgentCommitment> {
        sqlx::query(
            "INSERT INTO agent_commitment (
                id, owner_identity_id, scope_type, scope_id, title, description, status,
                due_at, correlation_id, originating_action_id, originating_task_id,
                evidence_required, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.owner_identity_id)
        .bind(&input.scope_type)
        .bind(&input.scope_id)
        .bind(&input.title)
        .bind(input.description.as_deref())
        .bind(input.status.to_string())
        .bind(input.due_at.as_deref())
        .bind(&input.correlation_id)
        .bind(input.originating_action_id.as_deref())
        .bind(input.originating_task_id.as_deref())
        .bind(if input.evidence_required { 1 } else { 0 })
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await
        .map_err(check_error)?;
        self.get_commitment(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn get_commitment(&self, id: &str) -> Result<Option<AgentCommitment>> {
        sqlx::query("SELECT * FROM agent_commitment WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_commitment)
            .transpose()
    }

    async fn list_commitments(
        &self,
        query: AgentCommitmentListQuery,
    ) -> Result<Vec<AgentCommitment>> {
        let mut builder =
            sqlx::QueryBuilder::<Sqlite>::new("SELECT * FROM agent_commitment WHERE 1 = 1");
        if let Some(owner_identity_id) = &query.owner_identity_id {
            builder
                .push(" AND owner_identity_id = ")
                .push_bind(owner_identity_id);
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
            .push(" ORDER BY COALESCE(due_at, created_at) ASC, created_at ASC, id ASC LIMIT ")
            .push_bind(query.limit.clamp(1, 500));
        let rows = builder.build().fetch_all(&self.pool).await?;
        rows.into_iter().map(map_agent_commitment).collect()
    }

    async fn update_commitment(&self, input: UpdateAgentCommitment) -> Result<AgentCommitment> {
        let current = self
            .get_commitment(&input.id)
            .await?
            .ok_or(DbError::NotFound)?;
        if current.version != input.expected_version {
            if self.lifecycle_exists(&input.id, &input.dedupe_key).await? {
                return Ok(current);
            }
            return Err(DbError::VersionConflict);
        }

        let status = input.status.unwrap_or_else(|| current.status.clone());
        let due_at = input.due_at.unwrap_or_else(|| current.due_at.clone());
        let description = input
            .description
            .unwrap_or_else(|| current.description.clone());
        let blocked_reason = input
            .blocked_reason
            .unwrap_or_else(|| current.blocked_reason.clone());
        let cancellation_reason = input
            .cancellation_reason
            .unwrap_or_else(|| current.cancellation_reason.clone());
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let result = sqlx::query(
            "UPDATE agent_commitment SET
                status = ?, due_at = ?, description = ?, blocked_reason = ?,
                cancellation_reason = CASE WHEN ? = 'cancelled' THEN ? ELSE cancellation_reason END,
                cancelled_at = CASE WHEN ? = 'cancelled' THEN ? ELSE cancelled_at END,
                version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(status.to_string())
        .bind(due_at.as_deref())
        .bind(description.as_deref())
        .bind(blocked_reason.as_deref())
        .bind(status.to_string())
        .bind(cancellation_reason.as_deref())
        .bind(status.to_string())
        .bind(if status == AgentCommitmentStatus::Cancelled {
            Some(input.updated_at.as_str())
        } else {
            None
        })
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }

        insert_commitment_lifecycle(
            &mut transaction,
            &CreateLifecycle {
                commitment_id: &input.id,
                from_status: Some(&current.status),
                to_status: &status,
                actor_type: &input.actor_type,
                actor_id: &input.actor_id,
                reason: input.reason.as_deref(),
                evidence_id: input.evidence_id.as_deref(),
                dedupe_key: &input.dedupe_key,
                id: new_uuid_v4(),
                created_at: &input.updated_at,
            },
        )
        .await?;
        transaction.commit().await?;
        self.get_commitment(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn complete_commitment(&self, input: CompleteAgentCommitment) -> Result<AgentCommitment> {
        let current = self
            .get_commitment(&input.id)
            .await?
            .ok_or(DbError::NotFound)?;
        if current.status == AgentCommitmentStatus::Completed
            && self.lifecycle_exists(&input.id, &input.dedupe_key).await?
        {
            return Ok(current);
        }
        if current.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        if current.status == AgentCommitmentStatus::Cancelled {
            return Err(DbError::InvalidTransition);
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        insert_commitment_evidence(&mut transaction, &input.evidence).await?;
        let result = sqlx::query(
            "UPDATE agent_commitment SET status = 'completed', completed_at = ?,
                blocked_reason = NULL, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?
               AND status NOT IN ('completed', 'cancelled')",
        )
        .bind(&input.completed_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        insert_commitment_lifecycle(
            &mut transaction,
            &CreateLifecycle {
                commitment_id: &input.id,
                from_status: Some(&current.status),
                to_status: &AgentCommitmentStatus::Completed,
                actor_type: &input.actor_type,
                actor_id: &input.actor_id,
                reason: input.reason.as_deref(),
                evidence_id: Some(&input.evidence.id),
                dedupe_key: &input.dedupe_key,
                id: new_uuid_v4(),
                created_at: &input.updated_at,
            },
        )
        .await?;
        transaction.commit().await?;
        self.get_commitment(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn transfer_commitment(&self, input: TransferAgentCommitment) -> Result<AgentCommitment> {
        let current = self
            .get_commitment(&input.id)
            .await?
            .ok_or(DbError::NotFound)?;
        if current.owner_identity_id == input.to_identity_id {
            if self.lifecycle_exists(&input.id, &input.dedupe_key).await? {
                return Ok(current);
            }
            return Err(DbError::Check(
                "commitment is already owned by identity".to_owned(),
            ));
        }
        if current.version != input.expected_version {
            if self.lifecycle_exists(&input.id, &input.dedupe_key).await? {
                return Ok(current);
            }
            return Err(DbError::VersionConflict);
        }
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        sqlx::query(
            "INSERT INTO agent_commitment_transfer (
                id, commitment_id, from_identity_id, to_identity_id, reason,
                actor_type, actor_id, dedupe_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(commitment_id, dedupe_key) DO NOTHING",
        )
        .bind(new_uuid_v4())
        .bind(&input.id)
        .bind(&current.owner_identity_id)
        .bind(&input.to_identity_id)
        .bind(&input.reason)
        .bind(&input.actor_type)
        .bind(&input.actor_id)
        .bind(&input.dedupe_key)
        .bind(&input.updated_at)
        .execute(&mut *transaction)
        .await?;
        let result = sqlx::query(
            "UPDATE agent_commitment SET owner_identity_id = ?, version = version + 1,
                updated_at = ? WHERE id = ? AND version = ?",
        )
        .bind(&input.to_identity_id)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        insert_commitment_lifecycle(
            &mut transaction,
            &CreateLifecycle {
                commitment_id: &input.id,
                from_status: Some(&current.status),
                to_status: &current.status,
                actor_type: &input.actor_type,
                actor_id: &input.actor_id,
                reason: Some(&input.reason),
                evidence_id: None,
                dedupe_key: &input.dedupe_key,
                id: new_uuid_v4(),
                created_at: &input.updated_at,
            },
        )
        .await?;
        transaction.commit().await?;
        self.get_commitment(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn add_commitment_evidence(
        &self,
        input: CreateAgentCommitmentEvidence,
    ) -> Result<AgentCommitmentEvidence> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        insert_commitment_evidence(&mut transaction, &input).await?;
        transaction.commit().await?;
        self.get_commitment_evidence_by_dedupe(&input.commitment_id, &input.dedupe_key)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn list_commitment_evidence(
        &self,
        commitment_id: &str,
    ) -> Result<Vec<AgentCommitmentEvidence>> {
        sqlx::query(
            "SELECT * FROM agent_commitment_evidence
             WHERE commitment_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(commitment_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_commitment_evidence)
        .collect()
    }

    async fn list_commitment_transfers(
        &self,
        commitment_id: &str,
    ) -> Result<Vec<AgentCommitmentTransfer>> {
        sqlx::query(
            "SELECT * FROM agent_commitment_transfer
             WHERE commitment_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(commitment_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_commitment_transfer)
        .collect()
    }

    async fn list_commitment_lifecycle(
        &self,
        commitment_id: &str,
    ) -> Result<Vec<AgentCommitmentLifecycle>> {
        sqlx::query(
            "SELECT * FROM agent_commitment_lifecycle
             WHERE commitment_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(commitment_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_commitment_lifecycle)
        .collect()
    }
}

struct CreateLifecycle<'a> {
    commitment_id: &'a str,
    from_status: Option<&'a AgentCommitmentStatus>,
    to_status: &'a AgentCommitmentStatus,
    actor_type: &'a str,
    actor_id: &'a str,
    reason: Option<&'a str>,
    evidence_id: Option<&'a str>,
    dedupe_key: &'a str,
    id: String,
    created_at: &'a str,
}

async fn insert_commitment_lifecycle(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &CreateLifecycle<'_>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agent_commitment_lifecycle (
            id, commitment_id, from_status, to_status, actor_type, actor_id,
            reason, evidence_id, dedupe_key, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(commitment_id, dedupe_key) DO NOTHING",
    )
    .bind(&input.id)
    .bind(input.commitment_id)
    .bind(input.from_status.map(ToString::to_string))
    .bind(input.to_status.to_string())
    .bind(input.actor_type)
    .bind(input.actor_id)
    .bind(input.reason)
    .bind(input.evidence_id)
    .bind(input.dedupe_key)
    .bind(input.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_commitment_evidence(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &CreateAgentCommitmentEvidence,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agent_commitment_evidence (
            id, commitment_id, evidence_type, evidence_id, scope_type, scope_id,
            description, metadata_json, authorized_by_type, authorized_by_id,
            dedupe_key, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(commitment_id, dedupe_key) DO NOTHING",
    )
    .bind(&input.id)
    .bind(&input.commitment_id)
    .bind(&input.evidence_type)
    .bind(&input.evidence_id)
    .bind(&input.scope_type)
    .bind(&input.scope_id)
    .bind(input.description.as_deref())
    .bind(&input.metadata_json)
    .bind(&input.authorized_by_type)
    .bind(&input.authorized_by_id)
    .bind(&input.dedupe_key)
    .bind(&input.created_at)
    .execute(&mut **transaction)
    .await
    .map_err(check_error)?;
    Ok(())
}

impl SqliteDb {
    async fn lifecycle_exists(&self, commitment_id: &str, dedupe_key: &str) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_commitment_lifecycle
             WHERE commitment_id = ? AND dedupe_key = ?",
        )
        .bind(commitment_id)
        .bind(dedupe_key)
        .fetch_one(&self.pool)
        .await?
            > 0)
    }

    async fn get_commitment_evidence_by_dedupe(
        &self,
        commitment_id: &str,
        dedupe_key: &str,
    ) -> Result<Option<AgentCommitmentEvidence>> {
        sqlx::query(
            "SELECT * FROM agent_commitment_evidence
             WHERE commitment_id = ? AND dedupe_key = ?",
        )
        .bind(commitment_id)
        .bind(dedupe_key)
        .fetch_optional(&self.pool)
        .await?
        .map(map_agent_commitment_evidence)
        .transpose()
    }
}

fn map_agent_commitment(row: SqliteRow) -> Result<AgentCommitment> {
    Ok(AgentCommitment {
        id: row.try_get("id")?,
        owner_identity_id: row.try_get("owner_identity_id")?,
        scope_type: row.try_get("scope_type")?,
        scope_id: row.try_get("scope_id")?,
        title: row.try_get("title")?,
        description: row.try_get("description")?,
        status: parse_enum(row.try_get("status")?)?,
        due_at: row.try_get("due_at")?,
        correlation_id: row.try_get("correlation_id")?,
        originating_action_id: row.try_get("originating_action_id")?,
        originating_task_id: row.try_get("originating_task_id")?,
        evidence_required: row.try_get::<i64, _>("evidence_required")? != 0,
        cancellation_reason: row.try_get("cancellation_reason")?,
        blocked_reason: row.try_get("blocked_reason")?,
        completed_at: row.try_get("completed_at")?,
        cancelled_at: row.try_get("cancelled_at")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_agent_commitment_evidence(row: SqliteRow) -> Result<AgentCommitmentEvidence> {
    Ok(AgentCommitmentEvidence {
        id: row.try_get("id")?,
        commitment_id: row.try_get("commitment_id")?,
        evidence_type: row.try_get("evidence_type")?,
        evidence_id: row.try_get("evidence_id")?,
        scope_type: row.try_get("scope_type")?,
        scope_id: row.try_get("scope_id")?,
        description: row.try_get("description")?,
        metadata_json: row.try_get("metadata_json")?,
        authorized_by_type: row.try_get("authorized_by_type")?,
        authorized_by_id: row.try_get("authorized_by_id")?,
        dedupe_key: row.try_get("dedupe_key")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_agent_commitment_transfer(row: SqliteRow) -> Result<AgentCommitmentTransfer> {
    Ok(AgentCommitmentTransfer {
        id: row.try_get("id")?,
        commitment_id: row.try_get("commitment_id")?,
        from_identity_id: row.try_get("from_identity_id")?,
        to_identity_id: row.try_get("to_identity_id")?,
        reason: row.try_get("reason")?,
        actor_type: row.try_get("actor_type")?,
        actor_id: row.try_get("actor_id")?,
        dedupe_key: row.try_get("dedupe_key")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_agent_commitment_lifecycle(row: SqliteRow) -> Result<AgentCommitmentLifecycle> {
    Ok(AgentCommitmentLifecycle {
        id: row.try_get("id")?,
        commitment_id: row.try_get("commitment_id")?,
        from_status: row
            .try_get::<Option<String>, _>("from_status")?
            .map(parse_enum)
            .transpose()?,
        to_status: parse_enum(row.try_get("to_status")?)?,
        actor_type: row.try_get("actor_type")?,
        actor_id: row.try_get("actor_id")?,
        reason: row.try_get("reason")?,
        evidence_id: row.try_get("evidence_id")?,
        dedupe_key: row.try_get("dedupe_key")?,
        created_at: row.try_get("created_at")?,
    })
}
