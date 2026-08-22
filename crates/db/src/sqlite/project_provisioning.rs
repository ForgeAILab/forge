use super::*;

fn map_operation(row: SqliteRow) -> Result<ProjectProvisioningOperation> {
    Ok(ProjectProvisioningOperation {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        status: row.try_get("status")?,
        current_checkpoint: row.try_get("current_checkpoint")?,
        attempt_count: row.try_get("attempt_count")?,
        max_attempts: row.try_get("max_attempts")?,
        lease_owner: row.try_get("lease_owner")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        next_retry_at: row.try_get("next_retry_at")?,
        retryable: row.try_get::<i64, _>("retryable")? != 0,
        last_error_code: row.try_get("last_error_code")?,
        last_error_message: row.try_get("last_error_message")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        completed_at: row.try_get("completed_at")?,
        version: row.try_get("version")?,
    })
}

fn map_checkpoint(row: SqliteRow) -> Result<ProjectProvisioningCheckpoint> {
    Ok(ProjectProvisioningCheckpoint {
        id: row.try_get("id")?,
        operation_id: row.try_get("operation_id")?,
        checkpoint: row.try_get("checkpoint")?,
        status: row.try_get("status")?,
        attempt_count: row.try_get("attempt_count")?,
        error_code: row.try_get("error_code")?,
        error_message: row.try_get("error_message")?,
        details_json: row.try_get("details_json")?,
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        version: row.try_get("version")?,
    })
}

fn map_error(row: SqliteRow) -> Result<ProjectProvisioningError> {
    Ok(ProjectProvisioningError {
        id: row.try_get("id")?,
        operation_id: row.try_get("operation_id")?,
        checkpoint_id: row.try_get("checkpoint_id")?,
        code: row.try_get("code")?,
        message: row.try_get("message")?,
        retryable: row.try_get::<i64, _>("retryable")? != 0,
        attempt_count: row.try_get("attempt_count")?,
        created_at: row.try_get("created_at")?,
    })
}

#[async_trait]
impl ProjectProvisioningRepo for SqliteDb {
    async fn get_provisioning_operation(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectProvisioningOperation>> {
        sqlx::query(
            "SELECT * FROM project_provisioning_operation
             WHERE project_id = ?",
        )
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_operation)
        .transpose()
    }

    async fn get_provisioning_operation_by_id(
        &self,
        id: &str,
    ) -> Result<Option<ProjectProvisioningOperation>> {
        sqlx::query("SELECT * FROM project_provisioning_operation WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_operation)
            .transpose()
    }

    async fn create_provisioning_operation(
        &self,
        input: CreateProjectProvisioningOperation,
    ) -> Result<ProjectProvisioningOperation> {
        sqlx::query(
            "INSERT INTO project_provisioning_operation (
                id, project_id, idempotency_key, status, current_checkpoint,
                max_attempts, lease_owner, lease_expires_at, next_retry_at,
                retryable, last_error_code, last_error_message,
                created_at, updated_at, version
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.idempotency_key)
        .bind(&input.status)
        .bind(&input.current_checkpoint)
        .bind(input.max_attempts)
        .bind(input.lease_owner.as_deref())
        .bind(input.lease_expires_at.as_deref())
        .bind(input.next_retry_at.as_deref())
        .bind(if input.retryable { 1_i64 } else { 0_i64 })
        .bind(input.last_error_code.as_deref())
        .bind(input.last_error_message.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await?;

        self.get_provisioning_operation_by_id(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn update_provisioning_operation(
        &self,
        input: UpdateProjectProvisioningOperation,
    ) -> Result<ProjectProvisioningOperation> {
        let current = self
            .get_provisioning_operation_by_id(&input.id)
            .await?
            .ok_or(DbError::NotFound)?;
        if current.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }

        let mut operation = current;
        if let Some(status) = input.status {
            operation.status = status;
        }
        if let Some(checkpoint) = input.current_checkpoint {
            operation.current_checkpoint = checkpoint;
        }
        if let Some(attempt_count) = input.attempt_count {
            operation.attempt_count = attempt_count;
        }
        if let Some(max_attempts) = input.max_attempts {
            operation.max_attempts = max_attempts;
        }
        if let Some(lease_owner) = input.lease_owner {
            operation.lease_owner = lease_owner;
        }
        if let Some(lease_expires_at) = input.lease_expires_at {
            operation.lease_expires_at = lease_expires_at;
        }
        if let Some(next_retry_at) = input.next_retry_at {
            operation.next_retry_at = next_retry_at;
        }
        if let Some(retryable) = input.retryable {
            operation.retryable = retryable;
        }
        if let Some(last_error_code) = input.last_error_code {
            operation.last_error_code = last_error_code;
        }
        if let Some(last_error_message) = input.last_error_message {
            operation.last_error_message = last_error_message;
        }
        if let Some(completed_at) = input.completed_at {
            operation.completed_at = completed_at;
        }
        operation.updated_at = input.updated_at;

        let result = sqlx::query(
            "UPDATE project_provisioning_operation
             SET status = ?, current_checkpoint = ?, attempt_count = ?, max_attempts = ?,
                 lease_owner = ?, lease_expires_at = ?, next_retry_at = ?, retryable = ?,
                 last_error_code = ?, last_error_message = ?, updated_at = ?,
                 completed_at = ?, version = version + 1
             WHERE id = ? AND version = ?",
        )
        .bind(&operation.status)
        .bind(&operation.current_checkpoint)
        .bind(operation.attempt_count)
        .bind(operation.max_attempts)
        .bind(operation.lease_owner.as_deref())
        .bind(operation.lease_expires_at.as_deref())
        .bind(operation.next_retry_at.as_deref())
        .bind(if operation.retryable { 1_i64 } else { 0_i64 })
        .bind(operation.last_error_code.as_deref())
        .bind(operation.last_error_message.as_deref())
        .bind(&operation.updated_at)
        .bind(operation.completed_at.as_deref())
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }

        self.get_provisioning_operation_by_id(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn list_provisioning_checkpoints(
        &self,
        operation_id: &str,
    ) -> Result<Vec<ProjectProvisioningCheckpoint>> {
        sqlx::query(
            "SELECT * FROM project_provisioning_checkpoint
             WHERE operation_id = ? ORDER BY checkpoint, id",
        )
        .bind(operation_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_checkpoint)
        .collect()
    }

    async fn get_provisioning_checkpoint(
        &self,
        operation_id: &str,
        checkpoint: &str,
    ) -> Result<Option<ProjectProvisioningCheckpoint>> {
        sqlx::query(
            "SELECT * FROM project_provisioning_checkpoint
             WHERE operation_id = ? AND checkpoint = ?",
        )
        .bind(operation_id)
        .bind(checkpoint)
        .fetch_optional(&self.pool)
        .await?
        .map(map_checkpoint)
        .transpose()
    }

    async fn upsert_provisioning_checkpoint(
        &self,
        input: UpsertProjectProvisioningCheckpoint,
    ) -> Result<ProjectProvisioningCheckpoint> {
        sqlx::query(
            "INSERT INTO project_provisioning_checkpoint (
                id, operation_id, checkpoint, status, attempt_count,
                error_code, error_message, details_json, started_at, completed_at,
                created_at, updated_at, version
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
             ON CONFLICT(operation_id, checkpoint) DO UPDATE SET
                status = excluded.status,
                attempt_count = excluded.attempt_count,
                error_code = excluded.error_code,
                error_message = excluded.error_message,
                details_json = excluded.details_json,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at,
                updated_at = excluded.updated_at,
                version = project_provisioning_checkpoint.version + 1",
        )
        .bind(&input.id)
        .bind(&input.operation_id)
        .bind(&input.checkpoint)
        .bind(&input.status)
        .bind(input.attempt_count)
        .bind(input.error_code.as_deref())
        .bind(input.error_message.as_deref())
        .bind(&input.details_json)
        .bind(input.started_at.as_deref())
        .bind(input.completed_at.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await?;

        self.get_provisioning_checkpoint(&input.operation_id, &input.checkpoint)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn list_provisioning_errors(
        &self,
        operation_id: &str,
    ) -> Result<Vec<ProjectProvisioningError>> {
        sqlx::query(
            "SELECT * FROM project_provisioning_error
             WHERE operation_id = ? ORDER BY created_at DESC, id DESC",
        )
        .bind(operation_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_error)
        .collect()
    }

    async fn record_provisioning_error(
        &self,
        input: CreateProjectProvisioningError,
    ) -> Result<ProjectProvisioningError> {
        sqlx::query(
            "INSERT INTO project_provisioning_error (
                id, operation_id, checkpoint_id, code, message,
                retryable, attempt_count, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.operation_id)
        .bind(input.checkpoint_id.as_deref())
        .bind(&input.code)
        .bind(&input.message)
        .bind(if input.retryable { 1_i64 } else { 0_i64 })
        .bind(input.attempt_count)
        .bind(&input.created_at)
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "SELECT * FROM project_provisioning_error
             WHERE id = ?",
        )
        .bind(&input.id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_error)
        .transpose()?
        .ok_or(DbError::NotFound)
    }
}
