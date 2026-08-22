use super::*;

#[async_trait]
impl ProjectExecutionSetupCommandRepo for SqliteDb {
    async fn apply_project_execution_setup_command(
        &self,
        input: ApplyProjectExecutionSetupCommand,
    ) -> Result<AppliedProjectExecutionSetupCommand> {
        if input.project_id != input.receipt.scope_id || input.receipt.scope_type != "project" {
            return Err(DbError::Check(
                "execution-setup command receipt scope does not match Project".to_owned(),
            ));
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let existing = CommandReceiptRepo::get_command_receipt_in_tx(
            self,
            &mut transaction,
            &input.receipt.principal_type,
            &input.receipt.principal_id,
            &input.receipt.scope_type,
            &input.receipt.scope_id,
            &input.receipt.operation,
            &input.receipt.idempotency_key,
            &input.receipt.input_digest,
        )
        .await?;

        let project_row = sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
        ))
        .bind(&input.project_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let project = map_project(project_row)?;

        if let Some(receipt) = existing {
            transaction.commit().await?;
            return Ok(AppliedProjectExecutionSetupCommand {
                project,
                receipt,
                replayed: true,
            });
        }

        if input.bump_project_version {
            let expected_version = input.expected_project_version.ok_or_else(|| {
                DbError::Check(
                    "execution-setup Project mutation is missing expected version".to_owned(),
                )
            })?;
            if project.version != expected_version {
                return Err(DbError::VersionConflict);
            }
            let updated_at = input.receipt.committed_at.clone();
            let result = match (&input.settings, &input.primary_repo_id) {
                (Some(settings), Some(primary_repo_id)) => {
                    sqlx::query(
                        "UPDATE project
                     SET settings = ?, primary_repo_id = ?, version = version + 1, updated_at = ?
                     WHERE id = ? AND version = ?",
                    )
                    .bind(settings)
                    .bind(primary_repo_id.as_deref())
                    .bind(&updated_at)
                    .bind(&input.project_id)
                    .bind(expected_version)
                    .execute(&mut *transaction)
                    .await?
                }
                (Some(settings), None) => {
                    sqlx::query(
                        "UPDATE project
                     SET settings = ?, version = version + 1, updated_at = ?
                     WHERE id = ? AND version = ?",
                    )
                    .bind(settings)
                    .bind(&updated_at)
                    .bind(&input.project_id)
                    .bind(expected_version)
                    .execute(&mut *transaction)
                    .await?
                }
                (None, Some(primary_repo_id)) => {
                    sqlx::query(
                        "UPDATE project
                     SET primary_repo_id = ?, version = version + 1, updated_at = ?
                     WHERE id = ? AND version = ?",
                    )
                    .bind(primary_repo_id.as_deref())
                    .bind(&updated_at)
                    .bind(&input.project_id)
                    .bind(expected_version)
                    .execute(&mut *transaction)
                    .await?
                }
                (None, None) => {
                    sqlx::query(
                        "UPDATE project
                     SET version = version + 1, updated_at = ?
                     WHERE id = ? AND version = ?",
                    )
                    .bind(&updated_at)
                    .bind(&input.project_id)
                    .bind(expected_version)
                    .execute(&mut *transaction)
                    .await?
                }
            };
            if result.rows_affected() == 0 {
                return Err(DbError::VersionConflict);
            }
        }

        if let Some(retry) = &input.provisioning_retry {
            let result = sqlx::query(
                "UPDATE project_provisioning_operation
                 SET status = 'provisioning', current_checkpoint = 'preflight',
                     attempt_count = attempt_count + 1, lease_owner = ?,
                     lease_expires_at = ?, next_retry_at = NULL, retryable = 0,
                     last_error_code = NULL, last_error_message = NULL,
                     completed_at = NULL, updated_at = ?, version = version + 1
                 WHERE id = ? AND project_id = ? AND version = ?
                   AND attempt_count < max_attempts
                   AND (lease_owner IS NULL OR lease_expires_at IS NULL
                        OR lease_expires_at <= ?)",
            )
            .bind(&retry.lease_owner)
            .bind(&retry.lease_expires_at)
            .bind(&retry.updated_at)
            .bind(&retry.operation_id)
            .bind(&input.project_id)
            .bind(retry.expected_version)
            .bind(&retry.updated_at)
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() == 0 {
                return Err(DbError::VersionConflict);
            }
        }

        if let Some(metadata) = &input.provisioning_metadata {
            let operation_state = sqlx::query(
                "SELECT status, version
                 FROM project_provisioning_operation
                 WHERE id = ? AND project_id = ?",
            )
            .bind(&metadata.operation_id)
            .bind(&input.project_id)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(operation_state) = operation_state else {
                return Err(DbError::VersionConflict);
            };
            let operation_status: String = operation_state.try_get("status")?;
            let operation_version: i64 = operation_state.try_get("version")?;
            if operation_status == "provisioning" || operation_version != metadata.expected_version
            {
                return Err(DbError::VersionConflict);
            }

            // Checkpoint evidence is part of the same command boundary. A
            // stale preparation snapshot aborts the whole transaction, so a
            // Project CAS/version conflict cannot leave checkpoints claiming
            // an uncommitted repository or role mutation.
            for checkpoint in &metadata.checkpoints {
                let result = sqlx::query(
                    "UPDATE project_provisioning_checkpoint
                     SET status = ?, attempt_count = ?, error_code = NULL,
                         error_message = NULL, details_json = ?, started_at = ?,
                         completed_at = ?, updated_at = ?, version = version + 1
                     WHERE id = ? AND operation_id = ? AND version = ?",
                )
                .bind(&checkpoint.status)
                .bind(checkpoint.attempt_count)
                .bind(&checkpoint.details_json)
                .bind(&checkpoint.started_at)
                .bind(&checkpoint.completed_at)
                .bind(&metadata.updated_at)
                .bind(&checkpoint.id)
                .bind(&checkpoint.operation_id)
                .bind(checkpoint.expected_version)
                .execute(&mut *transaction)
                .await?;
                if result.rows_affected() == 0 {
                    return Err(DbError::VersionConflict);
                }
            }

            // The operation/version guard above makes checkpoint evidence,
            // operation metadata, Project mutation, event, and receipt one
            // all-or-nothing command. A concurrent lease therefore rolls the
            // whole command back instead of being silently overwritten.
            let result = sqlx::query(
                "UPDATE project_provisioning_operation
                 SET status = ?, current_checkpoint = ?, retryable = ?,
                     lease_owner = NULL, lease_expires_at = NULL,
                     next_retry_at = NULL, last_error_code = NULL,
                     last_error_message = NULL, completed_at = ?,
                     updated_at = ?, version = version + 1
                 WHERE id = ? AND project_id = ? AND version = ?
                   AND status <> 'provisioning'",
            )
            .bind(&metadata.status)
            .bind(&metadata.current_checkpoint)
            .bind(metadata.retryable)
            .bind(&metadata.completed_at)
            .bind(&metadata.updated_at)
            .bind(&metadata.operation_id)
            .bind(&input.project_id)
            .bind(metadata.expected_version)
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() == 0 {
                return Err(DbError::VersionConflict);
            }
        }

        DomainEventRepo::append_event_in_tx(
            self,
            &mut transaction,
            &CreateDomainEvent {
                id: input.receipt.event_id.clone(),
                event_type: "project.execution_setup.command_committed".to_owned(),
                entity_type: "project".to_owned(),
                entity_id: input.project_id.clone(),
                actor_type: input.receipt.principal_type.clone(),
                actor_id: Some(input.receipt.principal_id.clone()),
                scope_type: "project".to_owned(),
                scope_id: input.project_id.clone(),
                correlation_id: input.receipt.correlation_id.clone(),
                causation_id: input.receipt.causation_id.clone(),
                causation_depth: input.receipt.causation_depth,
                dedupe_key: Some(format!(
                    "project-execution-setup-command:{}:{}:{}:{}",
                    input.project_id,
                    input.receipt.principal_id,
                    input.receipt.operation,
                    input.receipt.idempotency_key
                )),
                payload_json: input.receipt.outcome_json.clone(),
                created_at: input.receipt.committed_at.clone(),
            },
        )
        .await?;

        let receipt =
            CommandReceiptRepo::create_command_receipt_in_tx(self, &mut transaction, input.receipt)
                .await?;
        let updated_row = sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
        ))
        .bind(&input.project_id)
        .fetch_one(&mut *transaction)
        .await?;
        let project = map_project(updated_row)?;
        transaction.commit().await?;
        Ok(AppliedProjectExecutionSetupCommand {
            project,
            receipt,
            replayed: false,
        })
    }
}
