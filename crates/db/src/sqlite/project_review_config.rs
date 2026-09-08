use super::*;

#[async_trait]
impl ProjectReviewConfigCommandRepo for SqliteDb {
    async fn apply_project_review_config_command(
        &self,
        input: ApplyProjectReviewConfigCommand,
    ) -> Result<AppliedProjectReviewConfigCommand> {
        if input.project_id != input.receipt.scope_id || input.receipt.scope_type != "project" {
            return Err(DbError::Check(
                "review-config command receipt scope does not match Project".to_owned(),
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
            return Ok(AppliedProjectReviewConfigCommand {
                project,
                receipt,
                replayed: true,
            });
        }

        if project.version != input.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        let update = sqlx::query(
            "UPDATE project
             SET settings = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.settings)
        .bind(&input.receipt.committed_at)
        .bind(&input.project_id)
        .bind(input.expected_project_version)
        .execute(&mut *transaction)
        .await?;
        if update.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }

        DomainEventRepo::append_event_in_tx(
            self,
            &mut transaction,
            &CreateDomainEvent {
                id: input.receipt.event_id.clone(),
                event_type: "project.review_config.updated".to_owned(),
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
                    "project-review-config-command:{}:{}:{}:{}",
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

        Ok(AppliedProjectReviewConfigCommand {
            project,
            receipt,
            replayed: false,
        })
    }
}
