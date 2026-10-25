//! Project media attachment mutations and their command receipts.

use super::*;

impl SqliteDb {
    pub(super) async fn apply_project_media_attachment_mutation(
        &self,
        input: CreateProjectMediaAttachmentMutation,
    ) -> Result<ProjectMediaAttachment> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let mut attachment = input.attachment.clone();
        let command_receipt = input.command_receipt.as_ref();
        if command_receipt.is_none() && input.action_execution.is_some() {
            return Err(DbError::Check(
                "AgentAction execution requires a command receipt".to_owned(),
            ));
        }
        if let Some(receipt) = command_receipt {
            // The DB boundary does not accept a caller-selected Project or
            // operation.  A receipt is valid only for this canonical evidence
            // command and the exact mutation identity supplied to the method.
            if receipt.scope_type != "project"
                || receipt.scope_id != attachment.project_id
                || receipt.operation != "project.evidence"
                || receipt.idempotency_key != input.idempotency_key
                || receipt.principal_type.trim().is_empty()
                || receipt.principal_id.trim().is_empty()
            {
                return Err(DbError::IdempotencyConflict);
            }
            if attachment.author_type != receipt.principal_type
                || attachment.author_id.as_deref() != Some(receipt.principal_id.as_str())
            {
                return Err(DbError::IdempotencyConflict);
            }

            if let Some(existing) = CommandReceiptRepo::get_command_receipt_in_tx(
                self,
                &mut transaction,
                &receipt.principal_type,
                &receipt.principal_id,
                &receipt.scope_type,
                &receipt.scope_id,
                &receipt.operation,
                &receipt.idempotency_key,
                &receipt.input_digest,
            )
            .await?
            {
                if receipt.policy_result != existing.policy_result
                    || receipt.correlation_id != existing.correlation_id
                    || receipt.causation_id != existing.causation_id
                    || receipt.causation_depth != existing.causation_depth
                {
                    return Err(DbError::IdempotencyConflict);
                }
                if let Some(action_execution) = input.action_execution.as_ref() {
                    let replayed = AgentActionRepo::record_action_execution_in_tx(
                        self,
                        &mut transaction,
                        action_execution.clone(),
                    )
                    .await?;
                    if existing.agent_action_execution_id.is_none()
                        || replayed.result_json.as_deref() != Some(existing.outcome_json.as_str())
                        || replayed.executed_by_type != existing.principal_type
                        || replayed.executed_by_id != existing.principal_id
                        || replayed.idempotency_key != existing.idempotency_key
                    {
                        return Err(DbError::IdempotencyConflict);
                    }
                }
                let attachment = replay_project_media_attachment_command_in_tx(
                    &mut transaction,
                    &input,
                    &existing,
                )
                .await?;
                transaction.commit().await?;
                return Ok(attachment);
            }

            // A fresh receipt-only native Project command must be checked
            // against the current binding while this same BEGIN IMMEDIATE
            // transaction is held.  Exact replays returned above remain
            // intentionally independent of the live binding.
            crate::sqlite::orchestration::reauthorize_direct_project_command_in_tx(
                &mut transaction,
                receipt,
            )
            .await?;
        }

        let dedupe_key = command_receipt.map_or_else(
            || {
                format!(
                    "project-evidence-attach:{}:{}",
                    attachment.project_id, input.idempotency_key
                )
            },
            |receipt| {
                format!(
                    "command-event:{}:{}:{}",
                    receipt.scope_id, receipt.operation, receipt.idempotency_key
                )
            },
        );
        if command_receipt.is_none() {
            if let Some(existing) = sqlx::query(
                "SELECT entity_id, event_type, payload_json
                 FROM domain_event WHERE dedupe_key = ?",
            )
            .bind(&dedupe_key)
            .fetch_optional(&mut *transaction)
            .await?
            {
                let entity_id: String = existing.try_get("entity_id")?;
                let event_type: String = existing.try_get("event_type")?;
                let payload_json: String = existing.try_get("payload_json")?;
                let payload: serde_json::Value = serde_json::from_str(&payload_json)
                    .map_err(|error| DbError::Check(format!("invalid evidence event: {error}")))?;
                if event_type != "project.evidence.attached"
                    || payload.get("project_id").and_then(|v| v.as_str())
                        != Some(attachment.project_id.as_str())
                    || payload.get("milestone_id").and_then(|v| v.as_str())
                        != attachment.milestone_id.as_deref()
                    || payload.get("asset_id").and_then(|v| v.as_str())
                        != Some(attachment.asset_id.as_str())
                    || payload.get("checksum").and_then(|v| v.as_str())
                        != attachment.checksum.as_deref()
                    || payload.get("mutation_fingerprint").and_then(|v| v.as_str())
                        != Some(input.mutation_fingerprint.as_str())
                    || payload
                        .get("expected_milestone_version")
                        .and_then(|v| v.as_i64())
                        != Some(input.expected_milestone_version)
                {
                    return Err(DbError::IdempotencyConflict);
                }
                let row = sqlx::query(&format!(
                    "SELECT {PROJECT_MEDIA_ATTACHMENT_COLUMNS}
                     FROM project_media_attachment WHERE id = ?"
                ))
                .bind(&entity_id)
                .fetch_one(&mut *transaction)
                .await?;
                let attachment = map_project_media_attachment(&row)?;
                transaction.commit().await?;
                return Ok(attachment);
            }
        }

        if attachment.attachment_kind != "evidence" {
            return Err(DbError::Check(
                "Project media mutation must create evidence".to_owned(),
            ));
        }

        let milestone_id = attachment
            .milestone_id
            .as_deref()
            .ok_or_else(|| DbError::Check("evidence milestone is required".to_owned()))?
            .to_owned();
        let milestone = sqlx::query(
            "SELECT project_id, version, current_definition_revision_id
             FROM project_milestone WHERE id = ?",
        )
        .bind(&milestone_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let milestone_project: String = milestone.try_get("project_id")?;
        if milestone_project != attachment.project_id {
            return Err(DbError::Check(
                "evidence milestone is outside the Project".to_owned(),
            ));
        }
        let milestone_version: i64 = milestone.try_get("version")?;
        if milestone_version != input.expected_milestone_version {
            return Err(DbError::VersionConflict);
        }
        let current_definition_revision_id: Option<String> =
            milestone.try_get("current_definition_revision_id")?;

        populate_evidence_context_in_tx(
            &mut transaction,
            &mut attachment,
            current_definition_revision_id.as_deref(),
        )
        .await?;

        ensure_asset_attachable(
            &mut transaction,
            &attachment.asset_id,
            &attachment.project_id,
        )
        .await?;
        if attachment.availability != "available" {
            return Err(DbError::Check(
                "new evidence references must be available".to_owned(),
            ));
        }
        let attachment_checksum = attachment
            .checksum
            .as_deref()
            .filter(|checksum| is_sha256_digest(checksum))
            .ok_or_else(|| DbError::Check("evidence checksum is required".to_owned()))?;
        let asset_checksum = sqlx::query_scalar::<_, Option<String>>(
            "SELECT checksum FROM media_asset WHERE id = ? AND project_id = ?",
        )
        .bind(&attachment.asset_id)
        .bind(&attachment.project_id)
        .fetch_optional(&mut *transaction)
        .await?
        .flatten();
        if asset_checksum.as_deref() != Some(attachment_checksum) {
            return Err(DbError::Check(
                "evidence checksum does not match the media asset".to_owned(),
            ));
        }
        if let Some(task_id) = attachment.task_id.as_deref() {
            let task_project =
                sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                    .bind(task_id)
                    .fetch_optional(&mut *transaction)
                    .await?
                    .ok_or(DbError::NotFound)?;
            if task_project != attachment.project_id {
                return Err(DbError::Check(
                    "evidence Task is outside the Project".to_owned(),
                ));
            }
        }
        if let Some(task_media_id) = attachment.task_media_id.as_deref() {
            let task_media = sqlx::query(
                "SELECT tm.asset_id, tm.task_id, t.project_id
                 FROM task_media tm JOIN task t ON t.id = tm.task_id
                 WHERE tm.id = ?",
            )
            .bind(task_media_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
            let task_media_asset_id: String = task_media.try_get("asset_id")?;
            let task_media_task_id: String = task_media.try_get("task_id")?;
            let task_media_project: String = task_media.try_get("project_id")?;
            if task_media_project != attachment.project_id
                || task_media_asset_id != attachment.asset_id
                || attachment.task_id.as_deref() != Some(task_media_task_id.as_str())
            {
                return Err(DbError::Check(
                    "evidence Task media is outside the attached Project scope".to_owned(),
                ));
            }
        }

        if let Some(source_task_id) = attachment.source_task_id.as_deref() {
            let source_project =
                sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                    .bind(source_task_id)
                    .fetch_optional(&mut *transaction)
                    .await?
                    .ok_or(DbError::NotFound)?;
            if source_project != attachment.project_id
                || attachment.task_id.as_deref() != Some(source_task_id)
            {
                return Err(DbError::Check(
                    "evidence source Task is outside the attached Task scope".to_owned(),
                ));
            }
        }
        if let Some(source_execution_id) = attachment.source_execution_id.as_deref() {
            let source_execution = sqlx::query(
                "SELECT e.task_id, t.project_id
                 FROM execution e JOIN task t ON t.id = e.task_id
                 WHERE e.id = ?",
            )
            .bind(source_execution_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
            let execution_task_id: String = source_execution.try_get("task_id")?;
            let execution_project: String = source_execution.try_get("project_id")?;
            if execution_project != attachment.project_id
                || attachment.task_id.as_deref() != Some(execution_task_id.as_str())
            {
                return Err(DbError::Check(
                    "evidence source execution is outside the attached Task scope".to_owned(),
                ));
            }
        }

        let check_ids: Vec<String> = serde_json::from_str(&attachment.acceptance_check_ids_json)
            .map_err(|error| DbError::Check(format!("invalid acceptance check list: {error}")))?;
        if let Some(milestone_check_id) = attachment.milestone_check_id.as_deref() {
            if !check_ids
                .iter()
                .any(|check_id| check_id == milestone_check_id)
            {
                return Err(DbError::Check(
                    "evidence milestone check must be in the acceptance check list".to_owned(),
                ));
            }
        }
        for check_id in &check_ids {
            let check = sqlx::query(
                "SELECT project_id, milestone_id, definition_revision_id
                 FROM project_milestone_check WHERE id = ?",
            )
            .bind(check_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
            let check_project: String = check.try_get("project_id")?;
            let check_milestone: String = check.try_get("milestone_id")?;
            let check_revision: String = check.try_get("definition_revision_id")?;
            if check_project != attachment.project_id || check_milestone != milestone_id {
                return Err(DbError::Check(
                    "evidence acceptance check is outside the milestone".to_owned(),
                ));
            }
            if current_definition_revision_id.as_deref() != Some(check_revision.as_str()) {
                return Err(DbError::Check(
                    "evidence acceptance check is not from the current milestone definition"
                        .to_owned(),
                ));
            }
        }
        if let Some(source_validation_id) = attachment.source_validation_id.as_deref() {
            let validation = sqlx::query(
                "SELECT project_id, milestone_id, check_id, definition_revision_id
                 FROM project_milestone_check_result WHERE id = ?",
            )
            .bind(source_validation_id)
            .fetch_optional(&mut *transaction)
            .await?
            // A bare NotFound is redacted at the model boundary into "resource
            // unavailable", which hides the one mistake callers actually make
            // here: citing the acceptance-check id instead of the recorded
            // result's own id.
            .ok_or_else(|| {
                DbError::Check(
                    "evidence source_validation_id must name a recorded validation result \
                     (the result id returned by recording the validation), not an \
                     acceptance-check id"
                        .to_owned(),
                )
            })?;
            let validation_project: String = validation.try_get("project_id")?;
            let validation_milestone: String = validation.try_get("milestone_id")?;
            let validation_check: String = validation.try_get("check_id")?;
            let validation_revision: String = validation.try_get("definition_revision_id")?;
            if validation_project != attachment.project_id
                || validation_milestone != milestone_id
                || !check_ids.iter().any(|id| id == &validation_check)
                || current_definition_revision_id.as_deref() != Some(validation_revision.as_str())
            {
                return Err(DbError::Check(
                    "evidence source validation is outside the current milestone definition"
                        .to_owned(),
                ));
            }
        }

        sqlx::query(
            "INSERT INTO project_media_attachment (
                id, project_id, asset_id, attachment_kind, task_media_id, task_id,
                milestone_id, milestone_check_id, source_task_id, source_execution_id,
                source_validation_id, source_task_version, source_context_digest,
                source_definition_revision_id, acceptance_check_ids_json, caption, evidence_kind,
                checksum, availability, project_url, author_type, author_id,
                authorization_json, version, created_at, deleted_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, NULL, ?)",
        )
        .bind(&attachment.id)
        .bind(&attachment.project_id)
        .bind(&attachment.asset_id)
        .bind(&attachment.attachment_kind)
        .bind(attachment.task_media_id.as_deref())
        .bind(attachment.task_id.as_deref())
        .bind(attachment.milestone_id.as_deref())
        .bind(attachment.milestone_check_id.as_deref())
        .bind(attachment.source_task_id.as_deref())
        .bind(attachment.source_execution_id.as_deref())
        .bind(attachment.source_validation_id.as_deref())
        .bind(attachment.source_task_version)
        .bind(attachment.source_context_digest.as_deref())
        .bind(attachment.source_definition_revision_id.as_deref())
        .bind(&attachment.acceptance_check_ids_json)
        .bind(attachment.caption.as_deref())
        .bind(attachment.evidence_kind.as_deref())
        .bind(attachment.checksum.as_deref())
        .bind(&attachment.availability)
        .bind(attachment.project_url.as_deref())
        .bind(&attachment.author_type)
        .bind(attachment.author_id.as_deref())
        .bind(&attachment.authorization_json)
        .bind(&attachment.created_at)
        .bind(&attachment.created_at)
        .execute(&mut *transaction)
        .await?;
        let asset = reconcile_media_asset_in_tx(
            &mut transaction,
            &attachment.asset_id,
            &attachment.created_at,
        )
        .await?;
        if asset.gc_state == "deleted" {
            return Err(DbError::Check(
                "media asset was deleted while attaching evidence".to_owned(),
            ));
        }

        let mut payload = serde_json::json!({
            "project_id": attachment.project_id,
            "milestone_id": milestone_id,
            "asset_id": attachment.asset_id,
            "evidence_id": attachment.id,
            "checksum": attachment.checksum,
            "source_task_version": attachment.source_task_version,
            "source_context_digest": attachment.source_context_digest,
            "source_definition_revision_id": attachment.source_definition_revision_id,
            "expected_milestone_version": input.expected_milestone_version,
            "authorization_event_id": input.authorization_event_id,
            "mutation_fingerprint": input.mutation_fingerprint,
        });
        if command_receipt.is_some() {
            payload["attachment"] = serde_json::json!({
                "id": attachment.id,
                "project_id": attachment.project_id,
                "asset_id": attachment.asset_id,
                "attachment_kind": attachment.attachment_kind,
                "task_media_id": attachment.task_media_id,
                "task_id": attachment.task_id,
                "milestone_id": attachment.milestone_id,
                "milestone_check_id": attachment.milestone_check_id,
                "source_task_id": attachment.source_task_id,
                "source_execution_id": attachment.source_execution_id,
                "source_validation_id": attachment.source_validation_id,
                "acceptance_check_ids_json": attachment.acceptance_check_ids_json,
                "caption": attachment.caption,
                "evidence_kind": attachment.evidence_kind,
                "checksum": attachment.checksum,
                "availability": attachment.availability,
                "project_url": attachment.project_url,
                "author_type": attachment.author_type,
                "author_id": attachment.author_id,
                "authorization_json": attachment.authorization_json,
            });
        }
        let payload_json = payload.to_string();
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.evidence.attached".to_owned(),
            entity_type: "project_media_attachment".to_owned(),
            entity_id: attachment.id.clone(),
            actor_type: command_receipt
                .map(|receipt| receipt.principal_type.clone())
                .unwrap_or_else(|| attachment.author_type.clone()),
            actor_id: command_receipt
                .map(|receipt| Some(receipt.principal_id.clone()))
                .unwrap_or_else(|| attachment.author_id.clone()),
            scope_type: command_receipt
                .map(|receipt| receipt.scope_type.clone())
                .unwrap_or_else(|| "project".to_owned()),
            scope_id: command_receipt
                .map(|receipt| receipt.scope_id.clone())
                .unwrap_or_else(|| attachment.project_id.clone()),
            correlation_id: command_receipt
                .map(|receipt| receipt.correlation_id.clone())
                .unwrap_or_else(|| dedupe_key.clone()),
            causation_id: command_receipt.and_then(|receipt| receipt.causation_id.clone()),
            causation_depth: command_receipt.map_or(0, |receipt| receipt.causation_depth),
            dedupe_key: Some(dedupe_key),
            payload_json,
            created_at: attachment.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        finalize_command_in_tx(
            self,
            &mut transaction,
            &event.id,
            input.command_receipt.clone(),
            input.action_execution.clone(),
        )
        .await?;
        let row = sqlx::query(&format!(
            "SELECT {PROJECT_MEDIA_ATTACHMENT_COLUMNS}
             FROM project_media_attachment WHERE id = ?"
        ))
        .bind(&attachment.id)
        .fetch_one(&mut *transaction)
        .await?;
        let attachment = map_project_media_attachment(&row)?;
        transaction.commit().await?;
        Ok(attachment)
    }
}
