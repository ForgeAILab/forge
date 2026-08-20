use super::*;
use crate::{BeginProjectMediaUpload, ProjectMediaUpload};

fn map_media_asset(row: &SqliteRow) -> Result<MediaAsset> {
    Ok(MediaAsset {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        legacy_task_media_id: row.try_get("legacy_task_media_id")?,
        display_filename: row.try_get("display_filename")?,
        content_type: row.try_get("content_type")?,
        byte_size: row.try_get("byte_size")?,
        storage_key: row.try_get("storage_key")?,
        checksum: row.try_get("checksum")?,
        availability: row.try_get("availability")?,
        gc_state: row.try_get("gc_state")?,
        gc_candidate_at: row.try_get("gc_candidate_at")?,
        gc_lease_owner: row.try_get("gc_lease_owner")?,
        gc_lease_expires_at: row.try_get("gc_lease_expires_at")?,
        version: row.try_get("version")?,
        deleted_at: row.try_get("deleted_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_project_media_attachment(row: &SqliteRow) -> Result<ProjectMediaAttachment> {
    Ok(ProjectMediaAttachment {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        asset_id: row.try_get("asset_id")?,
        attachment_kind: row.try_get("attachment_kind")?,
        task_media_id: row.try_get("task_media_id")?,
        task_id: row.try_get("task_id")?,
        milestone_id: row.try_get("milestone_id")?,
        milestone_check_id: row.try_get("milestone_check_id")?,
        source_task_id: row.try_get("source_task_id")?,
        source_execution_id: row.try_get("source_execution_id")?,
        source_validation_id: row.try_get("source_validation_id")?,
        acceptance_check_ids_json: row.try_get("acceptance_check_ids_json")?,
        caption: row.try_get("caption")?,
        evidence_kind: row.try_get("evidence_kind")?,
        checksum: row.try_get("checksum")?,
        availability: row.try_get("availability")?,
        project_url: row.try_get("project_url")?,
        author_type: row.try_get("author_type")?,
        author_id: row.try_get("author_id")?,
        authorization_json: row.try_get("authorization_json")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        deleted_at: row.try_get("deleted_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_project_release_media_pin(row: &SqliteRow) -> Result<ProjectReleaseMediaPin> {
    Ok(ProjectReleaseMediaPin {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        release_id: row.try_get("release_id")?,
        asset_id: row.try_get("asset_id")?,
        attachment_id: row.try_get("attachment_id")?,
        legacy_task_media_id: row.try_get("legacy_task_media_id")?,
        asset_checksum: row.try_get("asset_checksum")?,
        attachment_digest: row.try_get("attachment_digest")?,
        availability: row.try_get("availability")?,
        pin_digest: row.try_get("pin_digest")?,
        created_at: row.try_get("created_at")?,
    })
}

const MEDIA_ASSET_COLUMNS: &str = "id, project_id, legacy_task_media_id, display_filename, content_type, byte_size, storage_key, checksum, availability, gc_state, gc_candidate_at, gc_lease_owner, gc_lease_expires_at, version, deleted_at, created_at, updated_at";
const PROJECT_MEDIA_ATTACHMENT_COLUMNS: &str = "id, project_id, asset_id, attachment_kind, task_media_id, task_id, milestone_id, milestone_check_id, source_task_id, source_execution_id, source_validation_id, acceptance_check_ids_json, caption, evidence_kind, checksum, availability, project_url, author_type, author_id, authorization_json, version, created_at, deleted_at, updated_at";
const PROJECT_RELEASE_MEDIA_PIN_COLUMNS: &str = "id, project_id, release_id, asset_id, attachment_id, legacy_task_media_id, asset_checksum, attachment_digest, availability, pin_digest, created_at";

/// Look up a committed media tombstone receipt in an existing transaction.
///
/// This is deliberately a read-only operation.  The API uses it before
/// checking the caller's current authorization so a retry can resolve from
/// the immutable receipt, while every receipt field (including the
/// authorization principal) remains part of exact idempotency matching.
async fn replay_project_media_tombstone_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &ProjectMediaTombstone,
) -> Result<Option<MediaAsset>> {
    let dedupe_key = format!(
        "project-media-tombstone:{}:{}",
        input.project_id, input.idempotency_key
    );
    let event_type = format!("project.media.{}", input.target_availability);
    let Some(event) = sqlx::query(
        "SELECT entity_id, entity_type, event_type, scope_type, scope_id, payload_json
         FROM domain_event WHERE dedupe_key = ?",
    )
    .bind(&dedupe_key)
    .fetch_optional(&mut **transaction)
    .await?
    else {
        return Ok(None);
    };

    let entity_id: String = event.try_get("entity_id")?;
    let entity_type: String = event.try_get("entity_type")?;
    let persisted_event_type: String = event.try_get("event_type")?;
    let scope_type: String = event.try_get("scope_type")?;
    let scope_id: String = event.try_get("scope_id")?;
    let payload_json: String = event.try_get("payload_json")?;
    let payload: serde_json::Value = serde_json::from_str(&payload_json)
        .map_err(|error| DbError::Check(format!("invalid media tombstone event: {error}")))?;
    let same_request = entity_id == input.asset_id
        && entity_type == "media_asset"
        && persisted_event_type == event_type
        && scope_type == "project"
        && scope_id == input.project_id
        && payload.get("project_id").and_then(|v| v.as_str()) == Some(input.project_id.as_str())
        && payload.get("asset_id").and_then(|v| v.as_str()) == Some(input.asset_id.as_str())
        && payload.get("target_availability").and_then(|v| v.as_str())
            == Some(input.target_availability.as_str())
        && payload.get("expected_version").and_then(|v| v.as_i64()) == Some(input.expected_version)
        && payload.get("mutation_fingerprint").and_then(|v| v.as_str())
            == Some(input.mutation_fingerprint.as_str())
        && payload.get("principal_type").and_then(|v| v.as_str())
            == Some(input.principal_type.as_str())
        && payload.get("principal_id").and_then(|v| v.as_str())
            == Some(input.principal_id.as_str())
        && payload.get("reason").and_then(|v| v.as_str()) == Some(input.reason.as_str())
        && payload.get("authorization_basis").and_then(|v| v.as_str())
            == Some(input.authorization_basis.as_str())
        && payload.get("authorization_action").and_then(|v| v.as_str())
            == Some(input.authorization_action.as_str())
        && payload
            .get("authorization_occurred_at")
            .and_then(|v| v.as_str())
            == Some(input.authorization_occurred_at.as_str())
        && payload
            .get("authorization_event_id")
            .and_then(|v| v.as_str())
            == Some(input.authorization_event_id.as_str())
        && payload.get("authorization_json").and_then(|v| v.as_str())
            == Some(input.authorization_json.as_str());
    if !same_request {
        return Err(DbError::IdempotencyConflict);
    }

    let row = sqlx::query(&format!(
        "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset
         WHERE id = ? AND project_id = ?"
    ))
    .bind(&input.asset_id)
    .bind(&input.project_id)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(Some(map_media_asset(&row)?))
}

#[async_trait]
impl SharedMediaRepo for SqliteDb {
    async fn get_media_asset(&self, asset_id: &str) -> Result<Option<MediaAsset>> {
        sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
        ))
        .bind(asset_id)
        .fetch_optional(self.pool())
        .await?
        .map(|row| map_media_asset(&row))
        .transpose()
    }

    async fn get_media_asset_for_task_media(
        &self,
        task_media_id: &str,
    ) -> Result<Option<MediaAsset>> {
        sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE legacy_task_media_id = ?"
        ))
        .bind(task_media_id)
        .fetch_optional(self.pool())
        .await?
        .map(|row| map_media_asset(&row))
        .transpose()
    }

    async fn begin_project_media_upload(
        &self,
        input: BeginProjectMediaUpload,
    ) -> Result<ProjectMediaUpload> {
        if input.byte_size < 0 || !is_sha256_digest(&input.checksum) {
            return Err(DbError::Check(
                "project media upload requires a valid byte size and SHA-256 checksum".to_owned(),
            ));
        }
        validate_project_media_metadata(
            &input.display_filename,
            &input.content_type,
            &input.final_storage_key,
            &input.staging_storage_key,
        )?;
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let dedupe_key = format!(
            "project-media-upload:{}:{}",
            input.project_id, input.idempotency_key
        );

        // Resolve a committed retry before checking the mutable Project
        // version. A lost response must remain replayable after another
        // Project mutation advances its version.
        if let Some(event) = sqlx::query(
            "SELECT entity_id, event_type, payload_json
             FROM domain_event WHERE dedupe_key = ?",
        )
        .bind(&dedupe_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let entity_id: String = event.try_get("entity_id")?;
            let event_type: String = event.try_get("event_type")?;
            let payload_json: String = event.try_get("payload_json")?;
            let payload: serde_json::Value = serde_json::from_str(&payload_json)
                .map_err(|error| DbError::Check(format!("invalid media upload event: {error}")))?;
            if event_type != "project.media.uploaded"
                || payload.get("project_id").and_then(|v| v.as_str())
                    != Some(input.project_id.as_str())
                || payload.get("filename").and_then(|v| v.as_str())
                    != Some(input.display_filename.as_str())
                || payload.get("content_type").and_then(|v| v.as_str())
                    != Some(input.content_type.as_str())
                || payload.get("byte_size").and_then(|v| v.as_i64()) != Some(input.byte_size)
                || payload.get("checksum").and_then(|v| v.as_str()) != Some(input.checksum.as_str())
                || payload.get("mutation_fingerprint").and_then(|v| v.as_str())
                    != Some(input.mutation_fingerprint.as_str())
                || payload
                    .get("expected_project_version")
                    .and_then(|v| v.as_i64())
                    != Some(input.expected_project_version)
            {
                return Err(DbError::IdempotencyConflict);
            }
            let row = sqlx::query(&format!(
                "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset
                 WHERE id = ? AND project_id = ?"
            ))
            .bind(&entity_id)
            .bind(&input.project_id)
            .fetch_one(&mut *transaction)
            .await?;
            let asset = map_media_asset(&row)?;
            let pending = sqlx::query(
                "SELECT staging_storage_key, status
                 FROM project_media_pending_upload
                 WHERE project_id = ? AND idempotency_key = ?",
            )
            .bind(&input.project_id)
            .bind(&input.idempotency_key)
            .fetch_optional(&mut *transaction)
            .await?;
            let (staging_storage_key, status) = pending.map_or_else(
                || (None, "finalized".to_owned()),
                |row| {
                    (
                        row.try_get::<String, _>("staging_storage_key").ok(),
                        row.try_get::<String, _>("status")
                            .unwrap_or_else(|_| "metadata_committed".to_owned()),
                    )
                },
            );
            transaction.commit().await?;
            return Ok(ProjectMediaUpload {
                project_id: asset.project_id,
                idempotency_key: input.idempotency_key,
                mutation_fingerprint: input.mutation_fingerprint,
                expected_project_version: input.expected_project_version,
                asset_id: asset.id,
                final_storage_key: asset.storage_key,
                staging_storage_key,
                display_filename: asset.display_filename,
                content_type: asset.content_type,
                byte_size: asset.byte_size,
                checksum: asset.checksum.ok_or_else(|| {
                    DbError::Check("replayed media asset checksum is unavailable".to_owned())
                })?,
                status,
                created_at: asset.created_at,
            });
        }

        if let Some(row) = sqlx::query(
            "SELECT project_id, idempotency_key, mutation_fingerprint,
                    expected_project_version, asset_id, final_storage_key,
                    staging_storage_key, display_filename, content_type,
                    byte_size, checksum, status, created_at
             FROM project_media_pending_upload
             WHERE project_id = ? AND idempotency_key = ?",
        )
        .bind(&input.project_id)
        .bind(&input.idempotency_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let existing_fingerprint: String = row.try_get("mutation_fingerprint")?;
            let existing_version: i64 = row.try_get("expected_project_version")?;
            if existing_fingerprint != input.mutation_fingerprint
                || existing_version != input.expected_project_version
                || row.try_get::<String, _>("display_filename")? != input.display_filename
                || row.try_get::<String, _>("content_type")? != input.content_type
                || row.try_get::<i64, _>("byte_size")? != input.byte_size
                || row.try_get::<String, _>("checksum")? != input.checksum
            {
                return Err(DbError::IdempotencyConflict);
            }
            let pending = ProjectMediaUpload {
                project_id: row.try_get("project_id")?,
                idempotency_key: row.try_get("idempotency_key")?,
                mutation_fingerprint: existing_fingerprint,
                expected_project_version: existing_version,
                asset_id: row.try_get("asset_id")?,
                final_storage_key: row.try_get("final_storage_key")?,
                staging_storage_key: row.try_get("staging_storage_key")?,
                display_filename: row.try_get("display_filename")?,
                content_type: row.try_get("content_type")?,
                byte_size: row.try_get("byte_size")?,
                checksum: row.try_get("checksum")?,
                status: row.try_get("status")?,
                created_at: row.try_get("created_at")?,
            };
            transaction.commit().await?;
            return Ok(pending);
        }

        let project_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
            .bind(&input.project_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        if project_version != input.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_media_pending_upload (
                project_id, idempotency_key, mutation_fingerprint,
                expected_project_version, asset_id, final_storage_key,
                staging_storage_key, display_filename, content_type, byte_size,
                checksum, status, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
        )
        .bind(&input.project_id)
        .bind(&input.idempotency_key)
        .bind(&input.mutation_fingerprint)
        .bind(input.expected_project_version)
        .bind(&input.asset_id)
        .bind(&input.final_storage_key)
        .bind(&input.staging_storage_key)
        .bind(&input.display_filename)
        .bind(&input.content_type)
        .bind(input.byte_size)
        .bind(&input.checksum)
        .bind(&input.created_at)
        .bind(&input.created_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(ProjectMediaUpload {
            project_id: input.project_id,
            idempotency_key: input.idempotency_key,
            mutation_fingerprint: input.mutation_fingerprint,
            expected_project_version: input.expected_project_version,
            asset_id: input.asset_id,
            final_storage_key: input.final_storage_key,
            staging_storage_key: Some(input.staging_storage_key),
            display_filename: input.display_filename,
            content_type: input.content_type,
            byte_size: input.byte_size,
            checksum: input.checksum,
            status: "pending".to_owned(),
            created_at: input.created_at,
        })
    }

    async fn list_pending_project_media_uploads(
        &self,
        limit: i64,
    ) -> Result<Vec<ProjectMediaUpload>> {
        sqlx::query(
            "SELECT project_id, idempotency_key, mutation_fingerprint,
                    expected_project_version, asset_id, final_storage_key,
                    staging_storage_key, display_filename, content_type,
                    byte_size, checksum, status, created_at
             FROM project_media_pending_upload
             ORDER BY updated_at ASC, project_id ASC, idempotency_key ASC
             LIMIT ?",
        )
        .bind(limit.clamp(1, 500))
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(|row| {
            Ok(ProjectMediaUpload {
                project_id: row.try_get("project_id")?,
                idempotency_key: row.try_get("idempotency_key")?,
                mutation_fingerprint: row.try_get("mutation_fingerprint")?,
                expected_project_version: row.try_get("expected_project_version")?,
                asset_id: row.try_get("asset_id")?,
                final_storage_key: row.try_get("final_storage_key")?,
                staging_storage_key: Some(row.try_get("staging_storage_key")?),
                display_filename: row.try_get("display_filename")?,
                content_type: row.try_get("content_type")?,
                byte_size: row.try_get("byte_size")?,
                checksum: row.try_get("checksum")?,
                status: row.try_get("status")?,
                created_at: row.try_get("created_at")?,
            })
        })
        .collect()
    }

    async fn delete_pending_project_media_upload(
        &self,
        project_id: &str,
        idempotency_key: &str,
    ) -> Result<bool> {
        let result = sqlx::query(
            "DELETE FROM project_media_pending_upload
             WHERE project_id = ? AND idempotency_key = ? AND status = 'pending'",
        )
        .bind(project_id)
        .bind(idempotency_key)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn create_project_media_asset(
        &self,
        input: CreateProjectMediaAsset,
    ) -> Result<MediaAsset> {
        if input.byte_size < 0 || !is_sha256_digest(&input.checksum) {
            return Err(DbError::Check(
                "project media asset requires a valid byte size and SHA-256 checksum".to_owned(),
            ));
        }
        validate_project_media_metadata(
            &input.display_filename,
            &input.content_type,
            &input.storage_key,
            &format!("pending/{}", input.storage_key),
        )?;
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let dedupe_key = format!(
            "project-media-upload:{}:{}",
            input.project_id, input.idempotency_key
        );
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
                .map_err(|error| DbError::Check(format!("invalid media upload event: {error}")))?;
            if event_type != "project.media.uploaded"
                || payload.get("project_id").and_then(|v| v.as_str())
                    != Some(input.project_id.as_str())
                || payload.get("filename").and_then(|v| v.as_str())
                    != Some(input.display_filename.as_str())
                || payload.get("content_type").and_then(|v| v.as_str())
                    != Some(input.content_type.as_str())
                || payload.get("byte_size").and_then(|v| v.as_i64()) != Some(input.byte_size)
                || payload.get("checksum").and_then(|v| v.as_str()) != Some(input.checksum.as_str())
                || payload.get("mutation_fingerprint").and_then(|v| v.as_str())
                    != Some(input.mutation_fingerprint.as_str())
                || payload
                    .get("expected_project_version")
                    .and_then(|v| v.as_i64())
                    != Some(input.expected_project_version)
            {
                return Err(DbError::IdempotencyConflict);
            }
            let row = sqlx::query(&format!(
                "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ? AND project_id = ?"
            ))
            .bind(&entity_id)
            .bind(&input.project_id)
            .fetch_one(&mut *transaction)
            .await?;
            let asset = map_media_asset(&row)?;
            transaction.commit().await?;
            return Ok(asset);
        }

        let project_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
            .bind(&input.project_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        if project_version != input.expected_project_version {
            return Err(DbError::VersionConflict);
        }

        sqlx::query(
            "INSERT INTO media_asset (
                id, project_id, legacy_task_media_id, display_filename, content_type,
                byte_size, storage_key, checksum, availability, gc_state, version,
                created_at, updated_at
             ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, 'quarantined', 'referenced', 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.display_filename)
        .bind(&input.content_type)
        .bind(input.byte_size)
        .bind(&input.storage_key)
        .bind(&input.checksum)
        .bind(&input.created_at)
        .bind(&input.created_at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE project_media_pending_upload
             SET status = 'metadata_committed', updated_at = ?
             WHERE project_id = ? AND asset_id = ?",
        )
        .bind(&input.created_at)
        .bind(&input.project_id)
        .bind(&input.id)
        .execute(&mut *transaction)
        .await?;

        let payload_json = serde_json::json!({
            "project_id": input.project_id,
            "asset_id": input.id,
            "filename": input.display_filename,
            "content_type": input.content_type,
            "byte_size": input.byte_size,
            "checksum": input.checksum,
            "authorization_event_id": input.authorization_event_id,
            "mutation_fingerprint": input.mutation_fingerprint,
            "expected_project_version": input.expected_project_version,
        })
        .to_string();
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.media.uploaded".to_owned(),
            entity_type: "media_asset".to_owned(),
            entity_id: input.id.clone(),
            actor_type: input.actor_type,
            actor_id: input.actor_id,
            scope_type: "project".to_owned(),
            scope_id: input.project_id.clone(),
            correlation_id: dedupe_key.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(dedupe_key),
            payload_json,
            created_at: input.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;

        let row = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
        ))
        .bind(&input.id)
        .fetch_one(&mut *transaction)
        .await?;
        let asset = map_media_asset(&row)?;
        transaction.commit().await?;
        Ok(asset)
    }

    async fn finalize_project_media_upload(
        &self,
        project_id: &str,
        asset_id: &str,
        now: &str,
    ) -> Result<MediaAsset> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let row = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset
             WHERE id = ? AND project_id = ?"
        ))
        .bind(asset_id)
        .bind(project_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let asset = map_media_asset(&row)?;
        if asset.availability == "available" {
            sqlx::query(
                "DELETE FROM project_media_pending_upload
                 WHERE project_id = ? AND asset_id = ?",
            )
            .bind(project_id)
            .bind(asset_id)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(asset);
        }
        if asset.availability != "quarantined" {
            return Err(DbError::Check(
                "media upload cannot be finalized from its current availability".to_owned(),
            ));
        }
        let result = sqlx::query(
            "UPDATE media_asset SET availability = 'available', version = version + 1,
                                    updated_at = ?
             WHERE id = ? AND project_id = ? AND availability = 'quarantined'",
        )
        .bind(now)
        .bind(asset_id)
        .bind(project_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "DELETE FROM project_media_pending_upload
             WHERE project_id = ? AND asset_id = ?",
        )
        .bind(project_id)
        .bind(asset_id)
        .execute(&mut *transaction)
        .await?;
        let row = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
        ))
        .bind(asset_id)
        .fetch_one(&mut *transaction)
        .await?;
        let asset = map_media_asset(&row)?;
        transaction.commit().await?;
        Ok(asset)
    }

    async fn set_media_asset_checksum(
        &self,
        asset_id: &str,
        expected_byte_size: i64,
        checksum: &str,
        now: &str,
    ) -> Result<MediaAsset> {
        if checksum.len() != 64 || !checksum.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(DbError::Check(
                "media checksum is not a SHA-256 digest".to_owned(),
            ));
        }
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let row = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
        ))
        .bind(asset_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let asset = map_media_asset(&row)?;
        if asset.byte_size != expected_byte_size {
            return Err(DbError::Check(
                "media bytes do not match the recorded byte size".to_owned(),
            ));
        }
        if let Some(existing) = asset.checksum.as_deref() {
            if existing != checksum {
                return Err(DbError::Check(
                    "media checksum does not match the persisted digest".to_owned(),
                ));
            }
            transaction.commit().await?;
            return Ok(asset);
        }
        let updated = sqlx::query(
            "UPDATE media_asset
             SET checksum = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND byte_size = ? AND checksum IS NULL AND version = ?",
        )
        .bind(checksum)
        .bind(now)
        .bind(asset_id)
        .bind(expected_byte_size)
        .bind(asset.version)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let row = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
        ))
        .bind(asset_id)
        .fetch_one(&mut *transaction)
        .await?;
        let asset = map_media_asset(&row)?;
        transaction.commit().await?;
        Ok(asset)
    }

    async fn list_purged_media_assets(&self, limit: i64) -> Result<Vec<MediaAsset>> {
        sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset a
             WHERE a.availability = 'purged' AND a.gc_state = 'deleted'
               AND NOT EXISTS (
                   SELECT 1 FROM media_asset_purge_reconciliation r
                   WHERE r.asset_id = a.id
               )
             ORDER BY a.updated_at ASC, a.id ASC LIMIT ?"
        ))
        .bind(limit.clamp(1, 500))
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(|row| map_media_asset(&row))
        .collect()
    }

    async fn mark_purged_media_asset_reconciled(
        &self,
        asset_id: &str,
        reconciled_at: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO media_asset_purge_reconciliation (asset_id, reconciled_at)
             SELECT id, ? FROM media_asset
             WHERE id = ? AND availability = 'purged' AND gc_state = 'deleted'
             ON CONFLICT(asset_id) DO NOTHING",
        )
        .bind(reconciled_at)
        .bind(asset_id)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn replay_project_media_tombstone(
        &self,
        input: ProjectMediaTombstone,
    ) -> Result<Option<MediaAsset>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let replay = replay_project_media_tombstone_in_tx(&mut transaction, &input).await?;
        transaction.commit().await?;
        Ok(replay)
    }

    async fn tombstone_project_media_asset(
        &self,
        input: ProjectMediaTombstone,
    ) -> Result<MediaAsset> {
        if !matches!(input.target_availability.as_str(), "redacted" | "purged") {
            return Err(DbError::Check(
                "media tombstone availability must be redacted or purged".to_owned(),
            ));
        }
        if input.reason.trim().is_empty() || input.reason.len() > 4096 {
            return Err(DbError::Check(
                "media tombstone reason is invalid".to_owned(),
            ));
        }
        if input.principal_type.trim().is_empty()
            || input.principal_id.trim().is_empty()
            || input.authorization_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.authorization_occurred_at.trim().is_empty()
            || input.authorization_event_id.trim().is_empty()
            || chrono::DateTime::parse_from_rfc3339(&input.authorization_occurred_at).is_err()
        {
            return Err(DbError::Check(
                "media tombstone requires a complete authorization envelope".to_owned(),
            ));
        }
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        if let Some(asset) = replay_project_media_tombstone_in_tx(&mut transaction, &input).await? {
            transaction.commit().await?;
            return Ok(asset);
        }
        let dedupe_key = format!(
            "project-media-tombstone:{}:{}",
            input.project_id, input.idempotency_key
        );
        let event_type = format!("project.media.{}", input.target_availability);

        let row = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset
             WHERE id = ? AND project_id = ?"
        ))
        .bind(&input.asset_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let current = map_media_asset(&row)?;
        if current.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        if current.availability == input.target_availability {
            return Err(DbError::Check(
                "media asset already has the requested disposition".to_owned(),
            ));
        }
        if current.availability == "purged" {
            return Err(DbError::Check("purged media cannot be restored".to_owned()));
        }
        if input.target_availability == "redacted" && current.gc_state == "deleted" {
            return Err(DbError::Check(
                "deleted media cannot be redacted".to_owned(),
            ));
        }

        let tombstone_id = new_uuid_v4();
        sqlx::query(
            "INSERT INTO media_asset_tombstone (
                id, asset_id, release_id, release_pin_id, previous_checksum,
                previous_availability, availability, principal_type, principal_id,
                reason, authorization_basis, authorization_action, explicit_event,
                authorization_occurred_at, idempotency_key, mutation_fingerprint,
                created_at
             ) VALUES (?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&tombstone_id)
        .bind(&input.asset_id)
        .bind(current.checksum.as_deref())
        .bind(&current.availability)
        .bind(&input.target_availability)
        .bind(&input.principal_type)
        .bind(&input.principal_id)
        .bind(&input.reason)
        .bind(&input.authorization_basis)
        .bind(&input.authorization_action)
        .bind(&input.authorization_event_id)
        .bind(&input.authorization_occurred_at)
        .bind(&input.idempotency_key)
        .bind(&input.mutation_fingerprint)
        .bind(&input.created_at)
        .execute(&mut *transaction)
        .await?;

        let pin_rows = sqlx::query(
            "SELECT id, release_id, availability
             FROM project_release_media_pin
             WHERE project_id = ? AND asset_id = ? AND availability != 'purged'",
        )
        .bind(&input.project_id)
        .bind(&input.asset_id)
        .fetch_all(&mut *transaction)
        .await?;
        for pin in pin_rows {
            let pin_id: String = pin.try_get("id")?;
            let release_id: String = pin.try_get("release_id")?;
            let previous_availability: String = pin.try_get("availability")?;
            sqlx::query(
                "INSERT INTO media_asset_tombstone (
                    id, asset_id, release_id, release_pin_id, previous_checksum,
                    previous_availability, availability, principal_type, principal_id,
                    reason, authorization_basis, authorization_action, explicit_event,
                    authorization_occurred_at, idempotency_key, mutation_fingerprint,
                    created_at
                 ) VALUES (?, ?, ?, ?, ?, ?, 'evidence_unavailable', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(new_uuid_v4())
            .bind(&input.asset_id)
            .bind(&release_id)
            .bind(&pin_id)
            .bind(current.checksum.as_deref())
            .bind(previous_availability)
            .bind(&input.principal_type)
            .bind(&input.principal_id)
            .bind(&input.reason)
            .bind(&input.authorization_basis)
            .bind(&input.authorization_action)
            .bind(&input.authorization_event_id)
            .bind(&input.authorization_occurred_at)
            .bind(format!("{}:pin:{}", input.idempotency_key, pin_id))
            .bind(&input.mutation_fingerprint)
            .bind(&input.created_at)
            .execute(&mut *transaction)
            .await?;
        }

        sqlx::query(
            "UPDATE project_media_attachment
             SET availability = ?, version = version + 1, updated_at = ?
             WHERE project_id = ? AND asset_id = ? AND availability != ?",
        )
        .bind(&input.target_availability)
        .bind(&input.created_at)
        .bind(&input.project_id)
        .bind(&input.asset_id)
        .bind(&input.target_availability)
        .execute(&mut *transaction)
        .await?;

        // A tombstone wins over an upload that was metadata-committed but
        // had not yet reached finalize.  Remove the durable pending marker so
        // startup reconciliation cannot resurrect a purged/redacted asset.
        sqlx::query(
            "DELETE FROM project_media_pending_upload
             WHERE project_id = ? AND asset_id = ?",
        )
        .bind(&input.project_id)
        .bind(&input.asset_id)
        .execute(&mut *transaction)
        .await?;

        let (gc_state, deleted_at) = if input.target_availability == "purged" {
            ("deleted", Some(input.created_at.as_str()))
        } else {
            ("referenced", None)
        };
        let updated = sqlx::query(
            "UPDATE media_asset
             SET availability = ?, gc_state = ?, gc_candidate_at = NULL,
                 gc_lease_owner = NULL, gc_lease_expires_at = NULL,
                 deleted_at = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(&input.target_availability)
        .bind(gc_state)
        .bind(deleted_at)
        .bind(&input.created_at)
        .bind(&input.asset_id)
        .bind(&input.project_id)
        .bind(input.expected_version)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

        let payload_json = serde_json::json!({
            "project_id": input.project_id,
            "asset_id": input.asset_id,
            "target_availability": input.target_availability,
            "expected_version": input.expected_version,
            "mutation_fingerprint": input.mutation_fingerprint,
            "principal_type": input.principal_type,
            "principal_id": input.principal_id,
            "reason": input.reason,
            "authorization_basis": input.authorization_basis,
            "authorization_action": input.authorization_action,
            "authorization_occurred_at": input.authorization_occurred_at,
            "authorization_event_id": input.authorization_event_id,
            "authorization_json": input.authorization_json,
        })
        .to_string();
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type,
            entity_type: "media_asset".to_owned(),
            entity_id: input.asset_id.clone(),
            actor_type: input.principal_type.clone(),
            actor_id: Some(input.principal_id.clone()),
            scope_type: "project".to_owned(),
            scope_id: input.project_id.clone(),
            correlation_id: dedupe_key.clone(),
            causation_id: Some(input.authorization_event_id.clone()),
            causation_depth: 0,
            dedupe_key: Some(dedupe_key),
            payload_json,
            created_at: input.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;

        let row = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
        ))
        .bind(&input.asset_id)
        .fetch_one(&mut *transaction)
        .await?;
        let asset = map_media_asset(&row)?;
        transaction.commit().await?;
        Ok(asset)
    }

    async fn create_project_media_attachment_mutation(
        &self,
        input: CreateProjectMediaAttachmentMutation,
    ) -> Result<ProjectMediaAttachment> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let attachment = &input.attachment;
        let dedupe_key = format!(
            "project-evidence-attach:{}:{}",
            attachment.project_id, input.idempotency_key
        );
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

        let milestone_id = attachment
            .milestone_id
            .as_deref()
            .ok_or_else(|| DbError::Check("evidence milestone is required".to_owned()))?;
        let milestone = sqlx::query(
            "SELECT project_id, version, current_definition_revision_id
             FROM project_milestone WHERE id = ?",
        )
        .bind(milestone_id)
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
            .ok_or(DbError::NotFound)?;
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
                source_validation_id, acceptance_check_ids_json, caption, evidence_kind,
                checksum, availability, project_url, author_type, author_id,
                authorization_json, version, created_at, deleted_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, NULL, ?)",
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

        let payload_json = serde_json::json!({
            "project_id": attachment.project_id,
            "milestone_id": milestone_id,
            "asset_id": attachment.asset_id,
            "evidence_id": attachment.id,
            "checksum": attachment.checksum,
            "expected_milestone_version": input.expected_milestone_version,
            "authorization_event_id": input.authorization_event_id,
            "mutation_fingerprint": input.mutation_fingerprint,
        })
        .to_string();
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.evidence.attached".to_owned(),
            entity_type: "project_media_attachment".to_owned(),
            entity_id: attachment.id.clone(),
            actor_type: attachment.author_type.clone(),
            actor_id: attachment.author_id.clone(),
            scope_type: "project".to_owned(),
            scope_id: attachment.project_id.clone(),
            correlation_id: dedupe_key.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(dedupe_key),
            payload_json,
            created_at: attachment.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
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

    async fn soft_delete_project_media_attachment_mutation(
        &self,
        input: SoftDeleteProjectMediaAttachmentMutation,
    ) -> Result<ProjectMediaAttachment> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let dedupe_key = format!(
            "project-evidence-remove:{}:{}",
            input.project_id, input.idempotency_key
        );
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
            if event_type != "project.evidence.removed"
                || entity_id != input.id
                || payload.get("project_id").and_then(|v| v.as_str())
                    != Some(input.project_id.as_str())
                || payload.get("milestone_id").and_then(|v| v.as_str())
                    != Some(input.milestone_id.as_str())
                || payload.get("expected_version").and_then(|v| v.as_i64())
                    != Some(input.expected_version)
                || payload.get("mutation_fingerprint").and_then(|v| v.as_str())
                    != Some(input.mutation_fingerprint.as_str())
            {
                return Err(DbError::IdempotencyConflict);
            }
            let row = sqlx::query(&format!(
                "SELECT {PROJECT_MEDIA_ATTACHMENT_COLUMNS}
                 FROM project_media_attachment WHERE id = ?"
            ))
            .bind(&input.id)
            .fetch_one(&mut *transaction)
            .await?;
            let attachment = map_project_media_attachment(&row)?;
            transaction.commit().await?;
            return Ok(attachment);
        }

        let row = sqlx::query(&format!(
            "SELECT {PROJECT_MEDIA_ATTACHMENT_COLUMNS}
             FROM project_media_attachment
             WHERE id = ? AND project_id = ? AND milestone_id = ?
               AND attachment_kind = 'evidence'"
        ))
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.milestone_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let current = map_project_media_attachment(&row)?;
        if current.deleted_at.is_some() {
            return Err(DbError::NotFound);
        }
        if current.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        let result = sqlx::query(
            "UPDATE project_media_attachment
             SET deleted_at = ?, availability = 'purged', version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND milestone_id = ?
               AND attachment_kind = 'evidence' AND deleted_at IS NULL AND version = ?",
        )
        .bind(&input.deleted_at)
        .bind(&input.deleted_at)
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.milestone_id)
        .bind(input.expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        reconcile_media_asset_in_tx(&mut transaction, &current.asset_id, &input.deleted_at).await?;
        let payload_json = serde_json::json!({
            "project_id": input.project_id,
            "milestone_id": input.milestone_id,
            "evidence_id": input.id,
            "expected_version": input.expected_version,
            "authorization_event_id": input.authorization_event_id,
            "mutation_fingerprint": input.mutation_fingerprint,
        })
        .to_string();
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.evidence.removed".to_owned(),
            entity_type: "project_media_attachment".to_owned(),
            entity_id: input.id.clone(),
            actor_type: input.actor_type,
            actor_id: input.actor_id,
            scope_type: "project".to_owned(),
            scope_id: input.project_id.clone(),
            correlation_id: dedupe_key.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(dedupe_key),
            payload_json,
            created_at: input.deleted_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        let row = sqlx::query(&format!(
            "SELECT {PROJECT_MEDIA_ATTACHMENT_COLUMNS}
             FROM project_media_attachment WHERE id = ?"
        ))
        .bind(&input.id)
        .fetch_one(&mut *transaction)
        .await?;
        let attachment = map_project_media_attachment(&row)?;
        transaction.commit().await?;
        Ok(attachment)
    }

    async fn create_project_media_attachment(
        &self,
        input: CreateProjectMediaAttachment,
    ) -> Result<ProjectMediaAttachment> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        ensure_asset_attachable(&mut transaction, &input.asset_id, &input.project_id).await?;
        if input.availability != "available" {
            return Err(DbError::Check(
                "new media references must be available".to_owned(),
            ));
        }
        let input_checksum = input
            .checksum
            .as_deref()
            .filter(|checksum| is_sha256_digest(checksum))
            .ok_or_else(|| DbError::Check("evidence checksum is required".to_owned()))?;
        let asset_checksum = sqlx::query_scalar::<_, Option<String>>(
            "SELECT checksum FROM media_asset WHERE id = ? AND project_id = ?",
        )
        .bind(&input.asset_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *transaction)
        .await?
        .flatten();
        if asset_checksum.as_deref() != Some(input_checksum) {
            return Err(DbError::Check(
                "evidence checksum does not match the media asset".to_owned(),
            ));
        }

        sqlx::query(
            "INSERT INTO project_media_attachment (
                id, project_id, asset_id, attachment_kind, task_media_id, task_id,
                milestone_id, milestone_check_id, source_task_id, source_execution_id,
                source_validation_id, acceptance_check_ids_json, caption, evidence_kind,
                checksum, availability, project_url, author_type, author_id,
                authorization_json, version, created_at, deleted_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, NULL, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.asset_id)
        .bind(&input.attachment_kind)
        .bind(input.task_media_id.as_deref())
        .bind(input.task_id.as_deref())
        .bind(input.milestone_id.as_deref())
        .bind(input.milestone_check_id.as_deref())
        .bind(input.source_task_id.as_deref())
        .bind(input.source_execution_id.as_deref())
        .bind(input.source_validation_id.as_deref())
        .bind(&input.acceptance_check_ids_json)
        .bind(input.caption.as_deref())
        .bind(input.evidence_kind.as_deref())
        .bind(input.checksum.as_deref())
        .bind(&input.availability)
        .bind(input.project_url.as_deref())
        .bind(&input.author_type)
        .bind(input.author_id.as_deref())
        .bind(&input.authorization_json)
        .bind(&input.created_at)
        .bind(&input.created_at)
        .execute(&mut *transaction)
        .await?;

        let asset =
            reconcile_media_asset_in_tx(&mut transaction, &input.asset_id, &input.created_at)
                .await?;
        if asset.gc_state == "deleted" {
            return Err(DbError::Check(
                "media asset was deleted while attaching evidence".to_owned(),
            ));
        }

        let attachment = sqlx::query(&format!(
            "SELECT {PROJECT_MEDIA_ATTACHMENT_COLUMNS} FROM project_media_attachment WHERE id = ?"
        ))
        .bind(&input.id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(DbError::from)
        .and_then(|row| map_project_media_attachment(&row))?;

        transaction.commit().await?;
        Ok(attachment)
    }

    async fn soft_delete_project_media_attachment(
        &self,
        id: &str,
        deleted_at: &str,
    ) -> Result<ProjectMediaAttachment> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let asset_id = sqlx::query_scalar::<_, String>(
            "SELECT asset_id FROM project_media_attachment WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;

        let result = sqlx::query(
            "UPDATE project_media_attachment
             SET deleted_at = ?, availability = 'purged', version = version + 1, updated_at = ?
             WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(deleted_at)
        .bind(deleted_at)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(DbError::NotFound);
        }

        reconcile_media_asset_in_tx(&mut transaction, &asset_id, deleted_at).await?;
        let attachment = sqlx::query(&format!(
            "SELECT {PROJECT_MEDIA_ATTACHMENT_COLUMNS} FROM project_media_attachment WHERE id = ?"
        ))
        .bind(id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(DbError::from)
        .and_then(|row| map_project_media_attachment(&row))?;
        transaction.commit().await?;
        Ok(attachment)
    }

    async fn create_project_release_media_pin(
        &self,
        input: CreateProjectReleaseMediaPin,
    ) -> Result<ProjectReleaseMediaPin> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;

        if !is_sha256_digest(&input.asset_checksum) {
            return Err(DbError::Check(
                "release media pin requires a SHA-256 asset checksum".to_owned(),
            ));
        }
        if input.availability != "available" || input.attachment_digest.trim().is_empty() {
            return Err(DbError::Check(
                "release media pin must reference available evidence with a digest".to_owned(),
            ));
        }
        let persisted_checksum = sqlx::query_scalar::<_, Option<String>>(
            "SELECT checksum FROM media_asset WHERE id = ? AND project_id = ?",
        )
        .bind(&input.asset_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *transaction)
        .await?
        .flatten()
        .ok_or(DbError::Check(
            "release media asset checksum is unavailable".to_owned(),
        ))?;
        if persisted_checksum != input.asset_checksum {
            return Err(DbError::Check(
                "release media pin asset checksum does not match asset".to_owned(),
            ));
        }

        if let Some(existing) = fetch_project_release_media_pin(
            &mut transaction,
            &input.release_id,
            &input.asset_id,
            input.attachment_id.as_deref(),
        )
        .await?
        {
            validate_pin_replay(&existing, &input)?;
            transaction.commit().await?;
            return Ok(existing);
        }

        ensure_asset_attachable(&mut transaction, &input.asset_id, &input.project_id).await?;

        // Release retries are intentionally idempotent by the immutable
        // release/asset/attachment identity.  A replay returns the original
        // pin instead of attempting a second insert.
        sqlx::query(
            "INSERT OR IGNORE INTO project_release_media_pin (
                id, project_id, release_id, asset_id, attachment_id,
                legacy_task_media_id, asset_checksum, attachment_digest,
                availability, pin_digest, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.release_id)
        .bind(&input.asset_id)
        .bind(input.attachment_id.as_deref())
        .bind(input.legacy_task_media_id.as_deref())
        .bind(&input.asset_checksum)
        .bind(&input.attachment_digest)
        .bind(&input.availability)
        .bind(&input.pin_digest)
        .bind(&input.created_at)
        .execute(&mut *transaction)
        .await?;

        reconcile_media_asset_in_tx(&mut transaction, &input.asset_id, &input.created_at).await?;
        let pin = fetch_project_release_media_pin(
            &mut transaction,
            &input.release_id,
            &input.asset_id,
            input.attachment_id.as_deref(),
        )
        .await?
        .ok_or_else(|| DbError::Check("release media pin insert was not persisted".to_owned()))?;
        validate_pin_replay(&pin, &input)?;
        transaction.commit().await?;
        Ok(pin)
    }

    async fn list_project_release_media_pins(
        &self,
        release_id: &str,
    ) -> Result<Vec<ProjectReleaseMediaPin>> {
        sqlx::query(&format!(
            "SELECT {PROJECT_RELEASE_MEDIA_PIN_COLUMNS}
             FROM project_release_media_pin WHERE release_id = ? ORDER BY id ASC"
        ))
        .bind(release_id)
        .fetch_all(self.pool())
        .await?
        .iter()
        .map(map_project_release_media_pin)
        .collect()
    }

    async fn reconcile_media_asset(&self, asset_id: &str, now: &str) -> Result<Option<MediaAsset>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let asset = reconcile_media_asset_in_tx(&mut transaction, asset_id, now).await?;
        transaction.commit().await?;
        Ok(Some(asset))
    }

    async fn claim_media_gc_candidates(
        &self,
        now: &str,
        lease_owner: &str,
        lease_expires_at: &str,
        limit: i64,
    ) -> Result<Vec<MediaAsset>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let rows = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS}
             FROM media_asset
             WHERE gc_state IN ('gc_candidate', 'gc_queued')
               AND availability != 'purged'
               AND gc_candidate_at IS NOT NULL
               AND gc_candidate_at <= ?
               AND (gc_lease_owner IS NULL OR gc_lease_expires_at IS NULL OR gc_lease_expires_at <= ?)
             ORDER BY gc_candidate_at ASC, id ASC LIMIT ?"
        ))
        .bind(now)
        .bind(now)
        .bind(limit.clamp(1, 500))
        .fetch_all(&mut *transaction)
        .await?;

        let mut candidates = Vec::new();
        for row in rows {
            let asset = map_media_asset(&row)?;
            let result = sqlx::query(
                "UPDATE media_asset
                 SET gc_state = 'gc_queued', gc_lease_owner = ?, gc_lease_expires_at = ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ?
                   AND gc_state IN ('gc_candidate', 'gc_queued')
                   AND availability != 'purged'
                   AND version = ?
                   AND (gc_lease_owner IS NULL OR gc_lease_expires_at IS NULL OR gc_lease_expires_at <= ?)
                   AND NOT EXISTS (
                       SELECT 1 FROM project_media_attachment
                       WHERE asset_id = media_asset.id
                         AND deleted_at IS NULL
                         AND availability != 'purged'
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM task_media
                       WHERE asset_id = media_asset.id AND deleted_at IS NULL
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM project_release_media_pin
                       WHERE asset_id = media_asset.id AND availability != 'purged'
                   )",
            )
            .bind(lease_owner)
            .bind(lease_expires_at)
            .bind(now)
            .bind(&asset.id)
            .bind(asset.version)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() == 1 {
                let queued = sqlx::query(&format!(
                    "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
                ))
                .bind(&asset.id)
                .fetch_one(&mut *transaction)
                .await?;
                candidates.push(map_media_asset(&queued)?);
            }
        }
        transaction.commit().await?;
        Ok(candidates)
    }

    async fn claim_media_gc_candidate(
        &self,
        asset_id: &str,
        now: &str,
        lease_owner: &str,
        lease_expires_at: &str,
    ) -> Result<Option<MediaAsset>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let row = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
        ))
        .bind(asset_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };
        let asset = map_media_asset(&row)?;
        let candidate_not_due = match asset.gc_candidate_at.as_deref() {
            None => true,
            Some(candidate_at) => candidate_at > now,
        };
        let lease_is_live = asset.gc_lease_owner.is_some()
            && asset
                .gc_lease_expires_at
                .as_deref()
                .is_some_and(|expires_at| expires_at > now);
        if candidate_not_due || lease_is_live {
            transaction.commit().await?;
            return Ok(None);
        }

        let result = sqlx::query(
            "UPDATE media_asset
             SET gc_state = 'gc_queued', gc_lease_owner = ?, gc_lease_expires_at = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ?
               AND gc_state IN ('gc_candidate', 'gc_queued')
               AND availability != 'purged'
               AND gc_candidate_at IS NOT NULL
               AND gc_candidate_at <= ?
               AND version = ?
               AND (gc_lease_owner IS NULL OR gc_lease_expires_at IS NULL OR gc_lease_expires_at <= ?)
               AND NOT EXISTS (
                   SELECT 1 FROM project_media_attachment
                   WHERE asset_id = media_asset.id
                     AND deleted_at IS NULL
                     AND availability != 'purged'
               )
               AND NOT EXISTS (
                   SELECT 1 FROM task_media
                   WHERE asset_id = media_asset.id AND deleted_at IS NULL
               )
               AND NOT EXISTS (
                   SELECT 1 FROM project_release_media_pin
                   WHERE asset_id = media_asset.id AND availability != 'purged'
               )",
        )
        .bind(lease_owner)
        .bind(lease_expires_at)
        .bind(now)
        .bind(asset_id)
        .bind(now)
        .bind(asset.version)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            transaction.commit().await?;
            return Ok(None);
        }

        let row = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
        ))
        .bind(asset_id)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(map_media_asset(&row)?))
    }

    async fn reset_media_gc_candidate(
        &self,
        asset_id: &str,
        lease_owner: &str,
        expected_version: i64,
        now: &str,
    ) -> Result<Option<MediaAsset>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let asset = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
        ))
        .bind(asset_id)
        .fetch_optional(&mut *transaction)
        .await?
        .map(|row| map_media_asset(&row))
        .transpose()?
        .ok_or(DbError::NotFound)?;
        if asset.gc_state != "gc_queued"
            || asset.availability == "purged"
            || asset.gc_lease_owner.as_deref() != Some(lease_owner)
            || asset.gc_lease_expires_at.as_deref() <= Some(now)
            || asset.version != expected_version
        {
            return Err(DbError::VersionConflict);
        }

        let referenced = media_asset_is_referenced(&mut transaction, asset_id).await?;
        let state = if referenced {
            "referenced"
        } else {
            "gc_candidate"
        };
        let candidate_at = if referenced {
            None
        } else {
            asset.gc_candidate_at.or_else(|| Some(now.to_owned()))
        };
        let result = sqlx::query(
            "UPDATE media_asset
             SET gc_state = ?, gc_candidate_at = ?, gc_lease_owner = NULL,
                 gc_lease_expires_at = NULL, version = version + 1, updated_at = ?
             WHERE id = ? AND gc_state = 'gc_queued' AND availability != 'purged'
               AND gc_lease_owner = ?
               AND gc_lease_expires_at > ? AND version = ?",
        )
        .bind(state)
        .bind(candidate_at.as_deref())
        .bind(now)
        .bind(asset_id)
        .bind(lease_owner)
        .bind(now)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let row = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
        ))
        .bind(asset_id)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(map_media_asset(&row)?))
    }

    async fn complete_media_gc(
        &self,
        asset_id: &str,
        lease_owner: &str,
        expected_version: i64,
        deleted_at: &str,
    ) -> Result<Option<MediaAsset>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let result = sqlx::query(
            "UPDATE media_asset
             SET gc_state = 'deleted', availability = 'purged',
                 deleted_at = COALESCE(deleted_at, ?), gc_lease_owner = NULL,
                 gc_lease_expires_at = NULL, version = version + 1, updated_at = ?
             WHERE id = ? AND gc_state = 'gc_queued'
               AND availability != 'purged'
               AND gc_lease_owner = ?
               AND gc_lease_expires_at > ?
               AND version = ?
               AND NOT EXISTS (
                   SELECT 1 FROM project_media_attachment
                   WHERE asset_id = media_asset.id
                     AND deleted_at IS NULL
                     AND availability != 'purged'
               )
               AND NOT EXISTS (
                   SELECT 1 FROM task_media
                   WHERE asset_id = media_asset.id AND deleted_at IS NULL
               )
               AND NOT EXISTS (
                   SELECT 1 FROM project_release_media_pin
                   WHERE asset_id = media_asset.id AND availability != 'purged'
               )",
        )
        .bind(deleted_at)
        .bind(deleted_at)
        .bind(asset_id)
        .bind(lease_owner)
        .bind(deleted_at)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await?;

        if result.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

        let row = sqlx::query(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
        ))
        .bind(asset_id)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(map_media_asset(&row)?))
    }
}

async fn ensure_asset_attachable(
    transaction: &mut Transaction<'_, Sqlite>,
    asset_id: &str,
    project_id: &str,
) -> Result<()> {
    let row = sqlx::query(
        "SELECT availability, gc_state FROM media_asset WHERE id = ? AND project_id = ?",
    )
    .bind(asset_id)
    .bind(project_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    let availability: String = row.try_get("availability")?;
    let gc_state: String = row.try_get("gc_state")?;
    if availability != "available" || gc_state == "deleted" || gc_state == "gc_queued" {
        return Err(DbError::Check(
            "media asset is unavailable for a new reference".to_owned(),
        ));
    }
    Ok(())
}

fn is_sha256_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_project_media_metadata(
    filename: &str,
    content_type: &str,
    final_storage_key: &str,
    staging_storage_key: &str,
) -> Result<()> {
    if filename.is_empty()
        || filename.len() > 255
        || filename.trim() != filename
        || filename == "."
        || filename == ".."
        || filename
            .chars()
            .any(|ch| ch.is_control() || ch == '/' || ch == '\\')
    {
        return Err(DbError::Check(
            "project media filename is invalid".to_owned(),
        ));
    }
    if !matches!(
        content_type,
        "image/png"
            | "image/jpeg"
            | "image/gif"
            | "image/webp"
            | "video/mp4"
            | "video/webm"
            | "video/quicktime"
            | "application/pdf"
            | "text/plain"
            | "application/zip"
    ) {
        return Err(DbError::Check(
            "project media content type is unsupported".to_owned(),
        ));
    }
    for storage_key in [final_storage_key, staging_storage_key] {
        let path = std::path::Path::new(storage_key);
        let has_normal_component = path
            .components()
            .any(|component| matches!(component, std::path::Component::Normal(_)));
        if storage_key.is_empty()
            || !has_normal_component
            || path.components().any(|component| {
                !matches!(
                    component,
                    std::path::Component::Normal(_) | std::path::Component::CurDir
                )
            })
        {
            return Err(DbError::Check(
                "project media storage key is invalid".to_owned(),
            ));
        }
    }
    Ok(())
}

async fn fetch_project_release_media_pin(
    transaction: &mut Transaction<'_, Sqlite>,
    release_id: &str,
    asset_id: &str,
    attachment_id: Option<&str>,
) -> Result<Option<ProjectReleaseMediaPin>> {
    let row = sqlx::query(&format!(
        "SELECT {PROJECT_RELEASE_MEDIA_PIN_COLUMNS}
         FROM project_release_media_pin
         WHERE release_id = ? AND asset_id = ? AND attachment_id IS ?"
    ))
    .bind(release_id)
    .bind(asset_id)
    .bind(attachment_id)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(|row| map_project_release_media_pin(&row))
        .transpose()
}

fn validate_pin_replay(
    existing: &ProjectReleaseMediaPin,
    input: &CreateProjectReleaseMediaPin,
) -> Result<()> {
    let same_payload = existing.project_id == input.project_id
        && existing.release_id == input.release_id
        && existing.asset_id == input.asset_id
        && existing.attachment_id == input.attachment_id
        && existing.legacy_task_media_id == input.legacy_task_media_id
        && existing.asset_checksum == input.asset_checksum
        && existing.attachment_digest == input.attachment_digest
        && existing.availability == input.availability
        && existing.pin_digest == input.pin_digest;
    if !same_payload {
        return Err(DbError::Check(
            "conflicting replay payload for release media pin".to_owned(),
        ));
    }
    Ok(())
}

async fn media_asset_is_referenced(
    transaction: &mut Transaction<'_, Sqlite>,
    asset_id: &str,
) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT CASE WHEN EXISTS (
             SELECT 1 FROM project_media_attachment
             WHERE asset_id = ? AND deleted_at IS NULL AND availability != 'purged'
         ) OR EXISTS (
             SELECT 1 FROM task_media
             WHERE asset_id = ? AND deleted_at IS NULL
         ) OR EXISTS (
             SELECT 1 FROM project_release_media_pin
             WHERE asset_id = ? AND availability != 'purged'
         ) THEN 1 ELSE 0 END",
    )
    .bind(asset_id)
    .bind(asset_id)
    .bind(asset_id)
    .fetch_one(&mut **transaction)
    .await?
        != 0)
}

async fn reconcile_media_asset_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    asset_id: &str,
    now: &str,
) -> Result<MediaAsset> {
    let row = sqlx::query(&format!(
        "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
    ))
    .bind(asset_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    let asset = map_media_asset(&row)?;

    let referenced = media_asset_is_referenced(transaction, asset_id).await?;

    // A live scheduler lease is authoritative while the worker is between
    // the guarded claim and its physical delete.  Reconciliation must not
    // turn that row back into an ordinary candidate merely because the
    // worker has not finalized it yet.  Expired leases become candidates and
    // are reclaimable by the next worker.
    let lease_is_live = asset.gc_state == "gc_queued"
        && asset.gc_lease_owner.is_some()
        && asset
            .gc_lease_expires_at
            .as_deref()
            .is_some_and(|expires_at| expires_at > now);

    let state = if asset.availability == "purged" || asset.gc_state == "deleted" {
        // A purged/deleted asset is a tombstone.  Never resurrect it merely
        // because a legacy trigger or an untrusted caller inserted metadata.
        "deleted"
    } else if referenced {
        "referenced"
    } else if lease_is_live {
        "gc_queued"
    } else {
        "gc_candidate"
    };
    let candidate_at = if state == "gc_candidate" || state == "gc_queued" {
        asset.gc_candidate_at.or_else(|| Some(now.to_owned()))
    } else {
        None
    };
    let deleted_at = if state == "deleted" {
        asset.deleted_at.or_else(|| Some(now.to_owned()))
    } else if state == "referenced" {
        None
    } else {
        asset.deleted_at.clone()
    };
    let lease_owner = if state == "gc_queued" {
        asset.gc_lease_owner.as_deref()
    } else {
        None
    };
    let lease_expires_at = if state == "gc_queued" {
        asset.gc_lease_expires_at.as_deref()
    } else {
        None
    };

    let result = sqlx::query(
        "UPDATE media_asset
         SET gc_state = ?, gc_candidate_at = ?, deleted_at = ?,
             gc_lease_owner = ?, gc_lease_expires_at = ?,
             version = version + 1, updated_at = ?
         WHERE id = ? AND version = ?",
    )
    .bind(state)
    .bind(candidate_at.as_deref())
    .bind(deleted_at.as_deref())
    .bind(lease_owner)
    .bind(lease_expires_at)
    .bind(now)
    .bind(asset_id)
    .bind(asset.version)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }

    let row = sqlx::query(&format!(
        "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
    ))
    .bind(asset_id)
    .fetch_one(&mut **transaction)
    .await?;
    map_media_asset(&row)
}
