use super::command_finalization::finalize_command_in_tx;
use super::*;
use crate::{BeginProjectMediaUpload, CommandReceipt, CommandReceiptRepo, ProjectMediaUpload};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

mod attachment_mutation;
mod reads;
pub use reads::{ProjectEvidenceAttachmentRead, ProjectMediaAssetIdRead};
mod repository;

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
        source_task_version: row.try_get("source_task_version")?,
        source_context_digest: row.try_get("source_context_digest")?,
        source_definition_revision_id: row.try_get("source_definition_revision_id")?,
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
const PROJECT_MEDIA_ATTACHMENT_COLUMNS: &str = "id, project_id, asset_id, attachment_kind, task_media_id, task_id, milestone_id, milestone_check_id, source_task_id, source_execution_id, source_validation_id, source_task_version, source_context_digest, source_definition_revision_id, acceptance_check_ids_json, caption, evidence_kind, checksum, availability, project_url, author_type, author_id, authorization_json, version, created_at, deleted_at, updated_at";
const PROJECT_RELEASE_MEDIA_PIN_COLUMNS: &str = "id, project_id, release_id, asset_id, attachment_id, legacy_task_media_id, asset_checksum, attachment_digest, availability, pin_digest, created_at";
const EVIDENCE_CONTEXT_DIGEST_SCHEMA: &str = "forge.milestone-readiness/v1";

fn canonicalize_evidence_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let sorted: BTreeMap<&str, Value> = object
                .iter()
                .map(|(key, value)| (key.as_str(), canonicalize_evidence_json(value)))
                .collect();
            let mut canonical = Map::new();
            for (key, value) in sorted {
                canonical.insert(key.to_owned(), value);
            }
            Value::Object(canonical)
        }
        Value::Array(values) => {
            Value::Array(values.iter().map(canonicalize_evidence_json).collect())
        }
        scalar => scalar.clone(),
    }
}

fn evidence_context_digest(value: &Value) -> Result<String> {
    let mut envelope = Map::new();
    envelope.insert(
        "schema_version".to_owned(),
        Value::String(EVIDENCE_CONTEXT_DIGEST_SCHEMA.to_owned()),
    );
    envelope.insert("value".to_owned(), canonicalize_evidence_json(value));
    let canonical = serde_json::to_vec(&canonicalize_evidence_json(&Value::Object(envelope)))
        .map_err(|error| DbError::Check(format!("serialize evidence context: {error}")))?;
    Ok(hex::encode(Sha256::digest(canonical)))
}

/// Populate the immutable context pins for a newly attached evidence row.
/// The values are derived inside the same transaction as the attachment so a
/// caller cannot claim a task version, execution, review, or definition it did
/// not actually observe.  Existing rows remain NULL and are treated as stale
/// by the readiness evaluator.
async fn populate_evidence_context_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    attachment: &mut CreateProjectMediaAttachment,
    current_definition_revision_id: Option<&str>,
) -> Result<()> {
    attachment.source_definition_revision_id = current_definition_revision_id.map(str::to_owned);

    let source_task_id = attachment
        .source_task_id
        .as_deref()
        .or(attachment.task_id.as_deref())
        .map(str::to_owned);
    if attachment.source_execution_id.is_some() && source_task_id.is_none() {
        return Err(DbError::Check(
            "evidence source execution requires a source Task".to_owned(),
        ));
    }

    let Some(task_id) = source_task_id else {
        attachment.source_task_version = None;
        attachment.source_context_digest = None;
        return Ok(());
    };

    let task = sqlx::query(
        "SELECT t.project_id, t.version, t.updated_at, p.primary_repo_id
         FROM task t
         JOIN project p ON p.id = t.project_id
         WHERE t.id = ?",
    )
    .bind(&task_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    let task_project: String = task.try_get("project_id")?;
    if task_project != attachment.project_id
        || attachment
            .task_id
            .as_deref()
            .is_some_and(|id| id != task_id)
    {
        return Err(DbError::Check(
            "evidence source Task is outside the attached Project scope".to_owned(),
        ));
    }
    let task_version: i64 = task.try_get("version")?;
    if task_version < 1 {
        return Err(DbError::Check(
            "evidence source Task has an invalid version".to_owned(),
        ));
    }
    attachment.source_task_version = Some(task_version);

    let execution = if let Some(source_execution_id) = attachment.source_execution_id.as_deref() {
        sqlx::query(
            "SELECT id, task_id, status, role, before_sha, after_sha, summary, workspace_id,
                    created_at, updated_at
             FROM execution
             WHERE id = ? AND task_id = ?",
        )
        .bind(source_execution_id)
        .bind(&task_id)
        .fetch_optional(&mut **transaction)
        .await?
    } else {
        sqlx::query(
            "SELECT id, task_id, status, role, before_sha, after_sha, summary, workspace_id,
                    created_at, updated_at
             FROM execution
             WHERE task_id = ?
             ORDER BY updated_at DESC, id DESC LIMIT 1",
        )
        .bind(&task_id)
        .fetch_optional(&mut **transaction)
        .await?
    };
    if attachment.source_execution_id.is_some() && execution.is_none() {
        return Err(DbError::NotFound);
    }

    let repository_id = match execution.as_ref() {
        Some(execution) => {
            let workspace_id = execution
                .try_get::<Option<String>, _>("workspace_id")?
                .ok_or_else(|| {
                    DbError::Check(
                        "evidence execution has no Workspace repository provenance".to_owned(),
                    )
                })?;
            sqlx::query_scalar::<_, String>(
                "SELECT w.repo_id
                 FROM workspace w
                 JOIN repo r ON r.id = w.repo_id AND r.project_id = ?
                 WHERE w.id = ?",
            )
            .bind(&task_project)
            .bind(workspace_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| {
                DbError::Check(
                    "evidence execution Workspace repository provenance is unavailable".to_owned(),
                )
            })?
        }
        None => task
            .try_get::<Option<String>, _>("primary_repo_id")?
            .unwrap_or_default(),
    };
    let repository = if repository_id.is_empty() {
        None
    } else {
        sqlx::query(
            "SELECT id, name, work_mode, remote_url, default_branch
             FROM repo
             WHERE id = ? AND project_id = ?",
        )
        .bind(&repository_id)
        .bind(&task_project)
        .fetch_optional(&mut **transaction)
        .await?
    };
    if execution.is_some() && repository.is_none() {
        return Err(DbError::Check(
            "evidence execution repository provenance is unavailable".to_owned(),
        ));
    }

    let execution_id = execution
        .as_ref()
        .map(|row| row.try_get::<String, _>("id"))
        .transpose()?;
    let review = if let Some(execution_id) = execution_id.as_deref() {
        sqlx::query(
            "SELECT id, status, step_results_json, created_at, updated_at
             FROM review
             WHERE execution_id = ?
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(execution_id)
        .fetch_optional(&mut **transaction)
        .await?
    } else {
        None
    };

    let context = serde_json::json!({
        "task_id": task_id,
        "task_version": task_version,
        "repository_id": repository.as_ref().map(|row| row.try_get::<String, _>("id")).transpose()?.unwrap_or_default(),
        "repository_name": repository.as_ref().map(|row| row.try_get::<String, _>("name")).transpose()?.unwrap_or_default(),
        "repository_kind": repository.as_ref().map(|row| row.try_get::<String, _>("work_mode")).transpose()?.unwrap_or_default(),
        "remote_url": normalize_repo_remote_url(repository.as_ref().map(|row| row.try_get::<Option<String>, _>("remote_url")).transpose()?.flatten()),
        "default_branch": repository.as_ref().map(|row| row.try_get::<String, _>("default_branch")).transpose()?.unwrap_or_default(),
        "execution_id": execution.as_ref().map(|row| row.try_get::<String, _>("id")).transpose()?,
        "execution_status": execution.as_ref().map(|row| row.try_get::<String, _>("status")).transpose()?,
        "execution_role": execution.as_ref().map(|row| row.try_get::<String, _>("role")).transpose()?,
        "before_sha": execution.as_ref().map(|row| row.try_get::<Option<String>, _>("before_sha")).transpose()?.flatten(),
        "after_sha": execution.as_ref().map(|row| row.try_get::<Option<String>, _>("after_sha")).transpose()?.flatten(),
        "execution_summary": execution.as_ref().map(|row| row.try_get::<Option<String>, _>("summary")).transpose()?.flatten(),
        "execution_created_at": execution.as_ref().map(|row| row.try_get::<String, _>("created_at")).transpose()?,
        "execution_updated_at": execution.as_ref().map(|row| row.try_get::<String, _>("updated_at")).transpose()?,
        "review_id": review.as_ref().map(|row| row.try_get::<String, _>("id")).transpose()?,
        "review_status": review.as_ref().map(|row| row.try_get::<String, _>("status")).transpose()?,
        "ci_results": review.as_ref().map(|row| row.try_get::<String, _>("step_results_json")).transpose()?,
        "audit_result": Value::Null,
        "human_decision": Value::Null,
        "review_created_at": review.as_ref().map(|row| row.try_get::<String, _>("created_at")).transpose()?,
        "review_updated_at": review.as_ref().map(|row| row.try_get::<String, _>("updated_at")).transpose()?,
        "observed_at": task.try_get::<String, _>("updated_at")?,
    });
    let canonical_context = canonicalize_evidence_json(&context);
    attachment.source_context_digest = Some(evidence_context_digest(&canonical_context)?);
    Ok(())
}

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

/// Resolve a committed evidence command from its immutable receipt/event
/// provenance.  This is intentionally performed before any current asset,
/// milestone, or authorization checks so a response-loss retry can replay the
/// exact original attachment even if one of those mutable projections has
/// since changed.
async fn replay_project_media_attachment_command_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &CreateProjectMediaAttachmentMutation,
    receipt: &CommandReceipt,
) -> Result<ProjectMediaAttachment> {
    let outcome: serde_json::Value =
        serde_json::from_str(&receipt.outcome_json).map_err(|_| DbError::IdempotencyConflict)?;
    let attachment_id = outcome
        .get("attachment_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(DbError::IdempotencyConflict)?;
    let event = sqlx::query(
        "SELECT event_type, entity_type, entity_id, actor_type, actor_id,
                scope_type, scope_id, correlation_id, causation_id,
                causation_depth, payload_json
         FROM domain_event WHERE id = ?",
    )
    .bind(&receipt.event_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::IdempotencyConflict)?;
    let event_type: String = event.try_get("event_type")?;
    let entity_type: String = event.try_get("entity_type")?;
    let entity_id: String = event.try_get("entity_id")?;
    let actor_type: String = event.try_get("actor_type")?;
    let actor_id: Option<String> = event.try_get("actor_id")?;
    let scope_type: String = event.try_get("scope_type")?;
    let scope_id: String = event.try_get("scope_id")?;
    let correlation_id: String = event.try_get("correlation_id")?;
    let causation_id: Option<String> = event.try_get("causation_id")?;
    let causation_depth: i64 = event.try_get("causation_depth")?;
    let payload_json: String = event.try_get("payload_json")?;
    let payload: serde_json::Value = serde_json::from_str(&payload_json)
        .map_err(|error| DbError::Check(format!("invalid evidence event: {error}")))?;

    let same_provenance = event_type == "project.evidence.attached"
        && entity_type == "project_media_attachment"
        && entity_id == attachment_id
        && actor_type == receipt.principal_type
        && actor_id.as_deref() == Some(receipt.principal_id.as_str())
        && scope_type == receipt.scope_type
        && scope_id == receipt.scope_id
        && correlation_id == receipt.correlation_id
        && causation_id == receipt.causation_id
        && causation_depth == receipt.causation_depth
        && scope_type == "project"
        && scope_id == input.attachment.project_id
        && payload.get("project_id").and_then(|value| value.as_str())
            == Some(input.attachment.project_id.as_str())
        && payload.get("milestone_id").and_then(|value| value.as_str())
            == input.attachment.milestone_id.as_deref()
        && payload.get("asset_id").and_then(|value| value.as_str())
            == Some(input.attachment.asset_id.as_str())
        && payload.get("evidence_id").and_then(|value| value.as_str()) == Some(attachment_id)
        && payload.get("checksum").and_then(|value| value.as_str())
            == input.attachment.checksum.as_deref()
        && payload
            .get("expected_milestone_version")
            .and_then(|value| value.as_i64())
            == Some(input.expected_milestone_version)
        && payload
            .get("authorization_event_id")
            .and_then(|value| value.as_str())
            == Some(input.authorization_event_id.as_str())
        && payload
            .get("mutation_fingerprint")
            .and_then(|value| value.as_str())
            == Some(input.mutation_fingerprint.as_str())
        && payload.get("attachment")
            == Some(&serde_json::json!({
                "id": attachment_id,
                "project_id": input.attachment.project_id,
                "asset_id": input.attachment.asset_id,
                "attachment_kind": input.attachment.attachment_kind,
                "task_media_id": input.attachment.task_media_id,
                "task_id": input.attachment.task_id,
                "milestone_id": input.attachment.milestone_id,
                "milestone_check_id": input.attachment.milestone_check_id,
                "source_task_id": input.attachment.source_task_id,
                "source_execution_id": input.attachment.source_execution_id,
                "source_validation_id": input.attachment.source_validation_id,
                "acceptance_check_ids_json": input.attachment.acceptance_check_ids_json,
                "caption": input.attachment.caption,
                "evidence_kind": input.attachment.evidence_kind,
                "checksum": input.attachment.checksum,
                "availability": input.attachment.availability,
                "project_url": input.attachment.project_url,
                "author_type": input.attachment.author_type,
                "author_id": input.attachment.author_id,
                "authorization_json": input.attachment.authorization_json,
            }));
    if !same_provenance {
        return Err(DbError::IdempotencyConflict);
    }

    let row = sqlx::query(&format!(
        "SELECT {PROJECT_MEDIA_ATTACHMENT_COLUMNS}
         FROM project_media_attachment
         WHERE id = ? AND project_id = ? AND attachment_kind = 'evidence'"
    ))
    .bind(attachment_id)
    .bind(&input.attachment.project_id)
    .fetch_one(&mut **transaction)
    .await?;
    map_project_media_attachment(&row)
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
    connection: &mut sqlx::SqliteConnection,
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
    .fetch_one(connection)
    .await?
        != 0)
}

pub(super) async fn reconcile_media_asset_in_tx(
    connection: &mut sqlx::SqliteConnection,
    asset_id: &str,
    now: &str,
) -> Result<MediaAsset> {
    let row = sqlx::query(&format!(
        "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
    ))
    .bind(asset_id)
    .fetch_optional(&mut *connection)
    .await?
    .ok_or(DbError::NotFound)?;
    let asset = map_media_asset(&row)?;

    let referenced = media_asset_is_referenced(connection, asset_id).await?;

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
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }

    let row = sqlx::query(&format!(
        "SELECT {MEDIA_ASSET_COLUMNS} FROM media_asset WHERE id = ?"
    ))
    .bind(asset_id)
    .fetch_one(connection)
    .await?;
    map_media_asset(&row)
}

pub(super) async fn reconcile_legacy_task_media_delete_in_tx(
    connection: &mut sqlx::SqliteConnection,
    asset_id: &str,
    deleted_at: &str,
) -> Result<()> {
    let (availability, version): (String, i64) =
        sqlx::query_as("SELECT availability, version FROM media_asset WHERE id = ?")
            .bind(asset_id)
            .fetch_optional(&mut *connection)
            .await?
            .ok_or(DbError::NotFound)?;
    let referenced = media_asset_is_referenced(connection, asset_id).await?;
    let state = if availability == "purged" {
        "deleted"
    } else if referenced {
        "referenced"
    } else {
        "gc_candidate"
    };
    let candidate_at = (state == "gc_candidate").then_some(deleted_at);
    let asset_deleted_at = (state != "referenced").then_some(deleted_at);
    let result = sqlx::query(
        "UPDATE media_asset
         SET gc_state = ?, gc_candidate_at = ?, deleted_at = ?,
             gc_lease_owner = NULL, gc_lease_expires_at = NULL,
             version = version + 1, updated_at = ?
         WHERE id = ? AND version = ?",
    )
    .bind(state)
    .bind(candidate_at)
    .bind(asset_deleted_at)
    .bind(deleted_at)
    .bind(asset_id)
    .bind(version)
    .execute(connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    Ok(())
}

#[derive(Debug)]
pub struct InsertReleaseEvidencePinInput<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
    pub release_id: &'a str,
    pub asset_id: &'a str,
    pub attachment_id: &'a str,
    pub task_media_id: Option<&'a str>,
    pub asset_checksum: &'a str,
    pub attachment_digest: &'a str,
    pub availability: &'a str,
    pub pin_digest: &'a str,
    pub created_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_release_evidence_pin_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &InsertReleaseEvidencePinInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO project_release_media_pin (
                    id, project_id, release_id, asset_id, attachment_id,
                    legacy_task_media_id, asset_checksum, attachment_digest,
                    availability, pin_digest, created_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(input.id)
        .bind(input.project_id)
        .bind(input.release_id)
        .bind(input.asset_id)
        .bind(input.attachment_id)
        .bind(input.task_media_id)
        .bind(input.asset_checksum)
        .bind(input.attachment_digest)
        .bind(input.availability)
        .bind(input.pin_digest)
        .bind(input.created_at)
        .execute(&mut *connection)
        .await?;
        if result.rows_affected() > 0 {
            reconcile_media_asset_in_tx(connection, input.asset_id, input.created_at).await?;
        }
        Ok(result.rows_affected())
    }
}

#[derive(Debug, Clone)]
pub struct MilestoneEvidenceRow {
    pub author_type: String,
    pub author_id: Option<String>,
    pub authorization_json: String,
    pub id: String,
    pub asset_id: String,
    pub task_id: Option<String>,
    pub source_task_id: Option<String>,
    pub source_execution_id: Option<String>,
    pub source_validation_id: Option<String>,
    pub source_context_digest: Option<String>,
    pub source_definition_revision_id: Option<String>,
    pub milestone_id: Option<String>,
    pub acceptance_check_ids_json: String,
    pub caption: Option<String>,
    pub evidence_kind: Option<String>,
    pub checksum: Option<String>,
    pub availability: String,
    pub created_at: String,
    pub version: i64,
    pub deleted_at: Option<String>,
    pub source_task_version: Option<i64>,
}

impl SqliteDb {
    pub async fn milestone_evidence_rows_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<MilestoneEvidenceRow>> {
        let rows = sqlx::query(
            "SELECT a.*, m.legacy_task_media_id, m.checksum AS asset_checksum
             FROM project_media_attachment a
             JOIN media_asset m ON m.id = a.asset_id AND m.project_id = a.project_id
             WHERE a.project_id = ? AND a.milestone_id = ?
               AND a.attachment_kind = 'evidence' AND a.deleted_at IS NULL
             ORDER BY a.id ASC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(connection)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(MilestoneEvidenceRow {
                    author_type: row.try_get("author_type")?,
                    author_id: row.try_get("author_id")?,
                    authorization_json: row.try_get("authorization_json")?,
                    id: row.try_get("id")?,
                    asset_id: row.try_get("asset_id")?,
                    task_id: row.try_get("task_id")?,
                    source_task_id: row.try_get("source_task_id")?,
                    source_execution_id: row.try_get("source_execution_id")?,
                    source_validation_id: row.try_get("source_validation_id")?,
                    source_context_digest: row.try_get("source_context_digest")?,
                    source_definition_revision_id: row.try_get("source_definition_revision_id")?,
                    milestone_id: row.try_get("milestone_id")?,
                    acceptance_check_ids_json: row.try_get("acceptance_check_ids_json")?,
                    caption: row.try_get("caption")?,
                    evidence_kind: row.try_get("evidence_kind")?,
                    checksum: row.try_get("checksum")?,
                    availability: row.try_get("availability")?,
                    created_at: row.try_get("created_at")?,
                    version: row.try_get("version")?,
                    deleted_at: row.try_get("deleted_at")?,
                    source_task_version: row.try_get("source_task_version")?,
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct MilestoneReleasePinMetadataRow {
    pub id: String,
    pub asset_id: String,
    pub task_media_id: Option<String>,
    pub project_url: Option<String>,
    pub attachment_digest: Option<String>,
    pub asset_checksum: Option<String>,
}

impl SqliteDb {
    pub async fn milestone_release_pin_metadata_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<MilestoneReleasePinMetadataRow>> {
        let rows = sqlx::query(
            "SELECT a.id, a.asset_id, a.task_media_id, a.project_url,
                    a.checksum AS attachment_digest, m.checksum AS asset_checksum
             FROM project_media_attachment a
             JOIN media_asset m ON m.id = a.asset_id AND m.project_id = a.project_id
             WHERE a.project_id = ? AND a.milestone_id = ?
               AND a.attachment_kind = 'evidence' AND a.deleted_at IS NULL
             ORDER BY a.id ASC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(connection)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(MilestoneReleasePinMetadataRow {
                    id: row.try_get("id")?,
                    asset_id: row.try_get("asset_id")?,
                    task_media_id: row.try_get("task_media_id")?,
                    project_url: row.try_get("project_url")?,
                    attachment_digest: row.try_get("attachment_digest")?,
                    asset_checksum: row.try_get("asset_checksum")?,
                })
            })
            .collect()
    }
}

impl SqliteDb {
    pub async fn evidence_authorization_json_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        attachment_id: &str,
        project_id: &str,
    ) -> crate::Result<String> {
        let result = sqlx::query_scalar::<_, String>(
            "SELECT authorization_json FROM project_media_attachment
                     WHERE id = ? AND project_id = ?",
        )
        .bind(attachment_id)
        .bind(project_id)
        .fetch_one(connection)
        .await?;
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct ReleaseEvidencePinRow {
    pub id: String,
    pub release_id: String,
    pub attachment_id: Option<String>,
    pub asset_id: String,
    pub asset_checksum: Option<String>,
    pub attachment_digest: Option<String>,
    pub availability: String,
    pub pin_digest: Option<String>,
    pub created_at: String,
    pub live_asset_checksum: Option<String>,
    pub tombstone_availability: Option<String>,
}

impl SqliteDb {
    pub async fn release_evidence_pin_rows(
        &self,
        project_id: &str,
        release_id: &str,
    ) -> crate::Result<Vec<ReleaseEvidencePinRow>> {
        let rows = sqlx::query(
            "SELECT p.id, p.release_id, p.attachment_id, p.asset_id,
                    p.asset_checksum, p.attachment_digest, p.availability,
                    p.pin_digest, p.created_at, m.checksum AS live_asset_checksum,
                    (SELECT t.availability FROM media_asset_tombstone t
                     WHERE t.asset_id = p.asset_id
                       AND (t.release_pin_id = p.id OR
                            (t.release_pin_id IS NULL AND t.release_id = p.release_id))
                     ORDER BY t.created_at DESC, t.id DESC LIMIT 1) AS tombstone_availability
             FROM project_release_media_pin p
             JOIN media_asset m ON m.id = p.asset_id
             WHERE p.project_id = ? AND p.release_id = ? ORDER BY p.id ASC",
        )
        .bind(project_id)
        .bind(release_id)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ReleaseEvidencePinRow {
                    id: row.try_get("id")?,
                    release_id: row.try_get("release_id")?,
                    attachment_id: row.try_get("attachment_id")?,
                    asset_id: row.try_get("asset_id")?,
                    asset_checksum: row.try_get("asset_checksum")?,
                    attachment_digest: row.try_get("attachment_digest")?,
                    availability: row.try_get("availability")?,
                    pin_digest: row.try_get("pin_digest")?,
                    created_at: row.try_get("created_at")?,
                    live_asset_checksum: row.try_get("live_asset_checksum")?,
                    tombstone_availability: row.try_get("tombstone_availability")?,
                })
            })
            .collect()
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct NativeProjectArtifactRow {
    row: sqlx::sqlite::SqliteRow,
}
impl NativeProjectArtifactRow {
    pub fn content_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content_type")?)
    }
    pub fn byte_size(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("byte_size")?)
    }
    pub fn storage_key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("storage_key")?)
    }
    pub fn asset_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("asset_id")?)
    }
    pub fn task_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("task_id")?)
    }
    pub fn display_filename(&self) -> crate::Result<String> {
        Ok(self.row.try_get("display_filename")?)
    }
    pub fn checksum(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("checksum")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
}

impl SqliteDb {
    pub async fn native_project_artifacts(
        &self,
        project_id: &str,
        task_filter: Option<&str>,
        task_id: Option<&str>,
        limit: i64,
    ) -> crate::Result<Vec<NativeProjectArtifactRow>> {
        let rows = sqlx::query(
            "SELECT a.id AS asset_id, m.task_id, a.display_filename, a.content_type,
                    a.byte_size, a.checksum, a.storage_key, m.created_at
             FROM task_media m
             JOIN media_asset a ON a.legacy_task_media_id = m.id
             JOIN task t ON t.id = m.task_id
             WHERE t.project_id = ? AND t.deleted_at IS NULL AND m.deleted_at IS NULL
               AND (? IS NULL OR m.task_id = ?)
             ORDER BY m.created_at DESC, m.id DESC LIMIT ?",
        )
        .bind(project_id)
        .bind(task_filter)
        .bind(task_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| NativeProjectArtifactRow { row })
            .collect())
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct EvidenceAttachmentSourceRow {
    row: sqlx::sqlite::SqliteRow,
}
impl EvidenceAttachmentSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn project_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("project_id")?)
    }
    pub fn asset_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("asset_id")?)
    }
    pub fn attachment_kind(&self) -> crate::Result<String> {
        Ok(self.row.try_get("attachment_kind")?)
    }
    pub fn task_media_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("task_media_id")?)
    }
    pub fn task_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("task_id")?)
    }
    pub fn milestone_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("milestone_id")?)
    }
    pub fn milestone_check_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("milestone_check_id")?)
    }
    pub fn source_task_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("source_task_id")?)
    }
    pub fn source_execution_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("source_execution_id")?)
    }
    pub fn source_validation_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("source_validation_id")?)
    }
    pub fn source_task_version(&self) -> crate::Result<Option<i64>> {
        Ok(self.row.try_get("source_task_version")?)
    }
    pub fn source_context_digest(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("source_context_digest")?)
    }
    pub fn source_definition_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("source_definition_revision_id")?)
    }
    pub fn acceptance_check_ids_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("acceptance_check_ids_json")?)
    }
    pub fn caption(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("caption")?)
    }
    pub fn evidence_kind(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("evidence_kind")?)
    }
    pub fn checksum(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("checksum")?)
    }
    pub fn availability(&self) -> crate::Result<String> {
        Ok(self.row.try_get("availability")?)
    }
    pub fn project_url(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("project_url")?)
    }
    pub fn author_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("author_type")?)
    }
    pub fn author_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("author_id")?)
    }
    pub fn authorization_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("authorization_json")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
    pub fn deleted_at(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("deleted_at")?)
    }
    pub fn updated_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("updated_at")?)
    }
}

impl SqliteDb {
    pub async fn validation_evidence_asset_in_project(
        &self,
        asset_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM media_asset
                 WHERE id = ? AND project_id = ? AND deleted_at IS NULL",
        )
        .bind(asset_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn evidence_attachment_source(
        &self,
        attachment_id: &str,
    ) -> crate::Result<Option<EvidenceAttachmentSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, project_id, asset_id, attachment_kind, task_media_id, task_id,
                milestone_id, milestone_check_id, source_task_id, source_execution_id,
                source_validation_id, source_task_version, source_context_digest,
                source_definition_revision_id, acceptance_check_ids_json, caption, evidence_kind,
                checksum, availability, project_url, author_type, author_id,
                authorization_json, version, created_at, deleted_at, updated_at
         FROM project_media_attachment WHERE id = ? AND attachment_kind = 'evidence'",
        )
        .bind(attachment_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| EvidenceAttachmentSourceRow { row }))
    }
}
