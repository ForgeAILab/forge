//! Project-scoped media and milestone-evidence routes.
//!
//! Project media is an authorized projection over the shared `media_asset`
//! row.  The bytes are never copied when an asset is attached as evidence;
//! release pins retain the asset after a Task attachment is removed.

use std::path::{Component, Path as StdPath, PathBuf};

use api_types::{
    AttachEvidenceRequest, EvidenceAttachment, EvidenceAttachmentListResponse,
    EvidenceAvailability, EvidenceKind, MediaAsset, ProjectMediaListResponse,
};
use axum::{
    extract::{Multipart, Path, Query, State},
    http::{
        header::{CONTENT_DISPOSITION, CONTENT_TYPE, X_CONTENT_TYPE_OPTIONS},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
    Json,
};
use db::{
    new_uuid_v4, now_rfc3339, CreateProjectMediaAsset, ProjectMediaTombstone, ProjectMemberRepo,
    ProjectRepo, SharedMediaRepo, SoftDeleteProjectMediaAttachmentMutation,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::AuthenticatedUser,
    state::AppState,
};
use services::{
    ProjectArtifactCommandService, ProjectCommandAuthorization, ProjectEvidenceCommand,
};

const EVIDENCE_ATTACH_ACTION: &str = "project.evidence.attach";
const EVIDENCE_REMOVE_ACTION: &str = "project.evidence.remove";
const MEDIA_UPLOAD_ACTION: &str = "project.media.upload";
const MEDIA_REDACT_ACTION: &str = "project.media.redact";
const MEDIA_PURGE_ACTION: &str = "project.media.purge";
const MAX_FILENAME_BYTES: usize = 255;
const ALLOWED_CONTENT_TYPES: &[&str] = &[
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
    "video/mp4",
    "video/webm",
    "video/quicktime",
    "application/pdf",
    "text/plain",
    "application/zip",
];

#[derive(Debug, Deserialize)]
pub struct MediaListQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct EvidenceListQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

pub async fn list_media(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Query(query): Query<MediaListQuery>,
) -> ApiResult<Json<ProjectMediaListResponse>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let (cursor_created_at, cursor_id) = decode_cursor(query.cursor.as_deref())?
        .map_or((None, None), |(created_at, id)| {
            (Some(created_at), Some(id))
        });
    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let mut statement = sqlx::query(
        "SELECT m.* FROM media_asset m
         WHERE m.project_id = ? AND m.deleted_at IS NULL
         ORDER BY m.created_at ASC, m.id ASC LIMIT ?",
    );
    if let (Some(created_at), Some(id)) = (cursor_created_at, cursor_id) {
        statement = sqlx::query(
            "SELECT m.* FROM media_asset m
             WHERE m.project_id = ? AND m.deleted_at IS NULL
               AND (m.created_at > ? OR (m.created_at = ? AND m.id > ?))
             ORDER BY m.created_at ASC, m.id ASC LIMIT ?",
        )
        .bind(&project_id)
        .bind(created_at.clone())
        .bind(created_at)
        .bind(id)
        .bind(limit + 1);
    } else {
        statement = statement.bind(&project_id).bind(limit + 1);
    }
    let mut rows = statement.fetch_all(state.db.pool()).await?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let mut assets = Vec::with_capacity(rows.len());
    for row in rows {
        let asset_id: String = row.try_get("id")?;
        let asset = SharedMediaRepo::get_media_asset(&*state.db, &asset_id)
            .await?
            .filter(|asset| asset.project_id == project_id)
            .ok_or_else(|| ApiError::internal("project media row disappeared during listing"))?;
        let asset = reconcile_asset_checksum(&state, asset).await?;
        let task_media_ids = sqlx::query_scalar::<_, String>(
            "SELECT task_media_id FROM project_media_attachment
             WHERE project_id = ? AND asset_id = ? AND task_media_id IS NOT NULL
               AND deleted_at IS NULL ORDER BY id ASC",
        )
        .bind(&project_id)
        .bind(&asset_id)
        .fetch_all(state.db.pool())
        .await?;
        let task_media_ids = if task_media_ids.is_empty() {
            asset.legacy_task_media_id.clone().into_iter().collect()
        } else {
            task_media_ids
        };
        let stable_project_url = Some(project_media_url(&project_id, &asset_id));
        assets.push(media_asset_response_from_model(
            asset,
            task_media_ids,
            stable_project_url,
        )?);
    }
    let next_cursor = has_more
        .then(|| {
            assets
                .last()
                .map(|asset| encode_cursor(&asset.created_at, &asset.id))
        })
        .flatten();
    Ok(Json(ProjectMediaListResponse {
        items: assets,
        next_cursor,
        has_more,
    }))
}

/// Upload a Project-owned asset.  This is intentionally separate from Task
/// media: it creates one `media_asset` row with no legacy Task attachment.
pub async fn upload_media(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    mut multipart: Multipart,
) -> ApiResult<(StatusCode, Json<MediaAsset>)> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let mut mutation: Option<api_types::MutationEnvelope> = None;
    let mut upload: Option<(String, String, Vec<u8>)> = None;
    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::bad_request(format!("invalid multipart body: {error}")))?
    {
        let field_name = field
            .name()
            .ok_or_else(|| ApiError::bad_request("multipart field name is required"))?;
        match field_name {
            "mutation" => {
                if mutation.is_some() {
                    return Err(ApiError::bad_request("only one mutation field is allowed"));
                }
                if field.file_name().is_some() {
                    return Err(ApiError::bad_request(
                        "mutation must be a text field without a filename",
                    ));
                }
                let value = read_text_field(&mut field, "mutation", 16 * 1024).await?;
                let request: api_types::ProjectMediaUploadRequest = serde_json::from_str(&value)?;
                mutation = Some(request.mutation);
            }
            "file" => {
                let raw_filename = field
                    .file_name()
                    .map(str::to_owned)
                    .ok_or_else(|| ApiError::bad_request("file filename is required"))?;
                if upload.is_some() {
                    return Err(ApiError::bad_request("only one file may be uploaded"));
                }
                let filename = normalize_filename(&raw_filename)?;
                let content_type = field
                    .content_type()
                    .map(normalize_content_type)
                    .ok_or_else(|| ApiError::bad_request("content_type is required"))?;
                if !ALLOWED_CONTENT_TYPES.contains(&content_type.as_str()) {
                    return Err(ApiError::bad_request(format!(
                        "unsupported content_type: {content_type}"
                    )));
                }
                let bytes = read_file_field(
                    &mut field,
                    state.effective_config.server.media_upload_limit_bytes,
                )
                .await?;
                validate_content_signature(&content_type, &filename, &bytes)?;
                upload = Some((filename, content_type, bytes));
            }
            other => {
                return Err(ApiError::bad_request(format!(
                    "unexpected multipart field: {other}"
                )));
            }
        }
    }
    let mutation = mutation.ok_or_else(|| ApiError::bad_request("mutation is required"))?;
    validate_authorization(&mutation.authorization, &user.user_id, MEDIA_UPLOAD_ACTION)?;
    if mutation.idempotency_key.trim().is_empty() {
        return Err(ApiError::bad_request("idempotency_key is required"));
    }
    let mutation_fingerprint = mutation_digest(&mutation);
    let (filename, content_type, bytes) =
        upload.ok_or_else(|| ApiError::bad_request("file is required"))?;
    let asset_id = new_uuid_v4();
    let storage_key = format!("projects/{project_id}/{asset_id}__{filename}");
    let staging_key = format!("pending/projects/{project_id}/{asset_id}__{filename}.uploading");
    let checksum = hex::encode(Sha256::digest(&bytes));
    let upload = SharedMediaRepo::begin_project_media_upload(
        &*state.db,
        db::BeginProjectMediaUpload {
            project_id: project_id.clone(),
            idempotency_key: mutation.idempotency_key.clone(),
            mutation_fingerprint: mutation_fingerprint.clone(),
            expected_project_version: mutation.expected_version,
            asset_id,
            final_storage_key: storage_key,
            staging_storage_key: staging_key,
            display_filename: filename,
            content_type,
            byte_size: i64::try_from(bytes.len())
                .map_err(|_| ApiError::bad_request("file is too large"))?,
            checksum,
            created_at: now_rfc3339(),
        },
    )
    .await?;
    if upload.status == "finalized" {
        let asset = SharedMediaRepo::get_media_asset(&*state.db, &upload.asset_id)
            .await?
            .ok_or_else(|| ApiError::internal("replayed media upload asset is missing"))?;
        let asset = reconcile_asset_checksum(&state, asset).await?;
        return Ok((
            StatusCode::OK,
            Json(media_asset_response_from_model(
                asset,
                Vec::new(),
                Some(project_media_url(&project_id, &upload.asset_id)),
            )?),
        ));
    }
    let staging_path = media_storage_path(
        &state,
        upload
            .staging_storage_key
            .as_deref()
            .ok_or_else(|| ApiError::internal("pending media upload has no staging key"))?,
    )?;
    let final_path = media_storage_path(&state, &upload.final_storage_key)?;
    if let Some(parent) = staging_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&staging_path, &bytes).await?;
    let insert = SharedMediaRepo::create_project_media_asset(
        &*state.db,
        CreateProjectMediaAsset {
            id: upload.asset_id.clone(),
            project_id: upload.project_id.clone(),
            display_filename: upload.display_filename.clone(),
            content_type: upload.content_type.clone(),
            byte_size: i64::try_from(bytes.len())
                .map_err(|_| ApiError::bad_request("file is too large"))?,
            storage_key: upload.final_storage_key.clone(),
            checksum: upload.checksum.clone(),
            idempotency_key: upload.idempotency_key.clone(),
            mutation_fingerprint,
            expected_project_version: upload.expected_project_version,
            actor_type: "user".to_owned(),
            actor_id: Some(user.user_id.clone()),
            authorization_event_id: mutation.authorization.event_id,
            created_at: upload.created_at.clone(),
        },
    )
    .await;
    let asset = match insert {
        Ok(asset) => asset,
        Err(error) => {
            let _ = tokio::fs::remove_file(&staging_path).await;
            return Err(error.into());
        }
    };
    promote_staged_file(
        &staging_path,
        &final_path,
        asset.byte_size,
        asset.checksum.as_deref(),
    )
    .await?;
    let asset = SharedMediaRepo::finalize_project_media_upload(
        &*state.db,
        &project_id,
        &asset.id,
        &now_rfc3339(),
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(media_asset_response_from_model(
            asset.clone(),
            Vec::new(),
            Some(project_media_url(&project_id, &asset.id)),
        )?),
    ))
}

pub async fn serve_media(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, asset_id)): Path<(String, String)>,
) -> ApiResult<Response> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let asset = SharedMediaRepo::get_media_asset(&*state.db, &asset_id)
        .await?
        .filter(|asset| asset.project_id == project_id)
        .ok_or_else(|| ApiError::not_found("media", asset_id.clone()))?;
    if asset.availability != "available" || asset.deleted_at.is_some() {
        return Err(ApiError::not_found("media", asset_id));
    }
    let asset = reconcile_asset_checksum(&state, asset).await?;
    let path = media_storage_path(&state, &asset.storage_key)?;
    let bytes = tokio::fs::read(&path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ApiError::not_found("media", asset.id.clone())
        } else {
            ApiError::from(error)
        }
    })?;
    verify_media_bytes(&asset, &bytes)?;
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&asset.content_type)
            .map_err(|error| ApiError::internal(format!("invalid content type: {error}")))?,
    );
    let filename = header_safe_filename(&asset.display_filename);
    let disposition = if is_safe_inline_content_type(&asset.content_type) {
        "inline"
    } else {
        "attachment"
    };
    headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(r#"{disposition}; filename="{filename}""#))
            .map_err(|error| ApiError::internal(format!("invalid content disposition: {error}")))?,
    );
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    Ok((headers, bytes).into_response())
}

pub async fn redact_media(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, asset_id)): Path<(String, String)>,
    Json(request): Json<api_types::ProjectMediaTombstoneRequest>,
) -> ApiResult<Json<MediaAsset>> {
    tombstone_media(
        &state,
        &user,
        &project_id,
        &asset_id,
        request,
        "redacted",
        MEDIA_REDACT_ACTION,
    )
    .await
}

pub async fn purge_media(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, asset_id)): Path<(String, String)>,
    Json(request): Json<api_types::ProjectMediaTombstoneRequest>,
) -> ApiResult<Json<MediaAsset>> {
    tombstone_media(
        &state,
        &user,
        &project_id,
        &asset_id,
        request,
        "purged",
        MEDIA_PURGE_ACTION,
    )
    .await
}

async fn tombstone_media(
    state: &AppState,
    user: &AuthenticatedUser,
    project_id: &str,
    asset_id: &str,
    request: api_types::ProjectMediaTombstoneRequest,
    target_availability: &str,
    action: &str,
) -> ApiResult<Json<MediaAsset>> {
    // A committed receipt is immutable: resolve it after checking only the
    // Project scope, before validating the caller's current authority.  This
    // lets an exact retry return the stored projection while any changed
    // authority, target, version, or reason is rejected by the repository's
    // complete receipt comparison.
    require_project_access(state, project_id, &user.user_id).await?;
    if request.mutation.idempotency_key.trim().is_empty() {
        return Err(ApiError::bad_request("idempotency_key is required"));
    }
    let mutation_fingerprint = mutation_digest(&request);
    let authorization_json = serde_json::to_string(&request.mutation.authorization)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let tombstone = ProjectMediaTombstone {
        asset_id: asset_id.to_owned(),
        project_id: project_id.to_owned(),
        expected_version: request.mutation.expected_version,
        idempotency_key: request.mutation.idempotency_key.clone(),
        mutation_fingerprint,
        target_availability: target_availability.to_owned(),
        // Bind the immutable receipt to the currently authenticated
        // principal before validation.  The submitted authorization
        // principal kind/id is also serialized in `authorization_json`, so a
        // changed authority receipt conflicts rather than being re-authorized
        // as a new request.
        principal_type: "user".to_owned(),
        principal_id: user.user_id.clone(),
        authorization_basis: request.mutation.authorization.authorization_basis.clone(),
        authorization_action: request.mutation.authorization.action.clone(),
        authorization_occurred_at: request.mutation.authorization.occurred_at.clone(),
        authorization_event_id: request.mutation.authorization.event_id.clone(),
        authorization_json,
        reason: request.reason.clone(),
        created_at: now_rfc3339(),
    };
    if let Some(replayed) =
        SharedMediaRepo::replay_project_media_tombstone(&*state.db, tombstone.clone()).await?
    {
        if target_availability == "purged" {
            let path = media_storage_path(state, &replayed.storage_key)?;
            remove_file_if_exists(&path).await?;
        }
        return Ok(Json(media_asset_response_from_model(
            replayed,
            Vec::new(),
            Some(project_media_url(project_id, asset_id)),
        )?));
    }

    // No receipt exists, so this is a new mutation and must pass the current
    // role and explicit user authorization checks before any write occurs.
    require_project_media_admin(state, project_id, &user.user_id).await?;
    validate_authorization(&request.mutation.authorization, &user.user_id, action)?;
    if request.reason.trim().is_empty() || request.reason.len() > 4096 {
        return Err(ApiError::bad_request(
            "reason must be between 1 and 4096 bytes",
        ));
    }
    let asset = SharedMediaRepo::get_media_asset(&*state.db, asset_id)
        .await?
        .filter(|asset| asset.project_id == project_id)
        .ok_or_else(|| ApiError::not_found("media", asset_id.to_owned()))?;
    let asset = if asset.availability == "purged" {
        match asset.checksum.as_deref() {
            Some(checksum) if is_sha256_digest(checksum) => asset,
            _ => {
                return Err(ApiError::internal(
                    "purged media checksum is unavailable or invalid",
                ));
            }
        }
    } else {
        reconcile_asset_checksum(state, asset).await?
    };
    let storage_key = asset.storage_key.clone();
    let tombstoned = SharedMediaRepo::tombstone_project_media_asset(&*state.db, tombstone).await?;
    if target_availability == "purged" {
        let path = media_storage_path(state, &storage_key)?;
        remove_file_if_exists(&path).await?;
    }
    Ok(Json(media_asset_response_from_model(
        tombstoned,
        Vec::new(),
        Some(project_media_url(project_id, asset_id)),
    )?))
}

pub async fn attach_evidence(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
    Json(request): Json<AttachEvidenceRequest>,
) -> ApiResult<Json<EvidenceAttachment>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    if request.milestone_id != milestone_id {
        return Err(ApiError::bad_request(
            "milestone_id in the path and request must match",
        ));
    }
    validate_authorization(
        &request.mutation.authorization,
        &user.user_id,
        EVIDENCE_ATTACH_ACTION,
    )?;
    if request.mutation.idempotency_key.trim().is_empty() {
        return Err(ApiError::bad_request("idempotency_key is required"));
    }
    if !is_sha256_digest(&request.checksum) {
        return Err(ApiError::conflict_with_code(
            "media.digest_mismatch",
            "evidence checksum does not match the asset",
        ));
    }
    let authorization_json = serde_json::to_string(&request.mutation.authorization)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let attachment = ProjectArtifactCommandService::new(state.db.clone())
        .attach_evidence(
            ProjectEvidenceCommand {
                project_id: project_id.clone(),
                milestone_id,
                asset_id: request.asset_id,
                task_id: request.task_id,
                source_run_id: request.source_run_id,
                source_validation_id: request.source_validation_id,
                acceptance_check_ids: request.acceptance_check_ids,
                caption: request.caption,
                evidence_kind: evidence_kind_name(request.kind).to_owned(),
                checksum: request.checksum,
                expected_milestone_version: request.mutation.expected_version,
                idempotency_key: request.mutation.idempotency_key,
                authorization: ProjectCommandAuthorization {
                    principal_type: "user".to_owned(),
                    principal_id: user.user_id.clone(),
                    policy_result: "allowed".to_owned(),
                    policy_revision: None,
                    policy_digest: None,
                    requested_permission: Some(EVIDENCE_ATTACH_ACTION.to_owned()),
                    correlation_id: request.mutation.authorization.event_id.clone(),
                    causation_id: None,
                    causation_depth: 0,
                    authorization_event_id: request.mutation.authorization.event_id,
                    authorization_basis: request.mutation.authorization.authorization_basis,
                    authorization_action: request.mutation.authorization.action,
                    authorization_occurred_at: request.mutation.authorization.occurred_at,
                    authorization_json,
                },
            },
            None,
        )
        .await?;
    Ok(Json(evidence_attachment_response(attachment)?))
}

pub async fn list_evidence(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id)): Path<(String, String)>,
    Query(query): Query<EvidenceListQuery>,
) -> ApiResult<Json<EvidenceAttachmentListResponse>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let (cursor_created_at, cursor_id) = decode_cursor(query.cursor.as_deref())?
        .map_or((None, None), |(created_at, id)| {
            (Some(created_at), Some(id))
        });
    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let mut statement = sqlx::query(
        "SELECT * FROM project_media_attachment
         WHERE project_id = ? AND milestone_id = ?
           AND attachment_kind = 'evidence' AND deleted_at IS NULL
         ORDER BY created_at ASC, id ASC LIMIT ?",
    );
    if let (Some(created_at), Some(id)) = (cursor_created_at, cursor_id) {
        statement = sqlx::query(
            "SELECT * FROM project_media_attachment
             WHERE project_id = ? AND milestone_id = ?
               AND attachment_kind = 'evidence' AND deleted_at IS NULL
               AND (created_at > ? OR (created_at = ? AND id > ?))
             ORDER BY created_at ASC, id ASC LIMIT ?",
        )
        .bind(&project_id)
        .bind(&milestone_id)
        .bind(created_at.clone())
        .bind(created_at)
        .bind(id)
        .bind(limit + 1);
    } else {
        statement = statement
            .bind(&project_id)
            .bind(&milestone_id)
            .bind(limit + 1);
    }
    let mut rows = statement.fetch_all(state.db.pool()).await?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(evidence_attachment_response(map_attachment_row(&row)?)?);
    }
    let next_cursor = has_more
        .then(|| {
            items
                .last()
                .map(|item| encode_cursor(&item.created_at, &item.id))
        })
        .flatten();
    Ok(Json(EvidenceAttachmentListResponse {
        items,
        next_cursor,
        has_more,
    }))
}

pub async fn get_evidence(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id, evidence_id)): Path<(String, String, String)>,
) -> ApiResult<Json<EvidenceAttachment>> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    let row = sqlx::query(
        "SELECT * FROM project_media_attachment
         WHERE id = ? AND project_id = ? AND milestone_id = ?
           AND attachment_kind = 'evidence' AND deleted_at IS NULL",
    )
    .bind(&evidence_id)
    .bind(&project_id)
    .bind(&milestone_id)
    .fetch_optional(state.db.pool())
    .await?
    .ok_or_else(|| ApiError::not_found("evidence", evidence_id))?;
    Ok(Json(evidence_attachment_response(map_attachment_row(
        &row,
    )?)?))
}

pub async fn remove_evidence(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, milestone_id, evidence_id)): Path<(String, String, String)>,
    Json(request): Json<api_types::MutationEnvelope>,
) -> ApiResult<StatusCode> {
    require_project_access(&state, &project_id, &user.user_id).await?;
    validate_authorization(
        &request.authorization,
        &user.user_id,
        EVIDENCE_REMOVE_ACTION,
    )?;
    if request.idempotency_key.trim().is_empty() {
        return Err(ApiError::bad_request("idempotency_key is required"));
    }
    SharedMediaRepo::soft_delete_project_media_attachment_mutation(
        &*state.db,
        SoftDeleteProjectMediaAttachmentMutation {
            id: evidence_id.clone(),
            project_id,
            milestone_id,
            expected_version: request.expected_version,
            idempotency_key: request.idempotency_key.clone(),
            mutation_fingerprint: mutation_digest(&request),
            actor_type: "user".to_owned(),
            actor_id: Some(user.user_id),
            authorization_json: serde_json::to_string(&request.authorization)
                .map_err(|error| ApiError::bad_request(error.to_string()))?,
            authorization_event_id: request.authorization.event_id,
            deleted_at: now_rfc3339(),
        },
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

fn media_asset_response_from_model(
    asset: db::MediaAsset,
    task_media_ids: Vec<String>,
    stable_project_url: Option<String>,
) -> ApiResult<MediaAsset> {
    if !ALLOWED_CONTENT_TYPES.contains(&asset.content_type.as_str()) {
        return Err(ApiError::internal(
            "persisted media content_type is not supported",
        ));
    }
    if !persisted_filename_is_safe(&asset.display_filename) {
        return Err(ApiError::internal(
            "persisted media display filename is invalid",
        ));
    }
    let byte_size = u64::try_from(asset.byte_size)
        .map_err(|_| ApiError::internal("persisted media byte_size is invalid"))?;
    let checksum = asset
        .checksum
        .filter(|checksum| is_sha256_digest(checksum))
        .ok_or_else(|| ApiError::internal("persisted media checksum is unavailable"))?;
    Ok(MediaAsset {
        id: asset.id,
        project_id: asset.project_id,
        original_filename: asset.display_filename,
        content_type: asset.content_type,
        byte_size,
        checksum,
        availability: evidence_availability(&asset.availability)?,
        task_media_ids,
        stable_project_url,
        created_at: asset.created_at,
        deleted_at: asset.deleted_at,
    })
}

fn map_attachment_row(row: &sqlx::sqlite::SqliteRow) -> ApiResult<db::ProjectMediaAttachment> {
    Ok(db::ProjectMediaAttachment {
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

fn evidence_attachment_response(row: db::ProjectMediaAttachment) -> ApiResult<EvidenceAttachment> {
    if row.attachment_kind != "evidence" {
        return Err(ApiError::internal(
            "persisted Project media attachment kind is invalid",
        ));
    }
    let acceptance_check_ids =
        serde_json::from_str(&row.acceptance_check_ids_json).map_err(|error| {
            ApiError::internal(format!("persisted evidence JSON is invalid: {error}"))
        })?;
    let caption = row
        .caption
        .ok_or_else(|| ApiError::internal("persisted evidence caption is missing"))?;
    let checksum = row
        .checksum
        .filter(|checksum| is_sha256_digest(checksum))
        .ok_or_else(|| ApiError::internal("persisted evidence checksum is unavailable"))?;
    let author_type = match row.author_type.as_str() {
        "user" => api_types::PrincipalKind::User,
        "agent" => api_types::PrincipalKind::Agent,
        "system" => api_types::PrincipalKind::System,
        _ => {
            return Err(ApiError::internal(
                "persisted evidence author type is unknown",
            ));
        }
    };
    let author_id = row
        .author_id
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| ApiError::internal("persisted evidence author id is missing"))?;
    Ok(EvidenceAttachment {
        id: row.id,
        project_id: row.project_id,
        asset_id: row.asset_id,
        task_id: row.task_id,
        source_task_id: row.source_task_id,
        source_run_id: row.source_execution_id,
        source_validation_id: row.source_validation_id,
        milestone_id: row.milestone_id,
        acceptance_check_ids,
        caption,
        kind: evidence_kind(row.evidence_kind.as_deref())?,
        checksum,
        availability: evidence_availability(&row.availability)?,
        author: api_types::PrincipalRef {
            kind: author_type,
            id: author_id,
            display_name: None,
        },
        captured_at: row.created_at.clone(),
        version: row.version,
        created_at: row.created_at,
        removed_at: row.deleted_at,
    })
}

async fn require_project_access(
    state: &AppState,
    project_id: &str,
    user_id: &str,
) -> ApiResult<()> {
    let project = ProjectRepo::get_by_id(&*state.db, project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    let membership_exists = match project.owner_id.as_deref() {
        Some(owner_id) if owner_id != user_id => {
            ProjectMemberRepo::get_member(&*state.db, project_id, user_id)
                .await?
                .is_some()
        }
        _ => false,
    };
    if !project_access_allowed(project.owner_id.as_deref(), user_id, membership_exists) {
        return Err(ApiError::not_found("project", project_id.to_owned()));
    }
    Ok(())
}

fn project_access_allowed(owner_id: Option<&str>, user_id: &str, membership_exists: bool) -> bool {
    owner_id.is_none() || owner_id == Some(user_id) || membership_exists
}

async fn require_project_media_admin(
    state: &AppState,
    project_id: &str,
    user_id: &str,
) -> ApiResult<()> {
    let project = ProjectRepo::get_by_id(&*state.db, project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    if project.owner_id.as_deref() == Some(user_id) {
        return Ok(());
    }
    let member = ProjectMemberRepo::get_member(&*state.db, project_id, user_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    if member.role == "owner" || member.role == "admin" {
        return Ok(());
    }
    Err(ApiError::forbidden_with_code(
        "insufficient_role",
        "project owner or admin role is required for media disposition",
    ))
}

fn validate_authorization(
    authorization: &api_types::AuthorizationProvenance,
    user_id: &str,
    action: &str,
) -> ApiResult<()> {
    if authorization.principal.kind != api_types::PrincipalKind::User
        || authorization.principal.id != user_id
        || authorization.action != action
        || authorization.authorization_basis.trim().is_empty()
        || authorization.event_id.trim().is_empty()
        || authorization.occurred_at.trim().is_empty()
    {
        return Err(ApiError::forbidden_with_code(
            "authorization.invalid",
            "explicit user authorization is required",
        ));
    }
    Ok(())
}

fn mutation_digest<T: Serialize>(value: &T) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(value).expect("public mutation types serialize"),
    ))
}

fn project_media_url(project_id: &str, asset_id: &str) -> String {
    format!("/api/v1/projects/{project_id}/media/{asset_id}")
}

fn is_sha256_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn normalize_content_type(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

async fn reconcile_asset_checksum(
    state: &AppState,
    asset: db::MediaAsset,
) -> ApiResult<db::MediaAsset> {
    if !ALLOWED_CONTENT_TYPES.contains(&asset.content_type.as_str())
        || !persisted_filename_is_safe(&asset.display_filename)
    {
        return Err(ApiError::internal(
            "persisted media metadata is invalid or unsupported",
        ));
    }
    let path = media_storage_path(state, &asset.storage_key)?;
    let bytes = tokio::fs::read(&path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ApiError::internal("media checksum cannot be reconciled because bytes are missing")
        } else {
            ApiError::from(error)
        }
    })?;
    let byte_size = i64::try_from(bytes.len())
        .map_err(|_| ApiError::internal("media byte size exceeds the supported range"))?;
    if asset.byte_size != byte_size {
        return Err(ApiError::internal(
            "media bytes do not match the persisted byte size",
        ));
    }
    if !content_signature_matches(&asset.content_type, &bytes) {
        return Err(ApiError::internal(
            "media bytes do not match the persisted content type",
        ));
    }
    let checksum = hex::encode(Sha256::digest(&bytes));
    if let Some(existing) = asset.checksum.as_deref() {
        if !is_sha256_digest(existing) || existing != checksum {
            return Err(ApiError::internal(
                "persisted media checksum does not match unchanged bytes",
            ));
        }
        return Ok(asset);
    }
    let reconciled = SharedMediaRepo::set_media_asset_checksum(
        &*state.db,
        &asset.id,
        asset.byte_size,
        &checksum,
        &now_rfc3339(),
    )
    .await?;
    if reconciled.checksum.as_deref() != Some(checksum.as_str()) {
        return Err(ApiError::internal(
            "media checksum reconciliation did not persist the digest",
        ));
    }
    Ok(reconciled)
}

async fn promote_staged_file(
    staging_path: &StdPath,
    final_path: &StdPath,
    expected_byte_size: i64,
    expected_checksum: Option<&str>,
) -> ApiResult<()> {
    let final_matches = file_matches(final_path, expected_byte_size, expected_checksum).await?;
    if final_matches {
        remove_file_if_exists(staging_path).await?;
        return Ok(());
    }
    if tokio::fs::try_exists(final_path).await.unwrap_or(false) {
        remove_file_if_exists(final_path).await?;
    }
    let parent = final_path
        .parent()
        .ok_or_else(|| ApiError::internal("media final path has no parent directory"))?;
    tokio::fs::create_dir_all(parent).await?;
    tokio::fs::rename(staging_path, final_path).await?;
    if !file_matches(final_path, expected_byte_size, expected_checksum).await? {
        return Err(ApiError::internal(
            "promoted media bytes do not match persisted metadata",
        ));
    }
    Ok(())
}

async fn file_matches(
    path: &StdPath,
    expected_byte_size: i64,
    expected_checksum: Option<&str>,
) -> ApiResult<bool> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let actual_size = i64::try_from(bytes.len())
        .map_err(|_| ApiError::internal("media byte size exceeds the supported range"))?;
    if actual_size != expected_byte_size {
        return Ok(false);
    }
    let Some(expected_checksum) = expected_checksum else {
        return Ok(false);
    };
    Ok(is_sha256_digest(expected_checksum)
        && hex::encode(Sha256::digest(&bytes)) == expected_checksum)
}

async fn remove_file_if_exists(path: &StdPath) -> ApiResult<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn verify_media_bytes(asset: &db::MediaAsset, bytes: &[u8]) -> ApiResult<()> {
    let expected_size = i64::try_from(bytes.len())
        .map_err(|_| ApiError::internal("media byte size exceeds the supported range"))?;
    if asset.byte_size != expected_size {
        return Err(ApiError::internal(
            "media byte size does not match metadata",
        ));
    }
    let Some(expected_checksum) = asset.checksum.as_deref() else {
        return Err(ApiError::internal("media checksum is unavailable"));
    };
    if !is_sha256_digest(expected_checksum)
        || hex::encode(Sha256::digest(bytes)) != expected_checksum
    {
        return Err(ApiError::internal("media checksum does not match bytes"));
    }
    if !content_signature_matches(&asset.content_type, bytes) {
        return Err(ApiError::internal(
            "media bytes do not match the persisted content type",
        ));
    }
    Ok(())
}

fn is_safe_inline_content_type(content_type: &str) -> bool {
    matches!(
        content_type,
        "image/png"
            | "image/jpeg"
            | "image/gif"
            | "image/webp"
            | "video/mp4"
            | "video/webm"
            | "video/quicktime"
    )
}

fn media_storage_path(state: &AppState, storage_key: &str) -> ApiResult<PathBuf> {
    let path = StdPath::new(storage_key);
    let has_normal_component = path
        .components()
        .any(|component| matches!(component, Component::Normal(_)));
    if storage_key.is_empty()
        || !has_normal_component
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
    {
        return Err(ApiError::internal("invalid media storage key"));
    }
    Ok(state
        .effective_config
        .forge
        .data_dir
        .join("media")
        .join(path))
}

fn normalize_filename(filename: &str) -> ApiResult<String> {
    let safe = filename
        .chars()
        .filter(|ch| !ch.is_control() && *ch != '/' && *ch != '\\')
        .collect::<String>();
    let safe = safe.trim();
    if safe.is_empty() || safe == "." || safe == ".." || safe.len() > MAX_FILENAME_BYTES {
        return Err(ApiError::bad_request("filename is invalid"));
    }
    if StdPath::new(safe)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "exe"
                    | "dll"
                    | "so"
                    | "dylib"
                    | "bat"
                    | "cmd"
                    | "com"
                    | "msi"
                    | "sh"
                    | "command"
                    | "app"
            )
        })
    {
        return Err(ApiError::bad_request("filename extension is not permitted"));
    }
    Ok(safe.to_owned())
}

fn persisted_filename_is_safe(filename: &str) -> bool {
    !filename.is_empty()
        && filename.len() <= MAX_FILENAME_BYTES
        && filename != "."
        && filename != ".."
        && filename.trim() == filename
        && filename
            .chars()
            .all(|ch| !ch.is_control() && ch != '/' && ch != '\\')
}

/// Verify a bounded magic-byte signature in addition to the client-declared
/// MIME type. The declaration is metadata, never authority to classify
/// arbitrary bytes as safe media.
fn validate_content_signature(content_type: &str, filename: &str, bytes: &[u8]) -> ApiResult<()> {
    if !content_signature_matches(content_type, bytes) {
        return Err(ApiError::bad_request(format!(
            "file bytes do not match declared content_type {content_type}"
        )));
    }
    if let Some(extension) = StdPath::new(filename)
        .extension()
        .and_then(|value| value.to_str())
    {
        let extension = extension.to_ascii_lowercase();
        let expected = match content_type {
            "image/png" => &["png"][..],
            "image/jpeg" => &["jpg", "jpeg"][..],
            "image/gif" => &["gif"][..],
            "image/webp" => &["webp"][..],
            "video/mp4" => &["mp4"][..],
            "video/webm" => &["webm"][..],
            "video/quicktime" => &["mov", "qt"][..],
            "application/pdf" => &["pdf"][..],
            "application/zip" => &["zip"][..],
            "text/plain" => &["txt", "text", "log", "md", "csv"][..],
            _ => &[][..],
        };
        if !expected.is_empty() && !expected.contains(&extension.as_str()) {
            return Err(ApiError::bad_request(
                "filename extension does not match declared content_type",
            ));
        }
    }
    Ok(())
}

fn content_signature_matches(content_type: &str, bytes: &[u8]) -> bool {
    match content_type {
        "image/png" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        "image/jpeg" => bytes.starts_with(b"\xff\xd8\xff"),
        "image/gif" => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
        "image/webp" => bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP",
        "video/mp4" | "video/quicktime" => bytes.len() >= 12 && &bytes[4..8] == b"ftyp",
        "video/webm" => bytes.starts_with(b"\x1a\x45\xdf\xa3"),
        "application/pdf" => bytes.starts_with(b"%PDF-"),
        "application/zip" => {
            bytes.starts_with(b"PK\x03\x04")
                || bytes.starts_with(b"PK\x05\x06")
                || bytes.starts_with(b"PK\x07\x08")
        }
        "text/plain" => !bytes.contains(&0) && std::str::from_utf8(bytes).is_ok(),
        _ => false,
    }
}

async fn read_file_field(
    field: &mut axum::extract::multipart::Field<'_>,
    limit: u64,
) -> ApiResult<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|error| ApiError::bad_request(format!("invalid file field: {error}")))?
    {
        if (bytes.len() as u64).saturating_add(chunk.len() as u64) > limit {
            return Err(ApiError::bad_request("file exceeds upload limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn read_text_field(
    field: &mut axum::extract::multipart::Field<'_>,
    name: &str,
    limit: usize,
) -> ApiResult<String> {
    let mut bytes = Vec::new();
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|error| ApiError::bad_request(format!("invalid {name} field: {error}")))?
    {
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(ApiError::bad_request(format!("{name} is too large")));
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).map_err(|error| ApiError::bad_request(error.to_string()))
}

fn encode_cursor(timestamp: &str, id: &str) -> String {
    hex::encode(format!("{timestamp}\0{id}"))
}

fn decode_cursor(value: Option<&str>) -> ApiResult<Option<(String, String)>> {
    let Some(value) = value else { return Ok(None) };
    let bytes = hex::decode(value).map_err(|_| ApiError::bad_request("invalid cursor"))?;
    let decoded = String::from_utf8(bytes).map_err(|_| ApiError::bad_request("invalid cursor"))?;
    let (timestamp, id) = decoded
        .split_once('\0')
        .ok_or_else(|| ApiError::bad_request("invalid cursor"))?;
    if timestamp.is_empty() || id.is_empty() {
        return Err(ApiError::bad_request("invalid cursor"));
    }
    Ok(Some((timestamp.to_owned(), id.to_owned())))
}

fn evidence_kind_name(kind: EvidenceKind) -> &'static str {
    match kind {
        EvidenceKind::Screenshot => "screenshot",
        EvidenceKind::WalkthroughVideo => "walkthrough_video",
        EvidenceKind::Log => "log",
        EvidenceKind::Report => "report",
        EvidenceKind::Other => "other",
    }
}

fn evidence_kind(value: Option<&str>) -> ApiResult<EvidenceKind> {
    match value {
        Some("screenshot") => Ok(EvidenceKind::Screenshot),
        Some("walkthrough_video") => Ok(EvidenceKind::WalkthroughVideo),
        Some("log") => Ok(EvidenceKind::Log),
        Some("report") => Ok(EvidenceKind::Report),
        Some("other") => Ok(EvidenceKind::Other),
        _ => Err(ApiError::internal("persisted evidence kind is unknown")),
    }
}

fn evidence_availability(value: &str) -> ApiResult<EvidenceAvailability> {
    match value {
        "available" => Ok(EvidenceAvailability::Available),
        "quarantined" => Ok(EvidenceAvailability::Quarantined),
        "redacted" => Ok(EvidenceAvailability::Redacted),
        "purged" => Ok(EvidenceAvailability::Purged),
        _ => Err(ApiError::internal(
            "persisted media availability is unknown",
        )),
    }
}

fn header_safe_filename(filename: &str) -> String {
    let safe = filename
        .chars()
        .map(|ch| match ch {
            '\'' | '"' | '\\' | '/' | ';' => '_',
            ch if ch.is_ascii_graphic() || ch == ' ' => ch,
            _ => '_',
        })
        .collect::<String>();
    if safe.trim().is_empty() {
        "download".to_owned()
    } else {
        safe
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_misleading_extension_and_signature() {
        assert!(validate_content_signature("image/png", "proof.png", b"\x89PNG\r\n\x1a\n").is_ok());
        assert!(validate_content_signature("image/png", "proof.png", b"%PDF-1.7").is_err());
        assert!(validate_content_signature("application/pdf", "proof.png", b"%PDF-1.7").is_err());
    }

    #[test]
    fn rejects_executable_and_invalid_filename_extensions() {
        assert!(normalize_filename("payload.exe").is_err());
        assert!(normalize_filename("payload.sh").is_err());
        assert!(normalize_filename(" proof.png ").is_ok());
    }

    #[test]
    fn cursor_is_opaque_and_validated() {
        let cursor = encode_cursor("2026-08-13T00:00:00Z", "asset-1");
        assert_eq!(
            decode_cursor(Some(&cursor)).unwrap(),
            Some(("2026-08-13T00:00:00Z".to_owned(), "asset-1".to_owned()))
        );
        assert!(decode_cursor(Some("not-a-cursor")).is_err());
    }

    #[test]
    fn persisted_media_enums_fail_closed() {
        assert!(evidence_kind(Some("unknown")).is_err());
        assert!(evidence_kind(None).is_err());
        assert!(evidence_availability("unknown").is_err());
        assert!(evidence_availability("available").is_ok());
    }

    #[test]
    fn response_filename_cannot_inject_headers() {
        let filename = header_safe_filename("../proof\";\r\nX-Evil: yes.png");
        assert!(!filename.contains('"'));
        assert!(!filename.contains('\r'));
        assert!(!filename.contains('\n'));
        assert!(filename.starts_with(".._proof_"));
        assert!(!filename.contains('/'));
    }

    #[test]
    fn project_owner_access_does_not_require_membership_row() {
        assert!(project_access_allowed(Some("owner"), "owner", false));
        assert!(!project_access_allowed(Some("owner"), "other", false));
        assert!(project_access_allowed(Some("owner"), "member", true));
        assert!(project_access_allowed(None, "any-user", false));
    }
}
