use std::path::{Component, Path as StdPath, PathBuf};

use axum::{
    extract::Multipart,
    http::{
        header::{CONTENT_DISPOSITION, CONTENT_TYPE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};

use super::*;
use crate::routes::auth::AuthenticatedUser;

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

const BLOCKED_EXTENSIONS: &[&str] = &["exe", "bat", "sh", "command", "app"];
const AUTHOR_NAME_MAX_BYTES: u64 = 256;

struct PendingUpload {
    filename: String,
    content_type: String,
    bytes: Vec<u8>,
}

pub async fn upload_media(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    mut multipart: Multipart,
) -> ApiResult<(StatusCode, Json<TaskMediaResponse>)> {
    require_task_media_access(&state, &id, &user).await?;

    let mut author_name = "User".to_owned();
    let mut upload = None;
    let upload_limit = state.effective_config.server.media_upload_limit_bytes;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::bad_request(format!("invalid multipart body: {error}")))?
    {
        if field.name() == Some("author_name") {
            let value = read_text_field(&mut field, "author_name", AUTHOR_NAME_MAX_BYTES).await?;
            let value = value.trim();
            if !value.is_empty() {
                author_name = value.to_owned();
            }
            continue;
        }

        let Some(raw_filename) = field.file_name().map(str::to_owned) else {
            continue;
        };
        if upload.is_some() {
            return Err(ApiError::bad_request("only one file may be uploaded"));
        }

        let filename = normalize_filename(&raw_filename)?;
        reject_blocked_extension(&filename)?;
        let content_type = field
            .content_type()
            .map(str::to_owned)
            .ok_or_else(|| ApiError::bad_request("content_type is required"))?;
        validate_content_type(&content_type)?;

        let bytes = read_field_bytes(&mut field, upload_limit).await?;
        upload = Some(PendingUpload {
            filename,
            content_type,
            bytes,
        });
    }

    let upload = upload.ok_or_else(|| ApiError::bad_request("file is required"))?;
    let media_id = db::new_uuid_v4();
    let storage_key = format!("{id}/{media_id}__{}", upload.filename);
    let path = media_storage_path(&state, &storage_key)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, &upload.bytes)?;

    let now = now_rfc3339();
    let created = match TaskMediaRepo::create_media(
        &*state.db,
        db::CreateTaskMedia {
            id: media_id,
            task_id: id.clone(),
            display_filename: upload.filename,
            content_type: upload.content_type,
            byte_size: i64::try_from(upload.bytes.len())
                .map_err(|_| ApiError::bad_request("file is too large"))?,
            storage_key,
            author_type: CommentAuthorType::User,
            author_id: Some(user.user_id),
            author_name,
            created_at: now,
        },
    )
    .await
    {
        Ok(media) => media,
        Err(error) => {
            remove_file_if_exists(&path)?;
            return Err(error.into());
        }
    };

    // The V076 promotion trigger mints this upload's Project media asset with a
    // NULL checksum, and milestone evidence attachment compares the caller's
    // checksum against that column. A NULL never matches, so every Task
    // attachment was permanently ineligible as milestone evidence — on the very
    // path the Project Overview points at for evidence capture. Forge is holding
    // the bytes here, so derive the digest rather than trust an asserted one.
    let checksum = hex::encode(Sha256::digest(&upload.bytes));
    if let Err(error) = db::SharedMediaRepo::set_media_asset_checksum(
        &*state.db,
        &created.id,
        created.byte_size,
        &checksum,
        &now_rfc3339(),
    )
    .await
    {
        remove_file_if_exists(&path)?;
        return Err(error.into());
    }

    publish_media_uploaded(&state, &created);
    Ok((StatusCode::CREATED, Json(media_response(created))))
}

pub async fn list_media(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<PaginatedResponse<TaskMediaResponse>>> {
    require_task_media_access(&state, &id, &user).await?;

    let media = TaskMediaRepo::list_media(
        &*state.db,
        &id,
        PageRequest {
            cursor: params.cursor,
            limit: params.limit.unwrap_or(50).clamp(1, 100),
            include_total: params.include_total.unwrap_or(false),
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await?;
    Ok(Json(paginated(media, media_response)))
}

pub async fn get_media(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(media_id): Path<String>,
) -> ApiResult<Response> {
    let media = TaskMediaRepo::get_media_by_id(&*state.db, &media_id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("media", media_id.clone()))?;
    require_task_media_access(&state, &media.task_id, &user).await?;
    let path = media_storage_path(&state, &media.storage_key)?;
    let bytes = std::fs::read(&path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ApiError::not_found("media", media.id.clone())
        } else {
            ApiError::from(error)
        }
    })?;

    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&media.content_type)
            .map_err(|error| ApiError::internal(format!("invalid content type: {error}")))?,
    );
    if !is_inline_content_type(&media.content_type) {
        let filename = header_safe_filename(&media.display_filename);
        let disposition = format!("attachment; filename='{filename}'");
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_str(&disposition).map_err(|error| {
                ApiError::internal(format!("invalid content disposition: {error}"))
            })?,
        );
    }

    Ok((headers, bytes).into_response())
}

pub async fn delete_media(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(media_id): Path<String>,
) -> ApiResult<StatusCode> {
    let media = TaskMediaRepo::get_media_by_id(&*state.db, &media_id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("media", media_id.clone()))?;
    require_task_media_delete_access(&state, &media.task_id, &user).await?;
    let asset = SharedMediaRepo::get_media_asset_for_task_media(&*state.db, &media.id).await?;
    let deleted_at = now_rfc3339();
    let deleted = TaskMediaRepo::soft_delete_media(&*state.db, &media_id, &deleted_at).await?;
    if let Some(asset) = asset {
        maybe_collect_media_asset(&state, &asset.id, &asset.storage_key, &deleted_at).await?;
    } else {
        // Databases created before the additive media metadata migration have
        // no asset row yet.  Keep the historical behavior for that narrow
        // fallback; normal migrated databases always take the guarded path.
        remove_media_file(&state, &media.storage_key)?;
    }
    publish_media_deleted(&state, &deleted);
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn delete_task_media_for_task(state: &AppState, task_id: &str) -> ApiResult<()> {
    let media = TaskMediaRepo::list_active_media_for_task(&*state.db, task_id).await?;
    for item in media {
        let asset = SharedMediaRepo::get_media_asset_for_task_media(&*state.db, &item.id).await?;
        let deleted_at = now_rfc3339();
        let deleted =
            match TaskMediaRepo::soft_delete_media(&*state.db, &item.id, &deleted_at).await {
                Ok(deleted) => deleted,
                Err(db::DbError::NotFound) => continue,
                Err(error) => return Err(error.into()),
            };
        if let Some(asset) = asset {
            maybe_collect_media_asset(state, &asset.id, &asset.storage_key, &deleted_at).await?;
        } else {
            remove_media_file(state, &item.storage_key)?;
        }
        publish_media_deleted(state, &deleted);
    }
    Ok(())
}

/// Claim and remove one unreferenced shared asset.  The claim is a guarded
/// database transaction: active Project attachments, active Task references,
/// and non-purged release pins are rechecked before the asset enters
/// `gc_queued`.  Attachment/pin writers reject queued assets, so a cleanup
/// worker can safely remove the existing bytes and then finalize the tombstone.
/// A queued row is deliberately restartable; if the process dies after file
/// removal, the next pass completes the idempotent database transition.
async fn maybe_collect_media_asset(
    state: &AppState,
    asset_id: &str,
    storage_key: &str,
    now: &str,
) -> ApiResult<()> {
    let lease_owner = format!("task-media-delete:{}", db::new_uuid_v4());
    let lease_expires_at = (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    let candidate = SharedMediaRepo::claim_media_gc_candidate(
        &*state.db,
        asset_id,
        now,
        &lease_owner,
        &lease_expires_at,
    )
    .await?;
    let Some(candidate) = candidate else {
        return Ok(());
    };
    if candidate.storage_key != storage_key {
        return Err(ApiError::internal("shared media storage metadata changed"));
    }

    let path = media_storage_path(state, &candidate.storage_key)?;
    if let Err(error) = remove_file_if_exists(&path) {
        let _ = SharedMediaRepo::reset_media_gc_candidate(
            &*state.db,
            asset_id,
            &lease_owner,
            candidate.version,
            now,
        )
        .await;
        return Err(error);
    }

    // Finalization rechecks every reference in a fresh transaction and
    // requires the exact persisted lease/version.  The schema also rejects
    // direct SQL attempts to create a live attachment or pin while queued.
    let _ = SharedMediaRepo::complete_media_gc(
        &*state.db,
        asset_id,
        &lease_owner,
        candidate.version,
        now,
    )
    .await?;
    Ok(())
}

async fn read_field_bytes(
    field: &mut axum::extract::multipart::Field<'_>,
    upload_limit: u64,
) -> ApiResult<Vec<u8>> {
    let capacity = usize::try_from(upload_limit.min(1024 * 1024)).unwrap_or(0);
    let mut bytes = Vec::with_capacity(capacity);
    let mut byte_size = 0_u64;
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|error| ApiError::bad_request(format!("invalid file field: {error}")))?
    {
        byte_size = byte_size
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| ApiError::bad_request("file is too large"))?;
        if byte_size > upload_limit {
            return Err(ApiError::bad_request(format!(
                "file exceeds upload limit of {upload_limit} bytes"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn read_text_field(
    field: &mut axum::extract::multipart::Field<'_>,
    field_name: &str,
    max_bytes: u64,
) -> ApiResult<String> {
    let capacity = usize::try_from(max_bytes).unwrap_or(0);
    let mut bytes = Vec::with_capacity(capacity);
    let mut byte_size = 0_u64;
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|error| ApiError::bad_request(format!("invalid {field_name} field: {error}")))?
    {
        byte_size = byte_size
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| ApiError::bad_request(format!("{field_name} is too large")))?;
        if byte_size > max_bytes {
            return Err(ApiError::bad_request(format!(
                "{field_name} must be at most {max_bytes} bytes"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes)
        .map_err(|error| ApiError::bad_request(format!("invalid {field_name} field: {error}")))
}

fn normalize_filename(filename: &str) -> ApiResult<String> {
    let safe = filename
        .chars()
        .filter(|ch| !ch.is_control() && *ch != '/' && *ch != '\\')
        .collect::<String>();
    let safe = safe.trim();
    if safe.is_empty() || safe == "." || safe == ".." {
        return Err(ApiError::bad_request("filename must not be empty"));
    }
    if safe.len() > 255 {
        return Err(ApiError::bad_request("filename must be at most 255 bytes"));
    }
    Ok(safe.to_owned())
}

fn validate_content_type(content_type: &str) -> ApiResult<()> {
    if ALLOWED_CONTENT_TYPES.contains(&content_type) {
        return Ok(());
    }
    Err(ApiError::bad_request(format!(
        "unsupported content_type: {content_type}"
    )))
}

fn reject_blocked_extension(filename: &str) -> ApiResult<()> {
    let extension = filename
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase());
    if extension
        .as_deref()
        .is_some_and(|extension| BLOCKED_EXTENSIONS.contains(&extension))
    {
        return Err(ApiError::bad_request(
            "filename extension is not allowed for task media",
        ));
    }
    Ok(())
}

fn is_inline_content_type(content_type: &str) -> bool {
    !is_svg_content_type(content_type)
        && (content_type.starts_with("image/") || content_type.starts_with("video/"))
}

fn is_svg_content_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .eq_ignore_ascii_case("image/svg+xml")
}

async fn require_task_media_access(
    state: &AppState,
    task_id: &str,
    user: &AuthenticatedUser,
) -> ApiResult<db::Task> {
    let task = TaskRepo::get_by_id(&*state.db, task_id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", task_id.to_owned()))?;
    require_project_media_access(state, &task.project_id, user).await?;
    Ok(task)
}

async fn require_task_media_delete_access(
    state: &AppState,
    task_id: &str,
    user: &AuthenticatedUser,
) -> ApiResult<db::Task> {
    let task = TaskRepo::get_by_id(&*state.db, task_id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", task_id.to_owned()))?;
    require_project_media_delete_access(state, &task.project_id, user).await?;
    Ok(task)
}

async fn require_project_media_access(
    state: &AppState,
    project_id: &str,
    user: &AuthenticatedUser,
) -> ApiResult<()> {
    let project = ProjectRepo::get_by_id(&*state.db, project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    if project.owner_id.as_deref() == Some(user.user_id.as_str()) || project.owner_id.is_none() {
        return Ok(());
    }
    let member = db::ProjectMemberRepo::get_member(&*state.db, project_id, &user.user_id).await?;
    if member.is_none() {
        return Err(ApiError::not_found("project", project_id.to_owned()));
    }
    Ok(())
}

async fn require_project_media_delete_access(
    state: &AppState,
    project_id: &str,
    user: &AuthenticatedUser,
) -> ApiResult<()> {
    let project = ProjectRepo::get_by_id(&*state.db, project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    if project.owner_id.as_deref() == Some(user.user_id.as_str()) || project.owner_id.is_none() {
        return Ok(());
    }
    let member = db::ProjectMemberRepo::get_member(&*state.db, project_id, &user.user_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    if member.role != "owner" && member.role != "admin" {
        return Err(ApiError::forbidden_with_code(
            "insufficient_role",
            "project owner or admin role is required",
        ));
    }
    Ok(())
}

fn media_root(state: &AppState) -> PathBuf {
    state.effective_config.forge.data_dir.join("media")
}

fn media_storage_path(state: &AppState, storage_key: &str) -> ApiResult<PathBuf> {
    let path = StdPath::new(storage_key);
    if path
        .components()
        .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
    {
        return Err(ApiError::internal("invalid media storage key"));
    }
    Ok(media_root(state).join(path))
}

fn remove_media_file(state: &AppState, storage_key: &str) -> ApiResult<()> {
    let path = media_storage_path(state, storage_key)?;
    remove_file_if_exists(&path)
}

fn remove_file_if_exists(path: &StdPath) -> ApiResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn header_safe_filename(filename: &str) -> String {
    let safe = filename
        .chars()
        .map(|ch| match ch {
            ' ' => ' ',
            '\'' | '"' | '\\' | ';' => '_',
            ch if ch.is_ascii_graphic() => ch,
            _ => '_',
        })
        .collect::<String>();
    if safe.trim().is_empty() {
        "download".to_owned()
    } else {
        safe
    }
}

fn publish_media_uploaded(state: &AppState, media: &db::TaskMedia) {
    state.event_bus.publish(events::ForgeEvent {
        event_type: "task.media.uploaded".to_owned(),
        entity_id: media.id.clone(),
        timestamp: events::event_timestamp(),
        context: events::EventContext::TaskMediaUploaded {
            task_id: media.task_id.clone(),
            media_id: media.id.clone(),
            content_type: media.content_type.clone(),
            byte_size: media.byte_size,
            filename: media.display_filename.clone(),
        },
    });
}

fn publish_media_deleted(state: &AppState, media: &db::TaskMedia) {
    state.event_bus.publish(events::ForgeEvent {
        event_type: "task.media.deleted".to_owned(),
        entity_id: media.id.clone(),
        timestamp: events::event_timestamp(),
        context: events::EventContext::TaskMediaDeleted {
            task_id: media.task_id.clone(),
            media_id: media.id.clone(),
        },
    });
}
