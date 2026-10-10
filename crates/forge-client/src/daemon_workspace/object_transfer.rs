//! Git object export / import between checkouts of one repository. The
//! algorithm is `git::integration`; this owner adds staging under its
//! workspace root, chunked framing and the queue fence. Neither operation
//! checks anything out or writes a ref outside `refs/forge/`.
use super::*;
use base64::Engine as _;
use sha2::Digest as _;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

const TRANSFER_DIRECTORY: &str = ".forge/transfer";
/// Staging older than this belongs to a transfer nobody will resume.
const STAGING_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

fn transfer_refusal(reason: ObjectTransferRefusal) -> DaemonErrorPayload {
    DaemonErrorPayload {
        code: OBJECT_TRANSFER_REFUSED.into(),
        message: format!("object transfer refused: {reason:?}"),
        details: Some(serde_json::json!({ "refusal": reason })),
    }
}

fn transfer_error(error: git::integration::ObjectTransferError) -> DaemonErrorPayload {
    use git::integration::ObjectTransferError as E;
    transfer_refusal(match error {
        E::TooLarge { bytes, max_bytes } => ObjectTransferRefusal::TooLarge { bytes, max_bytes },
        E::Invalid { reason } => ObjectTransferRefusal::Invalid { reason },
        E::MissingObject { sha } => ObjectTransferRefusal::MissingObject { sha },
        E::KeyConflict { existing_sha } => ObjectTransferRefusal::KeyConflict { existing_sha },
        E::Git(error) => return git_error(error),
    })
}

fn invalid(reason: &str) -> DaemonErrorPayload {
    transfer_refusal(ObjectTransferRefusal::Invalid {
        reason: reason.into(),
    })
}

#[derive(Serialize, Deserialize)]
struct ExportStaging {
    have: Vec<String>,
    want: String,
    receipt: ObjectExportReceipt,
}

struct RemoveFile(PathBuf);
impl Drop for RemoveFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

async fn sha256_file(path: &Path) -> CommandResult<String> {
    let mut file = tokio::fs::File::open(path).await.map_err(io_error)?;
    let mut hasher = sha2::Sha256::new();
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await.map_err(io_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Daemon start: no transfer survives a restart (the sender starts a key
/// over from its first chunk), so all staging goes, and so do the pin refs
/// and quarantine directories a killed transfer left in a checkout.
pub(super) fn sweep_at_start(workspace_root: &Path, state: &WorkspaceRegistry) {
    let staging = workspace_root.join(TRANSFER_DIRECTORY);
    if std::fs::symlink_metadata(&staging).is_ok_and(|metadata| metadata.is_dir()) {
        if let Ok(entries) = std::fs::read_dir(&staging) {
            for entry in entries.flatten() {
                if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                    let _ = std::fs::remove_dir_all(entry.path());
                } else {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }
    for location in state.locations.values() {
        let Ok(path) = location.path.canonicalize() else {
            continue;
        };
        if path.starts_with(workspace_root) && path.join(".git").exists() {
            let removed = git::integration::sweep_transfer_leftovers(&path);
            if removed > 0 {
                tracing::info!(path = %path.display(), removed, "removed leftovers of an interrupted object transfer");
            }
        }
    }
}

impl DaemonWorkspaceBackend {
    fn transfer_staging(&self) -> CommandResult<PathBuf> {
        let directory = self.workspace_root.join(TRANSFER_DIRECTORY);
        std::fs::create_dir_all(&directory).map_err(io_error)?;
        // Staging of an abandoned transfer is bounded by age.
        if let Ok(entries) = std::fs::read_dir(&directory) {
            for entry in entries.flatten() {
                let stale = entry
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .ok()
                    .and_then(|modified| modified.elapsed().ok())
                    .is_some_and(|age| age > STAGING_RETENTION);
                if stale {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        Ok(directory)
    }

    /// The key is one ref component; the fence must not be older than the
    /// queue's high-water mark, which this request also raises.
    fn admit_transfer(
        &self,
        daemon_id: &str,
        runtime_id: &str,
        fence: &IntegrationOwnerFence,
        key: &str,
        max_bytes: u64,
    ) -> CommandResult<u64> {
        self.check_owner(daemon_id, runtime_id)?;
        git::integration::transfer_ref(key).map_err(transfer_error)?;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut updated = state.clone();
        let previous = updated
            .advance_integration_fence(fence, unix_now())
            .map_err(|_| transfer_refusal(ObjectTransferRefusal::StaleFence))?;
        if previous.as_ref() != Some(fence) {
            self.journal
                .save_workspace_state(&updated)
                .map_err(storage_error)?;
            *state = updated;
        }
        Ok(max_bytes.min(MAX_OBJECT_TRANSFER_BYTES))
    }

    pub(super) async fn export_objects(
        &self,
        params: ExportObjectsParams,
    ) -> CommandResult<ExportObjectsResult> {
        let max_bytes = self.admit_transfer(
            &params.daemon_id,
            &params.runtime_id,
            &params.fence,
            &params.key,
            params.max_bytes,
        )?;
        let (_, repo) = self.location_path(&params.repo_location_id).await?;
        let lock = self.owner_lock(&format!("transfer:{}", params.key));
        let _guard = lock.lock().await;
        let staging = self.transfer_staging()?;
        let bundle = staging.join(format!("export-{}.bundle", params.key));
        let meta = staging.join(format!("export-{}.json", params.key));
        let staged = std::fs::read(&meta)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<ExportStaging>(&bytes).ok())
            .filter(|staged| {
                staged.have == params.have
                    && staged.want == params.want
                    && (staged.receipt.total_bytes == 0 || bundle.exists())
            });
        let receipt = match staged {
            Some(staged) => staged.receipt,
            None => {
                let _ = std::fs::remove_file(&meta);
                let _ = std::fs::remove_file(&bundle);
                let export = git::integration::export_objects(
                    &repo,
                    &params.key,
                    &params.have,
                    &params.want,
                    &bundle,
                    max_bytes,
                )
                .await
                .map_err(transfer_error)?;
                let partial = RemoveFile(bundle.clone());
                let sha256 = if export.total_bytes == 0 {
                    format!("{:x}", sha2::Sha256::digest([]))
                } else {
                    sha256_file(&bundle).await?
                };
                let receipt = ObjectExportReceipt {
                    key: params.key.clone(),
                    tip_sha: export.tip_sha,
                    total_bytes: export.total_bytes,
                    sha256,
                };
                let payload = serde_json::to_vec(&ExportStaging {
                    have: params.have.clone(),
                    want: params.want.clone(),
                    receipt: receipt.clone(),
                })
                .map_err(|error| error_message(&error.to_string()))?;
                std::fs::write(&meta, payload).map_err(io_error)?;
                std::mem::forget(partial);
                receipt
            }
        };
        if params.offset > receipt.total_bytes {
            return Err(error(INVALID_INPUT, "offset is past the end of the bundle"));
        }
        let length = (receipt.total_bytes - params.offset).min(MAX_OBJECT_TRANSFER_CHUNK_BYTES);
        let mut data = vec![0; length as usize];
        if length > 0 {
            let mut file = tokio::fs::File::open(&bundle).await.map_err(io_error)?;
            file.seek(std::io::SeekFrom::Start(params.offset))
                .await
                .map_err(io_error)?;
            file.read_exact(&mut data).await.map_err(io_error)?;
        }
        Ok(ExportObjectsResult {
            eof: params.offset + length >= receipt.total_bytes,
            receipt,
            offset: params.offset,
            data: base64::engine::general_purpose::STANDARD.encode(data),
        })
    }

    pub(super) async fn import_objects(
        &self,
        params: ImportObjectsParams,
    ) -> CommandResult<ImportObjectsResult> {
        let max_bytes = self.admit_transfer(
            &params.daemon_id,
            &params.runtime_id,
            &params.fence,
            &params.key,
            params.max_bytes,
        )?;
        let (_, repo) = self.location_path(&params.repo_location_id).await?;
        let transfer = self.owner_lock(&format!("transfer:{}", params.key));
        let _transfer = transfer.lock().await;
        let staging = self.transfer_staging()?;
        let part = staging.join(format!("import-{}.part", params.key));
        let imported = |receipt: git::integration::ObjectImport| ImportObjectsResult::Imported {
            receipt: ObjectImportReceipt {
                key: params.key.clone(),
                tip_sha: receipt.tip_sha,
                ref_name: receipt.ref_name,
                replayed: receipt.replayed,
            },
        };
        if let Some(receipt) =
            git::integration::imported_objects(&repo, &params.key, &params.expected_tip_sha)
                .await
                .map_err(transfer_error)?
        {
            let _ = std::fs::remove_file(&part);
            return Ok(imported(receipt));
        }
        let Some(chunk) = &params.chunk else {
            return Ok(ImportObjectsResult::Absent);
        };
        // Refuse before a single byte is stored on this owner.
        if chunk.total_bytes > max_bytes {
            let _ = std::fs::remove_file(&part);
            return Err(transfer_refusal(ObjectTransferRefusal::TooLarge {
                bytes: chunk.total_bytes,
                max_bytes,
            }));
        }
        let data = base64::engine::general_purpose::STANDARD
            .decode(&chunk.data)
            .map_err(|_| error(INVALID_INPUT, "chunk is not base64"))?;
        if data.len() as u64 > MAX_OBJECT_TRANSFER_CHUNK_BYTES {
            return Err(error(INVALID_INPUT, "chunk exceeds the frame limit"));
        }
        let mut stored = std::fs::metadata(&part).map(|m| m.len()).unwrap_or(0);
        if chunk.offset == 0 && stored > 0 {
            // The sender started over.
            std::fs::remove_file(&part).map_err(io_error)?;
            stored = 0;
        }
        // The declared size was checked against the cap above, so nothing
        // past it (and so nothing past 256 MiB) is ever written.
        let Some(end) = chunk
            .offset
            .checked_add(data.len() as u64)
            .filter(|end| *end <= chunk.total_bytes)
        else {
            let _ = std::fs::remove_file(&part);
            return Err(invalid("chunk runs past the declared transfer size"));
        };
        if chunk.offset > stored {
            let _ = std::fs::remove_file(&part);
            return Err(invalid("chunk does not continue the stored transfer"));
        }
        if end > stored {
            if chunk.offset != stored {
                let _ = std::fs::remove_file(&part);
                return Err(invalid("chunk overlaps the stored transfer"));
            }
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&part)
                .await
                .map_err(io_error)?;
            file.write_all(&data).await.map_err(io_error)?;
            file.sync_all().await.map_err(io_error)?;
            stored = end;
        }
        if !chunk.last {
            return Ok(ImportObjectsResult::Receiving {
                received_bytes: stored,
            });
        }
        // From here the staged file is consumed whatever the outcome.
        let _staged = RemoveFile(part.clone());
        if stored != chunk.total_bytes {
            return Err(invalid("transfer is shorter than its declared size"));
        }
        if chunk.total_bytes > 0 && sha256_file(&part).await? != chunk.sha256 {
            return Err(invalid("transfer does not match its declared digest"));
        }
        // Publishing objects and the transfer ref is serialized with every
        // other effect on the checkout.
        let checkout = self.owner_lock(&params.repo_location_id);
        let _checkout = checkout.lock().await;
        let receipt = git::integration::import_objects(
            &repo,
            &params.key,
            (chunk.total_bytes > 0).then_some(part.as_path()),
            &params.expected_tip_sha,
            max_bytes,
        )
        .await
        .map_err(transfer_error)?;
        Ok(imported(receipt))
    }

    pub(super) async fn release_objects(
        &self,
        params: ReleaseObjectsParams,
    ) -> CommandResult<ReleaseObjectsResult> {
        self.check_owner(&params.daemon_id, &params.runtime_id)?;
        git::integration::transfer_ref(&params.key).map_err(transfer_error)?;
        let lock = self.owner_lock(&format!("transfer:{}", params.key));
        let _guard = lock.lock().await;
        let staging = self.workspace_root.join(TRANSFER_DIRECTORY);
        let mut removed = false;
        for name in [
            format!("export-{}.bundle", params.key),
            format!("export-{}.json", params.key),
            format!("import-{}.part", params.key),
        ] {
            removed |= std::fs::remove_file(staging.join(name)).is_ok();
        }
        let mut removed_refs = 0;
        if let Some(attempt) = &params.attempt {
            // The attempt left the queue slot: nothing of it is resumed, so
            // every ref it imported here goes, and any staging of its keys.
            git::integration::transfer_ref(&attempt.attempt_id).map_err(transfer_error)?;
            let prefix = format!("{}-", attempt.attempt_id);
            if let Ok(entries) = std::fs::read_dir(&staging) {
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    let key = name
                        .strip_prefix("export-")
                        .or_else(|| name.strip_prefix("import-"));
                    if key.is_some_and(|key| key.starts_with(&prefix)) {
                        removed |= std::fs::remove_file(entry.path()).is_ok();
                    }
                }
            }
            let (_, repo) = self.location_path(&attempt.repo_location_id).await?;
            removed_refs = git::integration::release_attempt_refs(&repo, &attempt.attempt_id)
                .await
                .map_err(transfer_error)? as u32;
        }
        Ok(ReleaseObjectsResult {
            key: params.key,
            removed,
            removed_refs,
        })
    }
}

fn error_message(message: &str) -> DaemonErrorPayload {
    error(WORKSPACE_ERROR, message)
}
