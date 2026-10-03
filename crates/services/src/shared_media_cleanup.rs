//! Restartable cleanup for the additive shared-media metadata layer.
//!
//! The database owns reference and pin truth.  This scheduler only removes
//! bytes after `SharedMediaRepo` has atomically claimed an unreferenced asset;
//! the repository rechecks references again when the tombstone is finalized.
//! `gc_queued` is recoverable through the persisted scheduler lease: a process
//! restart waits for the lease to expire, then safely reclaims the candidate
//! without introducing a second source of truth.

use crate::{Result, ServiceError};
use db::{new_uuid_v4, now_rfc3339, CreateProjectMediaAsset, SharedMediaRepo, SqliteDb};
use sha2::{Digest, Sha256};
use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle, time::interval};
use tracing::warn;

const TICK_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_BATCH_SIZE: i64 = 32;

pub struct SharedMediaCleanupScheduler {
    db: Arc<SqliteDb>,
    media_root: PathBuf,
    batch_size: i64,
    lease_owner: String,
    lease_seconds: i64,
}

impl SharedMediaCleanupScheduler {
    pub fn new(db: Arc<SqliteDb>, media_root: PathBuf) -> Self {
        Self {
            db,
            media_root,
            batch_size: DEFAULT_BATCH_SIZE,
            lease_owner: format!("shared-media-cleanup:{}", new_uuid_v4()),
            lease_seconds: 300,
        }
    }

    pub fn with_lease_owner(mut self, lease_owner: impl Into<String>) -> Self {
        self.lease_owner = lease_owner.into();
        self
    }

    pub fn media_root(&self) -> &Path {
        &self.media_root
    }

    pub fn spawn(
        self: Arc<Self>,
        workers: &crate::worker_runtime::PeriodicWorkers,
        shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        workers.worker("shared-media-cleanup").start(shutdown, || false, move |worker, mut shutdown_rx| {
            let scheduler = Arc::clone(&self);
            async move {
                if let Err(error) = worker.tick(scheduler.cleanup_now()).await { warn!(worker = worker.name(), %error, "shared media startup reconciliation failed"); }
                let mut ticker = interval(TICK_INTERVAL);
                // Start the original Burst interval AFTER startup reconciliation.
                ticker.tick().await;
                loop {
                    tokio::select! {
                        _ = ticker.tick() => { if let Err(error) = worker.tick(scheduler.cleanup_now()).await { warn!(worker = worker.name(), %error, "shared media cleanup tick failed"); } }
                        result = shutdown_rx.changed() => {
                            if result.is_err() || *shutdown_rx.borrow() { return Ok(()); }
                        }
                    }
                }
            }
        })
    }

    /// Reconcile and physically remove one idempotent batch of unreferenced
    /// files.  Missing files are treated as already cleaned; queued database
    /// rows are finalized on the same or a later pass.
    pub async fn cleanup_now(&self) -> Result<usize> {
        let now = now_rfc3339();
        let mut completed = 0;
        match self.reconcile_purged_assets(&now).await {
            Ok(count) => completed += count,
            Err(error) => warn!(%error, "shared media purge reconciliation phase failed"),
        }
        match self.reconcile_pending_uploads(&now).await {
            Ok(count) => completed += count,
            Err(error) => warn!(%error, "shared media upload reconciliation phase failed"),
        }
        let lease_expires_at =
            (chrono::Utc::now() + chrono::Duration::seconds(self.lease_seconds)).to_rfc3339();
        let candidates = SharedMediaRepo::claim_media_gc_candidates(
            &*self.db,
            &now,
            &self.lease_owner,
            &lease_expires_at,
            self.batch_size,
        )
        .await?;
        for candidate in candidates {
            let path = match safe_media_path(&self.media_root, &candidate.storage_key) {
                Ok(path) => path,
                Err(error) => {
                    let _ = SharedMediaRepo::reset_media_gc_candidate(
                        &*self.db,
                        &candidate.id,
                        &self.lease_owner,
                        candidate.version,
                        &now,
                    )
                    .await;
                    warn!(asset_id = %candidate.id, %error, "invalid shared media GC candidate");
                    continue;
                }
            };

            if let Err(error) = remove_file_if_exists(&path).await {
                let _ = SharedMediaRepo::reset_media_gc_candidate(
                    &*self.db,
                    &candidate.id,
                    &self.lease_owner,
                    candidate.version,
                    &now,
                )
                .await;
                warn!(asset_id = %candidate.id, %error, "shared media GC unlink failed");
                continue;
            }

            match SharedMediaRepo::complete_media_gc(
                &*self.db,
                &candidate.id,
                &self.lease_owner,
                candidate.version,
                &now,
            )
            .await
            {
                Ok(Some(_)) => completed += 1,
                Ok(None) => {}
                Err(error) => {
                    let _ = SharedMediaRepo::reset_media_gc_candidate(
                        &*self.db,
                        &candidate.id,
                        &self.lease_owner,
                        candidate.version,
                        &now,
                    )
                    .await;
                    warn!(asset_id = %candidate.id, %error, "shared media GC finalization failed");
                }
            }
        }
        Ok(completed)
    }

    async fn reconcile_purged_assets(&self, now: &str) -> Result<usize> {
        let assets = SharedMediaRepo::list_purged_media_assets(&*self.db, self.batch_size).await?;
        let mut completed = 0;
        for asset in assets {
            let result = async {
                let path = safe_media_path(&self.media_root, &asset.storage_key)?;
                remove_file_if_exists(&path).await?;
                SharedMediaRepo::mark_purged_media_asset_reconciled(&*self.db, &asset.id, now)
                    .await?;
                Ok::<(), ServiceError>(())
            }
            .await;
            match result {
                Ok(()) => completed += 1,
                Err(error) => {
                    warn!(asset_id = %asset.id, %error, "purged media byte reconciliation failed");
                }
            }
        }
        Ok(completed)
    }

    /// Recover the small cross-resource window around a Project upload. A
    /// pending row is the durable authority for staging/final names; bytes
    /// are never inferred from an untrusted directory walk. Metadata that was
    /// committed but whose bytes are missing remains quarantined so a retry
    /// can restage it. A stale pre-metadata row with no bytes is safe to drop.
    async fn reconcile_pending_uploads(&self, now: &str) -> Result<usize> {
        let uploads =
            SharedMediaRepo::list_pending_project_media_uploads(&*self.db, self.batch_size).await?;
        let mut recovered = 0;
        let cutoff = chrono::DateTime::parse_from_rfc3339(now)
            .map(|time| time - chrono::Duration::hours(1))
            .ok();
        for upload in uploads {
            match self.reconcile_pending_upload(&upload, now, cutoff).await {
                Ok(true) => recovered += 1,
                Ok(false) => {}
                Err(error) => {
                    warn!(
                        project_id = %upload.project_id,
                        asset_id = %upload.asset_id,
                        %error,
                        "pending media upload reconciliation failed"
                    );
                }
            }
        }
        Ok(recovered)
    }

    async fn reconcile_pending_upload(
        &self,
        upload: &db::ProjectMediaUpload,
        now: &str,
        cutoff: Option<chrono::DateTime<chrono::FixedOffset>>,
    ) -> Result<bool> {
        let staging_key = upload.staging_storage_key.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("pending media upload has no staging key")
        })?;
        let staging_path = safe_media_path(&self.media_root, staging_key)?;
        let final_path = safe_media_path(&self.media_root, &upload.final_storage_key)?;
        let staging_exists = tokio::fs::try_exists(&staging_path)
            .await
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let final_exists = tokio::fs::try_exists(&final_path)
            .await
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;

        let staging_valid = file_matches(&staging_path, upload.byte_size, &upload.checksum).await?;
        let final_valid = file_matches(&final_path, upload.byte_size, &upload.checksum).await?;

        // A crash can leave both names. Prefer the verified final bytes;
        // otherwise promote only verified staging bytes. Never infer
        // validity from existence alone.
        if final_valid {
            if staging_exists {
                remove_file_if_exists(&staging_path).await?;
            }
        } else if staging_valid {
            if final_exists {
                remove_file_if_exists(&final_path).await?;
            }
            let parent = final_path.parent().ok_or_else(|| {
                ServiceError::invalid_operation("media final path has no parent directory")
            })?;
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
            tokio::fs::rename(&staging_path, &final_path)
                .await
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        }

        let final_valid = file_matches(&final_path, upload.byte_size, &upload.checksum).await?;

        if upload.status == "metadata_committed" {
            if final_valid {
                SharedMediaRepo::finalize_project_media_upload(
                    &*self.db,
                    &upload.project_id,
                    &upload.asset_id,
                    now,
                )
                .await?;
                return Ok(true);
            }
            return Ok(false);
        }

        if upload.status != "pending" {
            // Unknown durable states are not safe to interpret as
            // finalized. Leave the row and bytes for an operator-visible
            // retry rather than deleting them.
            return Ok(false);
        }

        if final_valid {
            let created_at = upload.created_at.clone();
            let metadata = SharedMediaRepo::create_project_media_asset(
                &*self.db,
                CreateProjectMediaAsset {
                    id: upload.asset_id.clone(),
                    project_id: upload.project_id.clone(),
                    display_filename: upload.display_filename.clone(),
                    content_type: upload.content_type.clone(),
                    byte_size: upload.byte_size,
                    storage_key: upload.final_storage_key.clone(),
                    checksum: upload.checksum.clone(),
                    idempotency_key: upload.idempotency_key.clone(),
                    mutation_fingerprint: upload.mutation_fingerprint.clone(),
                    expected_project_version: upload.expected_project_version,
                    actor_type: "system".to_owned(),
                    actor_id: None,
                    authorization_event_id: format!(
                        "project-media-reconciliation:{}:{}",
                        upload.project_id, upload.idempotency_key
                    ),
                    created_at: created_at.clone(),
                },
            )
            .await;
            match metadata {
                Ok(_) => {}
                // The pending row reserved the upload only after the
                // original expected Project version matched.  If a
                // concurrent Project mutation advanced that version
                // before a crash was reconciled, this upload is a stale
                // operation: discard its unreferenced bytes and durable
                // marker instead of retrying forever on every tick.
                Err(db::DbError::VersionConflict) => {
                    remove_file_if_exists(&staging_path).await?;
                    remove_file_if_exists(&final_path).await?;
                    SharedMediaRepo::delete_pending_project_media_upload(
                        &*self.db,
                        &upload.project_id,
                        &upload.idempotency_key,
                    )
                    .await?;
                    return Ok(true);
                }
                Err(error) => return Err(error.into()),
            }
            SharedMediaRepo::finalize_project_media_upload(
                &*self.db,
                &upload.project_id,
                &upload.asset_id,
                now,
            )
            .await?;
            return Ok(true);
        }

        // The pending row precedes metadata so a crash before the first
        // byte write cannot create an untracked final asset. Keep a
        // recent row for a client retry; remove stale rows and their
        // staging bytes once no metadata transaction committed.
        if !final_valid
            && cutoff.is_some_and(|cutoff| {
                chrono::DateTime::parse_from_rfc3339(&upload.created_at)
                    .is_ok_and(|created| created < cutoff)
            })
        {
            if staging_exists {
                remove_file_if_exists(&staging_path).await?;
            }
            if final_exists {
                remove_file_if_exists(&final_path).await?;
            }
            SharedMediaRepo::delete_pending_project_media_upload(
                &*self.db,
                &upload.project_id,
                &upload.idempotency_key,
            )
            .await?;
            return Ok(true);
        }
        Ok(false)
    }
}

fn safe_media_path(root: &Path, storage_key: &str) -> Result<PathBuf> {
    let relative = Path::new(storage_key);
    let has_normal_component = relative
        .components()
        .any(|component| matches!(component, Component::Normal(_)));
    if storage_key.is_empty()
        || !has_normal_component
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
    {
        return Err(ServiceError::invalid_operation(
            "invalid shared media storage key",
        ));
    }
    Ok(root.join(relative))
}

async fn file_matches(
    path: &Path,
    expected_byte_size: i64,
    expected_checksum: &str,
) -> Result<bool> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "shared media reconciliation failed for {}: {error}",
                path.display()
            )));
        }
    };
    let actual_size = i64::try_from(bytes.len())
        .map_err(|_| ServiceError::invalid_operation("media byte size exceeds i64"))?;
    if actual_size != expected_byte_size
        || expected_checksum.len() != 64
        || !expected_checksum
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Ok(false);
    }
    Ok(hex::encode(Sha256::digest(&bytes)) == expected_checksum)
}

async fn remove_file_if_exists(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ServiceError::invalid_operation(format!(
            "shared media cleanup failed for {}: {error}",
            path.display()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::safe_media_path;
    use std::path::Path;

    #[test]
    fn storage_key_guard_rejects_escape_components() {
        assert!(safe_media_path(Path::new("/tmp/media"), "task/id.png").is_ok());
        assert!(safe_media_path(Path::new("/tmp/media"), "../outside").is_err());
        assert!(safe_media_path(Path::new("/tmp/media"), "/absolute").is_err());
        assert!(safe_media_path(Path::new("/tmp/media"), ".").is_err());
    }
}
