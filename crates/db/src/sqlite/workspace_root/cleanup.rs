//! Ownership-fenced Project deletion cleanup.
use super::*;

#[derive(Debug, Clone, Copy)]
pub enum ProjectCleanupOwner {
    TaskId,
    WorkspacePath,
    RepoLocalPath,
    RepoId,
    ProjectId,
}

impl ProjectCleanupOwner {
    fn query(self) -> Option<&'static str> {
        match self {
            Self::TaskId => Some("SELECT 1 FROM task WHERE id = ? LIMIT 1"),
            Self::WorkspacePath => None,
            Self::RepoLocalPath => Some("SELECT 1 FROM repo WHERE local_path = ? LIMIT 1"),
            Self::RepoId => Some("SELECT 1 FROM repo WHERE id = ? LIMIT 1"),
            Self::ProjectId => Some("SELECT 1 FROM project WHERE id = ? LIMIT 1"),
        }
    }
}

impl SqliteDb {
    /// Re-check a captured filesystem target against live authoritative rows and
    /// reserve it while holding SQLite's write lock. Paths are not globally unique,
    /// so a later Project/Task/Workspace may have claimed the same path after the
    /// deletion transaction captured it. The ownership check and an O(1) rename to
    /// a unique quarantine child share `BEGIN IMMEDIATE`, preventing a creator
    /// from claiming the target between the check and reservation. The recursive
    /// removal runs only after the transaction commits, so SQLite's writer lock is
    /// not held for the duration of filesystem cleanup. Any read/lock/rename or
    /// removal failure is returned so callers can skip cleanup rather than risk
    /// deleting another owner's path.
    pub async fn remove_unowned_project_cleanup_path(
        &self,
        owner: ProjectCleanupOwner,
        ownership_value: &str,
        root: &std::path::Path,
        candidate: &std::path::Path,
        kind: &str,
    ) -> Result<bool> {
        let mut transaction = crate::begin_immediate(self.pool()).await.map_err(|error| {
            DbError::ProjectCleanup(format!("lock Project cleanup ownership: {error}"))
        })?;
        let live = match owner.query() {
            Some(query) => sqlx::query_scalar::<_, i64>(query)
                .bind(ownership_value)
                .fetch_optional(&mut *transaction)
                .await
                .map(|row| row.is_some())
                .map_err(DbError::from),
            None => {
                WorkspaceRepo::embedded_path_is_owned_in_tx(self, &mut transaction, ownership_value)
                    .await
            }
        }
        .map_err(|error| {
            DbError::ProjectCleanup(format!("recheck Project cleanup ownership: {error}"))
        })?;
        if live {
            transaction.commit().await.map_err(|error| {
                DbError::ProjectCleanup(format!("release Project cleanup lock: {error}"))
            })?;
            return Ok(true);
        }

        let quarantine = quarantine_confined_direct_child(root, candidate, kind).await?;
        if let Err(error) = transaction.commit().await {
            if let Some(quarantine) = quarantine {
                // The ownership reservation did not commit. Preserve the bytes:
                // restore the original name when it is still free, otherwise
                // leave the unique quarantine in place for operator recovery.
                let candidate_is_free = match tokio::fs::symlink_metadata(candidate).await {
                    Err(candidate_error)
                        if candidate_error.kind() == std::io::ErrorKind::NotFound =>
                    {
                        true
                    }
                    Ok(_) => false,
                    Err(candidate_error) => {
                        tracing::error!(
                            path = %candidate.display(),
                            error = %candidate_error,
                            "failed to inspect Project cleanup target after lock commit failure"
                        );
                        false
                    }
                };
                if candidate_is_free {
                    if let Err(restore_error) = tokio::fs::rename(&quarantine, candidate).await {
                        tracing::error!(
                            quarantine = %quarantine.display(),
                            path = %candidate.display(),
                            error = %restore_error,
                            "failed to restore Project cleanup quarantine after lock commit failure"
                        );
                    }
                } else {
                    tracing::error!(
                        quarantine = %quarantine.display(),
                        path = %candidate.display(),
                        "preserving Project cleanup quarantine because the original path is occupied"
                    );
                }
            }
            return Err(DbError::ProjectCleanup(format!(
                "commit Project cleanup lock: {error}"
            )));
        }
        if let Some(quarantine) = quarantine {
            tokio::fs::remove_dir_all(&quarantine)
                .await
                .map_err(|error| {
                    DbError::ProjectCleanup(format!("remove {kind} cleanup quarantine: {error}"))
                })?;
        }
        Ok(false)
    }
}

/// Atomically move an eligible direct child to a unique quarantine sibling.
/// The caller must hold the DB ownership lock while invoking this function;
/// the rename is deliberately separate from recursive removal so the lock is
/// held only for the short path reservation.
async fn quarantine_confined_direct_child(
    root: &std::path::Path,
    candidate: &std::path::Path,
    kind: &str,
) -> Result<Option<std::path::PathBuf>> {
    let canonical_root = match tokio::fs::canonicalize(root).await {
        Ok(root) => root,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(DbError::ProjectCleanup(format!(
                "resolve Project-owned root after deletion: {error}"
            )));
        }
    };
    let metadata = match tokio::fs::symlink_metadata(candidate).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(DbError::ProjectCleanup(format!(
                "inspect {kind} after Project deletion: {error}"
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Ok(None);
    }
    let canonical = tokio::fs::canonicalize(candidate).await.map_err(|error| {
        DbError::ProjectCleanup(format!("resolve {kind} after Project deletion: {error}"))
    })?;
    if canonical.parent() != Some(canonical_root.as_path()) {
        return Ok(None);
    }
    let metadata = match tokio::fs::symlink_metadata(candidate).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(DbError::ProjectCleanup(format!(
                "re-inspect {kind} after Project deletion: {error}"
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Ok(None);
    }
    let quarantine = canonical_root.join(format!(".forge-project-delete-{}", new_uuid_v4()));
    tokio::fs::rename(candidate, &quarantine)
        .await
        .map_err(|error| DbError::ProjectCleanup(format!("quarantine {kind}: {error}")))?;
    Ok(Some(quarantine))
}
