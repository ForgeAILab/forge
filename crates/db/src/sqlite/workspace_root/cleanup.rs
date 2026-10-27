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
    /// Check ownership while the caller holds the cleanup reservation transaction.
    pub async fn project_cleanup_owner_is_live_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        owner: ProjectCleanupOwner,
        ownership_value: &str,
    ) -> Result<bool> {
        match owner.query() {
            Some(query) => Ok(sqlx::query_scalar::<_, i64>(query)
                .bind(ownership_value)
                .fetch_optional(&mut **transaction)
                .await?
                .is_some()),
            None => {
                WorkspaceRepo::embedded_path_is_owned_in_tx(self, transaction, ownership_value)
                    .await
            }
        }
    }
}
