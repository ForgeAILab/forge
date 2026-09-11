use db::{now_rfc3339, Project, ProjectRepo, RepoRepo};

use crate::Result;

use super::TaskDispatcher;

/// `Project::system_pause_reason` value for a Project the dispatcher paused
/// because it has no primary repository. See migration V128.
pub(super) const MISSING_REPOSITORY: &str = "missing_repository";
/// `Project::system_pause_reason` for a primary Repo pointer that is missing
/// from storage or belongs to another Project.
pub(super) const INVALID_REPOSITORY: &str = "invalid_repository";

impl TaskDispatcher {
    /// Keep a Project's pause state in sync with whether it has a primary
    /// repository, instead of parking its Tasks in `backlog` at creation
    /// (that state is reserved for a deliberate later move by a user or the
    /// Agent). A Project with no repository is paused here with a recorded
    /// reason; once a repository is attached, a Project paused for exactly
    /// that reason is resumed. A Project the user paused for their own
    /// reason (`system_pause_reason` is `None`) is never touched — pausing
    /// or resuming through the ordinary Project routes always clears the
    /// reason, so a manual pause can never look like, or later be mistaken
    /// for, this automatic one.
    ///
    /// Returns `true` when this call changed the Project's pause state, so
    /// the caller can skip acting on stale in-memory state for the rest of
    /// this scan; the next tick sees the fresh state.
    pub(super) async fn sync_repository_pause(&self, project: &Project) -> Result<bool> {
        let setup_pause_reason = match project.primary_repo_id.as_deref() {
            None => Some(MISSING_REPOSITORY),
            Some(repo_id) => match RepoRepo::get_by_id(&*self.db, repo_id).await? {
                Some(repo) if repo.project_id == project.id => None,
                Some(_) | None => Some(INVALID_REPOSITORY),
            },
        };

        if let Some(reason) = setup_pause_reason {
            if project.paused_at.is_some() && project.system_pause_reason.as_deref() == Some(reason)
            {
                return Ok(false);
            }
            // Never overwrite a user pause or a system pause owned by another
            // subsystem. Repository sync only updates the reasons it owns.
            if project.paused_at.is_some()
                && !matches!(
                    project.system_pause_reason.as_deref(),
                    Some(MISSING_REPOSITORY | INVALID_REPOSITORY)
                )
            {
                return Ok(false);
            }
            ProjectRepo::set_system_pause_reason(&*self.db, &project.id, &now_rfc3339(), reason)
                .await?;
            tracing::info!(
                project_id = %project.id,
                system_pause_reason = reason,
                "paused Project automatically: primary repository setup is not valid"
            );
            return Ok(true);
        }
        if project.paused_at.is_some()
            && matches!(
                project.system_pause_reason.as_deref(),
                Some(MISSING_REPOSITORY | INVALID_REPOSITORY)
            )
        {
            ProjectRepo::set_paused_at(&*self.db, &project.id, None).await?;
            tracing::info!(
                project_id = %project.id,
                "resumed Project automatically: its primary repository is now attached"
            );
            return Ok(true);
        }
        Ok(false)
    }
}
