use db::{now_rfc3339, Project, ProjectRepo};

use crate::Result;

use super::TaskDispatcher;

/// `Project::system_pause_reason` value for a Project the dispatcher paused
/// because it has no primary repository. See migration V128.
pub(super) const MISSING_REPOSITORY: &str = "missing_repository";

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
        if project.primary_repo_id.is_none() {
            if project.paused_at.is_some() {
                return Ok(false);
            }
            ProjectRepo::set_system_pause_reason(
                &*self.db,
                &project.id,
                &now_rfc3339(),
                MISSING_REPOSITORY,
            )
            .await?;
            tracing::info!(
                project_id = %project.id,
                "paused Project automatically: no primary repository is attached yet"
            );
            return Ok(true);
        }
        if project.paused_at.is_some()
            && project.system_pause_reason.as_deref() == Some(MISSING_REPOSITORY)
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
