use db::{now_rfc3339, Project, ProjectRepo, Repo, RepoRepo};

use crate::Result;

use super::TaskDispatcher;

/// `Project::system_pause_reason` value for a Project the dispatcher paused
/// because it has no primary repository. See migration V128.
pub(super) const MISSING_REPOSITORY: &str = "missing_repository";
/// `Project::system_pause_reason` for a primary Repo pointer that is missing
/// from storage or belongs to another Project.
pub(super) const INVALID_REPOSITORY: &str = "invalid_repository";
/// `Project::system_pause_reason` for a linked primary Repo whose checkout
/// cannot host execution yet — typically a scaffold whose `main` branch has
/// no commit. Dispatching then would only park every Task on a setup refusal
/// that nothing outside Forge's own state changes would ever wake.
pub(super) const REPOSITORY_NOT_READY: &str = "repository_not_ready";

fn is_repository_pause_reason(reason: Option<&str>) -> bool {
    matches!(
        reason,
        Some(MISSING_REPOSITORY | INVALID_REPOSITORY | REPOSITORY_NOT_READY)
    )
}

impl TaskDispatcher {
    /// Keep a Project's pause state in sync with whether it has a primary
    /// repository that can host execution, instead of parking its Tasks in `backlog` at creation
    /// (that state is reserved for a deliberate later move by a user or the
    /// Agent). A Project with no repository is paused here with a recorded
    /// reason; once a ready repository is attached, a Project paused for one
    /// of these reasons is resumed, which wakes its parked Tasks.
    /// "Ready" is the same checkout check execution admission applies, so a
    /// Project is never left running while every dispatch is refused. A Project the user paused for their own
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
                Some(repo) if repo.project_id == project.id => {
                    // Checkout readiness is only needed where it could change
                    // the pause state; a user's pause is never touched.
                    if project.paused_at.is_some()
                        && !is_repository_pause_reason(project.system_pause_reason.as_deref())
                    {
                        return Ok(false);
                    }
                    match self.repository_ready(&repo).await {
                        Ok(true) => None,
                        Ok(false) => Some(REPOSITORY_NOT_READY),
                        Err(error) => {
                            // Unknown is not "not ready": leave the pause
                            // state alone and let the next scan re-check.
                            tracing::warn!(
                                project_id = %project.id,
                                repo_id = %repo.id,
                                %error,
                                "could not verify primary repository checkout"
                            );
                            return Ok(false);
                        }
                    }
                }
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
                && !is_repository_pause_reason(project.system_pause_reason.as_deref())
            {
                return Ok(false);
            }
            let paused = ProjectRepo::set_system_pause_reason_if_unchanged(
                &*self.db,
                &project.id,
                project.version,
                project.primary_repo_id.as_deref(),
                reason == REPOSITORY_NOT_READY,
                &now_rfc3339(),
                reason,
            )
            .await?;
            if paused {
                tracing::info!(
                    project_id = %project.id,
                    system_pause_reason = reason,
                    "paused Project automatically: primary repository setup is not valid"
                );
            }
            return Ok(paused);
        }
        if project.paused_at.is_some()
            && is_repository_pause_reason(project.system_pause_reason.as_deref())
        {
            // The repository lookup above and the Project snapshot are both
            // staleable.  In particular, a user can manually pause/resume
            // this Project after this scan listed it but before the automatic
            // resume write.  Clear the pause only if every authority-bearing
            // field still matches the snapshot that caused this decision.
            let Some(expected_paused_at) = project.paused_at.as_deref() else {
                return Ok(false);
            };
            let Some(expected_primary_repo_id) = project.primary_repo_id.as_deref() else {
                return Ok(false);
            };
            let Some(expected_reason) = project.system_pause_reason.as_deref() else {
                return Ok(false);
            };
            let cleared = ProjectRepo::clear_system_pause_if_unchanged(
                &*self.db,
                &project.id,
                project.version,
                expected_primary_repo_id,
                expected_paused_at,
                expected_reason,
            )
            .await?;
            if cleared {
                tracing::info!(
                    project_id = %project.id,
                    "resumed Project automatically: its primary repository is now ready"
                );
            }
            return Ok(cleared);
        }
        Ok(false)
    }

    /// Checkout readiness for `repo`, memoized once positive. A checkout does
    /// not become unborn again, so a verified Repo costs no git processes on
    /// later scans; any Repo update (new path or branch) changes the key and
    /// re-verifies. Execution admission still re-checks on every dispatch.
    async fn repository_ready(&self, repo: &Repo) -> Result<bool> {
        let key = format!("{}@{}", repo.id, repo.updated_at);
        if self
            .ready_repositories
            .lock()
            .expect("ready repository cache lock")
            .contains(&key)
        {
            return Ok(true);
        }
        let ready =
            crate::project_execution_setup_projection::verify_repository_state(repo).await?;
        if ready {
            self.ready_repositories
                .lock()
                .expect("ready repository cache lock")
                .insert(key);
        }
        Ok(ready)
    }
}
