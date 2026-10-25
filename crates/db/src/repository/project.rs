//! Project, provisioning, repository location, and integration repositories.

use super::*;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectDeletionRepositoryPath {
    pub id: String,
    pub local_path: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectDeletionPaths {
    pub task_ids: Vec<String>,
    pub workspace_paths: Vec<String>,
    pub repository_paths: Vec<ProjectDeletionRepositoryPath>,
}

#[async_trait]
pub trait ProjectRepo: Send + Sync {
    async fn create(&self, input: CreateProject) -> Result<Project>;
    /// Create a Project together with its singular Agent Chat and binding.
    /// Passing no identity/profile preserves explicit setup-required state;
    /// passing both values makes the binding active in the same transaction.
    async fn create_with_agent_binding(
        &self,
        input: CreateProject,
        identity_id: Option<String>,
        profile_id: Option<String>,
    ) -> Result<Project>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Project>>;
    /// Public system Projects, owned Projects, and explicit memberships only.
    async fn get_visible_by_id(&self, id: &str, user_id: &str) -> Result<Option<Project>>;
    async fn list(&self, page: PageRequest) -> Result<Page<Project>>;
    /// Apply account visibility before pagination and counts.
    async fn list_visible(&self, user_id: &str, page: PageRequest) -> Result<Page<Project>>;
    async fn update_at_version(
        &self,
        input: UpdateProject,
        expected_version: i64,
        project_hooks_json: Option<String>,
    ) -> Result<Project>;
    /// Update the persisted workflow and clear Project-derived dispatch
    /// decisions in the same SQLite write transaction.
    async fn update_workflow(
        &self,
        id: &str,
        workflow_definition: &str,
        workflow_template_name: Option<&str>,
        expected_version: i64,
        updated_at: &str,
    ) -> Result<()>;
    async fn set_project_hooks_json(
        &self,
        id: &str,
        project_hooks_json: &str,
        updated_at: &str,
    ) -> Result<()>;
    async fn increment_project_work_epoch(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        by: i64,
    ) -> Result<i64>;
    async fn set_paused_at(&self, id: &str, paused_at: Option<String>) -> Result<()>;
    /// Pause a Project the Task dispatcher found without a usable primary
    /// repository only when the exact Project/repository snapshot that
    /// produced that decision is still current. `repository_linked` states
    /// which snapshot that was: `false` for a missing/invalid selection (the
    /// guard requires no owned Repo), `true` for a linked Repo whose checkout
    /// is not ready yet (the guard requires the owned Repo to still be
    /// linked). A concurrent repository attachment, deletion, or explicit
    /// pause makes this a benign no-op.
    async fn set_system_pause_reason_if_unchanged(
        &self,
        id: &str,
        expected_version: i64,
        expected_primary_repo_id: Option<&str>,
        repository_linked: bool,
        paused_at: &str,
        reason: &str,
    ) -> Result<bool>;
    /// Pause only an unchanged, unpaused Project. User and other system
    /// pauses always win; a stale snapshot is a benign no-op.
    async fn set_environment_pause_if_unchanged(
        &self,
        id: &str,
        expected_version: i64,
        paused_at: &str,
        detail_json: &str,
    ) -> Result<bool>;
    /// Refresh the check detail only while the observed environment pause
    /// remains current. Does not change the pause's original timestamp.
    async fn update_environment_pause_if_unchanged(
        &self,
        id: &str,
        expected_version: i64,
        expected_paused_at: &str,
        detail_json: &str,
    ) -> Result<bool>;
    /// Clear a dispatcher-owned Project pause only when the exact pause
    /// snapshot that the dispatcher observed is still current.  This is the
    /// compare-and-set boundary used by repository reconciliation: a manual
    /// pause/resume or another system writer must win instead of being
    /// cleared by a stale auto-resume.
    async fn clear_system_pause_if_unchanged(
        &self,
        id: &str,
        expected_version: i64,
        expected_primary_repo_id: &str,
        expected_paused_at: &str,
        expected_reason: &str,
    ) -> Result<bool>;
    /// Check the Project's in-use guard under the SQLite write lock without
    /// mutating any Project-owned rows. Callers use this before the final
    /// deletion boundary so a rejected deletion cannot touch live worktrees.
    async fn ensure_deletable(&self, id: &str) -> Result<()>;
    /// Tear the Project down. Refuses with [`DbError::ProjectInUse`] while a
    /// running Execution or an active Workspace lease still belongs to it.
    /// Callers that need force semantics must complete provider cancellation
    /// and lease revocation before calling this always-guarded boundary.
    async fn delete(&self, id: &str) -> Result<()>;
    /// Delete a Project and return the exact Task IDs, task-worktree, and
    /// repository paths that were present in the same final write transaction.
    /// Callers use the returned values to clean filesystem state admitted
    /// after an earlier snapshot but before this transaction acquired
    /// SQLite's write lock. The boundary is always in-use guarded; force
    /// callers must stop providers and revoke leases before reaching it.
    async fn delete_with_workspace_paths(&self, id: &str) -> Result<ProjectDeletionPaths>;
}

/// Atomic Project execution-setup mutation boundary. The command receipt is
/// committed in the same SQLite transaction as the Project CAS update, so a
/// response-loss replay cannot apply the setup mutation twice.
#[async_trait]
pub trait ProjectExecutionSetupCommandRepo: Send + Sync {
    async fn apply_project_execution_setup_command(
        &self,
        input: ApplyProjectExecutionSetupCommand,
    ) -> Result<AppliedProjectExecutionSetupCommand>;
}

/// Atomic Project review-policy mutation boundary. The Project CAS update,
/// domain event, and durable command receipt commit together, so a lost
/// response can be replayed without applying the settings change twice.
#[async_trait]
pub trait ProjectReviewConfigCommandRepo: Send + Sync {
    async fn apply_project_review_config_command(
        &self,
        input: ApplyProjectReviewConfigCommand,
    ) -> Result<AppliedProjectReviewConfigCommand>;
}

/// Durable reconciliation state for the cross-filesystem Project setup
/// operation.  These methods are intentionally separate from `ProjectRepo`:
/// setup readiness is not a Project version or a chat-binding lifecycle.
#[async_trait]
pub trait ProjectProvisioningRepo: Send + Sync {
    async fn get_provisioning_operation(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectProvisioningOperation>>;
    async fn get_provisioning_operation_by_id(
        &self,
        id: &str,
    ) -> Result<Option<ProjectProvisioningOperation>>;
    async fn create_provisioning_operation(
        &self,
        input: CreateProjectProvisioningOperation,
    ) -> Result<ProjectProvisioningOperation>;
    async fn update_provisioning_operation(
        &self,
        input: UpdateProjectProvisioningOperation,
    ) -> Result<ProjectProvisioningOperation>;
    async fn list_provisioning_checkpoints(
        &self,
        operation_id: &str,
    ) -> Result<Vec<ProjectProvisioningCheckpoint>>;
    async fn get_provisioning_checkpoint(
        &self,
        operation_id: &str,
        checkpoint: &str,
    ) -> Result<Option<ProjectProvisioningCheckpoint>>;
    async fn upsert_provisioning_checkpoint(
        &self,
        input: UpsertProjectProvisioningCheckpoint,
    ) -> Result<ProjectProvisioningCheckpoint>;
    async fn list_provisioning_errors(
        &self,
        operation_id: &str,
    ) -> Result<Vec<ProjectProvisioningError>>;
    async fn record_provisioning_error(
        &self,
        input: CreateProjectProvisioningError,
    ) -> Result<ProjectProvisioningError>;
}

#[async_trait]
pub trait ProjectHookRunRepo: Send + Sync {
    async fn try_claim(&self, input: CreateProjectHookRun) -> Result<Option<ProjectHookRun>>;
    async fn try_claim_or_skip_at_limit(
        &self,
        input: CreateProjectHookRun,
        max_active_runs: i64,
        skip_reason: &str,
    ) -> Result<Option<ProjectHookRun>>;
    async fn update_status(&self, input: UpdateProjectHookRun) -> Result<ProjectHookRun>;
    async fn list_recent_for_project(
        &self,
        project_id: &str,
        limit: i64,
    ) -> Result<Vec<ProjectHookRun>>;
    async fn list_for_project(
        &self,
        project_id: &str,
        page: PageRequest,
    ) -> Result<Page<ProjectHookRun>>;
    async fn count_active_for_rule(&self, project_id: &str, rule_id: &str) -> Result<i64>;
}

#[async_trait]
pub trait RepoRepo: Send + Sync {
    async fn create(&self, input: CreateRepo) -> Result<Repo>;
    /// Create a primary repository and the Project's primary-repository link
    /// plus dispatch wake atomically.
    async fn create_primary_for_project(
        &self,
        input: CreateRepo,
        expected_project_version: i64,
        project_updated_at: String,
    ) -> Result<Repo>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Repo>>;
    async fn list_by_project(&self, project_id: &str, page: PageRequest) -> Result<Page<Repo>>;
    async fn update(&self, input: UpdateRepo) -> Result<Repo>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait RepoLocationRepo: Send + Sync {
    async fn create(&self, input: CreateRepoLocation) -> Result<RepoLocation>;
    async fn get_by_id(&self, id: &str) -> Result<Option<RepoLocation>>;
    async fn list_by_repo(&self, repo_id: &str, page: PageRequest) -> Result<Page<RepoLocation>>;
    async fn update(&self, input: UpdateRepoLocation) -> Result<RepoLocation>;
    /// Returns VersionConflict while a non-cleaned placement references the location.
    async fn delete(&self, id: &str) -> Result<()>;
    /// Supplies the Task identity for the delete-in-use response.
    async fn get_blocking_placement(&self, id: &str) -> Result<Option<WorkspacePlacement>>;
}

#[async_trait]
pub trait IntegrationRepo: Send + Sync {
    async fn create_integration(
        &self,
        input: CreateProjectIntegration,
    ) -> Result<ProjectIntegration>;
    async fn get_by_id(&self, id: &str) -> Result<Option<ProjectIntegration>>;
    async fn get_by_project_id(&self, project_id: &str) -> Result<Option<ProjectIntegration>>;
    async fn update_integration(
        &self,
        input: UpdateProjectIntegration,
    ) -> Result<ProjectIntegration>;
    async fn update_last_polled_at(
        &self,
        id: &str,
        last_polled_at: &str,
        updated_at: &str,
    ) -> Result<()>;
    async fn list_enabled(&self) -> Result<Vec<ProjectIntegration>>;
    async fn delete_integration(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait ExternalLinkRepo: Send + Sync {
    async fn create_link(&self, input: CreateTaskExternalLink) -> Result<TaskExternalLink>;
    async fn get_by_id(&self, id: &str) -> Result<Option<TaskExternalLink>>;
    async fn get_by_global_id(&self, global_id: &str) -> Result<Option<TaskExternalLink>>;
    async fn get_by_task_id(&self, task_id: &str) -> Result<Option<TaskExternalLink>>;
    async fn list_by_task_id(&self, task_id: &str) -> Result<Vec<TaskExternalLink>>;
    async fn list_latest_links_for_tasks(&self, task_ids: &[&str])
        -> Result<Vec<TaskExternalLink>>;
    async fn list_by_integration(&self, integration_id: &str) -> Result<Vec<TaskExternalLink>>;
    async fn delete_link(&self, id: &str) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProject {
    pub id: String,
    pub name: String,
    pub settings: String,
    pub workflow_definition: String,
    pub primary_repo_id: Option<String>,
    pub owner_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateProject {
    pub id: String,
    pub name: Option<String>,
    pub settings: Option<String>,
    pub primary_repo_id: Option<Option<String>>,
    pub paused_at: Option<Option<String>>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRepo {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub remote_url: Option<String>,
    pub local_path: Option<String>,
    pub default_branch: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateRepo {
    pub id: String,
    pub name: Option<String>,
    pub local_path: Option<Option<String>>,
    pub remote_url: Option<Option<String>>,
    pub default_branch: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRepoLocation {
    pub id: String,
    pub repo_id: String,
    pub owner_kind: RepoLocationOwnerKind,
    pub daemon_id: Option<String>,
    pub runtime_id: Option<String>,
    pub path: String,
    pub kind: RepoLocationKind,
    pub is_default: bool,
    pub status: RepoLocationStatus,
    pub last_verified_at: Option<String>,
    pub last_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateRepoLocation {
    pub id: String,
    pub expected_version: i64,
    pub path: Option<String>,
    pub kind: Option<RepoLocationKind>,
    pub is_default: Option<bool>,
    pub status: Option<RepoLocationStatus>,
    pub last_verified_at: Option<Option<String>>,
    pub last_error: Option<Option<String>>,
    pub updated_at: String,
}
