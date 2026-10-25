//! Workspace, placement, runtime, daemon, and terminal session repositories.

use super::*;

#[async_trait]
pub trait WorkspaceRepo: Send + Sync {
    async fn create(&self, input: CreateWorkspace) -> Result<Workspace>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Workspace>>;
    async fn get_by_task_id(&self, task_id: &str) -> Result<Option<Workspace>>;
    /// Cleanup ownership guard for the legacy embedded path, under the same
    /// writer lock as the caller's filesystem quarantine operation.
    async fn embedded_path_is_owned_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        path: &str,
    ) -> Result<bool>;
    /// Authorize an embedded runtime path without exposing the legacy column.
    /// A recorded placement is authoritative; unplaced rows retain their
    /// exact-path guard until the backend records their server placement.
    async fn task_owns_embedded_path(&self, task_id: &str, path: &str) -> Result<bool>;
    async fn set_cleanup_after(
        &self,
        id: &str,
        cleanup_after: Option<String>,
        updated_at: &str,
    ) -> Result<Workspace>;
    async fn mark_cleaned(&self, id: &str, updated_at: &str) -> Result<Workspace>;
    async fn list_pending_cleanup(&self, now: &str) -> Result<Vec<Workspace>>;
    async fn update_status(
        &self,
        id: &str,
        status: WorkspaceStatus,
        error: Option<String>,
        updated_at: &str,
    ) -> Result<Workspace>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait WorkspacePlacementRepo: Send + Sync {
    async fn create(&self, input: CreateWorkspacePlacement) -> Result<WorkspacePlacement>;
    async fn create_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateWorkspacePlacement,
    ) -> Result<WorkspacePlacement>;
    async fn get_by_id(&self, id: &str) -> Result<Option<WorkspacePlacement>>;
    async fn get_by_id_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        id: &str,
    ) -> Result<Option<WorkspacePlacement>>;
    async fn get_by_workspace_id(&self, workspace_id: &str) -> Result<Option<WorkspacePlacement>>;
    async fn get_by_workspace_id_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        workspace_id: &str,
    ) -> Result<Option<WorkspacePlacement>>;
    async fn update(&self, input: UpdateWorkspacePlacement) -> Result<WorkspacePlacement>;
    async fn update_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdateWorkspacePlacement,
    ) -> Result<WorkspacePlacement>;
    async fn list_by_daemon_and_state(
        &self,
        daemon_id: &str,
        state: PlacementState,
    ) -> Result<Vec<WorkspacePlacement>>;
    async fn list_by_state(&self, state: PlacementState) -> Result<Vec<WorkspacePlacement>>;
    async fn get_for_task(&self, task_id: &str) -> Result<Option<WorkspacePlacement>>;
    /// Suspend a ready placement only while the observed execution version
    /// still has an expired heartbeat lease and a live hard deadline.
    async fn suspend_expired_execution_lease(
        &self,
        placement: &WorkspacePlacement,
        execution: &Execution,
        now: &str,
    ) -> Result<Option<WorkspacePlacement>>;
    /// Rebind a suspended attempt to a new authenticated incarnation of its
    /// existing owner. Both the placement and execution versions are fenced.
    async fn resume_disconnected_execution_lease(
        &self,
        placement: &WorkspacePlacement,
        execution: &Execution,
        input: RenewExecutionLease,
    ) -> Result<ExecutionLeaseMutation>;
    async fn expire_unsuspended_workspace_leases(
        &self,
        now: &str,
        limit: i64,
    ) -> Result<Vec<WorkspaceLease>>;
}

/// Internal scheduler authority for a Task workspace.  A lease is deliberately
/// separate from the filesystem-backed `Workspace` row: chat agents never
/// receive this record, a path, or a bearer token.  The scheduler persists only
/// the opaque logical repository binding and a short-lived capability grant.
#[async_trait]
pub trait WorkspaceLeaseRepo: Send + Sync {
    async fn issue(&self, input: CreateWorkspaceLease) -> Result<WorkspaceLease>;
    /// Issue a scheduler lease only while the Project authority snapshot that
    /// selected the execution is still current. The version/workflow compare
    /// and lease INSERT must share one writer transaction so a workflow edit
    /// cannot land between the final read and authority grant.
    async fn issue_with_project_authority(
        &self,
        input: CreateWorkspaceLease,
        expected_project_version: i64,
        expected_workflow_definition: &str,
        expected_task_status: &str,
        expected_effective_role: &str,
    ) -> Result<WorkspaceLease>;
    /// Atomically retire the expected active lease and issue its replacement
    /// while rechecking the Project/Task/workflow authority snapshot. Any
    /// validation or INSERT failure rolls the retirement back with it.
    #[allow(clippy::too_many_arguments)]
    async fn replace_with_project_authority(
        &self,
        input: CreateWorkspaceLease,
        expected_project_version: i64,
        expected_workflow_definition: &str,
        expected_task_status: &str,
        expected_effective_role: &str,
        replacing_lease_id: &str,
        replacing_lease_version: i64,
    ) -> Result<WorkspaceLease>;
    async fn get_by_id(&self, id: &str) -> Result<Option<WorkspaceLease>>;
    async fn get_active_for_task(&self, task_id: &str) -> Result<Option<WorkspaceLease>>;
    async fn list_active_for_project(&self, project_id: &str) -> Result<Vec<WorkspaceLease>>;
    async fn revoke(
        &self,
        id: &str,
        expected_version: i64,
        revoked_at: &str,
    ) -> Result<WorkspaceLease>;
    /// Extend scheduler-owned leases that are approaching expiry while their
    /// exact Task, execution, assignment, and governance bindings remain
    /// current. Invalid or concurrently changed candidates are skipped.
    async fn renew_active(
        &self,
        now: &str,
        renew_before: &str,
        expires_at: &str,
        limit: i64,
    ) -> Result<Vec<WorkspaceLease>>;
    async fn expire(&self, now: &str, limit: i64) -> Result<Vec<WorkspaceLease>>;
}

#[async_trait]
pub trait DaemonRepo: Send + Sync {
    async fn upsert_by_machine_id(&self, input: UpsertDaemon) -> Result<Daemon>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Daemon>>;
    async fn get_by_machine_id(&self, machine_id: &str) -> Result<Option<Daemon>>;
    async fn list(&self, page: PageRequest) -> Result<Page<Daemon>>;
    async fn list_visible(&self, user_id: Option<&str>, page: PageRequest) -> Result<Page<Daemon>>;
    async fn get_visible(&self, id: &str, user_id: Option<&str>) -> Result<Option<Daemon>>;
    async fn update_report(&self, input: UpdateDaemonReport) -> Result<Daemon>;
    async fn update_run_limit(
        &self,
        id: &str,
        version: i64,
        run_limit: Option<u32>,
    ) -> Result<Daemon>;
    async fn mark_online(&self, id: &str, last_report_at: &str) -> Result<Daemon>;
    async fn mark_offline(&self, id: &str, updated_at: &str) -> Result<Daemon>;
    async fn list_available_for_executor(&self, executor_type: &str) -> Result<Vec<Daemon>>;
}

#[async_trait]
pub trait RuntimeRepo: Send + Sync {
    async fn create(&self, input: CreateRuntime) -> Result<Runtime>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Runtime>>;
    async fn get_by_daemon_id(&self, daemon_id: &str) -> Result<Option<Runtime>>;
    async fn upsert_by_daemon_kind(&self, input: CreateRuntime) -> Result<Runtime>;
    async fn list(&self, query: RuntimeListQuery) -> Result<Page<Runtime>>;
}

#[async_trait]
pub trait TerminalSessionRepo: Send + Sync {
    async fn create_terminal_session(
        &self,
        input: CreateTerminalSession,
    ) -> Result<TerminalSession>;
    async fn get_terminal_session(&self, id: &str) -> Result<Option<TerminalSession>>;
    async fn list_terminal_sessions_for_task(
        &self,
        task_id: &str,
        include_ended: bool,
    ) -> Result<Vec<TerminalSession>>;
    async fn list_running_terminal_sessions_for_task(
        &self,
        task_id: &str,
    ) -> Result<Vec<TerminalSession>>;
    async fn list_running_terminal_sessions_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<TerminalSession>>;
    async fn list_running_terminal_sessions_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<TerminalSession>>;
    async fn list_all_running_terminal_sessions(&self) -> Result<Vec<TerminalSession>>;
    async fn update_terminal_session_status(
        &self,
        id: &str,
        expected_version: i64,
        update: UpdateTerminalSessionStatus,
    ) -> Result<TerminalSession>;
    async fn update_terminal_session_size(
        &self,
        id: &str,
        rows: i64,
        cols: i64,
        last_activity_at: &str,
    ) -> Result<TerminalSession>;
    async fn touch_terminal_session_activity(&self, id: &str, last_activity_at: &str)
        -> Result<()>;
    async fn delete_terminal_sessions_for_workspace(&self, workspace_id: &str) -> Result<u64>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateWorkspace {
    pub id: String,
    pub task_id: String,
    pub repo_id: String,
    pub worktree_path: String,
    pub branch: String,
    pub status: WorkspaceStatus,
    pub before_sha: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateWorkspacePlacement {
    pub id: String,
    pub workspace_id: String,
    pub task_id: String,
    pub agent_id: Option<String>,
    pub owner_kind: PlacementOwnerKind,
    pub daemon_id: Option<String>,
    pub runtime_id: Option<String>,
    pub repo_location_id: String,
    pub execution_daemon_id: Option<String>,
    pub workspace_handle: Option<String>,
    pub generation: i64,
    pub state: PlacementState,
    pub selected_by: PlacementSelectedBy,
    pub selection_reason: String,
    pub reserved_until: Option<String>,
    pub disconnected_at: Option<String>,
    pub failure_cause: Option<PlacementFailureCause>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateWorkspacePlacement {
    pub id: String,
    pub expected_version: i64,
    pub agent_id: Option<Option<String>>,
    pub owner_kind: Option<PlacementOwnerKind>,
    pub daemon_id: Option<Option<String>>,
    pub runtime_id: Option<Option<String>>,
    pub repo_location_id: Option<String>,
    pub execution_daemon_id: Option<Option<String>>,
    pub workspace_handle: Option<Option<String>>,
    pub generation: Option<i64>,
    pub state: Option<PlacementState>,
    pub selected_by: Option<PlacementSelectedBy>,
    pub selection_reason: Option<String>,
    pub reserved_until: Option<Option<String>>,
    pub disconnected_at: Option<Option<String>>,
    pub failure_cause: Option<Option<PlacementFailureCause>>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateWorkspaceLease {
    pub id: String,
    pub project_id: String,
    pub task_id: String,
    pub task_version: i64,
    pub execution_id: String,
    pub operation_idempotency_key: String,
    pub repository_binding_id: String,
    pub base_ref: String,
    pub role: String,
    pub capabilities_json: String,
    pub assigned_principal_type: String,
    pub assigned_principal_id: String,
    pub capability_profile_revision: String,
    pub capability_profile_digest: String,
    pub issuing_principal_type: String,
    pub issuing_principal_id: String,
    pub issued_at: String,
    pub expires_at: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertDaemon {
    pub max_concurrent_runs: Option<u32>,
    pub id: String,
    pub machine_id: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub agent_version: Option<String>,
    pub labels_json: String,
    pub status: DaemonStatus,
    pub registration_token_hash: Option<String>,
    pub owner_id: Option<String>,
    pub visibility: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateDaemonReport {
    pub max_concurrent_runs: Option<u32>,
    pub id: String,
    pub last_report_at: String,
    pub status: DaemonStatus,
    pub detected_clis_json: String,
    pub labels_json: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRuntime {
    pub id: String,
    pub daemon_id: String,
    pub kind: String,
    pub workspace_root: String,
    pub status: RuntimeStatus,
    pub labels_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeListQuery {
    pub daemon_id: Option<String>,
    pub page: PageRequest,
}
