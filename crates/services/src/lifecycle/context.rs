use std::path::PathBuf;

#[derive(Clone)]
pub struct LifecycleHookContext {
    pub event: api_types::LifecycleEvent,
    pub task_id: String,
    pub task_title: String,
    pub task_status: String,
    pub previous_status: String,
    pub project_id: String,
    pub project_name: String,
    pub repo_path: String,
    pub worktree_path: Option<String>,
    pub agent_id: Option<String>,
    pub execution_id: Option<String>,
    pub log_dir: Option<PathBuf>,
    /// The Project environment variables; Forge's own `FORGE_*` values win.
    pub env: std::collections::BTreeMap<String, String>,
}

pub(crate) fn embedded_workspace_router_for_test(
    db: std::sync::Arc<db::SqliteDb>,
    root: PathBuf,
    locks: Option<std::sync::Arc<workspace::RepoCacheLockManager>>,
) -> std::sync::Arc<crate::workspace_backend::WorkspaceBackendRouter> {
    let merge_service = std::sync::Arc::new(crate::MergeService::new(
        std::sync::Arc::clone(&db),
        std::sync::Arc::new(events::EventBus::default()),
        root.clone(),
    ));
    let mut embedded =
        crate::workspace_backend::EmbeddedWorkspaceBackend::new(db, merge_service, root);
    if let Some(locks) = locks {
        embedded = embedded.with_repo_cache_locks(locks);
    }
    std::sync::Arc::new(crate::workspace_backend::WorkspaceBackendRouter::new(
        std::sync::Arc::new(embedded),
    ))
}

pub(crate) async fn workspace_context_paths(
    db: &db::SqliteDb,
    workspace: &crate::workspace_backend::ResolvedWorkspace,
) -> crate::Result<(String, String)> {
    let handle = if workspace.placement.owner_kind == db::PlacementOwnerKind::Daemon {
        workspace.owner_paths().await?.0
    } else {
        workspace.handle()?.to_owned()
    };
    let location = db::RepoLocationRepo::get_by_id(db, &workspace.placement.repo_location_id)
        .await?
        .ok_or_else(|| {
            crate::ServiceError::not_found(
                "repository location",
                workspace.placement.repo_location_id.clone(),
            )
        })?;
    // Hooks in managed server clones have always exposed the worktree as
    // FORGE_REPO_PATH when there is no primary checkout.
    let repo_path = if workspace.placement.owner_kind == db::PlacementOwnerKind::Server
        && location.kind == db::RepoLocationKind::ManagedClone
    {
        handle.clone()
    } else {
        location.path
    };
    Ok((repo_path, handle))
}

/// The environment variables declared in a Project's raw settings JSON.
pub fn project_env(settings: &str) -> std::collections::BTreeMap<String, String> {
    serde_json::from_str::<api_types::ProjectSettings>(settings)
        .map(|settings| settings.environment.env)
        .unwrap_or_default()
}
