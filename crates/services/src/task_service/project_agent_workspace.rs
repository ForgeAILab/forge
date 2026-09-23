use super::*;
use std::path::{Path, PathBuf};

/// Ensure the Project Agent's own workspace.
///
/// This is the Agent's working directory, not a delivery worktree. It holds
/// `forge/` -- the durable place it keeps notes, decisions, and helper scripts
/// that later runs and other agents pick up -- and `checkout/`, a disposable
/// copy of the repository it can read and run so it can exercise the delivered
/// software instead of reasoning about whether it works.
///
/// `checkout/` is a detached git worktree: a real working copy on no branch,
/// so it never occupies the task branch namespace and nothing accumulates on
/// it. The Agent has a shell there, so it can of course run `git` -- what it
/// does not have is a write tool for source, a branch anything merges, or a
/// push path. The guarantee is that its edits go nowhere, not that git is
/// disabled.
pub async fn ensure_project_agent_workspace(
    db: &SqliteDb,
    workspaces_root: &Path,
    project_id: &str,
) -> Result<Option<PathBuf>> {
    // Capture an immutable generation/repository snapshot before touching the
    // filesystem. A Project ID can be explicitly reused after deletion; the
    // operation ID distinguishes those generations even when both rows start
    // at Project.version = 1.
    let Some((authority, repo)) = capture_project_agent_workspace_authority(db, project_id).await?
    else {
        return Ok(None);
    };

    // `git worktree add` runs with the repository as its working directory, so
    // a relative workspaces root (a `--data-dir ./test` server) would resolve
    // the checkout *inside the repository*. Anchor it absolutely first.
    let workspaces_root = &std::fs::canonicalize(workspaces_root).unwrap_or_else(|_| {
        std::env::current_dir()
            .map(|cwd| cwd.join(workspaces_root))
            .unwrap_or_else(|_| workspaces_root.to_path_buf())
    });
    let workspace = workspaces_root.join(project_id);
    // Reserve the path and stamp its generation before creating notes or a
    // checkout. If a replacement Project already claimed the same ID, the
    // old marker is atomically quarantined under the DB writer lock and never
    // becomes part of the replacement workspace.
    if !reserve_project_agent_workspace_path(db, &authority, &workspace).await? {
        return Ok(None);
    }
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR)).map_err(|error| {
        ServiceError::invalid_operation(format!("Project Agent workspace is unavailable: {error}"))
    })?;
    write_verification_workspace_boundary(&workspace)?;

    let Some(repo) = repo else {
        // No repository yet: the Agent still gets its durable docs directory.
        return retain_project_agent_workspace_if_current(db, &authority, workspace, None).await;
    };
    let checkout = workspace.join(PROJECT_AGENT_CHECKOUT_DIR);
    let repo_cache = workspaces_root.join(".repos").join(&repo.id);
    if checkout.is_dir() {
        // The checkout is a disposable read-and-run copy: re-point it at the
        // current default-branch tip every time it is handed out, so the
        // Agent verifies what Tasks actually delivered rather than the tree
        // as of Project creation.
        let manager = WorkspaceManager::new(workspaces_root.to_path_buf());
        manager
            .refresh_detached_worktree(&checkout, &repo.default_branch)
            .await
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    } else {
        let source = match resolve_repo_source(&repo, workspaces_root).await {
            Ok(source) => source,
            Err(error) => {
                if let Err(cleanup_error) = retain_project_agent_workspace_if_current(
                    db,
                    &authority,
                    workspace.clone(),
                    Some(repo_cache.clone()),
                )
                .await
                {
                    tracing::error!(
                        project_id,
                        error = %cleanup_error,
                        "Project Agent workspace cleanup after repository-source failure failed"
                    );
                }
                return Err(error);
            }
        };
        let manager = WorkspaceManager::new(workspaces_root.to_path_buf());
        if let Err(error) = manager
            .create_detached_worktree_named(
                &source,
                project_id,
                PROJECT_AGENT_CHECKOUT_DIR,
                &repo.default_branch,
            )
            .await
        {
            if let Err(cleanup_error) = retain_project_agent_workspace_if_current(
                db,
                &authority,
                workspace.clone(),
                Some(repo_cache.clone()),
            )
            .await
            {
                tracing::error!(
                    project_id,
                    error = %cleanup_error,
                    "Project Agent workspace cleanup after checkout creation failure failed"
                );
            }
            return Err(ServiceError::invalid_operation(error.to_string()));
        }
    }
    retain_project_agent_workspace_if_current(db, &authority, workspace, Some(repo_cache)).await
}

async fn retain_project_agent_workspace_if_current(
    db: &SqliteDb,
    authority: &ProjectAgentWorkspaceAuthority,
    workspace: PathBuf,
    repo_cache: Option<PathBuf>,
) -> Result<Option<PathBuf>> {
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let current =
        query_project_agent_workspace_authority(&mut *transaction, &authority.project_id).await?;
    if current
        .as_ref()
        .is_some_and(|current| authority.same_generation_and_repository(current))
    {
        transaction.commit().await?;
        return Ok(Some(workspace));
    }

    // Only quarantine a workspace carrying this exact old generation's
    // marker. A replacement Project may already have claimed the same path;
    // in that case its marker makes the cleanup a no-op. A same-generation
    // repository change preserves notes and discards only the checkout.
    let stale_workspace = if current
        .as_ref()
        .is_some_and(|current| authority.same_generation(current))
    {
        quarantine_project_agent_checkout_if_repository_matches(
            &workspace,
            &authority.repository_marker_contents(),
        )
        .await?
    } else {
        quarantine_project_agent_workspace_if_marker_matches(
            &workspace,
            &authority.generation_marker_contents(),
        )
        .await?
    };

    // A cache is keyed by repository identity. Keep it whenever a live Repo
    // row with that identity exists, including a replacement Project that
    // reused the same repository ID; otherwise the captured generation owns
    // the cache and it can be quarantined with this cleanup.
    let stale_cache = if let (Some(repo_cache), Some(repository_identity)) = (
        repo_cache.as_deref(),
        authority.repository_identity.as_ref(),
    ) {
        let repo_live = sqlx::query_scalar::<_, i64>("SELECT 1 FROM repo WHERE id = ? LIMIT 1")
            .bind(&repository_identity.0)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
        if repo_live {
            None
        } else {
            quarantine_project_agent_cache(repo_cache).await?
        }
    } else {
        None
    };
    transaction.commit().await?;

    for path in [stale_workspace, stale_cache].into_iter().flatten() {
        remove_quarantined_project_agent_path(&path).await?;
    }
    Ok(None)
}

/// A repository clone can finish after Project deletion's DB transaction has
/// captured its repository rows. If the following Workspace admission then
/// loses the Project FK race, the worktree cleanup above is not enough: this
/// cache was created after the DB snapshot and would otherwise survive forever.
/// Remove it only after both authorities are gone, so a normal unique-row
/// admission failure cannot destroy a cache still owned by a live Project.
pub(super) async fn cleanup_repo_cache_if_authority_gone(
    db: &SqliteDb,
    project_id: &str,
    repo_id: &str,
    repo_cache: &Path,
) {
    // Serialize the authority check with Project/Repo admission. A plain
    // read followed by remove could delete a replacement repository cache
    // claimed in the gap between those operations.
    let mut transaction = match db::begin_immediate(db.pool()).await {
        Ok(transaction) => transaction,
        Err(error) => {
            tracing::error!(
                project_id,
                repo_id,
                error = %error,
                "could not acquire database guard before repository-cache cleanup"
            );
            return;
        }
    };
    let project_exists =
        match sqlx::query_scalar::<_, i64>("SELECT 1 FROM project WHERE id = ? LIMIT 1")
            .bind(project_id)
            .fetch_optional(&mut *transaction)
            .await
        {
            Ok(row) => row.is_some(),
            Err(error) => {
                tracing::error!(
                    project_id,
                    repo_id,
                    error = %error,
                    "could not verify Project authority before repository-cache cleanup"
                );
                return;
            }
        };
    let repo_exists = match sqlx::query_scalar::<_, i64>("SELECT 1 FROM repo WHERE id = ? LIMIT 1")
        .bind(repo_id)
        .fetch_optional(&mut *transaction)
        .await
    {
        Ok(row) => row.is_some(),
        Err(error) => {
            tracing::error!(
                project_id,
                repo_id,
                error = %error,
                "could not verify repository authority before repository-cache cleanup"
            );
            return;
        }
    };
    if project_exists && repo_exists {
        if let Err(error) = transaction.commit().await {
            tracing::error!(
                project_id,
                repo_id,
                error = %error,
                "could not release database guard after repository-cache cleanup check"
            );
        }
        return;
    }

    let quarantine = match quarantine_project_agent_cache(repo_cache).await {
        Ok(quarantine) => quarantine,
        Err(error) => {
            tracing::error!(
                project_id,
                repo_id,
                path = %repo_cache.display(),
                error = %error,
                "repository-cache quarantine after Project deletion failed"
            );
            return;
        }
    };
    if let Err(error) = transaction.commit().await {
        if let Some(quarantine) = quarantine.as_deref() {
            if let Err(restore_error) = tokio::fs::rename(quarantine, repo_cache).await {
                tracing::error!(
                    project_id,
                    repo_id,
                    path = %repo_cache.display(),
                    error = %restore_error,
                    "repository-cache restore after database-guard failure also failed"
                );
            }
        }
        tracing::error!(
            project_id,
            repo_id,
            error = %error,
            "could not commit database guard after repository-cache quarantine"
        );
        return;
    }
    if let Some(quarantine) = quarantine {
        if let Err(error) = remove_quarantined_project_agent_path(&quarantine).await {
            tracing::error!(
                project_id,
                repo_id,
                path = %quarantine.display(),
                error = %error,
                "repository-cache removal after Project deletion failed"
            );
        }
    }
}

/// The manifest that stops cargo's upward workspace search at the
/// verification workspace root.
///
/// Cargo resolves a package's workspace by climbing from its manifest to the
/// nearest parent `Cargo.toml` that declares `[workspace]`. A Project Agent's
/// checkout lives inside the Forge data directory, and a development server
/// keeps that directory inside Forge's own repository — so a build in
/// `checkout/` climbed into Forge's workspace and was refused as an unlisted
/// member ("current package believes it's in a workspace when it's not").
/// This root manifest is the nearest workspace instead, with the checkout as
/// its only member. A checkout that declares its own `[workspace]` never
/// reaches it; a checkout without a `Cargo.toml` never consults it.
pub const VERIFICATION_WORKSPACE_BOUNDARY_MANIFEST: &str =
    "# Forge verification workspace boundary.
# Cargo searches upward from checkout/ for a workspace root; this manifest is
# that root, so a build there never resolves against whatever repository
# holds this data directory. Do not add packages here.
[workspace]
members = [\"checkout\"]
resolver = \"2\"
";

/// Write the workspace-boundary manifest at `workspace` unless one exists.
pub(crate) fn write_verification_workspace_boundary(workspace: &Path) -> Result<()> {
    let manifest = workspace.join("Cargo.toml");
    if manifest.exists() {
        return Ok(());
    }
    std::fs::write(&manifest, VERIFICATION_WORKSPACE_BOUNDARY_MANIFEST).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Project Agent workspace boundary could not be written: {error}"
        ))
    })
}

/// The one directory a Project Agent may write to, inside its workspace.
pub const PROJECT_AGENT_DOCS_DIR: &str = "forge";
/// The read-and-run copy of the repository inside that workspace. The
/// runtime runs the Agent's verification commands inside this directory.
pub const PROJECT_AGENT_CHECKOUT_DIR: &str = forge_agent_host::PROJECT_VERIFICATION_CHECKOUT_DIR;

/// Stored inside a Project Agent workspace before any notes or checkout files
/// are created.  The directory name is intentionally still the public
/// Project ID, but the marker makes that path an ownership claim for one
/// immutable Project generation rather than for every future row with the
/// same ID.
const PROJECT_AGENT_GENERATION_MARKER: &str = ".forge-project-agent-generation";
/// Stores only a non-secret digest of the repository snapshot. Raw remote
/// URLs and local paths never enter the Agent-visible workspace.
const PROJECT_AGENT_REPOSITORY_MARKER: &str = ".forge-project-agent-repository";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectAgentWorkspaceAuthority {
    project_id: String,
    project_created_at: String,
    provisioning_operation_id: Option<String>,
    primary_repo_id: Option<String>,
    /// `(id, remote_url, local_path, default_branch)` for the exact primary
    /// repository snapshot used to build the checkout.
    repository_identity: Option<(String, String, Option<String>, String)>,
}

impl ProjectAgentWorkspaceAuthority {
    /// This marker is deliberately generation-only. Project.version and the
    /// selected repository can change inside one Project generation without
    /// invalidating the durable `forge/` notes.
    fn generation_marker_contents(&self) -> String {
        serde_json::to_string(&(
            &self.project_id,
            &self.project_created_at,
            &self.provisioning_operation_id,
        ))
        .expect("Project Agent workspace authority is serializable")
    }

    fn repository_marker_contents(&self) -> String {
        let serialized = serde_json::to_string(&self.repository_identity)
            .expect("Project Agent repository authority is serializable");
        let mut digest = Sha256::new();
        digest.update(serialized.as_bytes());
        hex::encode(digest.finalize())
    }

    fn same_generation(&self, other: &Self) -> bool {
        self.project_id == other.project_id
            && self.project_created_at == other.project_created_at
            && match (
                self.provisioning_operation_id.as_deref(),
                other.provisioning_operation_id.as_deref(),
            ) {
                (Some(left), Some(right)) => left == right,
                // Legacy rows without a provisioning operation have no
                // durable generation token; created_at is their immutable
                // generation fallback. Do not fold mutable Project.version
                // into the workspace marker.
                (None, None) => true,
                _ => false,
            }
    }

    fn same_repository(&self, other: &Self) -> bool {
        self.primary_repo_id == other.primary_repo_id
            && self.repository_identity == other.repository_identity
    }

    fn same_generation_and_repository(&self, other: &Self) -> bool {
        self.same_generation(other) && self.same_repository(other)
    }
}

type ProjectAgentWorkspaceAuthorityRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

async fn query_project_agent_workspace_authority<'e, E>(
    executor: E,
    project_id: &str,
) -> Result<Option<ProjectAgentWorkspaceAuthority>>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query_as::<_, ProjectAgentWorkspaceAuthorityRow>(
        "SELECT p.id, p.created_at, p.primary_repo_id,
                operation.id,
                repo.id, repo.remote_url, repo.local_path, repo.default_branch
         FROM project p
         LEFT JOIN project_provisioning_operation operation
           ON operation.project_id = p.id
         LEFT JOIN repo
           ON repo.id = p.primary_repo_id AND repo.project_id = p.id
         WHERE p.id = ?",
    )
    .bind(project_id)
    .fetch_optional(executor)
    .await?;

    Ok(row.map(
        |(
            project_id,
            project_created_at,
            primary_repo_id,
            provisioning_operation_id,
            repository_id,
            remote_url,
            local_path,
            default_branch,
        )| ProjectAgentWorkspaceAuthority {
            project_id,
            project_created_at,
            provisioning_operation_id,
            primary_repo_id,
            repository_identity: repository_id.zip(remote_url).zip(default_branch).map(
                |((repository_id, remote_url), default_branch)| {
                    (repository_id, remote_url, local_path, default_branch)
                },
            ),
        },
    ))
}

async fn capture_project_agent_workspace_authority(
    db: &SqliteDb,
    project_id: &str,
) -> Result<Option<(ProjectAgentWorkspaceAuthority, Option<db::Repo>)>> {
    let Some(authority) = query_project_agent_workspace_authority(db.pool(), project_id).await?
    else {
        return Ok(None);
    };
    let repo = match authority.primary_repo_id.as_deref() {
        Some(repo_id) => match RepoRepo::get_by_id(db, repo_id).await? {
            Some(repo) if repo.project_id == authority.project_id => Some(repo),
            Some(_) => {
                return Err(ServiceError::RepoMismatch {
                    project_id: authority.project_id.clone(),
                });
            }
            None => None,
        },
        None => None,
    };
    Ok(Some((authority, repo)))
}

async fn quarantine_project_agent_workspace_if_marker_differs(
    workspace: &Path,
    expected_marker: &str,
) -> Result<Option<PathBuf>> {
    let metadata = match tokio::fs::symlink_metadata(workspace).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect Project Agent workspace {}: {error}",
                workspace.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(ServiceError::invalid_operation(format!(
            "Project Agent workspace is a symlink: {}",
            workspace.display()
        )));
    }

    let marker_path = workspace.join(PROJECT_AGENT_GENERATION_MARKER);
    let marker_matches = if metadata.is_dir() {
        match tokio::fs::read_to_string(&marker_path).await {
            Ok(marker) => marker == expected_marker,
            // A markerless workspace predates generation stamping. The
            // current authority owns the path at this point, so adopt it and
            // stamp it rather than deleting durable Project Agent notes.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(ServiceError::invalid_operation(format!(
                    "read Project Agent workspace generation {}: {error}",
                    marker_path.display()
                )));
            }
        }
    } else {
        false
    };
    if marker_matches {
        return Ok(None);
    }

    let parent = workspace.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "Project Agent workspace has no parent: {}",
            workspace.display()
        ))
    })?;
    let name = workspace
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("project-agent");
    let quarantine = parent.join(format!(
        ".{name}.forge-project-agent-stale-{}",
        new_uuid_v4()
    ));
    tokio::fs::rename(workspace, &quarantine)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "quarantine stale Project Agent workspace {}: {error}",
                workspace.display()
            ))
        })?;
    Ok(Some(quarantine))
}

async fn quarantine_project_agent_workspace_if_marker_matches(
    workspace: &Path,
    expected_marker: &str,
) -> Result<Option<PathBuf>> {
    let metadata = match tokio::fs::symlink_metadata(workspace).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect Project Agent workspace {}: {error}",
                workspace.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(ServiceError::invalid_operation(format!(
            "Project Agent workspace is a symlink: {}",
            workspace.display()
        )));
    }
    if !metadata.is_dir() {
        return Ok(None);
    }
    let marker =
        match tokio::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER)).await {
            Ok(marker) => marker,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(ServiceError::invalid_operation(format!(
                    "read Project Agent workspace authority {}: {error}",
                    workspace.display()
                )));
            }
        };
    if marker != expected_marker {
        return Ok(None);
    }
    let parent = workspace.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "Project Agent workspace has no parent: {}",
            workspace.display()
        ))
    })?;
    let name = workspace
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("project-agent");
    let quarantine = parent.join(format!(
        ".{name}.forge-project-agent-stale-{}",
        new_uuid_v4()
    ));
    tokio::fs::rename(workspace, &quarantine)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "quarantine stale Project Agent workspace {}: {error}",
                workspace.display()
            ))
        })?;
    Ok(Some(quarantine))
}

/// Quarantine an unowned repository cache after the database check has proved
/// that no live Repo row still owns its exact repository ID. Caches do not
/// carry a Project-Agent generation marker, so this helper is intentionally
/// separate from marker-aware workspace adoption.
async fn quarantine_project_agent_cache(path: &Path) -> Result<Option<PathBuf>> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect repository cache {}: {error}",
                path.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Ok(None);
    }
    let parent = path.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "repository cache has no parent: {}",
            path.display()
        ))
    })?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("repo-cache");
    let quarantine = parent.join(format!(
        ".{name}.forge-project-agent-stale-{}",
        new_uuid_v4()
    ));
    tokio::fs::rename(path, &quarantine)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "quarantine repository cache {}: {error}",
                path.display()
            ))
        })?;
    Ok(Some(quarantine))
}

/// A primary repository can change within one Project generation. Preserve
/// the durable `forge/` notes, but discard only a checkout whose stamped
/// repository snapshot is no longer the current one. A markerless checkout
/// is disposable by definition: its repository origin cannot be proven, so
/// it is quarantined and rebuilt for the current repository.
async fn quarantine_project_agent_checkout_if_repository_differs(
    workspace: &Path,
    expected_repository_marker: &str,
) -> Result<Option<PathBuf>> {
    let checkout = workspace.join(PROJECT_AGENT_CHECKOUT_DIR);
    let metadata = match tokio::fs::symlink_metadata(&checkout).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect Project Agent checkout {}: {error}",
                checkout.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(ServiceError::invalid_operation(format!(
            "Project Agent checkout is a symlink: {}",
            checkout.display()
        )));
    }
    if !metadata.is_dir() {
        return Ok(None);
    }
    let repository_marker =
        match tokio::fs::read_to_string(workspace.join(PROJECT_AGENT_REPOSITORY_MARKER)).await {
            Ok(marker) => marker,
            // The workspace marker may be adopted from a legacy markerless
            // directory, but its checkout is disposable and unproven. Let
            // the mismatch path below quarantine it for a clean rebuild.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => {
                return Err(ServiceError::invalid_operation(format!(
                    "read Project Agent repository authority {}: {error}",
                    workspace.join(PROJECT_AGENT_REPOSITORY_MARKER).display()
                )));
            }
        };
    if repository_marker == expected_repository_marker {
        return Ok(None);
    }
    let parent = checkout.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "Project Agent checkout has no parent: {}",
            checkout.display()
        ))
    })?;
    let name = checkout
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("checkout");
    let quarantine = parent.join(format!(
        ".{name}.forge-project-agent-stale-{}",
        new_uuid_v4()
    ));
    tokio::fs::rename(&checkout, &quarantine)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "quarantine stale Project Agent checkout {}: {error}",
                checkout.display()
            ))
        })?;
    Ok(Some(quarantine))
}

/// A stale creator may finish after a same-generation repository change has
/// already reserved and rebuilt the checkout. In that ordering it must only
/// quarantine a checkout still stamped with its own repository snapshot; a
/// differing marker belongs to the newer repository admission and is left
/// untouched.
async fn quarantine_project_agent_checkout_if_repository_matches(
    workspace: &Path,
    expected_repository_marker: &str,
) -> Result<Option<PathBuf>> {
    let checkout = workspace.join(PROJECT_AGENT_CHECKOUT_DIR);
    let metadata = match tokio::fs::symlink_metadata(&checkout).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect Project Agent checkout {}: {error}",
                checkout.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(ServiceError::invalid_operation(format!(
            "Project Agent checkout is a symlink: {}",
            checkout.display()
        )));
    }
    if !metadata.is_dir() {
        return Ok(None);
    }
    let repository_marker =
        match tokio::fs::read_to_string(workspace.join(PROJECT_AGENT_REPOSITORY_MARKER)).await {
            Ok(marker) => marker,
            // Markerless legacy checkouts have no provenance that permits a
            // destructive cleanup, so preserve them conservatively.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(ServiceError::invalid_operation(format!(
                    "read Project Agent repository authority {}: {error}",
                    workspace.join(PROJECT_AGENT_REPOSITORY_MARKER).display()
                )));
            }
        };
    if repository_marker != expected_repository_marker {
        return Ok(None);
    }
    let parent = checkout.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "Project Agent checkout has no parent: {}",
            checkout.display()
        ))
    })?;
    let name = checkout
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("checkout");
    let quarantine = parent.join(format!(
        ".{name}.forge-project-agent-stale-{}",
        new_uuid_v4()
    ));
    tokio::fs::rename(&checkout, &quarantine)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "quarantine stale Project Agent checkout {}: {error}",
                checkout.display()
            ))
        })?;
    Ok(Some(quarantine))
}

async fn remove_quarantined_project_agent_path(path: &Path) -> Result<()> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect Project Agent quarantine {}: {error}",
                path.display()
            )));
        }
    };
    let result = if metadata.is_dir() && !metadata.file_type().is_symlink() {
        tokio::fs::remove_dir_all(path).await
    } else {
        tokio::fs::remove_file(path).await
    };
    result.map_err(|error| {
        ServiceError::invalid_operation(format!(
            "remove Project Agent quarantine {}: {error}",
            path.display()
        ))
    })
}

/// Restore a path moved aside by a reservation when the DB admission cannot
/// commit. The original name is restored only while it is still free; a
/// concurrent creator's occupied path is never overwritten, and the unique
/// quarantine remains available for recovery.
async fn restore_project_agent_quarantine_if_path_free(
    original: &Path,
    quarantine: Option<PathBuf>,
) {
    let Some(quarantine) = quarantine else {
        return;
    };
    let original_is_free = match tokio::fs::symlink_metadata(original).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Ok(_) => false,
        Err(error) => {
            tracing::error!(
                original = %original.display(),
                quarantine = %quarantine.display(),
                error = %error,
                "failed to inspect Project Agent path after reservation rollback"
            );
            false
        }
    };
    if original_is_free {
        if let Err(error) = tokio::fs::rename(&quarantine, original).await {
            tracing::error!(
                original = %original.display(),
                quarantine = %quarantine.display(),
                error = %error,
                "failed to restore Project Agent path after reservation rollback"
            );
        }
    } else {
        tracing::error!(
            original = %original.display(),
            quarantine = %quarantine.display(),
            "preserving Project Agent quarantine because the original path is occupied"
        );
    }
}

/// Move a replacement workspace created after a whole-workspace quarantine
/// aside before restoring the original tree. The generation marker proves it
/// is the replacement this reservation attempted to stamp; a markerless
/// partial is also safe to move because the caller has already reserved the
/// original path under the DB writer lock.
async fn quarantine_partial_project_agent_workspace(
    workspace: &Path,
    expected_marker: &str,
) -> Result<Option<PathBuf>> {
    let metadata = match tokio::fs::symlink_metadata(workspace).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect partial Project Agent workspace {}: {error}",
                workspace.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Ok(None);
    }
    if !metadata.is_dir() {
        return Ok(None);
    }
    match tokio::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER)).await {
        Ok(marker) if marker != expected_marker => return Ok(None),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "read partial Project Agent workspace generation {}: {error}",
                workspace.join(PROJECT_AGENT_GENERATION_MARKER).display()
            )));
        }
    }
    let parent = workspace.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "partial Project Agent workspace has no parent: {}",
            workspace.display()
        ))
    })?;
    let name = workspace
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("project-agent");
    let quarantine = parent.join(format!(
        ".{name}.forge-project-agent-reservation-failed-{}",
        new_uuid_v4()
    ));
    tokio::fs::rename(workspace, &quarantine)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "quarantine partial Project Agent workspace {}: {error}",
                workspace.display()
            ))
        })?;
    Ok(Some(quarantine))
}

async fn rollback_project_agent_reservation(
    workspace: &Path,
    expected_marker: &str,
    stale_workspace: Option<PathBuf>,
    stale_checkout: Option<PathBuf>,
) {
    let partial_workspace = if stale_workspace.is_some() {
        match quarantine_partial_project_agent_workspace(workspace, expected_marker).await {
            Ok(partial_workspace) => partial_workspace,
            Err(error) => {
                tracing::error!(
                    workspace = %workspace.display(),
                    error = %error,
                    "failed to quarantine partial Project Agent replacement during rollback"
                );
                None
            }
        }
    } else {
        None
    };
    restore_project_agent_quarantine_if_path_free(workspace, stale_workspace).await;
    restore_project_agent_quarantine_if_path_free(
        &workspace.join(PROJECT_AGENT_CHECKOUT_DIR),
        stale_checkout,
    )
    .await;
    if let Some(partial_workspace) = partial_workspace {
        // The reservation has not admitted this replacement. Keep the
        // quarantine for recovery rather than deleting bytes on a rollback.
        tracing::warn!(
            workspace = %workspace.display(),
            quarantine = %partial_workspace.display(),
            "preserved partial Project Agent replacement quarantine after reservation rollback"
        );
    }
}

async fn reserve_project_agent_workspace_path(
    db: &SqliteDb,
    authority: &ProjectAgentWorkspaceAuthority,
    workspace: &Path,
) -> Result<bool> {
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let Some(current) =
        query_project_agent_workspace_authority(&mut *transaction, &authority.project_id).await?
    else {
        transaction.commit().await?;
        return Ok(false);
    };
    if !authority.same_generation_and_repository(&current) {
        transaction.commit().await?;
        return Ok(false);
    }

    let expected_generation_marker = authority.generation_marker_contents();
    let stale_workspace = quarantine_project_agent_workspace_if_marker_differs(
        workspace,
        &expected_generation_marker,
    )
    .await?;
    let stale_checkout = if stale_workspace.is_none() {
        quarantine_project_agent_checkout_if_repository_differs(
            workspace,
            &authority.repository_marker_contents(),
        )
        .await?
    } else {
        None
    };
    if let Err(error) = tokio::fs::create_dir_all(workspace).await {
        drop(transaction);
        rollback_project_agent_reservation(
            workspace,
            &expected_generation_marker,
            stale_workspace,
            stale_checkout,
        )
        .await;
        return Err(ServiceError::Git(git::GitError::Io(error)));
    }
    if let Err(error) = tokio::fs::write(
        workspace.join(PROJECT_AGENT_GENERATION_MARKER),
        &expected_generation_marker,
    )
    .await
    {
        drop(transaction);
        rollback_project_agent_reservation(
            workspace,
            &expected_generation_marker,
            stale_workspace,
            stale_checkout,
        )
        .await;
        return Err(ServiceError::Git(git::GitError::Io(error)));
    }
    if let Err(error) = tokio::fs::write(
        workspace.join(PROJECT_AGENT_REPOSITORY_MARKER),
        authority.repository_marker_contents(),
    )
    .await
    {
        drop(transaction);
        rollback_project_agent_reservation(
            workspace,
            &expected_generation_marker,
            stale_workspace,
            stale_checkout,
        )
        .await;
        return Err(ServiceError::Git(git::GitError::Io(error)));
    }
    if let Err(error) = transaction.commit().await {
        rollback_project_agent_reservation(
            workspace,
            &expected_generation_marker,
            stale_workspace,
            stale_checkout,
        )
        .await;
        return Err(error.into());
    }

    for stale in [stale_workspace, stale_checkout].into_iter().flatten() {
        remove_quarantined_project_agent_path(&stale).await?;
    }
    Ok(true)
}

/// The filesystem path a repository's remote points at, when that remote is
/// a local path and the path is missing. A URL-shaped remote is never a local
/// path and is left to git.
fn missing_local_repo_source(remote_url: &str) -> Option<&str> {
    let remote = remote_url.trim();
    let path = remote.strip_prefix("file://").unwrap_or(remote);
    let is_local_path = path.starts_with('/') || path.starts_with('.') || path.starts_with('~');
    (is_local_path && !Path::new(path).exists()).then_some(path)
}

pub(super) async fn resolve_repo_source(
    repo: &db::Repo,
    workspace_root: &std::path::Path,
) -> Result<String> {
    if let Some(local_path) = repo
        .local_path
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
    {
        if Path::new(local_path).exists() {
            return Ok(local_path.to_owned());
        }
    }
    let clone_path = workspace_root.join(".repos").join(&repo.id);
    if !clone_path.exists() {
        // A remote that is itself a filesystem path can only be cloned while
        // that path is there. Letting git fail instead reports a command
        // error, which callers classify as potentially transient and retry on
        // every scan -- but a source directory that is gone is not coming
        // back on its own, and the Task should park saying exactly that.
        if let Some(missing) = missing_local_repo_source(&repo.remote_url) {
            return Err(ServiceError::invalid_operation(format!(
                "repo source path does not exist: {missing}"
            )));
        }
        tokio::fs::create_dir_all(
            clone_path
                .parent()
                .ok_or_else(|| ServiceError::invalid_operation("repo cache path has no parent"))?,
        )
        .await
        .map_err(|error| ServiceError::Git(git::GitError::Io(error)))?;
        let output = Command::new("git")
            .args(["clone", &repo.remote_url, &clone_path.to_string_lossy()])
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .await
            .map_err(|error| ServiceError::Git(git::GitError::Io(error)))?;
        if !output.status.success() {
            return Err(ServiceError::Git(git::GitError::CommandFailed {
                command: format!("git clone {} {}", repo.remote_url, clone_path.display()),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            }));
        }
    }
    Ok(clone_path.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests;
