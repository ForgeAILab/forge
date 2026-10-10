use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use api_types::{
    CreateRepoLocationRequest, DaemonRepoLocationKind, RepoLocationProbe, RepoLocationVerifyParams,
    UpdateRepoLocationRequest,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use db::{
    new_uuid_v4, now_rfc3339, CreateRepoLocation, DaemonRepo, DbError, Page, PageRequest,
    ProjectMemberRepo, Repo, RepoLocation, RepoLocationKind, RepoLocationOwnerKind,
    RepoLocationRepo, RepoLocationStatus, RepoRepo, Runtime, RuntimeRepo, SortBy, SortOrder,
    SqliteDb, TaskRepo, UpdateRepoLocation,
};
use serde::{Deserialize, Serialize};

use crate::{
    daemon_transport::{
        workspace_client::{DaemonWorkspaceClient, WorkspaceClientError},
        DaemonConnectionRegistry,
    },
    Result, ServiceError,
};

#[derive(Debug)]
pub struct LocationVerification {
    pub status: RepoLocationStatus,
    pub last_error: Option<String>,
}

impl LocationVerification {
    pub fn ready() -> Self {
        Self {
            status: RepoLocationStatus::Ready,
            last_error: None,
        }
    }

    pub fn invalid(error: impl Into<String>) -> Self {
        Self {
            status: RepoLocationStatus::Invalid,
            last_error: Some(error.into()),
        }
    }

    pub fn unavailable(error: impl Into<String>) -> Self {
        Self {
            status: RepoLocationStatus::Unavailable,
            last_error: Some(error.into()),
        }
    }
}

#[async_trait]
pub trait DaemonLocationVerifier: Send + Sync {
    /// Verify on the owning runtime. For shared_mount, ready also requires
    /// reading back a server-written probe at the same path.
    async fn verify(
        &self,
        repo: &Repo,
        location: &RepoLocation,
        runtime: &Runtime,
    ) -> Result<LocationVerification>;
}

pub struct RemoteDaemonLocationVerifier {
    client: DaemonWorkspaceClient,
    worktree_root: PathBuf,
}

impl RemoteDaemonLocationVerifier {
    pub fn new(registry: Arc<DaemonConnectionRegistry>, worktree_root: PathBuf) -> Self {
        Self {
            client: DaemonWorkspaceClient::new(registry),
            worktree_root,
        }
    }
}

#[async_trait]
impl DaemonLocationVerifier for RemoteDaemonLocationVerifier {
    async fn verify(
        &self,
        repo: &Repo,
        location: &RepoLocation,
        runtime: &Runtime,
    ) -> Result<LocationVerification> {
        if location.daemon_id.as_deref() != Some(runtime.daemon_id.as_str())
            || location.runtime_id.as_deref() != Some(runtime.id.as_str())
        {
            return Ok(LocationVerification::invalid(api_types::WRONG_OWNER));
        }
        if location.kind != RepoLocationKind::ManagedClone
            && !within_runtime_root(&location.path, &runtime.workspace_root)
        {
            return Ok(LocationVerification::invalid(
                api_types::OUTSIDE_WORKSPACE_ROOT,
            ));
        }
        let mut probe = if location.kind == RepoLocationKind::SharedMount {
            if location.owner_kind != RepoLocationOwnerKind::Server {
                return Ok(LocationVerification::invalid(
                    "shared_mount_requires_server_owner",
                ));
            }
            match SharedMountProbe::create(&self.worktree_root) {
                Ok(probe) => Some(probe),
                Err(error) => {
                    return Ok(LocationVerification::unavailable(format!(
                        "shared_mount_probe_write_failed: {error}"
                    )))
                }
            }
        } else {
            None
        };
        let response = self
            .client
            .verify_location(
                &runtime.daemon_id,
                RepoLocationVerifyParams {
                    repo_location_id: location.id.clone(),
                    daemon_id: runtime.daemon_id.clone(),
                    runtime_id: runtime.id.clone(),
                    path: location.path.clone(),
                    kind: match location.kind {
                        RepoLocationKind::PrimaryCheckout => {
                            DaemonRepoLocationKind::PrimaryCheckout
                        }
                        RepoLocationKind::ManagedClone => DaemonRepoLocationKind::ManagedClone,
                        RepoLocationKind::SharedMount => DaemonRepoLocationKind::SharedMount,
                    },
                    default_branch: repo.default_branch.clone(),
                    remote_url: repo.remote_url.clone().filter(|url| !url.is_empty()),
                    // The version this verification is stored as: the caller
                    // writes the row next, compare-and-set on the present
                    // version, which adds one. The owner keeps this number
                    // and compares a queue claim's frozen location
                    // generation with it; sending the present version left
                    // the owner one behind after every stored verification,
                    // so it refused every later claim `foreign_owner`.
                    expected_version: location.version + 1,
                    probe: probe.as_ref().map(|probe| RepoLocationProbe {
                        path: probe.path.to_string_lossy().into_owned(),
                        content: probe.content.clone(),
                    }),
                },
            )
            .await;
        let verification = match response {
            Ok(result) if result.repo_location_id != location.id => {
                LocationVerification::invalid("wrong_repo_location")
            }
            Ok(result) if result.default_branch_sha.is_empty() => {
                LocationVerification::invalid("default_branch_not_found")
            }
            Ok(result)
                if probe.as_ref().is_some_and(|probe| {
                    result.probe_content.as_deref() != Some(probe.content.as_str())
                }) =>
            {
                LocationVerification::invalid("shared_mount_probe_mismatch")
            }
            Ok(_) => LocationVerification::ready(),
            Err(WorkspaceClientError::Transport(
                error @ ServiceError::DaemonUpgradeRequired { .. },
            )) => LocationVerification {
                status: location.status.clone(),
                last_error: Some(error.to_string()),
            },
            Err(WorkspaceClientError::Transport(ServiceError::DaemonUnavailable { .. })) => {
                LocationVerification::unavailable(api_types::DAEMON_UNAVAILABLE)
            }
            Err(WorkspaceClientError::Transport(ServiceError::DaemonTimeout { .. })) => {
                LocationVerification::unavailable(api_types::DAEMON_TIMEOUT)
            }
            Err(WorkspaceClientError::Transport(ServiceError::InvalidOperation { message }))
                if message.starts_with(api_types::DAEMON_PROTOCOL_INCOMPATIBLE) =>
            {
                LocationVerification::unavailable("workspace_protocol_missing")
            }
            Err(WorkspaceClientError::Transport(error)) => {
                LocationVerification::unavailable(error.to_string())
            }
            Err(WorkspaceClientError::Daemon(error)) => match error.code.as_str() {
                api_types::DAEMON_UNAVAILABLE
                | api_types::DAEMON_TIMEOUT
                | "disconnected"
                | "timeout" => {
                    LocationVerification::unavailable(format!("{}: {}", error.code, error.message))
                }
                api_types::OUTSIDE_WORKSPACE_ROOT => LocationVerification::invalid(error.code),
                "version_conflict" => return Err(DbError::VersionConflict.into()),
                _ => LocationVerification::invalid(format!("{}: {}", error.code, error.message)),
            },
        };
        if let Some(probe) = probe.as_mut() {
            if let Err(error) = probe.remove() {
                return Ok(LocationVerification::unavailable(format!(
                    "shared_mount_probe_cleanup_failed: {error}"
                )));
            }
        }
        Ok(verification)
    }
}

struct SharedMountProbe {
    path: PathBuf,
    content: String,
    removed: bool,
}

impl SharedMountProbe {
    fn create(root: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(root)?;
        let root = root.canonicalize()?;
        let path = root.join(format!(".forge-location-probe-{}", new_uuid_v4()));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut probe = Self {
            path,
            content: format!("forge-shared-mount:{}", new_uuid_v4()),
            removed: false,
        };
        if let Err(error) = file
            .write_all(probe.content.as_bytes())
            .and_then(|_| file.sync_all())
        {
            let _ = probe.remove();
            return Err(error);
        }
        Ok(probe)
    }

    fn remove(&mut self) -> std::io::Result<()> {
        if !self.removed {
            match std::fs::remove_file(&self.path) {
                Ok(()) => self.removed = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => self.removed = true,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

impl Drop for SharedMountProbe {
    fn drop(&mut self) {
        if let Err(error) = self.remove() {
            tracing::warn!(path = %self.path.display(), %error, "shared-mount probe cleanup failed");
        }
    }
}

#[derive(Clone)]
pub struct RepoLocationService {
    db: Arc<SqliteDb>,
    daemon_verifier: Arc<dyn DaemonLocationVerifier>,
}

impl RepoLocationService {
    pub fn new(db: Arc<SqliteDb>, daemon_verifier: Arc<dyn DaemonLocationVerifier>) -> Self {
        Self {
            db,
            daemon_verifier,
        }
    }

    pub async fn register(
        &self,
        repo_id: &str,
        request: CreateRepoLocationRequest,
        user_id: &str,
        is_admin: bool,
    ) -> Result<RepoLocation> {
        self.authorized_repo(repo_id, user_id, is_admin).await?;
        if request.path.trim().is_empty() || request.path.contains('\0') {
            return Err(ServiceError::Domain(
                "path must be nonempty and contain no NUL".to_owned(),
            ));
        }
        let remote_verification = request.owner_kind == api_types::RepoLocationOwnerKind::Daemon
            || request.kind == api_types::RepoLocationKind::SharedMount;
        if request.kind == api_types::RepoLocationKind::SharedMount
            && request.owner_kind != api_types::RepoLocationOwnerKind::Server
        {
            return Err(ServiceError::Domain(
                "shared_mount locations must be server-owned".to_owned(),
            ));
        }
        if remote_verification {
            self.visible_runtime(
                request.daemon_id.as_deref(),
                request.runtime_id.as_deref(),
                user_id,
            )
            .await?;
        } else if request.daemon_id.is_some() || request.runtime_id.is_some() {
            return Err(ServiceError::Domain(
                "server locations must omit daemon_id and runtime_id unless kind is shared_mount"
                    .to_owned(),
            ));
        }
        let now = now_rfc3339();
        let location = RepoLocationRepo::create(
            &*self.db,
            CreateRepoLocation {
                id: new_uuid_v4(),
                repo_id: repo_id.to_owned(),
                owner_kind: match request.owner_kind {
                    api_types::RepoLocationOwnerKind::Server => RepoLocationOwnerKind::Server,
                    api_types::RepoLocationOwnerKind::Daemon => RepoLocationOwnerKind::Daemon,
                },
                daemon_id: request.daemon_id,
                runtime_id: request.runtime_id,
                path: request.path,
                kind: match request.kind {
                    api_types::RepoLocationKind::PrimaryCheckout => {
                        RepoLocationKind::PrimaryCheckout
                    }
                    api_types::RepoLocationKind::ManagedClone => RepoLocationKind::ManagedClone,
                    api_types::RepoLocationKind::SharedMount => RepoLocationKind::SharedMount,
                },
                is_default: false,
                status: RepoLocationStatus::Unverified,
                last_verified_at: None,
                last_error: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await?;
        let location = self
            .verify(repo_id, &location.id, location.version, user_id, is_admin)
            .await?;
        if request.is_default.unwrap_or(false) {
            self.update(
                repo_id,
                &location.id,
                UpdateRepoLocationRequest {
                    version: location.version,
                    is_default: true,
                },
                user_id,
                is_admin,
            )
            .await
        } else {
            Ok(location)
        }
    }

    pub async fn list(
        &self,
        repo_id: &str,
        page: PageRequest,
        user_id: &str,
        is_admin: bool,
    ) -> Result<Page<RepoLocation>> {
        self.authorized_repo(repo_id, user_id, is_admin).await?;
        let offset = match &page.cursor {
            None => 0,
            Some(cursor) => {
                let bytes = URL_SAFE_NO_PAD
                    .decode(cursor)
                    .map_err(|_| DbError::InvalidCursor)?;
                let cursor: LocationCursor =
                    serde_json::from_slice(&bytes).map_err(|_| DbError::InvalidCursor)?;
                if cursor.offset < 0 {
                    return Err(DbError::InvalidCursor.into());
                }
                cursor.offset
            }
        };
        let sort = match page.sort_by {
            SortBy::CreatedAt => "l.created_at",
            SortBy::UpdatedAt => "l.updated_at",
            SortBy::Id => "l.id",
            _ => {
                return Err(ServiceError::Domain(
                    "locations sort_by must be created_at, updated_at, or id".to_owned(),
                ))
            }
        };
        let order = match page.sort_order {
            SortOrder::Asc => "ASC",
            SortOrder::Desc => "DESC",
        };
        let limit = db::clamp_page_limit(Some(page.limit));
        // Apply daemon visibility before both the page boundary and the count.
        let predicate = "l.repo_id = ? AND (l.daemon_id IS NULL OR EXISTS (
            SELECT 1 FROM daemon d WHERE d.id = l.daemon_id
            AND (d.visibility = 'global' OR d.owner_id IS NULL OR d.owner_id = ?)))";
        let ids: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT l.id FROM repo_location l WHERE {predicate} ORDER BY {sort} {order}, l.id {order} LIMIT ? OFFSET ?"
        ))
        .bind(repo_id).bind(user_id).bind(limit + 1).bind(offset)
        .fetch_all(self.db.pool()).await?;
        let has_more = ids.len() > limit as usize;
        let mut items = Vec::new();
        for id in ids.into_iter().take(limit as usize) {
            if let Some(location) = RepoLocationRepo::get_by_id(&*self.db, &id).await? {
                items.push(location);
            }
        }
        let total_count = if page.include_total {
            Some(
                sqlx::query_scalar(&format!(
                    "SELECT COUNT(*) FROM repo_location l WHERE {predicate}"
                ))
                .bind(repo_id)
                .bind(user_id)
                .fetch_one(self.db.pool())
                .await?,
            )
        } else {
            None
        };
        let next_cursor = if has_more {
            let offset = offset.checked_add(limit).ok_or(DbError::InvalidCursor)?;
            Some(
                URL_SAFE_NO_PAD.encode(
                    serde_json::to_vec(&LocationCursor { offset })
                        .map_err(|_| DbError::InvalidCursor)?,
                ),
            )
        } else {
            None
        };
        Ok(Page {
            items,
            next_cursor,
            total_count,
        })
    }

    pub async fn update(
        &self,
        repo_id: &str,
        location_id: &str,
        request: UpdateRepoLocationRequest,
        user_id: &str,
        is_admin: bool,
    ) -> Result<RepoLocation> {
        self.authorized_repo(repo_id, user_id, is_admin).await?;
        self.visible_location(repo_id, location_id, user_id).await?;
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let now = now_rfc3339();
        let result = sqlx::query("UPDATE repo_location SET is_default = ?, version = version + 1, updated_at = ? WHERE id = ? AND repo_id = ? AND version = ?")
            .bind(request.is_default).bind(&now).bind(location_id).bind(repo_id).bind(request.version)
            .execute(&mut *transaction).await?;
        if result.rows_affected() != 1 {
            return Err(DbError::VersionConflict.into());
        }
        if request.is_default {
            let defaults: Vec<(String, i64)> = sqlx::query_as("SELECT id, version FROM repo_location WHERE repo_id = ? AND id <> ? AND is_default = 1")
                .bind(repo_id).bind(location_id).fetch_all(&mut *transaction).await?;
            for (id, version) in defaults {
                let result = sqlx::query("UPDATE repo_location SET is_default = 0, version = version + 1, updated_at = ? WHERE id = ? AND version = ?")
                    .bind(&now).bind(id).bind(version).execute(&mut *transaction).await?;
                if result.rows_affected() != 1 {
                    return Err(DbError::VersionConflict.into());
                }
            }
        }
        transaction.commit().await?;
        self.visible_location(repo_id, location_id, user_id).await
    }

    pub async fn verify(
        &self,
        repo_id: &str,
        location_id: &str,
        expected_version: i64,
        user_id: &str,
        is_admin: bool,
    ) -> Result<RepoLocation> {
        let repo = self.authorized_repo(repo_id, user_id, is_admin).await?;
        let location = self.visible_location(repo_id, location_id, user_id).await?;
        if location.version != expected_version {
            return Err(DbError::VersionConflict.into());
        }
        self.verify_registered_location(&repo, location).await
    }

    /// Verify one location again on behalf of the system (the merge queue
    /// asks when the owner of a default checkout refuses a claim because its
    /// record of the location differs from the server's). `Ok(None)`: the
    /// location is gone, or provisioning owns its verification right now.
    pub async fn reverify(&self, location_id: &str) -> Result<Option<RepoLocation>> {
        let provisioning: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM repo_provision_retry WHERE location_id = ?)",
        )
        .bind(location_id)
        .fetch_one(self.db.pool())
        .await
        .map_err(DbError::from)?;
        if provisioning {
            return Ok(None);
        }
        let Some(location) = RepoLocationRepo::get_by_id(&*self.db, location_id).await? else {
            return Ok(None);
        };
        let repo = RepoRepo::get_by_id(&*self.db, &location.repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", &location.repo_id))?;
        self.verify_registered_location(&repo, location)
            .await
            .map(Some)
    }

    /// Invoke after accepting the replacement daemon's command-stream handshake.
    /// Ready locations are also rechecked: the checkout or shared mount may have changed.
    pub async fn retry_verification_on_reconnect(&self, daemon_id: &str) -> Result<usize> {
        // Provisioning owns verification and full checks until its retry row
        // is removed. A delayed handshake scan must not mark that clone ready
        // or invalidate the provisioning job's location-version witness.
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM repo_location WHERE daemon_id = ?
             AND (owner_kind = 'daemon' OR kind = 'shared_mount')
             AND NOT EXISTS (SELECT 1 FROM repo_provision_retry j
                             WHERE j.location_id = repo_location.id)
             ORDER BY created_at, id",
        )
        .bind(daemon_id)
        .fetch_all(self.db.pool())
        .await?;
        let mut verified = 0;
        for id in ids {
            let result = async {
                let Some(location) = RepoLocationRepo::get_by_id(&*self.db, &id).await? else {
                    return Ok(None);
                };
                if location.daemon_id.as_deref() != Some(daemon_id) {
                    return Ok(None);
                }
                let repo = RepoRepo::get_by_id(&*self.db, &location.repo_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("repo", &location.repo_id))?;
                self.verify_registered_location(&repo, location)
                    .await
                    .map(Some)
            }
            .await;
            match result {
                Ok(Some(_)) => verified += 1,
                Ok(None) => {}
                Err(ServiceError::Db(DbError::VersionConflict)) => {
                    tracing::debug!(daemon_id, location_id = %id, "location changed during reconnect verification");
                }
                Err(error) => {
                    tracing::warn!(daemon_id, location_id = %id, %error, "reconnect location verification failed");
                }
            }
        }
        Ok(verified)
    }

    async fn verify_registered_location(
        &self,
        repo: &Repo,
        location: RepoLocation,
    ) -> Result<RepoLocation> {
        let verification = if location.owner_kind == RepoLocationOwnerKind::Daemon
            || location.kind == RepoLocationKind::SharedMount
        {
            let runtime = RuntimeRepo::get_by_id(
                &*self.db,
                location.runtime_id.as_deref().unwrap_or_default(),
            )
            .await?
            .filter(|runtime| location.daemon_id.as_deref() == Some(runtime.daemon_id.as_str()))
            .ok_or_else(|| {
                ServiceError::not_found(
                    "runtime",
                    location.runtime_id.as_deref().unwrap_or_default(),
                )
            })?;
            if location.kind != RepoLocationKind::ManagedClone
                && !runtime.workspace_root.trim().is_empty()
                && !within_runtime_root(&location.path, &runtime.workspace_root)
            {
                LocationVerification::invalid("outside_workspace_root")
            } else {
                match self.daemon_verifier.verify(repo, &location, &runtime).await {
                    Ok(result) => result,
                    Err(error @ ServiceError::DaemonUpgradeRequired { .. }) => {
                        LocationVerification {
                            status: location.status.clone(),
                            last_error: Some(error.to_string()),
                        }
                    }
                    Err(ServiceError::DaemonUnavailable { .. }) => {
                        LocationVerification::unavailable("daemon_unavailable")
                    }
                    Err(ServiceError::DaemonTimeout { .. }) => {
                        LocationVerification::unavailable("daemon_timeout")
                    }
                    Err(error) => return Err(error),
                }
            }
        } else {
            verify_server_location(repo, &location.path).await
        };
        let now = now_rfc3339();
        RepoLocationRepo::update(
            &*self.db,
            UpdateRepoLocation {
                id: location.id,
                expected_version: location.version,
                path: None,
                kind: None,
                is_default: None,
                status: Some(verification.status),
                last_verified_at: Some(Some(now.clone())),
                last_error: Some(verification.last_error),
                updated_at: now,
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn remove(
        &self,
        repo_id: &str,
        location_id: &str,
        user_id: &str,
        is_admin: bool,
    ) -> Result<()> {
        self.authorized_repo(repo_id, user_id, is_admin).await?;
        self.visible_location(repo_id, location_id, user_id).await?;
        self.check_not_in_use(location_id).await?;
        match RepoLocationRepo::delete(&*self.db, location_id).await {
            Err(DbError::VersionConflict) => {
                // Deletion rechecks inside its write transaction; name a Task
                // even when its placement appeared after our first check.
                self.check_not_in_use(location_id).await?;
                Err(DbError::VersionConflict.into())
            }
            result => result.map_err(Into::into),
        }
    }

    async fn check_not_in_use(&self, location_id: &str) -> Result<()> {
        if let Some(placement) =
            RepoLocationRepo::get_blocking_placement(&*self.db, location_id).await?
        {
            let task = TaskRepo::get_by_id(&*self.db, &placement.task_id, true).await?;
            let title = task
                .map(|task| task.title)
                .unwrap_or_else(|| placement.task_id.clone());
            return Err(ServiceError::Conflict(format!("repository location {location_id} is in use by Task {title} ({}) through placement {}", placement.task_id, placement.id)));
        }
        Ok(())
    }

    async fn authorized_repo(&self, repo_id: &str, user_id: &str, is_admin: bool) -> Result<Repo> {
        let repo = RepoRepo::get_by_id(&*self.db, repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", repo_id))?;
        if !is_admin {
            let member =
                ProjectMemberRepo::get_member(&*self.db, &repo.project_id, user_id).await?;
            if !member.is_some_and(|member| member.role == "owner" || member.role == "admin") {
                return Err(ServiceError::AuthorizationDenied {
                    message: "project owner or admin role is required".to_owned(),
                });
            }
        }
        Ok(repo)
    }

    async fn visible_location(
        &self,
        repo_id: &str,
        location_id: &str,
        user_id: &str,
    ) -> Result<RepoLocation> {
        let location = RepoLocationRepo::get_by_id(&*self.db, location_id)
            .await?
            .filter(|location| location.repo_id == repo_id)
            .ok_or_else(|| ServiceError::not_found("repo location", location_id))?;
        if let Some(daemon_id) = &location.daemon_id {
            self.visible_runtime(Some(daemon_id), location.runtime_id.as_deref(), user_id)
                .await?;
        }
        Ok(location)
    }

    async fn visible_runtime(
        &self,
        daemon_id: Option<&str>,
        runtime_id: Option<&str>,
        user_id: &str,
    ) -> Result<Runtime> {
        let (Some(daemon_id), Some(runtime_id)) = (daemon_id, runtime_id) else {
            return Err(ServiceError::Domain(
                "daemon_id and runtime_id are required".to_owned(),
            ));
        };
        DaemonRepo::get_visible(&*self.db, daemon_id, Some(user_id))
            .await?
            .ok_or_else(|| ServiceError::not_found("daemon", daemon_id))?;
        RuntimeRepo::get_by_id(&*self.db, runtime_id)
            .await?
            .filter(|runtime| runtime.daemon_id == daemon_id)
            .ok_or_else(|| ServiceError::not_found("runtime", runtime_id))
    }
}

#[derive(Deserialize, Serialize)]
struct LocationCursor {
    offset: i64,
}

// This is only an early lexical rejection. The owning daemon must resolve
// symlinks and enforce confinement on its own filesystem during verification.
fn within_runtime_root(path: &str, root: &str) -> bool {
    fn components(path: &str) -> Option<(String, Vec<String>)> {
        let windows = path.as_bytes().get(1) == Some(&b':') || path.starts_with("\\\\");
        let path = path.replace('\\', "/");
        if !path.starts_with('/')
            && !(path.as_bytes().get(1) == Some(&b':') && path.as_bytes().get(2) == Some(&b'/'))
        {
            return None;
        }
        let anchor = if path.as_bytes().get(1) == Some(&b':') {
            path.get(..2)?.to_lowercase()
        } else if path.starts_with("//") {
            "//".to_owned()
        } else {
            "/".to_owned()
        };
        let mut parts = Vec::new();
        for part in path.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    parts.pop()?;
                }
                part => parts.push(if windows {
                    part.to_lowercase()
                } else {
                    part.to_owned()
                }),
            }
        }
        Some((anchor, parts))
    }
    match (components(path), components(root)) {
        (Some((path_anchor, path)), Some((root_anchor, root))) => {
            path_anchor == root_anchor && path.starts_with(&root)
        }
        _ => false,
    }
}

pub(crate) async fn verify_server_location(
    repo: &Repo,
    location_path: &str,
) -> LocationVerification {
    let path = Path::new(location_path);
    if !path.is_absolute() {
        return LocationVerification::invalid("path_not_absolute");
    }
    match tokio::fs::metadata(path).await {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return LocationVerification::invalid("path_not_directory"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return LocationVerification::invalid("path_not_found")
        }
        Err(error) => return LocationVerification::invalid(format!("path_unreadable: {error}")),
    }
    if local_git(path, &["rev-parse", "--is-inside-work-tree"])
        .await
        .as_deref()
        != Ok("true")
    {
        return LocationVerification::invalid("not_git_work_tree");
    }
    let branch = format!("refs/heads/{}^{{commit}}", repo.default_branch);
    if local_git(
        path,
        &["rev-parse", "--verify", "--end-of-options", &branch],
    )
    .await
    .is_err()
    {
        return LocationVerification::invalid("default_branch_not_found");
    }
    let remote = match git::list_branches(path).await {
        Ok(branches) => branches.origin_url,
        Err(error) => return LocationVerification::invalid(format!("remote_read_failed: {error}")),
    };
    if let (Some(url), Some(expected)) = (remote, repo.remote_url.as_deref()) {
        if git::normalize_remote_url(&url) != git::normalize_remote_url(expected) {
            return LocationVerification::invalid("remote_mismatch");
        }
    }
    LocationVerification::ready()
}

pub(crate) async fn verify_managed_clone(repo: &Repo, path: &str) -> LocationVerification {
    let verified = verify_server_location(repo, path).await;
    if verified.status != RepoLocationStatus::Ready {
        return verified;
    }
    // A task worktree (including a migration's non-standard path) is not a
    // managed clone. Its origin may be inherited from an unrelated common dir.
    if !Path::new(path).join(".git").is_dir() {
        return LocationVerification::invalid("managed_clone_is_worktree");
    }
    match git::list_branches(Path::new(path)).await {
        Ok(branches)
            if branches
                .origin_url
                .as_deref()
                .zip(repo.remote_url.as_deref())
                .is_some_and(|(actual, expected)| {
                    git::normalize_remote_url(actual) == git::normalize_remote_url(expected)
                }) =>
        {
            verified
        }
        _ => LocationVerification::invalid("managed_clone_remote_mismatch"),
    }
}

async fn local_git(path: &Path, args: &[&str]) -> std::result::Result<String, String> {
    let output = tokio::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[cfg(test)]
mod tests {
    use db::{
        CreateProject, CreateRepo, CreateRuntime, DaemonStatus, ProjectRepo, RuntimeStatus,
        UpsertDaemon,
    };

    use super::*;
    use crate::daemon_transport::workspace_client::tests::{
        rejection, Reply, ScriptedDaemon, DAEMON_ID,
    };

    fn repo() -> Repo {
        Repo {
            id: "repo-test".to_owned(),
            project_id: "project-test".to_owned(),
            name: "repo".to_owned(),
            remote_url: Some("https://example.test/repo.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: "now".to_owned(),
            updated_at: "now".to_owned(),
        }
    }

    #[tokio::test]
    async fn placement_managed_clone_verification_rejects_worktrees_and_absent_remotes() {
        let root = tempfile::tempdir().unwrap();
        let checkout = root.path().join("clone");
        std::fs::create_dir_all(&checkout).unwrap();
        git::init(&checkout).await.unwrap();
        std::fs::write(checkout.join("README.md"), "base").unwrap();
        git::commit_all(&checkout, "base").await.unwrap();
        let repo = repo();
        assert_eq!(
            verify_managed_clone(&repo, checkout.to_str().unwrap())
                .await
                .status,
            RepoLocationStatus::Invalid
        );
        local_git(
            &checkout,
            &[
                "remote",
                "add",
                "origin",
                repo.remote_url.as_deref().unwrap(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(
            verify_managed_clone(&repo, checkout.to_str().unwrap())
                .await
                .status,
            RepoLocationStatus::Ready
        );
        let worktree = root.path().join("worktree");
        local_git(
            &checkout,
            &[
                "worktree",
                "add",
                "--detach",
                worktree.to_str().unwrap(),
                "main",
            ],
        )
        .await
        .unwrap();
        let verified = verify_managed_clone(&repo, worktree.to_str().unwrap()).await;
        assert_eq!(verified.status, RepoLocationStatus::Invalid);
        assert_eq!(
            verified.last_error.as_deref(),
            Some("managed_clone_is_worktree")
        );
    }

    #[tokio::test]
    async fn revision_two_verifier_error_preserves_location_status() {
        struct UpgradeVerifier;
        #[async_trait]
        impl DaemonLocationVerifier for UpgradeVerifier {
            async fn verify(
                &self,
                _: &Repo,
                _: &RepoLocation,
                runtime: &Runtime,
            ) -> Result<LocationVerification> {
                Err(ServiceError::DaemonUpgradeRequired {
                    daemon_id: runtime.daemon_id.clone(),
                })
            }
        }
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        let (_, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
        let location = RepoLocationRepo::get_by_id(&*db, &placement.repo_location_id)
            .await
            .unwrap()
            .unwrap();
        let repo = RepoRepo::get_by_id(&*db, &location.repo_id)
            .await
            .unwrap()
            .unwrap();
        let service = RepoLocationService::new(db.clone(), Arc::new(UpgradeVerifier));
        let verified = service
            .verify_registered_location(&repo, location.clone())
            .await
            .unwrap();
        assert_eq!(verified.status, location.status);
        assert!(verified
            .last_error
            .unwrap()
            .contains(api_types::DAEMON_UPGRADE_REQUIRED));
    }

    #[tokio::test]
    async fn server_location_verification_normalizes_remotes_and_rejects_mismatches() {
        let dir = tempfile::tempdir().unwrap();
        git::init(dir.path()).await.unwrap();
        std::fs::write(dir.path().join("README.md"), "base\n").unwrap();
        git::commit_all(dir.path(), "base").await.unwrap();
        local_git(
            dir.path(),
            &["remote", "add", "origin", "git@github.com:o/r.git"],
        )
        .await
        .unwrap();
        let mut repo = repo();
        repo.remote_url = Some("https://GITHUB.com:443/o/r/".into());
        let path = dir.path().to_str().unwrap();
        assert_eq!(
            verify_server_location(&repo, path).await.status,
            RepoLocationStatus::Ready
        );
        repo.remote_url = Some("ssh://git@github.com/other/r.git".into());
        let verified = verify_server_location(&repo, path).await;
        assert_eq!(verified.status, RepoLocationStatus::Invalid);
        assert_eq!(verified.last_error.as_deref(), Some("remote_mismatch"));

        repo.remote_url = None;
        assert_eq!(
            verify_server_location(&repo, path).await.status,
            RepoLocationStatus::Ready
        );

        let origin = dir.path().join("origin.git");
        local_git(
            dir.path(),
            &["remote", "set-url", "origin", origin.to_str().unwrap()],
        )
        .await
        .unwrap();
        repo.remote_url = Some(url::Url::from_file_path(origin).unwrap().to_string());
        assert_eq!(
            verify_server_location(&repo, path).await.status,
            RepoLocationStatus::Ready
        );
    }

    fn runtime(root: &str) -> Runtime {
        Runtime {
            id: "runtime-test".to_owned(),
            daemon_id: DAEMON_ID.to_owned(),
            kind: "cli".to_owned(),
            workspace_root: root.to_owned(),
            status: RuntimeStatus::Ready,
            labels_json: "{}".to_owned(),
            created_at: "now".to_owned(),
            updated_at: "now".to_owned(),
        }
    }

    fn location(path: &str, kind: RepoLocationKind) -> RepoLocation {
        RepoLocation {
            id: "location-test".to_owned(),
            repo_id: "repo-test".to_owned(),
            owner_kind: if kind == RepoLocationKind::SharedMount {
                RepoLocationOwnerKind::Server
            } else {
                RepoLocationOwnerKind::Daemon
            },
            daemon_id: Some(DAEMON_ID.to_owned()),
            runtime_id: Some("runtime-test".to_owned()),
            path: path.to_owned(),
            kind,
            is_default: false,
            status: RepoLocationStatus::Unverified,
            last_verified_at: None,
            last_error: None,
            version: 3,
            created_at: "now".to_owned(),
            updated_at: "now".to_owned(),
        }
    }

    #[tokio::test]
    async fn daemon_location_verification_succeeds_without_server_path_access() {
        let daemon = ScriptedDaemon::new(vec![Reply::Verify { mismatch: false }]);
        let server_root = tempfile::tempdir().expect("server root creates");
        let verifier = RemoteDaemonLocationVerifier::new(
            Arc::clone(&daemon.registry),
            server_root.path().to_owned(),
        );
        let location = location("/remote/workspaces/repo", RepoLocationKind::PrimaryCheckout);
        let verified = verifier
            .verify(&repo(), &location, &runtime("/remote/workspaces"))
            .await
            .expect("verify succeeds");
        assert_eq!(verified.status, RepoLocationStatus::Ready);
        assert_eq!(verified.last_error, None);
        let requests = daemon.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, api_types::METHOD_REPO_LOCATION_VERIFY);
        assert_eq!(requests[0].params["path"], "/remote/workspaces/repo");
        assert_eq!(requests[0].params["expected_version"], location.version + 1);
        assert!(!requests[0]
            .params
            .as_object()
            .unwrap()
            .contains_key("probe"));
        assert_eq!(std::fs::read_dir(server_root.path()).unwrap().count(), 0);
        daemon.finish().await;
    }

    #[tokio::test]
    async fn daemon_outside_workspace_root_rejection_is_invalid() {
        let daemon = ScriptedDaemon::new(vec![rejection(
            api_types::OUTSIDE_WORKSPACE_ROOT,
            "symlink escaped runtime root",
            None,
        )]);
        let server_root = tempfile::tempdir().expect("server root creates");
        let verifier = RemoteDaemonLocationVerifier::new(
            Arc::clone(&daemon.registry),
            server_root.path().to_owned(),
        );
        let verified = verifier
            .verify(
                &repo(),
                &location(
                    "/remote/workspaces/symlink",
                    RepoLocationKind::PrimaryCheckout,
                ),
                &runtime("/remote/workspaces"),
            )
            .await
            .expect("rejection recorded");
        assert_eq!(verified.status, RepoLocationStatus::Invalid);
        assert_eq!(
            verified.last_error.as_deref(),
            Some("outside_workspace_root")
        );
        assert_eq!(daemon.requests().len(), 1);
        daemon.finish().await;
    }

    #[tokio::test]
    async fn location_outside_runtime_root_is_rejected_before_rpc() {
        let daemon = ScriptedDaemon::new(vec![]);
        let server_root = tempfile::tempdir().expect("server root creates");
        let verifier = RemoteDaemonLocationVerifier::new(
            Arc::clone(&daemon.registry),
            server_root.path().to_owned(),
        );
        let verified = verifier
            .verify(
                &repo(),
                &location(
                    "/remote/workspaces-other/repo",
                    RepoLocationKind::PrimaryCheckout,
                ),
                &runtime("/remote/workspaces"),
            )
            .await
            .expect("lexical rejection recorded");
        assert_eq!(verified.status, RepoLocationStatus::Invalid);
        assert_eq!(
            verified.last_error.as_deref(),
            Some("outside_workspace_root")
        );
        assert!(daemon.requests().is_empty());
        daemon.finish().await;
    }

    #[tokio::test]
    async fn shared_mount_probe_round_trip_is_ready_and_removed() {
        let daemon = ScriptedDaemon::new(vec![Reply::Verify { mismatch: false }]);
        let server_root = tempfile::tempdir().expect("server root creates");
        let root = server_root.path().canonicalize().unwrap();
        let path = root.join("repo").to_string_lossy().into_owned();
        let verifier =
            RemoteDaemonLocationVerifier::new(Arc::clone(&daemon.registry), root.clone());
        let verified = verifier
            .verify(
                &repo(),
                &location(&path, RepoLocationKind::SharedMount),
                &runtime(&root.to_string_lossy()),
            )
            .await
            .expect("probe verified");
        assert_eq!(verified.status, RepoLocationStatus::Ready);
        let requests = daemon.requests();
        let probe = &requests[0].params["probe"];
        let probe_path = Path::new(probe["path"].as_str().unwrap());
        assert!(probe_path.is_absolute());
        assert_eq!(probe_path.parent(), Some(root.as_path()));
        assert!(!probe["content"].as_str().unwrap().is_empty());
        assert!(!probe_path.exists());
        daemon.finish().await;
    }

    #[tokio::test]
    async fn shared_mount_probe_mismatch_is_invalid_and_removed() {
        let daemon = ScriptedDaemon::new(vec![Reply::Verify { mismatch: true }]);
        let server_root = tempfile::tempdir().expect("server root creates");
        let root = server_root.path().canonicalize().unwrap();
        let path = root.join("repo").to_string_lossy().into_owned();
        let verifier =
            RemoteDaemonLocationVerifier::new(Arc::clone(&daemon.registry), root.clone());
        let verified = verifier
            .verify(
                &repo(),
                &location(&path, RepoLocationKind::SharedMount),
                &runtime(&root.to_string_lossy()),
            )
            .await
            .expect("probe mismatch recorded");
        assert_eq!(verified.status, RepoLocationStatus::Invalid);
        assert_eq!(
            verified.last_error.as_deref(),
            Some("shared_mount_probe_mismatch")
        );
        let requests = daemon.requests();
        assert!(!Path::new(requests[0].params["probe"]["path"].as_str().unwrap()).exists());
        daemon.finish().await;
    }

    #[tokio::test]
    async fn shared_mount_probe_is_removed_after_daemon_rejection() {
        let daemon = ScriptedDaemon::new(vec![rejection(
            api_types::INVALID_INPUT,
            "invalid checkout",
            None,
        )]);
        let server_root = tempfile::tempdir().expect("server root creates");
        let root = server_root.path().canonicalize().unwrap();
        let path = root.join("repo").to_string_lossy().into_owned();
        let verifier =
            RemoteDaemonLocationVerifier::new(Arc::clone(&daemon.registry), root.clone());
        let verified = verifier
            .verify(
                &repo(),
                &location(&path, RepoLocationKind::SharedMount),
                &runtime(&root.to_string_lossy()),
            )
            .await
            .expect("rejection recorded");
        assert_eq!(verified.status, RepoLocationStatus::Invalid);
        let requests = daemon.requests();
        assert!(!Path::new(requests[0].params["probe"]["path"].as_str().unwrap()).exists());
        daemon.finish().await;
    }

    #[tokio::test]
    async fn shared_mount_probe_is_removed_on_cancellation() {
        let daemon = ScriptedDaemon::new(vec![Reply::Ignore]);
        let server_root = tempfile::tempdir().expect("server root creates");
        let root = server_root.path().canonicalize().unwrap();
        let path = root.join("repo").to_string_lossy().into_owned();
        let verifier =
            RemoteDaemonLocationVerifier::new(Arc::clone(&daemon.registry), root.clone());
        let request = tokio::spawn(async move {
            verifier
                .verify(
                    &repo(),
                    &location(&path, RepoLocationKind::SharedMount),
                    &runtime(&root.to_string_lossy()),
                )
                .await
        });
        while daemon.requests().is_empty() {
            tokio::task::yield_now().await;
        }
        let requests = daemon.requests();
        let probe_path = Path::new(requests[0].params["probe"]["path"].as_str().unwrap());
        assert!(probe_path.exists());
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(!probe_path.exists());
        daemon.finish().await;
    }

    #[tokio::test]
    async fn verification_retries_on_reconnect_and_persists_with_version_cas() {
        let daemon = ScriptedDaemon::new(vec![
            Reply::Verify { mismatch: false },
            rejection(api_types::OUTSIDE_WORKSPACE_ROOT, "changed symlink", None),
        ]);
        let server_root = tempfile::tempdir().expect("server root creates");
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        let db = Arc::new(SqliteDb::new(pool));
        let now = now_rfc3339();
        ProjectRepo::create(
            &*db,
            CreateProject {
                id: "project-test".to_owned(),
                name: "Locations".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        let repo = repo();
        RepoRepo::create(
            &*db,
            CreateRepo {
                id: repo.id,
                project_id: repo.project_id,
                name: repo.name,
                remote_url: repo.remote_url,
                local_path: None,
                default_branch: repo.default_branch,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repository creates");
        DaemonRepo::upsert_by_machine_id(
            &*db,
            UpsertDaemon {
                max_concurrent_runs: None,
                id: DAEMON_ID.to_owned(),
                machine_id: "machine-test".to_owned(),
                hostname: "test-host".to_owned(),
                os: "linux".to_owned(),
                arch: "x86_64".to_owned(),
                agent_version: None,
                labels_json: "{}".to_owned(),
                status: DaemonStatus::Online,
                registration_token_hash: None,
                owner_id: None,
                visibility: "global".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("daemon creates");
        RuntimeRepo::create(
            &*db,
            CreateRuntime {
                id: "runtime-test".to_owned(),
                daemon_id: DAEMON_ID.to_owned(),
                kind: "cli".to_owned(),
                workspace_root: "/remote/workspaces".to_owned(),
                status: RuntimeStatus::Ready,
                labels_json: "{}".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("runtime creates");
        for (id, status) in [
            ("location-a", RepoLocationStatus::Unavailable),
            ("location-b", RepoLocationStatus::Ready),
        ] {
            RepoLocationRepo::create(
                &*db,
                CreateRepoLocation {
                    id: id.to_owned(),
                    repo_id: "repo-test".to_owned(),
                    owner_kind: RepoLocationOwnerKind::Daemon,
                    daemon_id: Some(DAEMON_ID.to_owned()),
                    runtime_id: Some("runtime-test".to_owned()),
                    path: "/remote/workspaces/repo".to_owned(),
                    kind: RepoLocationKind::PrimaryCheckout,
                    is_default: false,
                    status,
                    last_verified_at: None,
                    last_error: Some("old error".to_owned()),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
            )
            .await
            .expect("location creates");
        }
        let service = RepoLocationService::new(
            Arc::clone(&db),
            Arc::new(RemoteDaemonLocationVerifier::new(
                Arc::clone(&daemon.registry),
                server_root.path().to_owned(),
            )),
        );
        assert_eq!(
            service
                .retry_verification_on_reconnect(DAEMON_ID)
                .await
                .expect("reconnect retries"),
            2
        );
        let ready = RepoLocationRepo::get_by_id(&*db, "location-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ready.status, RepoLocationStatus::Ready);
        assert_eq!(ready.last_error, None);
        assert_eq!(ready.version, 2);
        assert!(ready.last_verified_at.is_some());
        let invalid = RepoLocationRepo::get_by_id(&*db, "location-b")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(invalid.status, RepoLocationStatus::Invalid);
        assert_eq!(
            invalid.last_error.as_deref(),
            Some("outside_workspace_root")
        );
        assert_eq!(invalid.version, 2);
        assert!(invalid.last_verified_at.is_some());
        daemon.finish().await;
    }
}
