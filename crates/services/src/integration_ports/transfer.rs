//! Object transfer between the Task's checkout and the default checkout over
//! the owner operations. Either end is a server-owned location (local Git) or
//! a daemon-owned one (the owner wire); the server relays the bundle.
use super::owner::{client_error, DaemonFences};
use crate::{
    daemon_transport::workspace_client::{DaemonObjectEndpoint, DaemonWorkspaceClient},
    integration_effects::EffectOwner,
    integration_owner::{
        ObjectTransferOutcome as OwnerOutcome, ServerIntegrationOwner, ServerObjectExport,
        ServerObjectImport,
    },
    integration_worker::{
        ObjectTransferDirection, ObjectTransferEndpoint, ObjectTransferOutcome, ObjectTransferPort,
        ObjectTransferRelease, ObjectTransferRequest,
    },
    Result, ServiceError,
};
use api_types::{ObjectExportReceipt, ObjectImportReceipt, ObjectTransferRefusal};
use async_trait::async_trait;
use db::{IntegrationOwnerFence, SqliteDb};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

/// The production [`ObjectTransferPort`].
///
/// One transfer, in order: `imported_objects` on the receiver (a repeat of
/// the key finds its import and moves nothing), `export_objects` on the
/// source into server staging, `import_objects` on the receiver, then
/// `release_objects` for the source's staging. A daemon end is told the claim
/// generation first ([`DaemonFences::announce`]).
pub struct OwnerObjectTransfer {
    db: Arc<SqliteDb>,
    server: Arc<ServerIntegrationOwner>,
    client: DaemonWorkspaceClient,
    fences: Arc<DaemonFences>,
    /// Bundles in flight on this server. Private to this instance.
    staging: PathBuf,
    /// Releases of ended attempts' refs owed by daemon-owned checkouts, found
    /// at start: `(endpoint, attempt, failed tries)`.
    owed: std::sync::Mutex<Vec<(OwedEnd, String, u32)>>,
}

#[derive(Clone)]
struct OwedEnd {
    daemon_id: String,
    runtime_id: String,
    repo_location_id: String,
}
/// An ended attempt older than this has had its refs released by an earlier
/// start, or its checkout is gone.
const OWED_RELEASE_WINDOW_DAYS: i64 = 30;
const OWED_RELEASE_MAX: u32 = 4096;
const OWED_RELEASE_TRIES: u32 = 3;

enum End<'a> {
    Server { location_id: &'a str },
    Daemon(DaemonObjectEndpoint<'a>),
}

fn end(endpoint: &ObjectTransferEndpoint) -> End<'_> {
    match &endpoint.owner {
        EffectOwner::Server => End::Server {
            location_id: &endpoint.repo_location_id,
        },
        EffectOwner::Daemon {
            daemon_id,
            runtime_id,
        } => End::Daemon(DaemonObjectEndpoint {
            daemon_id,
            runtime_id,
            repo_location_id: &endpoint.repo_location_id,
        }),
    }
}

fn refused(step: &str, refusal: ObjectTransferRefusal) -> ServiceError {
    ServiceError::invalid_operation(format!("object transfer {step} refused: {refusal:?}"))
}

struct RemoveFile(PathBuf);
impl Drop for RemoveFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

impl OwnerObjectTransfer {
    pub fn new(
        db: Arc<SqliteDb>,
        server: Arc<ServerIntegrationOwner>,
        client: DaemonWorkspaceClient,
        fences: Arc<DaemonFences>,
        staging_root: &Path,
    ) -> Self {
        Self {
            db,
            server,
            client,
            fences,
            staging: staging_root.join(db::new_uuid_v4()),
            owed: std::sync::Mutex::new(Vec::new()),
        }
    }

    async fn announce(&self, end: &End<'_>, fence: &IntegrationOwnerFence) -> Result<()> {
        match end {
            End::Server { .. } => Ok(()),
            End::Daemon(daemon) => {
                self.fences
                    .announce(daemon.daemon_id, daemon.runtime_id, fence)
                    .await
            }
        }
    }

    async fn imported(
        &self,
        receiver: &End<'_>,
        fence: &IntegrationOwnerFence,
        key: &str,
        want: &str,
    ) -> Result<OwnerOutcome<Option<ObjectImportReceipt>>> {
        match receiver {
            End::Server { location_id } => {
                self.server
                    .imported_objects(fence, location_id, key, want)
                    .await
            }
            End::Daemon(daemon) => self
                .client
                .imported_objects(*daemon, fence, key, want)
                .await
                .map_err(client_error),
        }
    }

    async fn export(
        &self,
        source: &End<'_>,
        request: &ObjectTransferRequest,
        key: &str,
        dest: &Path,
        cancel: &CancellationToken,
    ) -> Result<OwnerOutcome<ObjectExportReceipt>> {
        match source {
            End::Server { location_id } => {
                self.server
                    .export_objects(ServerObjectExport {
                        fence: &request.fence,
                        key,
                        repo_location_id: location_id,
                        have: &request.have,
                        want: &request.want,
                        dest,
                        cancel,
                    })
                    .await
            }
            End::Daemon(daemon) => self
                .client
                .export_objects(
                    *daemon,
                    &request.fence,
                    key,
                    &request.have,
                    &request.want,
                    dest,
                    cancel,
                )
                .await
                .map_err(client_error),
        }
    }

    async fn import(
        &self,
        receiver: &End<'_>,
        fence: &IntegrationOwnerFence,
        export: &ObjectExportReceipt,
        bundle: &Path,
        cancel: &CancellationToken,
    ) -> Result<OwnerOutcome<ObjectImportReceipt>> {
        match receiver {
            End::Server { location_id } => {
                self.server
                    .import_objects(ServerObjectImport {
                        fence,
                        export,
                        repo_location_id: location_id,
                        bundle,
                        cancel,
                    })
                    .await
            }
            End::Daemon(daemon) => self
                .client
                .import_objects(*daemon, fence, export, bundle, cancel)
                .await
                .map_err(client_error),
        }
    }

    async fn release_end(&self, endpoint: &ObjectTransferEndpoint, attempt_id: &str) -> Result<()> {
        match end(endpoint) {
            End::Server { location_id } => {
                let Some(path) = self.server_location(location_id).await? else {
                    return Ok(());
                };
                git::integration::release_attempt_refs(&path, attempt_id).await?;
                Ok(())
            }
            End::Daemon(daemon) => self
                .client
                .release_attempt_objects(daemon, attempt_id)
                .await
                .map(|_| ())
                .map_err(client_error),
        }
    }

    async fn server_location(&self, location_id: &str) -> Result<Option<PathBuf>> {
        let path: Option<String> = sqlx::query_scalar(
            "SELECT path FROM repo_location WHERE id=? AND owner_kind='server' AND status='ready'",
        )
        .bind(location_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(db::DbError::from)?;
        Ok(path.map(PathBuf::from))
    }

    /// The attempt a transfer key belongs to (`<attempt>-<generation>-<in|out>`).
    fn key_attempt(key: &str) -> Option<&str> {
        let (rest, direction) = key.rsplit_once('-')?;
        let (attempt, generation) = rest.rsplit_once('-')?;
        (matches!(direction, "in" | "out")
            && !attempt.is_empty()
            && generation.bytes().all(|byte| byte.is_ascii_digit()))
        .then_some(attempt)
    }
}

impl Drop for OwnerObjectTransfer {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.staging);
    }
}

#[async_trait]
impl ObjectTransferPort for OwnerObjectTransfer {
    async fn transfer(&self, request: ObjectTransferRequest) -> Result<ObjectTransferOutcome> {
        let key = api_types::object_transfer_key(
            &request.fence.attempt_id,
            request.fence.generation,
            request.direction,
        );
        let (source, receiver) = match request.direction {
            ObjectTransferDirection::Inbound => (end(&request.target), end(&request.task)),
            ObjectTransferDirection::Outbound => (end(&request.task), end(&request.target)),
        };
        // Before the first effect of the claim generation on either owner.
        self.announce(&source, &request.fence).await?;
        self.announce(&receiver, &request.fence).await?;

        match self
            .imported(&receiver, &request.fence, &key, &request.want)
            .await?
        {
            OwnerOutcome::Done(Some(_)) => {
                return Ok(ObjectTransferOutcome::Transferred { bytes: 0 })
            }
            OwnerOutcome::Done(None) => {}
            OwnerOutcome::Refused(refusal) => return Err(refused("lookup", refusal)),
            OwnerOutcome::Cancelled => {
                return Err(ServiceError::invalid_operation(
                    "object transfer was cancelled",
                ))
            }
        }
        // Synchronous on purpose: a dropped transfer must not leave a
        // directory created after its guard ran.
        std::fs::create_dir_all(&self.staging).map_err(crate::integration_owner::transfer_io)?;
        let bundle = self
            .staging
            .join(format!("{key}-{}.bundle", db::new_uuid_v4()));
        let _staged = RemoveFile(bundle.clone());
        let cancel = CancellationToken::new();
        let _stop = cancel.clone().drop_guard();
        let export = match self
            .export(&source, &request, &key, &bundle, &cancel)
            .await?
        {
            OwnerOutcome::Done(export) => export,
            OwnerOutcome::Refused(ObjectTransferRefusal::TooLarge { bytes, .. }) => {
                return Ok(ObjectTransferOutcome::TooLarge { bytes })
            }
            OwnerOutcome::Refused(refusal) => return Err(refused("export", refusal)),
            OwnerOutcome::Cancelled => {
                return Err(ServiceError::invalid_operation(
                    "object transfer was cancelled",
                ))
            }
        };
        let outcome = if export.total_bytes > request.max_bytes {
            Ok(ObjectTransferOutcome::TooLarge {
                bytes: export.total_bytes,
            })
        } else {
            match self
                .import(&receiver, &request.fence, &export, &bundle, &cancel)
                .await
            {
                Ok(OwnerOutcome::Done(_)) => Ok(ObjectTransferOutcome::Transferred {
                    bytes: export.total_bytes,
                }),
                Ok(OwnerOutcome::Refused(ObjectTransferRefusal::TooLarge { bytes, .. })) => {
                    Ok(ObjectTransferOutcome::TooLarge { bytes })
                }
                Ok(OwnerOutcome::Refused(refusal)) => Err(refused("import", refusal)),
                Ok(OwnerOutcome::Cancelled) => Err(ServiceError::invalid_operation(
                    "object transfer was cancelled",
                )),
                Err(error) => Err(error),
            }
        };
        // The source's staging for this key is of no further use, whatever
        // the import said. Best effort: the owner also bounds it by age.
        if let End::Daemon(daemon) = &source {
            self.client
                .release_objects(daemon.daemon_id, daemon.runtime_id, &key)
                .await;
        }
        outcome
    }

    async fn release(&self, release: ObjectTransferRelease) -> Result<()> {
        // Both ends are tried; the first failure is reported.
        let task = self.release_end(&release.task, &release.attempt_id).await;
        let target = self.release_end(&release.target, &release.attempt_id).await;
        task.and(target)
    }

    async fn sweep_at_start(&self) -> Result<()> {
        let locations: Vec<(String, String)> = sqlx::query_as(
            "SELECT id,path FROM repo_location WHERE owner_kind='server' AND status='ready' ORDER BY id",
        )
        .fetch_all(self.db.pool())
        .await
        .map_err(db::DbError::from)?;
        for (location_id, path) in locations {
            let path = PathBuf::from(path);
            if !path.join(".git").exists() {
                continue;
            }
            // Export pins and import quarantines of a transfer that died with
            // the previous process.
            let swept = path.clone();
            let leftovers = tokio::task::spawn_blocking(move || {
                git::integration::sweep_transfer_leftovers(&swept)
            })
            .await
            .unwrap_or(0);
            // Imported refs of attempts that hold no queue slot: a new claim
            // of such an attempt transfers under a new key.
            let mut attempts: Vec<String> = git::integration::imported_keys(&path)
                .await?
                .iter()
                .filter_map(|key| Self::key_attempt(key))
                .map(str::to_owned)
                .collect();
            attempts.sort();
            attempts.dedup();
            let mut released = 0;
            for attempt_id in attempts {
                let holds_slot: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM integration_queue WHERE head_attempt_id=?)",
                )
                .bind(&attempt_id)
                .fetch_one(self.db.pool())
                .await
                .map_err(db::DbError::from)?;
                if holds_slot {
                    continue;
                }
                match git::integration::release_attempt_refs(&path, &attempt_id).await {
                    Ok(removed) => released += removed,
                    Err(error) => {
                        tracing::warn!(target: "services::integration_ports", %location_id, %attempt_id, %error, "imported integration refs were not released");
                    }
                }
            }
            if leftovers > 0 || released > 0 {
                tracing::info!(target: "services::integration_ports", %location_id, leftovers, released, "removed leftovers of ended integration transfers");
            }
        }
        // Daemon-owned checkouts cannot be listed from here. Every attempt
        // that once held a slot, holds none now, and whose Task was placed in
        // another checkout than its queue's target may have left refs on a
        // daemon end (the server stopped between the transfer and the
        // release). One release per such attempt and daemon end, by the
        // existing wire; it is idempotent and deletes nothing else.
        let since = (chrono::Utc::now() - chrono::Duration::days(OWED_RELEASE_WINDOW_DAYS))
            .to_rfc3339();
        let owed: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT a.id,l.id,l.daemon_id,l.runtime_id FROM integration_attempt a \
             JOIN integration_queue q ON q.id=a.queue_id \
             JOIN repo_location l ON l.id IN (a.repo_location_id,q.target_location_id) \
             WHERE l.owner_kind='daemon' AND l.daemon_id IS NOT NULL AND l.runtime_id IS NOT NULL \
               AND a.repo_location_id IS NOT NULL AND q.target_location_id IS NOT NULL \
               AND a.repo_location_id<>q.target_location_id \
               AND (q.head_attempt_id IS NULL OR q.head_attempt_id<>a.id) \
               AND a.started_at IS NOT NULL AND a.updated_at>=? ORDER BY a.updated_at DESC,a.id LIMIT ?",
        )
        .bind(&since)
        .bind(OWED_RELEASE_MAX)
        .fetch_all(self.db.pool())
        .await
        .map_err(db::DbError::from)?;
        *self.owed.lock().expect("owed releases") = owed
            .into_iter()
            .map(|(attempt_id, repo_location_id, daemon_id, runtime_id)| {
                (
                    OwedEnd {
                        daemon_id,
                        runtime_id,
                        repo_location_id,
                    },
                    attempt_id,
                    0,
                )
            })
            .collect();
        self.sweep_owners().await
    }

    async fn sweep_owners(&self) -> Result<()> {
        let owed = std::mem::take(&mut *self.owed.lock().expect("owed releases"));
        if owed.is_empty() {
            return Ok(());
        }
        let mut left = Vec::new();
        for (end, attempt_id, tries) in owed {
            // An owner that is not connected is asked when it is back.
            if self.client.connection_id(&end.daemon_id).is_none() {
                left.push((end, attempt_id, tries));
                continue;
            }
            let released = self
                .client
                .release_attempt_objects(
                    DaemonObjectEndpoint {
                        daemon_id: &end.daemon_id,
                        runtime_id: &end.runtime_id,
                        repo_location_id: &end.repo_location_id,
                    },
                    &attempt_id,
                )
                .await;
            match released {
                Ok(removed) => {
                    if removed > 0 {
                        tracing::info!(target: "services::integration_ports", daemon_id = %end.daemon_id, location_id = %end.repo_location_id, %attempt_id, removed, "released refs of an ended integration attempt");
                    }
                }
                Err(error) if tries + 1 < OWED_RELEASE_TRIES => {
                    tracing::debug!(target: "services::integration_ports", daemon_id = %end.daemon_id, %attempt_id, error = %client_error(error), "release of an ended attempt's refs failed; it is tried again");
                    left.push((end, attempt_id, tries + 1));
                }
                Err(error) => {
                    tracing::warn!(target: "services::integration_ports", daemon_id = %end.daemon_id, location_id = %end.repo_location_id, %attempt_id, error = %client_error(error), "refs of an ended integration attempt were not released");
                }
            }
        }
        self.owed.lock().expect("owed releases").extend(left);
        Ok(())
    }
}
