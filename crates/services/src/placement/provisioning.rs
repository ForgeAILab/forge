//! Single-flight clone jobs. Durable retry deadlines survive panics and restart;
//! no job owns a Task run slot or persists a permanent running state.
use super::environment::{self, ProbeTarget};
use crate::{
    daemon_transport::{
        workspace_client::{DaemonWorkspaceClient, WorkspaceClientError},
        DaemonConnectionRegistry,
    },
    Result, ServiceError,
};
use api_types::{EnvironmentCheckScope, PlacementProvision, ProjectSettings};
use db::{EnvironmentMachine, ProjectMachineReadinessRepo, RepoLocationRepo};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex, OnceLock},
};

pub(crate) const MAX_PROVISION_ATTEMPTS: i64 = 5;

#[derive(Debug, Clone)]
pub(crate) struct ProvisionRetryState {
    pub attempts: i64,
    pub inputs_digest: String,
    pub connection_id: i64,
}
impl From<(i64, String, i64)> for ProvisionRetryState {
    fn from((attempts, inputs_digest, connection_id): (i64, String, i64)) -> Self {
        Self {
            attempts,
            inputs_digest,
            connection_id,
        }
    }
}
impl ProvisionRetryState {
    pub fn exhausted(&self, settings: &ProjectSettings, connection: Option<u64>) -> bool {
        self.attempts >= MAX_PROVISION_ATTEMPTS
            && self.inputs_digest == job_inputs_digest(settings)
            && connection.is_none_or(|id| self.connection_id == id as i64)
    }
}

type Key = (String, String);
static FLIGHTS: OnceLock<Mutex<HashSet<Key>>> = OnceLock::new();
struct Flight(Key);
impl Drop for Flight {
    fn drop(&mut self) {
        FLIGHTS
            .get()
            .expect("provision flights")
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.0);
    }
}

pub(crate) fn client_error(error: WorkspaceClientError) -> ServiceError {
    match error {
        WorkspaceClientError::Transport(error) => error,
        WorkspaceClientError::Daemon(error) => {
            ServiceError::invalid_operation(format!("{}: {}", error.code, error.message))
        }
    }
}

pub(crate) struct Job {
    pub db: db::SqliteDb,
    pub repo: db::Repo,
    pub project: db::Project,
    pub candidate: super::PlacementCandidate,
    pub registry: Arc<DaemonConnectionRegistry>,
    pub events: Arc<events::EventBus>,
    pub kick: Arc<tokio::sync::Notify>,
    pub role: String,
}

pub(crate) async fn start(job: Job) -> Result<()> {
    let runtime = job
        .candidate
        .location
        .runtime_id
        .as_ref()
        .ok_or(db::DbError::NotFound)?;
    let key = (job.repo.id.clone(), runtime.clone());
    if !FLIGHTS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(key.clone())
    {
        return Ok(());
    }
    let guard = Flight(key);
    let settings: ProjectSettings = serde_json::from_str(&job.project.settings)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let digest = job_inputs_digest(&settings);
    let connection = job
        .registry
        .get(
            job.candidate
                .location
                .daemon_id
                .as_deref()
                .ok_or(db::DbError::NotFound)?,
        )
        .ok_or_else(|| ServiceError::DaemonUnavailable {
            daemon_id: job.candidate.location.daemon_id.clone().unwrap_or_default(),
        })?
        .id();
    let mut tx = db::begin_immediate(job.db.pool()).await?;
    let raw: String = sqlx::query_scalar("SELECT settings FROM project WHERE id=?")
        .bind(&job.project.id)
        .fetch_one(&mut *tx)
        .await?;
    let latest: ProjectSettings = serde_json::from_str(&raw)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    if job_inputs_digest(&latest) != digest {
        return Err(db::DbError::VersionConflict.into());
    }
    let next = environment_retry_at(1);
    // Claim a due attempt and schedule its successor before starting I/O. A
    // response loss or process death can only delay work to a bounded deadline.
    let attempts: Option<i64> = sqlx::query_scalar("INSERT INTO repo_provision_retry (repo_id, runtime_id, attempts, next_attempt_at, checks_digest, connection_id, started_at) VALUES (?, ?, 1, ?, ?, ?, ?) ON CONFLICT(repo_id, runtime_id) DO UPDATE SET attempts = CASE WHEN checks_digest <> excluded.checks_digest OR connection_id <> excluded.connection_id THEN 1 ELSE attempts + 1 END, next_attempt_at = excluded.next_attempt_at, checks_digest = excluded.checks_digest, connection_id = excluded.connection_id, started_at = excluded.started_at, last_error = CASE WHEN checks_digest <> excluded.checks_digest THEN NULL ELSE last_error END WHERE (checks_digest <> excluded.checks_digest OR connection_id <> excluded.connection_id OR (attempts < ? AND julianday(next_attempt_at) <= julianday(?))) RETURNING attempts")
        .bind(&job.repo.id).bind(runtime).bind(next).bind(&digest).bind(connection as i64).bind(db::now_rfc3339()).bind(MAX_PROVISION_ATTEMPTS).bind(db::now_rfc3339()).fetch_optional(&mut *tx).await?;
    let Some(attempts) = attempts else {
        return Ok(());
    };
    sqlx::query(
        "UPDATE repo_provision_retry SET next_attempt_at = ? WHERE repo_id = ? AND runtime_id = ?",
    )
    .bind(environment_retry_at(attempts))
    .bind(&job.repo.id)
    .bind(runtime)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    tokio::spawn(async move {
        let mut location_witness = None;
        let mut readiness_witness = None;
        if let Err(error) = job
            .run(
                attempts,
                &digest,
                connection as i64,
                &mut location_witness,
                &mut readiness_witness,
            )
            .await
        {
            let error = match error {
                ServiceError::InvalidOperation { message } => {
                    ServiceError::invalid_operation(git::redact_remote_credentials(
                        &message,
                        job.repo.remote_url.as_deref().unwrap_or(""),
                    ))
                }
                error => error,
            };
            let stale = db::ProjectRepo::get_by_id(&job.db, &job.project.id)
                .await
                .ok()
                .flatten()
                .and_then(|project| serde_json::from_str::<ProjectSettings>(&project.settings).ok())
                .is_none_or(|settings| job_inputs_digest(&settings) != digest);
            if stale
                || job
                    .registry
                    .get(
                        job.candidate
                            .location
                            .daemon_id
                            .as_deref()
                            .unwrap_or_default(),
                    )
                    .is_some_and(|current| current.id() != connection)
                || matches!(error, ServiceError::Db(db::DbError::VersionConflict))
            {
                // A new digest/version owns the next attempt. No old error or
                // location result is applied to that epoch.
                let _ = sqlx::query("UPDATE repo_provision_retry SET next_attempt_at=? WHERE repo_id=? AND runtime_id=? AND attempts=? AND checks_digest=? AND connection_id=?").bind(db::now_rfc3339()).bind(&job.repo.id).bind(&job.candidate.location.runtime_id).bind(attempts).bind(&digest).bind(connection as i64).execute(job.db.pool()).await;
                drop(guard);
                job.kick.notify_one();
                return;
            }
            tracing::warn!(repo_id=%job.repo.id,%error,"repository provisioning attempt failed; retaining readiness and retrying with backoff");
            if let Some((readiness, location)) = location_witness.as_ref() {
                if location.status != db::RepoLocationStatus::Ready {
                    let _ = job
                        .update_location(
                            readiness,
                            location,
                            None,
                            db::RepoLocationStatus::Unavailable,
                            Some(crate::project_environment::bounded_output_tail(
                                &error.to_string(),
                            )),
                            false,
                        )
                        .await;
                }
            }
            if let Ok(mut tx) = db::begin_immediate(job.db.pool()).await {
                if fence_job_inputs(&mut tx, &job.project.id, &digest)
                    .await
                    .is_ok()
                {
                    let _ = sqlx::query("UPDATE repo_provision_retry SET last_error = ?, next_attempt_at = ? WHERE repo_id = ? AND runtime_id = ? AND attempts = ? AND checks_digest = ? AND connection_id = ? AND COALESCE((SELECT version FROM project_machine_readiness WHERE project_id=? AND owner_kind='daemon' AND daemon_id=? AND runtime_id=?),-1)=?")
                .bind(crate::project_environment::bounded_output_tail(&error.to_string())).bind(environment_retry_at(attempts))
                .bind(&job.repo.id).bind(&job.candidate.location.runtime_id).bind(attempts).bind(&digest).bind(connection as i64).bind(&job.project.id).bind(&job.candidate.location.daemon_id).bind(&job.candidate.location.runtime_id).bind(readiness_witness.as_ref().map(|row|row.version).unwrap_or(-1)).execute(&mut *tx).await;
                    let _ = tx.commit().await;
                }
            }
        }
        // Existing waiters receive the terminal failure/progress reason without
        // spending a Task version or relying on a later scan to display it.
        if let Ok(Some((started, Some(error)))) = sqlx::query_as::<_, (Option<String>, Option<String>)>("SELECT started_at, last_error FROM repo_provision_retry WHERE repo_id=? AND runtime_id=? AND attempts=? AND checks_digest=? AND connection_id=?")
            .bind(&job.repo.id).bind(&job.candidate.location.runtime_id).bind(attempts).bind(&digest).bind(connection as i64).fetch_optional(job.db.pool()).await {
            let machine = db::EnvironmentMachine::from_location(&job.candidate.location);
            if let Ok(name) = environment::machine_name(&job.db, &machine).await {
                let machine_json = serde_json::to_value(&machine).expect("machine").to_string();
                let tasks: Vec<(String,i64)> = sqlx::query_as("SELECT id,version FROM task WHERE project_id=? AND json_valid(metadata_json) AND json_extract(metadata_json,'$.environment_wait.machine')=json(?)")
                    .bind(&job.project.id).bind(&machine_json).fetch_all(job.db.pool()).await.unwrap_or_default();
                let elapsed = started.as_deref().and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok()).map(|time| (chrono::Utc::now()-time.with_timezone(&chrono::Utc)).num_seconds().max(0)).unwrap_or(0);
                let reason = format!("environment_probe_pending: {name}; provisioning elapsed {elapsed}s; last failure: {error}{}", if attempts >= MAX_PROVISION_ATTEMPTS { "; provisioning retry limit reached" } else { "" });
                if let Ok(mut tx)=db::begin_immediate(job.db.pool()).await {
                    if fence_job_inputs(&mut tx,&job.project.id,&digest).await.is_ok() {
                for (id,version) in tasks { let _ = db::task_writer::TaskQuery::new(&job.db,&id,"UPDATE task SET metadata_json=json_set(metadata_json,'$.deferred_dispatch.reason',?) WHERE id=? AND version=? AND json_extract(metadata_json,'$.environment_wait.machine')=json(?) AND COALESCE((SELECT version FROM project_machine_readiness WHERE project_id=? AND owner_kind='daemon' AND daemon_id=? AND runtime_id=?),-1)=?")
                    .bind(&reason).bind(&id).bind(version).bind(&machine_json).bind(&job.project.id).bind(&job.candidate.location.daemon_id).bind(&job.candidate.location.runtime_id).bind(readiness_witness.as_ref().map(|row|row.version).unwrap_or(-1)).identity_fenced().execute_in_tx(&mut tx).await; }
                        let _=tx.commit().await;
                    }
                }
            }
        }
        drop(guard);
        job.kick.notify_one();
    });
    Ok(())
}

pub(crate) fn job_inputs_digest(settings: &ProjectSettings) -> String {
    format!(
        "{}:{:?}:{}",
        db::environment_checks_digest(&settings.environment),
        settings.placement.provision,
        settings.placement.provision_timeout_seconds
    )
}

async fn fence_job_inputs(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    project_id: &str,
    digest: &str,
) -> Result<()> {
    let raw: String = sqlx::query_scalar("SELECT settings FROM project WHERE id=?")
        .bind(project_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(db::DbError::NotFound)?;
    let settings: ProjectSettings = serde_json::from_str(&raw)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    if job_inputs_digest(&settings) != digest {
        return Err(db::DbError::VersionConflict.into());
    }
    Ok(())
}

fn environment_retry_at(attempt: i64) -> String {
    environment::retry_deadline(attempt)
}

impl Job {
    async fn settings(&self) -> Result<ProjectSettings> {
        let project = db::ProjectRepo::get_by_id(&self.db, &self.project.id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let settings: ProjectSettings = serde_json::from_str(&project.settings)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let original: ProjectSettings = serde_json::from_str(&self.project.settings)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        if job_inputs_digest(&settings) != job_inputs_digest(&original) {
            return Err(db::DbError::VersionConflict.into());
        }
        if settings.placement.provision != PlacementProvision::WhenVerified {
            return Err(ServiceError::invalid_operation(
                "Project provisioning is disabled",
            ));
        }
        Ok(settings)
    }

    async fn ensure_attempt(&self, attempt: i64, digest: &str, connection: i64) -> Result<()> {
        let current:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM repo_provision_retry WHERE repo_id=? AND runtime_id=? AND attempts=? AND checks_digest=? AND connection_id=?)")
            .bind(&self.repo.id).bind(&self.candidate.location.runtime_id).bind(attempt).bind(digest).bind(connection).fetch_one(self.db.pool()).await?;
        if !current
            || self
                .registry
                .get(
                    self.candidate
                        .location
                        .daemon_id
                        .as_deref()
                        .unwrap_or_default(),
                )
                .is_some_and(|current| current.id() as i64 != connection)
        {
            return Err(db::DbError::VersionConflict.into());
        }
        Ok(())
    }
    async fn fence(
        &self,
        row: &db::ProjectMachineReadiness,
        environment: &api_types::ProjectEnvironment,
        attempt: i64,
        digest: &str,
        connection: i64,
    ) -> Result<()> {
        self.ensure_attempt(attempt, digest, connection).await?;
        let current = self.settings().await?;
        if db::environment_checks_digest(&current.environment)
            != db::environment_checks_digest(environment)
            || self
                .db
                .get_readiness(&self.project.id, &row.machine)
                .await?
                .is_none_or(|latest| {
                    latest.version != row.version || latest.checks_digest != row.checks_digest
                })
        {
            return Err(db::DbError::VersionConflict.into());
        }
        Ok(())
    }

    async fn fence_in_transaction(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        row: &db::ProjectMachineReadiness,
    ) -> Result<()> {
        let snapshot: ProjectSettings = serde_json::from_str(&self.project.settings)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        fence_job_inputs(tx, &self.project.id, &job_inputs_digest(&snapshot)).await?;
        let (owner, daemon, runtime) = row.machine.columns();
        let current:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project_machine_readiness r JOIN project p ON p.id=r.project_id WHERE r.project_id=? AND r.owner_kind=? AND r.daemon_id=? AND r.runtime_id=? AND r.version=? AND r.checks_digest=?)")
            .bind(&self.project.id).bind(owner).bind(daemon).bind(runtime).bind(row.version).bind(&row.checks_digest).fetch_one(&mut **tx).await?;
        if !current {
            return Err(db::DbError::VersionConflict.into());
        }
        Ok(())
    }

    async fn update_location(
        &self,
        row: &db::ProjectMachineReadiness,
        location: &db::RepoLocation,
        path: Option<String>,
        status: db::RepoLocationStatus,
        error: Option<String>,
        verified: bool,
    ) -> Result<db::RepoLocation> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        self.fence_in_transaction(&mut tx, row).await?;
        let now = db::now_rfc3339();
        let changed=sqlx::query("UPDATE repo_location SET path=COALESCE(?,path),status=?,last_error=?,last_verified_at=CASE WHEN ? THEN ? ELSE last_verified_at END,updated_at=?,version=version+1 WHERE id=? AND version=?")
            .bind(&path).bind(status.to_string()).bind(&error).bind(verified).bind(&now).bind(&now).bind(&location.id).bind(location.version).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(db::DbError::VersionConflict.into());
        }
        if verified {
            sqlx::query("DELETE FROM repo_provision_retry WHERE repo_id=? AND runtime_id=? AND checks_digest=?")
                .bind(&self.repo.id).bind(&location.runtime_id).bind(job_inputs_digest(&serde_json::from_str::<ProjectSettings>(&self.project.settings).map_err(|error|ServiceError::invalid_operation(error.to_string()))?)).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        let mut saved = location.clone();
        if let Some(path) = path {
            saved.path = path;
        }
        saved.status = status;
        saved.last_error = error;
        saved.updated_at = now.clone();
        saved.version += 1;
        if verified {
            saved.last_verified_at = Some(now);
        }
        Ok(saved)
    }

    async fn run(
        &self,
        attempt: i64,
        digest: &str,
        connection: i64,
        witness: &mut Option<(db::ProjectMachineReadiness, db::RepoLocation)>,
        readiness_witness: &mut Option<db::ProjectMachineReadiness>,
    ) -> Result<()> {
        let settings = self.settings().await?;
        let environment = settings.environment;
        let machine = EnvironmentMachine::from_location(&self.candidate.location);
        let daemon = self
            .candidate
            .location
            .daemon_id
            .as_ref()
            .ok_or(db::DbError::NotFound)?;
        let runtime = self
            .candidate
            .location
            .runtime_id
            .as_ref()
            .ok_or(db::DbError::NotFound)?;
        let client = DaemonWorkspaceClient::new(self.registry.clone());
        let target = ProbeTarget::Machine {
            machine: machine.clone(),
            client: client.clone(),
            location_id: None,
        };
        let checks: Vec<_> = environment
            .checks
            .iter()
            .filter(|check| check.scope == EnvironmentCheckScope::Machine)
            .cloned()
            .collect();
        if !checks.iter().any(|check| check.applies_to(&self.role)) {
            return Err(ServiceError::invalid_operation(
                "environment_unverified: declare a machine check or register a location",
            ));
        }
        let Some(_probe_guard) = environment::claim_probe(&self.project.id, &machine) else {
            return Ok(());
        };
        let observed = self.db.get_readiness(&self.project.id, &machine).await?;
        *readiness_witness = observed.clone();
        let results = environment::run_checks(&target, &environment, &checks).await?;
        let machine_passed = results.iter().all(|result| {
            result.passed
                || checks
                    .iter()
                    .find(|check| check.name == result.name)
                    .is_some_and(|check| !check.applies_to(&self.role))
        });
        self.ensure_attempt(attempt, digest, connection).await?;
        let mut row = environment::unknown_record(&self.project.id, machine.clone(), &environment);
        row.scope_covered = "machine".into();
        let row = self
            .db
            .put_readiness(row, observed.map(|row| row.version))
            .await?;
        let saved = environment::save_results(&self.db, row, &environment, results).await?;
        *readiness_witness = Some(saved.clone());
        if !machine_passed {
            return Ok(());
        }
        // Fence settings immediately before moving any code to the machine.
        let current = self.settings().await?;
        if db::environment_checks_digest(&current.environment) != saved.checks_digest {
            return Err(db::DbError::VersionConflict.into());
        }
        self.fence(&saved, &environment, attempt, digest, connection)
            .await?;
        let now = db::now_rfc3339();
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        self.fence_in_transaction(&mut tx, &saved).await?;
        let location_id: Option<String> = sqlx::query_scalar(
            "SELECT location_id FROM repo_provision_retry WHERE repo_id = ? AND runtime_id = ?",
        )
        .bind(&self.repo.id)
        .bind(runtime)
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        let location_id = if let Some(id) = location_id {
            id
        } else {
            // A concurrent manual registration wins; never provision over it.
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM repo_location WHERE repo_id = ? AND runtime_id = ?)",
            )
            .bind(&self.repo.id)
            .bind(runtime)
            .fetch_one(&mut *tx)
            .await?;
            if exists {
                return Err(db::DbError::VersionConflict.into());
            }
            let id = db::new_uuid_v4();
            sqlx::query("INSERT INTO repo_location (id, repo_id, owner_kind, daemon_id, runtime_id, path, kind, status, created_at, updated_at) VALUES (?, ?, 'daemon', ?, ?, ?, 'managed_clone', 'unverified', ?, ?)")
                .bind(&id).bind(&self.repo.id).bind(daemon).bind(runtime).bind(&self.candidate.location.path).bind(&now).bind(&now).execute(&mut *tx).await?;
            sqlx::query("UPDATE repo_provision_retry SET location_id = ? WHERE repo_id = ? AND runtime_id = ?")
                .bind(&id).bind(&self.repo.id).bind(runtime).execute(&mut *tx).await?;
            id
        };
        tx.commit().await?;
        let mut location = RepoLocationRepo::get_by_id(&self.db, &location_id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        *witness = Some((saved.clone(), location.clone()));
        let provision = client
            .provision_location(
                daemon,
                api_types::RepoLocationProvisionParams {
                    daemon_id: daemon.clone(),
                    runtime_id: runtime.clone(),
                    repo_id: self.repo.id.clone(),
                    remote_url: self.repo.remote_url.clone().ok_or(db::DbError::NotFound)?,
                    default_branch: self.repo.default_branch.clone(),
                    timeout_seconds: current.placement.provision_timeout_seconds,
                },
            )
            .await;
        let result = match provision {
            Ok(result) => result,
            Err(error) => return Err(client_error(error)),
        };
        if !managed_clone_path_matches(&result.workspace_root, &result.path, &self.repo.id)
            || result.default_branch != self.repo.default_branch
        {
            return Err(ServiceError::invalid_operation(
                "daemon returned an invalid managed clone location",
            ));
        }
        self.fence(&saved, &environment, attempt, digest, connection)
            .await?;
        location = self
            .update_location(
                &saved,
                &location,
                Some(result.path),
                db::RepoLocationStatus::Unverified,
                None,
                false,
            )
            .await?;
        *witness = Some((saved.clone(), location.clone()));
        let verified = client
            .verify_location(
                daemon,
                api_types::RepoLocationVerifyParams {
                    repo_location_id: location.id.clone(),
                    daemon_id: daemon.clone(),
                    runtime_id: runtime.clone(),
                    path: location.path.clone(),
                    kind: api_types::DaemonRepoLocationKind::ManagedClone,
                    default_branch: self.repo.default_branch.clone(),
                    remote_url: self.repo.remote_url.clone(),
                    expected_version: location.version,
                    probe: None,
                },
            )
            .await
            .map_err(client_error)?;
        if verified.repo_location_id != location.id
            || verified.path != location.path
            || verified.default_branch_sha.is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "daemon verification returned a different location",
            ));
        }
        let full = ProbeTarget::Machine {
            machine: machine.clone(),
            client,
            location_id: Some(location.id.clone()),
        };
        // Retain the exact machine-proof snapshot and version across full
        // checks. Never re-read a changed row and bless it with old results.
        self.fence(&saved, &environment, attempt, digest, connection)
            .await?;
        let mut row = saved;
        row.scope_covered = "full".into();
        let results = environment::run_checks(&full, &environment, &environment.checks).await?;
        let saved = environment::save_results(&self.db, row, &environment, results).await?;
        *readiness_witness = Some(saved.clone());
        self.fence(&saved, &environment, attempt, digest, connection)
            .await?;
        // A verified checkout can enter selection even when a full check fails:
        // readiness filters the role and scheduled re-checks own recovery.
        self.update_location(
            &saved,
            &location,
            None,
            db::RepoLocationStatus::Ready,
            None,
            true,
        )
        .await?;
        environment::clear_matching_pause(&self.db, &self.events, &self.project, &saved).await?;
        Ok(())
    }
}

// Owner paths are opaque to the server's OS: a Unix server can provision a
// Windows daemon. Validate the returned root/repository relationship lexically.
fn managed_clone_path_matches(root: &str, path: &str, repo: &str) -> bool {
    let drive = root.as_bytes().get(1) == Some(&b':')
        && root.as_bytes().first().is_some_and(u8::is_ascii_alphabetic);
    let windows = drive || root.starts_with("\\\\");
    let root = if windows {
        root.replace('\\', "/")
    } else {
        root.to_owned()
    };
    let path = if windows {
        path.replace('\\', "/")
    } else {
        path.to_owned()
    };
    let absolute = if drive {
        root.as_bytes().get(2) == Some(&b'/')
    } else {
        root.starts_with('/')
    };
    absolute
        && !root.contains('\0')
        && !path.contains('\0')
        && !root.split('/').any(|part| matches!(part, "." | ".."))
        && path == format!("{}/repos/{repo}", root.trim_end_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn provisioning_exhaustion_uses_current_inputs_and_socket_epoch() {
        let mut settings = ProjectSettings::default();
        let mut row = ProvisionRetryState {
            attempts: 5,
            inputs_digest: job_inputs_digest(&settings),
            connection_id: 42,
        };
        assert!(row.exhausted(&settings, Some(42)));
        assert!(row.exhausted(&settings, None));
        assert!(!row.exhausted(&settings, Some(43)));
        settings.max_active_tasks += 1;
        assert!(
            row.exhausted(&settings, Some(42)),
            "unrelated settings are not job inputs"
        );
        settings.placement.provision_timeout_seconds += 1;
        assert!(!row.exhausted(&settings, Some(42)));
        row.inputs_digest = job_inputs_digest(&settings);
        row.attempts = 4;
        assert!(!row.exhausted(&settings, Some(42)));
    }

    #[tokio::test]
    async fn transactional_job_input_fence_ignores_unrelated_settings_edits() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = db::SqliteDb::new(pool);
        let id = db::new_uuid_v4();
        let now = db::now_rfc3339();
        db::ProjectRepo::create(
            &db,
            db::CreateProject {
                id: id.clone(),
                name: "fence".into(),
                settings: "{}".into(),
                workflow_definition: "{}".into(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        let settings = ProjectSettings::default();
        let digest = job_inputs_digest(&settings);
        sqlx::query("UPDATE project SET settings=? WHERE id=?")
            .bind(serde_json::json!({"max_active_tasks":23}).to_string())
            .bind(&id)
            .execute(db.pool())
            .await
            .unwrap();
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        fence_job_inputs(&mut tx, &id, &digest).await.unwrap();
        tx.rollback().await.unwrap();
        sqlx::query("UPDATE project SET settings=? WHERE id=?")
            .bind(serde_json::json!({"placement":{"provision":"never"}}).to_string())
            .bind(&id)
            .execute(db.pool())
            .await
            .unwrap();
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        assert!(matches!(
            fence_job_inputs(&mut tx, &id, &digest).await,
            Err(ServiceError::Db(db::DbError::VersionConflict))
        ));
    }

    #[test]
    fn provision_path_validation_uses_the_daemons_path_syntax() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        assert!(managed_clone_path_matches(
            root.to_str().unwrap(),
            root.join("repos/repo-id").to_str().unwrap(),
            "repo-id"
        ));
        assert!(managed_clone_path_matches(
            r"C:\forge",
            r"C:\forge\repos\repo-id",
            "repo-id"
        ));
        assert!(managed_clone_path_matches(
            r"\\owner\share\forge",
            r"\\owner\share\forge\repos\repo-id",
            "repo-id"
        ));
        for (root, path) in [
            ("relative", "relative/repos/repo-id"),
            ("/root/../other", "/root/../other/repos/repo-id"),
            ("/root", "/other/repos/repo-id"),
            ("/root", "/root/repos/another-id"),
        ] {
            assert!(!managed_clone_path_matches(root, path, "repo-id"));
        }
    }

    #[tokio::test]
    async fn provisioning_flight_releases_when_job_panics_and_backoff_is_bounded() {
        let key = (db::new_uuid_v4(), db::new_uuid_v4());
        FLIGHTS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap()
            .insert(key.clone());
        let running_key = key.clone();
        let job = tokio::spawn(async move {
            let _guard = Flight(running_key);
            panic!("job died");
        });
        assert!(job.await.unwrap_err().is_panic());
        assert!(FLIGHTS.get().unwrap().lock().unwrap().insert(key.clone()));
        drop(Flight(key));
        for attempt in [0, 1, 2, 30] {
            let next =
                chrono::DateTime::parse_from_rfc3339(&environment_retry_at(attempt)).unwrap();
            assert!(next <= chrono::Utc::now() + chrono::Duration::seconds(601));
            assert!(next > chrono::Utc::now());
        }
    }
}
