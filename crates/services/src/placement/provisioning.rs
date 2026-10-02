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
    let mut tx = db::begin_immediate(job.db.pool()).await?;
    let next = environment_retry_at(1);
    // Claim a due attempt and schedule its successor before starting I/O. A
    // response loss or process death can only delay work to a bounded deadline.
    let attempts: Option<i64> = sqlx::query_scalar("INSERT INTO repo_provision_retry (repo_id, runtime_id, attempts, next_attempt_at) VALUES (?, ?, 1, ?) ON CONFLICT(repo_id, runtime_id) DO UPDATE SET attempts = attempts + 1, next_attempt_at = excluded.next_attempt_at WHERE julianday(next_attempt_at) <= julianday(?) RETURNING attempts")
        .bind(&job.repo.id).bind(runtime).bind(next).bind(db::now_rfc3339()).fetch_optional(&mut *tx).await?;
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
        if let Err(error) = job.run().await {
            tracing::warn!(repo_id=%job.repo.id,%error,"repository provisioning attempt failed; retaining readiness and retrying with backoff");
            if let Ok(Some(location_id)) = sqlx::query_scalar::<_, Option<String>>(
                "SELECT location_id FROM repo_provision_retry WHERE repo_id = ? AND runtime_id = ?",
            )
            .bind(&job.repo.id)
            .bind(&job.candidate.location.runtime_id)
            .fetch_optional(job.db.pool())
            .await
            .map(Option::flatten)
            {
                if let Ok(Some(location)) = RepoLocationRepo::get_by_id(&job.db, &location_id).await
                {
                    if location.status != db::RepoLocationStatus::Ready {
                        let _ = RepoLocationRepo::update(
                            &job.db,
                            db::UpdateRepoLocation {
                                id: location.id,
                                expected_version: location.version,
                                path: None,
                                kind: None,
                                is_default: None,
                                status: None,
                                last_verified_at: None,
                                last_error: Some(Some(
                                    crate::project_environment::bounded_output_tail(
                                        &error.to_string(),
                                    ),
                                )),
                                updated_at: db::now_rfc3339(),
                            },
                        )
                        .await;
                    }
                }
            }
            let _ = sqlx::query("UPDATE repo_provision_retry SET last_error = ?, next_attempt_at = ? WHERE repo_id = ? AND runtime_id = ?")
                .bind(crate::project_environment::bounded_output_tail(&error.to_string())).bind(environment_retry_at(attempts))
                .bind(&job.repo.id).bind(&job.candidate.location.runtime_id).execute(job.db.pool()).await;
        }
        drop(guard);
        job.kick.notify_one();
    });
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
        if settings.placement.provision != PlacementProvision::WhenVerified {
            return Err(ServiceError::invalid_operation(
                "Project provisioning is disabled",
            ));
        }
        Ok(settings)
    }

    async fn run(&self) -> Result<()> {
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
        let results = environment::run_checks(&target, &environment, &checks).await?;
        let machine_passed = results.iter().all(|result| {
            result.passed
                || checks
                    .iter()
                    .find(|check| check.name == result.name)
                    .is_some_and(|check| !check.applies_to(&self.role))
        });
        let mut row = environment::unknown_record(&self.project.id, machine.clone(), &environment);
        row.scope_covered = "machine".into();
        let row = self
            .db
            .put_readiness(row, observed.map(|row| row.version))
            .await?;
        let saved = environment::save_results(&self.db, row, &environment, results).await?;
        if !machine_passed {
            return Ok(());
        }
        // Fence settings immediately before moving any code to the machine.
        let current = self.settings().await?;
        if db::environment_checks_digest(&current.environment) != saved.checks_digest {
            return Err(db::DbError::VersionConflict.into());
        }
        let now = db::now_rfc3339();
        let mut tx = db::begin_immediate(self.db.pool()).await?;
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
        let provision = client
            .provision_location(
                daemon,
                api_types::RepoLocationProvisionParams {
                    daemon_id: daemon.clone(),
                    runtime_id: runtime.clone(),
                    repo_id: self.repo.id.clone(),
                    remote_url: self.repo.remote_url.clone().ok_or(db::DbError::NotFound)?,
                },
            )
            .await;
        match provision {
            Ok(result) if result.path == location.path && !result.default_branch.is_empty() => {}
            result => {
                let error = result.err().map(client_error).unwrap_or_else(|| {
                    ServiceError::invalid_operation(
                        "daemon returned a different managed clone path",
                    )
                });
                RepoLocationRepo::update(
                    &self.db,
                    db::UpdateRepoLocation {
                        path: None,
                        kind: None,
                        id: location.id,
                        is_default: None,
                        status: Some(db::RepoLocationStatus::Unavailable),
                        last_verified_at: None,
                        last_error: Some(Some(crate::project_environment::bounded_output_tail(
                            &error.to_string(),
                        ))),
                        expected_version: location.version,
                        updated_at: db::now_rfc3339(),
                    },
                )
                .await?;
                return Err(error);
            }
        }
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
        let results = environment::run_checks(&full, &environment, &environment.checks).await?;
        let mut row = self
            .db
            .get_readiness(&self.project.id, &machine)
            .await?
            .ok_or(db::DbError::NotFound)?;
        row.scope_covered = "full".into();
        let saved = environment::save_results(&self.db, row, &environment, results).await?;
        // A verified checkout can enter selection even when a full check fails:
        // readiness filters the role and scheduled re-checks own recovery.
        location.status = db::RepoLocationStatus::Ready;
        RepoLocationRepo::update(
            &self.db,
            db::UpdateRepoLocation {
                path: None,
                kind: None,
                id: location.id,
                expected_version: location.version,
                is_default: None,
                status: Some(location.status),
                last_verified_at: Some(Some(db::now_rfc3339())),
                last_error: Some(None),
                updated_at: db::now_rfc3339(),
            },
        )
        .await?;
        sqlx::query("DELETE FROM repo_provision_retry WHERE repo_id = ? AND runtime_id = ?")
            .bind(&self.repo.id)
            .bind(runtime)
            .execute(self.db.pool())
            .await?;
        environment::clear_matching_pause(&self.db, &self.events, &self.project, &saved).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
