//! Environment admission and owner-local probes outside workspace reservation.
use crate::{
    project_environment::{bounded_output_tail, next_check_at},
    workspace_backend::{RunSpec, WorkspaceBackend, WorkspaceBackendError, WorkspaceRunPurpose},
    Result, ServiceError,
};
use api_types::{EnvironmentCheck, ProjectEnvironment};
use db::{
    EnvironmentMachine, EnvironmentReadinessStatus, ProjectMachineReadiness,
    ProjectMachineReadinessRepo, ReadinessCheckFailure, ReadinessCheckResult, SqliteDb,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
};

#[derive(Clone)]
pub(crate) enum ProbeTarget {
    Server(PathBuf),
    Machine {
        machine: EnvironmentMachine,
        client: crate::daemon_transport::workspace_client::DaemonWorkspaceClient,
        location_id: Option<String>,
    },
    /// Only scheduled re-checks use this, and only for the failure's workspace.
    Daemon {
        placement: Box<db::WorkspacePlacement>,
        backend: Arc<dyn WorkspaceBackend>,
    },
}
type FlightKey = (String, EnvironmentMachine);
pub(crate) fn retry_deadline(attempt: i64) -> String {
    next_check_at(
        chrono::Utc::now(),
        (30_u64.saturating_mul(1_u64 << attempt.clamp(0, 5))).min(600),
    )
}
static FLIGHTS: OnceLock<Mutex<HashSet<FlightKey>>> = OnceLock::new();
pub(crate) struct ProbeGuard(FlightKey);
impl Drop for ProbeGuard {
    fn drop(&mut self) {
        FLIGHTS
            .get()
            .expect("probe flights")
            .lock()
            .expect("probe lock")
            .remove(&self.0);
    }
}
pub(crate) fn claim_probe(project: &str, machine: &EnvironmentMachine) -> Option<ProbeGuard> {
    let key = (project.into(), machine.clone());
    let mut flights = FLIGHTS
        .get_or_init(Mutex::default)
        .lock()
        .expect("probe lock");
    if !flights.insert(key.clone()) {
        return None;
    }
    Some(ProbeGuard(key))
}
pub(crate) fn unknown_record(
    project: &str,
    machine: EnvironmentMachine,
    environment: &ProjectEnvironment,
) -> ProjectMachineReadiness {
    ProjectMachineReadiness {
        project_id: project.into(),
        machine,
        status: EnvironmentReadinessStatus::Unknown,
        checks_digest: db::environment_checks_digest(environment),
        failing_checks: vec![],
        check_results: vec![],
        output_tail: String::new(),
        scope_covered: "full".into(),
        role: None,
        workspace_id: None,
        checked_at: None,
        next_check_at: None,
        version: 1,
    }
}
pub(crate) fn start_probe(
    db: SqliteDb,
    project: String,
    environment: ProjectEnvironment,
    observed: Option<ProjectMachineReadiness>,
    target: ProbeTarget,
    events: Arc<EventBus>,
    kick: Arc<tokio::sync::Notify>,
) {
    if environment.checks.is_empty() || !environment.assets.is_empty() {
        return;
    }
    let machine = match &target {
        ProbeTarget::Server(_) => EnvironmentMachine::Server,
        ProbeTarget::Machine { machine, .. } => machine.clone(),
        ProbeTarget::Daemon { .. } => return,
    };
    if observed
        .as_ref()
        .filter(|row| row.checks_digest == db::environment_checks_digest(&environment))
        .and_then(|row| row.next_check_at.as_ref())
        .is_some_and(|next| {
            chrono::DateTime::parse_from_rfc3339(next).is_ok_and(|next| next > chrono::Utc::now())
        })
    {
        return;
    }
    let Some(guard) = claim_probe(&project, &machine) else {
        return;
    };
    tokio::spawn(async move {
        let mut attempt = None;
        let result = async {
            let snapshot = db::ProjectRepo::get_by_id(&db, &project)
                .await?
                .ok_or(db::DbError::NotFound)?;
            let row = db
                .put_readiness(
                    unknown_record(&project, machine.clone(), &environment),
                    observed.map(|row| row.version),
                )
                .await?;
            // Persist a deadline before I/O. A panicking/aborted flight can be
            // retried by admission after this deadline, including after restart.
            let retry = next_check_at(chrono::Utc::now(), 30);
            if !db.reschedule_readiness(&row, &retry).await? {
                return Err(db::DbError::VersionConflict.into());
            }
            let mut row = row;
            row.version += 1;
            row.next_check_at = Some(retry);
            attempt = Some(row.clone());
            let results = run_checks(&target, &environment, &environment.checks).await?;
            let saved = save_results(&db, row, &environment, results).await?;
            if saved.status == EnvironmentReadinessStatus::Ready {
                clear_matching_pause(&db, &events, &snapshot, &saved).await?;
            }
            Ok::<_, ServiceError>(())
        }
        .await;
        if let Err(error) = result {
            if !matches!(error, ServiceError::Db(db::DbError::VersionConflict)) {
                tracing::warn!(%project, %error, "environment probe failed");
                if let Some(row) = attempt.as_ref() {
                    if db
                        .reschedule_readiness(row, &next_check_at(chrono::Utc::now(), 30))
                        .await
                        .unwrap_or(false)
                    {
                        let mut fenced = row.clone();
                        fenced.version += 1;
                        let _ = record_probe_error(&db, &fenced, &error.to_string()).await;
                    }
                }
            }
        }
        // Release single flight before the wake can start a new digest.
        drop(guard);
        // An edit may have tried to start its probe while this older flight was
        // running. Continue with the current digest even when no Task is queued;
        // the dispatcher kick alone cannot discover an unknown row without work.
        if let Ok(Some(latest)) = db::ProjectRepo::get_by_id(&db, &project).await {
            if let Ok(settings) =
                serde_json::from_str::<api_types::ProjectSettings>(&latest.settings)
            {
                if !settings.environment.checks.is_empty()
                    && db::environment_checks_digest(&settings.environment)
                        != db::environment_checks_digest(&environment)
                {
                    match db.get_readiness(&project, &machine).await {
                        Ok(observed)
                            if observed.as_ref().is_none_or(|row| {
                                row.status == EnvironmentReadinessStatus::Unknown
                            }) =>
                        {
                            start_probe(
                                db.clone(),
                                project.clone(),
                                settings.environment,
                                observed,
                                target,
                                events.clone(),
                                kick.clone(),
                            );
                        }
                        Ok(_) => {} // A newer authoritative result already won.
                        Err(error) => {
                            tracing::warn!(%project,%error,"could not schedule the current environment digest")
                        }
                    }
                }
            }
        }
        kick.notify_one();
    });
}
async fn record_probe_error(
    db: &SqliteDb,
    row: &ProjectMachineReadiness,
    error: &str,
) -> Result<()> {
    let mut tx = db::begin_immediate(db.pool()).await?;
    let rows = db.list_readiness_in_tx(&mut tx, &row.project_id).await?;
    if !rows.iter().any(|current| {
        current.machine == row.machine
            && current.version == row.version
            && current.checks_digest == row.checks_digest
    }) {
        return Ok(());
    }
    let raw: String = sqlx::query_scalar("SELECT settings FROM project WHERE id=?")
        .bind(&row.project_id)
        .fetch_one(&mut *tx)
        .await?;
    let settings: api_types::ProjectSettings = serde_json::from_str(&raw)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    if db::environment_checks_digest(&settings.environment) != row.checks_digest {
        return Ok(());
    }
    let error = bounded_output_tail(&executors::environment::redact_environment_values(
        error,
        &settings.environment.env,
    ));
    let machine = serde_json::to_value(&row.machine)
        .expect("machine")
        .to_string();
    // Only matching Task waits receive diagnostics; readiness facts are unchanged.
    db::task_writer::BulkTaskQuery::new(db,"UPDATE task SET metadata_json=json_set(metadata_json,'$.environment_wait.last_error',?,'$.deferred_dispatch.reason',COALESCE(json_extract(metadata_json,'$.deferred_dispatch.reason'),'environment_probe_pending') || '; last failure: ' || ?) WHERE project_id=? AND json_valid(metadata_json) AND json_extract(metadata_json,'$.environment_wait.machine')=json(?) AND json_extract(metadata_json,'$.environment_wait.kind')='environment_probe_pending'")
        .bind(&error).bind(&error).bind(&row.project_id).bind(machine).execute_in_tx(&mut tx).await?;
    tx.commit().await?;
    Ok(())
}
pub(crate) async fn run_checks(
    target: &ProbeTarget,
    environment: &ProjectEnvironment,
    checks: &[EnvironmentCheck],
) -> Result<Vec<api_types::ProjectEnvironmentCheckResult>> {
    if let ProbeTarget::Machine {
        machine:
            EnvironmentMachine::Daemon {
                daemon_id,
                runtime_id,
            },
        client,
        location_id,
    } = target
    {
        let reply = client
            .machine_probe(
                daemon_id,
                api_types::MachineProbeParams {
                    daemon_id: daemon_id.clone(),
                    runtime_id: runtime_id.clone(),
                    repo_location_id: location_id.clone(),
                    commands: checks
                        .iter()
                        .map(|check| api_types::MachineProbeCommand {
                            name: check.name.clone(),
                            command: check.command.clone(),
                            timeout_seconds: check.timeout_seconds.clamp(1, 300),
                        })
                        .collect(),
                    env: environment.env.clone(),
                },
            )
            .await
            .map_err(super::provisioning::client_error)?;
        if reply.results.len() != checks.len()
            || reply
                .results
                .iter()
                .zip(checks)
                .any(|(result, check)| result.name != check.name)
        {
            return Err(ServiceError::invalid_operation(
                "machine.probe returned different checks",
            ));
        }
        return Ok(reply
            .results
            .into_iter()
            .map(|result| api_types::ProjectEnvironmentCheckResult {
                name: result.name,
                passed: result.exit_code == Some(0) && !result.timed_out,
                exit_code: result.exit_code,
                output_tail: bounded_output_tail(
                    &executors::environment::redact_environment_values(
                        &if result.timed_out {
                            format!("{}\ncheck timed out", result.output_tail)
                        } else {
                            result.output_tail
                        },
                        &environment.env,
                    ),
                ),
            })
            .collect());
    }
    let mut results = Vec::new();
    for check in checks {
        let spec = RunSpec {
            purpose: WorkspaceRunPurpose::EnvironmentSetup,
            command: check.command.clone(),
            env: environment.env.clone(),
            timeout_secs: check.timeout_seconds.clamp(1, 300),
            max_output_bytes: 4096,
        };
        let result = match target {
            ProbeTarget::Server(path) => {
                crate::workspace_backend::run_environment_checkout(path, &spec).await
            }
            ProbeTarget::Daemon { placement, backend } => backend.run(placement, &spec).await,
            ProbeTarget::Machine { .. } => {
                return Err(ServiceError::invalid_operation("invalid probe owner"))
            }
        };
        let (passed, exit_code, output) = match result {
            Ok(result) => (
                result.exit_code == 0,
                (result.exit_code >= 0).then_some(result.exit_code),
                format!("{}{}", result.stdout_tail, result.stderr_tail),
            ),
            Err(WorkspaceBackendError::Other(error)) if matches!(&*error, ServiceError::InvalidOperation { message } if message == "review command timed out") => {
                (false, None, "check timed out".into())
            }
            Err(error) => return Err(error.into()), // Transport/fence/policy is not a check result.
        };
        results.push(api_types::ProjectEnvironmentCheckResult {
            name: check.name.clone(),
            passed,
            exit_code,
            output_tail: bounded_output_tail(&executors::environment::redact_environment_values(
                &output,
                &environment.env,
            )),
        });
    }
    Ok(results)
}
pub(crate) async fn save_results(
    db: &SqliteDb,
    mut row: ProjectMachineReadiness,
    environment: &ProjectEnvironment,
    results: Vec<api_types::ProjectEnvironmentCheckResult>,
) -> Result<ProjectMachineReadiness> {
    // Never attach results from one check snapshot to another digest, even if
    // the caller accidentally supplies a freshly re-read readiness row.
    if row.checks_digest != db::environment_checks_digest(environment) {
        return Err(db::DbError::VersionConflict.into());
    }
    let version = row.version;
    // Retain results for checks not re-run by a due failing-check re-check.
    for result in results {
        row.check_results.retain(|old| old.name != result.name);
        row.check_results.push(ReadinessCheckResult {
            name: result.name,
            passed: result.passed,
            exit_code: result.exit_code,
            output_tail: bounded_output_tail(&result.output_tail),
        });
    }
    row.failing_checks = row
        .check_results
        .iter()
        .filter(|result| !result.passed)
        .map(|result| ReadinessCheckFailure {
            name: result.name.clone(),
            output_tail: result.output_tail.clone(),
        })
        .collect();
    row.output_tail = bounded_output_tail(
        &row.check_results
            .iter()
            .map(|result| format!("{}: {}", result.name, result.output_tail))
            .collect::<Vec<_>>()
            .join("\n"),
    );
    row.status = if row.failing_checks.is_empty() {
        EnvironmentReadinessStatus::Ready
    } else {
        EnvironmentReadinessStatus::NotReady
    };
    let now = chrono::Utc::now();
    row.checked_at = Some(now.to_rfc3339());
    row.next_check_at = (row.status == EnvironmentReadinessStatus::NotReady)
        .then(|| next_check_at(now, environment.recheck_interval_seconds));
    Ok(db.put_readiness(row, Some(version)).await?)
}
/// Re-read only after a Project-version CAS loss. Keep the original pause
/// epoch and result version: a name edit can retry, a new pause or fact cannot.
pub(crate) async fn clear_matching_pause(
    db: &SqliteDb,
    events: &EventBus,
    original: &db::Project,
    result: &ProjectMachineReadiness,
) -> Result<bool> {
    let Some(current) = db::ProjectRepo::get_by_id(db, &original.id).await? else {
        return Ok(false);
    };
    let Some(epoch) = current.paused_at.as_deref() else {
        return Ok(false);
    };
    let mut snapshot = current.clone();
    loop {
        if snapshot.system_pause_reason.as_deref()
            != Some(crate::project_environment::ENVIRONMENT_NOT_READY)
            || snapshot.paused_at.as_deref() != Some(epoch)
        {
            return Ok(false);
        }
        let Some(repo) = snapshot.primary_repo_id.as_deref() else {
            return Ok(false);
        };
        let settings: api_types::ProjectSettings = match serde_json::from_str(&snapshot.settings) {
            Ok(settings) => settings,
            Err(_) => return Ok(false),
        };
        let pause: api_types::ProjectEnvironmentPause = match snapshot
            .environment_pause_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
        {
            Ok(Some(pause)) => pause,
            _ => return Ok(false),
        };
        if pause.checks.is_empty() {
            return Ok(false);
        } // Only resume/Check now clears unnamed setup failures.
        if result.failing_checks.iter().any(|failure| {
            settings
                .environment
                .checks
                .iter()
                .find(|check| check.name == failure.name)
                .is_none_or(|check| {
                    pause
                        .role
                        .as_ref()
                        .is_none_or(|role| check.applies_to(role))
                })
        }) {
            return Ok(false);
        }
        if db::environment_checks_digest(&settings.environment) != result.checks_digest
            || db
                .get_readiness(&snapshot.id, &result.machine)
                .await?
                .is_none_or(|row| row.version != result.version)
        {
            return Ok(false);
        }
        if db::ProjectRepo::clear_system_pause_if_unchanged(
            db,
            &snapshot.id,
            snapshot.version,
            repo,
            epoch,
            crate::project_environment::ENVIRONMENT_NOT_READY,
        )
        .await?
        {
            events.publish(ForgeEvent {
                event_type: "project.resumed".into(),
                entity_id: snapshot.id,
                timestamp: event_timestamp(),
                context: EventContext::ProjectResumed {},
            });
            return Ok(true);
        }
        let Some(latest) = db::ProjectRepo::get_by_id(db, &snapshot.id).await? else {
            return Ok(false);
        };
        if latest.version == snapshot.version {
            return Ok(false);
        } // e.g. the primary Repo is absent.
        snapshot = latest;
    }
}

pub(crate) async fn start_context_probes(
    db: &SqliteDb,
    context: &super::SelectionContext,
    registry: &Arc<crate::daemon_transport::DaemonConnectionRegistry>,
    events: &Arc<EventBus>,
    kick: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let project = db::ProjectRepo::get_by_id(db, &context.task.project_id)
        .await?
        .ok_or(db::DbError::NotFound)?;
    let settings: api_types::ProjectSettings = serde_json::from_str(&project.settings)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let mut provision_scheduled = false;
    let mut candidates = context.candidates.iter().collect::<Vec<_>>();
    candidates.sort_by_key(|candidate| (&candidate.location.created_at, &candidate.location.id));
    for candidate in candidates {
        if candidate.provisioning {
            if !provision_scheduled
                && !super::selection::ready_location_can_run(context)
                && super::selection::filter_candidate(context, candidate)
                    .iter()
                    .all(|code| {
                        matches!(
                            code,
                            super::PlacementFilterCode::EnvironmentProbePending
                                | super::PlacementFilterCode::LocationNotReady
                        )
                    })
            {
                provision_scheduled = true;
                if let Err(error) = super::provisioning::start(super::provisioning::Job {
                    db: db.clone(),
                    repo: context.repo.clone(),
                    project: project.clone(),
                    candidate: candidate.clone(),
                    registry: registry.clone(),
                    events: events.clone(),
                    kick: kick.clone(),
                    role: context.claiming_agent.role.clone(),
                })
                .await
                {
                    tracing::warn!(project_id=%project.id,%error,"provisioning setup failed; continuing other probes");
                }
            }
            continue;
        }
        if candidate.location.status != db::RepoLocationStatus::Ready
            || !candidate.connected
            || !candidate.visible
            || super::selection::environment_filter(context, candidate)
                != Some(super::PlacementFilterCode::EnvironmentProbePending)
        {
            continue;
        }
        let target = if candidate.location.owner_kind == db::RepoLocationOwnerKind::Server {
            ProbeTarget::Server(candidate.location.path.clone().into())
        } else {
            ProbeTarget::Machine {
                machine: EnvironmentMachine::from_location(&candidate.location),
                client: crate::daemon_transport::workspace_client::DaemonWorkspaceClient::new(
                    registry.clone(),
                ),
                location_id: Some(candidate.location.id.clone()),
            }
        };
        start_probe(
            db.clone(),
            project.id.clone(),
            settings.environment.clone(),
            candidate.environment_readiness.clone(),
            target,
            events.clone(),
            kick.clone(),
        );
    }
    Ok(())
}
pub(crate) async fn schedule_project_probes(
    db: &SqliteDb,
    project: &db::Project,
    kick: Arc<tokio::sync::Notify>,
    events: Arc<EventBus>,
    registry: Option<&Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
) -> Result<()> {
    let settings: api_types::ProjectSettings = match serde_json::from_str(&project.settings) {
        Ok(settings) => settings,
        Err(_) => return Ok(()),
    };
    if settings.environment.checks.is_empty() || !settings.environment.assets.is_empty() {
        return Ok(());
    }
    if let Some(registry) = registry {
        let locations: Vec<(String, String, String)> = sqlx::query_as("SELECT l.id, l.daemon_id, l.runtime_id FROM repo_location l JOIN repo r ON r.id = l.repo_id JOIN daemon d ON d.id = l.daemon_id WHERE r.project_id = ? AND l.owner_kind = 'daemon' AND l.status = 'ready' AND (d.visibility = 'global' OR d.owner_id IS NULL OR d.owner_id = ?)").bind(&project.id).bind(&project.owner_id).fetch_all(db.pool()).await?;
        for (location_id, daemon_id, runtime_id) in locations {
            let machine = EnvironmentMachine::Daemon {
                daemon_id,
                runtime_id,
            };
            let EnvironmentMachine::Daemon { daemon_id, .. } = &machine else {
                unreachable!()
            };
            if !registry
                .connection_snapshots()
                .get(daemon_id)
                .is_some_and(|facts| {
                    facts
                        .handshake
                        .capabilities
                        .iter()
                        .any(|fact| fact == api_types::DAEMON_CAPABILITY_MACHINE_PROBE)
                        && facts
                            .handshake
                            .workspace_run_policy
                            .allowed_purposes
                            .contains(&WorkspaceRunPurpose::EnvironmentProbe)
                })
            {
                continue;
            }
            match db.get_readiness(&project.id, &machine).await {
                Ok(observed)
                    if observed.as_ref().is_none_or(|row| {
                        row.status == EnvironmentReadinessStatus::Unknown
                            || row.checks_digest
                                != db::environment_checks_digest(&settings.environment)
                    }) =>
                {
                    start_probe(db.clone(), project.id.clone(), settings.environment.clone(), observed, ProbeTarget::Machine { machine, client: crate::daemon_transport::workspace_client::DaemonWorkspaceClient::new(registry.clone()), location_id: Some(location_id) }, events.clone(), kick.clone());
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(project_id=%project.id,%error,"isolating unreadable daemon readiness row")
                }
            }
        }
    }
    let observed = db
        .get_readiness(&project.id, &EnvironmentMachine::Server)
        .await?;
    if observed.as_ref().is_some_and(|row| {
        row.checks_digest == db::environment_checks_digest(&settings.environment)
            && row.status != EnvironmentReadinessStatus::Unknown
    }) {
        return Ok(());
    }
    let path: Option<String> = sqlx::query_scalar("SELECT l.path FROM repo_location l JOIN repo r ON r.id = l.repo_id WHERE r.project_id = ? AND l.owner_kind = 'server' AND l.status = 'ready' ORDER BY (r.id = ?) DESC, l.is_default DESC, l.created_at, l.id LIMIT 1")
        .bind(&project.id).bind(&project.primary_repo_id).fetch_optional(db.pool()).await?;
    // A retained host row is also a probe target after its location is removed;
    // use the repository's server checkout rather than waiting for a new Task.
    let path = if path.is_none() && observed.is_some() {
        sqlx::query_scalar::<_, Option<String>>("SELECT local_path FROM repo WHERE project_id = ? ORDER BY (id = ?) DESC, created_at, id LIMIT 1")
            .bind(&project.id).bind(&project.primary_repo_id).fetch_optional(db.pool()).await?.flatten()
    } else {
        path
    };
    if let Some(path) = path {
        start_probe(
            db.clone(),
            project.id.clone(),
            settings.environment,
            observed,
            ProbeTarget::Server(path.into()),
            events,
            kick,
        );
    }
    Ok(())
}
pub(crate) async fn target_for_machine(
    db: &SqliteDb,
    project: &db::Project,
    machine: &EnvironmentMachine,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
    registry: Option<&Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    workspace_id: Option<&str>,
) -> Result<Option<ProbeTarget>> {
    let EnvironmentMachine::Daemon { .. } = machine else {
        return Ok(None);
    };
    if let Some(registry) = registry {
        if let EnvironmentMachine::Daemon {
            daemon_id,
            runtime_id,
        } = machine
        {
            if registry
                .connection_snapshots()
                .get(daemon_id)
                .is_some_and(|facts| {
                    facts
                        .handshake
                        .capabilities
                        .iter()
                        .any(|fact| fact == api_types::DAEMON_CAPABILITY_MACHINE_PROBE)
                        && facts
                            .handshake
                            .workspace_run_policy
                            .allowed_purposes
                            .contains(&WorkspaceRunPurpose::EnvironmentProbe)
                })
            {
                let location: Option<String> = sqlx::query_scalar("SELECT l.id FROM repo_location l JOIN repo r ON r.id = l.repo_id WHERE r.project_id = ? AND l.daemon_id = ? AND l.runtime_id = ? AND l.status = 'ready' ORDER BY l.is_default DESC, l.created_at, l.id LIMIT 1")
                    .bind(&project.id).bind(daemon_id).bind(runtime_id).fetch_optional(db.pool()).await?;
                if location.is_none() {
                    let settings: api_types::ProjectSettings =
                        serde_json::from_str(&project.settings)
                            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                    if db
                        .get_readiness(&project.id, machine)
                        .await?
                        .is_some_and(|row| {
                            row.failing_checks.iter().any(|failure| {
                                settings.environment.checks.iter().any(|check| {
                                    check.name == failure.name
                                        && check.scope
                                            == api_types::EnvironmentCheckScope::Workspace
                                })
                            })
                        })
                    {
                        return Ok(None); // Missing checkout is not a failed command.
                    }
                }
                return Ok(Some(ProbeTarget::Machine {
                    machine: machine.clone(),
                    client: crate::daemon_transport::workspace_client::DaemonWorkspaceClient::new(
                        registry.clone(),
                    ),
                    location_id: location,
                }));
            }
        }
    }
    let Some(workspace_id) = workspace_id else {
        return Ok(None);
    };
    let Some(placement) = db::WorkspacePlacementRepo::get_by_workspace_id(db, workspace_id).await?
    else {
        return Ok(None);
    };
    let workspace = db::WorkspaceRepo::get_by_id(db, workspace_id).await?;
    if EnvironmentMachine::from_placement(&placement) != *machine
        || placement.state != db::PlacementState::Ready
        || placement.workspace_handle.is_none()
        || workspace
            .as_ref()
            .is_none_or(|workspace| workspace.task_id != placement.task_id)
    {
        return Ok(None);
    }
    let task = db::TaskRepo::get_by_id(db, &placement.task_id, false).await?;
    if task
        .as_ref()
        .is_none_or(|task| task.project_id != project.id)
    {
        return Ok(None);
    }
    Ok(Some(ProbeTarget::Daemon {
        backend: router.for_placement(&placement)?,
        placement: Box::new(placement),
    }))
}
pub(crate) fn machine_label(machine: &EnvironmentMachine) -> String {
    match machine {
        EnvironmentMachine::Server => "server".into(),
        EnvironmentMachine::Daemon {
            daemon_id,
            runtime_id,
        } => format!("{daemon_id}/{runtime_id}"),
    }
}

/// The same task/role-specific decision is used before state entry and after
/// a launch failure. Pin/placement restrictions cannot pause unrelated work.
pub(crate) async fn handle_refusal(
    db: &SqliteDb,
    events: &EventBus,
    task: &db::Task,
    project: &db::Project,
    context: &super::SelectionContext,
    refusal: &super::PlacementUnavailable,
    registry: Option<&crate::daemon_transport::DaemonConnectionRegistry>,
) -> Result<bool> {
    use super::PlacementFilterCode::*;
    if super::machine_precheck::capacity_only_wait(refusal) {
        return Ok(false);
    }
    let unfit = super::selection::environment_pause_candidates(context);
    if !unfit.is_empty() {
        let candidate = context
            .binding()
            .and_then(|binding| {
                unfit
                    .iter()
                    .copied()
                    .find(|candidate| candidate.location.id == binding.repo_location_id)
            })
            .unwrap_or(unfit[0]);
        let machine = EnvironmentMachine::from_location(&candidate.location);
        let row = candidate
            .environment_readiness
            .as_ref()
            .expect("current not-ready row");
        let now = row
            .checked_at
            .clone()
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
        let versions: Vec<_> = unfit.iter().filter_map(|candidate| candidate.environment_readiness.as_ref()).map(|row| {
            let (kind, daemon, runtime) = row.machine.columns(); serde_json::json!({"owner_kind":kind,"daemon_id":daemon,"runtime_id":runtime,"version":row.version})
        }).collect();
        let detail = serde_json::json!({"machine":machine,"workspace_id":row.workspace_id,"checks":super::selection::applicable_failing_checks(context,candidate),
            "role":context.claiming_agent.role, "output":row.output_tail,
            "paused_at":now,"last_checked_at":row.checked_at.as_ref().unwrap_or(&now),"next_check_at":row.next_check_at.as_ref().unwrap_or(&now),"readiness_versions":versions});
        if db::ProjectRepo::set_environment_pause_if_unchanged(
            db,
            &project.id,
            project.version,
            &now,
            &detail.to_string(),
        )
        .await?
        {
            events.publish(ForgeEvent {
                event_type: "project.paused".into(),
                entity_id: project.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::ProjectPaused { paused_at: now },
            });
        }
        return Ok(true); // A CAS loser retries fresh; never enter a failed transition.
    }
    if let Some(rejection) = refusal
        .rejected_candidates
        .iter()
        .find(|candidate| candidate.filter_codes == [EnvironmentNotReady])
    {
        let specific = context.binding().is_some()
            || context.agents().any(|role| role.agent.daemon_id.is_some());
        if specific {
            let machine = if rejection.owner_kind == "server" {
                EnvironmentMachine::Server
            } else {
                EnvironmentMachine::Daemon {
                    daemon_id: rejection.daemon_id.clone().unwrap_or_default(),
                    runtime_id: rejection.runtime_id.clone().unwrap_or_default(),
                }
            };
            persist_machine_wait(db, task, &machine, &rejection.failing_checks).await?;
            return Ok(true);
        }
    }
    Ok(defer_refusal(
        db,
        task,
        &ServiceError::PlacementUnavailable(refusal.clone()),
        registry,
    )
    .await?
    .unwrap_or(false))
}
pub(crate) async fn defer_refusal(
    db: &SqliteDb,
    task: &db::Task,
    error: &ServiceError,
    registry: Option<&crate::daemon_transport::DaemonConnectionRegistry>,
) -> Result<Option<bool>> {
    if matches!(
        error,
        ServiceError::DaemonUnavailable { .. } | ServiceError::DaemonTimeout { .. }
    ) {
        let metadata = db::TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        if let Some(wait) = metadata.extra.get("environment_wait") {
            if let Ok(machine) =
                serde_json::from_value::<EnvironmentMachine>(wait["machine"].clone())
            {
                if let Some(row) = db.get_readiness(&task.project_id, &machine).await? {
                    if let Some(project) = db::ProjectRepo::get_by_id(db, &task.project_id).await? {
                        let settings: api_types::ProjectSettings =
                            serde_json::from_str(&project.settings).map_err(|error| {
                                ServiceError::invalid_operation(error.to_string())
                            })?;
                        if row.status == EnvironmentReadinessStatus::NotReady
                            && row.checks_digest
                                == db::environment_checks_digest(&settings.environment)
                        {
                            let checks =
                                serde_json::from_value::<Vec<String>>(wait["checks"].clone())
                                    .unwrap_or_default();
                            persist_machine_wait(db, task, &machine, &checks).await?;
                            return Ok(Some(true));
                        }
                    }
                }
            }
        }
    }
    let ServiceError::PlacementUnavailable(refusal) = error else {
        return Ok(None);
    };
    if super::machine_precheck::capacity_only_wait(refusal) {
        return Ok(None);
    }

    if db::ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .is_some_and(|project| project.paused_at.is_some())
    {
        return Ok(Some(true));
    }
    let probe_pending = |candidate: &super::CandidateRejection| {
        candidate
            .filter_codes
            .contains(&super::PlacementFilterCode::EnvironmentProbePending)
            && super::retryable_filter_codes(&candidate.filter_codes)
    };
    let pending = refusal.rejected_candidates.iter().any(probe_pending);
    if let Some(candidate) = refusal.rejected_candidates.iter().find(|candidate| {
        !pending
            && candidate
                .filter_codes
                .contains(&super::PlacementFilterCode::EnvironmentNotReady)
            && super::retryable_filter_codes(&candidate.filter_codes)
    }) {
        let machine = if candidate.owner_kind == "server" {
            EnvironmentMachine::Server
        } else {
            EnvironmentMachine::Daemon {
                daemon_id: candidate.daemon_id.clone().unwrap_or_default(),
                runtime_id: candidate.runtime_id.clone().unwrap_or_default(),
            }
        };
        persist_machine_wait(db, task, &machine, &candidate.failing_checks).await?;
        return Ok(Some(true));
    }
    if !refusal.rejected_candidates.iter().any(&probe_pending) {
        return Ok(None);
    }
    let candidate = refusal
        .rejected_candidates
        .iter()
        .find(|candidate| probe_pending(candidate))
        .expect("environment wait");
    let machine = if candidate.owner_kind == "server" {
        EnvironmentMachine::Server
    } else {
        EnvironmentMachine::Daemon {
            daemon_id: candidate.daemon_id.clone().unwrap_or_default(),
            runtime_id: candidate.runtime_id.clone().unwrap_or_default(),
        }
    };
    let kind = "environment_probe_pending";
    let label = machine_name(db, &machine).await?;
    let progress = provisioning_wait_detail(db, &refusal.repo_id, &machine, registry).await?;
    let current: Option<String> = sqlx::query_scalar("SELECT metadata_json FROM task WHERE id=?")
        .bind(&task.id)
        .fetch_optional(db.pool())
        .await?
        .flatten();
    let previous: serde_json::Value = current
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default();
    let last_error = (previous["environment_wait"]["machine"]
        == serde_json::to_value(&machine).expect("machine"))
    .then(|| previous["environment_wait"]["last_error"].as_str())
    .flatten();
    let diagnostic = last_error
        .map(|error| format!("; last failure: {error}"))
        .unwrap_or_default();
    let marker = serde_json::json!({"kind":kind,"reason":format!("{kind}: {label}{progress}{diagnostic}"),"target_state":task.status,"not_before":(chrono::Utc::now()+chrono::Duration::seconds(30)).to_rfc3339()});
    let mut wait = serde_json::json!({"machine":machine,"checks":[],"kind":kind});
    if let Some(error) = last_error {
        wait["last_error"] = serde_json::json!(error);
    }
    let mut tx = db::begin_immediate(db.pool()).await?;
    // If completion won this race, leave no delayed marker behind.
    let rows = db.list_readiness_in_tx(&mut tx, &task.project_id).await?;
    if rows.iter().any(|row| {
        row.machine == machine
            && row.scope_covered == "full"
            && row.status != EnvironmentReadinessStatus::Unknown
    }) {
        return Ok(Some(true));
    }
    let changed = db::task_writer::TaskQuery::new(db,&task.id,"UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.deferred_dispatch', json(?), '$.environment_wait', json(?)) WHERE id = ? AND version = ?")
        .bind(marker.to_string()).bind(wait.to_string()).bind(&task.id).bind(task.version).execute_in_tx(&mut tx).await?;
    if changed.rows_affected() != 1 {
        return Err(db::DbError::VersionConflict.into());
    }
    tx.commit().await?;
    Ok(Some(true))
}
pub(crate) async fn machine_name(db: &SqliteDb, machine: &EnvironmentMachine) -> Result<String> {
    match machine {
        EnvironmentMachine::Server => Ok("server".into()),
        EnvironmentMachine::Daemon { daemon_id, .. } => Ok(sqlx::query_scalar::<_, String>(
            "SELECT hostname FROM daemon WHERE id = ?",
        )
        .bind(daemon_id)
        .fetch_optional(db.pool())
        .await?
        .unwrap_or_else(|| "daemon machine".into())),
    }
}

async fn provisioning_wait_detail(
    db: &SqliteDb,
    repo: &str,
    machine: &EnvironmentMachine,
    registry: Option<&crate::daemon_transport::DaemonConnectionRegistry>,
) -> Result<String> {
    let EnvironmentMachine::Daemon {
        runtime_id,
        daemon_id,
    } = machine
    else {
        return Ok(String::new());
    };
    // One read: a provisioning job that verifies the location deletes this
    // retry row concurrently, so a second lookup could find it gone.
    type RetryDetail = (Option<String>, Option<String>, i64, String, i64);
    let detail: Option<RetryDetail> = sqlx::query_as("SELECT j.started_at, COALESCE(l.last_error, j.last_error), j.attempts, j.checks_digest, j.connection_id FROM repo_provision_retry j LEFT JOIN repo_location l ON l.id = j.location_id WHERE j.repo_id = ? AND j.runtime_id = ?").bind(repo).bind(runtime_id).fetch_optional(db.pool()).await?;
    let Some((started, error, attempts, inputs_digest, connection_id)) = detail else {
        return Ok(String::new());
    };
    let retry =
        super::provisioning::ProvisionRetryState::from((attempts, inputs_digest, connection_id));
    let raw: String = sqlx::query_scalar(
        "SELECT p.settings FROM project p JOIN repo r ON r.project_id=p.id WHERE r.id=?",
    )
    .bind(repo)
    .fetch_one(db.pool())
    .await?;
    let settings: api_types::ProjectSettings = serde_json::from_str(&raw)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let exhausted = retry.exhausted(
        &settings,
        registry
            .and_then(|registry| registry.get(daemon_id))
            .map(|connection| connection.id()),
    );
    let elapsed = started
        .as_deref()
        .and_then(|time| chrono::DateTime::parse_from_rfc3339(time).ok())
        .map(|time| {
            (chrono::Utc::now() - time.with_timezone(&chrono::Utc))
                .num_seconds()
                .max(0)
        })
        .unwrap_or(0);
    let remote: Option<String> = sqlx::query_scalar("SELECT remote_url FROM repo WHERE id=?")
        .bind(repo)
        .fetch_optional(db.pool())
        .await?
        .flatten();
    let error =
        error.map(|error| git::redact_remote_credentials(&error, remote.as_deref().unwrap_or("")));
    Ok(format!(
        "; {}{}",
        if exhausted {
            "provision_failed: retry limit reached; reconnect the machine or update its Project settings".to_owned()
        } else {
            format!("provisioning elapsed {elapsed}s")
        },
        error
            .map(|error| format!("; last failure: {error}"))
            .unwrap_or_default()
    ))
}

pub(crate) async fn persist_machine_wait(
    db: &SqliteDb,
    task: &db::Task,
    machine: &EnvironmentMachine,
    checks: &[String],
) -> Result<()> {
    let label = machine_name(db, machine).await?;
    let mut tx = db::begin_immediate(db.pool()).await?;
    let project: (Option<String>, String) =
        sqlx::query_as("SELECT paused_at, settings FROM project WHERE id = ?")
            .bind(&task.project_id)
            .fetch_one(&mut *tx)
            .await?;
    if project.0.is_some() {
        return Ok(());
    } // The Project pause is the sole signal on one machine.
    let environment: api_types::ProjectSettings = serde_json::from_str(&project.1)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let rows = db.list_readiness_in_tx(&mut tx, &task.project_id).await?;
    if rows.iter().any(|row| {
        &row.machine == machine
            && row.status == EnvironmentReadinessStatus::Ready
            && row.checks_digest == db::environment_checks_digest(&environment.environment)
    }) {
        return Ok(());
    }
    let marker = serde_json::json!({"machine":machine,"checks":checks});
    let current: Option<String> =
        sqlx::query_scalar("SELECT metadata_json FROM task WHERE id = ? AND version = ?")
            .bind(&task.id)
            .bind(task.version)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(db::DbError::VersionConflict)?;
    let unchanged = db::TaskMetadata::parse(current.as_deref())
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
        .extra
        .get("environment_wait")
        == Some(&marker);
    let deferral = serde_json::json!({"kind":"environment_not_ready","reason":format!("environment_not_ready: {} ({})",label,checks.join(", ")),"target_state":task.status,"not_before":(chrono::Utc::now()+chrono::Duration::seconds(30)).to_rfc3339()});
    db::task_writer::TaskQuery::new(db,&task.id,"UPDATE task SET metadata_json = json_set(json_remove(COALESCE(metadata_json, '{}'), '$.owner_wait'), '$.environment_wait', json(?), '$.deferred_dispatch', json(?)) WHERE id = ? AND version = ?")
        .bind(marker.to_string()).bind(deferral.to_string()).bind(&task.id).bind(task.version).execute_in_tx(&mut tx).await?;
    if !unchanged {
        let now = db::now_rfc3339();
        let event_id = db::new_uuid_v4();
        db::DomainEventRepo::append_event_in_tx(
            db,
            &mut tx,
            &db::CreateDomainEvent {
                id: event_id.clone(),
                event_type: "task.environment_wait".into(),
                entity_type: "task".into(),
                entity_id: task.id.clone(),
                actor_type: "system".into(),
                actor_id: None,
                scope_type: "project".into(),
                scope_id: task.project_id.clone(),
                correlation_id: task.id.clone(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: None,
                payload_json: marker.to_string(),
                created_at: now.clone(),
            },
        )
        .await?;
        sqlx::query("INSERT INTO attention_projection (id, attention_type, scope_type, scope_id, source_event_id, priority, status, summary, details_json, dedupe_key, occurred_at, updated_at, recommended_action) VALUES (?, 'environment_not_ready', 'project', ?, ?, 80, 'open', ?, ?, ?, ?, ?, 'wait') ON CONFLICT(dedupe_key) DO UPDATE SET status = 'open', summary = excluded.summary, details_json = excluded.details_json, source_event_id = excluded.source_event_id, resolved_at = NULL, acknowledged_at = NULL, snoozed_until = NULL, updated_at = excluded.updated_at, version = attention_projection.version + 1")
            .bind(db::new_uuid_v4()).bind(&task.project_id).bind(event_id).bind(format!("Waiting for environment on {}: {}",label,checks.join(", ")))
            .bind(serde_json::json!({"task":{"id":task.id,"title":task.title},"machine":machine,"checks":checks,"cause":"environment_not_ready"}).to_string()).bind(format!("task-environment-wait:{}",task.id)).bind(&now).bind(&now).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}
pub(crate) struct LaunchEnvironmentFailure<'a> {
    pub checks: &'a [String],
    pub output: &'a str,
    pub role: &'a str,
}
pub(crate) async fn record_launch_failure(
    db: &SqliteDb,
    project: &db::Project,
    environment: &ProjectEnvironment,
    placement: &db::WorkspacePlacement,
    failure: LaunchEnvironmentFailure<'_>,
) -> Result<()> {
    if environment.checks.is_empty() {
        return Ok(());
    }
    let machine = EnvironmentMachine::from_placement(placement);
    for _ in 0..3 {
        let previous = db.get_readiness(&project.id, &machine).await?;
        let mut row = unknown_record(&project.id, machine.clone(), environment);
        if let Some(previous) = &previous {
            if previous.checks_digest == row.checks_digest {
                row.check_results = previous.check_results.clone();
            }
        }
        row.status = EnvironmentReadinessStatus::NotReady;
        row.role = Some(failure.role.into());
        row.output_tail = bounded_output_tail(failure.output);
        row.workspace_id = Some(placement.workspace_id.clone());
        row.failing_checks = failure
            .checks
            .iter()
            .map(|name| ReadinessCheckFailure {
                name: name.clone(),
                output_tail: bounded_output_tail(failure.output),
            })
            .collect();
        for name in failure.checks {
            row.check_results.retain(|result| &result.name != name);
            row.check_results.push(ReadinessCheckResult {
                name: name.clone(),
                passed: false,
                exit_code: None,
                output_tail: bounded_output_tail(failure.output),
            });
        }
        let now = chrono::Utc::now();
        row.checked_at = Some(now.to_rfc3339());
        row.next_check_at = Some(next_check_at(now, environment.recheck_interval_seconds));
        match db.put_readiness(row, previous.map(|row| row.version)).await {
            Ok(_) => return Ok(()),
            Err(db::DbError::VersionConflict) => {
                let current = db::ProjectRepo::get_by_id(db, &project.id)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                let settings: api_types::ProjectSettings = serde_json::from_str(&current.settings)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                if db::environment_checks_digest(&settings.environment)
                    != db::environment_checks_digest(environment)
                {
                    return Ok(());
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(db::DbError::VersionConflict.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::ProjectRepo;
    #[test]
    fn environment_probe_refusal_remains_retryable_alongside_an_old_daemon() {
        use crate::placement::{CandidateRejection, PlacementFilterCode, PlacementUnavailable};
        let rejection = |codes, kind: &str| CandidateRejection {
            failing_checks: Vec::new(),
            repo_location_id: kind.into(),
            owner_kind: kind.into(),
            daemon_id: None,
            runtime_id: None,
            filter_codes: codes,
        };
        let refusal = PlacementUnavailable {
            task_id: "task".into(),
            repo_id: "repo".into(),
            rejected_candidates: vec![
                rejection(vec![PlacementFilterCode::EnvironmentProbePending], "server"),
                rejection(
                    vec![
                        PlacementFilterCode::DaemonUpgradeRequired,
                        PlacementFilterCode::CapabilityMissing,
                        PlacementFilterCode::RunPurposeDenied,
                    ],
                    "daemon",
                ),
            ],
        };
        let mut not_ready = refusal.clone();
        not_ready.rejected_candidates[0].filter_codes =
            vec![PlacementFilterCode::EnvironmentNotReady];
        assert!(!not_ready.needs_daemon_upgrade());
        assert!(crate::placement::is_retryable_admission_refusal(
            &ServiceError::PlacementUnavailable(not_ready)
        ));
        assert!(!refusal.needs_daemon_upgrade());
        assert!(crate::placement::is_retryable_admission_refusal(
            &ServiceError::PlacementUnavailable(refusal)
        ));
    }
    async fn fixture(environment: &ProjectEnvironment) -> (SqliteDb, String) {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let id = db::new_uuid_v4();
        let now = db::now_rfc3339();
        ProjectRepo::create(
            &db,
            db::CreateProject {
                id: id.clone(),
                name: "Probe".into(),
                settings: serde_json::json!({"environment":environment}).to_string(),
                workflow_definition: "{}".into(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        (db, id)
    }
    #[tokio::test]
    async fn probe_error_is_visible_redacted_and_fenced_without_changing_facts() {
        let environment:ProjectEnvironment=serde_json::from_value(serde_json::json!({"env":{"PASSWORD":"hidden-value"},"checks":[{"name":"cargo","command":"true"}]})).unwrap();
        let (db, project) = fixture(&environment).await;
        let mut row = unknown_record(&project, EnvironmentMachine::Server, &environment);
        row.status = EnvironmentReadinessStatus::Ready;
        let row = db.put_readiness(row, None).await.unwrap();
        let now = db::now_rfc3339();
        let wait=serde_json::json!({"environment_wait":{"kind":"environment_probe_pending","machine":{"owner_kind":"server"}},"deferred_dispatch":{"reason":"environment_probe_pending: server"}}).to_string();
        sqlx::query("INSERT INTO task(id,project_id,title,task_type,status,created_at,updated_at,metadata_json) VALUES('probe-wait',?,'wait','task','todo',?,?,?)").bind(&project).bind(&now).bind(&now).bind(&wait).execute(db.pool()).await.unwrap();
        record_probe_error(&db, &row, "daemon offline: hidden-value")
            .await
            .unwrap();
        crate::test_support::drain_task_steps(&db, "probe-wait").await;
        let metadata: String =
            sqlx::query_scalar("SELECT metadata_json FROM task WHERE id='probe-wait'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(metadata.contains("last failure: daemon offline"));
        assert!(!metadata.contains("hidden-value"));
        let saved = db
            .get_readiness(&project, &EnvironmentMachine::Server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.status, row.status);
        assert_eq!(saved.version, row.version);
        let newer = db
            .put_readiness(saved.clone(), Some(saved.version))
            .await
            .unwrap();
        sqlx::query("UPDATE task SET metadata_json=? WHERE id='probe-wait'")
            .bind(&wait)
            .execute(db.pool())
            .await
            .unwrap();
        record_probe_error(&db, &row, "stale failure")
            .await
            .unwrap();
        let metadata: String =
            sqlx::query_scalar("SELECT metadata_json FROM task WHERE id='probe-wait'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(metadata, wait);
        assert_eq!(
            db.get_readiness(&project, &EnvironmentMachine::Server)
                .await
                .unwrap()
                .unwrap()
                .version,
            newer.version
        );
    }

    #[tokio::test]
    async fn environment_stale_probe_result_is_discarded_after_settings_edit() {
        let checkout = tempfile::TempDir::new().unwrap();
        let signals = tempfile::TempDir::new().unwrap();
        let environment: ProjectEnvironment = serde_json::from_value(serde_json::json!({"env":{"COUNT":signals.path().join("starts"), "RELEASE":signals.path().join("release")}, "checks":[{"name":"old","command":"echo started > \"$COUNT\"; while ! test -f \"$RELEASE\"; do sleep 0.05; done", "timeout_seconds":10}]})).unwrap();
        let (db, project) = fixture(&environment).await;
        let target = ProbeTarget::Server(checkout.path().to_owned());
        start_probe(
            db.clone(),
            project.clone(),
            environment.clone(),
            None,
            target.clone(),
            Arc::new(EventBus::default()),
            Arc::new(tokio::sync::Notify::new()),
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !signals.path().join("starts").exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let previous = db
            .get_readiness(&project, &EnvironmentMachine::Server)
            .await
            .unwrap()
            .unwrap();
        let settings: ProjectEnvironment =
            serde_json::from_value(serde_json::json!({"checks":[{"name":"new","command":"true"}]}))
                .unwrap();
        let current = ProjectRepo::get_by_id(&db, &project)
            .await
            .unwrap()
            .unwrap();
        ProjectRepo::update_at_version(
            &db,
            db::UpdateProject {
                id: project.clone(),
                name: None,
                settings: Some(serde_json::json!({"environment":settings}).to_string()),
                primary_repo_id: None,
                paused_at: None,
                updated_at: db::now_rfc3339(),
            },
            current.version,
            None,
        )
        .await
        .unwrap();
        let invalidated = db
            .get_readiness(&project, &EnvironmentMachine::Server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(invalidated.status, EnvironmentReadinessStatus::Unknown);
        assert!(invalidated.version > previous.version);
        std::fs::write(signals.path().join("release"), "go").unwrap();
        // Completion itself schedules the new digest; no Task, event or manual
        // second start is needed after the edit collided with the older flight.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if db
                    .get_readiness(&project, &EnvironmentMachine::Server)
                    .await
                    .unwrap()
                    .is_some_and(|row| row.status == EnvironmentReadinessStatus::Ready)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            db.get_readiness(&project, &EnvironmentMachine::Server)
                .await
                .unwrap()
                .unwrap()
                .checks_digest,
            db::environment_checks_digest(&settings)
        );
        let completed = db
            .get_readiness(&project, &EnvironmentMachine::Server)
            .await
            .unwrap()
            .unwrap();
        assert!(completed.version > invalidated.version);
        assert_eq!(
            completed
                .check_results
                .iter()
                .map(|result| result.name.as_str())
                .collect::<Vec<_>>(),
            vec!["new"]
        );
    }

    #[tokio::test]
    async fn daemon_probe_completion_records_full_readiness_and_kicks_dispatch() {
        let environment: ProjectEnvironment = serde_json::from_value(
            serde_json::json!({"checks":[{"name":"cargo","command":"true"}]}),
        )
        .unwrap();
        let (db, project) = fixture(&environment).await;
        let registry =
            Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
        let (connection, mut requests) =
            crate::daemon_transport::DaemonConnection::new("probe-daemon".into());
        registry.register("probe-daemon".into(), connection.clone());
        let handshake = api_types::DaemonHandshakeNotification {
            protocol_revision: 3,
            capabilities: api_types::DAEMON_REQUIRED_CAPABILITIES
                .iter()
                .map(|fact| (*fact).to_owned())
                .chain([
                    api_types::DAEMON_CAPABILITY_WORKSPACE.into(),
                    api_types::DAEMON_CAPABILITY_MACHINE_PROBE.into(),
                ])
                .collect(),
            executor_capabilities: Default::default(),
            workspace_run_policy: api_types::WorkspaceRunPolicy {
                allowed_purposes: vec![WorkspaceRunPurpose::EnvironmentProbe],
            },
        };
        registry.dispatch_incoming_for_connection(
            "probe-daemon",
            connection.id(),
            api_types::DaemonFrame::Notification {
                method: api_types::METHOD_DAEMON_HANDSHAKE.into(),
                params: serde_json::to_value(handshake).unwrap(),
            },
        );
        let incoming = registry.clone();
        let daemon = tokio::spawn(async move {
            let api_types::DaemonFrame::Request { id, method, params } =
                requests.recv().await.unwrap()
            else {
                panic!("probe request");
            };
            assert_eq!(method, api_types::METHOD_MACHINE_PROBE);
            assert_eq!(params["repo_location_id"], "location");
            incoming.dispatch_incoming_for_connection("probe-daemon", connection.id(), api_types::DaemonFrame::Response { id, result: serde_json::json!({"results":[{"name":"cargo","exit_code":0,"timed_out":false,"output_tail":"ready"}]}) });
        });
        let kick = Arc::new(tokio::sync::Notify::new());
        let machine = EnvironmentMachine::Daemon {
            daemon_id: "probe-daemon".into(),
            runtime_id: "probe-runtime".into(),
        };
        start_probe(
            db.clone(),
            project.clone(),
            environment,
            None,
            ProbeTarget::Machine {
                machine: machine.clone(),
                client: crate::daemon_transport::workspace_client::DaemonWorkspaceClient::new(
                    registry,
                ),
                location_id: Some("location".into()),
            },
            Arc::new(EventBus::default()),
            kick.clone(),
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), kick.notified())
            .await
            .unwrap();
        daemon.await.unwrap();
        let row = db.get_readiness(&project, &machine).await.unwrap().unwrap();
        assert_eq!(row.status, EnvironmentReadinessStatus::Ready);
        assert_eq!(row.scope_covered, "full");
    }

    #[tokio::test]
    async fn environment_project_without_checks_never_probes_or_creates_rows() {
        let environment = ProjectEnvironment::default();
        let (db, project) = fixture(&environment).await;
        start_probe(
            db.clone(),
            project.clone(),
            environment,
            None,
            ProbeTarget::Server(PathBuf::from("/does-not-exist")),
            Arc::new(EventBus::default()),
            Arc::new(tokio::sync::Notify::new()),
        );
        tokio::task::yield_now().await;
        assert!(db.list_readiness(&project).await.unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn provisioning_wait_detail_tolerates_the_retry_row_settling_concurrently() {
        let (db, project) = fixture(&ProjectEnvironment::default()).await;
        let now = db::now_rfc3339();
        db::RepoRepo::create(
            &db,
            db::CreateRepo {
                id: "wait-repo".into(),
                project_id: project,
                name: "repo".into(),
                local_path: None,
                remote_url: Some("https://example.com/repo.git".into()),
                default_branch: "main".into(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO daemon (id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES ('wait-daemon','wait-machine','owner','linux','aarch64','online',?,?)")
            .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO runtime (id,daemon_id,kind,workspace_root,status,created_at,updated_at) VALUES ('wait-runtime','wait-daemon','native','/owner','ready',?,?)")
            .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        let machine = EnvironmentMachine::Daemon {
            runtime_id: "wait-runtime".into(),
            daemon_id: "wait-daemon".into(),
        };
        // A provisioning job that verifies the location deletes the retry
        // row while a claim's refusal is describing the wait.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let churn = tokio::spawn({
            let (db, stop) = (db.clone(), stop.clone());
            async move {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    sqlx::query("INSERT INTO repo_provision_retry (repo_id,runtime_id,attempts,next_attempt_at,checks_digest,connection_id,started_at) VALUES ('wait-repo','wait-runtime',1,?,'',0,?)")
                        .bind(db::now_rfc3339()).bind(db::now_rfc3339()).execute(db.pool()).await.unwrap();
                    sqlx::query("DELETE FROM repo_provision_retry WHERE repo_id='wait-repo' AND runtime_id='wait-runtime'")
                        .execute(db.pool()).await.unwrap();
                }
            }
        });
        let mut described = 0;
        for _ in 0..2_000 {
            let detail = provisioning_wait_detail(&db, "wait-repo", &machine, None)
                .await
                .expect("a settled retry row is not an error");
            described += usize::from(!detail.is_empty());
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        churn.await.unwrap();
        assert!(described > 0, "the wait detail was never read mid-retry");
    }
}
