//! Environment-only admission handling and host probes. No daemon probes.
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
    /// Only scheduled re-checks use this, and only for the failure's workspace.
    Daemon {
        placement: Box<db::WorkspacePlacement>,
        backend: Arc<dyn WorkspaceBackend>,
    },
}
type FlightKey = (String, EnvironmentMachine);
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
    // Step A is deliberately host-only. A daemon check through a live Task
    // workspace is never a readiness probe; machine.probe lands in step 3.
    if environment.checks.is_empty() || !matches!(target, ProbeTarget::Server(_)) {
        return;
    }
    let Some(guard) = claim_probe(&project, &EnvironmentMachine::Server) else {
        return;
    };
    tokio::spawn(async move {
        let result = async {
            let snapshot = db::ProjectRepo::get_by_id(&db, &project)
                .await?
                .ok_or(db::DbError::NotFound)?;
            let row = db
                .put_readiness(
                    unknown_record(&project, EnvironmentMachine::Server, &environment),
                    observed.map(|row| row.version),
                )
                .await?;
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
                    match db
                        .get_readiness(&project, &EnvironmentMachine::Server)
                        .await
                    {
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
pub(crate) async fn run_checks(
    target: &ProbeTarget,
    environment: &ProjectEnvironment,
    checks: &[EnvironmentCheck],
) -> Result<Vec<api_types::ProjectEnvironmentCheckResult>> {
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
    let Some(epoch) = original.paused_at.as_deref() else {
        return Ok(false);
    };
    let mut snapshot = original.clone();
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
    events: &Arc<EventBus>,
    kick: Arc<tokio::sync::Notify>,
) -> Result<()> {
    if context.environment_digest.is_none() {
        return Ok(());
    }
    let Some(candidate) = context.candidates.iter().find(|candidate| {
        candidate.location.owner_kind == db::RepoLocationOwnerKind::Server
            && candidate.location.status == db::RepoLocationStatus::Ready
            && super::selection::environment_filter(context, candidate)
                == Some(super::PlacementFilterCode::EnvironmentProbePending)
    }) else {
        return Ok(());
    };
    let project = db::ProjectRepo::get_by_id(db, &context.task.project_id)
        .await?
        .ok_or(db::DbError::NotFound)?;
    let settings: api_types::ProjectSettings = serde_json::from_str(&project.settings)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    start_probe(
        db.clone(),
        project.id,
        settings.environment,
        candidate.environment_readiness.clone(),
        ProbeTarget::Server(PathBuf::from(&candidate.location.path)),
        events.clone(),
        kick,
    );
    Ok(())
}
pub(crate) async fn schedule_project_probes(
    db: &SqliteDb,
    project: &db::Project,
    kick: Arc<tokio::sync::Notify>,
    events: Arc<EventBus>,
) -> Result<()> {
    let settings: api_types::ProjectSettings = match serde_json::from_str(&project.settings) {
        Ok(settings) => settings,
        Err(_) => return Ok(()),
    };
    if settings.environment.checks.is_empty() {
        return Ok(());
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
    workspace_id: Option<&str>,
) -> Result<Option<ProbeTarget>> {
    let EnvironmentMachine::Daemon { .. } = machine else {
        return Ok(None);
    };
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
) -> Result<bool> {
    use super::PlacementFilterCode::*;
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
            "role":row.role.as_ref().unwrap_or(&context.claiming_agent.role), "output":row.output_tail,
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
    )
    .await?
    .unwrap_or(false))
}
pub(crate) async fn defer_refusal(
    db: &SqliteDb,
    task: &db::Task,
    error: &ServiceError,
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
    if db::ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .is_some_and(|project| project.paused_at.is_some())
    {
        return Ok(Some(true));
    }
    let pending = refusal.rejected_candidates.iter().any(|candidate| {
        candidate.filter_codes == [super::PlacementFilterCode::EnvironmentProbePending]
    });
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
    if !refusal.rejected_candidates.iter().any(|candidate| {
        candidate.filter_codes == [super::PlacementFilterCode::EnvironmentProbePending]
    }) {
        return Ok(None);
    }
    let marker = serde_json::json!({"kind":"environment_probe_pending","reason":"environment_probe_pending: server","target_state":task.status,"not_before":(chrono::Utc::now()+chrono::Duration::seconds(30)).to_rfc3339()});
    let mut tx = db::begin_immediate(db.pool()).await?;
    // If completion won this race, leave no delayed marker behind.
    let rows = db.list_readiness_in_tx(&mut tx, &task.project_id).await?;
    if rows.iter().any(|row| {
        row.machine == EnvironmentMachine::Server
            && row.status != EnvironmentReadinessStatus::Unknown
    }) {
        return Ok(Some(true));
    }
    let changed = sqlx::query("UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.deferred_dispatch', json(?)) WHERE id = ? AND version = ?")
        .bind(marker.to_string()).bind(&task.id).bind(task.version).execute(&mut *tx).await?;
    if changed.rows_affected() != 1 {
        return Err(db::DbError::VersionConflict.into());
    }
    tx.commit().await?;
    Ok(Some(true))
}
pub(crate) async fn persist_machine_wait(
    db: &SqliteDb,
    task: &db::Task,
    machine: &EnvironmentMachine,
    checks: &[String],
) -> Result<()> {
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
    let deferral = serde_json::json!({"kind":"environment_not_ready","reason":format!("environment_not_ready: {} ({})",machine_label(machine),checks.join(", ")),"target_state":task.status,"not_before":(chrono::Utc::now()+chrono::Duration::seconds(30)).to_rfc3339()});
    sqlx::query("UPDATE task SET metadata_json = json_set(json_remove(COALESCE(metadata_json, '{}'), '$.owner_wait'), '$.environment_wait', json(?), '$.deferred_dispatch', json(?)) WHERE id = ? AND version = ?")
        .bind(marker.to_string()).bind(deferral.to_string()).bind(&task.id).bind(task.version).execute(&mut *tx).await?;
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
            .bind(db::new_uuid_v4()).bind(&task.project_id).bind(event_id).bind(format!("Waiting for environment on {}: {}",machine_label(machine),checks.join(", ")))
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
}
