//! Probes own their transaction and command I/O, never admission's transaction.
use crate::{
    project_environment::{bounded_output_tail, next_check_at},
    workspace_backend::{RunSpec, WorkspaceBackend, WorkspaceBackendError, WorkspaceRunPurpose},
    Result, ServiceError,
};
use api_types::{EnvironmentCheck, ProjectEnvironment};
use db::{
    EnvironmentMachine, EnvironmentReadinessStatus, ProjectMachineReadiness,
    ProjectMachineReadinessRepo, ReadinessCheckFailure, SqliteDb,
};
use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
};

#[derive(Clone)]
pub(crate) enum ProbeTarget {
    Server(PathBuf),
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
pub(crate) fn claim_probe(project_id: &str, machine: &EnvironmentMachine) -> Option<ProbeGuard> {
    let key = (project_id.to_owned(), machine.clone());
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
    project_id: &str,
    machine: EnvironmentMachine,
    environment: &ProjectEnvironment,
) -> ProjectMachineReadiness {
    ProjectMachineReadiness {
        project_id: project_id.to_owned(),
        machine,
        status: EnvironmentReadinessStatus::Unknown,
        checks_digest: db::environment_checks_digest(environment),
        failing_checks: Vec::new(),
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
    project_id: String,
    machine: EnvironmentMachine,
    environment: ProjectEnvironment,
    observed: Option<ProjectMachineReadiness>,
    target: ProbeTarget,
) {
    if environment.checks.is_empty() {
        return;
    }
    let Some(guard) = claim_probe(&project_id, &machine) else {
        return;
    };
    tokio::spawn(async move {
        let _guard = guard;
        let result = async {
            let mut row = unknown_record(&project_id, machine, &environment);
            row.workspace_id = match &target {
                ProbeTarget::Daemon { placement, .. } => Some(placement.workspace_id.clone()),
                _ => None,
            };
            // This writer acquisition waits for admission to release its writer
            // lock. No commands can start while that admission is in flight.
            let row = db
                .put_readiness(row, observed.map(|row| row.version))
                .await?;
            let results = run_checks(&target, &environment, &environment.checks).await?;
            save_results(&db, row, &environment, results).await?;
            Ok::<_, ServiceError>(())
        }
        .await;
        if let Err(error) = result {
            if !matches!(error, ServiceError::Db(db::DbError::VersionConflict)) {
                tracing::warn!(%project_id, %error, "environment probe failed");
            }
        }
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
            max_output_bytes: isize::MAX as usize,
        };
        let outcome = match target {
            ProbeTarget::Server(path) => {
                crate::workspace_backend::run_environment_checkout(path, &spec).await
            }
            ProbeTarget::Daemon { placement, backend } => backend.run(placement, &spec).await,
        };
        let (passed, exit_code, output) = match outcome {
            Ok(result) => (
                result.exit_code == 0,
                (result.exit_code >= 0).then_some(result.exit_code),
                format!("{}{}", result.stdout_tail, result.stderr_tail),
            ),
            Err(
                error @ (WorkspaceBackendError::OwnerUnreachable { .. }
                | WorkspaceBackendError::RpcTimeoutBeforeStart { .. }),
            ) => return Err(error.into()),
            Err(WorkspaceBackendError::Other(error))
                if matches!(
                    *error,
                    ServiceError::DaemonUnavailable { .. } | ServiceError::DaemonTimeout { .. }
                ) =>
            {
                return Err(*error)
            }
            Err(error) => (false, None, error.to_string()),
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
    let expected = row.version;
    row.failing_checks = results
        .into_iter()
        .filter(|result| !result.passed)
        .map(|result| ReadinessCheckFailure {
            name: result.name,
            output_tail: bounded_output_tail(&result.output_tail),
        })
        .collect();
    row.status = if row.failing_checks.is_empty() {
        EnvironmentReadinessStatus::Ready
    } else {
        EnvironmentReadinessStatus::NotReady
    };
    let now = chrono::Utc::now();
    row.checked_at = Some(now.to_rfc3339());
    row.next_check_at = (row.status == EnvironmentReadinessStatus::NotReady)
        .then(|| next_check_at(now, environment.recheck_interval_seconds));
    Ok(db.put_readiness(row, Some(expected)).await?)
}

pub(crate) async fn defer_refusal(
    db: &SqliteDb,
    task: &db::Task,
    error: &ServiceError,
) -> Result<Option<bool>> {
    let ServiceError::PlacementUnavailable(refusal) = error else {
        return Ok(None);
    };
    use super::PlacementFilterCode::*;
    let placement = db::WorkspacePlacementRepo::get_for_task(db, &task.id).await?;
    if let Some(placement) = placement.as_ref() {
        if let Some(rejection) = refusal.rejected_candidates.iter().find(|candidate| {
            candidate.repo_location_id == placement.repo_location_id
                && candidate.filter_codes.contains(&EnvironmentNotReady)
        }) {
            persist_machine_wait(
                db,
                task,
                &EnvironmentMachine::from_placement(placement),
                &rejection.failing_checks,
            )
            .await?;
            return Ok(Some(true));
        }
    }
    let pending: Vec<_> = refusal
        .rejected_candidates
        .iter()
        .filter(|candidate| {
            candidate.filter_codes.contains(&EnvironmentProbePending)
                && candidate.filter_codes.iter().all(|code| {
                    matches!(
                        code,
                        EnvironmentProbePending | AgentCapacity | DaemonCapacity | OwnerUnreachable
                    )
                })
        })
        .collect();
    if pending.is_empty() {
        return Ok(None);
    }
    let reason = format!(
        "environment_probe_pending: {}",
        pending
            .iter()
            .map(|candidate| {
                if candidate.owner_kind == "server" {
                    "server".to_owned()
                } else {
                    format!(
                        "{}/{}",
                        candidate.daemon_id.as_deref().unwrap_or("daemon"),
                        candidate.runtime_id.as_deref().unwrap_or("runtime")
                    )
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    );
    let deferral = serde_json::json!({"reason":reason,"target_state":task.status,
        "not_before":(chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc3339()})
    .to_string();
    // State entry already advances Task.version. The queued dispatch marker
    // is metadata, so first and repeated deferrals spend no additional version.
    let result = sqlx::query("UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.deferred_dispatch', json(?)), updated_at = ? WHERE id = ? AND version = ?")
        .bind(deferral).bind(db::now_rfc3339()).bind(&task.id).bind(task.version).execute(db.pool()).await?;
    if result.rows_affected() != 1 {
        return Err(db::DbError::VersionConflict.into());
    }
    // The probe may have finished before the deferral was persisted. Fence
    // that race with a fresh readiness read and clear the time delay only.
    let rows = db.list_readiness(&task.project_id).await?;
    if pending.iter().any(|candidate| {
        rows.iter().any(|row| {
            row.status != EnvironmentReadinessStatus::Unknown
                && row.machine.columns()
                    == (
                        candidate.owner_kind.as_str(),
                        candidate
                            .daemon_id
                            .as_deref()
                            .filter(|_| candidate.owner_kind == "daemon")
                            .unwrap_or(""),
                        candidate
                            .runtime_id
                            .as_deref()
                            .filter(|_| candidate.owner_kind == "daemon")
                            .unwrap_or(""),
                    )
        })
    }) {
        crate::deferred_dispatch::clear(db, task).await?;
    }
    Ok(Some(true))
}

/// Resolve the same shared-worktree constraints used by admission. Initial
/// dispatch needs them before state entry to defer only an otherwise viable
/// environment probe, rather than spending a workflow transition on a wait.
pub(crate) async fn dispatch_worktree_agents(
    db: &SqliteDb,
    task: &db::Task,
    claiming_id: &str,
) -> Result<Vec<(String, db::Agent)>> {
    let mut assignments = db::TaskRoleAssignmentRepo::list_by_task(db, &task.id).await?;
    if let Some(root) = task.parent_task_id.as_deref() {
        for assignment in db::TaskRoleAssignmentRepo::list_by_task(db, root).await? {
            if assignment.role_name != "coder"
                && !assignments
                    .iter()
                    .any(|current| current.role_name == assignment.role_name)
            {
                assignments.push(assignment);
            }
        }
    }
    assignments.retain(|assignment| assignment.role_name != "coder");
    if let Some(coder) = crate::task_hierarchy::effective_coder_assignment(db, task).await? {
        assignments.push(coder.assignment);
    }
    let mut agents = Vec::new();
    for assignment in assignments {
        if !matches!(
            assignment.role_name.as_str(),
            "coder" | "executor" | "reviewer" | "planner" | "auditor"
        ) || assignment.assignee_type != Some(db::AssigneeKind::Agent)
        {
            continue;
        }
        if let Some(id) = assignment
            .assignee_id
            .as_deref()
            .filter(|id| *id != claiming_id)
        {
            let agent = db::AgentRepo::get_by_id(db, id)
                .await?
                .ok_or_else(|| ServiceError::not_found("agent", id.to_owned()))?;
            agents.push((assignment.role_name, agent));
        }
    }
    Ok(agents)
}

pub(crate) async fn persist_machine_wait(
    db: &SqliteDb,
    task: &db::Task,
    machine: &EnvironmentMachine,
    checks: &[String],
) -> Result<()> {
    let reason = format!(
        "environment_not_ready: {} ({})",
        machine_label(machine),
        checks.join(", ")
    );
    let metadata = db::TaskMetadata::parse(task.metadata_json.as_deref())
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let wait = serde_json::json!({"machine":machine,"checks":checks,"reason":reason});
    let unchanged = metadata.extra.get("environment_wait") == Some(&wait);
    let deferral = serde_json::json!({"reason":reason,"target_state":task.status,
        "not_before":(chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc3339()});
    let mut tx = db::begin_immediate(db.pool()).await?;
    let settings: String = sqlx::query_scalar("SELECT settings FROM project WHERE id = ?")
        .bind(&task.project_id)
        .fetch_one(&mut *tx)
        .await?;
    let environment: api_types::ProjectSettings = serde_json::from_str(&settings)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let digest = db::environment_checks_digest(&environment.environment);
    let rows = db.list_readiness_in_tx(&mut tx, &task.project_id).await?;
    if environment.environment.checks.is_empty()
        || rows.iter().any(|row| {
            &row.machine == machine
                && row.status == EnvironmentReadinessStatus::Ready
                && row.checks_digest == digest
        })
    {
        // A success/settings result won the race before this wait was saved.
        // It must not acquire new Attention or a fresh dispatch delay.
        return Ok(());
    }
    let result = sqlx::query("UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.environment_wait', json(?), '$.deferred_dispatch', json(?)) WHERE id = ? AND version = ?")
        .bind(wait.to_string()).bind(deferral.to_string()).bind(&task.id).bind(task.version).execute(&mut *tx).await?;
    if result.rows_affected() != 1 {
        return Err(db::DbError::VersionConflict.into());
    }
    if !unchanged {
        super::admission::record_wait_attention_in_tx(
            db,
            &mut tx,
            task,
            "execution_failed",
            &reason,
            &format!("task-environment-wait:{}", task.id),
        )
        .await?;
        sqlx::query("UPDATE attention_projection SET details_json = json_set(details_json, '$.cause', 'environment_not_ready', '$.machine', json(?), '$.checks', json(?)) WHERE dedupe_key = ?")
            .bind(serde_json::to_value(machine).expect("machine is serializable").to_string())
            .bind(serde_json::to_string(checks).expect("check names are serializable"))
            .bind(format!("task-environment-wait:{}", task.id)).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
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

pub(crate) async fn target_for_machine(
    db: &SqliteDb,
    project: &db::Project,
    machine: &EnvironmentMachine,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
    workspace_id: Option<&str>,
) -> Result<Option<ProbeTarget>> {
    match machine {
        EnvironmentMachine::Server => {
            let path: Option<String> = sqlx::query_scalar("SELECT l.path FROM repo_location l JOIN repo r ON r.id = l.repo_id WHERE r.project_id = ? AND l.owner_kind = 'server' AND l.status = 'ready' ORDER BY (r.id = ?) DESC, l.is_default DESC, l.created_at, l.id LIMIT 1")
                .bind(&project.id).bind(&project.primary_repo_id).fetch_optional(db.pool()).await?;
            Ok(path.map(|path| ProbeTarget::Server(PathBuf::from(path))))
        }
        EnvironmentMachine::Daemon {
            daemon_id,
            runtime_id,
        } => {
            let workspace: Option<String> = sqlx::query_scalar("SELECT p.workspace_id FROM workspace_placement p JOIN task t ON t.id = p.task_id WHERE t.project_id = ? AND p.owner_kind = 'daemon' AND p.daemon_id = ? AND p.runtime_id = ? AND p.state = 'ready' AND p.workspace_handle IS NOT NULL ORDER BY (p.workspace_id = ?) DESC, p.updated_at DESC LIMIT 1")
                .bind(&project.id).bind(daemon_id).bind(runtime_id).bind(workspace_id).fetch_optional(db.pool()).await?;
            let Some(workspace) = workspace else {
                return Ok(None);
            };
            let Some(placement) =
                db::WorkspacePlacementRepo::get_by_workspace_id(db, &workspace).await?
            else {
                return Ok(None);
            };
            Ok(router
                .for_placement(&placement)
                .ok()
                .map(|backend| ProbeTarget::Daemon {
                    placement: Box::new(placement),
                    backend,
                }))
        }
    }
}

/// Called by the settings event observer and by the periodic scan. It needs
/// no Task and includes both existing records and ready repository locations.
pub(crate) async fn schedule_project_probes(
    db: &SqliteDb,
    project: &db::Project,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
) -> Result<()> {
    let settings: api_types::ProjectSettings = serde_json::from_str(&project.settings)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    if settings.environment.checks.is_empty() {
        return Ok(());
    }
    let rows = db.list_readiness(&project.id).await?;
    let mut machines: HashSet<_> = rows.iter().map(|row| row.machine.clone()).collect();
    let locations = sqlx::query_as::<_, (String, Option<String>, Option<String>)>("SELECT l.owner_kind, l.daemon_id, l.runtime_id FROM repo_location l JOIN repo r ON r.id = l.repo_id WHERE r.project_id = ? AND l.status = 'ready'")
        .bind(&project.id).fetch_all(db.pool()).await?;
    for (kind, daemon, runtime) in locations {
        machines.insert(if kind == "server" {
            EnvironmentMachine::Server
        } else {
            EnvironmentMachine::Daemon {
                daemon_id: daemon.unwrap_or_default(),
                runtime_id: runtime.unwrap_or_default(),
            }
        });
    }
    let digest = db::environment_checks_digest(&settings.environment);
    for machine in machines {
        let observed = rows.iter().find(|row| row.machine == machine).cloned();
        if observed.as_ref().is_some_and(|row| {
            row.checks_digest == digest && row.status != EnvironmentReadinessStatus::Unknown
        }) {
            continue;
        }
        if let Some(target) = target_for_machine(
            db,
            project,
            &machine,
            router,
            observed
                .as_ref()
                .and_then(|row| row.workspace_id.as_deref()),
        )
        .await?
        {
            start_probe(
                db.clone(),
                project.id.clone(),
                machine,
                settings.environment.clone(),
                observed,
                target,
            );
        }
    }
    Ok(())
}

/// Unknown/unprobed owners still qualify as alternatives; only known current
/// failures justify stopping the whole Project. Provisioning comes in step 3.
pub(crate) async fn every_machine_not_ready(
    db: &SqliteDb,
    project: &db::Project,
    digest: &str,
) -> Result<bool> {
    let rows = db.list_readiness(&project.id).await?;
    let locations = sqlx::query_as::<_, (String, Option<String>, Option<String>)>("SELECT l.owner_kind, l.daemon_id, l.runtime_id FROM repo_location l JOIN repo r ON r.id = l.repo_id WHERE r.project_id = ? AND l.status = 'ready'")
        .bind(&project.id).fetch_all(db.pool()).await?;
    Ok(locations.iter().all(|(kind, daemon, runtime)| {
        rows.iter().any(|row| {
            row.machine.columns()
                == (
                    kind.as_str(),
                    daemon.as_deref().filter(|_| kind == "daemon").unwrap_or(""),
                    runtime
                        .as_deref()
                        .filter(|_| kind == "daemon")
                        .unwrap_or(""),
                )
                && row.status == EnvironmentReadinessStatus::NotReady
                && row.checks_digest == digest
        })
    }))
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
    let LaunchEnvironmentFailure {
        checks,
        output,
        role,
    } = failure;
    if environment.checks.is_empty() {
        return Ok(());
    }
    let machine = EnvironmentMachine::from_placement(placement);
    for _ in 0..3 {
        let previous = db.get_readiness(&project.id, &machine).await?;
        let mut row = unknown_record(&project.id, machine.clone(), environment);
        let now = chrono::Utc::now();
        row.status = EnvironmentReadinessStatus::NotReady;
        row.failing_checks = checks
            .iter()
            .map(|name| ReadinessCheckFailure {
                name: name.clone(),
                output_tail: bounded_output_tail(output),
            })
            .collect();
        row.role = Some(role.to_owned());
        row.workspace_id = Some(placement.workspace_id.clone());
        row.checked_at = Some(now.to_rfc3339());
        row.next_check_at = Some(next_check_at(now, environment.recheck_interval_seconds));
        match db.put_readiness(row, previous.map(|row| row.version)).await {
            Ok(_) => return Ok(()),
            Err(db::DbError::VersionConflict) => {
                let current = db::ProjectRepo::get_by_id(db, &project.id)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                let current: api_types::ProjectSettings =
                    serde_json::from_str(&current.settings)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                if db::environment_checks_digest(&current.environment)
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
            EnvironmentMachine::Server,
            environment.clone(),
            None,
            target.clone(),
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
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if claim_probe(&project, &EnvironmentMachine::Server).is_some() {
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
                .unwrap(),
            invalidated
        );
        start_probe(
            db.clone(),
            project.clone(),
            EnvironmentMachine::Server,
            settings.clone(),
            Some(invalidated),
            target,
        );
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
    }

    #[tokio::test]
    async fn environment_project_without_checks_never_probes_or_creates_rows() {
        let environment = ProjectEnvironment::default();
        let (db, project) = fixture(&environment).await;
        start_probe(
            db.clone(),
            project.clone(),
            EnvironmentMachine::Server,
            environment,
            None,
            ProbeTarget::Server(PathBuf::from("/does-not-exist")),
        );
        tokio::task::yield_now().await;
        assert!(db.list_readiness(&project).await.unwrap().is_empty());
    }
}
