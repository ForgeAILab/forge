//! Public projections and manual machine checks; placement and scheduling stay unchanged.
use crate::{
    placement::{context, environment},
    Result, ServiceError, TaskService,
};
use api_types::{MachineEnvironmentRecheckResult, MachineIdentity, ProjectEnvironmentReadiness};
use db::{EnvironmentMachine, ProjectMachineReadinessRepo, ProjectRepo, SqliteDb};
use std::collections::BTreeMap;

pub async fn machine_identity(
    db: &SqliteDb,
    machine: &EnvironmentMachine,
) -> Result<MachineIdentity> {
    let (id, name, owner_kind, daemon_id, runtime_id) = match machine {
        EnvironmentMachine::Server => (
            "server".into(),
            "Server host".into(),
            api_types::RepoLocationOwnerKind::Server,
            None,
            None,
        ),
        EnvironmentMachine::Daemon {
            daemon_id,
            runtime_id,
        } => {
            let name = sqlx::query_scalar::<_, String>("SELECT hostname FROM daemon WHERE id = ?")
                .bind(daemon_id)
                .fetch_optional(db.pool())
                .await?
                .unwrap_or_else(|| "Unavailable daemon".into());
            (
                runtime_id.clone(),
                name,
                api_types::RepoLocationOwnerKind::Daemon,
                Some(daemon_id.clone()),
                Some(runtime_id.clone()),
            )
        }
    };
    Ok(MachineIdentity {
        id,
        name,
        owner_kind,
        daemon_id,
        runtime_id,
    })
}

pub async fn readiness(db: &SqliteDb, project: &str) -> Result<Vec<ProjectEnvironmentReadiness>> {
    let mut entries = Vec::new();
    for row in db.list_readiness(project).await? {
        entries.push(ProjectEnvironmentReadiness {
            machine: machine_identity(db, &row.machine).await?,
            status: match row.status {
                db::EnvironmentReadinessStatus::Ready => {
                    api_types::EnvironmentReadinessStatus::Ready
                }
                db::EnvironmentReadinessStatus::NotReady => {
                    api_types::EnvironmentReadinessStatus::NotReady
                }
                db::EnvironmentReadinessStatus::Unknown => {
                    api_types::EnvironmentReadinessStatus::Unknown
                }
            },
            failing_checks: row
                .failing_checks
                .into_iter()
                .map(|check| api_types::EnvironmentCheckFailure {
                    name: check.name,
                    output_tail: check.output_tail,
                })
                .collect(),
            output_tail: row.output_tail,
            scope_covered: row.scope_covered,
            checked_at: row.checked_at,
            next_check_at: row.next_check_at,
        });
    }
    entries.sort_by(|a, b| a.machine.id.cmp(&b.machine.id));
    Ok(entries)
}

/// Executor fit only: no repository, environment or capacity assertion.
pub async fn runnable_on(
    db: &SqliteDb,
    agent: &db::Agent,
    registry: &executors::AdapterRegistry,
    connections: &crate::daemon_transport::DaemonConnectionRegistry,
    admin: bool,
) -> Result<api_types::AgentRunnableOn> {
    let role = context::worktree_agent("coder", agent.clone());
    let server = context::server_executor_facts(db, std::iter::once(&role), Some(registry)).await?;
    let mut machines = Vec::new();
    if agent
        .daemon_id
        .as_ref()
        .is_none_or(|pin| Some(pin) == server.execution_daemon_id.as_ref())
        && server
            .executors
            .get(&agent.id)
            .is_some_and(|facts| facts.installed && facts.authenticated && facts.enabled)
        && executor_enabled(db, agent, server.execution_daemon_id.as_deref()).await?
    {
        machines.push(machine_identity(db, &EnvironmentMachine::Server).await?);
    }
    if agent.backend_kind == "cli" && !agent.paused {
        let rows: Vec<(String,String,String,Option<String>,String,String)> = sqlx::query_as(
            "SELECT d.id, r.id, d.machine_id, d.owner_id, d.visibility, d.detected_clis_json FROM daemon d JOIN runtime r ON r.daemon_id=d.id WHERE d.status <> 'offline' AND r.status='ready' ORDER BY d.id,r.id")
            .fetch_all(db.pool()).await?;
        for (daemon_id, runtime_id, machine_id, owner, visibility, detected) in rows {
            if machine_id == db.server_run_cap.embedded_machine_id()
                || agent
                    .daemon_id
                    .as_ref()
                    .is_some_and(|pin| pin != &daemon_id)
                || !(visibility == "global" || owner.is_none() || owner == agent.owner_id)
            {
                continue;
            }
            let Some(connection) = connections.get(&daemon_id) else {
                continue;
            };
            if connection.is_stale() || !connection.protocol_allows_dispatch() {
                continue;
            }
            let detected: serde_json::Value = serde_json::from_str(&detected).unwrap_or_default();
            let authenticated = detected.as_array().is_some_and(|clis| {
                clis.iter().any(|cli| {
                    cli["kind"].as_str() == Some(&agent.executor_type)
                        && cli["availability"].as_str() == Some("authenticated")
                })
            });
            if authenticated && executor_enabled(db, agent, Some(&daemon_id)).await? {
                machines.push(
                    machine_identity(
                        db,
                        &EnvironmentMachine::Daemon {
                            daemon_id,
                            runtime_id,
                        },
                    )
                    .await?,
                );
            }
        }
    }
    Ok(api_types::AgentRunnableOn {
        count: machines.len().try_into().unwrap_or(u32::MAX),
        machines: admin.then_some(machines),
    })
}
async fn executor_enabled(db: &SqliteDb, agent: &db::Agent, daemon: Option<&str>) -> Result<bool> {
    if agent.backend_kind != "cli" {
        return Ok(true);
    }
    let Some(daemon) = daemon else {
        return Ok(true);
    };
    let enabled = if let Some(owner) = agent.owner_id.as_deref() {
        sqlx::query_scalar::<_, bool>("SELECT enabled FROM cli_runtime_policy WHERE owner_user_id=? AND daemon_id=? AND executor_type=?")
            .bind(owner).bind(daemon).bind(&agent.executor_type).fetch_optional(db.pool()).await?.unwrap_or(true)
    } else {
        sqlx::query_scalar::<_, Option<bool>>(
            "SELECT MIN(enabled) FROM cli_runtime_policy WHERE daemon_id=? AND executor_type=?",
        )
        .bind(daemon)
        .bind(&agent.executor_type)
        .fetch_one(db.pool())
        .await?
        .unwrap_or(true)
    };
    Ok(enabled)
}

pub async fn recheck(
    service: &TaskService,
    db: &SqliteDb,
    events: &events::EventBus,
    project_id: &str,
    requested: Option<&str>,
) -> Result<(Vec<MachineEnvironmentRecheckResult>, db::Project)> {
    let project = ProjectRepo::get_by_id(db, project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
    let settings: api_types::ProjectSettings = serde_json::from_str(&project.settings)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let env = settings.environment;
    let mut targets = BTreeMap::new();
    for row in db.list_readiness(project_id).await? {
        targets.insert(
            machine_identity(db, &row.machine).await?.id,
            (row.machine.clone(), Some(row)),
        );
    }
    let locations: Vec<(String,Option<String>,Option<String>)> = sqlx::query_as(
        "SELECT l.owner_kind,l.daemon_id,l.runtime_id FROM repo_location l JOIN repo r ON r.id=l.repo_id WHERE r.project_id=? AND l.status='ready'")
        .bind(project_id).fetch_all(db.pool()).await?;
    for (kind, daemon, runtime) in locations {
        let machine = if kind == "server" {
            EnvironmentMachine::Server
        } else {
            EnvironmentMachine::Daemon {
                daemon_id: daemon.unwrap_or_default(),
                runtime_id: runtime.unwrap_or_default(),
            }
        };
        targets
            .entry(machine_identity(db, &machine).await?.id)
            .or_insert((machine, None));
    }
    // The primary host checkout is the legacy check target even without a location row.
    if project.primary_repo_id.is_some() {
        let checkout: Option<Option<String>> =
            sqlx::query_scalar("SELECT local_path FROM repo WHERE id=?")
                .bind(&project.primary_repo_id)
                .fetch_optional(db.pool())
                .await?;
        if checkout.flatten().is_some() {
            targets
                .entry("server".into())
                .or_insert((EnvironmentMachine::Server, None));
        }
    }
    if let Some(id) = requested {
        if !targets.contains_key(id) {
            return Err(ServiceError::not_found("machine", id.to_owned()));
        }
        targets.retain(|key, _| key == id);
    }
    if env.checks.is_empty() {
        return Ok((Vec::new(), project));
    }
    let router = service.workspace_backend_router();
    let mut machines = Vec::new();
    for (_, (machine, observed)) in targets {
        let identity = machine_identity(db, &machine).await?;
        let Some(_probe) = environment::claim_probe(project_id, &machine) else {
            return Err(ServiceError::Conflict(
                "Machine environment re-check is already running".into(),
            ));
        };
        // Share the host guard with the existing scheduled/manual entry point.
        let _host_guard = if machine == EnvironmentMachine::Server {
            Some(
                service
                    .claim_environment_recheck(project_id)
                    .ok_or_else(|| {
                        ServiceError::Conflict(
                            "Project environment re-check is already running".into(),
                        )
                    })?,
            )
        } else {
            None
        };
        let target = if machine == EnvironmentMachine::Server {
            service
                .environment_check_checkout(&project)
                .await
                .map(|path| Some(environment::ProbeTarget::Server(path)))
        } else {
            environment::target_for_machine(
                db,
                &project,
                &machine,
                &router,
                observed
                    .as_ref()
                    .and_then(|row| row.workspace_id.as_deref()),
            )
            .await
        };
        let target = match target {
            Ok(target) => target,
            Err(error) => {
                reschedule_unavailable(db, observed.as_ref(), &env).await?;
                machines.push(MachineEnvironmentRecheckResult {
                    machine: identity,
                    checks: vec![],
                    error: Some(error.to_string()),
                });
                continue;
            }
        };
        let Some(target) = target else {
            reschedule_unavailable(db, observed.as_ref(), &env).await?;
            machines.push(MachineEnvironmentRecheckResult {
                machine: identity,
                checks: vec![],
                error: Some(
                    "This machine has no recorded ready workspace for environment checks.".into(),
                ),
            });
            continue;
        };
        let results = match environment::run_checks(&target, &env, &env.checks).await {
            Ok(results) => results,
            Err(error) => {
                reschedule_unavailable(db, observed.as_ref(), &env).await?;
                machines.push(MachineEnvironmentRecheckResult {
                    machine: identity,
                    checks: vec![],
                    error: Some(error.to_string()),
                });
                continue;
            }
        };
        let row = match observed {
            Some(row) => row,
            None => {
                db.put_readiness(
                    environment::unknown_record(project_id, machine.clone(), &env),
                    None,
                )
                .await?
            }
        };
        let saved = environment::save_results(db, row, &env, results.clone()).await?;
        if saved.status == db::EnvironmentReadinessStatus::Ready {
            let current = ProjectRepo::get_by_id(db, project_id)
                .await?
                .ok_or(db::DbError::NotFound)?;
            if crate::project_environment::pause_detail(&current)?
                .is_some_and(|pause| pause.checks.is_empty())
            {
                service.clear_environment_pause(&current).await?;
            } else {
                environment::clear_matching_pause(db, events, &project, &saved).await?;
            }
        } else if let Some(mut pause) = crate::project_environment::pause_detail(&project)? {
            let pause_machine: Option<EnvironmentMachine> = project
                .environment_pause_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
                .and_then(|value| serde_json::from_value(value["machine"].clone()).ok());
            if pause_machine.as_ref().is_none_or(|owner| owner == &machine) {
                pause.checks = saved
                    .failing_checks
                    .iter()
                    .map(|check| check.name.clone())
                    .collect();
                pause.output = saved.output_tail.clone();
                pause.last_checked_at = saved.checked_at.clone().unwrap_or_default();
                pause.next_check_at = saved.next_check_at.clone().unwrap_or_default();
                service.update_environment_pause(&project, &pause).await?;
            }
        }
        service.dispatch_notify().notify_one();
        machines.push(MachineEnvironmentRecheckResult {
            machine: identity,
            checks: results,
            error: None,
        });
    }
    let project = ProjectRepo::get_by_id(db, project_id)
        .await?
        .ok_or(db::DbError::NotFound)?;
    Ok((machines, project))
}

async fn reschedule_unavailable(
    db: &SqliteDb,
    observed: Option<&db::ProjectMachineReadiness>,
    env: &api_types::ProjectEnvironment,
) -> Result<()> {
    if let Some(row) = observed.filter(|row| row.status == db::EnvironmentReadinessStatus::NotReady)
    {
        let mut row = row.clone();
        let version = row.version;
        row.next_check_at = Some(crate::project_environment::next_check_at(
            chrono::Utc::now(),
            env.recheck_interval_seconds,
        ));
        match db.put_readiness(row, Some(version)).await {
            Ok(_) | Err(db::DbError::VersionConflict) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Expose only recorded placement facts; never run admission for a response.
pub async fn task_placement_diagnostics(
    db: &SqliteDb,
    task: &db::Task,
    placement: Option<&api_types::WorkspacePlacementResponse>,
) -> Result<Vec<api_types::TaskPlacementDiagnostic>> {
    let metadata = db::TaskMetadata::parse(task.metadata_json.as_deref())
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let mut diagnostics = Vec::new();
    if let Some(wait) = metadata.extra.get("environment_wait") {
        if let Ok(machine) = serde_json::from_value::<EnvironmentMachine>(wait["machine"].clone()) {
            diagnostics.push(api_types::TaskPlacementDiagnostic {
                machine: Some(machine_identity(db, &machine).await?),
                filter_codes: vec!["environment_not_ready".into()],
                failing_checks: serde_json::from_value(wait["checks"].clone()).unwrap_or_default(),
            });
        }
    }
    if metadata.extra.get("deferred_dispatch").is_some_and(|d| {
        d["kind"] == "environment_probe_pending"
            || d["reason"]
                .as_str()
                .is_some_and(|r| r.starts_with("environment_probe_pending:"))
    }) {
        diagnostics.push(api_types::TaskPlacementDiagnostic {
            machine: Some(machine_identity(db, &EnvironmentMachine::Server).await?),
            filter_codes: vec!["environment_probe_pending".into()],
            failing_checks: vec![],
        });
    }
    if crate::deferred_dispatch::current_dispatch_disposition(task)
        .is_some_and(|disposition| disposition.capability == "machine_capacity")
    {
        diagnostics.push(api_types::TaskPlacementDiagnostic {
            machine: None,
            filter_codes: vec!["machine_capacity".into()],
            failing_checks: vec![],
        });
    }
    if let Some(placement) = placement {
        let rejected = placement
            .selection_reason
            .get("rejected_candidates")
            .and_then(serde_json::Value::as_array);
        for rejection in rejected.into_iter().flatten() {
            let machine = if rejection["owner_kind"] == "server" {
                EnvironmentMachine::Server
            } else {
                EnvironmentMachine::Daemon {
                    daemon_id: rejection["daemon_id"].as_str().unwrap_or_default().into(),
                    runtime_id: rejection["runtime_id"].as_str().unwrap_or_default().into(),
                }
            };
            diagnostics.push(api_types::TaskPlacementDiagnostic {
                machine: Some(machine_identity(db, &machine).await?),
                filter_codes: serde_json::from_value(rejection["filter_codes"].clone())
                    .unwrap_or_default(),
                failing_checks: serde_json::from_value(rejection["failing_checks"].clone())
                    .unwrap_or_default(),
            });
        }
    }
    Ok(diagnostics)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn readiness_projects_recorded_facts_and_readable_host_name() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let now = db::now_rfc3339();
        let env: api_types::ProjectEnvironment = serde_json::from_value(
            serde_json::json!({"checks":[{"name":"cargo","command":"cargo --version"}]}),
        )
        .unwrap();
        db::ProjectRepo::create(
            &db,
            db::CreateProject {
                id: "project".into(),
                name: "Readiness".into(),
                settings: serde_json::json!({"environment":env}).to_string(),
                workflow_definition: "{}".into(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        assert!(readiness(&db, "project").await.unwrap().is_empty());
        let row = environment::unknown_record("project", EnvironmentMachine::Server, &env);
        db.put_readiness(row, None).await.unwrap();
        let rows = readiness(&db, "project").await.unwrap();
        assert_eq!(rows[0].machine.name, "Server host");
        assert_eq!(rows[0].machine.id, "server");
        assert_eq!(
            rows[0].status,
            api_types::EnvironmentReadinessStatus::Unknown
        );
        assert!(rows[0].checked_at.is_none());
    }
}
