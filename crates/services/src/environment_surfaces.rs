//! Public projections and manual machine checks; placement and scheduling stay unchanged.
use crate::{placement::environment, Result, ServiceError, TaskService};
use api_types::{MachineEnvironmentRecheckResult, MachineIdentity, ProjectEnvironmentReadiness};
use db::{EnvironmentMachine, ProjectMachineReadinessRepo, ProjectRepo, SqliteDb};
use sqlx::Row;
use std::collections::{BTreeMap, BTreeSet};

/// Daemon names and legacy pause owners resolved in one bounded statement.
#[derive(Default)]
struct MachineCatalog {
    names: BTreeMap<String, String>,
    legacy_owners: BTreeMap<String, EnvironmentMachine>,
}
impl MachineCatalog {
    async fn load(
        db: &SqliteDb,
        daemon_ids: &BTreeSet<String>,
        workspace_ids: &BTreeSet<String>,
    ) -> Result<Self> {
        let mut catalog = Self::default();
        if daemon_ids.is_empty() && workspace_ids.is_empty() {
            return Ok(catalog);
        }
        let rows = sqlx::query(
            "WITH legacy AS (SELECT workspace_id,owner_kind,daemon_id,runtime_id FROM workspace_placement WHERE workspace_id IN (SELECT value FROM json_each(?)))
             SELECT 'daemon' AS kind,id,hostname,NULL AS owner_kind,NULL AS daemon_id,NULL AS runtime_id FROM daemon
               WHERE id IN (SELECT value FROM json_each(?)) OR id IN (SELECT daemon_id FROM legacy)
             UNION ALL SELECT 'placement',workspace_id,NULL,owner_kind,daemon_id,runtime_id FROM legacy")
            .bind(json_ids(workspace_ids)?).bind(json_ids(daemon_ids)?).fetch_all(db.pool()).await?;
        for row in rows {
            let id: String = row.try_get("id")?;
            if row.try_get::<String, _>("kind")? == "daemon" {
                catalog.names.insert(id, row.try_get("hostname")?);
            } else {
                let machine = if row.try_get::<String, _>("owner_kind")? == "server" {
                    EnvironmentMachine::Server
                } else {
                    EnvironmentMachine::Daemon {
                        daemon_id: row.try_get("daemon_id")?,
                        runtime_id: row.try_get("runtime_id")?,
                    }
                };
                catalog.legacy_owners.insert(id, machine);
            }
        }
        Ok(catalog)
    }
    fn identity(&self, machine: &EnvironmentMachine) -> MachineIdentity {
        match machine {
            EnvironmentMachine::Server => MachineIdentity {
                id: "server".into(),
                name: "Server host".into(),
                owner_kind: api_types::RepoLocationOwnerKind::Server,
                daemon_id: None,
                runtime_id: None,
            },
            EnvironmentMachine::Daemon {
                daemon_id,
                runtime_id,
            } => MachineIdentity {
                id: runtime_id.clone(),
                name: self
                    .names
                    .get(daemon_id)
                    .cloned()
                    .unwrap_or_else(|| "Unavailable daemon".into()),
                owner_kind: api_types::RepoLocationOwnerKind::Daemon,
                daemon_id: Some(daemon_id.clone()),
                runtime_id: Some(runtime_id.clone()),
            },
        }
    }
}
fn json_ids(ids: impl serde::Serialize) -> Result<String> {
    serde_json::to_string(&ids).map_err(|error| ServiceError::invalid_operation(error.to_string()))
}
fn collect_daemon(machine: &EnvironmentMachine, ids: &mut BTreeSet<String>) {
    if let EnvironmentMachine::Daemon { daemon_id, .. } = machine {
        ids.insert(daemon_id.clone());
    }
}
pub async fn machine_identity(
    db: &SqliteDb,
    machine: &EnvironmentMachine,
) -> Result<MachineIdentity> {
    let mut ids = BTreeSet::new();
    collect_daemon(machine, &mut ids);
    Ok(MachineCatalog::load(db, &ids, &BTreeSet::new())
        .await?
        .identity(machine))
}
fn readiness_entry(
    row: db::ProjectMachineReadiness,
    catalog: &MachineCatalog,
) -> ProjectEnvironmentReadiness {
    ProjectEnvironmentReadiness {
        machine: catalog.identity(&row.machine),
        status: match row.status {
            db::EnvironmentReadinessStatus::Ready => api_types::EnvironmentReadinessStatus::Ready,
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
    }
}
pub async fn readiness(db: &SqliteDb, project: &str) -> Result<Vec<ProjectEnvironmentReadiness>> {
    let rows = db.list_readiness(project).await?;
    let mut ids = BTreeSet::new();
    for row in &rows {
        collect_daemon(&row.machine, &mut ids);
    }
    let catalog = MachineCatalog::load(db, &ids, &BTreeSet::new()).await?;
    let mut entries: Vec<_> = rows
        .into_iter()
        .map(|row| readiness_entry(row, &catalog))
        .collect();
    entries.sort_by(|a, b| a.machine.id.cmp(&b.machine.id));
    Ok(entries)
}

#[derive(Default)]
pub struct ProjectEnvironmentRead {
    pub readiness: Vec<ProjectEnvironmentReadiness>,
    pub pause_machine: Option<MachineIdentity>,
}
/// One readiness SELECT for the page, then at most one name/legacy-owner SELECT.
/// Response assembly itself never performs I/O. Empty Projects add no queries.
pub async fn project_environments(
    db: &SqliteDb,
    projects: &[db::Project],
) -> Result<BTreeMap<String, ProjectEnvironmentRead>> {
    if projects.is_empty() {
        return Ok(BTreeMap::new());
    }
    let ids: Vec<_> = projects.iter().map(|project| &project.id).collect();
    let rows = sqlx::query("SELECT * FROM project_machine_readiness WHERE project_id IN (SELECT value FROM json_each(?)) ORDER BY project_id,owner_kind,daemon_id,runtime_id")
        .bind(json_ids(&ids)?).fetch_all(db.pool()).await?;
    let mut records = Vec::new();
    let mut daemon_ids = BTreeSet::new();
    for row in rows {
        let machine = if row.try_get::<String, _>("owner_kind")? == "server" {
            EnvironmentMachine::Server
        } else {
            EnvironmentMachine::Daemon {
                daemon_id: row.try_get("daemon_id")?,
                runtime_id: row.try_get("runtime_id")?,
            }
        };
        collect_daemon(&machine, &mut daemon_ids);
        // Decode the same persisted record as the repository's single-row read.
        let decode = |column| -> Result<serde_json::Value> {
            serde_json::from_str(&row.try_get::<String, _>(column)?)
                .map_err(|error| db::DbError::Check(error.to_string()).into())
        };
        records.push(db::ProjectMachineReadiness {
            project_id: row.try_get("project_id")?,
            machine,
            status: match row.try_get::<String, _>("status")?.as_str() {
                "ready" => db::EnvironmentReadinessStatus::Ready,
                "not_ready" => db::EnvironmentReadinessStatus::NotReady,
                _ => db::EnvironmentReadinessStatus::Unknown,
            },
            checks_digest: row.try_get("checks_digest")?,
            failing_checks: serde_json::from_value(decode("failing_checks_json")?)
                .map_err(|error| db::DbError::Check(error.to_string()))?,
            check_results: serde_json::from_value(decode("check_results_json")?)
                .map_err(|error| db::DbError::Check(error.to_string()))?,
            output_tail: row.try_get("output_tail")?,
            scope_covered: row.try_get("scope_covered")?,
            role: row.try_get("role")?,
            workspace_id: row.try_get("workspace_id")?,
            checked_at: row.try_get("checked_at")?,
            next_check_at: row.try_get("next_check_at")?,
            version: row.try_get("version")?,
        });
    }
    let mut pauses = BTreeMap::new();
    let mut workspace_ids = BTreeSet::new();
    for project in projects {
        if let Some(raw) = project.environment_pause_json.as_deref() {
            // The API's shared assembler retains its strict pause validation.
            let value: serde_json::Value = serde_json::from_str(raw).unwrap_or_default();
            let machine: Option<EnvironmentMachine> =
                serde_json::from_value(value["machine"].clone()).ok();
            let workspace = if value.get("machine").is_none() {
                value["workspace_id"].as_str().map(str::to_owned)
            } else {
                None
            };
            if let Some(machine) = &machine {
                collect_daemon(machine, &mut daemon_ids);
            }
            if let Some(workspace) = &workspace {
                workspace_ids.insert(workspace.clone());
            }
            pauses.insert(project.id.clone(), (machine, workspace));
        }
    }
    let catalog = MachineCatalog::load(db, &daemon_ids, &workspace_ids).await?;
    let mut result: BTreeMap<_, _> = ids
        .into_iter()
        .map(|id| (id.clone(), ProjectEnvironmentRead::default()))
        .collect();
    for row in records {
        result
            .get_mut(&row.project_id)
            .expect("requested Project")
            .readiness
            .push(readiness_entry(row, &catalog));
    }
    for (id, (machine, workspace)) in pauses {
        let machine = machine
            .or_else(|| workspace.and_then(|id| catalog.legacy_owners.get(&id).cloned()))
            .unwrap_or(EnvironmentMachine::Server);
        result
            .get_mut(&id)
            .expect("requested Project")
            .pause_machine = Some(catalog.identity(&machine));
    }
    for entry in result.values_mut() {
        entry
            .readiness
            .sort_by(|a, b| a.machine.id.cmp(&b.machine.id));
    }
    Ok(result)
}

/// Executor fit only: no repository, environment or capacity assertion.
/// Single responses use the same fact loader and assembly as list pages.
pub async fn runnable_on(
    db: &SqliteDb,
    agent: &db::Agent,
    registry: &executors::AdapterRegistry,
    connections: &crate::daemon_transport::DaemonConnectionRegistry,
    admin: bool,
) -> Result<api_types::AgentRunnableOn> {
    Ok(runnable_on_for_agents(
        db,
        std::slice::from_ref(agent),
        registry,
        connections,
        admin,
    )
    .await?
    .remove(&agent.id)
    .expect("requested Agent"))
}
/// At most three statements per page: daemon/runtime facts (with names), native
/// profile health when needed, and CLI enabled policies when needed.
pub async fn runnable_on_for_agents(
    db: &SqliteDb,
    agents: &[db::Agent],
    registry: &executors::AdapterRegistry,
    connections: &crate::daemon_transport::DaemonConnectionRegistry,
    admin: bool,
) -> Result<BTreeMap<String, api_types::AgentRunnableOn>> {
    if agents.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query("SELECT d.id,d.hostname,d.machine_id,d.owner_id,d.visibility,d.detected_clis_json,r.id AS runtime_id FROM daemon AS d LEFT JOIN runtime r ON r.daemon_id=d.id AND r.status='ready' WHERE d.status <> 'offline' AND (r.id IS NOT NULL OR d.machine_id=?) ORDER BY d.id,r.id")
        .bind(db.server_run_cap.embedded_machine_id()).fetch_all(db.pool()).await?;
    let mut catalog = MachineCatalog::default();
    let mut embedded = None;
    let mut runtimes = Vec::new();
    let mut daemon_ids = BTreeSet::new();
    for row in rows {
        let id: String = row.try_get("id")?;
        catalog.names.insert(id.clone(), row.try_get("hostname")?);
        daemon_ids.insert(id.clone());
        if row.try_get::<String, _>("machine_id")? == db.server_run_cap.embedded_machine_id() {
            embedded = Some(id);
            continue;
        }
        let detected: serde_json::Value =
            serde_json::from_str(&row.try_get::<String, _>("detected_clis_json")?)
                .unwrap_or_default();
        runtimes.push((
            id,
            row.try_get::<String, _>("runtime_id")?,
            row.try_get::<Option<String>, _>("owner_id")?,
            row.try_get::<String, _>("visibility")?,
            detected,
        ));
    }
    let profiles: BTreeSet<_> = agents
        .iter()
        .filter(|agent| agent.backend_kind == "native")
        .map(|agent| &agent.profile_id)
        .collect();
    let mut healthy = BTreeSet::new();
    if !profiles.is_empty() {
        for row in sqlx::query("SELECT profile_id,status FROM agent_connection_health WHERE profile_id IN (SELECT value FROM json_each(?))")
            .bind(json_ids(&profiles)?).fetch_all(db.pool()).await? {
            if row.try_get::<String,_>("status")? == "healthy" { healthy.insert(row.try_get::<String,_>("profile_id")?); }
        }
    }
    let cli_types: BTreeSet<_> = agents
        .iter()
        .filter(|agent| agent.backend_kind == "cli")
        .map(|agent| &agent.executor_type)
        .collect();
    let mut policies = BTreeMap::new();
    let mut global_policies: BTreeMap<(String, String), bool> = BTreeMap::new();
    if !cli_types.is_empty() && !daemon_ids.is_empty() {
        for row in sqlx::query("SELECT owner_user_id,daemon_id,executor_type,enabled FROM cli_runtime_policy WHERE daemon_id IN (SELECT value FROM json_each(?)) AND executor_type IN (SELECT value FROM json_each(?))")
            .bind(json_ids(&daemon_ids)?).bind(json_ids(&cli_types)?).fetch_all(db.pool()).await? {
            let owner: String = row.try_get("owner_user_id")?; let daemon: String = row.try_get("daemon_id")?; let executor: String = row.try_get("executor_type")?; let enabled: bool = row.try_get("enabled")?;
            global_policies.entry((daemon.clone(),executor.clone())).and_modify(|old|*old &= enabled).or_insert(enabled);
            policies.insert((owner,daemon,executor),enabled);
        }
    }
    let enabled = |agent: &db::Agent, daemon: Option<&str>| {
        let Some(daemon) = daemon.filter(|_| agent.backend_kind == "cli") else {
            return true;
        };
        if let Some(owner) = &agent.owner_id {
            policies
                .get(&(owner.clone(), daemon.into(), agent.executor_type.clone()))
                .copied()
                .unwrap_or(true)
        } else {
            global_policies
                .get(&(daemon.into(), agent.executor_type.clone()))
                .copied()
                .unwrap_or(true)
        }
    };
    let mut availability = BTreeMap::new();
    let mut result = BTreeMap::new();
    for agent in agents {
        // Same installed/authenticated/enabled inputs as server_executor_facts;
        // avoid another SQL health read for every native Agent on the page.
        let available = if agent.paused {
            // Paused identities cannot contribute either host or remote
            // machines. CLI probes can spawn processes; their result is unused.
            false
        } else if agent.backend_kind == "native" {
            healthy.contains(&agent.profile_id)
        } else {
            *availability
                .entry(agent.executor_type.clone())
                .or_insert_with(|| {
                    agent
                        .executor_type
                        .parse::<executors::ExecutorKind>()
                        .ok()
                        .and_then(|kind| registry.get(&kind))
                        .is_some_and(|adapter| {
                            matches!(
                                adapter.check_availability().status,
                                executors::AvailabilityStatus::Authenticated
                            )
                        })
                })
        };
        let mut machines = Vec::new();
        if !agent.paused
            && available
            && agent
                .daemon_id
                .as_ref()
                .is_none_or(|pin| Some(pin) == embedded.as_ref())
            && enabled(agent, embedded.as_deref())
        {
            machines.push(catalog.identity(&EnvironmentMachine::Server));
        }
        if agent.backend_kind == "cli" && !agent.paused {
            for (daemon, runtime, owner, visibility, detected) in &runtimes {
                if agent.daemon_id.as_ref().is_some_and(|pin| pin != daemon)
                    || !(visibility == "global" || owner.is_none() || owner == &agent.owner_id)
                {
                    continue;
                }
                let Some(connection) = connections.get(daemon) else {
                    continue;
                };
                if connection.is_stale() || !connection.protocol_allows_dispatch() {
                    continue;
                }
                let authenticated = detected.as_array().is_some_and(|clis| {
                    clis.iter().any(|cli| {
                        cli["kind"].as_str() == Some(&agent.executor_type)
                            && cli["availability"].as_str() == Some("authenticated")
                    })
                });
                if authenticated && enabled(agent, Some(daemon)) {
                    machines.push(catalog.identity(&EnvironmentMachine::Daemon {
                        daemon_id: daemon.clone(),
                        runtime_id: runtime.clone(),
                    }));
                }
            }
        }
        result.insert(
            agent.id.clone(),
            api_types::AgentRunnableOn {
                count: machines.len().try_into().unwrap_or(u32::MAX),
                machines: admin.then_some(machines),
            },
        );
    }
    Ok(result)
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
            return Err(if id == "server" {
                ServiceError::not_found(
                    "repository location on server for project",
                    project_id.to_owned(),
                )
            } else {
                ServiceError::not_found("machine", id.to_owned())
            });
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
                service.environment_daemon_connections(),
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
    struct CountedAvailability(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait::async_trait]
    impl executors::CodingExecutorAdapter for CountedAvailability {
        fn kind(&self) -> executors::ExecutorKind {
            executors::ExecutorKind::Shell
        }
        fn check_availability(&self) -> executors::AvailabilityInfo {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            executors::AvailabilityInfo {
                status: executors::AvailabilityStatus::Authenticated,
                authenticated_at: None,
                config_path: None,
            }
        }
        async fn discover_options(
            &self,
            _: executors::DiscoverContext,
        ) -> std::result::Result<executors::DiscoveredOptions, executors::ExecutorError> {
            unreachable!()
        }
        async fn execute(
            &self,
            _: executors::ExecutionContext,
        ) -> std::result::Result<executors::ExecutionResult, executors::ExecutorError> {
            unreachable!()
        }
        async fn cancel(&self, _: &str) -> std::result::Result<(), executors::ExecutorError> {
            unreachable!()
        }
    }
    #[tokio::test]
    async fn paused_agent_pages_skip_unused_cli_probes() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let fixture_registry = cli_adapters::test_support::test_registry();
        let mut agent = crate::ensure_default_agents(&db, &fixture_registry)
            .await
            .unwrap()
            .into_iter()
            .find(|agent| agent.executor_type == "shell")
            .unwrap();
        agent.paused = true;
        agent.daemon_id = None;
        let probes = Arc::new(AtomicUsize::new(0));
        let mut registry = executors::AdapterRegistry::new();
        registry.register(Box::new(CountedAvailability(probes.clone())));
        let connections = crate::daemon_transport::DaemonConnectionRegistry::without_handlers();
        let mut page = (0..20)
            .map(|n| {
                let mut copy = agent.clone();
                copy.id = format!("paused-{n}");
                copy
            })
            .collect::<Vec<_>>();
        for admin in [true, false] {
            let result = runnable_on_for_agents(&db, &page, &registry, &connections, admin)
                .await
                .unwrap();
            assert!(result.values().all(|value| value.count == 0));
            assert_eq!(
                probes.load(Ordering::Relaxed),
                0,
                "paused page cannot need CLI installation/auth probes"
            );
        }
        page[19].paused = false;
        let result = runnable_on_for_agents(&db, &page, &registry, &connections, true)
            .await
            .unwrap();
        assert_eq!(result["paused-19"].count, 1);
        assert_eq!(
            probes.load(Ordering::Relaxed),
            1,
            "an active Agent still probes once per executor family"
        );
    }
    #[tokio::test]
    async fn runnable_batch_preserves_native_health_policy_pins_and_visibility() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let registry = cli_adapters::test_support::test_registry();
        let agents = crate::ensure_default_agents(&db, &registry).await.unwrap();
        let mut shell = agents
            .into_iter()
            .find(|agent| agent.executor_type == "shell")
            .unwrap();
        shell.id = "shell".into();
        sqlx::query("INSERT INTO daemon (id,machine_id,hostname,os,arch,status,detected_clis_json,created_at,updated_at) VALUES ('host',?,'Host','linux','x86_64','online','[]','now','now'),('remote','remote-machine','Remote','linux','x86_64','online',?,'now','now')")
            .bind(db.server_run_cap.embedded_machine_id()).bind(serde_json::json!([{"kind":"shell","availability":"authenticated"}]).to_string()).execute(db.pool()).await.unwrap();
        let root = tempfile::tempdir().unwrap();
        sqlx::query("INSERT INTO runtime(id,daemon_id,kind,workspace_root,status,created_at,updated_at) VALUES ('remote-runtime','remote','local',?,'ready','now','now')").bind(root.path().to_string_lossy().as_ref()).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO user(id,email,password_hash,created_at,updated_at) VALUES ('owner','owner@example.test','test','now','now')").execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO cli_runtime_policy(owner_user_id,daemon_id,executor_type,enabled,created_at,updated_at) VALUES ('owner','host','shell',0,'now','now')").execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO agent_connection_health(profile_id,status,updated_at) VALUES (?,'healthy','now')").bind(&shell.profile_id).execute(db.pool()).await.unwrap();
        let connections = crate::daemon_transport::DaemonConnectionRegistry::without_handlers();
        let (connection, _receiver) =
            crate::daemon_transport::DaemonConnection::new("remote".into());
        connections.register("remote".into(), connection);
        connections.dispatch_incoming(
            "remote",
            api_types::DaemonFrame::Notification {
                method: api_types::METHOD_DAEMON_HANDSHAKE.into(),
                params: serde_json::to_value(api_types::DaemonHandshakeNotification {
                    protocol_revision: api_types::DAEMON_PROTOCOL_REVISION,
                    capabilities: vec![
                        api_types::DAEMON_CAPABILITY_USAGE_REPORTS.into(),
                        api_types::DAEMON_CAPABILITY_JOURNAL_ACK.into(),
                        api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT.into(),
                    ],
                    executor_capabilities: Default::default(),
                    workspace_run_policy: Default::default(),
                })
                .unwrap(),
            },
        );
        let mut native = shell.clone();
        native.id = "native".into();
        native.backend_kind = "native".into();
        let mut paused = native.clone();
        paused.id = "paused".into();
        paused.paused = true;
        let mut pinned = shell.clone();
        pinned.id = "pinned".into();
        pinned.daemon_id = Some("remote".into());
        let mut owned = shell.clone();
        owned.id = "owned".into();
        owned.owner_id = Some("owner".into());
        let input = vec![shell, native, paused, pinned, owned];
        let result = runnable_on_for_agents(&db, &input, &registry, &connections, true)
            .await
            .unwrap();
        assert_eq!(result["native"].count, 1);
        assert_eq!(result["native"].machines.as_ref().unwrap()[0].id, "server");
        assert_eq!(result["paused"].count, 0);
        assert_eq!(
            result["shell"].count, 1,
            "global MIN policy disables the host"
        );
        assert_eq!(result["owned"].count, 1, "owned policy disables the host");
        assert_eq!(
            result["pinned"].machines.as_ref().unwrap()[0].name,
            "Remote"
        );
        assert_eq!(
            result["pinned"].machines.as_ref().unwrap()[0].id,
            "remote-runtime"
        );
        let hidden = runnable_on_for_agents(&db, &input, &registry, &connections, false)
            .await
            .unwrap();
        assert!(hidden.values().all(|entry| entry.machines.is_none()));
        assert_eq!(hidden["native"].count, 1);
        sqlx::query("UPDATE daemon SET visibility='account',owner_id='owner' WHERE id='remote'")
            .execute(db.pool())
            .await
            .unwrap();
        let private = runnable_on_for_agents(&db, &input, &registry, &connections, true)
            .await
            .unwrap();
        assert_eq!(private["shell"].count, 0);
        assert_eq!(private["owned"].count, 1);
        connections.get("remote").unwrap().mark_stale();
        let offline = runnable_on_for_agents(&db, &input, &registry, &connections, true)
            .await
            .unwrap();
        assert_eq!(offline["owned"].count, 0);
        assert_eq!(offline["native"].count, 1);
    }

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
