use api_types::ProjectSettings;
use chrono::{DateTime, Utc};
use db::Project;

use crate::{
    project_environment::{
        bounded_output_tail, pause_detail, ENVIRONMENT_NOT_READY, NO_RERUNNABLE_CHECK,
    },
    Result, ServiceError,
};

use super::TaskDispatcher;

impl TaskDispatcher {
    pub(super) async fn sync_environment_pause(&self, project: &Project) -> Result<bool> {
        self.observe_environment_settings();
        use db::ProjectMachineReadinessRepo;
        let mut rows = self.db.list_readiness(&project.id).await?;
        if rows.is_empty() && project.system_pause_reason.as_deref() == Some(ENVIRONMENT_NOT_READY)
        {
            let Some(mut detail) = pause_detail(project)? else {
                return Ok(false);
            };
            let environment = serde_json::from_str::<ProjectSettings>(&project.settings)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
                .environment;
            if !environment
                .checks
                .iter()
                .any(|check| detail.checks.contains(&check.name))
            {
                if !detail.output.contains(NO_RERUNNABLE_CHECK) {
                    detail.output =
                        bounded_output_tail(&format!("{}\n{NO_RERUNNABLE_CHECK}", detail.output));
                    self.task_service
                        .update_environment_pause(project, &detail)
                        .await?;
                }
                return Ok(false);
            }
            DateTime::parse_from_rfc3339(&detail.next_check_at).map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "invalid environment re-check time: {error}"
                ))
            })?;
            // A pause can also be recorded directly through the repository.
            // Give that fact the same machine record as a launch failure.
            let mut row = crate::placement::environment::unknown_record(
                &project.id,
                db::EnvironmentMachine::Server,
                &environment,
            );
            row.status = db::EnvironmentReadinessStatus::NotReady;
            row.failing_checks = detail
                .checks
                .into_iter()
                .map(|name| db::ReadinessCheckFailure {
                    name,
                    output_tail: detail.output.clone(),
                })
                .collect();
            row.workspace_id = detail.workspace_id;
            row.role = detail.role;
            row.checked_at = Some(detail.last_checked_at);
            row.next_check_at = Some(detail.next_check_at);
            match self.db.put_readiness(row, None).await {
                Ok(row) => rows.push(row),
                Err(db::DbError::VersionConflict) => {
                    rows = self.db.list_readiness(&project.id).await?
                }
                Err(error) => return Err(error.into()),
            }
        }
        crate::placement::environment::schedule_project_probes(
            &self.db,
            project,
            &self.task_service.workspace_backend_router(),
        )
        .await?;
        let environment = serde_json::from_str::<ProjectSettings>(&project.settings)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
            .environment;
        let digest = db::environment_checks_digest(&environment);
        if project.system_pause_reason.as_deref() == Some(ENVIRONMENT_NOT_READY)
            && pause_detail(project)?.is_some_and(|detail| !detail.checks.is_empty())
            && rows.iter().any(|row| {
                row.checks_digest == digest && row.status == db::EnvironmentReadinessStatus::Ready
            })
        {
            return self.task_service.clear_environment_pause(project).await;
        }
        let mut changed = false;
        for row in rows {
            let key = if row.machine == db::EnvironmentMachine::Server {
                project.id.clone()
            } else {
                format!(
                    "{}@{}",
                    project.id,
                    crate::placement::environment::machine_label(&row.machine)
                )
            };
            let finished = {
                let mut jobs = self
                    .environment_rechecks
                    .lock()
                    .expect("environment jobs lock");
                match jobs.get(&key) {
                    Some(job) if !job.is_finished() => continue,
                    Some(_) => jobs.remove(&key),
                    None => None,
                }
            };
            if let Some(job) = finished {
                changed |= job
                    .await
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))??;
                continue;
            }
            if row.status != db::EnvironmentReadinessStatus::NotReady || row.checks_digest != digest
            {
                continue;
            }
            let due = row
                .next_check_at
                .as_deref()
                .and_then(|at| DateTime::parse_from_rfc3339(at).ok());
            if due.is_some_and(|due| Utc::now() < due) {
                continue;
            }
            let checks: Vec<_> = environment
                .checks
                .iter()
                .filter(|check| {
                    row.failing_checks
                        .iter()
                        .any(|failure| failure.name == check.name)
                })
                .cloned()
                .collect();
            if checks.is_empty() {
                continue;
            }
            let Some(guard) = crate::placement::environment::claim_probe(&project.id, &row.machine)
            else {
                continue;
            };
            let legacy_guard = if row.machine == db::EnvironmentMachine::Server {
                let Some(guard) = self.task_service.claim_environment_recheck(&project.id) else {
                    continue;
                };
                Some(guard)
            } else {
                None
            };
            let service = std::sync::Arc::clone(&self.task_service);
            let db = std::sync::Arc::clone(&self.db);
            let project = project.clone();
            let environment = environment.clone();
            let job = tokio::spawn(async move {
                let _guard = guard;
                let _legacy_guard = legacy_guard;
                let target = if row.machine == db::EnvironmentMachine::Server {
                    service
                        .environment_check_checkout(&project)
                        .await
                        .map(|path| Some(crate::placement::environment::ProbeTarget::Server(path)))
                } else {
                    crate::placement::environment::target_for_machine(
                        &db,
                        &project,
                        &row.machine,
                        &service.workspace_backend_router(),
                        row.workspace_id.as_deref(),
                    )
                    .await
                };
                let results = match target {
                    Ok(Some(target)) => {
                        crate::placement::environment::run_checks(&target, &environment, &checks)
                            .await
                    }
                    Ok(None) => return Ok(false), // Keep the unreachable owner's fact.
                    Err(error) => Err(error),
                };
                let results = match results {
                    Ok(results) => results,
                    Err(
                        ServiceError::DaemonUnavailable { .. } | ServiceError::DaemonTimeout { .. },
                    ) => return Ok(false),
                    Err(error) => checks
                        .iter()
                        .map(|check| api_types::ProjectEnvironmentCheckResult {
                            name: check.name.clone(),
                            passed: false,
                            exit_code: None,
                            output_tail: bounded_output_tail(&error.to_string()),
                        })
                        .collect(),
                };
                let saved = match crate::placement::environment::save_results(
                    &db,
                    row,
                    &environment,
                    results,
                )
                .await
                {
                    Ok(saved) => saved,
                    Err(ServiceError::Db(db::DbError::VersionConflict)) => return Ok(false),
                    Err(error) => return Err(error),
                };
                if saved.status == db::EnvironmentReadinessStatus::Ready {
                    return service.clear_environment_pause(&project).await;
                }
                // Only the pause belonging to this machine receives its detail.
                if let Some(mut detail) = pause_detail(&project)? {
                    let pause: serde_json::Value = serde_json::from_str(
                        project.environment_pause_json.as_deref().unwrap_or("{}"),
                    )
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                    let machine: db::EnvironmentMachine =
                        serde_json::from_value(pause["machine"].clone())
                            .unwrap_or(db::EnvironmentMachine::Server);
                    if machine == saved.machine {
                        detail.checks = saved
                            .failing_checks
                            .iter()
                            .map(|check| check.name.clone())
                            .collect();
                        detail.output = bounded_output_tail(
                            &saved
                                .failing_checks
                                .iter()
                                .map(|check| format!("{}: {}", check.name, check.output_tail))
                                .collect::<Vec<_>>()
                                .join("\n"),
                        );
                        detail.last_checked_at = saved.checked_at.unwrap();
                        detail.next_check_at = saved.next_check_at.unwrap();
                        service.update_environment_pause(&project, &detail).await?;
                    }
                }
                Ok(false)
            });
            self.environment_rechecks
                .lock()
                .expect("environment jobs lock")
                .insert(key, job);
        }
        Ok(changed)
    }

    /// Project PATCH publishes project.updated after committing settings. A
    /// weak observer starts invalidated probes immediately, even with no Task.
    /// Periodic scans also reconcile changes made by non-event DB callers.
    fn observe_environment_settings(&self) {
        self.environment_settings_observer.get_or_init(|| {
        let mut events = self.event_bus.subscribe();
        let service = std::sync::Arc::downgrade(&self.task_service);
        let db = std::sync::Arc::downgrade(&self.db);
        tokio::spawn(async move {
            loop {
                let event = match events.recv().await {
                    Ok(event) => event,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(false),
                };
                if event.event_type != "project.updated" { continue; }
                let (Some(service), Some(db)) = (service.upgrade(), db.upgrade()) else { return Ok(false); };
                if let Some(project) = db::ProjectRepo::get_by_id(&*db, &event.entity_id).await? {
                    if let Err(error) = crate::placement::environment::schedule_project_probes(&db, &project, &service.workspace_backend_router()).await {
                        tracing::warn!(project_id = %project.id, %error, "settings environment probes could not start");
                    }
                }
            }
        })
        });
    }
}
