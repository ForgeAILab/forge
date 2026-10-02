//! One due-row scan starts per-machine jobs; no per-Project readiness polling.
use super::TaskDispatcher;
use crate::{
    placement::environment::{self, ProbeTarget},
    project_environment::{
        bounded_output_tail, next_check_at, pause_detail, ENVIRONMENT_NOT_READY,
    },
    Result, ServiceError,
};
use api_types::ProjectSettings;
use db::{EnvironmentMachine, ProjectMachineReadinessRepo, ProjectRepo};
use std::collections::HashSet;

impl TaskDispatcher {
    pub(super) async fn sync_due_environment_checks(&self) -> Result<HashSet<String>> {
        let finished: Vec<_> = {
            let mut jobs = self
                .environment_rechecks
                .lock()
                .expect("environment jobs lock");
            let keys: Vec<_> = jobs
                .iter()
                .filter(|(_, job)| job.is_finished())
                .map(|(key, _)| key.clone())
                .collect();
            keys.into_iter()
                .map(|key| {
                    let job = jobs.remove(&key).expect("finished job");
                    (key, job)
                })
                .collect()
        };
        let mut changed = HashSet::new();
        for (key, job) in finished {
            match job.await {
                Ok(Ok(true)) => {
                    changed.insert(key.split('@').next().unwrap_or(&key).to_owned());
                }
                Ok(Ok(false)) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%key,%error,"environment re-check job failed; continuing dispatch")
                }
                Err(error) => {
                    tracing::warn!(%key,%error,"environment re-check job panicked; continuing dispatch")
                }
            }
        }
        // Index-backed and executed once for the entire dispatcher pass.
        let due = self.db.due_readiness(&db::now_rfc3339()).await?;
        for row in due {
            match self.schedule_environment_recheck(row.clone()).await {
                Ok(Some(project)) => {
                    changed.insert(project);
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(project_id=%row.project_id,%error,"environment re-check setup failed; continuing dispatch");
                    if let Err(error) = self
                        .db
                        .reschedule_readiness(&row, &next_check_at(chrono::Utc::now(), 600))
                        .await
                    {
                        tracing::warn!(project_id=%row.project_id,%error,"could not reschedule environment row");
                    }
                }
            }
        }
        Ok(changed)
    }

    async fn schedule_environment_recheck(
        &self,
        row: db::ProjectMachineReadiness,
    ) -> Result<Option<String>> {
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &row.project_id).await? else {
            return Ok(None);
        };
        let settings: ProjectSettings = match serde_json::from_str(&project.settings) {
            Ok(settings) => settings,
            Err(error) => {
                tracing::warn!(project_id=%project.id,%error,"cannot re-check malformed environment settings; rescheduling");
                self.db
                    .reschedule_readiness(&row, &next_check_at(chrono::Utc::now(), 600))
                    .await?;
                return Ok(None);
            }
        };
        let environment = settings.environment;
        if project.paused_at.is_some()
            && project.system_pause_reason.as_deref() != Some(ENVIRONMENT_NOT_READY)
        {
            // A user/repository pause retains the base's explicit manual-check
            // behaviour; it must not start an automatic command behind the pause.
            self.db
                .reschedule_readiness(
                    &row,
                    &next_check_at(chrono::Utc::now(), environment.recheck_interval_seconds),
                )
                .await?;
            return Ok(None);
        }
        if row.checks_digest != db::environment_checks_digest(&environment) {
            self.db
                .reschedule_readiness(
                    &row,
                    &next_check_at(chrono::Utc::now(), environment.recheck_interval_seconds),
                )
                .await?;
            return Ok(None);
        }
        let key = if row.machine == EnvironmentMachine::Server {
            project.id.clone()
        } else {
            format!(
                "{}@{}",
                project.id,
                environment::machine_label(&row.machine)
            )
        };
        if self
            .environment_rechecks
            .lock()
            .expect("environment jobs lock")
            .contains_key(&key)
        {
            return Ok(None);
        }
        let Some(guard) = environment::claim_probe(&project.id, &row.machine) else {
            return Ok(None);
        };
        let manual_guard = if row.machine == EnvironmentMachine::Server {
            let Some(guard) = self.task_service.claim_environment_recheck(&project.id) else {
                return Ok(None);
            };
            Some(guard)
        } else {
            None
        };
        if matches!(row.machine, EnvironmentMachine::Daemon { .. }) {
            if let Some(workspace_id) = row.workspace_id.as_deref() {
                if db::WorkspaceRepo::get_by_id(&*self.db, workspace_id)
                    .await?
                    .is_none()
                {
                    let mut unknown = row.clone();
                    unknown.status = db::EnvironmentReadinessStatus::Unknown;
                    unknown.next_check_at = None;
                    self.db.put_readiness(unknown, Some(row.version)).await?;
                    let current = ProjectRepo::get_by_id(&*self.db, &project.id)
                        .await?
                        .ok_or(db::DbError::NotFound)?;
                    let matching = current
                        .environment_pause_json
                        .as_deref()
                        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
                        .is_some_and(|detail| {
                            detail["workspace_id"].as_str() == Some(workspace_id)
                        });
                    let cleared = matching
                        && !row.failing_checks.is_empty()
                        && self.task_service.clear_environment_pause(&current).await?;
                    self.task_service.dispatch_notify().notify_one();
                    return Ok(cleared.then_some(project.id));
                }
            }
        }
        if row.failing_checks.is_empty() {
            self.db
                .reschedule_readiness(
                    &row,
                    &next_check_at(chrono::Utc::now(), environment.recheck_interval_seconds),
                )
                .await?;
            return Ok(None); // Unnamed setup failures require resume or Check now.
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
        let db = self.db.clone();
        let service = self.task_service.clone();
        let kick = self.task_service.dispatch_notify();
        let result_events = self.event_bus.clone();
        let job = tokio::spawn(async move {
            let result = async {
                    let next = next_check_at(chrono::Utc::now(), environment.recheck_interval_seconds);
                    if checks.is_empty() {
                        db.reschedule_readiness(&row, &next).await?;
                        return Ok(false);
                    }
                    let target = if row.machine == EnvironmentMachine::Server {
                        service.environment_check_checkout(&project).await
                            .map(|path| Some(ProbeTarget::Server(path)))
                    } else {
                        environment::target_for_machine(&db, &project, &row.machine,
                            &service.workspace_backend_router(), row.workspace_id.as_deref()).await
                    };
                    let results = match target {
                        Ok(Some(target)) => environment::run_checks(&target, &environment, &checks).await,
                        Ok(None) => {
                            db.reschedule_readiness(&row, &next).await?;
                            return Ok(false);
                        }
                        Err(error) => Err(error),
                    };
                    let results = match results {
                        Ok(results) => results,
                        Err(error) => {
                            // A transport timeout may outlast the interval. The
                            // next attempt is due after completion, not job start.
                            let completed = chrono::Utc::now();
                            let next = next_check_at(completed,environment.recheck_interval_seconds);
                            db.reschedule_readiness(&row, &next).await?;
                            // Host Project diagnostics retain the base behaviour.
                            // Daemon transport/fence errors never become check facts
                            // or touch another Task's run.
                            if row.machine == EnvironmentMachine::Server {
                                if let Some(mut detail) = pause_detail(&project)? {
                                    detail.output = bounded_output_tail(&error.to_string());
                                    detail.last_checked_at = completed.to_rfc3339();
                                    detail.next_check_at = next;
                                    service.update_environment_pause(&project, &detail).await?;
                                }
                            }
                            tracing::warn!(project_id = %project.id, %error,
                                "environment re-check could not run; retaining readiness and rescheduling");
                            return Ok(false);
                        }
                    };
                    let passed = results.iter().all(|result| result.passed);
                    let saved = match environment::save_results(&db, row, &environment, results).await {
                        Ok(saved) => saved,
                        Err(ServiceError::Db(db::DbError::VersionConflict)) => return Ok(false),
                        Err(error) => return Err(error),
                    };
                    if passed {
                        return environment::clear_matching_pause(&db,&result_events,&project,&saved).await;
                    }
                    if project.system_pause_reason.as_deref() == Some(ENVIRONMENT_NOT_READY) {
                        if let Some(mut detail) = pause_detail(&project)? {
                            let original: serde_json::Value = serde_json::from_str(
                                project.environment_pause_json.as_deref().unwrap_or("{}"))
                                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                            let machine: EnvironmentMachine = serde_json::from_value(original["machine"].clone())
                                .unwrap_or(EnvironmentMachine::Server);
                            if machine == saved.machine {
                                detail.checks = saved.failing_checks.iter().map(|failure| failure.name.clone()).collect();
                                detail.output = bounded_output_tail(&saved.failing_checks.iter()
                                    .map(|failure| format!("{}: {}", failure.name, failure.output_tail))
                                    .collect::<Vec<_>>().join("\n"));
                                detail.last_checked_at = saved.checked_at.expect("completed check");
                                detail.next_check_at = saved.next_check_at.unwrap_or(next);
                                service.update_environment_pause(&project, &detail).await?;
                            }
                        }
                    }
                    Ok::<_, ServiceError>(false)
                }.await;
            drop(manual_guard);
            drop(guard);
            kick.notify_one();
            result
        });
        self.environment_rechecks
            .lock()
            .expect("environment jobs lock")
            .insert(key, job);
        Ok(None)
    }
    /// Settings events invalidate rows transactionally; only host rows/locations
    /// can start a probe in this build step, independently of queued Tasks.
    pub(super) fn observe_environment_settings(&self) {
        self.environment_settings_observer.get_or_init(|| {
            let mut events = self.event_bus.subscribe();
            let event_bus = self.event_bus.clone();
            let service = std::sync::Arc::downgrade(&self.task_service);
            let db = std::sync::Arc::downgrade(&self.db);
            tokio::spawn(async move {
                loop {
                    let event = match events.recv().await {
                        Ok(event) => event,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(false),
                    };
                    if event.event_type != "project.updated" && event.event_type != "project.resumed" {
                        continue;
                    }
                    let (Some(service), Some(db)) = (service.upgrade(), db.upgrade()) else {
                        return Ok(false);
                    };
                    if let Some(project) = ProjectRepo::get_by_id(&*db, &event.entity_id).await? {
                        if let Err(error) = environment::schedule_project_probes(
                            &db, &project, service.dispatch_notify(), event_bus.clone()).await {
                            tracing::warn!(project_id = %project.id, %error, "settings environment probe could not start");
                        }
                        service.dispatch_notify().notify_one();
                    }
                }
            })
        });
    }
}
