use api_types::ProjectSettings;
use chrono::{DateTime, Utc};
use db::Project;
use executors::environment::run_environment_checks;

use crate::{
    project_environment::{
        bounded_output_tail, next_check_at, pause_detail, ENVIRONMENT_NOT_READY,
        NO_RERUNNABLE_CHECK,
    },
    Result, ServiceError,
};

use super::TaskDispatcher;

impl TaskDispatcher {
    /// Start a due re-check without waiting for host commands. Observe finished
    /// jobs on a later tick; the job writes only against its original pause CAS.
    pub(super) async fn sync_environment_pause(&self, project: &Project) -> Result<bool> {
        let finished = {
            let mut jobs = self
                .environment_rechecks
                .lock()
                .expect("environment jobs lock");
            match jobs.get(&project.id) {
                Some(job) if !job.is_finished() => return Ok(false),
                Some(_) => jobs.remove(&project.id),
                None => None,
            }
        };
        if let Some(job) = finished {
            return job.await.map_err(|error| {
                ServiceError::invalid_operation(format!("environment re-check job failed: {error}"))
            })?;
        }
        if project.paused_at.is_none()
            || project.system_pause_reason.as_deref() != Some(ENVIRONMENT_NOT_READY)
        {
            return Ok(false);
        }
        let Some(mut detail) = pause_detail(project)? else {
            return Ok(false);
        };
        let due = DateTime::parse_from_rfc3339(&detail.next_check_at).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid environment re-check time: {error}"))
        })?;
        if Utc::now() < due {
            return Ok(false);
        }
        let environment = serde_json::from_str::<ProjectSettings>(&project.settings)
            .map_err(|error| {
                ServiceError::invalid_operation(format!("invalid project settings: {error}"))
            })?
            .environment;
        let checks = environment
            .checks
            .iter()
            // A pause that recorded no failing check (an asset could not be
            // staged) has nothing to re-run. Checks that already passed must
            // not resume it, or the Project relaunches and pauses every interval.
            .filter(|check| detail.checks.contains(&check.name))
            .cloned()
            .map(|mut check| {
                check.roles.clear();
                check
            })
            .collect::<Vec<_>>();
        if checks.is_empty() {
            if !detail.output.contains(NO_RERUNNABLE_CHECK) {
                detail.output =
                    bounded_output_tail(&format!("{}\n{NO_RERUNNABLE_CHECK}", detail.output));
                self.task_service
                    .update_environment_pause(project, &detail)
                    .await?;
            }
            return Ok(false);
        }
        let mut jobs = self
            .environment_rechecks
            .lock()
            .expect("environment jobs lock");
        if jobs.contains_key(&project.id) {
            return Ok(false);
        }
        let Some(guard) = self.task_service.claim_environment_recheck(&project.id) else {
            return Ok(false);
        };
        let service = std::sync::Arc::clone(&self.task_service);
        let project = project.clone();
        let project_id = project.id.clone();
        let job = tokio::spawn(async move {
            let _guard = guard;
            let checkout = service.environment_check_checkout(&project).await?;
            let mut failures = Vec::new();
            for check in checks {
                if let Some(failure) =
                    run_environment_checks(&checkout, &environment.env, &[check], "").await
                {
                    failures.push(failure);
                }
            }
            if failures.is_empty() {
                return service.clear_environment_pause(&project).await;
            }
            let now = Utc::now();
            detail.last_checked_at = now.to_rfc3339();
            detail.next_check_at = next_check_at(now, environment.recheck_interval_seconds);
            detail.output = bounded_output_tail(
                &failures
                    .iter()
                    .map(|failure| format!("{}: {}", failure.name, failure.output_tail))
                    .collect::<Vec<_>>()
                    .join("\n"),
            );
            service.update_environment_pause(&project, &detail).await?;
            Ok(false)
        });
        jobs.insert(project_id, job);
        Ok(false)
    }
}
