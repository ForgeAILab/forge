use api_types::ProjectSettings;
use chrono::{DateTime, Utc};
use db::Project;
use executors::environment::run_environment_checks;

use crate::{
    project_environment::{
        bounded_output_tail, next_check_at, pause_detail, ENVIRONMENT_NOT_READY,
    },
    Result, ServiceError,
};

use super::TaskDispatcher;

impl TaskDispatcher {
    /// Re-check only the recorded failures when due. Returns whether the
    /// pause changed, so this tick never dispatches from a stale snapshot.
    pub(super) async fn sync_environment_pause(&self, project: &Project) -> Result<bool> {
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
        let checkout = match self.task_service.environment_check_checkout(project).await {
            Ok(checkout) => checkout,
            Err(error) => {
                tracing::warn!(project_id = %project.id, %error, "could not resolve primary checkout for environment re-check");
                return Ok(false);
            }
        };
        let environment = serde_json::from_str::<ProjectSettings>(&project.settings)
            .map_err(|error| {
                ServiceError::invalid_operation(format!("invalid project settings: {error}"))
            })?
            .environment;
        let checks = environment
            .checks
            .iter()
            .filter(|check| detail.checks.contains(&check.name))
            .cloned()
            .map(|mut check| {
                check.roles.clear();
                check
            })
            .collect::<Vec<_>>();
        let mut failures = Vec::new();
        for check in checks {
            if let Some(failure) =
                run_environment_checks(&checkout, &environment.env, &[check], "").await
            {
                failures.push(failure);
            }
        }
        if failures.is_empty() {
            return self.task_service.clear_environment_pause(project).await;
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
        self.task_service
            .update_environment_pause(project, &detail)
            .await?;
        Ok(false)
    }
}
