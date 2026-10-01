//! Applying the Project environment immediately before an execution launches.

use super::*;
use crate::project_environment::{
    bounded_output_tail, next_check_at, pause_detail, ENVIRONMENT_NOT_READY,
    ENVIRONMENT_PRE_DISPATCH_ERROR_PREFIX, NO_RERUNNABLE_CHECK,
};
use executors::environment::{
    mark_task_environment, materialize_assets, run_environment_check, run_environment_checks,
    EnvironmentCheckFailure,
};

impl TaskService {
    pub(super) async fn project_environment(
        &self,
        project_id: &str,
    ) -> Result<api_types::ProjectEnvironment> {
        let project = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        let settings =
            serde_json::from_str::<ProjectSettings>(&project.settings).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid project settings: {error}"))
            })?;
        Ok(settings.environment)
    }

    /// Stamp the environment, copy assets and run the checks gating this role.
    /// A failed preflight terminalizes the execution before any provider call
    /// and pauses the Project, leaving the Task in its current workflow state.
    pub(super) async fn prepare_execution_environment(
        &self,
        task: &Task,
        execution: &Execution,
        worktree_path: &str,
        agent_config: &mut Value,
    ) -> Result<Option<Execution>> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let environment = serde_json::from_str::<ProjectSettings>(&project.settings)
            .map_err(|error| {
                ServiceError::invalid_operation(format!("invalid project settings: {error}"))
            })?
            .environment;
        mark_task_environment(agent_config, &environment.env);
        if environment.assets.is_empty() && environment.checks.is_empty() {
            return Ok(None);
        }
        let worktree = std::path::Path::new(worktree_path);
        let (message, checks, output) =
            match materialize_assets(worktree, &environment.assets).await {
                Err(message) => (
                    message.clone(),
                    Vec::new(),
                    bounded_output_tail(&format!("{message}\n{NO_RERUNNABLE_CHECK}")),
                ),
                Ok(()) => {
                    let Some(failure) = run_environment_checks(
                        worktree,
                        &environment.env,
                        &environment.checks,
                        &execution.role,
                    )
                    .await
                    else {
                        return Ok(None);
                    };
                    (
                        check_failure_message(&failure),
                        vec![failure.name.clone()],
                        bounded_output_tail(&failure.output_tail),
                    )
                }
            };
        let message = format!("{ENVIRONMENT_PRE_DISPATCH_ERROR_PREFIX}{message}");
        tracing::info!(execution_id = %execution.id, task_id = %task.id, %message,
            "execution failed before launch by a Project environment check");
        let failed = self
            .fail_execution_before_dispatch_without_task_block(&execution.id, message)
            .await?;
        let now = chrono::Utc::now();
        let paused_at = now.to_rfc3339();
        let detail = api_types::ProjectEnvironmentPause {
            checks,
            role: Some(execution.role.clone()),
            output,
            paused_at: paused_at.clone(),
            last_checked_at: paused_at.clone(),
            next_check_at: next_check_at(now, environment.recheck_interval_seconds),
        };
        let detail_json = serde_json::to_string(&detail).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize environment pause: {error}"
            ))
        })?;
        if ProjectRepo::set_environment_pause_if_unchanged(
            &*self.db,
            &project.id,
            project.version,
            &paused_at,
            &detail_json,
        )
        .await?
        {
            self.publish(ForgeEvent {
                event_type: "project.paused".to_owned(),
                entity_id: project.id,
                timestamp: event_timestamp(),
                context: EventContext::ProjectPaused { paused_at },
            });
        }
        Ok(Some(failed))
    }

    /// Resolve the primary checkout for host checks without copying assets or
    /// preparing a Task worktree. A missing checkout is an error, not a pass.
    pub(crate) async fn environment_check_checkout(
        &self,
        project: &db::Project,
    ) -> Result<std::path::PathBuf> {
        let repo_id = project
            .primary_repo_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Project has no primary repository"))?;
        let repo = db::RepoRepo::get_by_id(&*self.db, repo_id)
            .await?
            .filter(|repo| repo.project_id == project.id)
            .ok_or_else(|| {
                ServiceError::invalid_operation("Project primary repository is invalid")
            })?;
        let path = repo
            .local_path
            .as_deref()
            .map(str::trim)
            .filter(|path| !path.is_empty() && std::path::Path::new(path).is_dir())
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| self.workspace_root.join(".repos").join(repo_id));
        if !path.is_dir() || !git::is_git_repo(&path).await {
            return Err(ServiceError::invalid_operation(
                "Project primary checkout is unavailable",
            ));
        }
        Ok(path)
    }

    /// Run every configured check now, regardless of role, and resume only an
    /// unchanged environment-owned pause when all checks pass.
    pub async fn recheck_project_environment(
        &self,
        project_id: &str,
    ) -> Result<(Vec<api_types::ProjectEnvironmentCheckResult>, db::Project)> {
        let _check = self.claim_environment_recheck(project_id).ok_or_else(|| {
            ServiceError::Conflict("Project environment re-check is already running".to_owned())
        })?;
        let project = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        let environment = serde_json::from_str::<ProjectSettings>(&project.settings)
            .map_err(|error| {
                ServiceError::invalid_operation(format!("invalid project settings: {error}"))
            })?
            .environment;
        let checkout = self.environment_check_checkout(&project).await?;
        let mut results = Vec::new();
        for check in &environment.checks {
            let result = run_environment_check(&checkout, &environment.env, check).await;
            results.push(api_types::ProjectEnvironmentCheckResult {
                name: check.name.clone(),
                passed: result.passed,
                exit_code: result.exit_code,
                output_tail: bounded_output_tail(&result.output_tail),
            });
        }
        if project.system_pause_reason.as_deref() == Some(ENVIRONMENT_NOT_READY) {
            if !results.is_empty() && results.iter().all(|result| result.passed) {
                self.clear_environment_pause(&project).await?;
            } else if let Some(mut detail) = pause_detail(&project)? {
                let now = chrono::Utc::now();
                detail.last_checked_at = now.to_rfc3339();
                detail.next_check_at = next_check_at(now, environment.recheck_interval_seconds);
                detail.checks = results
                    .iter()
                    .filter(|result| !result.passed)
                    .map(|result| result.name.clone())
                    .collect();
                detail.output = if results.is_empty() {
                    bounded_output_tail(&format!("{}\n{NO_RERUNNABLE_CHECK}", detail.output))
                } else {
                    bounded_output_tail(
                        &results
                            .iter()
                            .filter(|result| !result.passed)
                            .map(|result| format!("{}: {}", result.name, result.output_tail))
                            .collect::<Vec<_>>()
                            .join("\n"),
                    )
                };
                self.update_environment_pause(&project, &detail).await?;
            }
        }
        let current = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        Ok((results, current))
    }

    pub(crate) fn claim_environment_recheck(
        &self,
        project_id: &str,
    ) -> Option<EnvironmentRecheckGuard> {
        let mut running = self
            .environment_rechecks
            .lock()
            .expect("environment re-check lock");
        if !running.insert(project_id.to_owned()) {
            return None;
        }
        Some(EnvironmentRecheckGuard {
            running: Arc::clone(&self.environment_rechecks),
            project_id: project_id.to_owned(),
        })
    }

    pub(crate) async fn clear_environment_pause(&self, project: &db::Project) -> Result<bool> {
        if project.system_pause_reason.as_deref() != Some(ENVIRONMENT_NOT_READY) {
            return Ok(false);
        }
        let (Some(repo_id), Some(paused_at)) = (
            project.primary_repo_id.as_deref(),
            project.paused_at.as_deref(),
        ) else {
            return Ok(false);
        };
        let cleared = ProjectRepo::clear_system_pause_if_unchanged(
            &*self.db,
            &project.id,
            project.version,
            repo_id,
            paused_at,
            ENVIRONMENT_NOT_READY,
        )
        .await?;
        if cleared {
            tracing::info!(project_id = %project.id, "resumed Project automatically: environment checks passed");
            self.publish(ForgeEvent {
                event_type: "project.resumed".to_owned(),
                entity_id: project.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::ProjectResumed {},
            });
        }
        Ok(cleared)
    }

    pub(crate) async fn update_environment_pause(
        &self,
        project: &db::Project,
        detail: &api_types::ProjectEnvironmentPause,
    ) -> Result<()> {
        let Some(paused_at) = project.paused_at.as_deref() else {
            return Ok(());
        };
        let json = serde_json::to_string(detail).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize environment pause: {error}"
            ))
        })?;
        if ProjectRepo::update_environment_pause_if_unchanged(
            &*self.db,
            &project.id,
            project.version,
            paused_at,
            &json,
        )
        .await?
        {
            self.publish(ForgeEvent {
                event_type: "project.updated".to_owned(),
                entity_id: project.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::ProjectUpdated {},
            });
        }
        Ok(())
    }
}

fn check_failure_message(failure: &EnvironmentCheckFailure) -> String {
    let output = bounded_output_tail(&failure.output_tail);
    if output.is_empty() {
        failure.message()
    } else {
        format!("{}\n{output}", failure.message())
    }
}

/// Releases single-flight ownership on success, error, cancellation or panic.
pub(crate) struct EnvironmentRecheckGuard {
    running: Arc<std::sync::Mutex<HashSet<String>>>,
    project_id: String,
}

impl Drop for EnvironmentRecheckGuard {
    fn drop(&mut self) {
        self.running
            .lock()
            .expect("environment re-check lock")
            .remove(&self.project_id);
    }
}
