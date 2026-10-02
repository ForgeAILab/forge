//! Applying the Project environment immediately before an execution launches.

use super::*;
use crate::project_environment::{
    bounded_output_tail, next_check_at, pause_detail, ENVIRONMENT_NOT_READY,
    ENVIRONMENT_PRE_DISPATCH_ERROR_PREFIX, NO_RERUNNABLE_CHECK,
};
use crate::workspace_backend::{
    ResolvedWorkspace, RunSpec, WorkspaceBackendError, WorkspaceRunPurpose,
};
use executors::environment::{
    mark_task_environment, EnvironmentCheckFailure, EnvironmentCheckResult,
};

impl TaskService {
    pub(crate) async fn project_environment(
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
        workspace: &ResolvedWorkspace,
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
        let assets = if environment.assets.is_empty() {
            Ok(())
        } else {
            workspace.materialize_assets(&environment).await
        };
        let (message, checks, output) = match assets {
            Err(WorkspaceBackendError::Other(error)) => match *error {
                ServiceError::InvalidOperation { message } => (
                    message.clone(),
                    Vec::new(),
                    bounded_output_tail(&format!("{message}\n{NO_RERUNNABLE_CHECK}")),
                ),
                error => return Err(error),
            },
            Err(error) => return Err(error.into()),
            Ok(()) => {
                match execution_environment_check(workspace, &environment, &execution.role).await {
                    Ok(Some(failure)) => (
                        check_failure_message(&failure),
                        vec![failure.name.clone()],
                        bounded_output_tail(&failure.output_tail),
                    ),
                    Ok(None) => return Ok(None),
                    Err(error @ WorkspaceBackendError::PurposeDenied { .. }) => (
                        error.to_string(),
                        Vec::new(),
                        bounded_output_tail(&format!("{error}\n{NO_RERUNNABLE_CHECK}")),
                    ),
                    Err(error) => return Err(error.into()),
                }
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
            workspace_id: Some(workspace.placement.workspace_id.clone()),
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
        let (results, policy_denied) = self
            .run_project_environment_checks(&project, &environment.checks)
            .await?;
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
                if policy_denied {
                    detail.checks.clear();
                    detail.output =
                        bounded_output_tail(&format!("{}\n{NO_RERUNNABLE_CHECK}", detail.output));
                }
                self.update_environment_pause(&project, &detail).await?;
            }
        }
        let current = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        Ok((results, current))
    }

    /// Re-check on the recorded daemon workspace while it is usable; otherwise
    /// use the primary checkout on the embedded backend command path.
    pub(crate) async fn run_project_environment_checks(
        &self,
        project: &db::Project,
        checks: &[api_types::EnvironmentCheck],
    ) -> Result<(Vec<api_types::ProjectEnvironmentCheckResult>, bool)> {
        let environment = serde_json::from_str::<ProjectSettings>(&project.settings)
            .map_err(|error| {
                ServiceError::invalid_operation(format!("invalid project settings: {error}"))
            })?
            .environment;
        let mut workspace =
            if let Some(id) = pause_detail(project)?.and_then(|detail| detail.workspace_id) {
                match db::WorkspaceRepo::get_by_id(&*self.db, &id).await? {
                    Some(workspace) => {
                        self.resolve_task_workspace(&workspace)
                            .await
                            .ok()
                            .filter(|resolved| {
                                resolved.placement.owner_kind == db::PlacementOwnerKind::Daemon
                                    && resolved.placement.state == db::PlacementState::Ready
                                    && resolved.handle().is_ok()
                            })
                    }
                    None => None,
                }
            } else {
                None
            };
        let mut checkout = if workspace.is_none() {
            Some(self.environment_check_checkout(project).await?)
        } else {
            None
        };
        let mut results = Vec::new();
        let mut policy_denied = false;
        for check in checks {
            let spec = environment_run_spec(check, &environment.env);
            let outcome = if let Some(workspace) = &workspace {
                workspace.backend.run(&workspace.placement, &spec).await
            } else {
                self.workspace_backend_router
                    .run_environment_checkout(checkout.as_deref().expect("checkout target"), &spec)
                    .await
            };
            let outcome = match outcome {
                Err(WorkspaceBackendError::OwnerUnreachable { .. }) if workspace.is_some() => {
                    workspace = None;
                    checkout = Some(self.environment_check_checkout(project).await?);
                    self.workspace_backend_router
                        .run_environment_checkout(
                            checkout.as_deref().expect("checkout target"),
                            &spec,
                        )
                        .await
                }
                outcome => outcome,
            };
            let result = match outcome {
                Err(error @ WorkspaceBackendError::PurposeDenied { .. }) => {
                    policy_denied = true;
                    EnvironmentCheckResult {
                        passed: false,
                        exit_code: None,
                        timed_out: false,
                        output_tail: error.to_string(),
                    }
                }
                outcome => environment_result(check, &environment.env, outcome)?,
            };
            results.push(api_types::ProjectEnvironmentCheckResult {
                name: check.name.clone(),
                passed: result.passed,
                exit_code: result.exit_code,
                output_tail: bounded_output_tail(&result.output_tail),
            });
        }
        Ok((results, policy_denied))
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

async fn execution_environment_check(
    workspace: &ResolvedWorkspace,
    environment: &api_types::ProjectEnvironment,
    role: &str,
) -> crate::workspace_backend::Result<Option<EnvironmentCheckFailure>> {
    for check in environment
        .checks
        .iter()
        .filter(|check| check.applies_to(role))
    {
        let result = environment_result(
            check,
            &environment.env,
            workspace
                .backend
                .run(
                    &workspace.placement,
                    &environment_run_spec(check, &environment.env),
                )
                .await,
        )?;
        if !result.passed {
            return Ok(Some(EnvironmentCheckFailure {
                name: check.name.clone(),
                command: check.command.clone(),
                exit_code: result.exit_code,
                timed_out: result.timed_out,
                output_tail: result.output_tail,
            }));
        }
    }
    Ok(None)
}

fn environment_run_spec(
    check: &api_types::EnvironmentCheck,
    env: &std::collections::BTreeMap<String, String>,
) -> RunSpec {
    RunSpec {
        purpose: WorkspaceRunPurpose::EnvironmentSetup,
        command: check.command.clone(),
        env: env.clone(),
        timeout_secs: check.timeout_seconds.clamp(1, 300),
        max_output_bytes: isize::MAX as usize,
    }
}

fn environment_result(
    _check: &api_types::EnvironmentCheck,
    env: &std::collections::BTreeMap<String, String>,
    outcome: crate::workspace_backend::Result<crate::workspace_backend::RunResult>,
) -> crate::workspace_backend::Result<EnvironmentCheckResult> {
    let (passed, exit_code, timed_out, output) = match outcome {
        Ok(result) => (
            result.exit_code == 0,
            (result.exit_code >= 0).then_some(result.exit_code),
            false,
            format!("{}{}", result.stdout_tail, result.stderr_tail),
        ),
        Err(WorkspaceBackendError::Other(error)) => match *error {
            ServiceError::InvalidOperation { message } => {
                let timed_out = message == "review command timed out";
                (
                    false,
                    None,
                    timed_out,
                    if timed_out { String::new() } else { message },
                )
            }
            error => return Err(error.into()),
        },
        Err(error) => return Err(error),
    };
    let output = executors::environment::redact_environment_values(&output, env);
    Ok(EnvironmentCheckResult {
        passed,
        exit_code,
        timed_out,
        output_tail: environment_output_tail(&output),
    })
}

fn environment_output_tail(output: &str) -> String {
    const LIMIT: usize = 4096;
    let mut start = output.len().saturating_sub(LIMIT);
    while !output.is_char_boundary(start) {
        start += 1;
    }
    output[start..].to_owned()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_backend::DaemonWorkspaceBackend;

    async fn fixture(
        deny: bool,
    ) -> (
        Arc<db::SqliteDb>,
        TaskService,
        Task,
        Execution,
        ResolvedWorkspace,
        tokio::task::JoinHandle<()>,
    ) {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = Arc::new(db::SqliteDb::new(pool));
        let (task, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
            .bind(json!({"environment":{"env":{"SECRET":"redact-me"},"checks":[{"name":"owner-probe","command":"owner-check", "timeout_seconds":1}]}}).to_string())
            .bind(&task.project_id).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE workspace_placement SET state = 'ready' WHERE id = ?")
            .bind(&placement.id)
            .execute(db.pool())
            .await
            .unwrap();
        let registry =
            Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
        let daemon_id = placement.daemon_id.unwrap();
        let (connection_id, mut outbound) =
            crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
        let responder_registry = registry.clone();
        let responder = tokio::spawn(async move {
            let mut runs = 0;
            while let Some(api_types::DaemonFrame::Request { id, method, params }) =
                outbound.recv().await
            {
                let frame = match method.as_str() {
                    api_types::METHOD_WORKSPACE_DESCRIBE => api_types::DaemonFrame::Response {
                        id,
                        result: json!({"workspace_handle":"opaque-owner-handle","generation":1,"exists":true,"head_sha":"head", "dirty":false,"branch":"task/remote", "locked":false,"active_execution_ids":[],"journaled_execution_ids":[]}),
                    },
                    api_types::METHOD_WORKSPACE_RUN if deny => api_types::DaemonFrame::Error {
                        id: Some(id),
                        error: api_types::DaemonErrorPayload {
                            code: api_types::PURPOSE_DENIED.into(),
                            message: "environment_setup disabled by owner".into(),
                            details: None,
                        },
                    },
                    api_types::METHOD_WORKSPACE_RUN => {
                        assert_eq!(params["purpose"], "environment_setup");
                        runs += 1;
                        api_types::DaemonFrame::Response {
                            id,
                            result: json!({"entry_id":format!("run-{runs}"), "operation_id":params["operation_id"], "exit_code": if runs == 1 {1} else {0}, "stdout":"redact-me owner output", "stderr":"", "duration_ms":1, "timed_out":false, "stdout_truncated":false, "stderr_truncated":false}),
                        }
                    }
                    api_types::METHOD_JOURNAL_ACK => api_types::DaemonFrame::Response {
                        id,
                        result: json!({"entry_id":params["entry_id"],"acknowledged":true}),
                    },
                    _ => panic!("unexpected owner command {method}"),
                };
                responder_registry.dispatch_incoming_for_connection(
                    &daemon_id,
                    connection_id,
                    frame,
                );
            }
        });
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()));
        let router = (*service.workspace_backend_router())
            .clone()
            .with_daemon(Arc::new(DaemonWorkspaceBackend::new(db.clone(), registry)));
        let service = service.with_workspace_backend_router(Arc::new(router));
        let workspace = WorkspaceRepo::get_by_id(&*db, execution.workspace_id.as_deref().unwrap())
            .await
            .unwrap()
            .unwrap();
        let resolved = service.resolve_task_workspace(&workspace).await.unwrap();
        (db, service, task, execution, resolved, responder)
    }

    #[tokio::test]
    async fn daemon_environment_pause_rechecks_the_owner_and_redacts_output() {
        let (db, service, task, execution, workspace, responder) = fixture(false).await;
        assert!(service
            .prepare_execution_environment(&task, &execution, &workspace, &mut json!({}))
            .await
            .unwrap()
            .is_some());
        let project = ProjectRepo::get_by_id(&*db, &task.project_id)
            .await
            .unwrap()
            .unwrap();
        let detail = pause_detail(&project).unwrap().unwrap();
        assert_eq!(
            detail.workspace_id.as_deref(),
            Some(workspace.placement.workspace_id.as_str())
        );
        assert!(detail.output.contains("owner output"));
        assert!(!detail.output.contains("redact-me"));
        let (results, project) = service
            .recheck_project_environment(&project.id)
            .await
            .unwrap();
        assert!(results[0].passed);
        assert!(project.paused_at.is_none());
        responder.abort();
    }

    #[tokio::test]
    async fn daemon_environment_policy_denial_pauses_without_scheduled_rechecks() {
        let (db, service, task, execution, workspace, responder) = fixture(true).await;
        assert!(service
            .prepare_execution_environment(&task, &execution, &workspace, &mut json!({}))
            .await
            .unwrap()
            .is_some());
        let project = ProjectRepo::get_by_id(&*db, &task.project_id)
            .await
            .unwrap()
            .unwrap();
        let detail = pause_detail(&project).unwrap().unwrap();
        assert!(detail.checks.is_empty());
        assert!(detail.output.contains("purpose_denied"));
        assert!(detail.output.contains(NO_RERUNNABLE_CHECK));
        let (results, project) = service
            .recheck_project_environment(&project.id)
            .await
            .unwrap();
        assert!(!results[0].passed);
        assert!(pause_detail(&project).unwrap().unwrap().checks.is_empty());
        assert!(project.paused_at.is_some());
        responder.abort();
    }

    async fn pause_with_primary_checkout(
        db: &db::SqliteDb,
        service: &TaskService,
        task: &Task,
        execution: &Execution,
        workspace: &ResolvedWorkspace,
    ) -> tempfile::TempDir {
        assert!(service
            .prepare_execution_environment(task, execution, workspace, &mut json!({}))
            .await
            .unwrap()
            .is_some());
        let primary = tempfile::TempDir::new().unwrap();
        git::init(primary.path()).await.unwrap();
        sqlx::query("UPDATE repo SET local_path = ? WHERE id = (SELECT repo_id FROM workspace WHERE id = ?)")
            .bind(primary.path().to_str().unwrap())
            .bind(&workspace.placement.workspace_id)
            .execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE project SET primary_repo_id = (SELECT repo_id FROM workspace WHERE id = ?), settings = ? WHERE id = ?")
            .bind(&workspace.placement.workspace_id)
            .bind(json!({"environment":{"env":{"SECRET":"redact-me"},"checks":[{"name":"owner-probe","command":"test \"$SECRET\" = redact-me && pwd"}]}}).to_string())
            .bind(&task.project_id).execute(db.pool()).await.unwrap();
        primary
    }

    async fn assert_primary_checkout_recheck(
        service: &TaskService,
        task: &Task,
        primary: &std::path::Path,
    ) {
        let (results, project) = service
            .recheck_project_environment(&task.project_id)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].passed);
        assert_eq!(
            results[0].output_tail,
            primary.canonicalize().unwrap().to_str().unwrap()
        );
        assert!(project.paused_at.is_none());
        assert!(project.environment_pause_json.is_none());
    }

    #[tokio::test]
    async fn environment_recheck_deleted_workspace_uses_primary_checkout() {
        let (db, service, task, execution, workspace, responder) = fixture(false).await;
        let primary =
            pause_with_primary_checkout(&db, &service, &task, &execution, &workspace).await;
        sqlx::query("DELETE FROM workspace WHERE id = ?")
            .bind(&workspace.placement.workspace_id)
            .execute(db.pool())
            .await
            .unwrap();
        assert_primary_checkout_recheck(&service, &task, primary.path()).await;
        responder.abort();
    }

    #[tokio::test]
    async fn environment_recheck_disconnected_owner_uses_primary_checkout() {
        let (db, service, task, execution, workspace, responder) = fixture(false).await;
        let primary =
            pause_with_primary_checkout(&db, &service, &task, &execution, &workspace).await;
        // The placement can still say ready while its transport is offline.
        let offline =
            Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
        let router = (*service.workspace_backend_router())
            .clone()
            .with_daemon(Arc::new(DaemonWorkspaceBackend::new(db.clone(), offline)));
        let service = service.with_workspace_backend_router(Arc::new(router));
        assert_primary_checkout_recheck(&service, &task, primary.path()).await;
        responder.abort();
    }

    #[tokio::test]
    async fn environment_recheck_unready_or_unresolved_workspace_uses_primary_checkout() {
        for state in [
            "reserved",
            "preparing",
            "disconnected",
            "cleaning",
            "cleaned",
            "failed",
            "unresolved",
        ] {
            let (db, service, task, execution, workspace, responder) = fixture(false).await;
            let primary =
                pause_with_primary_checkout(&db, &service, &task, &execution, &workspace).await;
            let service = if state == "unresolved" {
                // Keep the daemon placement, but remove its resolving backend.
                let router = crate::lifecycle::context::embedded_workspace_router_for_test(
                    db.clone(),
                    primary.path().to_path_buf(),
                    None,
                );
                service.with_workspace_backend_router(router)
            } else {
                sqlx::query("UPDATE workspace_placement SET state = ? WHERE id = ?")
                    .bind(state)
                    .bind(&workspace.placement.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                service
            };
            assert_primary_checkout_recheck(&service, &task, primary.path()).await;
            responder.abort();
        }
    }
}
