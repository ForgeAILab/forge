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
    /// and records the owner failure. Task-aware admission decides whether to
    /// pause the Project or wait on that owner without changing workflow state.
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
        let machine = db::EnvironmentMachine::from_placement(&workspace.placement);
        crate::placement::environment::record_launch_failure(
            &self.db,
            &project,
            &environment,
            &workspace.placement,
            crate::placement::environment::LaunchEnvironmentFailure {
                checks: &checks,
                output: &output,
                role: &execution.role,
            },
        )
        .await?;
        let agent = AgentRepo::get_by_id(
            &*self.db,
            execution.agent_id.as_deref().ok_or(DbError::NotFound)?,
        )
        .await?
        .ok_or(DbError::NotFound)?;
        let prepared = crate::placement::context::prepare_selection(
            &self.db,
            task,
            Some(&agent),
            &execution.role,
            self.placement_adapter_registry.as_deref(),
        )
        .await?;
        if environment.checks.is_empty() {
            // Asset-only Projects retain the base's global failure signal.
            let now = chrono::Utc::now();
            let detail = serde_json::json!({"machine":machine,"workspace_id":workspace.placement.workspace_id,"checks":checks,"role":execution.role,"output":output,"paused_at":now.to_rfc3339(),"last_checked_at":now.to_rfc3339(),"next_check_at":next_check_at(now,environment.recheck_interval_seconds)});
            if ProjectRepo::set_environment_pause_if_unchanged(
                &*self.db,
                &project.id,
                project.version,
                &now.to_rfc3339(),
                &detail.to_string(),
            )
            .await?
            {
                self.publish(ForgeEvent {
                    event_type: "project.paused".into(),
                    entity_id: project.id,
                    timestamp: event_timestamp(),
                    context: EventContext::ProjectPaused {
                        paused_at: now.to_rfc3339(),
                    },
                });
            }
        } else {
            let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
                .await?
                .ok_or(DbError::NotFound)?;
            let context = self
                .environment_selection_context(&current, &prepared)
                .await?;
            if let Err(refusal) = crate::placement::select_placement(&context).into_result() {
                crate::placement::environment::handle_refusal(
                    &self.db,
                    &self.event_bus,
                    &current,
                    &prepared.project,
                    &context,
                    &refusal,
                    self.environment_daemon_connections()
                        .map(|registry| &**registry),
                )
                .await?;
            }
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
        use db::ProjectMachineReadinessRepo;
        let observed = self.db.list_readiness(project_id).await?;
        let (results, policy_denied, machine) = self
            .run_project_environment_checks(&project, &environment.checks)
            .await?;
        let current_result =
            if let Some(row) = observed.into_iter().find(|row| row.machine == machine) {
                match crate::placement::environment::save_results(
                    &self.db,
                    row,
                    &environment,
                    results.clone(),
                )
                .await
                {
                    Ok(_) => true,
                    Err(ServiceError::Db(DbError::VersionConflict)) => false,
                    Err(error) => return Err(error),
                }
            } else {
                true
            };
        if current_result && project.system_pause_reason.as_deref() == Some(ENVIRONMENT_NOT_READY) {
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
    ) -> Result<(
        Vec<api_types::ProjectEnvironmentCheckResult>,
        bool,
        db::EnvironmentMachine,
    )> {
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
        let machine = workspace
            .as_ref()
            .map(|workspace| db::EnvironmentMachine::from_placement(&workspace.placement))
            .unwrap_or(db::EnvironmentMachine::Server);
        Ok((results, policy_denied, machine))
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
        let mut json = serde_json::to_value(detail).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize environment pause: {error}"
            ))
        })?;
        if let Some(original) = project.environment_pause_json.as_deref() {
            let original: Value = serde_json::from_str(original)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
            if let Some(machine) = original.get("machine") {
                json["machine"] = machine.clone();
            }
        }
        let json = json.to_string();
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
        // These launch fixtures must advertise the actual executor and policy
        // facts a real admitted daemon supplies; no readiness row is fabricated.
        registry.dispatch_incoming_for_connection(&daemon_id,connection_id,api_types::DaemonFrame::Notification {
            method:api_types::METHOD_DAEMON_HANDSHAKE.into(),
            params:json!({"protocol_revision":3,"capabilities":["workspace.v1",api_types::DAEMON_CAPABILITY_JOURNAL_ACK,api_types::DAEMON_CAPABILITY_USAGE_REPORTS,api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT],"executor_capabilities":{"shell":{"structured_events":true,"usage":true,"resume":true,"cancel_ack":true,"terminal_observed":true}},"workspace_run_policy":{"allowed_purposes":["environment_setup","ci_step"]}})});
        sqlx::query("UPDATE daemon SET detected_clis_json=? WHERE id=?")
            .bind(r#"[{"kind":"shell","availability":"authenticated"}]"#)
            .bind(&daemon_id)
            .execute(db.pool())
            .await
            .unwrap();
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
                    api_types::METHOD_WORKSPACE_PREPARE => api_types::DaemonFrame::Response {
                        id,
                        result: json!({"entry_id":"prepared-fixture","operation_id":params["operation_id"],"workspace_handle":"opaque-owner-handle","workspace_path":"/owner-only/task","base_sha":"base-head","branch":"task/remote","generation":1}),
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
                        let cargo_probe = params["command"] == "cargo --version";
                        api_types::DaemonFrame::Response {
                            id,
                            result: json!({"entry_id":format!("run-{runs}"), "operation_id":params["operation_id"], "exit_code": if runs == 1 { if cargo_probe {127} else {1} } else {0}, "stdout":if cargo_probe {format!("{}cargo: command not found redact-me", "界".repeat(2000))} else {"redact-me owner output".into()}, "stderr":"", "duration_ms":1, "timed_out":false, "stdout_truncated":false, "stderr_truncated":false}),
                        }
                    }
                    api_types::METHOD_JOURNAL_ACK => api_types::DaemonFrame::Response {
                        id,
                        result: json!({"entry_id":params["entry_id"],"acknowledged":true}),
                    },
                    api_types::METHOD_EXECUTION_START => api_types::DaemonFrame::Response {
                        id,
                        result: json!({"execution_id":params["execution_id"],"accepted":true}),
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
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_daemon_connections(registry.clone());
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
    async fn environment_daemon_launch_records_role_scoped_cargo_failure() {
        use db::ProjectMachineReadinessRepo;
        let (db, service, task, mut execution, workspace, responder) = fixture(false).await;
        let environment: api_types::ProjectEnvironment = serde_json::from_value(json!({"env":{"SECRET":"redact-me"},"checks":[{"name":"cargo","command":"cargo --version","roles":["reviewer"],"timeout_seconds":1}]})).unwrap();
        sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
            .bind(json!({"environment":environment}).to_string())
            .bind(&task.project_id)
            .execute(db.pool())
            .await
            .unwrap();
        assert!(
            service
                .prepare_execution_environment(&task, &execution, &workspace, &mut json!({}))
                .await
                .unwrap()
                .is_none(),
            "reviewer check must not run for coder"
        );
        assert!(
            db.list_readiness(&task.project_id)
                .await
                .unwrap()
                .is_empty(),
            "no daemon probe or synthetic fact"
        );
        execution.role = "reviewer".into();
        assert!(service
            .prepare_execution_environment(&task, &execution, &workspace, &mut json!({}))
            .await
            .unwrap()
            .is_some());
        let row = db
            .get_readiness(
                &task.project_id,
                &db::EnvironmentMachine::from_placement(&workspace.placement),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.failing_checks[0].name, "cargo");
        assert_eq!(row.role.as_deref(), Some("reviewer"));
        assert!(row.failing_checks[0]
            .output_tail
            .contains("command not found"));
        assert!(!row.failing_checks[0].output_tail.contains("redact-me"));
        responder.abort();
    }

    #[tokio::test]
    async fn environment_launch_failures_are_per_machine_and_recheck_redispatches_the_owner() {
        use db::ProjectMachineReadinessRepo;
        let (db, original_service, task, execution, daemon_workspace, responder) =
            fixture(false).await;
        daemon_workspace
            .backend
            .prepare(
                &daemon_workspace.placement,
                &crate::workspace_backend::PrepareSpec {
                    base_ref: "main".into(),
                },
            )
            .await
            .unwrap();
        // Coder/planner dispatch currently uses server-only plan paths.
        // Exercise the daemon execution path without plan I/O, leaving those
        // separate prompt/outbox contracts to the owning refactor.
        let mut workflow = WorkflowEngine::resolve_workflow("{}");
        workflow
            .states
            .iter_mut()
            .find(|state| state.name == "in_progress")
            .unwrap()
            .role = Some("interactive".into());
        sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
            .bind(serde_json::to_string(&workflow).unwrap())
            .bind(&task.project_id)
            .execute(db.pool())
            .await
            .unwrap();
        TaskRoleAssignmentRepo::assign(
            &*db,
            db::CreateTaskRoleAssignment {
                id: db::new_uuid_v4(),
                task_id: task.id.clone(),
                role_name: "interactive".into(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: execution.agent_id.clone(),
                created_at: db::now_rfc3339(),
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .unwrap();
        sqlx::query("UPDATE execution SET role = 'interactive' WHERE id = ?")
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .unwrap();
        let execution = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        let task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let checkout = tempfile::TempDir::new().unwrap();
        let root = tempfile::TempDir::new().unwrap();
        git::init(checkout.path()).await.unwrap();
        git::commit_all(checkout.path(), "initial").await.unwrap();
        let workspace_row =
            WorkspaceRepo::get_by_id(&*db, &daemon_workspace.placement.workspace_id)
                .await
                .unwrap()
                .unwrap();
        sqlx::query("UPDATE repo SET local_path = ?, remote_url = NULL WHERE id = ?")
            .bind(checkout.path().to_str().unwrap())
            .bind(&workspace_row.repo_id)
            .execute(db.pool())
            .await
            .unwrap();
        let now = db::now_rfc3339();
        db::RepoLocationRepo::create(
            &*db,
            db::CreateRepoLocation {
                id: db::new_uuid_v4(),
                repo_id: workspace_row.repo_id,
                owner_kind: db::RepoLocationOwnerKind::Server,
                daemon_id: None,
                runtime_id: None,
                path: checkout.path().to_string_lossy().into_owned(),
                kind: db::RepoLocationKind::PrimaryCheckout,
                is_default: false,
                status: db::RepoLocationStatus::Ready,
                last_verified_at: Some(now.clone()),
                last_error: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        let project = ProjectRepo::get_by_id(&*db, &task.project_id)
            .await
            .unwrap()
            .unwrap();
        let environment = serde_json::from_str::<ProjectSettings>(&project.settings)
            .unwrap()
            .environment;
        for machine in [
            db::EnvironmentMachine::Server,
            db::EnvironmentMachine::from_placement(&daemon_workspace.placement),
        ] {
            let mut row =
                crate::placement::environment::unknown_record(&project.id, machine, &environment);
            row.status = db::EnvironmentReadinessStatus::Ready;
            db.put_readiness(row, None).await.unwrap();
        }
        let registry = original_service
            .daemon_connections
            .as_ref()
            .unwrap()
            .clone();
        let daemon_id = daemon_workspace.placement.daemon_id.as_deref().unwrap();
        let connection = registry.get(daemon_id).unwrap();
        registry.dispatch_incoming_for_connection(daemon_id, connection.id(), api_types::DaemonFrame::Notification { method:api_types::METHOD_DAEMON_HANDSHAKE.into(), params:json!({"protocol_revision":3,"capabilities":["workspace.v1",api_types::DAEMON_CAPABILITY_JOURNAL_ACK,api_types::DAEMON_CAPABILITY_USAGE_REPORTS,api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT],"executor_capabilities":{"shell":{"structured_events":true,"usage":true,"resume":true,"cancel_ack":true,"terminal_observed":true}},"workspace_run_policy":{"allowed_purposes":["environment_setup","ci_step"]}}) });
        sqlx::query("UPDATE daemon SET detected_clis_json = ? WHERE id = ?")
            .bind(r#"[{"kind":"shell","availability":"authenticated"}]"#)
            .bind(daemon_id)
            .execute(db.pool())
            .await
            .unwrap();
        let events = Arc::new(EventBus::new(64));
        let merge = Arc::new(crate::MergeService::new(
            db.clone(),
            events.clone(),
            root.path().to_owned(),
        ));
        let router = crate::workspace_backend::WorkspaceBackendRouter::new(Arc::new(
            crate::workspace_backend::EmbeddedWorkspaceBackend::new(
                db.clone(),
                merge,
                root.path().to_owned(),
            ),
        ))
        .with_daemon(daemon_workspace.backend.clone());
        let service = Arc::new(
            TaskService::new(db.clone(), events.clone())
                .with_workspace_root(root.path().to_owned())
                .with_workspace_backend_router(Arc::new(router))
                .with_daemon_connections(registry),
        );
        assert!(service
            .prepare_execution_environment(&task, &execution, &daemon_workspace, &mut json!({}))
            .await
            .unwrap()
            .is_some());
        let project = ProjectRepo::get_by_id(&*db, &task.project_id)
            .await
            .unwrap()
            .unwrap();
        assert!(project.paused_at.is_none());
        let machine = db::EnvironmentMachine::from_placement(&daemon_workspace.placement);
        let failed = db
            .get_readiness(&project.id, &machine)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, db::EnvironmentReadinessStatus::NotReady);
        assert_eq!(failed.failing_checks[0].name, "owner-probe");
        let waiting = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(waiting.status, task.status);
        assert!(waiting.error_annotation.is_none());
        let attention: String = sqlx::query_scalar(
            "SELECT summary FROM attention_projection WHERE dedupe_key = ? AND status = 'open'",
        )
        .bind(format!("task-environment-wait:{}", task.id))
        .fetch_one(db.pool())
        .await
        .unwrap();
        let hostname: String = sqlx::query_scalar("SELECT hostname FROM daemon WHERE id=?")
            .bind(daemon_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert!(attention.contains(&hostname));
        assert!(attention.contains("owner-probe"));

        let (kind, action): (String, String) = sqlx::query_as(
            "SELECT attention_type,recommended_action FROM attention_projection WHERE dedupe_key=?",
        )
        .bind(format!("task-environment-wait:{}", task.id))
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(kind, "environment_not_ready");
        assert_eq!(action, "wait");
        let execution_failed:i64=sqlx::query_scalar("SELECT count(*) FROM domain_event WHERE entity_id=? AND event_type='task.execution_failed'")
            .bind(&task.id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(
            execution_failed, 0,
            "waiting is not a Task execution failure"
        );
        let pinned = original_service
            .create_task(
                project.id.clone(),
                "Pinned wait",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let pinned_agent = AgentRepo::get_by_id(&*db, execution.agent_id.as_deref().unwrap())
            .await
            .unwrap()
            .unwrap();
        // No placement yet: an Agent pin uses exactly the same durable wait.
        assert!(service
            .defer_initial_environment_probe(&pinned, &pinned_agent, "interactive")
            .await
            .unwrap());
        let kind: String = sqlx::query_scalar(
            "SELECT attention_type FROM attention_projection WHERE dedupe_key=? AND status='open'",
        )
        .bind(format!("task-environment-wait:{}", pinned.id))
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(kind, "environment_not_ready");
        assert!(ProjectRepo::get_by_id(&*db, &project.id)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_none());
        let agent = db::AgentRepo::create(
            &*db,
            db::CreateAgent {
                id: db::new_uuid_v4(),
                name: "Unpinned shell".into(),
                description: None,
                executor_type: "shell".into(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                capabilities_json: "[]".into(),
                config_json: "{}".into(),
                credential_ref: None,
                daemon_id: None,
                max_concurrent_tasks: 2,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: db::AgentStatus::Idle,
                last_heartbeat_at: Some(now.clone()),
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".into(),
                prompt_template: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        let fresh = service
            .create_task(
                project.id.clone(),
                "Use the other machine",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let admitted = service
            .claim_task(fresh.id.clone(), crate::Assignee::Agent(agent.id), None)
            .await
            .unwrap();
        let server_workspace =
            WorkspaceRepo::get_by_id(&*db, admitted.execution.workspace_id.as_deref().unwrap())
                .await
                .unwrap()
                .unwrap();
        let server_workspace = service
            .resolve_task_workspace(&server_workspace)
            .await
            .unwrap();
        assert_eq!(
            server_workspace.placement.owner_kind,
            db::PlacementOwnerKind::Server
        );
        assert!(service
            .prepare_execution_environment(
                &admitted.task,
                &admitted.execution,
                &server_workspace,
                &mut json!({})
            )
            .await
            .unwrap()
            .is_some());
        let paused = ProjectRepo::get_by_id(&*db, &project.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            paused.system_pause_reason.as_deref(),
            Some(ENVIRONMENT_NOT_READY)
        );
        let detail: Value =
            serde_json::from_str(paused.environment_pause_json.as_deref().unwrap()).unwrap();
        assert_eq!(detail["machine"]["owner_kind"], "server");
        let mut due = db
            .get_readiness(&project.id, &machine)
            .await
            .unwrap()
            .unwrap();
        let version = due.version;
        due.next_check_at = Some("2026-01-01T00:00:00Z".into());
        db.put_readiness(due, Some(version)).await.unwrap();
        let dispatcher = crate::TaskDispatcher::new(db.clone(), events, service);
        dispatcher.check_once().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if ProjectRepo::get_by_id(&*db, &project.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .paused_at
                    .is_none()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            db.get_readiness(&project.id, &machine)
                .await
                .unwrap()
                .unwrap()
                .status,
            db::EnvironmentReadinessStatus::Ready
        );
        assert_eq!(
            db.get_readiness(&project.id, &db::EnvironmentMachine::Server)
                .await
                .unwrap()
                .unwrap()
                .status,
            db::EnvironmentReadinessStatus::Unknown,
            "clearing the pause resets other failed rows so resumed work can try again"
        );
        let status: String =
            sqlx::query_scalar("SELECT status FROM attention_projection WHERE dedupe_key = ?")
                .bind(format!("task-environment-wait:{}", task.id))
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(status, "resolved");
        dispatcher.check_once().await.unwrap(); // Observe the successful pause CAS.
        dispatcher.check_once().await.unwrap();
        let runs = ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap();
        let history = ExecutionRepo::list_by_task(
            &*db,
            &task.id,
            db::PageRequest {
                cursor: None,
                limit: 10,
                include_total: false,
                sort_by: db::SortBy::CreatedAt,
                sort_order: db::SortOrder::Desc,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            runs.len(),
            1,
            "the waiting daemon Task was redispatched without user action; history: {:?}; task metadata: {:?}",
            history.items.iter().map(|execution| (&execution.role, &execution.status, &execution.error)).collect::<Vec<_>>(),
            TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap()
                .metadata_json
        );
        assert_ne!(runs[0].id, execution.id);
        assert_eq!(
            runs[0].workspace_id.as_deref(),
            Some(daemon_workspace.placement.workspace_id.as_str())
        );
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

    #[tokio::test]
    async fn environment_unreachable_machine_keeps_its_readiness_record() {
        use db::ProjectMachineReadinessRepo;
        let (db, service, task, execution, workspace, responder) = fixture(false).await;
        service
            .prepare_execution_environment(&task, &execution, &workspace, &mut json!({}))
            .await
            .unwrap();
        let machine = db::EnvironmentMachine::from_placement(&workspace.placement);
        let mut row = db
            .get_readiness(&task.project_id, &machine)
            .await
            .unwrap()
            .unwrap();
        let version = row.version;
        row.next_check_at = Some("2026-01-01T00:00:00Z".into());
        let row = db.put_readiness(row, Some(version)).await.unwrap();
        let offline =
            Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
        let router = (*service.workspace_backend_router())
            .clone()
            .with_daemon(Arc::new(DaemonWorkspaceBackend::new(db.clone(), offline)));
        let events = Arc::new(EventBus::new(64));
        let service = Arc::new(
            TaskService::new(db.clone(), events.clone())
                .with_workspace_backend_router(Arc::new(router)),
        );
        let dispatcher = crate::TaskDispatcher::new(db.clone(), events, service);
        dispatcher.check_once().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if crate::placement::environment::claim_probe(&task.project_id, &machine).is_some()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let rechecked = db
            .get_readiness(&task.project_id, &machine)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rechecked.status, row.status);
        assert_eq!(rechecked.failing_checks, row.failing_checks);
        assert_eq!(rechecked.checked_at, row.checked_at);
        assert!(rechecked.next_check_at > row.next_check_at);
        assert!(ProjectRepo::get_by_id(&*db, &task.project_id)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_some());
        responder.abort();
    }

    #[tokio::test]
    async fn environment_daemon_recheck_version_error_preserves_facts_and_reschedules() {
        use db::ProjectMachineReadinessRepo;
        let (db, service, task, execution, workspace, responder) = fixture(false).await;
        service
            .prepare_execution_environment(&task, &execution, &workspace, &mut json!({}))
            .await
            .unwrap();
        responder.abort();
        let machine = db::EnvironmentMachine::from_placement(&workspace.placement);
        let mut row = db
            .get_readiness(&task.project_id, &machine)
            .await
            .unwrap()
            .unwrap();
        let version = row.version;
        row.next_check_at = Some("2000-01-01T00:00:00Z".into());
        let before = db.put_readiness(row, Some(version)).await.unwrap();
        let registry =
            Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
        let daemon = workspace
            .placement
            .daemon_id
            .as_deref()
            .unwrap()
            .to_string();
        let (connection, mut outbound) =
            crate::recovery::tests::owner_connection(&registry, &daemon, false);
        let responding = registry.clone();
        let recorded_handle = workspace.placement.workspace_handle.clone().unwrap();
        let error_server = tokio::spawn(async move {
            while let Some(api_types::DaemonFrame::Request { id, method, params }) =
                outbound.recv().await
            {
                assert_eq!(
                    params["workspace_handle"], recorded_handle,
                    "only the failure's workspace is used"
                );
                let frame = match method.as_str() {
                    api_types::METHOD_WORKSPACE_DESCRIBE => api_types::DaemonFrame::Response {
                        id,
                        result: json!({"workspace_handle":recorded_handle,"generation":1,"exists":true,"head_sha":"head","dirty":false,"branch":"task/remote","locked":false,"active_execution_ids":[],"journaled_execution_ids":[]}),
                    },
                    api_types::METHOD_WORKSPACE_RUN => api_types::DaemonFrame::Error {
                        id: Some(id),
                        error: api_types::DaemonErrorPayload {
                            code: "stale_generation".into(),
                            message: "owner version fence".into(),
                            details: None,
                        },
                    },
                    _ => panic!("unexpected re-check operation {method}"),
                };
                responding.dispatch_incoming_for_connection(&daemon, connection, frame);
            }
        });
        let router = (*service.workspace_backend_router())
            .clone()
            .with_daemon(Arc::new(DaemonWorkspaceBackend::new(db.clone(), registry)));
        let service = Arc::new(service.with_workspace_backend_router(Arc::new(router)));
        let dispatcher = crate::TaskDispatcher::new(db.clone(), service.event_bus.clone(), service);
        dispatcher.check_once().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if db
                    .get_readiness(&task.project_id, &machine)
                    .await
                    .unwrap()
                    .unwrap()
                    .version
                    > before.version
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let after = db
            .get_readiness(&task.project_id, &machine)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.status, before.status);
        assert_eq!(after.failing_checks, before.failing_checks);
        assert_eq!(after.check_results, before.check_results);
        assert_eq!(after.output_tail, before.output_tail);
        assert_eq!(after.checked_at, before.checked_at);
        assert!(after.next_check_at > before.next_check_at);
        assert!(TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap()
            .error_annotation
            .is_none());
        error_server.abort();
    }

    #[tokio::test]
    async fn environment_missing_recorded_daemon_workspace_resets_fact_and_pause() {
        use db::ProjectMachineReadinessRepo;
        let (db, service, task, execution, workspace, responder) = fixture(false).await;
        service
            .prepare_execution_environment(&task, &execution, &workspace, &mut json!({}))
            .await
            .unwrap();
        let machine = db::EnvironmentMachine::from_placement(&workspace.placement);
        let mut row = db
            .get_readiness(&task.project_id, &machine)
            .await
            .unwrap()
            .unwrap();
        let version = row.version;
        row.next_check_at = Some("2000-01-01T00:00:00Z".into());
        db.put_readiness(row, Some(version)).await.unwrap();
        sqlx::query("DELETE FROM workspace WHERE id=?")
            .bind(&workspace.placement.workspace_id)
            .execute(db.pool())
            .await
            .unwrap();
        let events = service.event_bus.clone();
        let dispatcher = crate::TaskDispatcher::new(db.clone(), events, Arc::new(service));
        dispatcher.check_once().await.unwrap();
        let reset = db
            .get_readiness(&task.project_id, &machine)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reset.status, db::EnvironmentReadinessStatus::Unknown);
        assert!(ProjectRepo::get_by_id(&*db, &task.project_id)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_none());
        assert!(TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap()
            .error_annotation
            .is_none());
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
