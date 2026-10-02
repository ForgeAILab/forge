use super::super::*;
use crate::task_service::tests::helpers::{seed_execution, seed_role_assignment};
use db::{PageRequest, SortBy, SortOrder};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[tokio::test]
async fn claim_uses_injected_cli_availability() {
    use cli_adapters::test_support::TestAdapter;
    use executors::{AdapterRegistry, AvailabilityStatus};

    let db = Arc::new(sqlite_db().await);
    let workspace_root = TempDir::new().unwrap();
    let mut service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "todo".into()).await;

    for status in [AvailabilityStatus::NotFound, AvailabilityStatus::Installed] {
        let mut registry = AdapterRegistry::new();
        registry.register(Box::new(TestAdapter::new(ExecutorKind::Codex, status)));
        service = service.with_placement_adapter_registry(Arc::new(registry));
        let error = service
            .claim_task(&task.id, Assignee::Agent(agent_id.clone()), None)
            .await
            .expect_err("unavailable fixture adapter must refuse claim");
        let ServiceError::PlacementUnavailable(refusal) = error else {
            panic!("unexpected refusal: {error:?}");
        };
        assert!(refusal.rejected_candidates.iter().any(|candidate| {
            candidate
                .filter_codes
                .contains(&crate::placement::PlacementFilterCode::ExecutorUnavailable)
        }));
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workspace_placement")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "refused claim cannot reserve placement");
    }

    service = service
        .with_placement_adapter_registry(Arc::new(cli_adapters::test_support::test_registry()));
    let claimed = service
        .claim_task(&task.id, Assignee::Agent(agent_id.clone()), None)
        .await
        .expect("available fixture adapter admits claim");
    assert_eq!(
        claimed.execution.agent_id.as_deref(),
        Some(agent_id.as_str())
    );
    assert_eq!(claimed.execution.status, ExecutionStatus::Running);
}

#[tokio::test]
async fn project_pause_precedes_placement_on_every_execution_launch_path() {
    let db = Arc::new(sqlite_db().await);
    let workspace_root = TempDir::new().unwrap();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(workspace_root.path().to_path_buf())
        // Any placement attempt would fail availability even on a signed-in host.
        .with_placement_adapter_registry(Arc::new(executors::AdapterRegistry::new()));
    let (project_id, _, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let claimable = seed_task_with_status(&db, &project_id, "todo".into()).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".into()).await;
    seed_role_assignment(&db, &task.id, "coder", Some(&agent_id)).await;
    let parent = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        "coder",
        ExecutionStatus::Completed,
        Some("fixture-session"),
        &now_rfc3339(),
    )
    .await;
    sqlx::query("UPDATE project SET paused_at = ?, system_pause_reason = 'environment_not_ready' WHERE id = ?")
        .bind(now_rfc3339()).bind(&project_id).execute(db.pool()).await.unwrap();

    let mut errors = vec![service
        .claim_task(&claimable.id, Assignee::Agent(agent_id.clone()), None)
        .await
        .expect_err("paused Project refuses launch")];
    errors.push(
        service
            .launch_execution(&task.id, &agent_id, None, None)
            .await
            .err()
            .expect("paused Project refuses launch"),
    );
    errors.push(
        service
            .dispatch_initial_role_execution(&task.id, &agent_id, "coder", "Continue".into())
            .await
            .expect_err("paused Project refuses launch"),
    );
    errors.push(
        service
            .re_execute_execution(&parent.id)
            .await
            .err()
            .expect("paused Project refuses launch"),
    );
    errors.push(
        service
            .follow_up_execution(&parent.id, "Continue".into(), None, None)
            .await
            .err()
            .expect("paused Project refuses launch"),
    );
    errors.push(
        service
            .follow_up_interactive_execution(&parent.id, "Continue".into(), None, None)
            .await
            .err()
            .expect("paused Project refuses launch"),
    );
    errors.push(
        service
            .resume_task_execution(
                &task,
                &crate::workflow::default_workflow::default_workflow(),
                None,
                task.version,
            )
            .await
            .expect_err("paused Project refuses launch"),
    );
    for error in errors {
        assert!(
            matches!(error, ServiceError::ProjectPaused { project_id: id } if id == project_id)
        );
    }
    for table in [
        "workspace",
        "workspace_placement",
        "workspace_lease",
        "review",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "paused launch cannot mutate {table}");
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 1, "paused launch cannot create an execution");
    for original in [claimable, task] {
        let current = TaskRepo::get_by_id(&*db, &original.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.version, original.version);
        assert_eq!(current.status, original.status);
        assert_eq!(current.metadata_json, original.metadata_json);
        assert_eq!(current.error_annotation, original.error_annotation);
    }
}

#[tokio::test]
async fn unpinned_cli_execution_routes_start_and_cancel_to_ledger_daemon() {
    use crate::daemon_transport::{DaemonConnection, DaemonConnectionRegistry};
    use db::{PricingSubjectRepo, UserRepo};

    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace root creates");
    let connections = Arc::new(DaemonConnectionRegistry::without_handlers());
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf())
        .with_daemon_connections(Arc::clone(&connections));
    let (project_id, repo_id, repo_dir) = seed_project_repo(&db).await;
    let agent_id =
        seed_agent_with_executor_type(&db, "codex", r#"{"model":"routing-test-model"}"#).await;
    let agent = AgentRepo::get_by_id(&*db, &agent_id)
        .await
        .expect("agent loads")
        .expect("agent exists");
    let daemon_id = agent.daemon_id.clone().expect("fixture daemon is pinned");
    let mut agent_update = db::UpdateAgent {
        id: agent_id.clone(),
        expected_version: agent.version,
        name: None,
        description: None,
        model: Some(Some("routing-test-model".to_owned())),
        reasoning_effort: None,
        permission_policy: None,
        prompt_template: None,
        capabilities_json: None,
        config_json: None,
        daemon_id: Some(None),
        max_concurrent_tasks: None,
        heartbeat_interval_seconds: None,
        max_missed_heartbeats: None,
        status: None,
        last_heartbeat_at: None,
        is_default: None,
        paused: None,
        updated_at: now_rfc3339(),
    };
    let agent = AgentRepo::update(&*db, agent_update.clone())
        .await
        .expect("agent is unpinned");
    assert!(agent.daemon_id.is_none());

    let now = now_rfc3339();
    let runtime = db::RuntimeRepo::create(
        &*db,
        db::CreateRuntime {
            id: new_uuid_v4(),
            daemon_id: daemon_id.clone(),
            kind: "local".to_owned(),
            workspace_root: repo_dir
                .path()
                .parent()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            status: db::RuntimeStatus::Ready,
            labels_json: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("shared runtime creates");
    // Seed the verified shared mount whose execution provider admission
    // must record for this unpinned Agent.
    db::RepoLocationRepo::create(
        &*db,
        db::CreateRepoLocation {
            id: new_uuid_v4(),
            repo_id,
            owner_kind: db::RepoLocationOwnerKind::Server,
            daemon_id: Some(daemon_id.clone()),
            runtime_id: Some(runtime.id),
            path: repo_dir.path().to_string_lossy().into_owned(),
            kind: db::RepoLocationKind::SharedMount,
            is_default: true,
            status: db::RepoLocationStatus::Ready,
            last_verified_at: Some(now.clone()),
            last_error: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("ready shared mount creates");
    let (connection, mut outbound) = DaemonConnection::new(daemon_id.clone());
    let connection_id = connection.id();
    connections.register(daemon_id.clone(), connection);
    assert!(connections.dispatch_incoming_for_connection(
        &daemon_id,
        connection_id,
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.to_owned(),
            params: json!({
                "protocol_revision": api_types::DAEMON_PROTOCOL_REVISION,
                "capabilities": api_types::DAEMON_REQUIRED_CAPABILITIES,
                "executor_capabilities": {
                    "codex": {
                        "structured_events": true,
                        "usage": true,
                        "resume": true,
                        "cancel_ack": true,
                        "terminal_observed": true,
                    },
                },
            }),
        },
    ));
    let task = service
        .create_task(
            project_id.clone(),
            "Route the resolved daemon",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id, Assignee::Agent(agent_id.clone()), None)
        .await
        .expect("unpinned CLI agent claims");
    let snapshot: Value = serde_json::from_str(
        claimed
            .execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("snapshot exists"),
    )
    .expect("snapshot parses");
    let placement = db::WorkspacePlacementRepo::get_by_workspace_id(
        &*db,
        claimed
            .execution
            .workspace_id
            .as_deref()
            .expect("execution workspace"),
    )
    .await
    .expect("placement loads")
    .expect("workspace is placed");
    assert_eq!(placement.owner_kind, db::PlacementOwnerKind::Server);
    assert_eq!(
        placement.execution_daemon_id.as_deref(),
        Some(daemon_id.as_str())
    );
    assert_eq!(
        snapshot["placement_id"].as_str(),
        Some(placement.id.as_str())
    );
    assert!(snapshot.get("resolved_daemon_id").is_none());

    // Give admission an account and a fixed rate so the ledger retains the
    // exact CLI runtime subject without depending on a pricing catalog.
    let now = now_rfc3339();
    let owner_id = new_uuid_v4();
    UserRepo::create_user(
        &*db,
        &db::User {
            id: owner_id.clone(),
            email: "daemon-routing@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("ledger owner creates");
    let project = db::ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let updated = sqlx::query(
        "UPDATE project SET owner_id = ?, version = version + 1 WHERE id = ? AND version = ?",
    )
    .bind(&owner_id)
    .bind(&project_id)
    .bind(project.version)
    .execute(db.pool())
    .await
    .expect("project owner sets");
    assert_eq!(updated.rows_affected(), 1);
    PricingSubjectRepo::upsert_pricing_adjustment(
        &*db,
        db::UpsertPricingAdjustment {
            owner_user_id: owner_id,
            scope: db::PricingAdjustmentScope::Agent(agent_id.clone()),
            mode: db::PricingAdjustmentMode::Fixed,
            discount_bps: None,
            fixed_rates: db::RateBuckets::new(Some(1_000_000), Some(2_000_000), None, None),
            catalog_provider_id: None,
            catalog_model_id: None,
            expected_version: 0,
            now,
        },
    )
    .await
    .expect("fixed pricing adjustment creates");

    let respond = async {
        for method in [
            api_types::METHOD_EXECUTION_START,
            api_types::METHOD_EXECUTION_CANCEL,
        ] {
            let frame = outbound.recv().await.expect("daemon receives command");
            let api_types::DaemonFrame::Request {
                id,
                method: actual_method,
                params,
            } = frame
            else {
                panic!("expected daemon request");
            };
            assert_eq!(actual_method, method);
            assert_eq!(params["execution_id"], claimed.execution.id);
            let result = if method == api_types::METHOD_EXECUTION_START {
                json!({ "execution_id": claimed.execution.id, "accepted": true })
            } else {
                json!({ "execution_id": claimed.execution.id, "cancelled": true })
            };
            assert!(connections.dispatch_incoming_for_connection(
                &daemon_id,
                connection_id,
                api_types::DaemonFrame::Response { id, result },
            ));
        }
    };
    let dispatch = async {
        let result = service
            .start_execution(claimed.execution.id.clone())
            .await
            .expect("start dispatches remotely");
        assert!(result.accepted);
        let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(
            execution.lease_owner,
            Some(crate::daemon_transport::execution_lease_owner(
                &daemon_id,
                connection_id,
            )),
        );
        let ledger_daemon: String = sqlx::query_scalar(
            "SELECT s.daemon_id FROM pricing_selection p
             JOIN pricing_subject s ON s.id = p.subject_id
             WHERE p.execution_id = ? AND s.subject_kind = 'cli_runtime'",
        )
        .bind(&execution.id)
        .fetch_one(db.pool())
        .await
        .expect("ledger records the CLI runtime");
        assert_eq!(ledger_daemon, daemon_id);
        let agent = AgentRepo::get_by_id(&*db, &agent_id)
            .await
            .expect("running agent loads")
            .expect("agent exists");
        agent_update.expected_version = agent.version;
        agent_update.daemon_id = Some(Some("different-daemon".to_owned()));
        AgentRepo::update(&*db, agent_update)
            .await
            .expect("agent pin changes after dispatch");
        service
            .cancel_execution_with_provider(&execution, "routing regression")
            .await
            .expect("cancel dispatches to the same daemon");
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(dispatch, respond);
    })
    .await
    .expect("remote start and cancel complete");
}

#[tokio::test]
async fn run_execution_dispatches_shell_adapter_and_updates_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    // This case executes through the adapter registry, so the agent has to be the
    // shell harness: it runs the Task's own command and logs its output.
    // Any other harness shells out to a CLI that is not on the host.
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Run shell",
            Some("printf service-run-ok".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id, Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    assert_eq!(
        claimed.execution.hard_deadline_at, None,
        "Task executions have no wall-clock limit unless one is explicitly configured"
    );
    ExecutionRepo::update(
        &*db,
        db::UpdateExecution {
            id: claimed.execution.id.clone(),
            status: None,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: Some(Some("test-session".to_owned())),
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("seed executor session id");
    let workspace =
        WorkspaceRepo::get_by_id(&*db, claimed.execution.workspace_id.as_deref().unwrap())
            .await
            .expect("workspace loads")
            .expect("workspace exists");
    std::fs::create_dir_all(
        service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves"),
    )
    .expect("workspace dir creates");

    let registry = Arc::new(cli_adapters::test_support::test_registry());
    let executor = executors::AdapterExecutor::new(registry);
    let execution = service
        .run_execution(claimed.execution.id, &executor)
        .await
        .expect("execution runs");

    assert_eq!(execution.status, ExecutionStatus::Completed);
    let logs_path = execution.logs_path.expect("logs path recorded");
    assert!(
        logs_path.contains(&format!(
            "/.forge/logs/{}/{}/",
            task.project_id, workspace.task_id
        )),
        "logs path should live under durable project/task log dir, got {logs_path}"
    );
    let logs = executors::LogReader::read(std::path::Path::new(&logs_path), 0, 100)
        .await
        .expect("logs read");
    assert!(logs.entries.iter().any(|entry| {
        entry.payload.get("line").and_then(|line| line.as_str()) == Some("service-run-ok")
    }));
}

/// Claim a shell-harness Task whose command is `command` in a Project whose
/// settings declare `environment`, and return the claimed execution.
async fn claim_shell_task_with_environment(
    db: &Arc<SqliteDb>,
    service: &TaskService,
    command: &str,
    environment: Value,
) -> (Task, db::Execution, db::Workspace) {
    let (project_id, _repo_id, repo_dir) = seed_project_repo(db).await;
    // The repository directory must outlive the test's executions.
    std::mem::forget(repo_dir);
    let agent_id = seed_agent(db).await;
    let task = service
        .create_task(
            project_id,
            "Needs its environment",
            Some(command.to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
        .bind(json!({ "environment": environment }).to_string())
        .bind(&task.project_id)
        .execute(db.pool())
        .await
        .expect("environment sets");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let workspace =
        WorkspaceRepo::get_by_id(&**db, claimed.execution.workspace_id.as_deref().unwrap())
            .await
            .expect("workspace loads")
            .expect("workspace exists");
    std::fs::create_dir_all(
        service
            .workspace_backend_router()
            .embedded_path(db, &workspace)
            .await
            .expect("workspace path resolves"),
    )
    .expect("workspace dir creates");
    (task, claimed.execution, workspace)
}

#[tokio::test]
async fn run_execution_applies_the_project_environment() {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    let assets = tempfile::tempdir().expect("asset dir");
    std::fs::write(assets.path().join("fountain.png"), "art").expect("asset writes");
    let (_task, execution, workspace) = claim_shell_task_with_environment(
        &db,
        &service,
        "printf \"$PROJECT_TOOL:\"; cat vendor/fountain.png",
        json!({
            "env": { "PROJECT_TOOL": "godot-4.7.2" },
            "assets": [{
                "source": assets.path().to_string_lossy(),
                "target": "vendor",
            }],
            "checks": [{ "name": "tool", "command": "test -n \"$PROJECT_TOOL\"" }],
        }),
    )
    .await;

    let registry = Arc::new(cli_adapters::test_support::test_registry());
    let executor = executors::AdapterExecutor::new(registry);
    let execution = service
        .run_execution(execution.id, &executor)
        .await
        .expect("execution runs");

    assert_eq!(execution.status, ExecutionStatus::Completed);
    assert!(std::path::Path::new(
        &service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves")
    )
    .join("vendor/fountain.png")
    .exists());
    let logs = executors::LogReader::read(
        std::path::Path::new(&execution.logs_path.expect("logs path recorded")),
        0,
        100,
    )
    .await
    .expect("logs read");
    assert!(
        logs.entries.iter().any(|entry| {
            entry.payload.get("line").and_then(|line| line.as_str()) == Some("godot-4.7.2:art")
        }),
        "the executor sees the Project env and the copied asset: {:?}",
        logs.entries
            .iter()
            .filter_map(|entry| entry.payload.get("line"))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn run_execution_passes_checks_configured_before_direct_claim() {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(16)));
    let (_, execution, _) = claim_shell_task_with_environment(
        &db,
        &service,
        "true",
        json!({"checks":[{"name":"tool","command":"true"}]}),
    )
    .await;
    let executor =
        executors::AdapterExecutor::new(Arc::new(cli_adapters::test_support::test_registry()));
    let completed = service
        .run_execution(execution.id, &executor)
        .await
        .unwrap();
    assert_eq!(completed.status, ExecutionStatus::Completed);
}

#[tokio::test]
async fn a_failed_environment_check_pauses_the_project_without_blocking_the_task() {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    let (task, execution, workspace) = claim_shell_task_with_environment(
        &db,
        &service,
        "touch agent-ran",
        json!({
            "checks": [{
                "name": "browser",
                "command": "echo chromium is not installed >&2; exit 4",
            }],
        }),
    )
    .await;
    let before = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads before dispatch")
        .expect("task exists before dispatch");

    let registry = Arc::new(cli_adapters::test_support::test_registry());
    let executor = executors::AdapterExecutor::new(registry);
    let execution = service
        .run_execution(execution.id, &executor)
        .await
        .expect("dispatch settles");

    assert_eq!(execution.status, ExecutionStatus::Failed);
    assert_eq!(execution.resume_policy, Some(db::ResumePolicy::Auto));
    assert!(
        !std::path::Path::new(
            &service
                .workspace_backend_router()
                .embedded_path(&db, &workspace)
                .await
                .expect("workspace path resolves")
        )
        .join("agent-ran")
        .exists(),
        "no agent run is spent on a known-broken environment"
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, before.status);
    assert_eq!(
        current.version, before.version,
        "environment failure does not mutate the Task"
    );
    assert!(current.error_annotation.is_none());
    assert!(current.blocked_json.is_none());
    assert!(current.failed_json.is_none());
    assert!(execution
        .error
        .as_deref()
        .unwrap()
        .starts_with("environment not ready: "));
    let project = ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        project.system_pause_reason.as_deref(),
        Some("environment_not_ready")
    );
    let detail: api_types::ProjectEnvironmentPause =
        serde_json::from_str(project.environment_pause_json.as_deref().unwrap()).unwrap();
    assert_eq!(detail.checks, vec!["browser"]);
    assert_eq!(detail.role.as_deref(), Some(execution.role.as_str()));
    assert!(detail.output.contains("chromium is not installed"));
    assert_eq!(
        project.paused_at.as_deref(),
        Some(detail.paused_at.as_str())
    );
    assert_eq!(detail.paused_at, detail.last_checked_at);
    let last = chrono::DateTime::parse_from_rfc3339(&detail.last_checked_at).unwrap();
    let next = chrono::DateTime::parse_from_rfc3339(&detail.next_check_at).unwrap();
    assert_eq!((next - last).num_seconds(), 600);
}

#[tokio::test]
async fn workflow_dispatched_commitless_completion_fails_and_schedules_retry() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "No-op completion",
            Some("narrate without changing anything".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    sqlx::query("UPDATE execution SET role = 'coder' WHERE id = ?")
        .bind(&claimed.execution.id)
        .execute(db.pool())
        .await
        .expect("execution uses the autonomous coder role");
    // Stamp the dispatcher metadata the workflow dispatch path records, so
    // this run is measured as an autonomous role dispatch (user-claimed runs
    // without the metadata stay exempt).
    let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    let mut snapshot: serde_json::Value = serde_json::from_str(
        execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("claimed execution has a snapshot"),
    )
    .expect("snapshot parses");
    snapshot["dispatch"] = json!({ "target_role": execution.role });
    ExecutionRepo::update(
        &*db,
        db::UpdateExecution {
            id: claimed.execution.id.clone(),
            status: None,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(Some(snapshot.to_string())),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("dispatch metadata stamps");

    let updated = service
        .run_execution(claimed.execution.id.clone(), &NoDiffExecutor)
        .await
        .expect("execution runs");

    assert_eq!(updated.status, ExecutionStatus::Failed);
    assert_eq!(updated.role, "coder");
    assert!(
        updated
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("nothing committed on the Task branch"),
        "error names the commit-less completion: {:?}",
        updated.error
    );

    // The failure enters the normal executor-failure machinery: the retry
    // budget is consumed and the execution becomes dispatcher-resumable.
    let after = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert_eq!(after.resume_policy, Some(db::ResumePolicy::Auto));
    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(task_after.metadata_json.as_deref().unwrap_or("{}"))
            .expect("task metadata parses");
    assert_eq!(metadata["execution_retry_count"], 1);
    assert!(
        metadata.get("deferred_dispatch").is_some(),
        "a deferred redispatch is scheduled: {metadata}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn finalized_completion_does_not_run_repository_fsmonitor_diagnostic() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Finalize without a second status",
            Some("write and finalize the implementation".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id, Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    sqlx::query("UPDATE execution SET role = 'coder' WHERE id = ?")
        .bind(&claimed.execution.id)
        .execute(db.pool())
        .await
        .expect("execution uses the autonomous coder role");
    let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    let mut snapshot: serde_json::Value = serde_json::from_str(
        execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("claimed execution has a snapshot"),
    )
    .expect("snapshot parses");
    snapshot["dispatch"] = json!({ "target_role": execution.role });
    ExecutionRepo::update(
        &*db,
        db::UpdateExecution {
            id: claimed.execution.id.clone(),
            status: None,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(Some(snapshot.to_string())),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("dispatch metadata stamps");

    let workspace = WorkspaceRepo::get_by_id(
        &*db,
        claimed
            .execution
            .workspace_id
            .as_deref()
            .expect("workspace id exists"),
    )
    .await
    .expect("workspace loads")
    .expect("workspace exists");
    let workspace_path = service
        .workspace_backend_router()
        .embedded_path(&db, &workspace)
        .await
        .expect("workspace path resolves");
    let worktree = workspace_path.as_path();
    let marker = repo_dir.path().join("fsmonitor-ran");
    let monitor = worktree.join("fsmonitor.sh");
    std::fs::write(
        &monitor,
        format!(
            "#!/bin/sh\nprintf ran > \"{}\"\nprintf 'token\\n'\n",
            marker.display()
        ),
    )
    .expect("fsmonitor script writes");
    let mut permissions = std::fs::metadata(&monitor)
        .expect("fsmonitor metadata")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&monitor, permissions).expect("fsmonitor becomes executable");
    let config = std::process::Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(["config", "core.fsmonitor"])
        .arg(&monitor)
        .output()
        .expect("git config runs");
    assert!(
        config.status.success(),
        "git config failed: {}",
        String::from_utf8_lossy(&config.stderr)
    );

    let updated = service
        .run_execution(claimed.execution.id, &HostFinalizingExecutor)
        .await
        .expect("execution runs");

    assert_eq!(updated.status, ExecutionStatus::Completed);
    assert!(updated.after_sha.is_some());
    assert!(
        !marker.exists(),
        "runner must not execute fsmonitor after host finalization"
    );
}

#[tokio::test]
async fn completed_reviewer_execution_keeps_the_task_retry_budget_it_has_spent() {
    // A reviewer process exiting cleanly says nothing about whether its verdict
    // was usable. Clearing the retry budget on that completion reset the counter
    // on every attempt, so `attempt > budget` never tripped and a reviewer whose
    // assessment could not be parsed was re-dispatched without bound. The
    // sibling bounded-retry tests pre-seed `execution_retry_count` and drive the
    // cascade directly, so none of them covered this reset.
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Reviewer completes without a usable verdict",
            Some("review the current worktree".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    sqlx::query("UPDATE execution SET role = 'reviewer' WHERE id = ?")
        .bind(&claimed.execution.id)
        .execute(db.pool())
        .await
        .expect("execution becomes reviewer-scoped");
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        claimed.execution.agent_id.as_deref(),
    )
    .await;
    sqlx::query(
        "UPDATE task SET status = 'review', metadata_json = ?, version = version + 1 WHERE id = ?",
    )
    .bind(r#"{"execution_retry_count":2}"#)
    .bind(&task.id)
    .execute(db.pool())
    .await
    .expect("task enters review carrying spent budget");
    let now = now_rfc3339();
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: claimed.execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running review binds the reviewer attempt");

    let updated = service
        .run_execution(claimed.execution.id.clone(), &NoDiffExecutor)
        .await
        .expect("reviewer execution completes");
    assert_eq!(updated.status, ExecutionStatus::Completed);
    assert_eq!(updated.role, crate::workflow::default_roles::REVIEWER);

    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(task_after.metadata_json.as_deref().unwrap_or("{}"))
            .expect("task metadata parses");
    assert_eq!(
        metadata
            .get("execution_retry_count")
            .and_then(|v| v.as_u64()),
        Some(2),
        "a completed reviewer must not refund the retry budget it has already spent: {metadata}"
    );
}

#[tokio::test]
async fn reviewer_provider_unavailability_uses_bounded_task_retry_budget() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Review after provider recovery",
            Some("review the current worktree".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    sqlx::query("UPDATE execution SET role = 'reviewer' WHERE id = ?")
        .bind(&claimed.execution.id)
        .execute(db.pool())
        .await
        .expect("execution becomes reviewer-scoped");
    // A reviewer execution is only authorized while the Task is the
    // reviewer's to work, so move the Task with its assignment the way the
    // workflow would before the reviewer runs.
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        claimed.execution.agent_id.as_deref(),
    )
    .await;
    sqlx::query("UPDATE task SET status = 'review', version = version + 1 WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("task enters review");
    let now = now_rfc3339();
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: claimed.execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running review binds the reviewer attempt");

    let updated = service
        .run_execution(claimed.execution.id.clone(), &ExecutorUnavailableExecutor)
        .await
        .expect("provider unavailability is persisted");

    assert_eq!(updated.status, ExecutionStatus::Failed);
    assert_eq!(updated.role, "reviewer");
    assert!(updated
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("provider unavailable"));

    let execution_after = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert_eq!(execution_after.resume_policy, Some(db::ResumePolicy::Auto));

    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(task_after.metadata_json.as_deref().unwrap_or("{}"))
            .expect("task metadata parses");
    assert!(
        metadata.get("deferred_dispatch").is_some(),
        "reviewer execution should wait for provider recovery: {metadata}"
    );
    assert_eq!(metadata["execution_retry_count"], 1);
    assert!(task_after.blocked_json.is_none());
}

struct CapacityLimitedExecutor {
    retry_after: Option<std::time::Duration>,
}

#[async_trait]
impl TaskExecutor for CapacityLimitedExecutor {
    async fn execute(
        &self,
        _ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult {
            status: ExecutionOutcome::Failed,
            error: Some("You've hit your usage limit".to_owned()),
            failure_class: Some(executors::ExecutionFailureClass::ExecutorUnavailable),
            retry_after: self.retry_after,
            route_attempts: vec![executors::RouteAttempt {
                candidate_key: "only-candidate".to_owned(),
                outcome: executors::RouteAttemptOutcome::UsageExhausted,
            }],
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

#[tokio::test]
async fn capacity_limit_without_fallback_defers_until_reset_or_backoff() {
    for hint in [
        Some(std::time::Duration::from_secs(3900)),
        None,
        Some(std::time::Duration::from_secs(7 * 86400)),
    ] {
        let db = Arc::new(sqlite_db().await);
        let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
        let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
        let agent_id = seed_agent(&db).await;
        let task = service
            .create_task(
                project_id,
                "Wait for quota",
                Some("implement the task".to_owned()),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let claimed = service
            .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
            .await
            .unwrap();
        let retry_count = u64::from(hint.is_none());
        if retry_count > 0 {
            sqlx::query("UPDATE task SET metadata_json = ?, version = version + 1 WHERE id = ?")
                .bind(json!({"execution_retry_count": retry_count}).to_string())
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        let before = chrono::Utc::now();
        let execution = service
            .run_execution(
                claimed.execution.id,
                &CapacityLimitedExecutor { retry_after: hint },
            )
            .await
            .unwrap();
        let after = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let metadata: Value =
            serde_json::from_str(after.metadata_json.as_deref().unwrap()).unwrap();
        let at = chrono::DateTime::parse_from_rfc3339(
            metadata["deferred_dispatch"]["not_before"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        let delay = (at.with_timezone(&chrono::Utc) - before).num_seconds();
        let expected = hint.map_or(20, |hint| hint.as_secs().min(6 * 3600)) as i64;
        assert!(
            delay >= expected && delay <= expected + 30,
            "{delay} vs {expected}"
        );
        assert_eq!(metadata["execution_retry_count"], retry_count + 1);
        let persisted_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted_execution.resume_policy,
            Some(db::ResumePolicy::Auto)
        );
        assert!(after.version > claimed.task.version);
        assert!(after.blocked_json.is_none());
        assert!(after.failed_json.is_none());
        assert!(after.error_annotation.is_none());
        let assignments = db::TaskRoleAssignmentRepo::list_by_task(&*db, &task.id)
            .await
            .unwrap();
        let health = crate::task_diagnostics::derive_workflow_health(
            &after,
            &crate::workflow::default_workflow::default_workflow(),
            &assignments,
            None,
            Some(&execution),
            false,
            None,
        );
        assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
        assert_eq!(health.label, "Retry Scheduled");
        assert!(health.message.unwrap().contains("usage limit"));

        service
            .annotate_executor_unavailable_block(
                &execution,
                hint.map(|hint| {
                    (chrono::Utc::now() + chrono::Duration::seconds(hint.as_secs() as i64))
                        .to_rfc3339()
                }),
                json!([{"outcome": "usage_exhausted"}]),
            )
            .await
            .unwrap();
        let repeated = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(repeated.metadata_json, after.metadata_json);
        assert_eq!(repeated.version, after.version);
    }
}

#[tokio::test]
async fn capacity_limit_retry_budget_exhaustion_blocks() {
    assert_capacity_limit_retry_block(3, 3, "retry budget exhausted").await;
}

#[tokio::test]
async fn capacity_limit_zero_retry_budget_blocks_as_disabled() {
    assert_capacity_limit_retry_block(0, 0, "automatic retries are disabled").await;
}

async fn assert_capacity_limit_retry_block(budget: u64, retry_count: u64, reason: &str) {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Exhausted quota retries",
            Some("implement the task".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .unwrap();
    sqlx::query("UPDATE task SET metadata_json = ?, task_state_config = ?, version = version + 1 WHERE id = ?")
        .bind(json!({"execution_retry_count": retry_count}).to_string())
        .bind(json!({"retry_budgets": {"execution": budget}}).to_string())
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    let execution = service
        .run_execution(
            claimed.execution.id,
            &CapacityLimitedExecutor {
                retry_after: Some(std::time::Duration::from_secs(3900)),
            },
        )
        .await
        .unwrap();
    let after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let annotation: api_types::TaskBlockingAnnotation =
        serde_json::from_str(after.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(
        annotation.annotation_type,
        api_types::FailureKind::ExecutorUnavailable
    );
    let message = annotation.message.unwrap();
    assert!(message.contains(reason), "{message}");
    if budget == 0 {
        assert!(!message.contains("retry budget exhausted"), "{message}");
    }
    assert!(after.blocked_json.is_some());
    assert!(after.failed_json.is_none());
    assert_eq!(execution.resume_policy, Some(db::ResumePolicy::Manual));
    let metadata: Value = serde_json::from_str(after.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(metadata["execution_retry_count"], retry_count);
    assert!(metadata.get("deferred_dispatch").is_none());
}

#[tokio::test]
async fn uncommitted_native_worker_failure_prompts_a_retry_and_preserves_the_diff() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Commit the preserved implementation",
            Some("write the implementation and commit it".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let workspace = WorkspaceRepo::get_by_id(
        &*db,
        claimed
            .execution
            .workspace_id
            .as_deref()
            .expect("workspace id exists"),
    )
    .await
    .expect("workspace loads")
    .expect("workspace exists");

    let updated = service
        .run_execution(
            claimed.execution.id.clone(),
            &UncommittedWorktreeFailureExecutor,
        )
        .await
        .expect("execution failure is persisted");

    let expected_error = ExecutorError::Other(
        crate::embedded_task_executor::UNCOMMITTED_WORKTREE_FAILURE.to_owned(),
    )
    .to_string();
    assert_eq!(updated.status, ExecutionStatus::Failed);
    assert_eq!(updated.error.as_deref(), Some(expected_error.as_str()));
    assert!(std::path::Path::new(
        &service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves")
    )
    .join("uncommitted.txt")
    .exists());
    assert!(!git::is_worktree_clean(std::path::Path::new(
        &service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves")
    ))
    .await
    .expect("worktree cleanliness reads"));

    let comments = db::TaskCommentRepo::list_comments(
        &*db,
        &task.id,
        db::PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: db::SortBy::CreatedAt,
            sort_order: db::SortOrder::Asc,
        },
    )
    .await
    .expect("comments load");
    assert!(comments.items.iter().any(|comment| {
        comment.content.contains("worktree was preserved")
            && comment.content.contains("commit the result")
    }));

    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(task_after.metadata_json.as_deref().unwrap_or("{}"))
            .expect("task metadata parses");
    assert_eq!(metadata["execution_retry_count"], 1);
    assert!(metadata.get("deferred_dispatch").is_some());
    let execution_after = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert_eq!(execution_after.resume_policy, Some(db::ResumePolicy::Auto));
}

#[tokio::test]
async fn run_execution_reissues_stale_lease_after_task_row_moves() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Lease survives task-row movement",
            Some("printf lease-race-ok".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");

    // Any Task-row mutation between lease issuance and dispatch (role
    // handoff, retry-metadata clear, concurrent transition) bumps the
    // version and breaks the lease's exact match. The dispatch must
    // recover by reissuing this execution's lease, not hard-fail.
    sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("task version bumps");

    let updated = service
        .run_execution(claimed.execution.id, &NoDiffExecutor)
        .await
        .expect("execution recovers from the stale lease instead of failing");

    assert_eq!(updated.status, ExecutionStatus::Completed);
}

#[tokio::test]
async fn run_execution_rejects_stale_coder_lease_after_task_moves_to_review() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Reject stale coder lease after review handoff",
            Some("this must not run after the role handoff".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims as coder");

    // Move the Task to the default workflow's reviewer state without changing
    // the execution's persisted coder role or its active lease. The lease
    // version is now stale, which forces run_execution through the reissue
    // branch that previously accepted any role found elsewhere in the
    // workflow.
    sqlx::query(
        "UPDATE task
         SET status = 'review', version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(now_rfc3339())
    .bind(&task.id)
    .execute(db.pool())
    .await
    .expect("task moves to review");

    let error = service
        .run_execution(claimed.execution.id.clone(), &NoDiffExecutor)
        .await
        .expect_err("a coder lease must not be reissued for reviewer state");
    assert!(
        error
            .to_string()
            .contains("current effective role is 'reviewer'"),
        "unexpected admission error: {error}"
    );

    let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert_eq!(
        execution.status,
        ExecutionStatus::Failed,
        "obsolete role admission must terminalize the execution instead of leaving a running orphan"
    );
    assert_ne!(execution.status, ExecutionStatus::Running);
    assert!(
        ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .expect("running execution lookup")
            .is_empty(),
        "authority loss must not leave a running execution occupying the Task slot"
    );
    assert!(
        db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
            .await
            .expect("active lease lookup")
            .is_none(),
        "the stale coder lease must be revoked without issuing reviewer authority"
    );
    let current_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(
        current_task.error_annotation.is_none() && current_task.blocked_json.is_none(),
        "the obsolete coder failure must not block or annotate the newer reviewer state"
    );
}

#[tokio::test]
async fn workspace_lease_reissue_rejects_stale_project_workflow_authority() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Reject stale project workflow lease",
            Some("workflow authority race".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let lease = db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
        .await
        .expect("active lease loads")
        .expect("claim creates a workspace lease");
    let project = db::ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let expected_project_version = project.version;
    let expected_workflow_definition = project.workflow_definition.clone();

    // Simulate the Project workflow edit winning after the dispatcher took
    // its authority snapshot but before the lease INSERT. The workflow stays
    // valid and can retain the same effective role; the Project revision must
    // still be exact because the prompt/config authority came from the old
    // snapshot.
    let edited_workflow =
        serde_json::to_string(&crate::workflow::default_workflow::default_workflow())
            .expect("workflow serializes");
    sqlx::query(
        "UPDATE project
         SET workflow_definition = ?, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(edited_workflow)
    .bind(now_rfc3339())
    .bind(&task.project_id)
    .execute(db.pool())
    .await
    .expect("project workflow edits");

    let now = now_rfc3339();
    let stale_input = db::CreateWorkspaceLease {
        id: new_uuid_v4(),
        project_id: lease.project_id.clone(),
        task_id: lease.task_id.clone(),
        task_version: lease.task_version,
        execution_id: claimed.execution.id.clone(),
        operation_idempotency_key: format!("{}::workflow-race", claimed.execution.id),
        repository_binding_id: lease.repository_binding_id.clone(),
        base_ref: lease.base_ref.clone(),
        role: lease.role.clone(),
        capabilities_json: lease.capabilities_json.clone(),
        assigned_principal_type: lease.assigned_principal_type.clone(),
        assigned_principal_id: lease.assigned_principal_id.clone(),
        capability_profile_revision: lease.capability_profile_revision.clone(),
        capability_profile_digest: lease.capability_profile_digest.clone(),
        issuing_principal_type: lease.issuing_principal_type.clone(),
        issuing_principal_id: lease.issuing_principal_id.clone(),
        issued_at: now.clone(),
        expires_at: now.clone(),
        created_at: now.clone(),
        updated_at: now,
    };
    let error = db::WorkspaceLeaseRepo::replace_with_project_authority(
        &*db,
        stale_input.clone(),
        expected_project_version,
        &expected_workflow_definition,
        &task.status,
        crate::workflow::default_roles::CODER,
        &lease.id,
        lease.version,
    )
    .await
    .expect_err("edited Project authority must reject the stale lease snapshot");
    assert!(matches!(error, db::DbError::VersionConflict));
    assert!(
        db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
            .await
            .expect("active lease lookup")
            .is_some_and(|active| active.id == lease.id && active.version == lease.version),
        "a rejected authority snapshot must roll back the replacement and preserve the old lease"
    );

    // The Project snapshot now is current, so this second rejection can only
    // come from the transaction's live Task/workflow role check. The default
    // in-progress state is coder, never reviewer.
    let current_project = db::ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .expect("current project loads")
        .expect("current project exists");
    let mut wrong_role_input = stale_input.clone();
    wrong_role_input.id = new_uuid_v4();
    wrong_role_input.operation_idempotency_key =
        format!("{}::workflow-role-race", claimed.execution.id);
    let role_error = db::WorkspaceLeaseRepo::replace_with_project_authority(
        &*db,
        wrong_role_input,
        current_project.version,
        &current_project.workflow_definition,
        &task.status,
        crate::workflow::default_roles::REVIEWER,
        &lease.id,
        lease.version,
    )
    .await
    .expect_err("live effective role must reject a reviewer lease for coder state");
    assert!(matches!(role_error, db::DbError::VersionConflict));
    assert!(
        db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
            .await
            .expect("active lease lookup after role rejection")
            .is_some_and(|active| active.id == lease.id && active.version == lease.version),
        "role rejection must preserve the old lease transactionally"
    );

    // Finally force the INSERT trigger to reject after the replacement code
    // has reached the old-lease retirement point. BEGIN IMMEDIATE must roll
    // that retirement back as well.
    let mut trigger_failure_input = stale_input;
    trigger_failure_input.id = new_uuid_v4();
    trigger_failure_input.operation_idempotency_key =
        format!("{}::insert-trigger-race", claimed.execution.id);
    trigger_failure_input.assigned_principal_id = "missing-agent".to_owned();
    let issued_at = now_rfc3339();
    trigger_failure_input.issued_at = issued_at.clone();
    trigger_failure_input.expires_at =
        (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    trigger_failure_input.created_at = issued_at.clone();
    trigger_failure_input.updated_at = issued_at;
    let trigger_error = db::WorkspaceLeaseRepo::replace_with_project_authority(
        &*db,
        trigger_failure_input,
        current_project.version,
        &current_project.workflow_definition,
        &task.status,
        crate::workflow::default_roles::CODER,
        &lease.id,
        lease.version,
    )
    .await
    .expect_err("lease scope trigger must reject the mismatched assignment");
    assert!(matches!(trigger_error, db::DbError::VersionConflict));
    assert!(
        db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
            .await
            .expect("active lease lookup after trigger rejection")
            .is_some_and(|active| active.id == lease.id && active.version == lease.version),
        "an INSERT-trigger failure must roll back old-lease retirement"
    );
}

#[tokio::test]
async fn user_claimed_commitless_completion_stays_completed() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Claimed no-op",
            Some("user-invoked run".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id, Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");

    // No dispatcher metadata: the claim-launched run keeps the historical
    // completion semantics even when it changes nothing.
    let updated = service
        .run_execution(claimed.execution.id, &NoDiffExecutor)
        .await
        .expect("execution runs");

    assert_eq!(updated.status, ExecutionStatus::Completed);
}

#[tokio::test]
async fn run_execution_emits_terminal_execution_event() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Emit terminal event",
            Some("complete".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let mut rx = event_bus.subscribe();

    let execution = service
        .run_execution(claimed.execution.id.clone(), &NoDiffExecutor)
        .await
        .expect("execution runs");

    assert_eq!(execution.status, ExecutionStatus::Completed);
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let event = rx.recv().await.expect("terminal event emits");
            if event.event_type == "execution.completed" {
                break event;
            }
        }
    })
    .await
    .expect("execution.completed event received");
    assert_eq!(event.entity_id, claimed.execution.id);
    match event.context {
        EventContext::ExecutionCompleted { task_id } => assert_eq!(task_id, task.id),
        other => panic!("unexpected event context: {other:?}"),
    }
}

#[tokio::test]
async fn run_execution_rejects_when_terminal_active_in_workspace() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let terminal_activity = Arc::new(TerminalActivityTracker::default());
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_terminal_activity_tracker(Arc::clone(&terminal_activity));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Terminal blocks execution",
            Some("complete".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id, Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let workspace_id = claimed
        .execution
        .workspace_id
        .as_deref()
        .expect("execution has workspace");
    assert!(terminal_activity.try_mark_active(workspace_id).await);
    let executor = CountingExecutor::default();

    let error = service
        .run_execution(claimed.execution.id, &executor)
        .await
        .expect_err("active terminal rejects execution");

    assert!(matches!(
        error,
        ServiceError::TerminalActiveExecution { .. }
    ));
    assert_eq!(
        executor.calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "executor must not launch while a terminal is active"
    );
}

#[tokio::test]
async fn run_execution_batches_execution_log_events() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(128));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Batch execution logs",
            Some("emit logs".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let mut rx = event_bus.subscribe();

    let execution = service
        .run_execution(
            claimed.execution.id.clone(),
            &BurstLogExecutor { count: 55 },
        )
        .await
        .expect("execution runs");

    assert_eq!(execution.status, ExecutionStatus::Completed);
    let log_events = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        let mut events = Vec::new();
        while events.len() < 2 {
            let event = rx.recv().await.expect("execution log event emits");
            if event.event_type == "execution.log" {
                events.push(event);
            }
        }
        events
    })
    .await
    .expect("batched execution.log events received");

    assert_eq!(log_events.len(), 2);
    let mut total_logs = 0;
    let mut saw_multi_log_event = false;
    for event in log_events {
        assert_eq!(event.entity_id, claimed.execution.id);
        match event.context {
            EventContext::ExecutionLog { task_id, log, logs } => {
                assert_eq!(task_id, task.id);
                assert!(!log.is_null());
                let logs = logs.expect("batched logs included");
                saw_multi_log_event |= logs.len() > 1;
                total_logs += logs.len();
            }
            other => panic!("unexpected event context: {other:?}"),
        }
    }
    assert_eq!(total_logs, 55);
    assert!(saw_multi_log_event);
}

#[tokio::test]
async fn launch_execution_creates_interactive_execution_and_workspace() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    crate::test_support::clear_project_execution_role_defaults(&db, &project_id).await;
    let task = service
        .create_task(
            project_id,
            "Launch interactive",
            Some("printf launch-ok".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");

    let launched = service
        .launch_execution(task.id.clone(), agent_id.clone(), None, None)
        .await
        .expect("interactive launch succeeds");

    assert_eq!(launched.task.status, "in_progress".to_owned());
    assert_eq!(launched.execution.role, "interactive".to_owned());
    assert_eq!(launched.execution.status, ExecutionStatus::Running);
    assert_eq!(
        launched.execution.agent_id.as_deref(),
        Some(agent_id.as_str())
    );
    assert_eq!(launched.workspace.task_id, task.id);
}

#[tokio::test]
async fn dispatch_initial_role_execution_creates_execution_and_spawns() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;

    let execution = service
        .dispatch_initial_role_execution(
            &task.id,
            &agent_id,
            crate::workflow::default_roles::CODER,
            "implement the task".to_owned(),
        )
        .await
        .expect("initial role dispatch succeeds");

    assert_eq!(execution.role, crate::workflow::default_roles::CODER);
    assert_eq!(execution.status, ExecutionStatus::Running);
    assert_eq!(execution.agent_id.as_deref(), Some(agent_id.as_str()));
    assert_eq!(execution.summary.as_deref(), Some("implement the task"));

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let current = ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .expect("execution loads")
                .expect("execution exists");
            if current.status == ExecutionStatus::Completed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("spawned execution completes");
}

#[tokio::test]
async fn planner_completion_advances_default_planning_gate() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let embedded = Arc::new(crate::EmbeddedAgentService::new(
        Arc::clone(&db),
        b"planner-outbox-test-key",
    ));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(OutboxPlanExecutor {
            plan: "- [ ] implement the plan\n",
        }))
        .with_provider_credential_env(embedded)
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::TODO.to_owned(),
    )
    .await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        Some(&agent_id),
    )
    .await;

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads after role assignment")
        .expect("task exists");
    let in_flight = service
        .claim_completion_cascade(&task.id)
        .expect("originating service claims the Task completion slot");
    service
        .transition(
            task.id.clone(),
            crate::workflow::default_states::PLANNING.to_owned(),
            task.version,
        )
        .await
        .expect("workflow transition dispatches the planner");
    let execution = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("planner executions load")
    .items
    .into_iter()
    .find(|execution| execution.role == crate::workflow::default_roles::PLANNER)
    .expect("the workflow hook creates a planner execution");

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let current = ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .expect("execution loads")
                .expect("execution exists");
            if current.status == ExecutionStatus::Completed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("planner execution completes");

    // Poll against a deadline instead of wrapping the loop in a timeout: a
    // timeout can cancel a query mid-flight, and the single in-memory
    // connection is then replaced by an empty database.
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(250);
    let mut settled_early = false;
    while std::time::Instant::now() < deadline {
        let logs = TransitionLogRepo::list_by_task(&*db, &task.id)
            .await
            .expect("transition logs load while completion slot is held");
        if logs.iter().any(|log| {
            log.from_state == crate::workflow::default_states::PLANNING
                && log.to_state == crate::workflow::default_states::IN_PROGRESS
        }) || workspace_root
            .path()
            .join(&task.id)
            .join("plan.md")
            .exists()
        {
            settled_early = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        !settled_early,
        "the workflow-dispatched runner must wait on the originating service's completion slot"
    );
    let waiting = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads while completion slot is held")
        .expect("task exists");
    assert_eq!(
        waiting.status,
        crate::workflow::default_states::PLANNING,
        "the workflow-dispatched runner must share the originating service's completion slot"
    );
    let plan_path = workspace_root.path().join(&task.id).join("plan.md");
    assert!(
        !plan_path.exists(),
        "plan publication waits for the shared completion slot"
    );
    drop(in_flight);

    // The default planning gate is not a human approval boundary: the
    // finished planner advances the Task instead of leaving a plan-review
    // marker the dispatcher would keep relaunching the planner against.
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let logs = TransitionLogRepo::list_by_task(&*db, &task.id)
                .await
                .expect("transition logs load");
            if logs.iter().any(|log| {
                log.from_state == crate::workflow::default_states::PLANNING
                    && log.to_state == crate::workflow::default_states::IN_PROGRESS
            }) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("planner completion advances the planning gate");
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = task.metadata().expect("metadata parses");
    assert!(metadata.extra.get("awaiting_human").is_none());
    assert!(metadata.extra.get("awaiting_human_reason").is_none());
    assert_eq!(
        std::fs::read_to_string(plan_path).expect("the terminal-CAS winner publishes the plan"),
        "- [ ] implement the plan\n"
    );
    let outbox = executors::execution_outbox_path(
        &workspace_root.path().join(&task.id).join("forge"),
        &execution.id,
    )
    .expect("outbox path");
    assert!(!outbox.exists(), "published execution outbox is consumed");
}

#[tokio::test]
async fn plan_publication_cleanup_release_preserves_public_task_version() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::PLANNING.to_owned(),
    )
    .await;
    let execution_id = "execution-with-private-plan-cleanup";
    let cleanup = json!({
        "execution_id": execution_id,
        "state": task.status,
        "state_entry_token": null,
        "project_version": 0,
    });
    let marked = TaskRepo::mutate_metadata(
        &*db,
        &task.id,
        Some(task.version),
        vec![db::TaskMetadataMutation::Set {
            key: "plan_publication_cleanup".to_owned(),
            value: cleanup,
        }],
        &now_rfc3339(),
    )
    .await
    .expect("cleanup marker writes");

    let cleared =
        crate::task_service::execution::clear_plan_publication_cleanup(&db, &marked, execution_id)
            .await
            .expect("cleanup marker clears");

    assert_eq!(
        cleared.version, marked.version,
        "host-private cleanup must not invalidate a client-visible Task version"
    );
    assert!(cleared
        .metadata()
        .expect("metadata parses")
        .extra
        .get("plan_publication_cleanup")
        .is_none());
}

#[tokio::test]
async fn approval_gated_planner_completion_waits_for_human() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let embedded = Arc::new(crate::EmbeddedAgentService::new(
        Arc::clone(&db),
        b"approval-gated-planner-test-key",
    ));
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_task_executor(Arc::new(OutboxPlanExecutor {
            plan: "- [ ] approved implementation\n",
        }))
        .with_provider_credential_env(embedded)
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::PLANNING)
        .and_then(|state| state.gate_config.as_mut())
        .expect("planning gate config")
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project workflow updates");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::PLANNING.to_owned(),
    )
    .await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        Some(&agent_id),
    )
    .await;

    let execution = service
        .dispatch_initial_role_execution(
            &task.id,
            &agent_id,
            crate::workflow::default_roles::PLANNER,
            "plan the approval-gated task".to_owned(),
        )
        .await
        .expect("planner dispatch succeeds");

    let waiting = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let current = TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .expect("task loads")
                .expect("task exists");
            let metadata = current.metadata().expect("metadata parses");
            if metadata
                .extra
                .get("awaiting_human_reason")
                .and_then(Value::as_str)
                == Some("plan_review")
            {
                break current;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("planner completion marks the approval gate awaiting human");
    assert_eq!(waiting.status, crate::workflow::default_states::PLANNING);
    assert_eq!(
        waiting
            .metadata()
            .expect("metadata parses")
            .extra
            .get("planning_execution_id")
            .and_then(Value::as_str),
        Some(execution.id.as_str())
    );
    assert_eq!(
        std::fs::read_to_string(workspace_root.path().join(&task.id).join("plan.md"))
            .expect("approved plan publishes before review"),
        "- [ ] approved implementation\n"
    );
    assert!(!TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs load")
        .iter()
        .any(
            |log| log.from_state == crate::workflow::default_states::PLANNING
                && log.to_state == crate::workflow::default_states::IN_PROGRESS
        ));

    let approved = service
        .transition(
            task.id.clone(),
            crate::workflow::default_states::IN_PROGRESS.to_owned(),
            waiting.version,
        )
        .await
        .expect("explicit approval advances planning");
    assert_eq!(
        approved.task.status,
        crate::workflow::default_states::IN_PROGRESS
    );
}

#[tokio::test]
async fn approval_gated_planner_without_a_valid_plan_retries_instead_of_waiting() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(32));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_task_executor(Arc::new(SessionNoPlanExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::PLANNING)
        .and_then(|state| state.gate_config.as_mut())
        .expect("planning gate config")
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project workflow updates");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::PLANNING.to_owned(),
    )
    .await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        Some(&agent_id),
    )
    .await;

    service
        .dispatch_initial_role_execution(
            &task.id,
            &agent_id,
            crate::workflow::default_roles::PLANNER,
            "plan but omit the artifact".to_owned(),
        )
        .await
        .expect("planner dispatch succeeds");

    let blocked = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let current = TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .expect("task loads")
                .expect("task exists");
            if current.blocked_json.is_some() {
                break current;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("missing plan exhausts its bounded retry budget");
    assert_eq!(blocked.status, crate::workflow::default_states::PLANNING);
    assert!(blocked.error_annotation.is_some());
    let blocked_metadata = blocked.metadata().expect("metadata parses");
    assert!(
        blocked_metadata.extra.get("awaiting_human").is_none(),
        "an approval gate cannot wait on a missing plan"
    );
    assert_eq!(
        blocked
            .blocked_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .and_then(|value| value.get("source").cloned())
            .and_then(|value| value.as_str().map(str::to_owned))
            .as_deref(),
        Some("planning_plan_ready")
    );
    let attempts = ExecutionRepo::count_by_task_and_role(
        &*db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
    )
    .await
    .expect("planner attempt count loads");
    assert!(attempts > 1, "a resumable planner receives a bounded retry");
    assert!(
        attempts < 10,
        "the retry budget must prevent an unbounded loop"
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::PLANNER,
        )
        .await
        .expect("stable planner attempt count loads"),
        attempts,
        "a blocked plan contract must not launch another planner"
    );
}

#[tokio::test]
async fn before_enter_runs_required_before_work_hook_before_role_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "printf required-ok > required-hook.out; exit 0",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(agent_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads after the role assignment")
        .expect("task exists");

    let transitioned = service
        .transition(task.id.clone(), "in_progress".to_owned(), task.version)
        .await
        .expect("required hook passes and transition succeeds");

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].role,
        crate::workflow::default_roles::CODER
    );
    assert_eq!(
        executions.items[0].agent_id.as_deref(),
        Some(agent_id.as_str())
    );
    assert_eq!(transitioned.task.entry_barrier_json, None);

    let workspace =
        WorkspaceRepo::get_by_id(&*db, executions.items[0].workspace_id.as_deref().unwrap())
            .await
            .expect("workspace loads")
            .expect("workspace exists");
    let marker = std::fs::read_to_string(
        std::path::Path::new(
            &service
                .workspace_backend_router()
                .embedded_path(&db, &workspace)
                .await
                .expect("workspace path resolves"),
        )
        .join("required-hook.out"),
    )
    .expect("required hook marker exists");
    assert_eq!(marker, "required-ok");
}

#[tokio::test]
async fn before_enter_blocks_when_required_before_work_hook_fails() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "echo preflight-out; echo preflight-err >&2; exit 9",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(agent_id),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads after the role assignment")
        .expect("task exists");

    let result = service
        .transition(task.id.clone(), "in_progress".to_owned(), task.version)
        .await
        .expect("required hook failure records a blocked entry");
    assert_eq!(result.task.status, "in_progress");
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert!(
        executions.items.is_empty(),
        "no execution should be created"
    );

    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(blocked.status, "in_progress");
    let barrier: serde_json::Value = serde_json::from_str(
        blocked
            .entry_barrier_json
            .as_deref()
            .expect("entry barrier remains blocked"),
    )
    .expect("entry barrier parses");
    assert_eq!(barrier["state"], "in_progress");
    assert_eq!(barrier["status"], "blocked");
    let annotation: serde_json::Value = serde_json::from_str(
        blocked
            .error_annotation
            .as_deref()
            .expect("blocking annotation is recorded"),
    )
    .expect("annotation parses");
    assert_eq!(annotation["type"], "before_work_hook_failed");
    assert_eq!(annotation["artifact"]["kind"], "hook");
    assert_eq!(annotation["hook"]["exit_code"], 9);
    assert_eq!(annotation["hook"]["stdout"], "preflight-out\n");
    assert_eq!(annotation["hook"]["stderr"], "preflight-err\n");
    let recovery_actions = annotation["recovery_actions"]
        .as_array()
        .expect("recovery actions array");
    assert!(recovery_actions.iter().any(|value| value == "retry_hook"));
    assert!(recovery_actions
        .iter()
        .any(|value| value == "update_workspace_and_retry_hook"));
    assert!(recovery_actions
        .iter()
        .any(|value| value == "skip_hook_once"));
    assert!(recovery_actions.iter().any(|value| value == "cancel_task"));
    let interruption_payload: String = sqlx::query_scalar(
        "SELECT payload_json FROM domain_event
         WHERE event_type = 'task.interruption_changed' AND entity_id = ?
         ORDER BY sequence DESC LIMIT 1",
    )
    .bind(&task.id)
    .fetch_one(db.pool())
    .await
    .expect("blocking annotation commits an interruption event");
    let interruption: serde_json::Value =
        serde_json::from_str(&interruption_payload).expect("interruption event parses");
    assert_eq!(interruption["requires_intervention"], true);
    assert_eq!(
        interruption["interruption"]["recovery_actions"],
        annotation["recovery_actions"]
    );
    let log_path = annotation["hook"]["log_path"]
        .as_str()
        .expect("hook log path recorded");
    assert!(
        std::path::Path::new(log_path).exists(),
        "hook log path should exist: {log_path}"
    );
}

#[tokio::test]
async fn retry_hook_reruns_blocked_before_enter_and_dispatches_when_it_passes() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let failing_settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "echo preflight-out; echo preflight-err >&2; exit 9",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(failing_settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(agent_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads after the role assignment")
        .expect("task exists");

    service
        .transition(task.id.clone(), "in_progress".to_owned(), task.version)
        .await
        .expect("required hook failure records a blocked entry");
    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(blocked.entry_barrier_json.is_some());
    assert!(blocked.error_annotation.is_some());

    let passing_settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "printf retry-ok > retry-hook.out; exit 0",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(passing_settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::RetryHook,
            None,
            None,
        )
        .await
        .expect("retry hook recovers");

    assert_eq!(recovered.status, "in_progress");
    assert_eq!(recovered.entry_barrier_json, None);
    assert_eq!(recovered.error_annotation, None);
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].agent_id.as_deref(),
        Some(agent_id.as_str())
    );
    let workspace =
        WorkspaceRepo::get_by_id(&*db, executions.items[0].workspace_id.as_deref().unwrap())
            .await
            .expect("workspace loads")
            .expect("workspace exists");
    let marker = std::fs::read_to_string(
        std::path::Path::new(
            &service
                .workspace_backend_router()
                .embedded_path(&db, &workspace)
                .await
                .expect("workspace path resolves"),
        )
        .join("retry-hook.out"),
    )
    .expect("retry hook marker exists");
    assert_eq!(marker, "retry-ok");
}

#[tokio::test]
async fn retry_hook_after_manual_merge_repair_returns_to_fresh_review_without_worker_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::REVIEW)
        .and_then(|state| state.gate_config.as_mut())
        .expect("review gate config")
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project workflow requires human review");
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::MERGING.to_owned(),
    )
    .await;
    let task = TaskRepo::set_review_passed_at_cas(
        &*db,
        &task.id,
        task.version,
        Some("2026-09-12T10:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .expect("review authority seeds");
    let annotation = api_types::TaskAnnotation::Blocking(api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::MergeConflict,
        blocking_reason: "manual task-worktree repair required".to_owned(),
        blocked_by: Some("manual_workspace_repair".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: Some("repair and re-review".to_owned()),
        hook: None,
        recovery_actions: vec![api_types::RecoveryAction::RetryHook],
    });
    let task = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation serializes"),
            )),
            blocked_json: Some(Some(
                json!({
                    "reason": "manual task-worktree repair required",
                    "kind": "merge_conflict"
                })
                .to_string(),
            )),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("manual repair blocker seeds");

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::RetryHook,
            Some("manual conflict repair committed".to_owned()),
            None,
        )
        .await
        .expect("manual repair returns through review");

    assert_eq!(recovered.status, crate::workflow::default_states::REVIEW);
    assert_eq!(recovered.review_passed_at, None);
    assert_eq!(recovered.error_annotation, None);
    assert_eq!(recovered.error_annotation, None);
    let entries = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition log loads");
    assert!(entries.iter().any(|entry| {
        entry.from_state == crate::workflow::default_states::MERGING
            && entry.to_state == crate::workflow::default_states::MERGE_FAILED
            && entry
                .trigger_reason
                .contains(crate::workflow::REVIEW_REFRESH_MARKER)
    }));
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert!(
        executions.items.is_empty(),
        "review refresh must not dispatch a merge-fix Worker"
    );
}

#[tokio::test]
async fn update_workspace_and_retry_hook_rebases_before_retrying_blocked_hook() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, repo_dir) = seed_project_repo(&db).await;
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "test -f hook-marker.txt",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(agent_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads after the role assignment")
        .expect("task exists");

    service
        .transition(task.id.clone(), "in_progress".to_owned(), task.version)
        .await
        .expect("required hook failure records a blocked entry");
    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(blocked.entry_barrier_json.is_some());
    let workspace = WorkspaceRepo::get_by_task_id(&*db, &task.id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    assert!(!std::path::Path::new(
        &service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves")
    )
    .join("hook-marker.txt")
    .exists());

    std::fs::write(repo_dir.path().join("hook-marker.txt"), "updated\n").expect("marker writes");
    run_git(repo_dir.path(), &["add", "-A"]);
    run_git(repo_dir.path(), &["commit", "-m", "add hook marker"]);

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::UpdateWorkspaceAndRetryHook,
            None,
            None,
        )
        .await
        .expect("update workspace and retry hook recovers");

    assert_eq!(recovered.status, "in_progress");
    assert_eq!(recovered.entry_barrier_json, None);
    assert_eq!(recovered.error_annotation, None);
    assert!(std::path::Path::new(
        &service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves")
    )
    .join("hook-marker.txt")
    .exists());
}

#[tokio::test]
async fn skip_hook_once_bypasses_only_one_dispatch_attempt() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "test -f hook-marker.txt",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let planner_id = seed_agent(&db).await;
    let coder_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::PLANNER,
                Some(planner_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("planner role assignment succeeds");
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(coder_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads after the role assignment")
        .expect("task exists");

    service
        .transition(task.id.clone(), "planning".to_owned(), task.version)
        .await
        .expect("blocking hook failure records a blocked entry");
    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(blocked.entry_barrier_json.is_some());

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::SkipHookOnce,
            None,
            None,
        )
        .await
        .expect("skip hook once recovers");
    assert_eq!(recovered.status, "planning");
    assert_eq!(recovered.entry_barrier_json, None);
    assert_eq!(recovered.error_annotation, None);

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].agent_id.as_deref(),
        Some(planner_id.as_str())
    );

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let transitioned = service
        .transition(
            task.id.clone(),
            "planning".to_owned(),
            (current.version, None, true),
        )
        .await
        .expect("second transition runs hook normally");
    assert_eq!(transitioned.task.status, "planning");
    let blocked_again = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(blocked_again.entry_barrier_json.is_some());
    let annotation: serde_json::Value = serde_json::from_str(
        blocked_again
            .error_annotation
            .as_deref()
            .expect("blocking annotation is recorded"),
    )
    .expect("annotation parses");
    assert_eq!(annotation["type"], "before_work_hook_failed");
}

#[tokio::test]
async fn dispatch_initial_role_execution_runs_reviewer_when_agent_is_busy_on_same_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;

    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::REVIEWER,
            Some(agent_id.clone()),
            None,
        ),
    )
    .await
    .expect("reviewer assignment created");

    // The same agent already holds this Task's executor attempt, which is
    // also the candidate the review is bound to. A reviewer execution is
    // only admissible against that exact candidate, so build the review-bound
    // admission the workflow dispatcher builds.
    let candidate = seed_completed_coder_execution(&db, &task, &agent_id, None).await;
    let now = now_rfc3339();
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running review creates");
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let agent = AgentRepo::get_by_id(&*db, &agent_id)
        .await
        .expect("agent loads")
        .expect("agent exists");
    let mut admission = crate::task_service::execution_admission_for_task(
        &db,
        &task,
        &project.workflow_definition,
        crate::workflow::default_roles::REVIEWER,
        Some(&agent),
        project.version,
    )
    .await
    .expect("reviewer admission builds");
    admission.expected_reviewer_parent_execution_id = Some(candidate.id.clone());
    admission.expected_latest_review_candidate_execution_id = Some(candidate.id.clone());
    admission.expected_reviewer_id = Some(review.id.clone());
    admission.expected_reviewer_attempt_number = Some(review.attempt_number);
    admission.expected_reviewer_status = Some(review.status.to_string());
    admission.expected_reviewer_updated_at = Some(review.updated_at.clone());

    let execution = service
        .dispatch_initial_role_execution_with_metadata_and_admission(
            &task.id,
            &agent_id,
            crate::workflow::default_roles::REVIEWER,
            "review the task".to_owned(),
            None,
            admission,
        )
        .await
        .expect("reviewer dispatch succeeds");

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let current = ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .expect("execution loads")
                .expect("execution exists");
            if current.status == ExecutionStatus::Completed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("reviewer execution completes");
}

#[tokio::test]
async fn failed_reviewer_execution_keeps_review_running_while_retry_waits() {
    assert_failed_reviewer_disposition(0, 3, true, false).await;
}

#[tokio::test]
async fn failed_reviewer_execution_blocks_when_retries_are_exhausted() {
    assert_failed_reviewer_disposition(3, 3, false, false).await;
}

#[tokio::test]
async fn failed_reviewer_execution_blocks_when_retries_are_disabled() {
    assert_failed_reviewer_disposition(0, 0, false, false).await;
}

#[tokio::test]
async fn precontract_reviewer_pass_uses_bounded_protocol_failure_dispositions() {
    for (count, budget, retry) in [(0, 3, true), (3, 3, false), (0, 0, false)] {
        assert_failed_reviewer_disposition(count, budget, retry, true).await;
    }
}

#[tokio::test]
async fn settled_reviewer_outcome_reconciles_a_missed_task_cascade() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("review completed".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("reviewer execution creates");
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    ReviewRepo::update_status(
        &*db,
        &review.id,
        ReviewStatus::Failed,
        json!({ "ci_steps": [], "auditor": { "verdict": "fail" } }).to_string(),
        Some(now.clone()),
        &now,
    )
    .await
    .expect("review outcome commits");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("missed task cascade reconciles");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, crate::workflow::default_states::IN_PROGRESS);
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Failed);
}

#[tokio::test]
async fn reviewer_completion_cascade_waits_for_an_in_flight_task_cascade() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    let now = now_rfc3339();
    let candidate = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("candidate completed".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("candidate execution creates");
    let reviewer = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: Some(candidate.id.clone()),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("review completed".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("reviewer execution creates");
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&reviewer.id)
        .bind(&review.id)
        .execute(db.pool())
        .await
        .expect("reviewer attempt binding records");

    ReviewRepo::update_status(
        &*db,
        &review.id,
        ReviewStatus::Failed,
        json!({ "ci_steps": [], "auditor": { "verdict": "fail" } }).to_string(),
        Some(now.clone()),
        &now,
    )
    .await
    .expect("review outcome commits");

    // A successor completion may arrive while the prior role's inline
    // cascade is still transitioning this Task. It must wait for the Task
    // slot and retry automatically, not depend on a later dispatcher scan.
    let in_flight = service
        .claim_completion_cascade(&task.id)
        .expect("first claim succeeds");
    assert!(service.claim_completion_cascade(&task.id).is_none());
    let waiting_service = service.clone();
    let reviewer_id = reviewer.id.clone();
    let waiting = tokio::spawn(async move {
        waiting_service
            .maybe_cascade_executor_completion(&reviewer_id)
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !waiting.is_finished(),
        "successor completion waits while the Task slot is held"
    );
    let untouched = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(untouched.status, "review");
    assert_eq!(untouched.version, task.version);

    drop(in_flight);
    tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
        .await
        .expect("queued completion wakes after the Task slot frees")
        .expect("queued completion task joins")
        .expect("queued completion settles");
    let settled = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(settled.status, crate::workflow::default_states::IN_PROGRESS);
}

#[tokio::test]
async fn duplicate_parent_bound_reviewer_delivery_reconciles_without_new_review_attempt() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    let now = now_rfc3339();
    let candidate = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("candidate completed".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("candidate execution creates");
    let reviewer = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: Some(candidate.id.clone()),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("review completed".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("reviewer execution creates");
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&reviewer.id)
        .bind(&review.id)
        .execute(db.pool())
        .await
        .expect("reviewer attempt binding records");
    ReviewRepo::update_status(
        &*db,
        &review.id,
        ReviewStatus::Passed,
        json!({ "ci_steps": [], "auditor": { "verdict": "pass" } }).to_string(),
        Some(now.clone()),
        &now,
    )
    .await
    .expect("review outcome commits");

    service
        .maybe_cascade_executor_completion(&reviewer.id)
        .await
        .expect("parent-bound settled review reconciles");
    service
        .maybe_cascade_executor_completion(&reviewer.id)
        .await
        .expect("duplicate parent-bound delivery is idempotent");

    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1, "duplicate delivery created a new attempt");
    assert_eq!(reviews[0].id, review.id);
    assert_eq!(reviews[0].execution_id, candidate.id);
}

#[tokio::test]
async fn old_reviewer_cannot_settle_newer_review_sharing_its_candidate() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    let now = now_rfc3339();
    let agent_id = seed_agent(&db).await;
    let candidate = seed_completed_coder_execution(&db, &task, &agent_id, None).await;

    let old_reviewer = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: Some(candidate.id.clone()),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("old reviewer".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("old reviewer creates");
    let new_reviewer = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: Some(candidate.id.clone()),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("new reviewer".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("new reviewer creates");
    let first_review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("first review creates");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&old_reviewer.id)
        .bind(&first_review.id)
        .execute(db.pool())
        .await
        .expect("first review binding records");
    let second_review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 2,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("second review creates");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&new_reviewer.id)
        .bind(&second_review.id)
        .execute(db.pool())
        .await
        .expect("second review binding records");

    service
        .maybe_cascade_executor_completion(&old_reviewer.id)
        .await
        .expect("superseded reviewer delivery is ignored");

    let current_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(current_task.status, crate::workflow::default_states::REVIEW);
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews reload");
    assert_eq!(reviews.len(), 2);
    assert!(reviews
        .iter()
        .all(|review| review.status == ReviewStatus::Running));
}

#[tokio::test]
async fn unbound_legacy_reviewer_cannot_settle_a_newer_review() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    let candidate = seed_completed_coder_execution(&db, &task, &agent_id, None).await;
    let review_started_at = "2026-01-01T00:00:00Z";
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 2,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: review_started_at.to_owned(),
            created_at: review_started_at.to_owned(),
            updated_at: review_started_at.to_owned(),
        },
    )
    .await
    .expect("new review creates");

    // This reviewer has no direct Review binding and no candidate parent. Its
    // later timestamp is deliberately chosen so timestamp-only legacy repair
    // would mistake it for the current attempt.
    let reviewer = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some("2026-01-01T00:00:02Z".to_owned()),
            parent_execution_id: None,
            agent_session_id: Some("legacy-reviewer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("legacy reviewer completion".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: "2026-01-01T00:00:01Z".to_owned(),
            updated_at: "2026-01-01T00:00:02Z".to_owned(),
        },
    )
    .await
    .expect("legacy reviewer execution creates");

    service
        .maybe_cascade_executor_completion(&reviewer.id)
        .await
        .expect("unbound reviewer completion is ignored");

    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].id, review.id);
    assert_eq!(reviews[0].status, ReviewStatus::Running);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, crate::workflow::default_states::REVIEW);
}

/// A Task in review with a running reviewer execution whose contract is
/// admitted, ready for the reviewer's final reply.
async fn seed_admitted_review() -> (Arc<SqliteDb>, TaskService, Task, db::Execution, TempDir) {
    seed_admitted_review_with(r#"{"retry_budgets":{"execution":3,"review":3}}"#).await
}

async fn seed_admitted_review_with(
    task_state_config: &str,
) -> (Arc<SqliteDb>, TaskService, Task, db::Execution, TempDir) {
    // `run_ci_steps` records each configured step before the contract admits.
    let pre_review_ci_steps: Vec<Value> = serde_json::from_str::<Value>(task_state_config)
        .expect("task state config parses")
        .pointer("/review/ci_steps")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(index, command)| {
            json!({"index": index, "command": command, "exit_code": 0, "stderr_tail": ""})
        })
        .collect();
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    sqlx::query("UPDATE task SET task_state_config = ?, metadata_json = ? WHERE id = ?")
        .bind(task_state_config)
        .bind(r#"{"execution_retry_count":0}"#)
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("retry policy sets");

    let now = now_rfc3339();
    let workspace = WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: repo_dir.path().to_string_lossy().into_owned(),
            branch: "review-candidate".to_owned(),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review workspace creates");
    let candidate = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("candidate implementation".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace.id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("candidate execution creates");
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: Some(candidate.id.clone()),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: Some(workspace.id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("running reviewer execution creates");
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id,
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": pre_review_ci_steps }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("running review creates");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&execution.id)
        .bind(&review.id)
        .execute(db.pool())
        .await
        .expect("reviewer execution binds");

    ::review::contract::admit(&db, &execution.id, &task.id, repo_dir.path())
        .await
        .expect("review contract admits");
    (db, service, task, execution, repo_dir)
}

#[tokio::test]
async fn daemon_placement_reviewer_completion_evaluates_through_owner() {
    use crate::workspace_backend::DaemonWorkspaceBackend;
    let (db, service, task, execution, repo_dir) = seed_admitted_review().await;
    let (_, owner, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    let workspace_id = execution.workspace_id.as_deref().unwrap();
    let repo_id: String = sqlx::query_scalar("SELECT repo_id FROM workspace WHERE id = ?")
        .bind(workspace_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    let location = db::RepoLocationRepo::create(
        &*db,
        db::CreateRepoLocation {
            id: new_uuid_v4(),
            repo_id,
            owner_kind: db::RepoLocationOwnerKind::Daemon,
            daemon_id: owner.daemon_id.clone(),
            runtime_id: owner.runtime_id.clone(),
            path: "/owner-only/repo".into(),
            kind: db::RepoLocationKind::PrimaryCheckout,
            is_default: true,
            status: db::RepoLocationStatus::Ready,
            last_verified_at: Some(now.clone()),
            last_error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::WorkspacePlacementRepo::create(
        &*db,
        db::CreateWorkspacePlacement {
            id: new_uuid_v4(),
            workspace_id: workspace_id.into(),
            task_id: task.id.clone(),
            agent_id: None,
            owner_kind: db::PlacementOwnerKind::Daemon,
            daemon_id: owner.daemon_id.clone(),
            runtime_id: owner.runtime_id,
            repo_location_id: location.id,
            execution_daemon_id: owner.daemon_id.clone(),
            workspace_handle: Some("opaque-review-owner".into()),
            generation: 1,
            state: db::PlacementState::Ready,
            selected_by: db::PlacementSelectedBy::Scheduler,
            selection_reason: "{}".into(),
            reserved_until: None,
            disconnected_at: None,
            failure_cause: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let daemon_id = owner.daemon_id.unwrap();
    let (connection_id, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    let sha = git::get_current_sha(repo_dir.path()).await.unwrap();
    let evidence_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let responder = {
        let registry = registry.clone();
        let evidence_reads = evidence_reads.clone();
        tokio::spawn(async move {
            while let Some(api_types::DaemonFrame::Request { id, method, params }) =
                outbound.recv().await
            {
                assert_eq!(params["workspace_handle"], "opaque-review-owner");
                let result = match method.as_str() {
                    api_types::METHOD_WORKSPACE_DESCRIBE => {
                        json!({"workspace_handle": "opaque-review-owner", "generation": 1,
                        "exists": true, "head_sha": sha, "dirty": false, "branch": "review-candidate", "locked": false,
                        "active_execution_ids": [], "journaled_execution_ids": []})
                    }
                    api_types::METHOD_WORKSPACE_READ => {
                        evidence_reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        match params["query"]["kind"].as_str().unwrap() {
                            "head" | "resolve_ref" | "merge_base" => {
                                json!({"kind": "git", "output": format!("{sha}\n")})
                            }
                            "tracked_changes" | "candidate_paths" => {
                                json!({"kind": "git", "output": ""})
                            }
                            query => panic!("unexpected conformance query: {query}"),
                        }
                    }
                    method => panic!("unexpected reviewer owner operation: {method}"),
                };
                registry.dispatch_incoming_for_connection(
                    &daemon_id,
                    connection_id,
                    api_types::DaemonFrame::Response { id, result },
                );
            }
        })
    };
    let router = (*service.workspace_backend_router())
        .clone()
        .with_daemon(Arc::new(DaemonWorkspaceBackend::new(db.clone(), registry)));
    let service = service.with_workspace_backend_router(Arc::new(router));
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        complete_review_with(
            &db,
            &service,
            &execution,
            r#"{"result":"pass","reason":"acceptance satisfied"}"#,
        ),
    )
    .await
    .unwrap();
    responder.abort();
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(review.status, ReviewStatus::Passed);
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Passed
    );
    assert!(evidence_reads.load(std::sync::atomic::Ordering::SeqCst) >= 2);
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "merging"
    );
}

/// Complete the reviewer execution with `reply` and run the review cascade.
async fn complete_review_with(
    db: &SqliteDb,
    service: &TaskService,
    execution: &db::Execution,
    reply: &str,
) {
    sqlx::query(
        "UPDATE execution
         SET status = 'completed', summary = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(reply)
    .bind(now_rfc3339())
    .bind(&execution.id)
    .execute(db.pool())
    .await
    .expect("reviewer execution completes");
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("completed review cascades");
}

#[tokio::test]
async fn timed_out_review_checks_rerun_alone_then_park_without_a_new_reviewer() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review_with(
        r#"{"retry_budgets":{"execution":3,"review":3},"review":{"ci_steps":["sleep 3"],"check_timeout_seconds":1}}"#,
    )
    .await;
    // Live case (NK-28): the reviewer passed, but Forge's own clean-checkout
    // `cargo test` outran the limit while a sibling held the build lock, and
    // every timeout dispatched a whole new reviewer.
    complete_review_with(
        &db,
        &service,
        &execution,
        "All criteria met.\n\n{\"result\": \"pass\", \"reason\": \"criteria met\"}",
    )
    .await;

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, crate::workflow::default_states::REVIEW);
    let annotation: api_types::TaskBlockingAnnotation = serde_json::from_str(
        current
            .error_annotation
            .as_deref()
            .expect("the Task is parked for its owner"),
    )
    .expect("annotation parses");
    assert_eq!(
        annotation.annotation_type,
        api_types::FailureKind::ReviewBlocked
    );
    let message = annotation.message.unwrap_or_default();
    assert!(
        message.contains("`sleep 3` ran longer than 1s"),
        "{message}"
    );
    assert!(message.contains("3 attempts"), "{message}");

    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load")
        .into_iter()
        .next()
        .expect("review exists");
    assert!(
        !review.step_results_json.contains("execution_retry"),
        "no replacement reviewer is scheduled: {}",
        review.step_results_json
    );
    assert!(
        db::ReviewConformanceRepo::review_conformance(&*db, &execution.id)
            .await
            .expect("conformance loads")
            .is_none()
    );

    // Duplicate terminal delivery is inert: the checks do not run again.
    let started = std::time::Instant::now();
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("duplicate delivery is inert");
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}

#[tokio::test]
async fn reviewer_conformance_recovers_deleted_worktree_before_checks() {
    let (db, service, task, execution, repo_dir) = seed_admitted_review_with(
        r#"{"retry_budgets":{"execution":3,"review":3},"review":{"ci_steps":["test -f README.md"]}}"#,
    ).await;
    let workspace_root = TempDir::new().unwrap();
    let service = service.with_workspace_root(workspace_root.path().to_path_buf());
    let workspace = WorkspaceRepo::get_by_id(&*db, execution.workspace_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let worktree = ::workspace::WorkspaceManager::new(workspace_root.path().to_path_buf())
        .create_worktree_named(repo_dir.path().to_str().unwrap(), &task.id, "forge", "main")
        .await
        .unwrap();
    sqlx::query("UPDATE workspace SET worktree_path = ?, branch = ? WHERE id = ?")
        .bind(worktree.to_string_lossy().as_ref())
        .bind(::workspace::task_branch_name(&task.id))
        .bind(&workspace.id)
        .execute(db.pool())
        .await
        .unwrap();
    let sha = git::get_current_sha(&worktree).await.unwrap();
    std::fs::remove_dir_all(&worktree).unwrap();
    run_git(repo_dir.path(), &["worktree", "prune"]);

    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"pass","reason":"acceptance satisfied"}"#,
    )
    .await;
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(review.status, ReviewStatus::Passed);
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Passed
    );
    assert_eq!(git::get_current_sha(&worktree).await.unwrap(), sha);
    let recovered = WorkspaceRepo::get_by_task_id(&*db, &task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.id, workspace.id);
    assert_eq!(recovered.branch, ::workspace::task_branch_name(&task.id));
}

#[tokio::test]
async fn a_reply_without_a_result_uses_bounded_reviewer_execution_retry() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    // A reply with no result block is unusable: Forge cannot tell pass from
    // fail, so neither the coder nor the Task's authority may act on it.
    complete_review_with(
        &db,
        &service,
        &execution,
        "I looked at the change and it seems mostly fine.",
    )
    .await;

    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load")
        .into_iter()
        .next()
        .expect("review exists");
    assert_eq!(
        review.status,
        ReviewStatus::Running,
        "an unusable reply keeps the review on the bounded reviewer retry path"
    );
    assert!(review.finished_at.is_none());
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).expect("review details parse");
    let details_json: serde_json::Value =
        serde_json::from_str(&review.step_results_json).expect("review details json parses");
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Unverified
    );
    assert!(details.conformance.assessment.is_none());
    assert_eq!(
        details_json["execution_retry"]["execution_id"],
        execution.id
    );
    assert_eq!(details_json["execution_retry"]["status"], "scheduled");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(
        current.status,
        crate::workflow::default_states::REVIEW,
        "an unusable reply must not route the coder into remediation"
    );
    assert!(current.blocked_json.is_none());
    assert!(current.error_annotation.is_none());
    let metadata: serde_json::Value =
        serde_json::from_str(current.metadata_json.as_deref().unwrap_or("{}"))
            .expect("metadata parses");
    assert_eq!(
        metadata
            .get("execution_retry_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        1
    );
    assert!(metadata.get("deferred_dispatch").is_some());

    let completed_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(
        completed_execution.resume_policy,
        Some(db::ResumePolicy::Auto)
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("coder executions load"),
        1,
        "unverified reviewer evidence must never dispatch coder remediation \
         (the one coder execution is the seeded review candidate)"
    );
}

#[tokio::test]
async fn a_blocked_review_parks_the_task_for_its_owner_without_the_coder() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    // Live case: the reviewer had no `tsc` or `tauri` and a read-only cargo
    // home. Neither the coder nor another reviewer can fix that.
    complete_review_with(
        &db,
        &service,
        &execution,
        "`npm run build` exits 127 (`tsc: not found`).\n\n\
         {\"result\": \"blocked\", \"reason\": \"tsc and tauri are not installed\"}",
    )
    .await;

    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load")
        .into_iter()
        .next()
        .expect("review exists");
    assert_eq!(review.status, ReviewStatus::Failed);
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).expect("review details parse");
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Blocked
    );
    let assessment = details.conformance.assessment.expect("review is kept");
    assert_eq!(
        assessment.report,
        "`npm run build` exits 127 (`tsc: not found`)."
    );

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(
        current.status,
        crate::workflow::default_states::REVIEW,
        "a blocked review must not route the coder into remediation"
    );
    let annotation: api_types::TaskBlockingAnnotation = serde_json::from_str(
        current
            .error_annotation
            .as_deref()
            .expect("the Task is parked for its owner"),
    )
    .expect("annotation parses");
    assert_eq!(
        annotation.annotation_type,
        api_types::FailureKind::ReviewBlocked
    );
    assert!(annotation
        .message
        .as_deref()
        .is_some_and(|message| message.contains("tsc and tauri are not installed")));
    assert!(annotation
        .recovery_actions
        .contains(&api_types::RecoveryAction::Reexecute));
    assert!(annotation
        .recovery_actions
        .contains(&api_types::RecoveryAction::MarkReviewed));

    let missing_reason = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::MarkReviewed,
            None,
            None,
        )
        .await
        .expect_err("manual pass requires a reason");
    assert!(missing_reason
        .to_string()
        .contains("requires a recovery reason"));

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::MarkReviewed,
            Some("Provider check was verified manually".to_owned()),
            None,
        )
        .await
        .expect("manual review pass succeeds");
    assert_ne!(recovered.status, crate::workflow::default_states::REVIEW);
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews reload");
    assert_eq!(reviews.len(), 2);
    assert_eq!(reviews[0].id, review.id);
    assert_eq!(reviews[0].status, ReviewStatus::Failed);
    assert_eq!(reviews[0].step_results_json, review.step_results_json);
    assert_eq!(reviews[1].status, ReviewStatus::Passed);
    let override_details: serde_json::Value =
        serde_json::from_str(&reviews[1].step_results_json).expect("override details parse");
    assert_eq!(
        override_details["manual_override"]["reason"],
        "Provider check was verified manually"
    );
    assert_eq!(override_details["manual_override"]["actor_type"], "user");
    assert_eq!(
        override_details["manual_override"]["source_review_id"],
        review.id
    );

    let comments = TaskCommentRepo::list_comments(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await
    .expect("comments reload");
    assert!(comments.items.iter().any(|comment| {
        comment
            .content
            .contains("Review passed manually (attempt 2): Provider check was verified manually")
    }));
    let transitions = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transitions reload");
    let manual_transition = transitions
        .iter()
        .find(|transition| transition.from_state == crate::workflow::default_states::REVIEW)
        .expect("manual pass transition is audited");
    assert_eq!(
        manual_transition.trigger_reason,
        "Provider check was verified manually"
    );
    assert!(manual_transition.triggered_by.starts_with("user:"));
}

/// Insert the preceding attempt without changing the current execution binding.
async fn seed_previous_review_attempt(db: &SqliteDb, task_id: &str, status: ReviewStatus) {
    let failed = status == ReviewStatus::Failed;
    let current = ReviewRepo::list_by_task(db, task_id)
        .await
        .unwrap()
        .remove(0);
    sqlx::query("UPDATE review SET attempt_number = 2 WHERE id = ?")
        .bind(&current.id)
        .execute(db.pool())
        .await
        .unwrap();
    ReviewRepo::create(
        db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            execution_id: current.execution_id,
            attempt_number: 1,
            status,
            step_results_json: json!({"conformance": {"status":"failed", "contract":null,
                "checks":[], "reason":"previous finding", "assessment":{"result":"fail", "reason":"previous finding"}}}).to_string(),
            started_at: current.started_at,
            created_at: current.created_at,
            updated_at: current.updated_at,
        },
    )
    .await
    .unwrap();
    if failed {
        TransitionLogRepo::insert(
            db,
            db::CreateTransitionLog {
                id: new_uuid_v4(),
                task_id: task_id.to_owned(),
                from_state: crate::workflow::default_states::REVIEW.to_owned(),
                to_state: crate::workflow::default_states::IN_PROGRESS.to_owned(),
                trigger_name: Some("reject".to_owned()),
                triggered_by: "system:workflow".to_owned(),
                trigger_reason: "previous review failed".to_owned(),
                hook_results_json: None,
                rejection: true,
                created_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
    }
}

async fn assert_owner_review_park(db: &SqliteDb, task_id: &str, message: &str) {
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, crate::workflow::default_states::REVIEW);
    assert!(task.review_passed_at.is_none());
    let annotation: api_types::TaskBlockingAnnotation =
        serde_json::from_str(task.error_annotation.as_deref().expect("owner annotation")).unwrap();
    assert_eq!(
        annotation.annotation_type,
        api_types::FailureKind::ReviewNeedsOwner
    );
    assert_eq!(annotation.blocking_reason, "review_needs_owner");
    assert_eq!(annotation.message.as_deref(), Some(message));
    assert_eq!(
        annotation.recovery_actions,
        vec![
            api_types::RecoveryAction::Reexecute,
            api_types::RecoveryAction::MarkReviewed,
            api_types::RecoveryAction::DeferToFollowUp,
            api_types::RecoveryAction::OpenInteractive,
            api_types::RecoveryAction::CancelTask,
        ]
    );
    let blocked: Value = serde_json::from_str(task.blocked_json.as_deref().unwrap()).unwrap();
    assert_eq!(blocked["kind"], "review_needs_owner");
    assert!(task.failed_json.is_none());
    let reviews = ReviewRepo::list_by_task(db, task_id).await.unwrap();
    let latest = reviews
        .iter()
        .max_by_key(|review| review.attempt_number)
        .unwrap();
    assert_eq!(latest.status, ReviewStatus::Failed);
    assert!(latest.finished_at.is_some());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(db, task_id, crate::workflow::default_roles::CODER,)
            .await
            .unwrap(),
        1,
        "only the seeded candidate coder execution exists"
    );
    let exception = crate::task_diagnostics::derive_workflow_exception(
        &task,
        &crate::workflow::default_workflow::default_workflow(),
        &[],
        Some(latest),
        None,
        &std::collections::HashMap::new(),
    )
    .expect("owner exception");
    let defer = exception
        .actions
        .iter()
        .find(|action| action.kind == api_types::RecoveryAction::DeferToFollowUp)
        .expect("defer action");
    assert_eq!(defer.label, "Defer to Follow-up Task");
    assert!(defer.enabled && defer.requires_reason && defer.propagates);
}

#[tokio::test]
async fn review_finding_routing_owner_parks_without_spending_budget() {
    let (db, service, task, execution, _repo_dir) =
        seed_admitted_review_with(r#"{"retry_budgets":{"execution":3,"review":1}}"#).await;
    let before = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Forge linked_documents is empty","fixable_by":"owner"}"#,
    )
    .await;
    assert_owner_review_park(
        &db,
        &task.id,
        "fixable by owner: Forge linked_documents is empty",
    )
    .await;
    let after = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    assert_eq!(
        before.len(),
        after.len(),
        "parking creates no rejection transition"
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let metadata: Value = serde_json::from_str(current.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(metadata["execution_retry_count"], 0);
    let version = current.version;
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .unwrap();
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap()
            .version,
        version,
        "duplicate completion leaves the owner park unchanged"
    );
}

#[tokio::test]
async fn review_finding_routing_repeat_after_failed_attempt_parks() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    seed_previous_review_attempt(&db, &task.id, ReviewStatus::Failed).await;
    let before = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Cross-platform measurements remain missing","repeat":true}"#,
    )
    .await;
    assert_owner_review_park(
        &db,
        &task.id,
        "repeated finding: Cross-platform measurements remain missing",
    )
    .await;
    let after = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    assert_eq!(
        before.len(),
        after.len(),
        "repeat park spends no review budget"
    );
    assert_eq!(
        crate::task_diagnostics::count_gate_rejections_since_boundary(
            &after,
            crate::workflow::default_states::REVIEW,
        ),
        1,
        "only the preceding failure spent budget"
    );
}

#[tokio::test]
async fn review_finding_routing_repeat_requires_previous_reviewer_finding() {
    for details in [
        json!({"auditor":{"verdict":"fail", "reason":"reviewer crashed"}}),
        json!({"ci_steps":[{"exit_code":1}]}),
        json!({"conformance":{"status":"unverified", "contract":null, "checks":[], "reason":"no result"}}),
        json!({"conformance":{"status":"failed", "contract":null,
            "checks":[{"check_id":"ci", "command":"false", "exit_code":1, "output":"failed"}],
            "reason":"CI failed", "assessment":{"result":"fail", "reason":"finding"}}}),
    ] {
        let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
        seed_previous_review_attempt(&db, &task.id, ReviewStatus::Failed).await;
        sqlx::query(
            "UPDATE review SET step_results_json = ? WHERE task_id = ? AND attempt_number = 1",
        )
        .bind(details.to_string())
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
        complete_review_with(
            &db,
            &service,
            &execution,
            r#"{"result":"fail","reason":"Null input crashes","repeat":true}"#,
        )
        .await;
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, crate::workflow::default_states::IN_PROGRESS);
        assert!(current.blocked_json.is_none());
    }
}

#[tokio::test]
async fn review_finding_routing_first_attempt_repeat_returns_to_coder() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Null input still crashes","repeat":true}"#,
    )
    .await;
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, crate::workflow::default_states::IN_PROGRESS);
    assert!(current.blocked_json.is_none());
    assert!(current.error_annotation.is_none());
    let transitions = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    assert_eq!(
        crate::task_diagnostics::count_gate_rejections_since_boundary(
            &transitions,
            crate::workflow::default_states::REVIEW,
        ),
        1
    );
}

#[tokio::test]
async fn review_finding_routing_repeat_after_cancelled_attempt_returns_to_coder() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    seed_previous_review_attempt(&db, &task.id, ReviewStatus::Cancelled).await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Null input crashes","repeat":true}"#,
    )
    .await;
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, crate::workflow::default_states::IN_PROGRESS);
    assert!(current.blocked_json.is_none());
}

#[tokio::test]
async fn review_finding_routing_legacy_fail_returns_to_coder() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Null input crashes"}"#,
    )
    .await;
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, crate::workflow::default_states::IN_PROGRESS);
    assert!(current.blocked_json.is_none());
}

#[tokio::test]
async fn review_finding_routing_forge_checks_override_owner_and_repeat() {
    for checks in [
        r#"{"review":{"setup_steps":["exit 1"]},"retry_budgets":{"review":3}}"#,
        r#"{"review":{"ci_steps":["exit 1"]},"retry_budgets":{"review":3}}"#,
    ] {
        let (db, service, task, execution, _repo_dir) = seed_admitted_review_with(checks).await;
        seed_previous_review_attempt(&db, &task.id, ReviewStatus::Failed).await;
        complete_review_with(&db, &service, &execution,
            r#"{"result":"fail","reason":"Owner needs external credentials","fixable_by":"owner","repeat":true}"#,
        ).await;
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, crate::workflow::default_states::IN_PROGRESS);
        assert!(current.blocked_json.is_none());
    }
}

#[tokio::test]
async fn review_finding_routing_settled_owner_failure_reconciles_to_park() {
    let (db, service, task, execution, repo_dir) = seed_admitted_review().await;
    let reply = r#"{"result":"fail","reason":"Hardware measurements needed","fixable_by":"owner"}"#;
    let conformance = ::review::contract::evaluate(&db, &execution.id, repo_dir.path(), reply)
        .await
        .unwrap();
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap()
        .remove(0);
    let now = now_rfc3339();
    ReviewRepo::update_status(
        &*db,
        &review.id,
        ReviewStatus::Failed,
        json!({"ci_steps":[], "conformance":conformance}).to_string(),
        Some(now.clone()),
        &now,
    )
    .await
    .unwrap();
    complete_review_with(&db, &service, &execution, reply).await;
    assert_owner_review_park(
        &db,
        &task.id,
        "fixable by owner: Hardware measurements needed",
    )
    .await;
}

#[tokio::test]
async fn review_finding_routing_owner_manual_pass_clears_the_park() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Hardware measurements needed","fixable_by":"owner"}"#,
    )
    .await;
    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::MarkReviewed,
            Some("Owner verified the hardware measurements".to_owned()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(recovered.status, crate::workflow::default_states::MERGING);
    assert!(recovered.review_passed_at.is_some());
    assert!(recovered.blocked_json.is_none() && recovered.error_annotation.is_none());
    assert_eq!(
        ReviewRepo::list_by_task(&*db, &task.id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .status,
        ReviewStatus::Passed
    );
}

#[tokio::test]
async fn review_finding_routing_defer_follow_up_is_charter_dispatchable() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Owner must collect measurements","fixable_by":"owner"}"#,
    )
    .await;
    let now = now_rfc3339();
    let owner = new_uuid_v4();
    let charter = new_uuid_v4();
    let revision = new_uuid_v4();
    sqlx::query("INSERT INTO user (id, email, password_hash, created_at, updated_at) VALUES (?, ?, 'unused', ?, ?)")
        .bind(&owner).bind(format!("{owner}@example.com")).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE project SET owner_id = ? WHERE id = ?")
        .bind(&owner)
        .bind(&task.project_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO project_charter (id, account_id, project_id, project_mode, maturity, lifecycle, created_at, updated_at)
                 VALUES (?, ?, ?, 'compact', 'prototype', 'attached', ?, ?)")
        .bind(&charter).bind(&owner).bind(&task.project_id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO project_charter_revision (id, charter_id, revision, lifecycle, schema_version, render_version,
                 content_json, rendered_view, author_type, source_refs_json, content_digest, rendered_digest, created_at)
                 VALUES (?, ?, 1, 'approved', 'forge.project-charter/v1', 'forge.project-charter-render/v1', '{}', '# Charter', 'user', '[]', 'content', 'render', ?)")
        .bind(&revision).bind(&charter).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(&revision)
        .bind(&charter)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE project SET current_charter_id = ?, current_charter_revision_id = ?,
                 current_charter_version = 1, charter_status = 'charter_backed', charter_setup_required = 0,
                 version = version + 1 WHERE id = ?")
        .bind(&charter).bind(&revision).bind(&task.project_id).execute(db.pool()).await.unwrap();
    let project = ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .unwrap()
        .unwrap();
    let governance = service
        .prepare_task_governance(&project, "task", None)
        .await
        .unwrap()
        .unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    service
        .insert_task_governance(&mut tx, &task.id, &project.id, governance, &now)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::DeferToFollowUp,
            Some("Schedule the measurements separately".to_owned()),
            None,
        )
        .await
        .unwrap();
    let follow_ups = TaskRepo::list_by_project_with_metadata_key(&*db, &project.id, "follow_up_of")
        .await
        .unwrap();
    assert_eq!(follow_ups.len(), 1);
    let follow_up = &follow_ups[0];
    service
        .ensure_task_runnable(follow_up)
        .await
        .expect("follow-up passes normal Charter dispatch admission");
    let current = ProjectRepo::get_by_id(&*db, &project.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.project_work_epoch, project.project_work_epoch + 1);
    let governance: (String, i64) = sqlx::query_as(
        "SELECT charter_revision_id, runnable FROM project_task_governance WHERE task_id = ?",
    )
    .bind(&follow_up.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(governance, (revision, 1));
}

#[tokio::test]
async fn review_finding_routing_defer_creates_linked_backlog_and_passes_review() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Required cross-platform measurements","fixable_by":"owner"}"#,
    )
    .await;
    let before = ReviewRepo::list_by_task(&*db, &task.id).await.unwrap();
    for reason in [None, Some("  \n ".to_owned())] {
        let error = service
            .recover_task(
                task.id.clone(),
                api_types::RecoveryAction::DeferToFollowUp,
                reason,
                None,
            )
            .await
            .expect_err("defer requires reason");
        assert!(error
            .to_string()
            .contains("defer_to_follow_up requires a recovery reason"));
    }
    let mut events = service.event_bus.subscribe();
    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::DeferToFollowUp,
            Some("macOS/Windows runs need a human".to_owned()),
            None,
        )
        .await
        .expect("defer succeeds");
    assert_eq!(recovered.status, crate::workflow::default_states::MERGING);
    assert!(recovered.review_passed_at.is_some());
    assert!(recovered.blocked_json.is_none() && recovered.error_annotation.is_none());
    let follow_ups =
        TaskRepo::list_by_project_with_metadata_key(&*db, &task.project_id, "follow_up_of")
            .await
            .unwrap();
    assert_eq!(follow_ups.len(), 1);
    let follow_up = &follow_ups[0];
    assert_eq!(follow_up.project_id, task.project_id);
    assert_eq!(follow_up.status, "backlog");
    assert_eq!(
        follow_up.title,
        format!(
            "Follow-up: {} — Required cross-platform measurements",
            task.title
        )
    );
    let metadata: Value =
        serde_json::from_str(follow_up.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(metadata["follow_up_of"], task.id);
    let description = follow_up.description.as_deref().unwrap();
    for expected in [
        task.id.as_str(),
        "Required cross-platform measurements",
        "macOS/Windows runs need a human",
    ] {
        assert!(description.contains(expected));
    }
    let reviews = ReviewRepo::list_by_task(&*db, &task.id).await.unwrap();
    assert_eq!(reviews.len(), 2);
    assert_eq!(reviews[0], before[0], "failed assessment remains intact");
    assert_eq!(reviews[1].status, ReviewStatus::Passed);
    let details: Value = serde_json::from_str(&reviews[1].step_results_json).unwrap();
    assert_eq!(details["manual_override"]["action"], "defer_to_follow_up");
    let pass_reason = details["manual_override"]["reason"].as_str().unwrap();
    assert!(pass_reason.contains(&follow_up.id) && pass_reason.contains(&follow_up.title));
    assert!(pass_reason.contains("macOS/Windows runs need a human"));
    let transitions = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    assert_eq!(
        crate::task_diagnostics::count_gate_rejections_since_boundary(
            &transitions,
            crate::workflow::default_states::REVIEW,
        ),
        0
    );
    let mut created = false;
    let mut updated = false;
    while let Ok(event) = events.try_recv() {
        created |= event.event_type == "task.created" && event.entity_id == follow_up.id;
        updated |= event.event_type == "task.updated" && event.entity_id == task.id;
    }
    assert!(
        created && updated,
        "created/updated events follow the atomic commit"
    );
    assert!(service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::DeferToFollowUp,
            Some("duplicate defer".to_owned()),
            None
        )
        .await
        .is_err());
    assert_eq!(
        TaskRepo::list_by_project_with_metadata_key(&*db, &task.project_id, "follow_up_of")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn review_finding_routing_defer_uses_workflow_backlog_and_bounds_unicode_title() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    let first_line = "测".repeat(100);
    let finding = format!("{first_line}\nSecond line of the parked finding");
    complete_review_with(
        &db,
        &service,
        &execution,
        &json!({
            "result": "fail", "reason": finding, "fixable_by": "owner",
        })
        .to_string(),
    )
    .await;
    let project = ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .unwrap()
        .unwrap();
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    for state in &mut workflow.states {
        if state.name == "backlog" {
            state.name = "waiting".to_owned();
        }
        for trigger in state.triggers.values_mut() {
            if trigger.to == "backlog" {
                trigger.to = "waiting".to_owned();
            }
        }
    }
    sqlx::query("UPDATE project SET workflow_definition = ?, version = version + 1 WHERE id = ?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&project.id)
        .execute(db.pool())
        .await
        .unwrap();
    service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::DeferToFollowUp,
            Some("Collect hardware evidence separately".to_owned()),
            None,
        )
        .await
        .unwrap();
    let follow_ups =
        TaskRepo::list_by_project_with_metadata_key(&*db, &task.project_id, "follow_up_of")
            .await
            .unwrap();
    assert_eq!(follow_ups.len(), 1);
    assert_eq!(follow_ups[0].status, "waiting");
    assert_eq!(
        follow_ups[0].title,
        format!("Follow-up: {} — {}…", task.title, "测".repeat(80))
    );
    assert!(follow_ups[0]
        .description
        .as_deref()
        .unwrap()
        .contains(&finding));
}

#[tokio::test]
async fn review_finding_routing_defer_rolls_back_follow_up_when_manual_pass_fails() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Hardware measurements needed","fixable_by":"owner"}"#,
    )
    .await;
    let before = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    // Invalidate the candidate so the authority check fails after follow-up insertion.
    sqlx::query("UPDATE execution SET status = 'failed' WHERE id = ?")
        .bind(execution.parent_execution_id.as_deref().unwrap())
        .execute(db.pool())
        .await
        .unwrap();
    let mut events = service.event_bus.subscribe();
    service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::DeferToFollowUp,
            Some("human follow-up required".to_owned()),
            None,
        )
        .await
        .expect_err("invalid candidate rejects the manual pass");
    assert!(
        TaskRepo::list_by_project_with_metadata_key(&*db, &task.project_id, "follow_up_of")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    let reviews = ReviewRepo::list_by_task(&*db, &task.id).await.unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Failed);
    assert!(
        events.try_recv().is_err(),
        "rolled-back mutations publish no events"
    );
}

async fn assert_failed_reviewer_disposition(
    retry_count: u64,
    budget: u64,
    should_retry: bool,
    protocol: bool,
) {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut events = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        Some(&agent_id),
    )
    .await;
    sqlx::query("UPDATE task SET task_state_config = ?, metadata_json = ? WHERE id = ?")
        .bind(json!({ "retry_budgets": { "execution": budget } }).to_string())
        .bind(json!({ "execution_retry_count": retry_count }).to_string())
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("retry policy sets");
    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: if protocol {
                ExecutionStatus::Completed
            } else {
                ExecutionStatus::Failed
            },
            stop_reason: Some(db::StopReason::ExecutorFailed),
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: Some(db::ResumePolicy::Manual),
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: Some("reviewer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some(
                if protocol {
                    "===REVIEW: PASS==="
                } else {
                    "reviewer quit before verdict"
                }
                .to_owned(),
            ),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("claude-code exited with status exit status: 1".to_owned()),
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("failed reviewer execution creates");
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running review creates");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("reviewer failure cascade succeeds");

    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(
        reviews[0].status,
        if should_retry {
            ReviewStatus::Running
        } else {
            ReviewStatus::Failed
        }
    );
    assert_eq!(reviews[0].finished_at.is_some(), !should_retry);
    let details: serde_json::Value =
        serde_json::from_str(&reviews[0].step_results_json).expect("details parse");
    assert_eq!(details["auditor"]["verdict"], "fail");
    assert_eq!(
        details["execution"]["error"],
        if protocol {
            "review execution has no workspace evidence"
        } else {
            "claude-code exited with status exit status: 1"
        }
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "review");
    let current_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    let metadata: serde_json::Value =
        serde_json::from_str(current.metadata_json.as_deref().unwrap_or("{}"))
            .expect("metadata parses");
    if should_retry {
        assert_eq!(details["execution_retry"]["execution_id"], execution.id);
        assert_eq!(details["execution_retry"]["status"], "scheduled");
        assert_eq!(
            current_execution.resume_policy,
            Some(db::ResumePolicy::Auto)
        );
        assert!(current.blocked_json.is_none());
        assert!(current.error_annotation.is_none());
        assert_eq!(metadata["execution_retry_count"], retry_count + 1);
        assert!(metadata.get("deferred_dispatch").is_some());
    } else {
        assert_eq!(
            current_execution.resume_policy,
            Some(db::ResumePolicy::Manual)
        );
        let blocked: serde_json::Value = serde_json::from_str(
            current
                .blocked_json
                .as_deref()
                .expect("task has a durable blocker"),
        )
        .expect("blocker parses");
        assert_eq!(blocked["kind"], "internal_command_failed");
        assert_eq!(blocked["execution_id"], execution.id);
        let annotation: api_types::TaskBlockingAnnotation = serde_json::from_str(
            current
                .error_annotation
                .as_deref()
                .expect("recovery annotation exists"),
        )
        .expect("annotation parses");
        assert_eq!(
            annotation.annotation_type,
            api_types::FailureKind::ExecutorFailed
        );
        assert_eq!(
            annotation.blocked_execution_id.as_deref(),
            Some(execution.id.as_str())
        );
        assert!(annotation
            .recovery_actions
            .contains(&api_types::RecoveryAction::Reexecute));
        assert!(!annotation
            .recovery_actions
            .contains(&api_types::RecoveryAction::ResumeSession));
        assert_eq!(metadata["execution_retry_count"], retry_count);
        assert!(metadata.get("deferred_dispatch").is_none());
        assert!(current.failed_json.is_none());
    }

    // Repeated completion delivery must not emit another blocker or spend budget.
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("duplicate completion succeeds");
    let repeated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(repeated.version, current.version);
    assert_eq!(repeated.blocked_json, current.blocked_json);
    assert_eq!(repeated.metadata_json, current.metadata_json);
    let mut blocker_events = 0;
    while let Ok(event) = events.try_recv() {
        if event.event_type == "task.blocked" && event.entity_id == task.id {
            blocker_events += 1;
        }
    }
    assert_eq!(blocker_events, usize::from(!should_retry));
}

#[tokio::test]
async fn human_required_review_can_be_rejected_by_the_bound_project_agent() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::REVIEW)
        .and_then(|state| state.gate_config.as_mut())
        .expect("review gate config")
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project workflow updates");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("reviewer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("completed reviewer execution creates");
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::AwaitingHuman,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running review creates");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("reviewer completion cascade succeeds");
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("reviewer completion cascade is idempotent");

    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::AwaitingHuman);
    assert!(reviews[0].finished_at.is_none());
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "review");
    assert!(service
        .is_awaiting_human(task.id.clone())
        .await
        .expect("awaiting human resolves"));
    // Explicit human review does not invent an agent conformance assessment.
    assert!(
        serde_json::from_str::<serde_json::Value>(&reviews[0].step_results_json)
            .unwrap()
            .get("conformance")
            .is_none()
    );

    // Model the completed implementation attempt that preceded review and
    // exhaust the optional automatic review-fix retry. This keeps the test
    // focused on Project-Agent authority instead of launching a follow-up
    // executor against this deliberately workspace-free fixture.
    sqlx::query(
        "INSERT INTO execution (
             id, task_id, agent_id, role, status, created_at, updated_at
         ) VALUES (?, ?, ?, 'coder', 'completed', ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&task.id)
    .bind(&agent_id)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .expect("completed coder attempt creates");
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(r#"{"retry_budgets":{"review":0}}"#)
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("review retry budget updates");

    let agent = AgentRepo::get_by_id(&*db, &agent_id)
        .await
        .expect("Project Agent reads")
        .expect("Project Agent exists");
    let binding = ProjectAgentBindingRepo::get_active_project_binding(&*db, &project_id)
        .await
        .expect("Project Agent binding reads")
        .expect("Project Agent binding exists");
    ProjectAgentBindingRepo::replace_project_binding(
        &*db,
        ReplaceProjectAgentBinding {
            project_id: project_id.clone(),
            expected_version: binding.version,
            replacement: CreateProjectAgentBinding {
                id: new_uuid_v4(),
                project_id: project_id.clone(),
                identity_id: Some(agent_id.clone()),
                profile_id: Some(agent.profile_id),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: r#"{"permissions":["propose_task"]}"#.to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 1,
                operating_skill_revision_id: None,
                policy_revision: "default".to_owned(),
                policy_digest: String::new(),
                charter_id: None,
                charter_revision_id: None,
                charter_setup_required: true,
                admission_receipt_id: None,
                charter_approval_id: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
            replacement_reason: Some("test human-required review".to_owned()),
        },
    )
    .await
    .expect("Project Agent binding updates");
    crate::test_support::clear_project_execution_role_defaults(&db, &project_id).await;

    let unbound = service
        .perform_project_agent_review(
            &project_id,
            task.id.clone(),
            false,
            Some("not authorized".to_owned()),
            current.version,
            "another-agent",
        )
        .await;
    assert!(matches!(
        unbound,
        Err(ServiceError::InvalidOperation { .. })
    ));

    let reviewed = service
        .perform_project_agent_review(
            &project_id,
            task.id.clone(),
            false,
            Some("Please address the review feedback".to_owned()),
            current.version,
            &agent_id,
        )
        .await
        .expect("bound Project Agent rejects human-required review");
    assert_eq!(reviewed.action, api_types::TaskAction::RequestChanges);
    assert_eq!(
        reviewed.task.status,
        crate::workflow::default_states::IN_PROGRESS
    );
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews reload");
    assert_eq!(reviews[0].status, ReviewStatus::Failed);
}

#[tokio::test]
async fn follow_up_execution_creates_interactive_child() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "claude_code", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let message = "Please continue with the remaining edge cases".to_owned();
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("parent execution".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"claude_code","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id.clone(), message.clone(), None, None)
        .await
        .expect("follow-up succeeds");

    assert_eq!(
        result.execution.parent_execution_id.as_deref(),
        Some(parent_execution.id.as_str())
    );
    assert_eq!(result.execution.summary.as_deref(), Some(message.as_str()));
    assert_eq!(result.execution.role, "interactive".to_owned());
    let snapshot: serde_json::Value = serde_json::from_str(
        result
            .execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("snapshot exists"),
    )
    .expect("snapshot is valid json");
    assert_eq!(snapshot["config"]["resume_session_id"], "test-session");
}

#[tokio::test]
async fn open_interactive_recovery_starts_the_created_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let parent = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        Some("parent-session"),
        "2026-01-01T00:00:00Z",
    )
    .await;
    let annotation = api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::ManualStop,
        blocking_reason: "user_paused".to_owned(),
        blocked_by: Some("user:api".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: Some(parent.id.clone()),
        artifact: None,
        message: Some("paused".to_owned()),
        hook: None,
        recovery_actions: vec![api_types::RecoveryAction::OpenInteractive],
    };
    let task = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation"),
            )),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("manual-stop annotation saves");

    service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::OpenInteractive,
            Some("continue interactively".to_owned()),
            None,
        )
        .await
        .expect("open-interactive recovery succeeds");

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let executions = ExecutionRepo::list_by_task(
                &*db,
                &task.id,
                PageRequest {
                    cursor: None,
                    limit: 20,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Desc,
                },
            )
            .await
            .expect("executions load");
            if executions.items.iter().any(|execution| {
                execution.role == crate::workflow::default_roles::INTERACTIVE
                    && execution.status == ExecutionStatus::Completed
            }) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("recovery execution starts and completes");
}

#[tokio::test]
async fn follow_up_execution_preserves_workflow_role() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "claude_code", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_id.clone()), None),
            false,
            false,
        )
        .await
        .expect("workflow role assignment creates");
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("workflow-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("coder execution".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("failed".to_owned()),
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"claude_code","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("workflow parent execution creates");

    let result = service
        .follow_up_execution(
            parent_execution.id.clone(),
            "resume coder work".to_owned(),
            None,
            None,
        )
        .await
        .expect("workflow follow-up succeeds");

    assert_eq!(
        result.execution.parent_execution_id.as_deref(),
        Some(parent_execution.id.as_str())
    );
    assert_eq!(result.execution.role, "coder");
    assert_eq!(result.execution.agent_session_id, None);
}

#[tokio::test]
async fn reviewer_follow_up_binds_review_candidate_parent_while_resuming_reviewer_session() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace root creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "claude_code", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::REVIEWER,
            Some(agent_id.clone()),
            None,
        ),
    )
    .await
    .expect("reviewer assignment creates");
    let workspace_id = seed_workspace_for_task(&db, &task, &repo_id, workspace_root.path()).await;
    let now = now_rfc3339();
    let candidate = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        None,
        &now,
    )
    .await;
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    let reviewer = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: Some(candidate.id.clone()),
            agent_session_id: Some("review-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("reviewer execution".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("review failed".to_owned()),
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"claude_code","config":{}}"#.to_owned(),
            ),
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("reviewer execution creates");

    let result = service
        .follow_up_execution(reviewer.id, "resume reviewer".to_owned(), None, None)
        .await
        .expect("reviewer follow-up succeeds");

    assert_eq!(
        result.execution.role,
        crate::workflow::default_roles::REVIEWER
    );
    assert_eq!(
        result.execution.parent_execution_id.as_deref(),
        Some(candidate.id.as_str())
    );
    assert_eq!(
        result
            .execution
            .executor_config_snapshot_json
            .as_deref()
            .and_then(|snapshot| serde_json::from_str::<serde_json::Value>(snapshot).ok())
            .and_then(|snapshot| snapshot["config"]["resume_session_id"]
                .as_str()
                .map(str::to_owned)),
        Some("review-session".to_owned())
    );
    assert_eq!(review.execution_id, candidate.id);
}

#[tokio::test]
async fn reviewer_resume_repairs_legacy_reviewer_parent_to_review_candidate() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace root creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "claude_code", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::REVIEWER,
            Some(agent_id.clone()),
            None,
        ),
    )
    .await
    .expect("reviewer assignment creates");
    let workspace_id = seed_workspace_for_task(&db, &task, &repo_id, workspace_root.path()).await;
    let now = now_rfc3339();
    let candidate = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        None,
        &now,
    )
    .await;
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    let legacy_reviewer = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::REVIEWER,
        ExecutionStatus::Failed,
        Some("review-session"),
        &now,
    )
    .await;

    let resumed = service
        .create_running_execution(
            db::CreateExecution {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                agent_id: Some(agent_id),
                role: crate::workflow::default_roles::REVIEWER.to_owned(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: Some(legacy_reviewer.id),
                agent_session_id: Some("review-session".to_owned()),
                agent_message_id: None,
                last_activity_at: None,
                summary: Some("resume reviewer".to_owned()),
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: Some(
                    r#"{"executor_type":"claude_code","config":{}}"#.to_owned(),
                ),
                workspace_id: Some(workspace_id),
                created_at: now.clone(),
                updated_at: now,
            },
            false,
        )
        .await
        .expect("reviewer resume creates");

    assert_eq!(
        resumed.parent_execution_id.as_deref(),
        Some(candidate.id.as_str())
    );
    assert_eq!(resumed.agent_session_id.as_deref(), Some("review-session"));
}

#[tokio::test]
async fn follow_up_rejects_a_running_repository_role_without_mutating_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace root creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "claude_code", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let workspace_id = seed_workspace_for_task(&db, &task, &repo_id, workspace_root.path()).await;
    let now = now_rfc3339();
    let parent = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("failed-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("failed".to_owned()),
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"claude_code","config":{}}"#.to_owned(),
            ),
            workspace_id: Some(workspace_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("failed parent creates");
    let running = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"claude_code","config":{}}"#.to_owned(),
            ),
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running execution creates");

    let result = service
        .follow_up_execution(parent.id, "continue".to_owned(), None, None)
        .await;

    assert!(matches!(
        &result,
        Err(ServiceError::ExecutionAlreadyRunning { scope, execution_id })
            if scope == "repository" && execution_id == &running.id
    ));
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert_eq!(executions.items.len(), 2);
    let unchanged = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(unchanged.version, task.version);
    assert!(unchanged.error_annotation.is_none());
}

#[tokio::test]
async fn follow_up_execution_codex_resumes_with_message_only_fallback() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let message = "Please continue with the remaining edge cases".to_owned();
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("codex-thread".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("parent execution".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"codex","config":{"resume_fallback_prompt":"do not send this full prompt"}}"#
                    .to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id.clone(), message.clone(), None, None)
        .await
        .expect("follow-up succeeds");

    assert_eq!(result.execution.summary.as_deref(), Some(message.as_str()));
    let snapshot: serde_json::Value = serde_json::from_str(
        result
            .execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("snapshot exists"),
    )
    .expect("snapshot is valid json");
    assert_eq!(snapshot["config"]["resume_thread_id"], "codex-thread");
    assert_eq!(snapshot["config"]["resume_thread_in_place"], true);
    assert!(snapshot["config"].get("resume_fallback_prompt").is_none());
}

#[tokio::test]
async fn follow_up_execution_rejects_running_parent() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("running parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id, "continue".to_owned(), None, None)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn follow_up_on_cancelled_execution_with_session_succeeds() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("cancelled parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id, "continue".to_owned(), None, None)
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn follow_up_on_cancelled_execution_without_session_returns_error() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("cancelled parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id, "continue".to_owned(), None, None)
        .await;

    assert!(matches!(
        result,
        Err(ServiceError::InvalidOperation { message })
            if message.contains("no resumable session")
    ));
}

#[tokio::test]
async fn follow_up_execution_rejects_missing_session_id() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("completed parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id, "continue".to_owned(), None, None)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn follow_up_execution_rejects_terminal_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "done".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("completed parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id, "continue".to_owned(), None, None)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn follow_up_execution_on_blocked_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: Some(Some(
                r#"{"reason":"test block","created_at":"2026-04-28T00:00:00Z","kind":"ci_failed"}"#
                    .to_owned(),
            )),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("set blocked_json");
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("completed parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id, "continue".to_owned(), None, None)
        .await
        .expect("follow-up succeeds");

    assert_eq!(result.execution.role, "interactive".to_owned());
    assert_eq!(result.task.status, "in_progress".to_owned());
}

#[tokio::test]
async fn follow_up_execution_rejects_executor_mismatch() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let shell_agent_id = seed_agent(&db).await;
    let codex_agent_id = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(shell_agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("completed parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(
            parent_execution.id,
            "continue".to_owned(),
            Some(codex_agent_id),
            None,
        )
        .await;

    assert!(matches!(
        result,
        Err(ServiceError::InvalidOperation { message })
            if message.contains("same executor type")
    ));
}

#[tokio::test]
async fn re_execute_cancelled_execution_dispatches_fresh() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_id.clone()), None),
            false,
            false,
        )
        .await
        .expect("workflow role assignment creates");
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("cancelled parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .re_execute_execution(parent_execution.id)
        .await
        .expect("re-execute succeeds");

    assert_eq!(result.execution.role, "coder".to_owned());
    assert_eq!(result.execution.status, ExecutionStatus::Running);
    assert_eq!(result.execution.parent_execution_id, None);
    assert_eq!(result.execution.agent_session_id, None);
}

/// Re-execute used to carry the parent execution's Agent over, so a role
/// reassigned after that execution failed the assignment CAS inside the
/// execution INSERT and surfaced as a bare version conflict -- which made
/// recovery impossible in exactly the case it exists for: swapping a broken
/// principal for a working one.
#[tokio::test]
async fn re_execute_follows_a_reassigned_role_principal() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let original_agent_id = seed_agent(&db).await;
    let replacement_agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(original_agent_id.clone()), None),
            false,
            false,
        )
        .await
        .expect("workflow role assignment creates");
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(original_agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("failed parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("executor died".to_owned()),
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(replacement_agent_id.clone()), None),
            false,
            false,
        )
        .await
        .expect("role reassignment applies");

    let result = service
        .re_execute_execution(parent_execution.id)
        .await
        .expect("re-execute succeeds after the role was reassigned");

    assert_eq!(result.execution.agent_id, Some(replacement_agent_id));
    assert_eq!(result.execution.status, ExecutionStatus::Running);
}

#[tokio::test]
async fn re_execute_rejects_running_parent() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("running parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service.re_execute_execution(parent_execution.id).await;

    assert!(matches!(
        result,
        Err(ServiceError::InvalidOperation { message })
            if message.contains("re-execute requires")
    ));
}

#[tokio::test]
async fn re_execute_rejects_concurrent_running_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    // The concurrency guard is workspace-scoped: a second worker is only a
    // conflict when it would enter the same worktree.
    let workspace_id = seed_workspace_for_task(&db, &task, &repo_id, workspace_root.path()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("cancelled parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: Some(workspace_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("parent execution creates");
    ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("running sibling".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: Some(workspace_id.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running execution creates");

    let result = service.re_execute_execution(parent_execution.id).await;

    assert!(
        matches!(result, Err(ServiceError::ExecutionAlreadyRunning { .. })),
        "a running sibling must reject re-execution, got {:?}",
        result.as_ref().err()
    );
}

#[tokio::test]
async fn interactive_execution_completion_does_not_trigger_review_cascade() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    crate::test_support::clear_project_execution_role_defaults(&db, &project_id).await;
    let task = service
        .create_task(
            project_id,
            "Interactive no cascade",
            Some("printf no-cascade".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let launched = service
        .launch_execution(task.id.clone(), agent_id, None, None)
        .await
        .expect("launch succeeds");

    let registry = Arc::new(cli_adapters::test_support::test_registry());
    let executor = executors::AdapterExecutor::new(registry);
    let execution = service
        .run_execution(launched.execution.id.clone(), &executor)
        .await
        .expect("interactive execution runs");
    assert_eq!(execution.status, ExecutionStatus::Completed);

    service
        .maybe_cascade_executor_completion(&launched.execution.id)
        .await
        .expect("cascade check succeeds");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "in_progress".to_owned());
}

#[tokio::test]
async fn recover_reexecute_without_blocked_execution_dispatches_current_state_role() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_task_executor(Arc::new(PendingExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::CODER,
            Some(agent_id.clone()),
            None,
        ),
    )
    .await
    .expect("coder assignment created");
    let annotation = json!({
        "type": "recovery_required",
        "blocking_reason": "crash_recovery",
        "blocked_by": "system:crash_recovery",
        "blocked_at": now_rfc3339(),
        "message": "Recovered after server restart",
        "recovery_actions": ["reexecute", "reset_to_initial", "cancel_task"],
    })
    .to_string();
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation)),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task recovery annotation saved");

    assert_eq!(
        service
            .available_recovery_actions(task.id.clone())
            .await
            .expect("recovery actions resolve"),
        vec![
            api_types::RecoveryAction::Reexecute,
            api_types::RecoveryAction::ResetToInitial,
            api_types::RecoveryAction::CancelTask,
        ]
    );

    for unadvertised in [
        api_types::RecoveryAction::RetryHook,
        api_types::RecoveryAction::ResetRetryWindow,
    ] {
        let error = service
            .recover_task(
                task.id.clone(),
                unadvertised,
                Some("must not widen recovery contract".to_owned()),
                None,
            )
            .await
            .expect_err("unadvertised recovery action is rejected");
        assert!(matches!(error, ServiceError::InvalidOperation { .. }));
        let still_blocked = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task reloads")
            .expect("task exists");
        assert!(still_blocked.error_annotation.is_some());
        assert_eq!(
            ExecutionRepo::count_by_task_and_role(
                &*db,
                &task.id,
                crate::workflow::default_roles::CODER,
            )
            .await
            .expect("execution count loads"),
            0
        );
    }

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::Reexecute,
            Some("test".to_owned()),
            Some("resume current work".to_owned()),
        )
        .await
        .expect("reexecute recovers");

    assert_eq!(recovered.status, "in_progress");
    assert_eq!(recovered.error_annotation, None);
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].role,
        crate::workflow::default_roles::CODER
    );
    assert_eq!(executions.items[0].status, ExecutionStatus::Running);
    assert!(executions.items[0]
        .summary
        .as_deref()
        .unwrap_or_default()
        .contains("resume current work"));
}

#[tokio::test]
async fn submit_is_not_available_while_agent_work_has_not_completed() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::CODER,
            Some(agent_id.clone()),
            None,
        ),
    )
    .await
    .expect("coder assignment created");
    let never_run_actions = service
        .available_task_actions(task.id.clone())
        .await
        .expect("never-run actions resolve");
    assert!(
        !never_run_actions.contains(&api_types::TaskAction::Submit),
        "an assigned Task must not skip its first coder execution"
    );
    let running = seed_running_coder_execution(&db, &task.id, Some(agent_id), None).await;

    let actions = service
        .available_task_actions(task.id.clone())
        .await
        .expect("actions resolve");
    assert!(!actions.contains(&api_types::TaskAction::Submit));

    let error = service
        .perform_task_action(
            task.id.clone(),
            api_types::TaskAction::Submit,
            None,
            Some(task.version),
        )
        .await
        .expect_err("submit cannot bypass unfinished agent work");
    assert!(matches!(error, ServiceError::TaskActionUnavailable { .. }));
    let execution = ExecutionRepo::get_by_id(&*db, &running.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution.status, ExecutionStatus::Running);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "in_progress");
}

#[tokio::test]
async fn submit_does_not_reuse_a_completed_attempt_from_before_review_remediation() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    seed_completed_coder_execution(&db, &task, &agent_id, None).await;
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: crate::workflow::default_states::REVIEW.to_owned(),
            to_state: crate::workflow::default_states::IN_PROGRESS.to_owned(),
            trigger_name: Some("reject".to_owned()),
            triggered_by: "user:api".to_owned(),
            trigger_reason: "review remediation".to_owned(),
            hook_results_json: None,
            rejection: true,
            // Keep the boundary unambiguously newer than the seeded attempt.
            created_at: "2099-01-01T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("review remediation boundary records");

    let actions = service
        .available_task_actions(task.id.clone())
        .await
        .expect("actions resolve");
    assert!(!actions.contains(&api_types::TaskAction::Submit));
}

#[tokio::test]
async fn resume_is_not_offered_for_unrelated_role_history() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        Some("coder-session"),
        "2026-01-01T00:00:00Z",
    )
    .await;
    seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::INTERACTIVE,
        ExecutionStatus::Completed,
        Some("interactive-session"),
        "2026-01-02T00:00:00Z",
    )
    .await;

    let actions = service
        .available_task_actions(task.id.clone())
        .await
        .expect("actions resolve");
    assert!(!actions.contains(&api_types::TaskAction::Resume));
}

#[tokio::test]
async fn submit_uses_latest_current_role_execution_not_later_interactive_history() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    seed_completed_coder_execution(&db, &task, &agent_id, None).await;
    seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::INTERACTIVE,
        ExecutionStatus::Completed,
        Some("interactive-session"),
        "2099-01-01T00:00:00Z",
    )
    .await;

    let actions = service
        .available_task_actions(task.id.clone())
        .await
        .expect("actions resolve");
    assert!(actions.contains(&api_types::TaskAction::Submit));
}

#[tokio::test]
async fn resume_without_session_clears_manual_stop_before_reexecute() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::CODER,
            Some(agent_id.clone()),
            None,
        ),
    )
    .await
    .expect("coder assignment creates");
    let parent = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        "executor",
        ExecutionStatus::Completed,
        None,
        "2026-01-01T00:00:00Z",
    )
    .await;
    let annotation = api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::ManualStop,
        blocking_reason: "user_paused".to_owned(),
        blocked_by: Some("user:api".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: Some(parent.id.clone()),
        artifact: None,
        message: Some("paused".to_owned()),
        hook: None,
        recovery_actions: vec![
            api_types::RecoveryAction::Reexecute,
            api_types::RecoveryAction::ResetToInitial,
            api_types::RecoveryAction::CancelTask,
        ],
    };
    let task = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation serializes"),
            )),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("manual-stop annotation saves");

    let result = service
        .perform_task_action(
            task.id.clone(),
            api_types::TaskAction::Resume,
            Some("continue after pause".to_owned()),
            Some(task.version),
        )
        .await
        .expect("resume re-executes the no-session parent");
    assert_eq!(result.task.error_annotation, None);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(current.error_annotation.is_none());
    assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
        .await
        .expect("running executions load")
        .iter()
        .any(|execution| execution.id != parent.id));
}

/// A reviewer execution binds the current Review attempt and that binding
/// only accepts a `Running` attempt. Recovery re-executes inside `review`,
/// where no transition hook runs, so a settled attempt -- the normal shape
/// after a reviewer execution dies -- made `reexecute` reject every attempt
/// as a bare `version_conflict` with no other advertised way out.
#[tokio::test]
async fn reexecute_opens_a_fresh_review_attempt_when_the_last_one_settled() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service =
        TaskService::new(Arc::clone(&db), event_bus).with_task_executor(Arc::new(NoDiffExecutor));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let coder_agent_id = seed_agent(&db).await;
    let reviewer_agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    for (role, agent_id) in [
        (crate::workflow::default_roles::CODER, &coder_agent_id),
        (crate::workflow::default_roles::REVIEWER, &reviewer_agent_id),
    ] {
        TaskRoleAssignmentRepo::assign(
            &*db,
            role_assignment_input(&task.id, role, Some(agent_id.clone()), None),
        )
        .await
        .expect("role assignment creates");
    }
    let candidate = seed_execution(
        &db,
        &task.id,
        Some(&coder_agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        None,
        "2026-01-01T00:00:00Z",
    )
    .await;
    seed_execution(
        &db,
        &task.id,
        Some(&reviewer_agent_id),
        crate::workflow::default_roles::REVIEWER,
        ExecutionStatus::Failed,
        None,
        "2026-01-01T00:01:00Z",
    )
    .await;
    crate::task_service::tests::helpers::seed_failed_review(
        &db,
        &task.id,
        &candidate.id,
        1,
        serde_json::json!({}),
    )
    .await;
    let annotation = serde_json::to_string(&api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::ExecutorFailed,
        blocking_reason: "executor_error".to_owned(),
        blocked_by: Some("system:executor".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: Some("the reviewer executor died".to_owned()),
        hook: None,
        recovery_actions: vec![
            api_types::RecoveryAction::Reexecute,
            api_types::RecoveryAction::CancelTask,
        ],
    })
    .expect("annotation serializes");
    let task = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation)),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("blocking annotation saves");

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::Reexecute,
            Some("test".to_owned()),
            None,
        )
        .await
        .expect("reexecute recovers a task whose review attempt already settled");

    assert_eq!(recovered.status, "review");
    assert_eq!(recovered.error_annotation, None);
    // Recovery returning at all is the regression: the reviewer execution
    // could only be created because a fresh attempt was open for it to bind.
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    let latest = reviews
        .iter()
        .max_by_key(|review| review.attempt_number)
        .expect("a review attempt exists");
    assert_eq!(latest.attempt_number, 2);
    assert_eq!(latest.execution_id, candidate.id);
    assert!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER,
        )
        .await
        .expect("reviewer executions count")
            >= 2
    );
}

#[tokio::test]
async fn hard_failed_active_task_cannot_resume_or_submit() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let task = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: Some(Some(json!({ "reason": "executor failed" }).to_string())),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("hard failure records");

    let actions = service
        .available_task_actions(task.id.clone())
        .await
        .expect("actions resolve");
    assert!(!actions.contains(&api_types::TaskAction::Resume));
    assert!(!actions.contains(&api_types::TaskAction::Submit));
    for action in [api_types::TaskAction::Resume, api_types::TaskAction::Submit] {
        let error = service
            .perform_task_action(task.id.clone(), action, None, Some(task.version))
            .await
            .expect_err("hard failure blocks generic task action");
        assert!(matches!(error, ServiceError::TaskActionUnavailable { .. }));
    }
}

#[tokio::test]
async fn manual_stop_annotation_retries_after_task_version_conflict() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let execution = seed_running_coder_execution(&db, &task.id, None, None).await;
    let workflow_definition = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .workflow_definition;
    let stale_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("stale task loads")
        .expect("task exists");

    let concurrently_updated = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: stale_task.version,
            title: Some("concurrent task update".to_owned()),
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("concurrent task update commits");

    service
        .persist_manual_stop_annotation(
            &execution,
            &stale_task,
            "in_progress",
            None,
            Some(crate::workflow::default_roles::CODER),
            &workflow_definition,
            None,
            serde_json::json!({
                "type": "manual_stop",
                "blocked_execution_id": execution.id,
            })
            .to_string(),
            now_rfc3339(),
        )
        .await
        .expect("manual-stop annotation retries on a stale Task snapshot");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("current task loads")
        .expect("task exists");
    assert_eq!(current.title, concurrently_updated.title);
    assert!(current
        .error_annotation
        .as_deref()
        .is_some_and(|annotation| annotation.contains(&execution.id)));
}

#[tokio::test]
async fn manual_stop_annotation_skips_after_same_role_replacement_starts() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let execution = seed_running_coder_execution(&db, &task.id, None, None).await;
    let workflow_definition = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .workflow_definition;
    let stale_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("stale task loads")
        .expect("task exists");
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: stale_task.version,
            title: Some("concurrent task update".to_owned()),
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("concurrent task update commits");
    let _replacement = seed_running_coder_execution(&db, &task.id, None, None).await;

    service
        .persist_manual_stop_annotation(
            &execution,
            &stale_task,
            "in_progress",
            None,
            Some(crate::workflow::default_roles::CODER),
            &workflow_definition,
            None,
            serde_json::json!({
                "type": "manual_stop",
                "blocked_execution_id": execution.id,
            })
            .to_string(),
            now_rfc3339(),
        )
        .await
        .expect("stale stop does not fail when replacement is running");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("current task loads")
        .expect("task exists");
    assert!(current.error_annotation.is_none());
}

#[tokio::test]
async fn manual_stop_annotation_skips_after_same_role_reassignment() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let old_agent_id = seed_agent(&db).await;
    let new_agent_id = seed_agent(&db).await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::CODER,
            Some(old_agent_id),
            None,
        ),
    )
    .await
    .expect("initial coder assignment creates");
    let execution = seed_execution(
        &db,
        &task.id,
        None,
        "executor",
        ExecutionStatus::Completed,
        None,
        &now_rfc3339(),
    )
    .await;
    let workflow_definition = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .workflow_definition;
    let stale_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("stale task loads")
        .expect("task exists");
    let old_assignment = TaskRoleAssignmentRepo::get_by_task_and_role(
        &*db,
        &task.id,
        crate::workflow::default_roles::CODER,
    )
    .await
    .expect("old assignment loads")
    .expect("old assignment exists");

    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::CODER,
            Some(new_agent_id),
            None,
        ),
    )
    .await
    .expect("same-role reassignment commits");

    let db_result = TaskRepo::set_error_annotation_if_no_running_execution(
        &*db,
        &stale_task.id,
        stale_task.version,
        "in_progress",
        None,
        &workflow_definition,
        Some(crate::workflow::default_roles::CODER),
        Some(old_assignment.clone()),
        "stale-stop",
        &now_rfc3339(),
        &execution.id,
        None,
        Vec::new(),
    )
    .await;
    assert!(
        matches!(db_result, Err(db::DbError::VersionConflict)),
        "assignment CAS must reject a stale same-role stop: {db_result:?}"
    );

    service
        .persist_manual_stop_annotation(
            &execution,
            &stale_task,
            "in_progress",
            None,
            Some(crate::workflow::default_roles::CODER),
            &workflow_definition,
            Some(old_assignment),
            serde_json::json!({
                "type": "manual_stop",
                "blocked_execution_id": execution.id,
            })
            .to_string(),
            now_rfc3339(),
        )
        .await
        .expect("stale stop does not fail after reassignment");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("current task loads")
        .expect("task exists");
    assert!(current.error_annotation.is_none());
}

#[tokio::test]
async fn manual_stop_annotation_rejects_stale_state_entry_after_cycle() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let execution = seed_running_coder_execution(&db, &task.id, None, None).await;
    let workflow_definition = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .workflow_definition;
    let stale_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("stale task loads")
        .expect("task exists");

    let in_review = TaskRepo::update_status(
        &*db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: stale_task.version,
            status: "review".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("first state transition commits");
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: "in_progress".to_owned(),
            to_state: "review".to_owned(),
            trigger_name: Some("test_cycle".to_owned()),
            triggered_by: "system:test".to_owned(),
            trigger_reason: "state-entry epoch regression".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: "2099-01-01T00:00:01Z".to_owned(),
        },
    )
    .await
    .expect("first transition log commits");
    let back_in_progress = TaskRepo::update_status(
        &*db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: in_review.version,
            status: "in_progress".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("return transition commits");
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: "review".to_owned(),
            to_state: "in_progress".to_owned(),
            trigger_name: Some("test_cycle".to_owned()),
            triggered_by: "system:test".to_owned(),
            trigger_reason: "state-entry epoch regression".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: "2099-01-01T00:00:02Z".to_owned(),
        },
    )
    .await
    .expect("return transition log commits");

    // A caller that retries with the current Task version still cannot use the
    // initial/no-log token captured before the X -> Y -> X cycle.
    let db_result = TaskRepo::set_error_annotation_if_no_running_execution(
        &*db,
        &task.id,
        back_in_progress.version,
        "in_progress",
        None,
        &workflow_definition,
        Some(crate::workflow::default_roles::CODER),
        None,
        "stale-stop",
        &now_rfc3339(),
        &execution.id,
        None,
        Vec::new(),
    )
    .await;
    assert!(
        matches!(db_result, Err(db::DbError::VersionConflict)),
        "state-entry CAS must reject the old initial epoch: {db_result:?}"
    );

    service
        .persist_manual_stop_annotation(
            &execution,
            &stale_task,
            "in_progress",
            None,
            Some(crate::workflow::default_roles::CODER),
            &workflow_definition,
            None,
            serde_json::json!({
                "type": "manual_stop",
                "blocked_execution_id": execution.id,
            })
            .to_string(),
            now_rfc3339(),
        )
        .await
        .expect("stale stop does not fail after state cycle");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("current task loads")
        .expect("task exists");
    assert!(current.error_annotation.is_none());
}

#[tokio::test]
async fn state_entry_authority_breaks_timestamp_ties_by_insertion_order() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let workflow_definition = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .workflow_definition;
    let tied_created_at = "2099-01-01T00:00:00Z";
    let older_id = "zzzz-older-state-entry";
    let newer_id = "aaaa-newer-state-entry";

    for id in [older_id, newer_id] {
        TransitionLogRepo::insert(
            &*db,
            db::CreateTransitionLog {
                id: id.to_owned(),
                task_id: task.id.clone(),
                from_state: "review".to_owned(),
                to_state: "in_progress".to_owned(),
                trigger_name: Some("test_tie".to_owned()),
                triggered_by: "system:test".to_owned(),
                trigger_reason: "state-entry timestamp tie regression".to_owned(),
                hook_results_json: None,
                rejection: false,
                created_at: tied_created_at.to_owned(),
            },
        )
        .await
        .expect("transition log commits");
    }

    let latest = crate::task_service::action_resolver::latest_state_entry_authority(
        &db,
        &task.id,
        "in_progress",
    )
    .await
    .expect("state-entry authority loads")
    .expect("state-entry authority exists");
    assert_eq!(latest.id, newer_id);

    let db_result = TaskRepo::set_error_annotation_if_no_running_execution(
        &*db,
        &task.id,
        task.version,
        "in_progress",
        Some(older_id),
        &workflow_definition,
        None,
        None,
        "stale-stop",
        &now_rfc3339(),
        "stopped-execution",
        None,
        Vec::new(),
    )
    .await;
    assert!(
        matches!(db_result, Err(db::DbError::VersionConflict)),
        "the transactional CAS must reject the older tied entry: {db_result:?}"
    );
}

#[tokio::test]
async fn failed_resume_does_not_restore_metadata_over_running_replacement() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let annotation = serde_json::json!({
        "type": "manual_stop",
        "blocked_execution_id": "stopped-execution"
    })
    .to_string();
    let annotated = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation.clone())),
            blocked_json: Some(Some(
                serde_json::json!({"reason": "manual stop"}).to_string(),
            )),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("manual-stop metadata sets");
    let cleared = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: annotated.id.clone(),
            expected_version: annotated.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: Some(None),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("recovery clear commits");
    let _replacement = seed_running_coder_execution(&db, &task.id, None, None).await;

    let result = TaskRepo::update_recovery_metadata_if_no_running_execution(
        &*db,
        &cleared.id,
        cleared.version,
        annotated.error_annotation.clone(),
        annotated.blocked_json.clone(),
        annotated.failed_json.clone(),
        &now_rfc3339(),
        Some("workspace-after-reset"),
        Vec::new(),
        Vec::new(),
    )
    .await;
    assert!(
        matches!(
            result,
            Err(db::DbError::ExecutionAlreadyRunning {
                ref scope,
            ref execution_id,
            }) if scope == "repository" && execution_id == &_replacement.id
        ),
        "unexpected restore result: {result:?}"
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("current task loads")
        .expect("task exists");
    assert!(current.error_annotation.is_none());
    assert!(current.blocked_json.is_none());
}

#[tokio::test]
async fn merge_fix_completion_invalidates_cached_review_before_reentering_review() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::MERGE_FAILED.to_owned(),
    )
    .await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::REVIEWER,
            None,
            Some("review-owner".to_owned()),
        ),
    )
    .await
    .expect("human reviewer assignment creates");
    TaskRepo::set_review_passed_at(&*db, &task.id, Some(now_rfc3339()), &now_rfc3339())
        .await
        .expect("cached review authority seeds");
    let now = now_rfc3339();
    let execution_snapshot = workflow_execution_snapshot(&db, &project_id).await;
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(execution_snapshot),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("merge-fix execution creates");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("merge-fix completion enters a fresh review");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, crate::workflow::default_states::REVIEW);
    assert!(
        current.review_passed_at.is_none(),
        "the prior commit's review authority must be gone before review hooks run"
    );
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::AwaitingHuman);
}

struct PendingExecutor;

#[async_trait::async_trait]
impl TaskExecutor for PendingExecutor {
    async fn execute(
        &self,
        _ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        std::future::pending::<std::result::Result<ExecutionResult, ExecutorError>>().await
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

#[tokio::test]
async fn executor_completion_guard_rejection_follows_up_before_blocking() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service =
        TaskService::new(Arc::clone(&db), event_bus).with_task_executor(Arc::new(PendingExecutor));
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let workspace =
        seed_workspace_with_plan(&db, &task, &repo_id, "- [ ] finish implementation\n").await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    let execution =
        seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("guard rejection dispatches follow-up");

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    let resumed = executions
        .items
        .iter()
        .find(|candidate| candidate.status == ExecutionStatus::Running)
        .expect("lease-backed follow-up exists");
    assert_eq!(
        resumed.parent_execution_id.as_deref(),
        Some(execution.id.as_str())
    );
    let execution = ExecutionRepo::get_by_id(&*db, &resumed.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution.status, ExecutionStatus::Running);
    assert!(execution.summary.as_deref().is_some_and(|summary| {
        summary.contains("Workflow guard failed: require_plan_checklist_complete")
    }));
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = task.metadata().expect("metadata parses");
    assert_eq!(metadata.extra["workflow_guard_retry_count"], json!(1));
    assert!(task.blocked_json.is_none());
}

#[tokio::test]
async fn superseded_project_revision_cannot_apply_completed_coder_guard_effects() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let workspace =
        seed_workspace_with_plan(&db, &task, &repo_id, "- [ ] finish implementation\n").await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    let execution =
        seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;

    sqlx::query("UPDATE project SET version = version + 1 WHERE id = ?")
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project revision advances");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("superseded completion is inert");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "in_progress");
    let metadata = current.metadata().expect("metadata parses");
    assert!(metadata.extra.get("workflow_guard_retry_count").is_none());
    assert!(current.blocked_json.is_none());
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    assert_eq!(executions.items.len(), 1, "no follow-up may be launched");
}

#[tokio::test]
async fn superseded_project_revision_cannot_schedule_failed_execution_retry_or_block() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    let execution = seed_completed_coder_execution(&db, &task, &agent_id, None).await;
    sqlx::query(
        "UPDATE execution
         SET status = 'failed', error = 'executor failed', resume_policy = 'manual'
         WHERE id = ?",
    )
    .bind(&execution.id)
    .execute(db.pool())
    .await
    .expect("execution fails");
    let execution = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");

    sqlx::query("UPDATE project SET version = version + 1 WHERE id = ?")
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project revision advances");

    service
        .annotate_executor_failure_block(&execution)
        .await
        .expect("superseded failure is inert");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = current.metadata().expect("metadata parses");
    assert!(metadata.extra.get("execution_retry_count").is_none());
    assert!(metadata.extra.get("deferred_dispatch").is_none());
    assert!(current.blocked_json.is_none());
    assert!(current.error_annotation.is_none());
}

#[tokio::test]
async fn superseded_execution_cannot_mutate_workflow_guard_retry_count() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let project_version = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .version;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let workspace =
        seed_workspace_with_plan(&db, &task, &repo_id, "- [ ] finish implementation\n").await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    let old = seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;
    ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: Some(old.id.clone()),
            agent_session_id: Some("newer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                json!({
                    "executor_type": "shell",
                    "config": {},
                    "project_version": project_version,
                })
                .to_string(),
            ),
            workspace_id: Some(workspace.id),
            created_at: "9999-01-01T00:00:00Z".to_owned(),
            updated_at: "9999-01-01T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("newer execution creates");

    service
        .maybe_cascade_executor_completion(&old.id)
        .await
        .expect("superseded completion is inert");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = current.metadata().expect("metadata parses");
    assert!(metadata.extra.get("workflow_guard_retry_count").is_none());
    assert!(current.blocked_json.is_none());
}

#[tokio::test]
async fn subtask_sequence_guard_rejection_never_resumes_root_coder() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service =
        TaskService::new(Arc::clone(&db), event_bus).with_task_executor(Arc::new(PendingExecutor));
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let _subtask = seed_subtask_with_status(&db, &task, "child", "todo".to_owned(), 0).await;
    let workspace = seed_workspace_with_plan(&db, &task, &repo_id, "- [x] parent work\n").await;
    let execution =
        seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;

    let result = service
        .maybe_cascade_executor_completion(&execution.id)
        .await;
    assert!(
        result.is_ok(),
        "a coder completion without coordination-root role authority is inert"
    );

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    assert!(
        executions
            .items
            .iter()
            .all(|candidate| candidate.status != ExecutionStatus::Running),
        "coordination roots must not receive a coder follow-up"
    );

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = task.metadata().expect("metadata parses");
    assert!(metadata.extra.get("workflow_guard_retry_count").is_none());
    assert!(task.blocked_json.is_none());
}

#[tokio::test]
async fn executor_completion_guard_rejection_blocks_when_retry_budget_exhausted() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let task = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: Some(Some(r#"{"retry_budgets":{"execution":0}}"#.to_owned())),
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task config updates");
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    let workspace =
        seed_workspace_with_plan(&db, &task, &repo_id, "- [ ] finish implementation\n").await;
    let execution =
        seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("guard rejection blocks after exhausted budget");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let blocked = task.blocked_json.expect("task blocked");
    assert!(blocked.contains("workflow_guard_rejected"));
    let annotation = task.error_annotation.expect("annotation recorded");
    assert!(annotation.contains("require_plan_checklist_complete"));
}

#[tokio::test]
async fn executor_completion_comment_uses_execution_agent() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("implemented the change".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("cascade check succeeds");

    let comments = TaskCommentRepo::list_comments(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await
    .expect("comments list");
    let comment = comments
        .items
        .iter()
        .find(|comment| comment.content.contains("Agent completed execution"))
        .expect("executor completion comment exists");
    assert_eq!(comment.author_type, CommentAuthorType::Agent);
    assert_eq!(comment.author_id.as_deref(), Some(agent_id.as_str()));
    assert_eq!(comment.author_name, "shell");
}

async fn seed_workspace_with_plan(
    db: &SqliteDb,
    task: &Task,
    repo_id: &str,
    plan: &str,
) -> Workspace {
    let workspace_dir = std::env::temp_dir()
        .join(format!("forge-guard-plan-{}", new_uuid_v4()))
        .join(&task.id);
    let worktree_path = workspace_dir.join("worktree");
    std::fs::create_dir_all(&worktree_path).expect("worktree creates");
    std::fs::write(workspace_dir.join("plan.md"), plan).expect("plan writes");
    git::init(&worktree_path).await.expect("git init succeeds");
    std::fs::write(worktree_path.join("README.md"), "# Test\n").expect("readme writes");
    git::commit_all(&worktree_path, "initial commit")
        .await
        .expect("initial commit creates");
    WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id: repo_id.to_owned(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("workspace creates")
}

async fn seed_completed_coder_execution(
    db: &SqliteDb,
    task: &Task,
    agent_id: &str,
    workspace_id: Option<&str>,
) -> Execution {
    let now = now_rfc3339();
    let project_version = ProjectRepo::get_by_id(db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .version;
    ExecutionRepo::create(
        db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.to_owned()),
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("implemented the change".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                json!({
                    "executor_type": "shell",
                    "config": {},
                    "project_version": project_version,
                })
                .to_string(),
            ),
            workspace_id: workspace_id.map(str::to_owned),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates")
}

async fn workflow_execution_snapshot(db: &SqliteDb, project_id: &str) -> String {
    let project_version = ProjectRepo::get_by_id(db, project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .version;
    json!({
        "executor_type": "shell",
        "config": {},
        "project_version": project_version,
    })
    .to_string()
}

#[tokio::test]
async fn claim_task_records_execution_policy_overrides_in_snapshot() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id =
        seed_shell_agent_with_config(&db, r#"{"command":"echo","args":["profile-default"]}"#).await;
    let task = service
        .create_task(
            project_id,
            "Snapshot override",
            Some("unused".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");

    let claimed = service
        .claim_task(
            task.id,
            Assignee::Agent(agent_id),
            Some(ExecutionOverrides {
                model_id: None,
                reasoning_effort: None,
                permission_policy: Some("auto".to_owned()),
                hard_deadline_seconds: Some(600),
            }),
        )
        .await
        .expect("task claims");
    let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    let snapshot: Value = serde_json::from_str(
        execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("snapshot recorded"),
    )
    .expect("snapshot parses");

    assert_eq!(snapshot["config"]["permission_policy"], "auto");
    assert_eq!(snapshot["hard_deadline_seconds"], 600);
    assert!(execution.hard_deadline_at.is_some());
    let execution_keys = snapshot["overrides_applied"]["execution"]
        .as_array()
        .expect("execution override keys are recorded");
    assert!(execution_keys
        .iter()
        .any(|key| key.as_str() == Some("permission_policy")));
}

#[tokio::test]
async fn claim_task_applies_agent_run_time_limit_unless_overridden() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(
        &db,
        "codex",
        r#"{"model":"agent-model","hard_deadline_seconds":5400}"#,
    )
    .await;
    let mut snapshots = Vec::new();
    for overrides in [
        None,
        Some(ExecutionOverrides {
            model_id: None,
            reasoning_effort: None,
            permission_policy: None,
            hard_deadline_seconds: Some(600),
        }),
    ] {
        let task = service
            .create_task(
                project_id.clone(),
                "Agent run time limit",
                Some("unused".to_owned()),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("task creates");
        let claimed = service
            .claim_task(task.id, Assignee::Agent(agent_id.clone()), overrides)
            .await
            .expect("task claims");
        let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        let snapshot: Value = serde_json::from_str(
            execution
                .executor_config_snapshot_json
                .as_deref()
                .expect("snapshot recorded"),
        )
        .expect("snapshot parses");
        assert!(execution.hard_deadline_at.is_some());
        assert!(snapshot["config"].get("hard_deadline_seconds").is_none());
        snapshots.push(snapshot["hard_deadline_seconds"].clone());
    }
    assert_eq!(snapshots, vec![json!(5400), json!(600)]);
}

#[tokio::test]
async fn claim_task_records_codex_overrides_in_normalized_snapshot() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(
        &db,
        "codex",
        r#"{"model":"agent-model","model_reasoning_effort":"medium","sandbox":"danger-full-access","permission_policy":"supervised"}"#,
    )
    .await;
    let task = service
        .create_task(
            project_id,
            "Snapshot codex overrides",
            Some("unused".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");

    let claimed = service
        .claim_task(
            task.id,
            Assignee::Agent(agent_id),
            Some(ExecutionOverrides {
                model_id: Some("gpt-5-codex".to_owned()),
                reasoning_effort: Some("high".to_owned()),
                permission_policy: Some("auto".to_owned()),
                hard_deadline_seconds: None,
            }),
        )
        .await
        .expect("task claims");
    let snapshot: Value = serde_json::from_str(
        claimed
            .execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("snapshot recorded"),
    )
    .expect("snapshot parses");

    assert_eq!(snapshot["executor_type"], "codex");
    assert_eq!(snapshot["config"]["model"], "gpt-5-codex");
    assert_eq!(snapshot["config"]["model_reasoning_effort"], "high");
    assert_eq!(snapshot["config"]["permission_policy"], "auto");
    assert!(snapshot["config"].get("effort").is_none());

    let agent_keys = snapshot["overrides_applied"]["agent"]
        .as_array()
        .expect("agent keys are recorded");
    assert!(agent_keys.iter().any(|key| key.as_str() == Some("model")));
    assert!(agent_keys
        .iter()
        .any(|key| key.as_str() == Some("model_reasoning_effort")));
    assert!(agent_keys
        .iter()
        .any(|key| key.as_str() == Some("permission_policy")));

    let execution_keys = snapshot["overrides_applied"]["execution"]
        .as_array()
        .expect("execution keys are recorded");
    assert!(execution_keys
        .iter()
        .any(|key| key.as_str() == Some("model")));
    assert!(execution_keys
        .iter()
        .any(|key| key.as_str() == Some("model_reasoning_effort")));
    assert!(execution_keys
        .iter()
        .any(|key| key.as_str() == Some("permission_policy")));
    assert!(!execution_keys
        .iter()
        .any(|key| key.as_str() == Some("effort")));
}
