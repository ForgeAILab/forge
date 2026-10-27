//! Execution placement, launching, terminal activity and log events.
use super::*;

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
            .expect_err("paused Project refuses launch"),
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
            .expect_err("paused Project refuses launch"),
    );
    errors.push(
        service
            .follow_up_execution(&parent.id, "Continue".into(), None, None)
            .await
            .expect_err("paused Project refuses launch"),
    );
    errors.push(
        service
            .follow_up_interactive_execution(&parent.id, "Continue".into(), None, None)
            .await
            .expect_err("paused Project refuses launch"),
    );
    assert!(
        matches!(
            service
                .perform_task_action(&task.id, api_types::TaskAction::retry(), task.version)
                .await,
            Err(ServiceError::TaskActionUnavailable { .. })
        ),
        "paused Project must not advertise a launchable retry"
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
