//! Tests for role defaults in the task dispatcher.

use super::*;
#[tokio::test]
async fn dispatcher_gives_unassigned_initial_tasks_the_project_defaults() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    // Already released to `todo` but never assigned: exactly the shape a Task
    // proposed before provisioning has once it leaves backlog.
    let task = seed_task(&db, &project_id, "released but unassigned", "todo", 1).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Offline, AgentStatus::Idle).await;
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: Some(
                serde_json::json!({
                    "default_role_assignments": [
                        { "role_name": "coder", "assignee_type": "agent", "assignee_id": agent_id }
                    ]
                })
                .to_string(),
            ),
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project defaults update");
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    let coder = TaskRoleAssignmentRepo::get_by_task_and_role(
        &*db,
        &task.id,
        crate::workflow::default_roles::CODER,
    )
    .await
    .expect("assignment loads")
    .expect("unassigned Task received the Project's default coder");
    assert_eq!(coder.assignee_id.as_deref(), Some(agent_id.as_str()));
}

#[tokio::test]
async fn dispatcher_inherits_root_default_coder_without_copying_it_to_subtask() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let next_agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let root = seed_task(&db, &project_id, "root", "todo", 1).await;
    let child = seed_subtask(&db, &root, "child", "todo", 0).await;
    assign_role(
        &db,
        &root.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    // The first pass starts the coordination root (todo -> in_progress);
    // its first subtask is dispatched on the next.
    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("the root starts"),
        1
    );
    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        1
    );
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, child.id);
    let execution = ExecutionRepo::latest_agent_execution_by_task(&*db, &child.id)
        .await
        .expect("execution loads")
        .expect("child execution exists");
    assert_eq!(execution.agent_id.as_deref(), Some(agent_id.as_str()));
    assert!(
        TaskRoleAssignmentRepo::get_by_task_and_role(
            &*db,
            &child.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("child assignment loads")
        .is_none(),
        "inherited dispatch must not materialize a child assignment"
    );

    crate::test_support::configure_project_execution_test_setup(
        &db,
        &project_id,
        &next_agent_id,
        &next_agent_id,
    )
    .await;
    let now = now_rfc3339();
    dispatcher
        .task_service
        .reassign_role(
            CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: root.id.clone(),
                role_name: crate::workflow::default_roles::CODER.to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(next_agent_id.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
            false,
            false,
        )
        .await
        .expect("root default worker changes while child runs");
    let running_child = TaskRepo::get_by_id(&*db, &child.id, false)
        .await
        .expect("child reloads")
        .expect("child exists");
    assert!(
        dispatcher
            .task_service
            .execution_owns_current_role_attempt(&running_child, &execution)
            .await
            .expect("execution ownership resolves"),
        "a root default change must not supersede the admitted child execution"
    );
    assert_eq!(
        ExecutionRepo::latest_agent_execution_by_task(&*db, &child.id)
            .await
            .expect("execution reloads")
            .expect("child execution still exists")
            .agent_id
            .as_deref(),
        Some(agent_id.as_str())
    );
}

#[tokio::test]
async fn dispatcher_prefers_subtask_coder_over_root_default() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let root_agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let child_agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let root = seed_task(&db, &project_id, "root", "todo", 1).await;
    let child = seed_subtask(&db, &root, "child", "todo", 0).await;
    assign_role(
        &db,
        &root.id,
        crate::workflow::default_roles::CODER,
        &root_agent,
    )
    .await;
    assign_role(
        &db,
        &child.id,
        crate::workflow::default_roles::CODER,
        &child_agent,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    // The first pass starts the coordination root (todo -> in_progress);
    // its first subtask is dispatched on the next.
    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("the root starts"),
        1
    );
    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        1
    );
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, child.id);
    let execution = ExecutionRepo::latest_agent_execution_by_task(&*db, &child.id)
        .await
        .expect("execution loads")
        .expect("child execution exists");
    assert_eq!(execution.agent_id.as_deref(), Some(child_agent.as_str()));
}

#[tokio::test]
async fn changing_root_default_wakes_and_dispatches_unstarted_subtask() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let first_agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let next_agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let root = seed_task(&db, &project_id, "root", "todo", 1).await;
    let child = seed_subtask(&db, &root, "child", "todo", 0).await;
    assign_role(
        &db,
        &root.id,
        crate::workflow::default_roles::CODER,
        &first_agent,
    )
    .await;
    deferred_dispatch::record_dispatch_disposition(
        &db,
        &child,
        crate::workflow::default_roles::CODER,
        "waiting for a usable default worker",
    )
    .await
    .expect("child dispatch parks");
    crate::test_support::configure_project_execution_test_setup(
        &db,
        &project_id,
        &next_agent,
        &next_agent,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let now = now_rfc3339();
    dispatcher
        .task_service
        .reassign_role(
            CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: root.id.clone(),
                role_name: crate::workflow::default_roles::CODER.to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(next_agent.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
            false,
            false,
        )
        .await
        .expect("root default worker changes");
    dispatcher.task_service.drain(&child.id).await.unwrap();

    let woken_child = TaskRepo::get_by_id(&*db, &child.id, false)
        .await
        .expect("child reloads")
        .expect("child exists");
    assert!(deferred_dispatch::dispatch_disposition_for_test(&woken_child).is_none());
    // The first pass starts the coordination root (todo -> in_progress);
    // its first subtask is dispatched on the next.
    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("the root starts"),
        1
    );
    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        1
    );
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, child.id);
    let execution = ExecutionRepo::latest_agent_execution_by_task(&*db, &child.id)
        .await
        .expect("execution loads")
        .expect("child execution exists");
    assert_eq!(execution.agent_id.as_deref(), Some(next_agent.as_str()));
}

#[tokio::test]
async fn disconnected_owner_is_skipped_before_dispatch_and_never_falls_back() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    let bus = Arc::new(events::EventBus::new(16));
    let service = Arc::new(TaskService::new(db.clone(), bus.clone()));
    let dispatcher = TaskDispatcher::new(db.clone(), bus, service);
    assert_eq!(
        dispatcher
            .list_tasks(&task.project_id, vec![task.status.clone()])
            .await
            .unwrap()
            .len(),
        1
    );
    let project = ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    assert_eq!(
        dispatcher
            .recover_active_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    assert!(!dispatcher
        .dispatch_initial_task(
            &task,
            &super::initial_scheduling::InitialScheduleTarget {
                transition_to: "in_progress".to_owned(),
                role: "coder".to_owned(),
                agent_id: execution.agent_id.clone().unwrap(),
            }
        )
        .await
        .unwrap());
    assert_eq!(
        db::WorkspacePlacementRepo::get_for_task(&*db, &task.id)
            .await
            .unwrap()
            .unwrap()
            .id,
        placement.id
    );
    assert_eq!(
        ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Running
    );
}

#[tokio::test]
async fn revision_two_refused_task_dispatches_after_upgrade_without_manual_action() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let agent = AgentRepo::get_by_id(&*db, &agent_id)
        .await
        .unwrap()
        .unwrap();
    let daemon_id = agent.daemon_id.as_deref().unwrap();
    sqlx::query("UPDATE daemon SET machine_id = 'remote-upgrade-test' WHERE id = ?")
        .bind(daemon_id)
        .execute(db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    let runtime = db::RuntimeRepo::create(
        &*db,
        db::CreateRuntime {
            id: new_uuid_v4(),
            daemon_id: daemon_id.into(),
            kind: "local".into(),
            workspace_root: workspace_dir.path().to_string_lossy().into(),
            status: db::RuntimeStatus::Ready,
            labels_json: "{}".into(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::RepoLocationRepo::create(
        &*db,
        db::CreateRepoLocation {
            id: new_uuid_v4(),
            repo_id,
            owner_kind: db::RepoLocationOwnerKind::Server,
            daemon_id: Some(daemon_id.into()),
            runtime_id: Some(runtime.id),
            path: repo_dir.path().to_string_lossy().into(),
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
    .unwrap();
    let task = seed_task(&db, &project_id, "upgrade refused", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent_id).await;
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let (connection, _outbound) = crate::daemon_transport::DaemonConnection::new(daemon_id.into());
    let id = connection.id();
    registry.register(daemon_id.into(), connection);
    registry.dispatch_incoming_for_connection(daemon_id, id, api_types::DaemonFrame::Notification {
        method:api_types::METHOD_DAEMON_HANDSHAKE.into(),
        params:serde_json::json!({"protocol_revision":2,"capabilities":["execution.terminal.usage_reports","execution.terminal.ack"]}),
    });
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspace_dir.path()).await;
    let service = Arc::new(
        dispatcher
            .task_service
            .as_ref()
            .clone()
            .with_daemon_connections(registry.clone()),
    );
    let dispatcher = TaskDispatcher::new(db.clone(), dispatcher.event_bus.clone(), service);
    dispatcher.check_once_and_drain().await.unwrap();
    let refused = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(refused.status, "todo");
    let annotation: serde_json::Value = serde_json::from_str(
        refused
            .error_annotation
            .as_deref()
            .expect("upgrade annotation"),
    )
    .unwrap();
    assert_eq!(annotation["code"], api_types::DAEMON_UPGRADE_REQUIRED);
    assert_eq!(annotation["daemon_ids"], serde_json::json!([daemon_id]));
    assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
        .await
        .unwrap()
        .is_empty());
    assert!(db::WorkspacePlacementRepo::get_for_task(&*db, &task.id)
        .await
        .unwrap()
        .is_none());
    assert!(rx.try_recv().is_err());
    // The refusal is parked; the accepted handshake wakes it through the monitor.
    dispatcher.check_once_and_drain().await.unwrap();
    let (connection, mut upgraded_outbound) =
        crate::daemon_transport::DaemonConnection::new(daemon_id.into());
    let id = connection.id();
    registry.register(daemon_id.into(), connection);
    registry.dispatch_incoming_for_connection(daemon_id, id, api_types::DaemonFrame::Notification {
        method:api_types::METHOD_DAEMON_HANDSHAKE.into(),
        params:serde_json::json!({"protocol_revision":api_types::DAEMON_PROTOCOL_REVISION,"capabilities":["workspace.v1","journal.ack","execution.terminal.usage_reports"],
            "executor_capabilities":{"shell":{"cancel_ack":true,"terminal_observed":true}},"workspace_run_policy":{"allowed_purposes":["ci_step","hook","environment_setup"]}}),
    });
    let responses = registry.clone();
    let response_daemon = daemon_id.to_owned();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let responder = tokio::spawn(async move {
        while let Some(api_types::DaemonFrame::Request {
            id: request_id,
            method,
            params,
        }) = upgraded_outbound.recv().await
        {
            let result = match method.as_str() {
                api_types::METHOD_REPO_LOCATION_VERIFY => serde_json::json!({
                    "repo_location_id":params["repo_location_id"],"path":params["path"],
                    "default_branch_sha":"verified-head","origin_url":null,
                    "probe_content":params["probe"]["content"],
                }),
                api_types::METHOD_EXECUTION_START => {
                    started_tx
                        .send(params["execution_id"].as_str().unwrap().to_owned())
                        .unwrap();
                    serde_json::json!({"execution_id":params["execution_id"],"accepted":true})
                }
                _ => panic!("unexpected owner request {method}"),
            };
            responses.dispatch_incoming_for_connection(
                &response_daemon,
                id,
                api_types::DaemonFrame::Response {
                    id: request_id,
                    result,
                },
            );
        }
    });
    crate::HeartbeatMonitor::new(db.clone(), dispatcher.event_bus.clone())
        .with_daemon_connections(registry)
        .check_once()
        .await
        .unwrap();
    let woken = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(woken.error_annotation.is_none());
    dispatcher.check_once_and_drain().await.unwrap();
    let executions = ExecutionRepo::list_running_by_task(&*db, &task.id)
        .await
        .unwrap();
    assert_eq!(
        executions.len(),
        1,
        "{:?}",
        TaskRepo::get_by_id(&*db, &task.id, false).await.unwrap()
    );
    let started = tokio::time::timeout(Duration::from_secs(5), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started, executions[0].id);
    responder.abort();
}
