//! Execution owner recovery cases.

use super::*;

#[tokio::test]
async fn daemon_report_reconcile_interrupts_missing_old_running_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_service = Arc::new(TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)));
    let service =
        DaemonService::new(Arc::clone(&db), Arc::clone(&event_bus)).with_task_service(task_service);
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let (agent_id, agent) =
        seed_agent_with_daemon(&db, "remote-machine-report", AgentStatus::Idle).await;
    let daemon_id = agent.daemon_id.clone().expect("daemon id");
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent_id.clone()),
    )
    .await;
    let execution = seed_running_execution_with_created_at(
        &db,
        task.id.clone(),
        agent_id,
        "1970-01-01T00:00:00+00:00",
    )
    .await;

    service
        .ingest_report(
            &daemon_id,
            DaemonReportInput {
                max_concurrent_runs: None,
                detected_clis: vec![DetectedCliInput {
                    kind: "shell".to_owned(),
                    availability: "authenticated".to_owned(),
                    config_path: None,
                    version: None,
                    path: None,
                }],
                runtimes: Vec::new(),
                labels: None,
                active_execution_ids: Some(Vec::new()),
            },
        )
        .await
        .expect("report ingests");

    let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(updated.status, ExecutionStatus::Failed);
    assert_eq!(updated.stop_reason, Some(StopReason::DaemonDisconnected));
}

#[tokio::test]
async fn daemon_report_reconcile_leaves_fresh_running_execution_untouched() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = DaemonService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let (agent_id, agent) =
        seed_agent_with_daemon(&db, "remote-machine-fresh", AgentStatus::Idle).await;
    let daemon_id = agent.daemon_id.clone().expect("daemon id");
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent_id.clone()),
    )
    .await;
    let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;

    service
        .ingest_report(
            &daemon_id,
            DaemonReportInput {
                max_concurrent_runs: None,
                detected_clis: vec![DetectedCliInput {
                    kind: "shell".to_owned(),
                    availability: "authenticated".to_owned(),
                    config_path: None,
                    version: None,
                    path: None,
                }],
                runtimes: Vec::new(),
                labels: None,
                active_execution_ids: Some(Vec::new()),
            },
        )
        .await
        .expect("report ingests");

    let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(updated.status, ExecutionStatus::Running);
}

#[tokio::test]
async fn daemon_report_without_active_execution_ids_does_not_reconcile() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = DaemonService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let (agent_id, agent) =
        seed_agent_with_daemon(&db, "remote-machine-none", AgentStatus::Idle).await;
    let daemon_id = agent.daemon_id.clone().expect("daemon id");
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent_id.clone()),
    )
    .await;
    let execution = seed_running_execution_with_created_at(
        &db,
        task.id.clone(),
        agent_id,
        "1970-01-01T00:00:00+00:00",
    )
    .await;

    service
        .ingest_report(
            &daemon_id,
            DaemonReportInput {
                max_concurrent_runs: None,
                detected_clis: vec![DetectedCliInput {
                    kind: "shell".to_owned(),
                    availability: "authenticated".to_owned(),
                    config_path: None,
                    version: None,
                    path: None,
                }],
                runtimes: Vec::new(),
                labels: None,
                active_execution_ids: None,
            },
        )
        .await
        .expect("report ingests");

    let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(updated.status, ExecutionStatus::Running);
}

#[tokio::test]
async fn execution_daemon_resolution_follows_placement_instead_of_agent_pin() {
    let db = sqlite_db().await;
    let (project_id, repo_id) = seed_project_repo(&db).await;
    let (_, placed_agent) = seed_agent_with_daemon(&db, "placement-owner", AgentStatus::Idle).await;
    let (agent_id, _) = seed_agent_with_daemon(&db, "different-agent-pin", AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent_id.clone()),
    )
    .await;
    let now = now_rfc3339();
    let workspace = db::WorkspaceRepo::create(
        &db,
        db::CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: "server-owned-handle".to_owned(),
            branch: "task/placed".to_owned(),
            status: db::WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");
    let location = db::RepoLocationRepo::create(
        &db,
        db::CreateRepoLocation {
            id: new_uuid_v4(),
            repo_id,
            owner_kind: db::RepoLocationOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            path: "verified-shared-checkout".to_owned(),
            kind: db::RepoLocationKind::SharedMount,
            is_default: true,
            status: db::RepoLocationStatus::Ready,
            last_verified_at: Some(now.clone()),
            last_error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("location creates");
    let owner_id = placed_agent.daemon_id.expect("owner daemon exists");
    db::WorkspacePlacementRepo::create(
        &db,
        db::CreateWorkspacePlacement {
            id: new_uuid_v4(),
            workspace_id: workspace.id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            owner_kind: db::PlacementOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            repo_location_id: location.id,
            execution_daemon_id: Some(owner_id.clone()),
            workspace_handle: Some("server-owned-handle".to_owned()),
            generation: 1,
            state: db::PlacementState::Ready,
            selected_by: db::PlacementSelectedBy::Scheduler,
            selection_reason: "{}".to_owned(),
            reserved_until: None,
            disconnected_at: None,
            failure_cause: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("placement creates");
    let mut execution = seed_running_execution(&db, task.id, agent_id, None).await;
    execution.workspace_id = Some(workspace.id);
    let (resolved_id, _) = resolve_execution_daemon(&db, &execution)
        .await
        .expect("daemon resolves")
        .expect("placed daemon exists");
    assert_eq!(resolved_id, owner_id);
    assert!(execution_is_remote_owned(&db, &execution)
        .await
        .expect("owner classifies"));
}
