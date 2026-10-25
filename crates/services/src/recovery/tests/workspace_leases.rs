//! Workspace lease renewal, expiry and disconnect ordering.

use super::*;

#[tokio::test]
async fn heartbeat_skips_agent_timeout_when_owner_suspension_fails() {
    let db = Arc::new(sqlite_db().await);
    let agent = seed_agent(
        &db,
        AgentStatus::Busy,
        Some("1970-01-01T00:00:00+00:00".to_owned()),
    )
    .await;
    let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(16)));

    assert_eq!(monitor.check_agent_timeouts_for_tick(false).await, 0);
    assert_eq!(
        AgentRepo::get_by_id(&*db, &agent.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        AgentStatus::Busy
    );
}

#[tokio::test]
async fn heartbeat_skips_workspace_lease_expiry_when_renewal_fails() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(16));
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    let grant = expired_owner_grant(&db, &task, &placement, &execution).await;
    let monitor = HeartbeatMonitor::new(db.clone(), bus);

    assert_eq!(monitor.expire_workspace_leases_for_tick(false).await, 0);
    assert_eq!(
        WorkspaceLeaseRepo::get_by_id(&*db, &grant.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "active"
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
async fn worker_robustness_heartbeat_runs_lease_expiry_after_early_failure() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    let ready = WorkspacePlacementRepo::update(
        &*db,
        placement_update(&placement, PlacementState::Ready, None),
    )
    .await
    .unwrap();
    let now = Utc::now();
    let db::ExecutionLeaseMutation::Updated(execution) = ExecutionRepo::claim_lease(
        &*db,
        db::ClaimExecutionLease {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            owner: execution.lease_owner.clone().unwrap(),
            lease_expires_at: (now + ChronoDuration::hours(1)).to_rfc3339(),
            hard_deadline_at: execution.hard_deadline_at.clone(),
            now: now.to_rfc3339(),
        },
    )
    .await
    .unwrap() else {
        panic!("execution lease updates");
    };
    let grant = expired_owner_grant(&db, &task, &ready, &execution).await;
    sqlx::query(
        "UPDATE agent_identity
             SET status = 'busy', heartbeat_interval_seconds = 'invalid'
             WHERE id = ?",
    )
    .bind(execution.agent_id.as_deref().unwrap())
    .execute(db.pool())
    .await
    .unwrap();

    HeartbeatMonitor::new(db.clone(), bus)
        .check_once()
        .await
        .expect("later heartbeat passes still run");

    assert_eq!(
        WorkspaceLeaseRepo::get_by_id(&*db, &grant.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "expired"
    );
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Failed
    );
}

#[tokio::test]
async fn daemon_owned_workspace_lease_expiry_before_socket_disconnect_reconciles_terminal_once() {
    assert_owner_disconnect_order(false).await;
}

#[tokio::test]
async fn daemon_owned_workspace_socket_disconnect_before_lease_expiry_reconciles_terminal_once() {
    assert_owner_disconnect_order(true).await;
}

#[tokio::test]
async fn revision_two_ready_placement_disconnects_and_expires_despite_rest_online() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::default());
    let (_, placement, _) = daemon_owned_fixture(&db).await;
    let mut update = placement_update(&placement, PlacementState::Ready, None);
    update.disconnected_at = Some(None);
    let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let daemon_id = ready.daemon_id.as_deref().unwrap();
    let (connection, _outbound) = crate::daemon_transport::DaemonConnection::new(daemon_id.into());
    let id = connection.id();
    registry.register(daemon_id.into(), connection);
    registry.dispatch_incoming_for_connection(daemon_id, id, api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.into(),
            params: json!({"protocol_revision":2,"capabilities":["execution.terminal.usage_reports","execution.terminal.ack"]}),
        });
    let monitor = HeartbeatMonitor::new(db.clone(), bus)
        .with_daemon_connections(registry)
        .with_max_disconnect(Duration::from_secs(86400));
    monitor.check_once().await.unwrap();
    let disconnected = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(disconnected.state, PlacementState::Disconnected);
    assert!(disconnected.disconnected_at.is_some());
    let mut expired = placement_update(
        &disconnected,
        PlacementState::Disconnected,
        Some(PlacementFailureCause::OwnerDisconnected),
    );
    expired.disconnected_at = Some(Some((Utc::now() - ChronoDuration::hours(25)).to_rfc3339()));
    WorkspacePlacementRepo::update(&*db, expired).await.unwrap();
    monitor.check_once().await.unwrap();
    let failed = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.state, PlacementState::Failed);
    assert_eq!(
        failed.failure_cause,
        Some(PlacementFailureCause::OwnerDisconnectedTimeout)
    );
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_daemon_connections(monitor.daemon_connections.as_ref().unwrap().clone());
    let error = service
        .test_apply_action(
            ready.task_id,
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, ServiceError::DaemonUpgradeRequired { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn daemon_owned_workspace_lease_expiry_cas_loses_to_heartbeat_and_socket_disconnect() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    let mut update = placement_update(&placement, PlacementState::Ready, None);
    update.disconnected_at = Some(None);
    let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
    let now = Utc::now();
    let claimed = ExecutionRepo::claim_lease(
        &*db,
        db::ClaimExecutionLease {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            owner: execution.lease_owner.clone().unwrap(),
            lease_expires_at: (now + ChronoDuration::minutes(1)).to_rfc3339(),
            hard_deadline_at: execution.hard_deadline_at.clone(),
            now: now.to_rfc3339(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(claimed, db::ExecutionLeaseMutation::Updated(_)));
    assert!(WorkspacePlacementRepo::suspend_expired_execution_lease(
        &*db,
        &ready,
        &execution,
        &now.to_rfc3339()
    )
    .await
    .unwrap()
    .is_none());
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap(),
        ready
    );
    assert_eq!(
        disconnect_daemon_placements(&db, &bus, ready.daemon_id.as_deref().unwrap())
            .await
            .unwrap(),
        1
    );
    assert!(WorkspacePlacementRepo::suspend_expired_execution_lease(
        &*db,
        &ready,
        &execution,
        &now.to_rfc3339()
    )
    .await
    .unwrap()
    .is_none());
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap()
            .version,
        ready.version + 1
    );
}

#[tokio::test]
async fn daemon_owned_workspace_lease_expiry_terminal_admission_suspends_before_monitor() {
    use crate::daemon_transport::{
        DaemonExecutionEventHandler, DaemonTerminalDisposition, ServerExecutionEventSink,
    };
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    let mut update = placement_update(&placement, PlacementState::Ready, None);
    update.disconnected_at = Some(None);
    let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let sink = ServerExecutionEventSink::new(db.clone(), bus.clone(), root.path().to_path_buf());
    let service = Arc::new(TaskService::new(db.clone(), bus));
    sink.set_task_service(Arc::downgrade(&service));
    let notification = serde_json::from_value(json!({
        "terminal_report_id": new_uuid_v4(), "execution_id": execution.id, "exit_code": 0,
        "ts": now_rfc3339(), "status": "completed", "after_sha": "owner-head", "usage_reports": [],
    }))
    .unwrap();
    assert_eq!(
        sink.handle_terminal_with_ack(ready.daemon_id.as_deref().unwrap(), 1, notification)
            .await
            .unwrap(),
        DaemonTerminalDisposition::AwaitingCascade
    );
    let disconnected = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(disconnected.state, PlacementState::Disconnected);
    assert_eq!(
        disconnected.failure_cause,
        Some(PlacementFailureCause::OwnerDisconnected)
    );
    assert_eq!(disconnected.version, ready.version + 1);
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Completed
    );
}

#[tokio::test]
async fn daemon_owned_workspace_lease_expiry_heartbeat_thaw_resumes_suspended_lease() {
    use crate::daemon_transport::{DaemonExecutionEventHandler, ServerExecutionEventSink};
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    let mut update = placement_update(&placement, PlacementState::Ready, None);
    update.disconnected_at = Some(None);
    let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let sink = ServerExecutionEventSink::new(db.clone(), bus, root.path().to_path_buf());
    sink.handle_heartbeat(ready.daemon_id.as_deref().unwrap(), 1, 1)
        .await
        .unwrap();
    let current = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, ExecutionStatus::Running);
    assert_eq!(current.hard_deadline_at, execution.hard_deadline_at);
    assert!(rfc3339_is_after(
        current.lease_expires_at.as_deref().unwrap(),
        &now_rfc3339()
    ));
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Disconnected
    );
}

#[tokio::test]
async fn daemon_owned_workspace_lease_expiry_races_socket_disconnect_once() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let mut events = bus.subscribe();
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    let mut update = placement_update(&placement, PlacementState::Ready, None);
    update.disconnected_at = Some(None);
    let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
    let (expired, socket) = tokio::join!(
        suspend_expired_remote_execution(&db, &bus, &execution),
        disconnect_daemon_placements(&db, &bus, ready.daemon_id.as_deref().unwrap()),
    );
    assert!(expired.unwrap());
    assert!(socket.unwrap() <= 1);
    let disconnected = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(disconnected.version, ready.version + 1);
    assert_eq!(disconnected.state, PlacementState::Disconnected);
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap(),
        execution
    );
    let mut disconnects = 0;
    while let Ok(event) = events.try_recv() {
        disconnects += u64::from(event.event_type == "workspace.disconnected");
    }
    assert_eq!(disconnects, 1);
}

#[tokio::test]
async fn server_workspace_remote_provider_lease_expiry_suspends_execution() {
    use crate::daemon_transport::{
        DaemonExecutionEventHandler, DaemonTerminalDisposition, ServerExecutionEventSink,
    };
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    let location = db::RepoLocationRepo::get_by_id(&*db, &placement.repo_location_id)
        .await
        .unwrap()
        .unwrap();
    let shared = db::RepoLocationRepo::create(
        &*db,
        db::CreateRepoLocation {
            id: new_uuid_v4(),
            repo_id: location.repo_id,
            owner_kind: db::RepoLocationOwnerKind::Server,
            daemon_id: placement.execution_daemon_id.clone(),
            runtime_id: location.runtime_id,
            path: "/server/worktree".to_owned(),
            kind: db::RepoLocationKind::SharedMount,
            is_default: true,
            status: db::RepoLocationStatus::Ready,
            last_verified_at: Some(now_rfc3339()),
            last_error: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let mut update = placement_update(&placement, PlacementState::Ready, None);
    update.owner_kind = Some(PlacementOwnerKind::Server);
    update.daemon_id = Some(None);
    update.runtime_id = Some(None);
    update.repo_location_id = Some(shared.id);
    update.workspace_handle = Some(Some("/server/worktree".to_owned()));
    update.disconnected_at = Some(None);
    let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
    let monitor = HeartbeatMonitor::new(db.clone(), bus.clone());
    monitor.check_once().await.unwrap();
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap(),
        execution
    );
    let suspended = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(suspended.state, PlacementState::Disconnected);
    assert_eq!(
        suspended.failure_cause,
        Some(PlacementFailureCause::OwnerDisconnected)
    );
    let root = tempfile::tempdir().unwrap();
    let sink = ServerExecutionEventSink::new(db.clone(), bus.clone(), root.path().to_path_buf());
    let service = Arc::new(TaskService::new(db.clone(), bus));
    sink.set_task_service(Arc::downgrade(&service));
    assert_eq!(sink.handle_terminal_with_ack(ready.execution_daemon_id.as_deref().unwrap(), 1,
            serde_json::from_value(json!({
                "terminal_report_id": new_uuid_v4(), "execution_id": execution.id, "exit_code": 0,
                "ts": now_rfc3339(), "status": "completed", "after_sha": "owner-head", "usage_reports": [],
            })).unwrap()).await.unwrap(), DaemonTerminalDisposition::AwaitingCascade);
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Completed
    );
}

#[tokio::test]
async fn server_workspace_embedded_lease_expiry_still_fails_execution() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    let location = db::RepoLocationRepo::get_by_id(&*db, &placement.repo_location_id)
        .await
        .unwrap()
        .unwrap();
    let server_location = db::RepoLocationRepo::create(
        &*db,
        db::CreateRepoLocation {
            id: new_uuid_v4(),
            repo_id: location.repo_id,
            owner_kind: db::RepoLocationOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            path: "/server/repo".to_owned(),
            kind: db::RepoLocationKind::PrimaryCheckout,
            is_default: true,
            status: db::RepoLocationStatus::Ready,
            last_verified_at: Some(now_rfc3339()),
            last_error: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let mut update = placement_update(&placement, PlacementState::Ready, None);
    update.owner_kind = Some(PlacementOwnerKind::Server);
    update.daemon_id = Some(None);
    update.runtime_id = Some(None);
    update.repo_location_id = Some(server_location.id);
    update.execution_daemon_id = Some(None);
    update.disconnected_at = Some(None);
    let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
    let db::ExecutionLeaseMutation::Updated(execution) = ExecutionRepo::claim_lease(
        &*db,
        db::ClaimExecutionLease {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            owner: "embedded:test".to_owned(),
            lease_expires_at: execution.lease_expires_at.clone().unwrap(),
            hard_deadline_at: execution.hard_deadline_at.clone(),
            now: now_rfc3339(),
        },
    )
    .await
    .unwrap() else {
        panic!("embedded owner claims");
    };
    let executor = Arc::new(RecordingCancelExecutor::default());
    let monitor = HeartbeatMonitor::new(db.clone(), bus).with_task_executor(executor.clone());
    assert_eq!(monitor.check_once().await.unwrap(), 1);
    let failed = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, ExecutionStatus::Failed);
    assert_eq!(failed.stop_reason, Some(StopReason::ExecutionStalled));
    assert!(failed
        .error
        .as_deref()
        .unwrap()
        .starts_with("Execution owner lease expired at "));
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap(),
        ready
    );
    assert_eq!(*executor.cancelled.lock().unwrap(), vec![execution.id]);
}
