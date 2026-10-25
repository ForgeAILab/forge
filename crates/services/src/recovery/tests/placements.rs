//! Placement monitor suspension, guards and reconcile sweeps.

use super::*;

#[tokio::test]
async fn daemon_owned_workspace_daemon_monitor_suspends_ready_placements() {
    let db = Arc::new(sqlite_db().await);
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    let mut update = placement_update(&placement, PlacementState::Ready, None);
    update.disconnected_at = Some(None);
    WorkspacePlacementRepo::update(&*db, update).await.unwrap();
    DaemonRepo::mark_offline(
        &*db,
        placement.daemon_id.as_deref().unwrap(),
        &now_rfc3339(),
    )
    .await
    .unwrap();
    crate::daemon_monitor::DaemonMonitor::new(db.clone(), Arc::new(EventBus::new(16)))
        .check_once()
        .await
        .unwrap();
    let disconnected = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(disconnected.state, PlacementState::Disconnected);
    assert!(disconnected.disconnected_at.is_some());
    assert_eq!(
        disconnected.failure_cause,
        Some(PlacementFailureCause::OwnerDisconnected)
    );
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap(),
        execution
    );
}

#[tokio::test]
async fn daemon_owned_workspace_cleanup_ack_is_owner_and_generation_fenced() {
    let db = Arc::new(sqlite_db().await);
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET status = 'cancelled' WHERE id = ?")
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    WorkspacePlacementRepo::update(
        &*db,
        placement_update(&placement, PlacementState::Cleaning, None),
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE workspace
             SET cleanup_attempts = 4, last_cleanup_error = 'prior cleanup failure'
             WHERE id = ?",
    )
    .bind(&placement.workspace_id)
    .execute(db.pool())
    .await
    .unwrap();
    let mut report: api_types::WorkspaceCleanupResult = serde_json::from_value(json!({
        "entry_id": new_uuid_v4(), "operation_id": new_uuid_v4(),
        "workspace_handle": placement.workspace_handle, "generation": 2, "cleaned": true,
    }))
    .unwrap();
    use crate::daemon_transport::DaemonTerminalDisposition;
    assert_eq!(
        apply_owner_cleanup(&db, placement.daemon_id.as_deref().unwrap(), &report)
            .await
            .unwrap(),
        DaemonTerminalDisposition::Ignore
    );
    report.generation = 1;
    assert_eq!(
        apply_owner_cleanup(&db, "wrong-owner", &report)
            .await
            .unwrap(),
        DaemonTerminalDisposition::Ignore
    );
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Cleaning
    );
    assert_eq!(
        apply_owner_cleanup(&db, placement.daemon_id.as_deref().unwrap(), &report)
            .await
            .unwrap(),
        DaemonTerminalDisposition::Acknowledge
    );
    assert_eq!(
        apply_owner_cleanup(&db, placement.daemon_id.as_deref().unwrap(), &report)
            .await
            .unwrap(),
        DaemonTerminalDisposition::Acknowledge
    );
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Cleaned
    );
    let workspace = WorkspaceRepo::get_by_id(&*db, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
    assert_eq!(workspace.cleanup_attempts, 0);
    assert!(workspace.last_cleanup_error.is_none());
}

#[tokio::test]
async fn daemon_owned_workspace_timeout_rejects_late_terminal_and_outbox() {
    use crate::daemon_transport::{
        DaemonExecutionEventHandler, DaemonTerminalDisposition, ServerExecutionEventSink,
    };
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    HeartbeatMonitor::new(db.clone(), bus.clone())
        .with_max_disconnect(Duration::from_secs(300))
        .check_once()
        .await
        .unwrap();
    let failed = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    let service = Arc::new(TaskService::new(db.clone(), bus.clone()));
    let root = tempfile::tempdir().unwrap();
    let sink = ServerExecutionEventSink::new(db.clone(), bus, root.path().to_path_buf());
    sink.set_task_service(Arc::downgrade(&service));
    let terminal = serde_json::from_value(json!({"terminal_report_id": new_uuid_v4(), "execution_id": execution.id,
            "exit_code": 0, "ts": now_rfc3339(), "status": "completed", "usage_reports": [],
            "outbox_entries": [{"type": "worklog", "position": "1", "kind": "validation", "summary": "Late result"}]})).unwrap();
    assert_eq!(
        sink.handle_terminal_with_ack(placement.daemon_id.as_deref().unwrap(), 1, terminal)
            .await
            .unwrap(),
        DaemonTerminalDisposition::Ignore
    );
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap(),
        failed
    );
    let comments: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM task_comment WHERE task_id = ? AND idempotency_key = ?",
    )
    .bind(task.id)
    .bind(format!("outbox:{}:worklog:1", execution.id))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(comments, 0);
}

#[tokio::test]
async fn placement_monitor_stop_aborts_workers_and_releases_guards() {
    let db = Arc::new(sqlite_db().await);
    let monitor = HeartbeatMonitor::new(db, Arc::new(EventBus::default()));
    let keys = vec!["daemon:stopped-owner".to_owned()];
    monitor
        .placement_in_flight
        .lock()
        .unwrap()
        .extend(keys.clone());
    let guard = PlacementAttemptGuard {
        in_flight: monitor.placement_in_flight.clone(),
        keys,
    };
    let workers = monitor.placement_workers.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel::<()>();
    let worker = tokio::spawn(async move {
        let _finished = finished_tx;
        let _guard = guard;
        let _permit = workers.acquire_owned().await.unwrap();
        started_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    monitor
        .placement_worker_handles
        .lock()
        .unwrap()
        .push(worker);
    started_rx.await.unwrap();
    monitor.stop();
    assert!(finished_rx.await.is_err(), "stop aborts the owner worker");
    assert!(monitor.placement_in_flight.lock().unwrap().is_empty());
    assert_eq!(monitor.placement_workers.available_permits(), 2);
}

#[test]
fn placement_attempt_guard_recovers_a_poisoned_mutex() {
    let in_flight = Arc::new(Mutex::new(HashSet::from(["placement:poisoned".into()])));
    let poisoned = in_flight.clone();
    assert!(std::panic::catch_unwind(move || {
        let _lock = poisoned.lock().unwrap();
        panic!("poison fixture");
    })
    .is_err());
    drop(PlacementAttemptGuard {
        in_flight: in_flight.clone(),
        keys: vec!["placement:poisoned".into()],
    });
    assert!(in_flight
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_empty());
}

#[tokio::test]
async fn placement_sweep_skips_ready_workspaces_without_terminal_or_recent_reconcile() {
    let db = Arc::new(sqlite_db().await);
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
    WorkspacePlacementRepo::update(
        &*db,
        placement_update(&placement, PlacementState::Ready, None),
    )
    .await
    .unwrap();
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let (_, mut outbound) =
        owner_connection(&registry, placement.daemon_id.as_deref().unwrap(), false);
    let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::default()))
        .with_daemon_connections(registry);
    monitor.check_once().await.unwrap();
    monitor.finish_placement_workers().await;
    assert!(
        outbound.try_recv().is_err(),
        "no workspace RPC for an unrelated ready placement"
    );
}

#[tokio::test]
async fn heartbeat_placement_timeout_does_not_delay_liveness_or_next_owner() {
    let db = Arc::new(sqlite_db().await);
    let (_, bad, bad_execution) = daemon_owned_fixture(&db).await;
    let (_, good, good_execution) = daemon_owned_fixture(&db).await;
    for execution in [&bad_execution, &good_execution] {
        sqlx::query("UPDATE execution SET status = 'completed', after_sha = 'base-head', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
                .bind(&execution.id).execute(db.pool()).await.unwrap();
    }
    let stale = seed_agent(&db, AgentStatus::Busy, Some("1970-01-01T00:00:00Z".into())).await;
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let (_, mut silent) = owner_connection(&registry, bad.daemon_id.as_deref().unwrap(), false);
    let daemon_id = good.daemon_id.clone().unwrap();
    let (connection_id, mut responsive) = owner_connection(&registry, &daemon_id, false);
    let responder = {
        let registry = registry.clone();
        tokio::spawn(async move {
            let api_types::DaemonFrame::Request { id, method, params } =
                responsive.recv().await.unwrap()
            else {
                panic!("describe request")
            };
            assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
            registry.dispatch_incoming_for_connection(&daemon_id, connection_id,
                    api_types::DaemonFrame::Response { id, result: json!({"workspace_handle": params["workspace_handle"],
                        "generation": 1, "exists": true, "head_sha": "base-head", "dirty": false,
                        "branch": "task/remote", "locked": false, "active_execution_ids": [], "journaled_execution_ids": []}) });
        })
    };
    let bus = Arc::new(EventBus::new(32));
    let mut events = bus.subscribe();
    let monitor =
        Arc::new(HeartbeatMonitor::new(db.clone(), bus).with_daemon_connections(registry));
    let tick = {
        let monitor = monitor.clone();
        tokio::spawn(async move { monitor.check_once().await })
    };
    silent.recv().await.unwrap();
    assert_eq!(
        AgentRepo::get_by_id(&*db, &stale.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        AgentStatus::Error,
        "core liveness must finish before a silent owner RPC starts"
    );
    tick.await.unwrap().unwrap();
    responder.await.unwrap();
    // Wait for the responsive owner's committed reconciliation before
    // pausing time; SQLite work must not race Tokio's auto-advance.
    loop {
        let event = events.recv().await.unwrap();
        if event.event_type == "reconciliation.event" && event.entity_id == good.task_id {
            break;
        }
    }
    // Only the silent owner's RPC deadline remains.

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(
        api_types::DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
    ))
    .await;
    tokio::time::resume();
    monitor.finish_placement_workers().await;
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &good.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Ready
    );
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &bad.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Disconnected
    );
}

#[tokio::test]
async fn placement_reconcile_accepts_forge_rebase_head() {
    let db = Arc::new(sqlite_db().await);
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET status = 'completed', after_sha = 'old-head', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
    let placement = WorkspacePlacementRepo::update(
        &*db,
        placement_update(&placement, PlacementState::Ready, None),
    )
    .await
    .unwrap();
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
    let responder = {
        let registry = registry.clone();
        let daemon_id = daemon_id.clone();
        tokio::spawn(async move {
            for step in 0..4 {
                let api_types::DaemonFrame::Request { id, method, params } =
                    outbound.recv().await.unwrap()
                else {
                    panic!("owner rebase request")
                };
                let result = match step {
                    0 | 3 => {
                        assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                        json!({"workspace_handle": params["workspace_handle"], "generation": 1, "exists": true,
                                "head_sha": if step == 0 { "old-head" } else { "rebased-head" }, "dirty": false,
                                "branch": "task/remote", "locked": false, "active_execution_ids": [], "journaled_execution_ids": []})
                    }
                    1 => {
                        assert_eq!(method, api_types::METHOD_WORKSPACE_RESET);
                        json!({"entry_id": "rebase-entry", "operation_id": params["operation_id"], "outcome": {"kind": "rebased"}})
                    }
                    _ => {
                        assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
                        json!({"entry_id": params["entry_id"], "acknowledged": true})
                    }
                };
                registry.dispatch_incoming_for_connection(
                    &daemon_id,
                    connection_id,
                    api_types::DaemonFrame::Response { id, result },
                );
            }
        })
    };
    let resolved = crate::workspace_backend::ResolvedWorkspace {
        placement: placement.clone(),
        backend: Arc::new(crate::workspace_backend::DaemonWorkspaceBackend::new(
            db.clone(),
            registry.clone(),
        )),
    };
    assert!(matches!(
        resolved.rebase_target("main", false).await.unwrap(),
        api_types::WorkspaceOwnerOperationOutcome::Rebased
    ));
    let facts = crate::integration_effects::rebase::rebase_head_facts(
        resolved
            .backend
            .describe(&resolved.placement)
            .await
            .unwrap(),
    )
    .unwrap();
    resolved.record_rebase_head(&db, facts).await.unwrap();
    sqlx::query("UPDATE execution SET updated_at = ? WHERE id = ?")
        .bind((Utc::now() + ChronoDuration::minutes(1)).to_rfc3339())
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    responder.await.unwrap();
    let placement = WorkspacePlacementRepo::update(
        &*db,
        placement_update(&placement, PlacementState::Disconnected, None),
    )
    .await
    .unwrap();
    let state = serde_json::from_value(json!({"workspace_handle": placement.workspace_handle, "generation": 1,
            "exists": true, "head_sha": "rebased-head", "dirty": false, "branch": "task/remote", "locked": false,
            "active_execution_ids": [], "journaled_execution_ids": []})).unwrap();
    assert!(complete_workspace_reconciliation(
        &db,
        &EventBus::new(32),
        &registry,
        None,
        &placement,
        state,
        false,
        connection_id
    )
    .await
    .unwrap());
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Ready
    );
}

#[tokio::test]
async fn placement_crash_orphaned_preparing_without_expiry_releases_capacity() {
    let db = Arc::new(sqlite_db().await);
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET status = 'completed' WHERE id = ?")
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE workspace_placement SET state = 'preparing', reserved_until = NULL, updated_at = ? WHERE id = ?")
            .bind((Utc::now() - ChronoDuration::minutes(11)).to_rfc3339())
            .bind(&placement.id).execute(db.pool()).await.unwrap();
    assert_eq!(
        crate::placement::admission::sweep_expired_reservations(&db, &now_rfc3339())
            .await
            .unwrap(),
        1
    );
    let expired = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expired.state, PlacementState::Failed);
    assert!(expired.workspace_handle.is_none());
    assert_eq!(
        expired.failure_cause,
        Some(PlacementFailureCause::PrepareFailed)
    );
    assert_eq!(
        crate::agent_capacity::count_occupied_agent_slots(
            &db,
            placement.agent_id.as_deref().unwrap()
        )
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn daemon_owned_workspace_reservation_expiry_runs_without_active_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE project SET paused_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&task.project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let mut update = placement_update(&placement, PlacementState::Reserved, None);
    update.reserved_until = Some(Some((Utc::now() - ChronoDuration::minutes(1)).to_rfc3339()));
    WorkspacePlacementRepo::update(&*db, update).await.unwrap();
    HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
        .check_once()
        .await
        .unwrap();
    let expired = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expired.state, PlacementState::Failed);
    assert_eq!(
        expired.failure_cause,
        Some(PlacementFailureCause::PrepareFailed)
    );
    let executions: i64 = sqlx::query_scalar("SELECT count(*) FROM execution WHERE task_id = ?")
        .bind(&task.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(executions, 1);
}

#[tokio::test]
async fn daemon_owned_workspace_sweep_reconciles_operations_then_retries_cleaned_acknowledgements()
{
    use crate::daemon_transport::workspace_client::DaemonWorkspaceClient;
    use crate::daemon_transport::{
        DaemonExecutionEventHandler, DaemonTerminalDisposition, ServerExecutionEventSink,
    };
    let db = Arc::new(sqlite_db().await);
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
    WorkspacePlacementRepo::update(
        &*db,
        placement_update(&placement, PlacementState::Cleaning, None),
    )
    .await
    .unwrap();
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
    let client = DaemonWorkspaceClient::new(registry.clone()).with_receipts(db.clone());
    let params = json!({"daemon_id": daemon_id, "runtime_id": placement.runtime_id,
            "placement_id": placement.id, "workspace_handle": placement.workspace_handle,
            "generation": 1, "operation_id": "run-receipt", "expected": {"kind": "base_sha", "sha": "base-head"},
            "purpose": "ci_step", "command": "printf once", "env": [], "timeout_secs": 0, "max_output_bytes": 1024});
    client
        .remember_mutation(
            &daemon_id,
            api_types::METHOD_WORKSPACE_RUN,
            params.clone(),
            None,
        )
        .await
        .unwrap();
    let sink = ServerExecutionEventSink::new(
        db.clone(),
        Arc::new(EventBus::new(32)),
        std::path::PathBuf::new(),
    );
    sink.set_connection_registry(Arc::downgrade(&registry));
    let cleanup: api_types::WorkspaceCleanupResult = serde_json::from_value(json!({"entry_id": "cleaned-entry", "operation_id": "cleanup-receipt", "workspace_handle": placement.workspace_handle,
            "generation": 1, "cleaned": true})).unwrap();
    for _ in 0..2 {
        assert_eq!(
            sink.handle_workspace_cleanup(&daemon_id, connection_id, cleanup.clone())
                .await
                .unwrap(),
            DaemonTerminalDisposition::Acknowledge
        );
    }
    let responder = {
        let registry = registry.clone();
        tokio::spawn(async move {
            for step in 0..3 {
                let api_types::DaemonFrame::Request { id, method, params } =
                    outbound.recv().await.unwrap()
                else {
                    panic!("receipt reconciliation request");
                };
                let result = if step == 0 {
                    assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                    assert_eq!(params["operation"], "reconcile");
                    assert_eq!(params["operation_id"], "run-receipt");
                    json!({"entry_id": "run-entry", "operation_id": "run-receipt", "outcome": {"kind": "result", "result": {
                            "entry_id": "run-entry", "operation_id": "run-receipt", "exit_code": 0, "stdout": "once", "stderr": "",
                            "duration_ms": 1, "timed_out": false, "stdout_truncated": false, "stderr_truncated": false}}})
                } else {
                    assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
                    assert_eq!(
                        params["entry_id"],
                        if step == 1 {
                            "run-entry"
                        } else {
                            "cleaned-entry"
                        }
                    );
                    json!({"entry_id": params["entry_id"], "acknowledged": true})
                };
                registry.dispatch_incoming_for_connection(
                    &daemon_id,
                    connection_id,
                    api_types::DaemonFrame::Response { id, result },
                );
            }
        })
    };
    let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
        .with_daemon_connections(registry);
    monitor.check_once().await.unwrap();
    responder.await.unwrap();
    monitor.check_once().await.unwrap();
    monitor.finish_placement_workers().await;
    let acknowledgements: i64 = sqlx::query_scalar("SELECT count(*) FROM command_receipt WHERE operation IN ('daemon.workspace.run.ack', 'daemon.workspace.cleanup.ack')")
            .fetch_one(db.pool()).await.unwrap();
    assert_eq!(acknowledgements, 2);
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Cleaned
    );
}
