//! Owner reconciliation recovery cases.

use super::*;

#[tokio::test]
async fn daemon_owned_workspace_disconnect_freezes_expired_lease_without_duplicate_execution() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    // The release's default has no hard deadline. Disconnect suspension
    // still applies; max_disconnect independently bounds the wait.
    sqlx::query("UPDATE execution SET hard_deadline_at = NULL WHERE id = ?")
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    let grant = expired_owner_grant(&db, &task, &placement, &execution).await;
    let monitor = HeartbeatMonitor::new(db.clone(), bus.clone());
    monitor.check_once().await.unwrap();
    CrashRecovery::new(db.clone(), bus)
        .run_recovery()
        .await
        .unwrap();
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Running
    );
    assert_eq!(
        ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Disconnected
    );
    let attention: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM attention_projection WHERE dedupe_key = ? AND status = 'open'",
    )
    .bind(format!("workspace-owner:{}", placement.id))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention, 1);
    assert_eq!(
        WorkspaceLeaseRepo::get_by_id(&*db, &grant.id)
            .await
            .unwrap()
            .unwrap(),
        grant
    );
    let current = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    let suspended = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    let now = now_rfc3339();
    let expires_at = (Utc::now() + ChronoDuration::seconds(60)).to_rfc3339();
    let resumed = WorkspacePlacementRepo::resume_disconnected_execution_lease(
        &*db,
        &suspended,
        &current,
        db::RenewExecutionLease {
            execution_id: current.id.clone(),
            expected_version: current.execution_version,
            owner: current.lease_owner.clone().unwrap(),
            lease_expires_at: expires_at.clone(),
            now,
        },
    )
    .await
    .unwrap();
    let db::ExecutionLeaseMutation::Updated(resumed) = resumed else {
        panic!("unbounded execution resumes");
    };
    assert_eq!(resumed.hard_deadline_at, None);
    assert_eq!(resumed.lease_expires_at, Some(expires_at));
}

#[tokio::test]
async fn daemon_owned_workspace_max_disconnect_fails_once_without_spending_retry_budget() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    let monitor =
        HeartbeatMonitor::new(db.clone(), bus).with_max_disconnect(Duration::from_secs(300));
    monitor.check_once().await.unwrap();
    monitor.check_once().await.unwrap();
    let failed = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, ExecutionStatus::Failed);
    assert_eq!(
        failed.executor_config_snapshot_json,
        execution.executor_config_snapshot_json
    );
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .failure_cause,
        Some(PlacementFailureCause::OwnerDisconnectedTimeout)
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(failed.error.as_deref().unwrap()).unwrap()
            ["cause"],
        "owner_disconnected_timeout"
    );
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let metadata = task
        .metadata_json
        .as_deref()
        .map(|raw| serde_json::from_str::<serde_json::Value>(raw).unwrap())
        .unwrap_or_else(|| json!({}));
    assert!(metadata.get("execution_retry_count").is_none());
    assert!(task.failed_json.is_none());
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM domain_event WHERE event_type = 'execution.failed' AND json_extract(payload_json, '$.execution_id') = ?")
            .bind(&execution.id).fetch_one(db.pool()).await.unwrap();
    assert_eq!(events, 1);
}

#[tokio::test]
async fn daemon_owned_workspace_frozen_lease_does_not_extend_hard_deadline() {
    let db = Arc::new(sqlite_db().await);
    let (_, _, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET hard_deadline_at = ? WHERE id = ?")
        .bind((Utc::now() - ChronoDuration::seconds(1)).to_rfc3339())
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
        .check_once()
        .await
        .unwrap();
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
async fn daemon_owned_workspace_interrupted_reconciliation_completes_through_sweep() {
    let db = Arc::new(sqlite_db().await);
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    // The terminal/drain committed before the server stopped; readiness
    // did not. The new monitor receives no reconnect event.
    sqlx::query("UPDATE task SET status = 'review', entry_barrier_json = ?, error_annotation = ?, blocked_json = ? WHERE id = ?")
            .bind(json!({"state": "review", "status": "blocked", "started_at":"episode"}).to_string())
            .bind(json!({"type": "before_work_hook_failed", "blocking_reason": "review_ci_infrastructure_exhausted", "recovery_actions": ["retry_hook"]}).to_string())
            .bind("{}").bind(&placement.task_id).execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE execution SET status = 'completed', after_sha = 'owner-head', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,'review_ci_infrastructure','episode',5) ON CONFLICT(task_id,kind) DO UPDATE SET spent=5,window_id='episode'").bind(&placement.task_id).execute(db.pool()).await.unwrap();
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (id, mut outbound) = owner_connection(&registry, &daemon_id, false);
    let responder = {
        let registry = registry.clone();
        tokio::spawn(async move {
            let api_types::DaemonFrame::Request {
                id: request_id,
                method,
                params,
            } = outbound.recv().await.unwrap()
            else {
                panic!("describe request");
            };
            assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
            registry.dispatch_incoming_for_connection(&daemon_id, id, api_types::DaemonFrame::Response {
                    id: request_id, result: json!({"workspace_handle": params["workspace_handle"], "generation": 1,
                        "exists": true, "head_sha": "owner-head", "dirty": false, "branch": "task/remote", "locked": false,
                        "active_execution_ids": [], "journaled_execution_ids": []}),
                });
        })
    };
    let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
        .with_daemon_connections(registry);
    monitor.check_once().await.unwrap();
    responder.await.unwrap();
    monitor.finish_placement_workers().await;
    for task_id in sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    {
        crate::test_support::drain_task_steps(&db, &task_id).await;
    }
    let task = TaskRepo::get_by_id(&*db, &placement.task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(task.blocked_json.is_none());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(task.error_annotation.as_deref().unwrap())
            .unwrap()["blocking_reason"],
        "review_ci_infrastructure"
    );
    assert_eq!(
        db::budget::spent(
            db.pool(),
            &task.id,
            db::budget::Kind::ReviewCiInfrastructure.key()
        )
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Ready
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM execution WHERE workspace_id = ?")
        .bind(&placement.workspace_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn daemon_owned_workspace_replayed_terminal_after_ten_minutes_ingests_outbox_once() {
    use crate::daemon_transport::{
        DaemonExecutionEventHandler, DaemonTerminalDisposition, ServerExecutionEventSink,
    };
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    let root = tempfile::tempdir().unwrap();
    let sink = Arc::new(ServerExecutionEventSink::new(
        db.clone(),
        bus.clone(),
        root.path().to_path_buf(),
    ));
    let registry = Arc::new(DaemonConnectionRegistry::new(bus.clone(), sink.clone()));
    sink.set_connection_registry(Arc::downgrade(&registry));
    let service = TaskService::new(db.clone(), bus)
        .with_daemon_connections(registry.clone())
        .with_provider_credential_env(Arc::new(crate::test_support::TestExecutionSupport::new(
            db.clone(),
        )));
    let router = (*service.workspace_backend_router())
        .clone()
        .with_daemon(Arc::new(
            crate::workspace_backend::DaemonWorkspaceBackend::new(db.clone(), registry.clone()),
        ));
    let service = Arc::new(service.with_workspace_backend_router(Arc::new(router)));
    sink.set_task_service(Arc::downgrade(&service));
    let daemon_id = placement.daemon_id.as_deref().unwrap();
    let (connection_id, mut outbound) = owner_connection(&registry, daemon_id, false);
    let notification: api_types::ExecutionTerminalNotification = serde_json::from_value(json!({
            "terminal_report_id": new_uuid_v4(), "execution_id": execution.id, "exit_code": 0,
            "ts": now_rfc3339(), "status": "completed", "after_sha": "owner-head", "usage_reports": [],
            "outbox_entries": [{"type": "worklog", "position": "1", "kind": "validation", "summary": "Owner-only checks passed"}],
        })).unwrap();
    assert_eq!(
        sink.handle_terminal_with_ack(daemon_id, connection_id, notification.clone())
            .await
            .unwrap(),
        DaemonTerminalDisposition::AwaitingCascade
    );
    assert_eq!(
        sink.handle_terminal_with_ack(daemon_id, connection_id, notification.clone())
            .await
            .unwrap(),
        DaemonTerminalDisposition::AwaitingCascade
    );
    // A fresh sink proves the receipt/outbox path also survives restart.
    let restarted = Arc::new(ServerExecutionEventSink::new(
        db.clone(),
        Arc::new(EventBus::new(16)),
        root.path().to_path_buf(),
    ));
    let restarted_registry = Arc::new(DaemonConnectionRegistry::new(
        Arc::new(EventBus::new(16)),
        restarted.clone(),
    ));
    restarted_registry.register(daemon_id.to_owned(), registry.get(daemon_id).unwrap());
    restarted.set_task_service(Arc::downgrade(&service));
    restarted.set_connection_registry(Arc::downgrade(&restarted_registry));
    assert_eq!(
        restarted
            .handle_terminal_with_ack(daemon_id, connection_id, notification.clone())
            .await
            .unwrap(),
        DaemonTerminalDisposition::AwaitingCascade
    );
    let finished = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.status, ExecutionStatus::Completed);
    assert_eq!(finished.after_sha.as_deref(), Some("owner-head"));
    let comments: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM task_comment WHERE task_id = ? AND idempotency_key = ?",
    )
    .bind(task.id)
    .bind(format!("outbox:{}:worklog:1", execution.id))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(comments, 1);
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Disconnected
    );
    // The restarted registry retains the replay until reconciliation
    // commits its cascade, then actively ACKs it without another delivery.
    restarted_registry.dispatch_incoming_for_connection(
        daemon_id,
        connection_id,
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_EXECUTION_TERMINAL.to_owned(),
            params: serde_json::to_value(notification).unwrap(),
        },
    );
    let mut responder = {
        let registry = restarted_registry.clone();
        let daemon_id = daemon_id.to_owned();
        let execution_id = execution.id.clone();
        tokio::spawn(async move {
            while let Some(api_types::DaemonFrame::Request { id, method, params }) =
                outbound.recv().await
            {
                let result = match method.as_str() {
                    api_types::METHOD_WORKSPACE_DESCRIBE => json!({
                        "workspace_handle": params["workspace_handle"], "generation": 1,
                        "exists": true, "head_sha": "owner-head", "dirty": false, "branch": "task/remote",
                        "locked": false, "active_execution_ids": [], "journaled_execution_ids": [execution_id],
                    }),
                    api_types::METHOD_WORKSPACE_READ => match params["operation"].as_str() {
                        Some("git") => {
                            json!({"kind": "git", "output": match params["query"]["kind"].as_str() {
                                Some("head" | "resolve_ref" | "merge_base" | "target_head") => "owner-head\n",
                                _ => "",
                            }})
                        }
                        Some("paths") => {
                            json!({"kind": "paths", "workspace_path": "/owner-only/workspace", "repo_path": "/owner-only/repo"})
                        }
                        Some("files") => json!({"kind": "files", "files": []}),
                        _ => json!({"path": params["path"], "bytes": [], "truncated": false}),
                    },
                    api_types::METHOD_JOURNAL_ACK => {
                        json!({"entry_id": params["entry_id"], "acknowledged": true})
                    }
                    _ => panic!("unexpected owner RPC {method}"),
                };
                registry.dispatch_incoming_for_connection(
                    &daemon_id,
                    connection_id,
                    api_types::DaemonFrame::Response { id, result },
                );
                if method == api_types::METHOD_JOURNAL_ACK {
                    return;
                }
            }
            panic!("journal ACK was not sent");
        })
    };
    let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
        .with_daemon_connections(restarted_registry.clone())
        .with_task_service(service);
    monitor.check_once().await.unwrap();
    let acknowledged = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    result = &mut responder => { result.unwrap(); break; },
                    _ = tokio::time::sleep(Duration::from_millis(25)) => { monitor.check_once().await.unwrap(); },
                }
            }
        }).await;
    match acknowledged {
        Ok(()) => {}
        Err(_) => panic!(
            "terminal ACK missing: placement={:?}, task={:?}, steps={:?}, retained={:?}",
            WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                .await
                .unwrap()
                .map(|p| p.state),
            TaskRepo::get_by_id(&*db, &execution.task_id, false)
                .await
                .unwrap()
                .map(|t| t.status),
            db::TaskStepRepo::task_steps(&*db, &execution.task_id)
                .await
                .unwrap()
                .iter()
                .map(|s| (
                    &s.kind,
                    &s.status,
                    &s.last_error,
                    serde_json::from_str::<Value>(&s.payload_json)
                        .ok()
                        .map(|p| p["operation"].clone())
                ))
                .collect::<Vec<_>>(),
            restarted_registry.retained_terminal_execution_ids()
        ),
    }
    monitor.finish_placement_workers().await;
    let receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM execution_terminal_receipt WHERE execution_id = ?",
    )
    .bind(&execution.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(receipts, 1);
    let comments: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM task_comment WHERE task_id = ? AND idempotency_key = ?",
    )
    .bind(&execution.task_id)
    .bind(format!("outbox:{}:worklog:1", execution.id))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(comments, 1);
}

#[tokio::test]
async fn daemon_owned_workspace_resume_requires_current_online_owner_capability() {
    let db = Arc::new(sqlite_db().await);
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE daemon SET detected_clis_json = ? WHERE id = ?")
        .bind(json!([{"kind":"shell","availability":"authenticated"}]).to_string())
        .bind(placement.daemon_id.as_deref().unwrap())
        .execute(db.pool())
        .await
        .unwrap();
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let bus = Arc::new(EventBus::new(16));
    fail_owned_execution(
        &db,
        &bus,
        &placement,
        &execution,
        PlacementFailureCause::OwnerLostExecution,
    )
    .await
    .unwrap();
    WorkspacePlacementRepo::update(
        &*db,
        placement_update(&placement, PlacementState::Ready, None),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE execution SET agent_session_id = 'owner-session' WHERE id = ?")
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    for task_id in sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    {
        crate::test_support::drain_task_steps(&db, &task_id).await;
    }
    let mut task = TaskRepo::get_by_id(&*db, &execution.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let mut annotation: serde_json::Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    annotation["recovery_actions"] = json!(["resume_session", "reexecute", "cancel_task"]);
    task = TaskRepo::update_status(
        &*db,
        UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: task.status.clone(),
            assignee_id: None,
            error_annotation: Some(Some(annotation.to_string())),
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let service = TaskService::new(db.clone(), bus).with_daemon_connections(registry.clone());
    assert!(!service
        .test_action_values(&task.id)
        .await
        .unwrap()
        .contains(&api_types::TaskAction::Retry {
            reason: None,
            fresh_session: Some(false),
            refresh_workspace: None,
            reset_budget: None,
            guidance: None
        }));
    let (_, _first) = owner_connection(&registry, placement.daemon_id.as_deref().unwrap(), false);
    assert!(!service
        .test_action_values(&task.id)
        .await
        .unwrap()
        .contains(&api_types::TaskAction::Retry {
            reason: None,
            fresh_session: Some(false),
            refresh_workspace: None,
            reset_budget: None,
            guidance: None
        }));
    let (_, _second) = owner_connection(&registry, placement.daemon_id.as_deref().unwrap(), true);
    assert!(service
        .test_action_values(&task.id)
        .await
        .unwrap()
        .contains(&api_types::TaskAction::Retry {
            reason: None,
            fresh_session: Some(false),
            refresh_workspace: None,
            reset_budget: None,
            guidance: None
        }));
    DaemonRepo::mark_offline(
        &*db,
        placement.daemon_id.as_deref().unwrap(),
        &now_rfc3339(),
    )
    .await
    .unwrap();
    assert!(!service
        .test_action_values(&task.id)
        .await
        .unwrap()
        .contains(&api_types::TaskAction::Retry {
            reason: None,
            fresh_session: Some(false),
            refresh_workspace: None,
            reset_budget: None,
            guidance: None
        }));
}

#[tokio::test]
async fn daemon_owned_workspace_active_execution_rebinds_without_redispatch() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    let grant = expired_owner_grant(&db, &task, &placement, &execution).await;
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
    // A server restart can reuse the old socket's numeric incarnation.
    // Its creation time still proves this is a new reconciliation path.
    let db::ExecutionLeaseMutation::Updated(execution) = ExecutionRepo::claim_lease(
        &*db,
        db::ClaimExecutionLease {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            owner: crate::daemon_transport::execution_lease_owner(&daemon_id, connection_id),
            lease_expires_at: execution.lease_expires_at.clone().unwrap(),
            hard_deadline_at: execution.hard_deadline_at.clone(),
            now: now_rfc3339(),
        },
    )
    .await
    .unwrap() else {
        panic!("reused owner fixture claims");
    };
    let execution_id = execution.id.clone();
    let responder = {
        let registry = registry.clone();
        let daemon_id = daemon_id.clone();
        tokio::spawn(async move {
            let api_types::DaemonFrame::Request { id, params, .. } = outbound.recv().await.unwrap()
            else {
                panic!("describe");
            };
            registry.dispatch_incoming_for_connection(&daemon_id, connection_id, api_types::DaemonFrame::Response {
                    id, result: json!({"workspace_handle": params["workspace_handle"], "generation": 1,
                        "exists": true, "head_sha": "base-head", "dirty": false, "branch": "task/remote", "locked": false,
                        "active_execution_ids": [execution_id], "journaled_execution_ids": []}),
                });
        })
    };
    let service =
        TaskService::new(db.clone(), Arc::new(EventBus::new(32))).with_daemon_connections(registry);
    service
        .test_apply_action(
            &task.id,
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
        .unwrap();
    responder.await.unwrap();
    let current = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, ExecutionStatus::Running);
    assert_eq!(
        current.lease_owner.as_deref(),
        Some(crate::daemon_transport::execution_lease_owner(&daemon_id, connection_id).as_str())
    );
    assert_eq!(current.hard_deadline_at, execution.hard_deadline_at);
    assert!(rfc3339_is_after(
        current.lease_expires_at.as_deref().unwrap(),
        &now_rfc3339()
    ));
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Ready
    );
    assert_eq!(
        ExecutionRepo::list_running_by_task(&*db, &execution.task_id)
            .await
            .unwrap()
            .len(),
        1
    );
    let resumed_grant = WorkspaceLeaseRepo::get_by_id(&*db, &grant.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed_grant.status, "active");
    assert!(rfc3339_is_after(&resumed_grant.expires_at, &now_rfc3339()));
    assert_eq!(resumed_grant.execution_id, execution.id);
}

#[tokio::test]
async fn daemon_owned_workspace_unknown_execution_fails_with_owner_lost_execution() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
    let responder = {
        let registry = registry.clone();
        tokio::spawn(async move {
            let api_types::DaemonFrame::Request { id, params, .. } = outbound.recv().await.unwrap()
            else {
                panic!("describe");
            };
            registry.dispatch_incoming_for_connection(&daemon_id, connection_id, api_types::DaemonFrame::Response {
                    id, result: json!({"workspace_handle": params["workspace_handle"], "generation": 1,
                        "exists": true, "head_sha": "base-head", "dirty": false, "branch": "task/remote", "locked": false,
                        "active_execution_ids": [], "journaled_execution_ids": []}),
                });
        })
    };
    let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
        .with_daemon_connections(registry);
    monitor.check_once().await.unwrap();
    responder.await.unwrap();
    monitor.finish_placement_workers().await;
    let failed = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, ExecutionStatus::Failed);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(failed.error.as_deref().unwrap()).unwrap()
            ["cause"],
        "owner_lost_execution"
    );
    let ready = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ready.state, PlacementState::Ready);
    assert_eq!(
        ready.failure_cause,
        Some(PlacementFailureCause::OwnerLostExecution)
    );
    for task_id in sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    {
        crate::test_support::drain_task_steps(&db, &task_id).await;
    }
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let annotation: serde_json::Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["blocking_reason"], "owner_lost_execution");
    assert_eq!(annotation["blocked_execution_id"], execution.id);
    assert!(task.failed_json.is_none());
    assert!(task
        .metadata_json
        .as_deref()
        .is_none_or(|raw| serde_json::from_str::<serde_json::Value>(raw)
            .unwrap()
            .get("execution_retry_count")
            .is_none()));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM execution WHERE task_id = ?")
        .bind(task.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn daemon_owned_workspace_sweep_repairs_terminal_before_recovery_annotation() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    WorkspacePlacementRepo::update(
        &*db,
        placement_update(
            &placement,
            PlacementState::Failed,
            Some(PlacementFailureCause::OwnerDisconnectedTimeout),
        ),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE execution SET status = 'failed', lease_owner = NULL, lease_expires_at = NULL,
            stop_reason = 'daemon_disconnected', error = '{\"cause\":\"owner_disconnected_timeout\"}' WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
    let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)));
    monitor.check_once().await.unwrap();
    for task_id in sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    {
        crate::test_support::drain_task_steps(&db, &task_id).await;
    }
    let recovered = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(recovered.error_annotation.as_deref().unwrap())
            .unwrap()["blocked_execution_id"],
        execution.id
    );
    // A user retry clearing the annotation must not be undone by the
    // next timeout sweep over this historical failed execution.
    let cleared = TaskRepo::update_status(
        &*db,
        UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: recovered.version,
            status: recovered.status,
            assignee_id: None,
            error_annotation: Some(None),
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    monitor.check_once().await.unwrap();
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(current.error_annotation.is_none());
    assert_eq!(current.version, cleared.version);
}

#[tokio::test]
async fn daemon_owned_workspace_stale_describe_generation_stays_suspended() {
    let db = Arc::new(sqlite_db().await);
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
    let execution_id = execution.id.clone();
    let responder = {
        let registry = registry.clone();
        tokio::spawn(async move {
            let api_types::DaemonFrame::Request { id, params, .. } = outbound.recv().await.unwrap()
            else {
                panic!("describe");
            };
            registry.dispatch_incoming_for_connection(&daemon_id, connection_id, api_types::DaemonFrame::Response {
                    id, result: json!({"workspace_handle": params["workspace_handle"], "generation": 2,
                        "exists": true, "head_sha": "base-head", "dirty": false, "branch": "task/remote", "locked": false,
                        "active_execution_ids": [execution_id], "journaled_execution_ids": []}),
                });
        })
    };
    HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
        .with_daemon_connections(registry)
        .check_once()
        .await
        .unwrap();
    responder.await.unwrap();
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap(),
        placement
    );
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap(),
        execution
    );
}
