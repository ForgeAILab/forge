//! Workspace reset recovery cases.

use super::*;

#[tokio::test]
async fn placement_launch_recreates_deleted_daemon_worktree() {
    assert_daemon_launch_recreates_worktree(false).await;
}

#[tokio::test]
async fn placement_launch_leaves_damaged_daemon_worktree_for_reset() {
    assert_daemon_launch_recreates_worktree(true).await;
}

#[tokio::test]
async fn daemon_owned_workspace_missing_worktree_recovers_same_owner_branch() {
    let db = Arc::new(sqlite_db().await);
    let (_, placement, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET status = 'completed', after_sha = 'owner-head', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
    let responder = {
        let registry = registry.clone();
        tokio::spawn(async move {
            for step in 0..4 {
                let api_types::DaemonFrame::Request { id, method, params } =
                    outbound.recv().await.unwrap()
                else {
                    panic!("same-owner recovery request");
                };
                let result = match step {
                    0 | 3 => {
                        assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                        json!({"workspace_handle": params["workspace_handle"], "generation": 1, "exists": step == 3,
                                "head_sha": if step == 3 { Some("owner-head") } else { None }, "dirty": false,
                                "branch": "task/remote", "locked": false, "active_execution_ids": [], "journaled_execution_ids": []})
                    }
                    1 => {
                        assert_eq!(method, api_types::METHOD_WORKSPACE_PREPARE);
                        assert_eq!(params["generation"], 1);
                        assert_eq!(params["branch"], "task/remote");
                        json!({"entry_id": "recovered-entry", "operation_id": params["operation_id"],
                                "workspace_handle": "opaque-owner-handle", "workspace_path": "/owner-only/rebuilt", "generation": 1,
                                "base_sha": "base-head", "branch": "task/remote"})
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
    let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
        .with_daemon_connections(registry);
    monitor.check_once().await.unwrap();
    responder.await.unwrap();
    monitor.finish_placement_workers().await;
    let ready = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ready.state, PlacementState::Ready);
    assert_eq!(ready.daemon_id, placement.daemon_id);
    assert_eq!(ready.generation, 1);
    assert_eq!(ExecutionRepo::list_running(&*db).await.unwrap().len(), 0);
}

#[tokio::test]
async fn daemon_owned_ready_workspace_reset_persists_owner_handle_and_next_generation() {
    assert_ready_owner_reset(false).await;
}

#[tokio::test]
async fn daemon_owned_ready_task_reset_to_initial_recreates_on_same_owner() {
    assert_ready_owner_reset(true).await;
}

#[tokio::test]
async fn daemon_owned_workspace_reset_receipt_repairs_generation_cas_conflict() {
    use crate::workspace_backend::{
        DaemonWorkspaceBackend, EmbeddedWorkspaceBackend, WorkspaceBackendRouter,
    };
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
    annotate_owner_workspace_reset(&db, &placement, "owner HEAD differs")
        .await
        .unwrap();
    let bus = Arc::new(EventBus::new(32));
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
    let root = tempfile::tempdir().unwrap();
    let merge = Arc::new(crate::MergeService::new(
        db.clone(),
        root.path().to_path_buf(),
    ));
    let router = Arc::new(
        WorkspaceBackendRouter::new(Arc::new(EmbeddedWorkspaceBackend::new(
            db.clone(),
            merge,
            root.path().to_path_buf(),
        )))
        .with_daemon(Arc::new(DaemonWorkspaceBackend::new(
            db.clone(),
            registry.clone(),
        ))),
    );
    let service = TaskService::new(db.clone(), bus)
        .with_daemon_connections(registry.clone())
        .with_workspace_backend_router(router);
    let responder = {
        let registry = registry.clone();
        let db = db.clone();
        let placement = placement.clone();
        tokio::spawn(async move {
            for step in 0..3 {
                let api_types::DaemonFrame::Request { id, method, params } =
                    outbound.recv().await.unwrap()
                else {
                    panic!("reset recovery request");
                };
                let result = match step {
                    0 => {
                        assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                        json!({"workspace_handle": params["workspace_handle"], "generation": 1, "exists": true, "head_sha": "unrecorded-head",
                                "dirty": false, "branch": "task/remote", "locked": false, "active_execution_ids": [], "journaled_execution_ids": []})
                    }
                    1 => {
                        assert_eq!(method, api_types::METHOD_WORKSPACE_RESET);
                        assert_eq!(params["generation"], 2);
                        assert_eq!(params["expected"]["sha"], "unrecorded-head");
                        assert_eq!(params["base_ref"], "base-head");
                        WorkspacePlacementRepo::update(
                            &*db,
                            placement_update(
                                &placement,
                                PlacementState::Disconnected,
                                placement.failure_cause.clone(),
                            ),
                        )
                        .await
                        .unwrap();
                        json!({"entry_id": "reset-entry", "operation_id": params["operation_id"], "workspace_handle": "opaque-owner-handle",
                                "workspace_path": "/owner-only/rebuilt", "generation": 2, "base_sha": "base-head", "branch": "task/remote"})
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
    assert!(matches!(
        service
            .test_apply_action(
                &task.id,
                api_types::TaskAction::Restart { reason: None },
                None,
                None
            )
            .await,
        Err(ServiceError::Db(db::DbError::VersionConflict))
    ));
    responder.await.unwrap();
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .generation,
        1
    );
    let recovered = service
        .test_apply_action(
            &task.id,
            api_types::TaskAction::Restart { reason: None },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(recovered.error_annotation.is_none());
    let ready = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ready.generation, 2);
    assert_eq!(
        ready.workspace_handle.as_deref(),
        Some("opaque-owner-handle")
    );
    assert_eq!(ready.daemon_id, placement.daemon_id);
    assert_eq!(ready.state, PlacementState::Ready);
}

#[tokio::test]
async fn placement_recovery_refuses_offline_agent_and_preserves_blocker() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET status = 'failed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
    let ready = WorkspacePlacementRepo::update(
        &*db,
        placement_update(&placement, PlacementState::Ready, None),
    )
    .await
    .unwrap();
    db::TaskRoleAssignmentRepo::assign(
        &*db,
        db::CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            role_name: "coder".into(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: placement.agent_id.clone(),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(json!({"type":"manual_stop", "blocking_reason":"manual_stop", "blocked_by":"user",
                "blocked_at":now_rfc3339(), "blocked_execution_id":execution.id, "message":"retry on same owner",
}).to_string())
            .bind(&task.id).execute(db.pool()).await.unwrap();
    DaemonRepo::mark_offline(&*db, ready.daemon_id.as_deref().unwrap(), &now_rfc3339())
        .await
        .unwrap();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(32)));
    let before = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let error = service
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
        .expect_err("explicit recovery preserves the base offline-agent refusal");
    assert!(
        matches!(error, ServiceError::TaskActionUnavailable { ref available_actions, .. } if available_actions.iter().all(|offer| offer.action.verb() != "retry")),
        "{error:?}"
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.error_annotation, before.error_annotation);
    assert!(crate::deferred_dispatch::queued_recovery(&current).is_none());
    assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn daemon_owned_workspace_offline_retry_waits_on_same_owner_and_can_cancel() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = daemon_owned_fixture(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(32)));
    let actions = service.test_action_values(&task.id).await.unwrap();
    assert_eq!(
        actions,
        vec![
            api_types::TaskAction::Cancel { reason: None },
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None
            },
        ]
    );
    let retried = service
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
    assert_eq!(retried.status, task.status);
    let unchanged = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.daemon_id, placement.daemon_id);
    assert_eq!(unchanged.workspace_handle, placement.workspace_handle);
    assert_eq!(
        ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap()
            .len(),
        1
    );
    let cancelled = service
        .test_apply_action(
            &task.id,
            api_types::TaskAction::Cancel { reason: None },
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(cancelled.status, "cancelled");
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Cancelled
    );
}
