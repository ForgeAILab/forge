//! Offline-owner waits and daemon describe failures at claim.

use super::*;

#[tokio::test]
async fn placement_ready_embedded_claim_describes_without_source_lock_or_prepare() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let locks = Arc::new(RepoCacheLockManager::new());
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().to_path_buf())
        .with_repo_cache_locks(locks.clone());
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workspace
             SET cleanup_attempts = 3, last_cleanup_error = 'prior cleanup failure'
             WHERE id = ?",
    )
    .bind(&admission.workspace.id)
    .execute(db.pool())
    .await
    .unwrap();
    let prepared = service.prepare_claim_workspace(admission).await.unwrap();
    assert_eq!(prepared.workspace.cleanup_attempts, 0);
    assert!(prepared.workspace.last_cleanup_error.is_none());
    // Ready reclaims don't rerun history validation or clone/worktree creation.
    sqlx::query("UPDATE workspace SET before_sha = 'obsolete-base-object' WHERE id = ?")
        .bind(&prepared.workspace.id)
        .execute(db.pool())
        .await
        .unwrap();
    let _source_lock = locks.acquire(repo.path().to_str().unwrap()).await;
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let admitted_version = admission.placement.version;
    let reclaimed = service.prepare_claim_workspace(admission).await.unwrap();
    assert_eq!(
        reclaimed.placement.workspace_handle,
        prepared.placement.workspace_handle
    );
    assert_eq!(reclaimed.placement.version, admitted_version);
    assert_eq!(
        reclaimed.workspace.before_sha.as_deref(),
        Some("obsolete-base-object")
    );
}

#[tokio::test]
async fn placement_first_offline_owner_wait_creates_attention_and_expires_without_execution() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
    let mut task = seed_task(&db, &project_id, None).await;
    sqlx::query("UPDATE task SET status = 'in_progress' WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    task.status = "in_progress".into();
    sqlx::query("UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.concurrent_note', 'retained') WHERE id = ?")
            .bind(&task.id).execute(db.pool()).await.unwrap();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_max_disconnect(Duration::from_secs(1));
    let refusal = ServiceError::DaemonUnavailable {
        daemon_id: "offline-owner".into(),
    };
    assert!(service
        .defer_placement_refusal(&task, &refusal)
        .await
        .unwrap());
    let attention: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM attention_projection WHERE dedupe_key = ? AND status = 'open'",
    )
    .bind(format!("task-owner-wait:{}", task.id))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention, 1);
    service.drain(&task.id).await.unwrap();
    let waiting = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let attention_version: i64 =
        sqlx::query_scalar("SELECT version FROM attention_projection WHERE dedupe_key = ?")
            .bind(format!("task-owner-wait:{}", task.id))
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert!(service
        .defer_placement_refusal(&waiting, &refusal)
        .await
        .unwrap());
    service.drain(&task.id).await.unwrap();
    let again = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.version, waiting.version);
    let unchanged: i64 =
        sqlx::query_scalar("SELECT version FROM attention_projection WHERE dedupe_key = ?")
            .bind(format!("task-owner-wait:{}", task.id))
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(unchanged, attention_version);
    sqlx::query("UPDATE task SET metadata_json = json_set(metadata_json, '$.owner_wait.started_at', ?) WHERE id = ?")
            .bind((Utc::now() - chrono::Duration::seconds(2)).to_rfc3339()).bind(&task.id).execute(db.pool()).await.unwrap();
    let service = Arc::new(service);
    let dispatcher = crate::task_dispatcher::TaskDispatcher::new(
        db.clone(),
        Arc::new(EventBus::default()),
        service.clone(),
    );
    dispatcher.check_once().await.unwrap();
    service.drain(&task.id).await.unwrap();
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let blocked: api_types::InterruptionMetadata =
        serde_json::from_str(task.blocked_json.as_deref().unwrap()).unwrap();
    assert_eq!(blocked.kind, Some(api_types::FailureKind::RecoveryRequired));
    assert!(!blocked.reason.is_empty());
    assert!(!blocked.created_at.is_empty());
    let metadata: Value = serde_json::from_str(task.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(metadata["concurrent_note"], "retained");
    assert!(task
        .error_annotation
        .as_deref()
        .unwrap()
        .contains("owner_disconnected_timeout"));
    assert!(crate::deferred_dispatch::pending_until(&task).is_none());
    assert_eq!(execution_count(&db, &task.id).await, 0);
}
#[tokio::test]
async fn placement_expired_owner_wait_on_terminal_placement_falls_through() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_max_disconnect(Duration::ZERO);
    for state in [
        PlacementState::Failed,
        PlacementState::Cleaning,
        PlacementState::Cleaned,
    ] {
        sqlx::query("UPDATE workspace_placement SET state = ? WHERE id = ?")
            .bind(state.to_string())
            .bind(&placement.id)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
                .bind(json!({"owner_wait":{"daemon_id":placement.daemon_id,"started_at":"1970-01-01T00:00:00Z"}}).to_string())
                .bind(&task.id).execute(db.pool()).await.unwrap();
        let task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(!service.expire_owner_wait(&task).await.unwrap());
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let metadata: Value =
            serde_json::from_str(current.metadata_json.as_deref().unwrap()).unwrap();
        assert!(metadata.get("owner_wait").is_none());
        assert!(current.blocked_json.is_none());
    }
    let mut malformed = task;
    malformed.metadata_json = Some("{malformed".into());
    assert!(!service.expire_owner_wait(&malformed).await.unwrap());
}
#[tokio::test]
async fn placement_daemon_describe_failure_is_transient_without_prepare() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    sqlx::query(
        "UPDATE workspace_placement SET state = 'ready', disconnected_at = NULL WHERE id = ?",
    )
    .bind(&placement.id)
    .execute(db.pool())
    .await
    .unwrap();
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection_id, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_daemon_connections(registry.clone());
    let router = Arc::new(
        (*service.workspace_backend_router())
            .clone()
            .with_daemon(Arc::new(
                crate::workspace_backend::DaemonWorkspaceBackend::new(db.clone(), registry.clone()),
            )),
    );
    let responder = tokio::spawn(async move {
        let api_types::DaemonFrame::Request { id, method, .. } = outbound.recv().await.unwrap()
        else {
            panic!("describe request");
        };
        assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
        registry.dispatch_incoming_for_connection(
            &daemon_id,
            connection_id,
            api_types::DaemonFrame::Error {
                id: Some(id),
                error: api_types::DaemonErrorPayload {
                    code: "workspace_error".into(),
                    message: "temporary Git read failure".into(),
                    details: None,
                },
            },
        );
        outbound
    });
    let error = prepare_workspace(
        &db,
        std::path::Path::new("/owner-only"),
        &task,
        &task.id,
        None,
        &router,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServiceError::Git(_)), "{error:?}");
    assert!(
        responder.await.unwrap().try_recv().is_err(),
        "describe failure must not send prepare"
    );
}
