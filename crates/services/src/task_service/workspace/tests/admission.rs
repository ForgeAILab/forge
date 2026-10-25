//! Admission cases.

use super::*;

#[tokio::test]
async fn placement_admission_reservation_is_durable_before_prepare() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let stored = WorkspacePlacementRepo::get_by_id(&*db, &admission.placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, PlacementState::Reserved);
    assert!(stored.workspace_handle.is_none());
    assert!(!workspace_root.path().join(&task.id).join("repo").exists());
    assert!(execution_count(&db, &task.id).await == 0);
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap(),
        task
    );
    let prepared = service.prepare_claim_workspace(admission).await.unwrap();
    assert_eq!(prepared.placement.state, PlacementState::Ready);
    assert!(std::path::Path::new(prepared.placement.workspace_handle.as_deref().unwrap()).exists());
    assert!(execution_count(&db, &task.id).await == 0);
}

#[tokio::test]
async fn placement_admission_embedded_pin_binds_an_existing_server_workspace() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let prepared = service.prepare_claim_workspace(admission).await.unwrap();
    assert!(prepared.placement.execution_daemon_id.is_none());

    let now = now_rfc3339();
    let daemon = db::DaemonRepo::upsert_by_machine_id(
        &*db,
        db::UpsertDaemon {
            max_concurrent_runs: None,
            id: new_uuid_v4(),
            machine_id: crate::embedded_daemon::embedded_machine_id(),
            hostname: "server".into(),
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            agent_version: None,
            labels_json: "{}".into(),
            status: db::DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".into(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let pinned = AgentRepo::update(
        &*db,
        db::UpdateAgent {
            id: agent.id.clone(),
            expected_version: agent.version,
            name: None,
            description: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: None,
            config_json: None,
            daemon_id: Some(Some(daemon.id.clone())),
            max_concurrent_tasks: None,
            heartbeat_interval_seconds: None,
            max_missed_heartbeats: None,
            status: None,
            last_heartbeat_at: None,
            is_default: None,
            paused: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let admission = service
        .reserve_claim_workspace(&task, Some(&pinned), "coder")
        .await
        .unwrap();
    assert_eq!(admission.placement.id, prepared.placement.id);
    assert_eq!(admission.placement.owner_kind, PlacementOwnerKind::Server);
    assert_eq!(
        admission.placement.workspace_handle,
        prepared.placement.workspace_handle
    );
    assert_eq!(
        admission.placement.execution_daemon_id.as_deref(),
        Some(daemon.id.as_str())
    );
    let admission = service.prepare_claim_workspace(admission).await.unwrap();
    // Claim checks placement inside the Task's claim step.
    let lease = crate::test_support::TestTaskLease::claim(&db, &task.id).await;
    let mut transaction = db::begin_immediate(db.pool()).await.unwrap();
    lease
        .run(service.check_claim_placement_in_tx(&mut transaction, &task, &admission))
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    lease.release().await;
    assert_eq!(execution_count(&db, &task.id).await, 0);
}

#[tokio::test]
async fn placement_admission_prepare_failure_keeps_retry_budget() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,'execution','fixture',2) ON CONFLICT(task_id,kind) DO UPDATE SET spent=2")
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let placement_id = admission.placement.id.clone();
    std::fs::remove_dir_all(repo_dir.path()).unwrap();
    assert!(matches!(
        service.prepare_claim_workspace(admission).await,
        Err(ServiceError::PrepareFailed { .. })
    ));
    let stored = WorkspacePlacementRepo::get_by_id(&*db, &placement_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, PlacementState::Failed);
    assert_eq!(
        stored.failure_cause,
        Some(db::PlacementFailureCause::PrepareFailed)
    );
    assert!(execution_count(&db, &task.id).await == 0);
    assert!(WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap(),
        task
    );
    assert_eq!(
        crate::agent_capacity::count_occupied_agent_slots(&db, &agent.id)
            .await
            .unwrap(),
        0
    );
    let retry = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    assert_eq!(retry.placement.id, stored.id);
    assert_eq!(retry.placement.generation, stored.generation + 1);
    assert_eq!(retry.placement.state, PlacementState::Reserved);
    assert!(retry.placement.failure_cause.is_none());
    assert!(matches!(
        service.prepare_claim_workspace(retry).await,
        Err(ServiceError::PrepareFailed { .. })
    ));
    assert_eq!(execution_count(&db, &task.id).await, 0);
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap(),
        task
    );
}

#[tokio::test]
async fn placement_admission_concurrent_claims_cannot_take_preparing_slot() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let first = seed_task(&db, &project_id, None).await;
    let second = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (first_result, second_result) = tokio::join!(
        service.reserve_claim_workspace(&first, Some(&agent), "coder"),
        service.reserve_claim_workspace(&second, Some(&agent), "coder"),
    );
    let (mut admission, refused, error) = match (first_result, second_result) {
        (Ok(admission), Err(ServiceError::PlacementUnavailable(error))) => {
            (admission, &second, error)
        }
        (Err(ServiceError::PlacementUnavailable(error)), Ok(admission)) => {
            (admission, &first, error)
        }
        _ => panic!("only one concurrent reservation may take the last Agent slot"),
    };
    assert!(error.rejected_candidates.iter().all(|candidate| candidate
        .filter_codes
        .contains(&crate::placement::PlacementFilterCode::AgentCapacity)));
    let mut update = crate::placement::admission::placement_update(&admission.placement);
    update.state = Some(PlacementState::Preparing);
    admission.placement = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
    let error = match service
        .reserve_claim_workspace(refused, Some(&agent), "coder")
        .await
    {
        Err(ServiceError::PlacementUnavailable(error)) => error,
        _ => panic!("a preparing reservation must hold the last Agent slot"),
    };
    assert!(error.rejected_candidates.iter().all(|candidate| candidate
        .filter_codes
        .contains(&crate::placement::PlacementFilterCode::AgentCapacity)));
    assert!(WorkspaceRepo::get_by_task_id(&*db, &refused.id)
        .await
        .unwrap()
        .is_none());
    assert!(execution_count(&db, &refused.id).await == 0);
}

#[tokio::test]
async fn placement_admission_fence_refusal_creates_attention_without_an_execution() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let cause = db::PlacementFailureCause::WrongOwner;
    crate::placement::admission::fail_preparation(&db, &admission.placement, cause.clone())
        .await
        .unwrap();
    crate::placement::admission::record_fence_rejection(
        &db,
        &admission.placement,
        &cause,
        "wrong_owner",
    )
    .await
    .unwrap();
    let details: String = sqlx::query_scalar(
            "SELECT details_json FROM attention_projection WHERE attention_type = 'execution_failed' AND dedupe_key LIKE 'workspace-fence:%'",
        ).fetch_one(db.pool()).await.unwrap();
    let details: Value = serde_json::from_str(&details).unwrap();
    assert_eq!(details["failure_cause"], "wrong_owner");
    assert_eq!(details["placement_id"], admission.placement.id);
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap(),
        task
    );
    assert_eq!(execution_count(&db, &task.id).await, 0);
}

#[tokio::test]
async fn placement_admission_reclaim_reuses_and_fences_stale_start() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let first = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let first = service.prepare_claim_workspace(first).await.unwrap();
    let second = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let second = service.prepare_claim_workspace(second).await.unwrap();
    assert_eq!(first.placement.id, second.placement.id);
    assert_eq!(
        first.placement.workspace_handle,
        second.placement.workspace_handle
    );
    assert_eq!(first.placement.generation, second.placement.generation);
    // Claim checks placement inside the Task's claim step.
    let lease = crate::test_support::TestTaskLease::claim(&db, &task.id).await;
    let mut transaction = db::begin_immediate(db.pool()).await.unwrap();
    assert!(matches!(
        lease
            .run(service.check_claim_placement_in_tx(&mut transaction, &task, &first))
            .await,
        Err(ServiceError::Db(DbError::VersionConflict))
    ));
    assert!(lease
        .run(service.check_claim_placement_in_tx(&mut transaction, &task, &second))
        .await
        .is_ok());
    transaction.rollback().await.unwrap();
    lease.release().await;
}

#[tokio::test]
async fn placement_admission_expiry_releases_capacity() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let mut update = crate::placement::admission::placement_update(&admission.placement);
    update.state = Some(PlacementState::Preparing);
    update.reserved_until = Some(Some("2020-01-01T00:00:00Z".to_owned()));
    WorkspacePlacementRepo::update(&*db, update).await.unwrap();
    assert_eq!(
        crate::placement::admission::sweep_expired_reservations(&db, &now_rfc3339())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        crate::agent_capacity::count_occupied_agent_slots(&db, &agent.id)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap(),
        task
    );
    assert!(execution_count(&db, &task.id).await == 0);
}
