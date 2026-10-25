//! Terminal cleanup, failure persistence and backfill sweeps.

use super::*;

#[test]
fn cleanup_backoff_starts_at_one_minute_and_caps_at_one_hour() {
    assert_eq!(cleanup_backoff(0), Duration::from_secs(60));
    assert_eq!(cleanup_backoff(1), Duration::from_secs(120));
    assert_eq!(cleanup_backoff(6), Duration::from_secs(3600));
    assert_eq!(cleanup_backoff(7), Duration::from_secs(3600));
}

#[tokio::test]
async fn cleanup_accepts_owner_notification_winning_the_cleaned_state_cas() {
    let db = sqlite_db().await;
    let (_, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE execution SET status = 'cancelled' WHERE id = ?")
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    let cleaning = WorkspacePlacementRepo::update(
        &*db,
        cleanup_state_update(&placement, PlacementState::Cleaning, &now_rfc3339()),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE workspace SET status = 'cleaning' WHERE id = ?")
        .bind(&cleaning.workspace_id)
        .execute(db.pool())
        .await
        .unwrap();
    let notification: api_types::WorkspaceCleanupResult =
        serde_json::from_value(serde_json::json!({
            "entry_id": "cleanup-entry",
            "operation_id": "cleanup-operation",
            "workspace_handle": cleaning.workspace_handle,
            "generation": cleaning.generation,
            "cleaned": true,
        }))
        .unwrap();
    assert_eq!(
        crate::recovery::apply_owner_cleanup(
            &db,
            cleaning.daemon_id.as_deref().unwrap(),
            &notification,
        )
        .await
        .unwrap(),
        crate::daemon_transport::DaemonTerminalDisposition::Acknowledge
    );

    let temp = TempDir::new().unwrap();
    let scheduler = WorkspaceCleanupScheduler::new(
        db.clone(),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    let workspace = scheduler
        .acknowledge_cleanup(
            &cleaning,
            &crate::workspace_backend::CleanupAck { removed: true },
        )
        .await
        .expect("the already committed owner acknowledgement is idempotent");
    assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &cleaning.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementState::Cleaned
    );
}

#[tokio::test]
async fn cleanup_now_removes_worktree_and_marks_cleaned() {
    let db = sqlite_db().await;
    let event_bus = Arc::new(EventBus::new(16));
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let scheduler =
        WorkspaceCleanupScheduler::new(Arc::clone(&db), event_bus, temp.path().to_path_buf());

    scheduler
        .cleanup_now(workspace_id.clone())
        .await
        .expect("cleanup succeeds");

    assert!(!worktree_path.exists());
    let task = fixture_task(&db, &workspace_id).await;
    assert_retained_logs(temp.path(), &task);
    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
}

#[tokio::test]
async fn cleanup_marks_cleaned_when_worktree_is_missing() {
    let db = sqlite_db().await;
    let event_bus = Arc::new(EventBus::new(16));
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    std::fs::remove_dir_all(worktree_path.parent().expect("task root exists"))
        .expect("worktree removes");
    let scheduler =
        WorkspaceCleanupScheduler::new(Arc::clone(&db), event_bus, temp.path().to_path_buf());

    scheduler
        .cleanup_now(workspace_id.clone())
        .await
        .expect("cleanup succeeds");

    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
    assert!(workspace.cleanup_after.is_none());
}

#[tokio::test]
async fn cleanup_failure_persistence_bounds_errors_and_keeps_offline_retry_fixed() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (offline_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let offline_task_id = WorkspaceRepo::get_by_id(&*db, &offline_id)
        .await
        .unwrap()
        .unwrap()
        .task_id;
    sqlx::query(
        "UPDATE workspace
             SET cleanup_attempts = 6, last_cleanup_error = 'prior failure'
             WHERE id = ?",
    )
    .bind(&offline_id)
    .execute(db.pool())
    .await
    .unwrap();
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    let before = chrono::Utc::now();
    scheduler
        .record_cleanup_failure(
            &offline_task_id,
            &ServiceError::DaemonUnavailable {
                daemon_id: "offline-owner".to_owned(),
            },
        )
        .await
        .unwrap();
    let after = chrono::Utc::now();
    let offline = WorkspaceRepo::get_by_id(&*db, &offline_id)
        .await
        .unwrap()
        .unwrap();
    let retry_at = chrono::DateTime::parse_from_rfc3339(
        offline
            .cleanup_after
            .as_deref()
            .expect("retry is scheduled"),
    )
    .unwrap()
    .with_timezone(&chrono::Utc);
    assert_eq!(offline.cleanup_attempts, 6);
    assert!(retry_at >= before + chrono::Duration::seconds(60));
    assert!(retry_at <= after + chrono::Duration::seconds(60));

    let (bounded_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let bounded_task_id = WorkspaceRepo::get_by_id(&*db, &bounded_id)
        .await
        .unwrap()
        .unwrap()
        .task_id;
    let long_error = format!("{}diagnostic-tail", "界".repeat(2_000));
    scheduler
        .record_cleanup_failure(
            &bounded_task_id,
            &ServiceError::invalid_operation(long_error),
        )
        .await
        .unwrap();
    let bounded = WorkspaceRepo::get_by_id(&*db, &bounded_id)
        .await
        .unwrap()
        .unwrap();
    let error = bounded
        .last_cleanup_error
        .as_deref()
        .expect("last cleanup error is stored");
    assert_eq!(
        error.chars().count(),
        crate::project_environment::BLOCK_MESSAGE_OUTPUT_CHARS
    );
    assert!(error.ends_with("diagnostic-tail"));
}

#[tokio::test]
async fn cleanup_failure_does_not_emit_transition_effect_failed() {
    let db = sqlite_db().await;
    let bus = Arc::new(EventBus::new(256));
    let mut events = bus.subscribe();
    let temp = TempDir::new().unwrap();
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    let task = set_fixture_status(&db, &task, "todo").await;
    let scheduler = Arc::new(WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&bus),
        temp.path().to_path_buf(),
    ));
    scheduler.set_terminal_cleanup_handler(Arc::new(FailingTerminalCleanup));
    let service = crate::TaskService::new(Arc::clone(&db), bus)
        .with_cleanup_scheduler(Arc::clone(&scheduler))
        .with_workspace_root(temp.path().to_path_buf());

    // Cleanup holds this same lock during physical deletion. The transition
    // must finish without waiting for the worker to acquire it.
    let guard = scheduler.lock_task(&task).await;
    let outcome = timeout(
        Duration::from_secs(5),
        service.transition(task.id.clone(), "done".to_owned(), (task.version, None)),
    )
    .await
    .expect("transition does not wait for cleanup")
    .expect("transition succeeds");
    drop(guard);
    assert_eq!(outcome.task.status, "done");
    service.drain(&task.id).await.unwrap();
    assert!(worktree_path.exists());
    assert!(logs_dir(temp.path(), &task)
        .join(".codex-managed-home")
        .exists());
    scheduler
        .tick()
        .await
        .expect("worker isolates cleanup failure");
    assert!(worktree_path.exists());
    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        chrono::DateTime::parse_from_rfc3339(workspace.cleanup_after.as_deref().unwrap()).unwrap()
            > chrono::Utc::now()
    );
    while let Ok(event) = events.try_recv() {
        assert_ne!(event.event_type, "transition.effect_failed");
    }
}

#[tokio::test]
async fn sweep_preserves_cancelled_managed_home_without_workspace_until_grace_expires() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (workspace_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let root = fixture_task(&db, &workspace_id).await;
    let task = seed_log_only_task(&db, temp.path(), &root, new_uuid_v4(), None, "cancelled").await;
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler.sweep().await.unwrap();
    assert!(logs_dir(temp.path(), &task)
        .join(".codex-managed-home")
        .exists());
    sqlx::query("UPDATE task SET updated_at = ?, version = version + 1 WHERE id = ?")
        .bind((chrono::Utc::now() - chrono::Duration::hours(25)).to_rfc3339())
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    scheduler.sweep().await.unwrap();
    assert_retained_logs(temp.path(), &task);
}

#[tokio::test]
async fn sweep_never_prunes_user_owned_local_repository() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (workspace_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    set_fixture_status(&db, &task, "review").await;
    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    let user_repo = temp.path().join("user-repo");
    std::fs::create_dir_all(&user_repo).unwrap();
    git::init(&user_repo).await.unwrap();
    git::commit_all(&user_repo, "initial commit").await.unwrap();
    let user_worktree = temp.path().join("user-worktree");
    let output = tokio::process::Command::new("git")
        .args(["worktree", "add", "--detach"])
        .arg(&user_worktree)
        .arg("HEAD")
        .current_dir(&user_repo)
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    std::fs::remove_dir_all(&user_worktree).unwrap();
    sqlx::query("UPDATE repo SET local_path = ? WHERE id = ?")
        .bind(user_repo.to_string_lossy().as_ref())
        .bind(&workspace.repo_id)
        .execute(db.pool())
        .await
        .unwrap();
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler.sweep().await.unwrap();
    let output = tokio::process::Command::new("git")
        .args(["worktree", "list", "--porcelain"])
        .current_dir(&user_repo)
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains(user_worktree.to_str().unwrap()));
}

#[tokio::test]
async fn sweep_backfills_terminal_tasks_and_prunes_missing_registrations() {
    let db = sqlite_db().await;
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    // An earlier cleaner left a registration and only an outbox directory.
    std::fs::remove_dir_all(&worktree_path).expect("worktree removes");
    std::fs::create_dir_all(worktree_path.join(".forge-outbox")).expect("outbox remains");
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    assert!(WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap()
        .cleanup_after
        .is_none());
    scheduler.sweep().await.expect("sweep succeeds");
    assert!(!worktree_path.exists());
    assert_retained_logs(temp.path(), &task);
    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    let source = temp.path().join(".repos").join(&workspace.repo_id);
    let output = tokio::process::Command::new("git")
        .args(["worktree", "list", "--porcelain"])
        .current_dir(&source)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await
        .expect("worktree list");
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(worktree_path.to_str().unwrap()));
    assert!(git::branch_exists(&source, &workspace.branch)
        .await
        .expect("branch kept"));
}

#[tokio::test]
async fn terminal_subtask_keeps_shared_root_worktree() {
    let db = sqlite_db().await;
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let root = fixture_task(&db, &workspace_id).await;
    let root = set_fixture_status(&db, &root, "review").await;
    let child = seed_log_only_task(
        &db,
        temp.path(),
        &root,
        new_uuid_v4(),
        Some(root.id.clone()),
        "done",
    )
    .await;
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler.sweep().await.expect("sweep succeeds");
    assert!(worktree_path.join("node_modules/build-output").exists());
    assert!(logs_dir(temp.path(), &root)
        .join(".codex-managed-home")
        .exists());
    assert_retained_logs(temp.path(), &child);
}

#[tokio::test]
async fn sweep_is_bounded_and_cleans_homes_without_workspaces() {
    let db = sqlite_db().await;
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Cleaned).await;
    let root = fixture_task(&db, &workspace_id).await;
    let mut tasks = Vec::new();
    for index in 0..SWEEP_LIMIT + 1 {
        tasks.push(
            seed_log_only_task(
                &db,
                temp.path(),
                &root,
                format!("{}-backfill-{index:03}", root.id),
                None,
                "done",
            )
            .await,
        );
    }
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler.sweep().await.expect("first sweep succeeds");
    assert_eq!(
        tasks
            .iter()
            .filter(|task| logs_dir(temp.path(), task)
                .join(".codex-managed-home")
                .exists())
            .count(),
        2
    );
    scheduler.sweep().await.expect("next page succeeds");
    for task in &tasks {
        assert_retained_logs(temp.path(), task);
    }
    assert_retained_logs(temp.path(), &root);
}

#[tokio::test]
async fn sweep_prunes_missing_registration_without_cleaning_live_task() {
    let db = sqlite_db().await;
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    let task = set_fixture_status(&db, &task, "review").await;
    std::fs::remove_dir_all(&worktree_path).expect("external cleaner removes worktree");
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler.sweep().await.expect("sweep succeeds");
    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(workspace.status, WorkspaceStatus::Ready);
    assert!(logs_dir(temp.path(), &task)
        .join(".codex-managed-home")
        .exists());
    let source = temp.path().join(".repos").join(&workspace.repo_id);
    let output = tokio::process::Command::new("git")
        .args(["worktree", "list", "--porcelain"])
        .current_dir(&source)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await
        .expect("worktree list");
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(worktree_path.to_str().unwrap()));
}

#[tokio::test]
async fn five_failed_cleanups_raise_one_attention_item_that_success_clears() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler.set_terminal_cleanup_handler(Arc::new(FailingWorkspaceCleanup {
        workspace_id: workspace_id.clone(),
    }));

    for attempt in 1..=4 {
        clean_due(&scheduler, &workspace_id).await;
        assert!(
            cleanup_attention(&db, &workspace_id).await.is_empty(),
            "attempt {attempt} is still an ordinary retry"
        );
    }
    clean_due(&scheduler, &workspace_id).await;
    let raised = cleanup_attention(&db, &workspace_id).await;
    assert_eq!(raised.len(), 1);
    assert_eq!(raised[0].0, "open");
    let details = sqlx::query_scalar::<_, String>(
        "SELECT details_json FROM attention_projection WHERE dedupe_key = ?",
    )
    .bind(cleanup_attention_key(&workspace_id))
    .fetch_one(db.pool())
    .await
    .unwrap();
    let details: serde_json::Value = serde_json::from_str(&details).unwrap();
    assert_eq!(details["attempts"], 5);
    assert_eq!(details["path"], worktree_path.to_string_lossy().as_ref());
    assert!(details["last_cleanup_error"]
        .as_str()
        .unwrap()
        .contains("cleanup observer failed"));

    // Later retries keep failing and keep the one item untouched.
    for _ in 0..3 {
        clean_due(&scheduler, &workspace_id).await;
    }
    assert_eq!(cleanup_attention(&db, &workspace_id).await, raised);
    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(workspace.cleanup_attempts, 8);
    assert!(worktree_path.exists());
    let events = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'workspace.cleanup_failed'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(events, 1);

    // Success clears it.
    scheduler.set_terminal_cleanup_handler(Arc::new(FailingWorkspaceCleanup {
        workspace_id: "another-workspace".to_owned(),
    }));
    clean_due(&scheduler, &workspace_id).await;
    assert!(!worktree_path.exists());
    let cleared = cleanup_attention(&db, &workspace_id).await;
    assert_eq!(cleared.len(), 1);
    assert_eq!(cleared[0].0, "resolved");
}
