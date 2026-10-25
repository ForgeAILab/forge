//! Cleanup when a Task's repository is gone or temporarily missing.

use super::*;

#[tokio::test]
async fn terminal_cleanup_reclaims_the_task_root_when_its_repository_is_gone() {
    let db = sqlite_db().await;
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    std::fs::remove_dir_all(temp.path().join(".repos").join(&workspace.repo_id))
        .expect("source disappears");
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler.cleanup_now(workspace_id.clone()).await.unwrap();
    assert!(!worktree_path.parent().unwrap().exists());
    assert_eq!(
        WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        WorkspaceStatus::Cleaned
    );
}

#[tokio::test]
async fn sweep_cleans_historical_workspace_after_repo_row_is_deleted() {
    let db = sqlite_db().await;
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    RepoRepo::delete(&*db, &workspace.repo_id)
        .await
        .expect("repo row deletes");
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler.sweep().await.expect("sweep succeeds");
    assert!(!worktree_path.exists());
    assert_retained_logs(temp.path(), &task);
    assert!(git::branch_exists(
        &temp.path().join(".repos").join(&workspace.repo_id),
        &workspace.branch
    )
    .await
    .expect("branch kept"));
}
#[tokio::test]
async fn cleanup_terminal_sweep_removes_managed_home_after_owner_handle_retirement() {
    let db = sqlite_db().await;
    let (task, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    sqlx::query(
        "UPDATE task SET status = 'done', updated_at = '1970-01-01T00:00:00Z' WHERE id = ?",
    )
    .bind(&task.id)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE execution SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?").bind(&execution.id).execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE workspace SET status = 'cleaned' WHERE id = ?")
        .bind(&placement.workspace_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE workspace_placement SET state = 'cleaned' WHERE id = ?")
        .bind(&placement.id)
        .execute(db.pool())
        .await
        .unwrap();
    let root = TempDir::new().unwrap();
    let home = root
        .path()
        .join(".forge/logs")
        .join(&task.project_id)
        .join(&task.id)
        .join(".codex-managed-home");
    std::fs::create_dir_all(&home).unwrap();
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.unwrap();
    let (connection_id, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    let service = crate::TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().into());
    let router = Arc::new(
        (*service.workspace_backend_router())
            .clone()
            .with_daemon(Arc::new(
                crate::workspace_backend::DaemonWorkspaceBackend::new(db.clone(), registry.clone()),
            )),
    );
    let scheduler = WorkspaceCleanupScheduler::new(
        db.clone(),
        Arc::new(EventBus::default()),
        root.path().into(),
    );
    scheduler.set_workspace_backend_router(router);
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
                    code: "invalid_input".into(),
                    message: "unknown workspace_handle".into(),
                    details: None,
                },
            },
        );
    });
    scheduler.sweep().await.unwrap();
    responder.await.unwrap();
    assert!(!home.exists());
}

/// A user's repository that is away right now (an unmounted volume) is
/// not a repository that is gone: cleanup keeps the worktree, retries, and
/// removes the registration from the user's repository once it is back.
#[tokio::test]
async fn recorded_repository_missing_right_now_is_retried_not_treated_as_gone() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (workspace_id, worktree_path, user_repo) = user_repo_fixture(&db, temp.path()).await;
    assert!(worktree_registered(&user_repo, &worktree_path).await);
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );

    // The volume is unmounted: the repository's parent is gone with it.
    let volume = temp.path().join("vol");
    let away = temp.path().join("vol.unmounted");
    std::fs::rename(&volume, &away).unwrap();
    for attempt in 1..=CLEANUP_ATTENTION_ATTEMPTS {
        // An unmounted mount point may also be left behind as an empty
        // directory: that is still "away".
        if attempt == 2 {
            std::fs::create_dir(&volume).unwrap();
        }
        clean_due(&scheduler, &workspace_id).await;
        let row = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(row.status, WorkspaceStatus::Cleaned, "attempt {attempt}");
        assert_eq!(row.cleanup_attempts, attempt);
        assert!(row
            .last_cleanup_error
            .as_deref()
            .unwrap()
            .contains("is not reachable right now"));
        assert!(worktree_path.exists(), "the worktree is kept for the retry");
        assert_eq!(
            cleanup_attention(&db, &workspace_id).await.len(),
            usize::from(attempt == CLEANUP_ATTENTION_ATTEMPTS),
            "attention is raised at the threshold, attempt {attempt}"
        );
    }

    std::fs::remove_dir(&volume).unwrap();
    std::fs::rename(&away, &volume).unwrap();
    clean_due(&scheduler, &workspace_id).await;
    let row = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, WorkspaceStatus::Cleaned);
    assert!(!worktree_path.parent().unwrap().exists());
    assert!(
        !worktree_registered(&user_repo, &worktree_path).await,
        "the registration is removed from the user's repository"
    );
    assert_eq!(cleanup_attention(&db, &workspace_id).await[0].0, "resolved");
}

/// A checkout deleted for good, in a directory that is still there, took
/// its worktree registrations with it: cleanup removes the Task root at
/// once instead of retrying for a repository that will never come back.
#[tokio::test]
async fn recorded_repository_deleted_for_good_is_cleaned_without_retries() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (workspace_id, worktree_path, user_repo) = user_repo_fixture(&db, temp.path()).await;
    std::fs::write(temp.path().join("vol").join("other-project"), "still here").unwrap();
    std::fs::remove_dir_all(&user_repo).unwrap();
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );

    clean_due(&scheduler, &workspace_id).await;

    let row = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, WorkspaceStatus::Cleaned);
    assert_eq!(row.cleanup_attempts, 0);
    assert!(!worktree_path.parent().unwrap().exists());
    assert!(cleanup_attention(&db, &workspace_id).await.is_empty());
}

/// Waiting for a repository that is away is bounded: a set time after the
/// attention item was raised, the Task root is removed without it and the
/// item records why.
#[tokio::test]
async fn recorded_repository_that_stays_away_stops_being_waited_for() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (workspace_id, worktree_path, _user_repo) = user_repo_fixture(&db, temp.path()).await;
    std::fs::rename(temp.path().join("vol"), temp.path().join("vol.unmounted")).unwrap();
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    for _ in 0..CLEANUP_ATTENTION_ATTEMPTS + 2 {
        clean_due(&scheduler, &workspace_id).await;
    }
    assert!(worktree_path.exists(), "still waiting inside the bound");
    assert_eq!(cleanup_attention(&db, &workspace_id).await[0].0, "open");

    let raised = (chrono::Utc::now() - chrono::Duration::days(8)).to_rfc3339();
    sqlx::query("UPDATE attention_projection SET occurred_at = ? WHERE dedupe_key = ?")
        .bind(raised)
        .bind(cleanup_attention_key(&workspace_id))
        .execute(db.pool())
        .await
        .unwrap();
    clean_due(&scheduler, &workspace_id).await;

    let row = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, WorkspaceStatus::Cleaned);
    assert!(!worktree_path.parent().unwrap().exists());
    let (status, settled) = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT status, json_extract(details_json, '$.settled')
             FROM attention_projection WHERE dedupe_key = ?",
    )
    .bind(cleanup_attention_key(&workspace_id))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(status, "resolved");
    assert!(
        settled
            .as_deref()
            .is_some_and(|settled| settled.contains("stayed unreachable for 7 days")),
        "{settled:?}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn terminal_cleanup_reclaims_read_only_trees_and_broken_copies() {
    use std::os::unix::fs::PermissionsExt;

    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task_root = worktree_path.parent().unwrap().to_path_buf();
    let cache = worktree_path.join("pkg/mod");
    let broken = task_root.join("repo.broken-1700000000000");
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::create_dir_all(&broken).unwrap();
    std::fs::write(cache.join("module.go"), "package module\n").unwrap();
    std::fs::write(broken.join("leftover"), "leftover").unwrap();
    for (path, mode) in [
        (cache.join("module.go"), 0o400),
        (cache.clone(), 0o500),
        (worktree_path.join("pkg"), 0o500),
        (broken.join("leftover"), 0o400),
        (broken.clone(), 0o500),
    ] {
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
    }
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );

    clean_due(&scheduler, &workspace_id).await;

    assert!(!task_root.exists());
    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
    assert_eq!(workspace.cleanup_attempts, 0);
}
