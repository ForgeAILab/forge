//! Delivery branches cases.

use super::*;

#[tokio::test]
async fn terminal_cleanup_deletes_the_branch_only_when_git_proves_delivery() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );

    // Delivered: the tip is contained in the target branch.
    let (delivered_id, delivered_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let (source, branch) = fixture_source(&db, temp.path(), &delivered_id).await;
    fixture_git(&source, &["merge", "--no-ff", "-m", "deliver", &branch]).await;
    clean_due(&scheduler, &delivered_id).await;
    assert!(!delivered_path.exists());
    assert!(!git::branch_exists(&source, &branch).await.unwrap());
    assert!(git::branch_exists(&source, "main").await.unwrap());
    // Idempotent re-run over an already reclaimed workspace.
    scheduler.cleanup_now(delivered_id.clone()).await.unwrap();
    scheduler.sweep().await.unwrap();
    assert_eq!(
        WorkspaceRepo::get_by_id(&*db, &delivered_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        WorkspaceStatus::Cleaned
    );

    // Delivered once, but the target moved and no longer contains the tip.
    let (moved_id, moved_path) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let (source, branch) = fixture_source(&db, temp.path(), &moved_id).await;
    let before = fixture_git(&source, &["rev-parse", "HEAD"]).await;
    fixture_git(&source, &["merge", "--no-ff", "-m", "deliver", &branch]).await;
    fixture_git(&source, &["reset", "--hard", &before]).await;
    clean_due(&scheduler, &moved_id).await;
    assert!(!moved_path.exists());
    assert!(git::branch_exists(&source, &branch).await.unwrap());
}

#[tokio::test]
async fn reset_and_shared_branch_names_keep_a_delivered_branch() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );

    // A reset of a live Task rebuilds from the branch, so it stays.
    let (reset_id, reset_path) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &reset_id).await;
    set_fixture_status(&db, &task, "todo").await;
    let (source, branch) = fixture_source(&db, temp.path(), &reset_id).await;
    fixture_git(&source, &["merge", "--no-ff", "-m", "deliver", &branch]).await;
    scheduler
        .reset_workspace_now(reset_id.clone())
        .await
        .unwrap();
    assert!(!reset_path.exists());
    assert!(git::branch_exists(&source, &branch).await.unwrap());

    // Two Task ids can share the eight-character branch name. While the
    // other workspace is live, the name is not this Task's to delete.
    let (shared_id, shared_path) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &shared_id).await;
    let (source, branch) = fixture_source(&db, temp.path(), &shared_id).await;
    fixture_git(&source, &["merge", "--no-ff", "-m", "deliver", &branch]).await;
    let other =
        seed_log_only_task(&db, temp.path(), &task, new_uuid_v4(), None, "in_progress").await;
    let shared = WorkspaceRepo::get_by_id(&*db, &shared_id)
        .await
        .unwrap()
        .unwrap();
    let now = now_rfc3339();
    WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: other.id.clone(),
            repo_id: shared.repo_id.clone(),
            worktree_path: temp
                .path()
                .join(&other.id)
                .join("repo")
                .to_string_lossy()
                .into_owned(),
            branch: branch.clone(),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    clean_due(&scheduler, &shared_id).await;
    assert!(!shared_path.exists());
    assert!(git::branch_exists(&source, &branch).await.unwrap());
}

#[tokio::test]
async fn terminal_cleanup_never_deletes_a_branch_forge_did_not_name_for_the_task() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );

    // Merged branches of the user's, one of them under `task/`, and the
    // target itself: a row that names one of them deletes nothing.
    for recorded in ["task/users-own", "feature/mine", "main"] {
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let (source, task_branch) = fixture_source(&db, temp.path(), &workspace_id).await;
        fixture_git(
            &source,
            &["merge", "--no-ff", "-m", "deliver", &task_branch],
        )
        .await;
        if recorded != "main" {
            fixture_git(&source, &["branch", recorded, "main"]).await;
        }
        sqlx::query("UPDATE workspace SET branch = ? WHERE id = ?")
            .bind(recorded)
            .bind(&workspace_id)
            .execute(db.pool())
            .await
            .unwrap();

        clean_due(&scheduler, &workspace_id).await;

        assert!(!worktree_path.exists());
        assert_eq!(
            WorkspaceRepo::get_by_id(&*db, &workspace_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkspaceStatus::Cleaned
        );
        assert!(git::branch_exists(&source, recorded).await.unwrap());
        // The Task's own branch is not what the row names, so it stays too.
        assert!(git::branch_exists(&source, &task_branch).await.unwrap());
    }
}

#[tokio::test]
async fn leftovers_of_an_already_cleaned_workspace_keep_its_branch() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Cleaned).await;
    let (source, branch) = fixture_source(&db, temp.path(), &workspace_id).await;
    fixture_git(&source, &["merge", "--no-ff", "-m", "deliver", &branch]).await;
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );

    scheduler.sweep().await.unwrap();

    assert!(!worktree_path.exists());
    assert!(git::branch_exists(&source, &branch).await.unwrap());
}
