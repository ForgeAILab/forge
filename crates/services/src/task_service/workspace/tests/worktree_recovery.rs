//! Worktree recovery cases.

use super::*;

#[tokio::test]
async fn missing_worktree_with_branch_auto_recovers() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let task = seed_task(&db, &project_id, None).await;

    let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("first workspace creates");
    assert_eq!(fresh.status, WorkspaceStatus::Ready);
    let branch = fresh.branch.clone();

    // Delete the worktree directory to simulate a stale workspace
    std::fs::remove_dir_all(fresh.embedded_worktree_path_for_backend())
        .expect("remove worktree dir");
    assert!(!std::path::Path::new(fresh.embedded_worktree_path_for_backend()).exists());

    // Branch still exists in repo — recovery should recreate the worktree
    let recovered = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("workspace auto-recovers");
    assert_eq!(recovered.id, fresh.id);
    assert_eq!(recovered.branch, branch);
    assert!(std::path::Path::new(recovered.embedded_worktree_path_for_backend()).exists());
}

#[tokio::test]
async fn directory_without_git_metadata_is_recreated() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let task = seed_task(&db, &project_id, None).await;

    let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("first workspace creates");
    let worktree_path = std::path::PathBuf::from(fresh.embedded_worktree_path_for_backend());
    std::fs::remove_file(worktree_path.join(".git")).expect("worktree metadata removes");
    assert!(worktree_path.exists());
    assert!(!worktree_path.join(".git").exists());

    let recovered = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("workspace with missing Git metadata recovers");

    assert_eq!(recovered.id, fresh.id);
    assert!(worktree_path.join(".git").exists());
    assert!(git::get_current_sha(&worktree_path).await.is_ok());
}

#[tokio::test]
async fn transient_worktree_probe_failure_preserves_workspace() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let task = seed_task(&db, &project_id, None).await;
    let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("first workspace creates");
    let worktree_path = std::path::PathBuf::from(fresh.embedded_worktree_path_for_backend());
    let sentinel = worktree_path.join("uncommitted.txt");
    std::fs::write(&sentinel, "keep me\n").expect("sentinel writes");
    inject_worktree_probe_failure(&worktree_path, InjectedWorktreeProbeFailure::Io);

    let result =
        prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None).await;

    assert!(matches!(
        result,
        Err(ServiceError::Git(git::GitError::Io(_)))
    ));
    assert_eq!(
        WorkspaceRepo::get_by_id(&db, &fresh.id)
            .await
            .expect("workspace reloads"),
        Some(fresh)
    );
    assert_eq!(
        std::fs::read_to_string(sentinel).expect("sentinel survives"),
        "keep me\n"
    );
}

#[tokio::test]
async fn worktree_reprobe_ready_preserves_existing_directory() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let task = seed_task(&db, &project_id, None).await;
    let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("first workspace creates");
    let worktree_path = std::path::PathBuf::from(fresh.embedded_worktree_path_for_backend());
    let sentinel = worktree_path.join("uncommitted.txt");
    std::fs::write(&sentinel, "keep me\n").expect("sentinel writes");
    inject_worktree_probe_failure(&worktree_path, InjectedWorktreeProbeFailure::NotRepository);

    let recovered = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("ready re-probe returns the existing workspace");

    assert_eq!(recovered.id, fresh.id);
    assert_eq!(
        std::fs::read_to_string(sentinel).expect("sentinel survives"),
        "keep me\n"
    );
    let parent = worktree_path.parent().expect("worktree has a parent");
    assert!(std::fs::read_dir(parent)
        .expect("worktree parent reads")
        .all(|entry| !entry
            .expect("directory entry reads")
            .file_name()
            .to_string_lossy()
            .contains(".broken-")));
}

#[tokio::test]
async fn branch_lookup_io_failure_preserves_workspace() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let task = seed_task(&db, &project_id, None).await;
    let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("first workspace creates");
    let worktree_path = std::path::PathBuf::from(fresh.embedded_worktree_path_for_backend());
    std::fs::remove_dir_all(&worktree_path).expect("worktree removes");
    let output = std::process::Command::new("git")
        .args(["worktree", "prune"])
        .current_dir(repo_dir.path())
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("git worktree prune runs");
    assert!(output.status.success());
    std::fs::create_dir_all(&worktree_path).expect("unusable directory creates");
    let sentinel = worktree_path.join("uncommitted.txt");
    std::fs::write(&sentinel, "keep me\n").expect("sentinel writes");
    inject_branch_lookup_failure(repo_dir.path());

    let result =
        prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None).await;

    assert!(matches!(
        result,
        Err(ServiceError::Git(git::GitError::Io(_)))
    ));
    assert_eq!(
        WorkspaceRepo::get_by_id(&db, &fresh.id)
            .await
            .expect("workspace reloads"),
        Some(fresh)
    );
    assert_eq!(
        std::fs::read_to_string(sentinel).expect("sentinel survives"),
        "keep me\n"
    );
}

#[tokio::test]
async fn cleaned_workspace_is_rebuilt_from_its_task_branch() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let task = seed_task(&db, &project_id, None).await;

    let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("first workspace creates");
    // What a reassignment reset does: the cleanup scheduler removes the
    // worktree and marks the row cleaned, but the task branch survives.
    ::workspace::WorkspaceManager::new(workspace_root.path().to_path_buf())
        .cleanup_worktree(
            &task.id,
            repo_dir.path(),
            Path::new(&fresh.embedded_worktree_path_for_backend()),
        )
        .await
        .expect("worktree cleans");
    WorkspaceRepo::mark_cleaned(&db, &fresh.id, &now_rfc3339())
        .await
        .expect("workspace marks cleaned");

    let rebuilt = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("cleaned workspace is rebuilt");
    assert_eq!(rebuilt.id, fresh.id);
    assert_eq!(rebuilt.branch, fresh.branch);
    assert_eq!(rebuilt.status, WorkspaceStatus::Ready);
    assert!(std::path::Path::new(rebuilt.embedded_worktree_path_for_backend()).exists());
}

#[tokio::test]
async fn cleaned_workspace_without_branch_starts_fresh() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let task = seed_task(&db, &project_id, None).await;

    let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("first workspace creates");
    ::workspace::WorkspaceManager::new(workspace_root.path().to_path_buf())
        .cleanup_worktree(
            &task.id,
            repo_dir.path(),
            Path::new(&fresh.embedded_worktree_path_for_backend()),
        )
        .await
        .expect("worktree cleans");
    WorkspaceRepo::mark_cleaned(&db, &fresh.id, &now_rfc3339())
        .await
        .expect("workspace marks cleaned");
    for args in [
        vec!["worktree", "prune"],
        vec!["branch", "-D", fresh.branch.as_str()],
    ] {
        std::process::Command::new("git")
            .args(&args)
            .current_dir(repo_dir.path())
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("git runs");
    }

    let rebuilt = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("cleaned workspace starts fresh");
    assert_ne!(rebuilt.id, fresh.id);
    assert_eq!(rebuilt.status, WorkspaceStatus::Ready);
    assert!(std::path::Path::new(rebuilt.embedded_worktree_path_for_backend()).exists());
}

#[tokio::test]
async fn unusable_existing_worktree_with_branch_auto_recovers() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let task = seed_task(&db, &project_id, None).await;

    let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("first workspace creates");
    std::fs::write(
        std::path::Path::new(fresh.embedded_worktree_path_for_backend()).join(".git"),
        "gitdir: /tmp/forge-missing-gitdir\n",
    )
    .expect("break gitdir reference");
    assert!(git::get_current_sha(std::path::Path::new(
        fresh.embedded_worktree_path_for_backend()
    ))
    .await
    .is_err());

    let recovered = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("workspace auto-recovers");

    assert_eq!(recovered.id, fresh.id);
    assert_eq!(recovered.branch, fresh.branch);
    assert!(git::get_current_sha(std::path::Path::new(
        recovered.embedded_worktree_path_for_backend()
    ))
    .await
    .is_ok());
}

#[tokio::test]
async fn existing_worktree_with_missing_repo_source_errors_before_reuse() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let task = seed_task(&db, &project_id, None).await;

    let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("first workspace creates");
    std::fs::remove_dir_all(repo_dir.path()).expect("remove repo dir");

    let result =
        prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None).await;
    assert!(
        matches!(&result, Err(ServiceError::InvalidOperation { message }) if message.contains("does not exist")),
        "expected InvalidOperation about missing repo, got: {result:?}"
    );
    assert!(
        std::path::Path::new(fresh.embedded_worktree_path_for_backend()).exists(),
        "unrecoverable worktree should be left in place"
    );
}

#[tokio::test]
async fn missing_worktree_and_branch_returns_reset_required() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let task = seed_task(&db, &project_id, None).await;

    let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("first workspace creates");
    let branch = fresh.branch.clone();

    // Delete worktree AND the branch
    std::fs::remove_dir_all(fresh.embedded_worktree_path_for_backend())
        .expect("remove worktree dir");
    std::process::Command::new("git")
        .args(["worktree", "prune"])
        .current_dir(repo_dir.path())
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("git worktree prune");
    std::process::Command::new("git")
        .args(["branch", "-D", &branch])
        .current_dir(repo_dir.path())
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("delete branch");

    let result =
        prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None).await;
    assert!(
        matches!(&result, Err(ServiceError::WorkspaceResetRequired { task_id, .. }) if *task_id == task.id),
        "expected WorkspaceResetRequired, got: {result:?}"
    );

    // Workspace record should have been deleted
    let ws = WorkspaceRepo::get_by_task_id(&db, &task.id)
        .await
        .expect("db query ok");
    assert!(ws.is_none(), "workspace record should be deleted");
}

#[tokio::test]
async fn missing_repo_source_returns_io_error() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let task = seed_task(&db, &project_id, None).await;

    let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("first workspace creates");

    // Delete worktree AND the entire repo directory
    std::fs::remove_dir_all(fresh.embedded_worktree_path_for_backend())
        .expect("remove worktree dir");
    std::fs::remove_dir_all(repo_dir.path()).expect("remove repo dir");

    let result =
        prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None).await;
    assert!(
        matches!(&result, Err(ServiceError::InvalidOperation { message }) if message.contains("does not exist")),
        "expected InvalidOperation about missing repo, got: {result:?}"
    );
}
#[tokio::test]
async fn missing_worktree_child_claim_missing_branch_preserves_parent_row() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
    let parent = seed_task(&db, &project_id, None).await;
    let child = seed_task(&db, &project_id, Some(parent.id.clone())).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().to_path_buf());
    let admission = service
        .reserve_claim_workspace(&parent, Some(&agent), "coder")
        .await
        .unwrap();
    let prepared = service.prepare_claim_workspace(admission).await.unwrap();
    std::fs::remove_dir_all(prepared.placement.workspace_handle.as_deref().unwrap()).unwrap();
    let output = Command::new("git")
        .args(["worktree", "prune"])
        .current_dir(repo.path())
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let output = Command::new("git")
        .args(["branch", "-D", &prepared.workspace.branch])
        .current_dir(repo.path())
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let admission = service
        .reserve_claim_workspace(&child, Some(&agent), "coder")
        .await
        .unwrap();
    assert!(service.prepare_claim_workspace(admission).await.is_err());
    assert_eq!(
        WorkspaceRepo::get_by_id(&*db, &prepared.workspace.id)
            .await
            .unwrap()
            .unwrap()
            .task_id,
        parent.id
    );
    assert!(
        WorkspacePlacementRepo::get_by_id(&*db, &prepared.placement.id)
            .await
            .unwrap()
            .is_some()
    );
}
