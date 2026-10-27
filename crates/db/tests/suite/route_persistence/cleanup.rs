use super::*;
use db::ProjectCleanupOwner;
use std::time::Duration;

#[tokio::test]
async fn cleanup_rechecks_each_live_owner_and_removes_an_unowned_directory() {
    let db = database().await;
    let root = tempfile::tempdir().unwrap();
    let candidate = root.path().join("captured");
    tokio::fs::create_dir(&candidate).await.unwrap();
    tokio::fs::write(candidate.join("keep"), b"owned")
        .await
        .unwrap();
    project(&db, "project", None).await;
    task(&db, "task", "project", "todo").await;
    sqlx::query("INSERT INTO repo (id, project_id, name, local_path, created_at, updated_at) VALUES ('repo', 'project', 'repo', ?, ?, ?)")
        .bind(candidate.to_str().unwrap()).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace (id, task_id, repo_id, worktree_path, branch, status, created_at, updated_at) VALUES ('workspace', 'task', 'repo', ?, 'main', 'ready', ?, ?)")
        .bind(candidate.to_str().unwrap()).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    for (owner, value) in [
        (ProjectCleanupOwner::TaskId, "task"),
        (ProjectCleanupOwner::ProjectId, "project"),
        (ProjectCleanupOwner::RepoId, "repo"),
        (
            ProjectCleanupOwner::RepoLocalPath,
            candidate.to_str().unwrap(),
        ),
        (
            ProjectCleanupOwner::WorkspacePath,
            candidate.to_str().unwrap(),
        ),
    ] {
        assert!(db
            .remove_unowned_project_cleanup_path(
                owner,
                value,
                root.path(),
                &candidate,
                "Task workspace"
            )
            .await
            .unwrap());
        assert_eq!(
            tokio::fs::read(candidate.join("keep")).await.unwrap(),
            b"owned"
        );
    }
    assert!(!db
        .remove_unowned_project_cleanup_path(
            ProjectCleanupOwner::TaskId,
            "missing",
            root.path(),
            &candidate,
            "Task workspace"
        )
        .await
        .unwrap());
    assert!(!candidate.exists());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn cleanup_rename_failure_rolls_back_the_write_lock_and_preserves_bytes() {
    let db = database().await;
    let root = tempfile::tempdir().unwrap();
    let candidate = root.path().join("captured");
    tokio::fs::create_dir(&candidate).await.unwrap();
    tokio::fs::write(candidate.join("keep"), b"preserved")
        .await
        .unwrap();
    // A trailing `.` passes direct-child canonicalization but cannot be renamed.
    let error = db
        .remove_unowned_project_cleanup_path(
            ProjectCleanupOwner::TaskId,
            "missing",
            root.path(),
            &candidate.join("."),
            "Task workspace",
        )
        .await
        .unwrap_err();
    assert!(matches!(error, db::DbError::ProjectCleanup(_)));
    assert!(error.to_string().starts_with("quarantine Task workspace: "));
    assert_eq!(
        tokio::fs::read(candidate.join("keep")).await.unwrap(),
        b"preserved"
    );
    let transaction = tokio::time::timeout(Duration::from_secs(2), db::begin_immediate(db.pool()))
        .await
        .unwrap()
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(!db
        .remove_unowned_project_cleanup_path(
            ProjectCleanupOwner::TaskId,
            "missing",
            root.path(),
            &candidate,
            "Task workspace"
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn cleanup_ignores_missing_paths_and_nested_or_external_directories() {
    let db = database().await;
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let nested = root.path().join("child/nested");
    tokio::fs::create_dir_all(&nested).await.unwrap();
    for candidate in [
        root.path().join("missing"),
        nested.clone(),
        outside.path().to_owned(),
    ] {
        assert!(!db
            .remove_unowned_project_cleanup_path(
                ProjectCleanupOwner::TaskId,
                "missing",
                root.path(),
                &candidate,
                "Task workspace"
            )
            .await
            .unwrap());
    }
    assert!(nested.is_dir());
    assert!(outside.path().is_dir());
    let missing_root = root.path().join("missing-root");
    assert!(!db
        .remove_unowned_project_cleanup_path(
            ProjectCleanupOwner::TaskId,
            "missing",
            &missing_root,
            &missing_root.join("missing"),
            "Task workspace"
        )
        .await
        .unwrap());
}

#[cfg(unix)]
#[tokio::test]
async fn cleanup_never_quarantines_a_symlink() {
    let db = database().await;
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    tokio::fs::write(outside.path().join("keep"), b"preserved")
        .await
        .unwrap();
    let candidate = root.path().join("symlink");
    std::os::unix::fs::symlink(outside.path(), &candidate).unwrap();
    assert!(!db
        .remove_unowned_project_cleanup_path(
            ProjectCleanupOwner::TaskId,
            "missing",
            root.path(),
            &candidate,
            "Task workspace"
        )
        .await
        .unwrap());
    assert!(candidate
        .symlink_metadata()
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(
        tokio::fs::read(outside.path().join("keep")).await.unwrap(),
        b"preserved"
    );
}
