use super::*;
use db::ProjectCleanupOwner;

#[tokio::test]
async fn cleanup_rechecks_each_live_owner_in_the_callers_transaction() {
    let db = database().await;
    project(&db, "project", None).await;
    task(&db, "task", "project", "todo").await;
    let candidate = "/captured";
    sqlx::query("INSERT INTO repo (id, project_id, name, local_path, created_at, updated_at) VALUES ('repo', 'project', 'repo', ?, ?, ?)")
        .bind(candidate).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace (id, task_id, repo_id, worktree_path, branch, status, created_at, updated_at) VALUES ('workspace', 'task', 'repo', ?, 'main', 'ready', ?, ?)")
        .bind(candidate).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    let mut transaction = db::begin_immediate(db.pool()).await.unwrap();
    for (owner, value) in [
        (ProjectCleanupOwner::TaskId, "task"),
        (ProjectCleanupOwner::ProjectId, "project"),
        (ProjectCleanupOwner::RepoId, "repo"),
        (ProjectCleanupOwner::RepoLocalPath, candidate),
        (ProjectCleanupOwner::WorkspacePath, candidate),
    ] {
        assert!(db
            .project_cleanup_owner_is_live_in_tx(&mut transaction, owner, value)
            .await
            .unwrap());
        assert!(!db
            .project_cleanup_owner_is_live_in_tx(&mut transaction, owner, "missing")
            .await
            .unwrap());
    }
    transaction.commit().await.unwrap();
}
