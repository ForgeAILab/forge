//! Repository clone cases.

use super::*;

#[tokio::test]
async fn placement_migrated_remote_only_repo_first_claim_verifies_clone() {
    assert_migrated_clone_claim(false).await;
}

#[tokio::test]
async fn placement_migrated_worktree_location_replaces_invalid_path_on_claim() {
    assert_migrated_clone_claim(true).await;
}

#[tokio::test]
async fn placement_remote_clone_failure_is_recorded_and_backed_off() {
    let db = Arc::new(sqlite_db().await);
    let remote = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project_id, repo_id) = seed_project_with_real_repo(&db, remote.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let missing = root.path().join("missing-remote");
    sqlx::query("UPDATE repo SET local_path = NULL, remote_url = ? WHERE id = ?")
        .bind(missing.to_str().unwrap())
        .bind(&repo_id)
        .execute(db.pool())
        .await
        .unwrap();
    let location = db::RepoLocationRepo::create(
        &*db,
        db::CreateRepoLocation {
            id: new_uuid_v4(),
            repo_id: repo_id.clone(),
            owner_kind: db::RepoLocationOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            path: root
                .path()
                .join(".repos")
                .join(&repo_id)
                .to_string_lossy()
                .into_owned(),
            kind: RepoLocationKind::ManagedClone,
            is_default: true,
            status: db::RepoLocationStatus::Unverified,
            last_verified_at: None,
            last_error: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let locks = Arc::new(RepoCacheLockManager::new());
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().to_path_buf())
        .with_repo_cache_locks(locks.clone());
    assert!(service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .is_err());
    let failed = db::RepoLocationRepo::get_by_id(&*db, &location.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, db::RepoLocationStatus::Unavailable);
    let error: Value = serde_json::from_str(failed.last_error.as_deref().unwrap()).unwrap();
    assert_eq!(error["cause"], "clone_failed");
    assert_eq!(error["attempts"], 1);
    assert!(
        DateTime::parse_from_rfc3339(error["retry_at"].as_str().unwrap()).unwrap() > Utc::now()
    );
    let _source_lock = locks.acquire(&location.path).await;
    assert!(service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .is_err());
    let deferred = db::RepoLocationRepo::get_by_id(&*db, &location.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        deferred.version, failed.version,
        "backoff must not retry the clone on every scan"
    );
    assert_eq!(deferred.last_error, failed.last_error);
}
