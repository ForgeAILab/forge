//! Terminal Task log retention.

use super::*;

#[tokio::test]
async fn terminal_codex_home_is_removed_when_repository_cleanup_fails() {
    let db = sqlite_db().await;
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    // A vanished repository no longer fails cleanup (the Task root is
    // simply removed). A checkout of some other repository at the
    // recorded path still does: it is not Forge's to delete.
    let source = temp.path().join(".repos").join(&workspace.repo_id);
    fixture_git(
        &source,
        &[
            "worktree",
            "remove",
            "--force",
            worktree_path.to_str().unwrap(),
        ],
    )
    .await;
    let other = temp.path().join("other-repository");
    std::fs::create_dir_all(&other).unwrap();
    git::init(&other).await.unwrap();
    git::commit_all(&other, "initial commit").await.unwrap();
    fixture_git(
        &other,
        &[
            "worktree",
            "add",
            "--detach",
            worktree_path.to_str().unwrap(),
            "HEAD",
        ],
    )
    .await;
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    assert!(scheduler.cleanup_now(workspace_id.clone()).await.is_err());
    assert!(worktree_path.exists());
    assert_retained_logs(temp.path(), &task);
    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(workspace.status, WorkspaceStatus::Cleaning);
    assert!(
        chrono::DateTime::parse_from_rfc3339(workspace.cleanup_after.as_deref().unwrap()).unwrap()
            > chrono::Utc::now()
    );
}

#[tokio::test]
async fn sweep_deletes_logs_of_terminal_tasks_after_the_retention() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let (workspace_id, _) = seed_workspace(&db, &root, WorkspaceStatus::Cleaned).await;
    let base = fixture_task(&db, &workspace_id).await;
    let mut tasks = Vec::new();
    for (status, days) in [("done", 31), ("done", 29), ("in_progress", 400)] {
        let task = seed_log_only_task(&db, &root, &base, new_uuid_v4(), None, status).await;
        sqlx::query("UPDATE task SET updated_at = ? WHERE id = ?")
            .bind((chrono::Utc::now() - chrono::Duration::days(days)).to_rfc3339())
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        backdate(&logs_dir(&root, &task), 60);
        tasks.push(task);
    }
    // Terminal for 31 days by its record, but its logs were written
    // inside the retention (a Task imported or restored): kept.
    let recent = seed_log_only_task(&db, &root, &base, new_uuid_v4(), None, "done").await;
    sqlx::query("UPDATE task SET updated_at = ? WHERE id = ?")
        .bind((chrono::Utc::now() - chrono::Duration::days(31)).to_rfc3339())
        .bind(&recent.id)
        .execute(db.pool())
        .await
        .unwrap();
    // The record says the future (a skewed clock): kept.
    let skewed = seed_log_only_task(&db, &root, &base, new_uuid_v4(), None, "done").await;
    sqlx::query("UPDATE task SET updated_at = ? WHERE id = ?")
        .bind((chrono::Utc::now() + chrono::Duration::days(400)).to_rfc3339())
        .bind(&skewed.id)
        .execute(db.pool())
        .await
        .unwrap();
    backdate(&logs_dir(&root, &skewed), 60);
    let scheduler = gc_scheduler(&db, &root).await;

    // `0` keeps every log.
    scheduler.set_gc_limits(0, executors::gc::FreeFloor::default());
    scheduler.sweep().await.unwrap();
    assert!(logs_dir(&root, &tasks[0]).join("execution.jsonl").exists());

    // That sweep removed the legacy agent home from each log directory,
    // which is a write to it: age them again.
    for task in tasks.iter().chain([&skewed]) {
        backdate(&logs_dir(&root, task), 60);
    }
    scheduler.set_gc_limits(30, executors::gc::FreeFloor::default());
    scheduler.sweep().await.unwrap();
    assert!(!logs_dir(&root, &tasks[0]).exists());
    assert!(logs_dir(&root, &tasks[1]).join("execution.jsonl").exists());
    assert!(logs_dir(&root, &tasks[2]).join("execution.jsonl").exists());
    assert!(logs_dir(&root, &recent).join("execution.jsonl").exists());
    assert!(logs_dir(&root, &skewed).join("execution.jsonl").exists());
}

#[tokio::test]
async fn log_retention_skips_a_task_with_an_open_review_or_attention_item() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let (workspace_id, _) = seed_workspace(&db, &root, WorkspaceStatus::Cleaned).await;
    let base = fixture_task(&db, &workspace_id).await;
    let mut tasks = Vec::new();
    for _ in 0..3 {
        let task = seed_log_only_task(&db, &root, &base, new_uuid_v4(), None, "done").await;
        sqlx::query("UPDATE task SET updated_at = ? WHERE id = ?")
            .bind((chrono::Utc::now() - chrono::Duration::days(31)).to_rfc3339())
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        tasks.push(task);
    }
    let (reviewed, flagged, plain) = (&tasks[0], &tasks[1], &tasks[2]);
    // A review a person has not decided yet.
    let execution =
        fixture_execution(&db, reviewed, &workspace_id, db::ExecutionStatus::Completed).await;
    let now = now_rfc3339();
    sqlx::query("INSERT INTO review (id, task_id, execution_id, attempt_number, status, step_results_json, started_at, created_at, updated_at) VALUES (?, ?, ?, 1, 'awaiting_human', '[]', ?, ?, ?)")
            .bind(new_uuid_v4())
            .bind(&reviewed.id)
            .bind(&execution.id)
            .bind(&now)
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
    // An attention item that points a person at the Task.
    let mut transaction = db::begin_immediate(db.pool()).await.unwrap();
    let event = db::DomainEventRepo::append_event_in_tx(
        &*db,
        &mut transaction,
        &db::CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "task.needs_attention".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: flagged.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: flagged.project_id.clone(),
            correlation_id: flagged.id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: "{}".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db.insert_attention_in_tx(
        &mut transaction,
        db::CreateAttentionProjection {
            id: new_uuid_v4(),
            attention_type: "progress_warning".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: flagged.project_id.clone(),
            identity_id: None,
            source_event_id: event.id,
            priority: 60,
            status: "open".to_owned(),
            summary: "look at this Task".to_owned(),
            details_json: serde_json::json!({"task": {"id": flagged.id}}).to_string(),
            dedupe_key: format!("test:{}", flagged.id),
            occurred_at: now.clone(),
            updated_at: now.clone(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect".to_owned(),
            source_sequence: Some(event.sequence),
        },
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let scheduler = gc_scheduler(&db, &root).await;
    scheduler.set_gc_limits(30, executors::gc::FreeFloor::default());
    let age = |tasks: &[db::Task]| {
        for task in tasks {
            if logs_dir(&root, task).exists() {
                backdate(&logs_dir(&root, task), 60);
            }
        }
    };
    // The first sweep removes the legacy agent home from each log
    // directory, which is a write to it: age them, then sweep again.
    scheduler.sweep().await.unwrap();
    age(&tasks);
    scheduler.sweep().await.unwrap();
    assert!(!logs_dir(&root, plain).exists());
    assert!(logs_dir(&root, reviewed).join("execution.jsonl").exists());
    assert!(logs_dir(&root, flagged).join("execution.jsonl").exists());

    // Decided and resolved: both are ordinary old logs now.
    sqlx::query("UPDATE review SET status = 'passed' WHERE task_id = ?")
        .bind(&reviewed.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE attention_projection SET status = 'resolved' WHERE dedupe_key = ?")
        .bind(format!("test:{}", flagged.id))
        .execute(db.pool())
        .await
        .unwrap();
    age(&tasks);
    scheduler.sweep().await.unwrap();
    assert!(!logs_dir(&root, reviewed).exists());
    assert!(!logs_dir(&root, flagged).exists());
}
