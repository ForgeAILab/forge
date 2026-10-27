//! Tests for dispatch admission in the task dispatcher.

use super::*;
#[tokio::test]
async fn dispatcher_skips_task_when_agent_at_capacity() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let blocked = seed_task(&db, &project_id, "blocked", "in_progress", 0).await;
    assign_role(
        &db,
        &blocked.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_running_execution(
        &db,
        &blocked.id,
        &agent_id,
        crate::workflow::default_roles::CODER,
    )
    .await;
    crate::test_support::set_test_agent_capacity(&db, &agent_id, 1).await;
    let task = seed_task(&db, &project_id, "todo", "todo", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, "todo");
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_skips_task_when_agent_offline() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Offline, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "todo", "todo", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, "todo");
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_skips_paused_project() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "todo", "todo", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    sqlx::query("UPDATE project SET paused_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project paused");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, "todo");
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_respects_priority_ordering() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let low = seed_task(&db, &project_id, "low", "todo", 1).await;
    assign_role(
        &db,
        &low.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let high = seed_task(&db, &project_id, "high", "todo", "10".parse().unwrap()).await;
    assign_role(
        &db,
        &high.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    crate::test_support::force_task_version_conflict_after_transition(
        &db,
        &high.id,
        crate::workflow::default_states::PLANNING,
        &low.id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");
    assert_eq!(dispatched, 1);
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(ctx.task_id, high.id);

    let high_task = TaskRepo::get_by_id(&*db, &high.id, false)
        .await
        .expect("high task loads")
        .expect("high task exists");
    let low_task = TaskRepo::get_by_id(&*db, &low.id, false)
        .await
        .expect("low task loads")
        .expect("low task exists");
    assert_eq!(high_task.status, "in_progress");
    assert_eq!(low_task.status, "todo");
}

#[tokio::test]
async fn dispatcher_skips_active_task_with_blocking_annotation() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "blocked", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let blocked = serde_json::json!({
        "type": "manual_stop",
        "blocking_reason": "user_cancelled",
        "blocked_by": "user:test",
        "blocked_at": now_rfc3339(),
        "message": "blocked for review",
        "recovery_actions": ["resume_session", "reexecute", "reset_to_initial", "cancel_task"],
    })
    .to_string();
    TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(blocked)),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task update creates");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        0
    );
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_skips_todo_task_with_dispatch_failed_annotation() {
    // A task parked in todo after a deterministic dispatch failure (e.g. its
    // configured Agent source is disabled) must not be rescheduled until a
    // user restarts it — otherwise the dispatcher loops todo -> ... -> todo.
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "parked", "todo", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let annotation = serde_json::json!({
        "type": "dispatch_failed",
        "message": "repository Task cannot run while its configured Agent source is disabled",
        "state": "in_progress",
        "detected_at": now_rfc3339(),
        "recovery_actions": ["reexecute", "reset_to_initial", "cancel_task"],
    })
    .to_string();
    TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation)),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task annotation updates");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "todo", "parked task stays in todo");
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        0
    );
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn worker_robustness_dispatcher_isolates_projects_and_tasks() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (broken, _) = seed_project_repo(&db, repo.path()).await;
    sqlx::query("UPDATE project SET settings = 'invalid', created_at = ? WHERE id = ?")
        .bind("2000-01-01T00:00:00Z")
        .bind(&broken)
        .execute(db.pool())
        .await
        .unwrap();
    let healthy_repo = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, healthy_repo.path()).await;
    sqlx::query("UPDATE project SET created_at = ? WHERE id = ?")
        .bind("2001-01-01T00:00:00Z")
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let bad = seed_task(&db, &project_id, "bad metadata", "in_progress", 0).await;
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(r#"{"owner_wait":{"daemon_id":"owner","started_at":"2000-01-01T00:00:00Z"},"plan_publication_claim":42}"#)
        .bind(&bad.id).execute(db.pool()).await.unwrap();
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let healthy = seed_task(&db, &project_id, "healthy", "todo", 0).await;
    assign_role(&db, &healthy.id, "coder", &agent).await;
    let (dispatcher, mut rx) = build_dispatcher(db, workspace.path()).await;
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 1);
    assert_eq!(rx.recv().await.unwrap().task_id, healthy.id);

    let healthy_again = seed_task(&dispatcher.db, &project_id, "healthy again", "todo", 0).await;
    assign_role(&dispatcher.db, &healthy_again.id, "coder", &agent).await;
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 1);
    assert_eq!(rx.recv().await.unwrap().task_id, healthy_again.id);
}
