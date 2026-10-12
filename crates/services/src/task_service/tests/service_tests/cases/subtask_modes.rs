use super::super::*;

#[tokio::test]
async fn batch_5_4_subtask_management_allows_manual_child_transition() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let child = seed_subtask_with_status(&db, &root, "child", "todo".to_owned(), 0).await;

    // The root is still in `todo`: starting its first subtask by hand
    // starts the root first, as the dispatcher's next pass would.
    let result = service
        .transition(
            child.id.clone(),
            "in_progress".to_owned(),
            TransitionOptions {
                bridge: Default::default(),
                version: child.version,
                reason: None,
                triggered_by: api_types::Actor::user(api_types::UserActionSource::Api),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await;

    let result = result.expect("user can manage child subtask status");

    assert_eq!(result.task.id, child.id);
    assert_eq!(
        result.task.parent_task_id.as_deref(),
        Some(root.id.as_str())
    );
    assert_eq!(
        result.task.status,
        crate::workflow::default_states::IN_PROGRESS
    );
}

#[tokio::test]
async fn batch_5_6_root_is_coordination_only() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_workspace_root(workspace_root.path().to_path_buf())
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let root = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let child = seed_subtask_with_status(&db, &root, "child", "todo".to_owned(), 0).await;

    let result = service
        .claim_task(root.id.clone(), Assignee::Agent(agent_id), None)
        .await;
    assert!(matches!(result, Err(ServiceError::InvalidOperation { .. })));

    let child_after = TaskRepo::get_by_id(&*db, &child.id, false)
        .await
        .expect("child loads")
        .expect("child exists");
    assert_eq!(child_after.status, "todo");
}

#[tokio::test]
async fn batch_5_7_reorder_subtasks_requires_all_ids() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let first = seed_subtask_with_status(&db, &root, "first", "todo".to_owned(), 0).await;
    let second = seed_subtask_with_status(&db, &root, "second", "todo".to_owned(), 1).await;
    let third = seed_subtask_with_status(&db, &root, "third", "todo".to_owned(), 2).await;

    service
        .reorder_subtasks(
            root.id.clone(),
            vec![first.id.clone(), third.id.clone(), second.id.clone()],
        )
        .await
        .expect("reorder with all ids succeeds");

    let reordered = TaskRepo::list_subtasks_ordered(&*db, &root.id)
        .await
        .expect("subtasks load");
    assert_eq!(reordered[0].id, first.id);
    assert_eq!(reordered[1].id, third.id);
    assert_eq!(reordered[2].id, second.id);
}

/// Starting a subtask by hand never runs it under a root that holds it. A
/// root with a lead is moved to its planning state (the dispatcher's own
/// next move), the claim is refused, and the subtask is left untouched. An
/// Agent that moves the first subtask of a root without a lead starts the
/// root, as a person does.
#[tokio::test]
async fn hand_starting_a_subtask_respects_the_root_that_holds_it() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_workspace_root(workspace_root.path().to_path_buf())
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let planner = crate::test_support::seed_native_agent(&db).await;

    // With a lead: the claim is refused and the root plans.
    let root = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let child = seed_subtask_with_status(&db, &root, "child", "todo".to_owned(), 0).await;
    service
        .reassign_role(
            role_assignment_input(&root.id, "planner", Some(planner.clone()), None),
            false,
            false,
        )
        .await
        .expect("a native Agent leads the root");
    service.drain(&root.id).await.expect("root steps settle");
    let result = service
        .claim_task(child.id.clone(), Assignee::Agent(agent_id.clone()), None)
        .await;
    assert!(
        matches!(result, Err(ServiceError::InvalidOperation { .. })),
        "{result:?}"
    );
    service.drain(&root.id).await.expect("root steps settle");
    let root_after = TaskRepo::get_by_id(&*db, &root.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root_after.status, "planning");
    let child_after = TaskRepo::get_by_id(&*db, &child.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child_after.status, "todo");
    let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id=?")
        .bind(&child.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(runs, 0);

    // An Agent moves the first subtask of a root without a lead.
    let root = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let child = seed_subtask_with_status(&db, &root, "child", "todo".to_owned(), 0).await;
    let moved = service
        .transition(
            child.id.clone(),
            "in_progress".to_owned(),
            TransitionOptions {
                bridge: Default::default(),
                version: child.version,
                reason: None,
                triggered_by: api_types::Actor::agent(agent_id.clone()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .expect("an Agent starts the first subtask of a root without a lead");
    assert_eq!(moved.task.status, "in_progress");
    let root_after = TaskRepo::get_by_id(&*db, &root.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root_after.status, "in_progress");
}
