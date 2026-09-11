use super::super::*;

#[tokio::test]
async fn subtask_helpers_resolve_root_and_subtask() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    let subtask = seed_subtask_with_status(&db, &root, "child", "todo".to_owned(), 0).await;
    let root_id = root.id.clone();

    assert!(is_root_task(&db, &root.id).await.expect("root checks"));
    assert!(!is_subtask(&db, &root.id).await.expect("subtask checks"));
    assert!(!is_root_task(&db, &subtask.id).await.expect("root checks"));
    assert!(is_subtask(&db, &subtask.id).await.expect("subtask checks"));
    let resolved_root = root_for(&db, &subtask.id).await.expect("root resolves");
    assert_eq!(resolved_root.id.as_str(), root_id.as_str());
    let resolved_self = root_for(&db, &root.id).await.expect("root resolves");
    assert_eq!(resolved_self.id.as_str(), root_id.as_str());
}

#[tokio::test]
async fn create_task_assigns_subtask_order_and_rejects_nested_subtask() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = service
        .create_task(
            project_id.clone(),
            "Root",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("root task creates");

    let subtask = service
        .create_task(
            project_id.clone(),
            "Child",
            None,
            Some(root.id.clone()),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("subtask creates");

    assert_eq!(subtask.parent_task_id.as_deref(), Some(root.id.as_str()));
    assert_eq!(subtask.subtask_order, Some(0));

    let result = service
        .create_task(
            project_id,
            "Grandchild",
            None,
            Some(subtask.id),
            None,
            None,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(
        result,
        Err(ServiceError::NestedSubtaskUnsupported)
    ));
}

#[tokio::test]
async fn create_subtasks_preserves_input_order_and_rejects_different_assignee_atomically() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let _agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let root = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&root.id, "coder", Some(agent_a.clone()), None),
            false,
            false,
        )
        .await
        .expect("coder assignment succeeds");

    let subtasks = service
        .create_subtasks(
            root.id.clone(),
            vec![
                NewSubtaskInput {
                    title: "First".to_owned(),
                    description: None,
                    assignee_id: None,
                },
                NewSubtaskInput {
                    title: "Second".to_owned(),
                    description: Some("details".to_owned()),
                    assignee_id: None,
                },
            ],
        )
        .await
        .expect("subtasks create");

    assert_eq!(subtasks.len(), 2);
    assert_eq!(subtasks[0].subtask_order, Some(0));
    assert_eq!(subtasks[1].subtask_order, Some(1));
    assert_eq!(subtasks[0].project_id.as_str(), root.project_id.as_str());
    assert_eq!(
        subtasks[0].repo_id.as_deref().unwrap(),
        root.repo_id.as_deref().unwrap()
    );
    assert_eq!(
        subtasks[0].parent_task_id.as_deref(),
        Some(root.id.as_str())
    );
    let subtask_roles = TaskRoleAssignmentRepo::list_by_task(&*db, &subtasks[0].id)
        .await
        .expect("subtask roles load");
    assert!(subtask_roles.is_empty());

    let after = TaskRepo::list_subtasks_ordered(&*db, &root.id)
        .await
        .expect("subtasks load");
    assert_eq!(after.len(), 2);
}

#[tokio::test]
async fn reorder_subtasks_updates_order() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    let subtasks = service
        .create_subtasks(
            root.id.clone(),
            vec![
                NewSubtaskInput {
                    title: "A".to_owned(),
                    description: None,
                    assignee_id: None,
                },
                NewSubtaskInput {
                    title: "B".to_owned(),
                    description: None,
                    assignee_id: None,
                },
                NewSubtaskInput {
                    title: "C".to_owned(),
                    description: None,
                    assignee_id: None,
                },
            ],
        )
        .await
        .expect("subtasks create");
    let reordered_ids = vec![
        subtasks[0].id.clone(),
        subtasks[2].id.clone(),
        subtasks[1].id.clone(),
    ];

    service
        .reorder_subtasks(root.id.clone(), reordered_ids)
        .await
        .expect("reorder succeeds");

    let reordered = TaskRepo::list_subtasks_ordered(&*db, &root.id)
        .await
        .expect("subtasks load");
    assert_eq!(reordered[0].title, "A");
    assert_eq!(reordered[1].title, "C");
    assert_eq!(reordered[2].title, "B");
}
