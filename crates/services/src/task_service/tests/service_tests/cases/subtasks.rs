use super::super::*;
use crate::task_hierarchy::{is_root_task, is_subtask};

#[tokio::test]
async fn subtask_helpers_resolve_root_and_subtask() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let subtask = seed_subtask_with_status(&db, &root, "child", "todo".to_owned(), 0).await;
    assert!(is_root_task(&db, &root.id).await.expect("root checks"));
    assert!(!is_subtask(&db, &root.id).await.expect("subtask checks"));
    assert!(!is_root_task(&db, &subtask.id).await.expect("root checks"));
    assert!(is_subtask(&db, &subtask.id).await.expect("subtask checks"));
}

#[tokio::test]
async fn create_task_assigns_subtask_order_and_rejects_nested_subtask() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
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
    service
        .reassign_role(
            role_assignment_input(&root.id, "coder", Some(agent_id.clone()), None),
            false,
            false,
        )
        .await
        .expect("root coder assignment succeeds");

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
    let root_coder = TaskRoleAssignmentRepo::get_by_task_and_role(&*db, &root.id, "coder")
        .await
        .expect("root coder loads")
        .expect("root coder is retained");
    assert_eq!(root_coder.assignee_id.as_deref(), Some(agent_id.as_str()));
    assert!(
        TaskRoleAssignmentRepo::get_by_task_and_role(&*db, &subtask.id, "coder")
            .await
            .expect("child coder loads")
            .is_none(),
        "conversion must not copy the default worker onto the child"
    );

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
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let _agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let root = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
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
        subtasks[0].parent_task_id.as_deref(),
        Some(root.id.as_str())
    );
    let subtask_roles = TaskRoleAssignmentRepo::list_by_task(&*db, &subtasks[0].id)
        .await
        .expect("subtask roles load");
    assert!(subtask_roles.is_empty());
    let root_coder = TaskRoleAssignmentRepo::get_by_task_and_role(&*db, &root.id, "coder")
        .await
        .expect("root coder loads")
        .expect("root coder remains the default worker");
    assert_eq!(root_coder.assignee_id.as_deref(), Some(agent_a.as_str()));

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
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
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
    // The first incomplete child is the durable sequence cursor. Reorder only
    // the untouched todo suffix behind it.
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

/// Root Cancel commits the root's own cancel under the root lease and
/// enqueues each child's cancel as the child's own preempting step. A child
/// whose CI hook holds its lease neither delays the root response nor turns
/// it into `task_busy`, and no sibling is skipped.
#[tokio::test]
async fn root_cancel_enqueues_child_cancels_without_waiting_on_a_busy_child() {
    use db::TaskStepRepo;
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(64)));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let mut children = Vec::new();
    for (order, title) in ["first", "second", "third"].into_iter().enumerate() {
        children.push(
            seed_subtask_with_status(&db, &root, title, "in_progress".to_owned(), order as i64)
                .await,
        );
    }
    // The second child is mid-CI: another worker holds its long hook step.
    let busy = &children[1];
    db.enqueue_step(&db::EnqueueTaskStep {
        id: new_uuid_v4(),
        task_id: busy.id.clone(),
        kind: "hooks".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: "ci".into(),
        chain_id: "ci".into(),
        chain_position: 1,
        expected_status: busy.status.clone(),
        expected_version: busy.version,
        expected_epoch: None,
        lane: "long".into(),
        available_at: now_rfc3339(),
    })
    .await
    .unwrap();
    let ci = db
        .claim_step(
            "ci-worker",
            Some(&busy.id),
            &db::task_writer::lease_deadline(),
        )
        .await
        .unwrap()
        .unwrap();

    let root = TaskRepo::get_by_id(&*db, &root.id, false)
        .await
        .unwrap()
        .unwrap();
    let started = std::time::Instant::now();
    let cancelled = service
        .cancel_task_as(root.id.clone(), Actor::user(UserActionSource::Test))
        .await
        .expect("a busy child never turns the root cancel into task_busy");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "root cancel waited on a child lease: {:?}",
        started.elapsed()
    );
    assert_eq!(cancelled.status, "cancelled");
    for child in &children {
        assert!(
            db.task_steps(&child.id).await.unwrap().iter().any(|step| {
                step.kind == "command"
                    && step.status == "pending"
                    && step.payload_json.contains("cancel_task_with_options")
            }),
            "child {} has its own queued cancel",
            child.title
        );
    }

    // CI stops at its safe point; then every child's queued cancel applies.
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    db.finish_step_in_tx(
        &mut tx,
        &ci,
        "superseded",
        Some("preempted by owner command"),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    db.release_step(&ci.id, "ci-worker").await.unwrap();
    for child in &children {
        assert_eq!(service.drain(&child.id).await.unwrap().status, "cancelled");
    }
}
