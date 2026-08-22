use super::super::*;
use crate::task_service::tests::helpers::seed_role_assignment;

#[tokio::test]
async fn test_done_transition_emits_dependency_satisfied_event() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    crate::test_support::clear_project_execution_role_defaults(&db, &project_id).await;

    let prerequisite = service
        .create_task(
            project_id.clone(),
            "Implement prerequisite",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("prerequisite task creates");
    let dependent = service
        .create_task(
            project_id,
            "Implement dependent",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("dependent task creates");

    let claimed = service
        .claim_task(prerequisite.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("prerequisite claims");
    let review = service
        .transition(
            claimed.task.id.clone(),
            "review".to_owned(),
            claimed.task.version,
        )
        .await
        .expect("prerequisite enters review");
    assert_eq!(review.task.status, "merging");
    TaskDependencyRepo::add_dependency(&*db, &dependent.id, &prerequisite.id, &now_rfc3339())
        .await
        .expect("dependency creates");

    let mut rx = event_bus.subscribe();
    let done = service
        .transition(review.task.id, "done".to_owned(), review.task.version)
        .await
        .expect("prerequisite completes");
    assert_eq!(done.task.status, "done".to_owned());

    let mut status_event_seen = false;
    let mut committed_event_seen = false;
    let mut dependency_event = None;
    for _ in 0..3 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("transition event emits before timeout")
            .expect("transition event receives");
        match event.event_type.as_str() {
            "task.status_changed" => status_event_seen = true,
            "domain_event.committed" => committed_event_seen = true,
            "task.dependency_satisfied" => dependency_event = Some(event),
            _ => {}
        }
    }
    assert!(status_event_seen);
    assert!(committed_event_seen);
    let dependency_event = dependency_event.expect("dependency event receives");
    assert_eq!(dependency_event.entity_id, dependent.id);
    match dependency_event.context {
        EventContext::TaskDependencySatisfied {
            task_id,
            depends_on_id,
            timestamp,
        } => {
            assert_eq!(task_id, dependent.id);
            assert_eq!(depends_on_id, prerequisite.id);
            assert!(!timestamp.is_empty());
        }
        other => panic!("unexpected event context: {other:?}"),
    }
}

#[tokio::test]
async fn test_unsatisfied_dependency_blocks_agent_work_but_not_user_managed_moves() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;

    let prerequisite = service
        .create_task(
            project_id.clone(),
            "Implement prerequisite",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("prerequisite task creates");
    let dependent = service
        .create_task(
            project_id.clone(),
            "Implement dependent",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("dependent task creates");
    TaskDependencyRepo::add_dependency(&*db, &dependent.id, &prerequisite.id, &now_rfc3339())
        .await
        .expect("dependency creates");
    seed_role_assignment(
        &db,
        &dependent.id,
        crate::workflow::default_roles::PLANNER,
        Some(&agent_id),
    )
    .await;

    let blocked = service
        .transition(
            dependent.id.clone(),
            crate::workflow::default_states::PLANNING.to_owned(),
            dependent.version,
        )
        .await;
    assert!(matches!(
        blocked,
        Err(ServiceError::GuardRejection { guard, .. }) if guard == "dependency_gate"
    ));
    let still_todo = TaskRepo::get_by_id(&*db, &dependent.id, false)
        .await
        .expect("dependent reloads")
        .expect("dependent exists");
    assert_eq!(still_todo.status, crate::workflow::default_states::TODO);

    let user_moved = service
        .transition(
            still_todo.id.clone(),
            crate::workflow::default_states::PLANNING.to_owned(),
            (still_todo.version, None),
        )
        .await
        .expect("user-managed move bypasses dependency gate");
    assert_eq!(
        user_moved.task.status,
        crate::workflow::default_states::PLANNING
    );

    let dispatch = service
        .dispatch_initial_role_execution(
            &user_moved.task.id,
            &agent_id,
            crate::workflow::default_roles::PLANNER,
            "plan the task".to_owned(),
        )
        .await;
    assert!(matches!(dispatch, Err(ServiceError::DependencyGate)));

    let parked = service
        .create_task(
            project_id.clone(),
            "Parkable dependent",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("parkable task creates");
    TaskDependencyRepo::add_dependency(&*db, &parked.id, &prerequisite.id, &now_rfc3339())
        .await
        .expect("dependency creates");

    let moved_back = service
        .transition(
            parked.id.clone(),
            crate::workflow::default_states::BACKLOG.to_owned(),
            parked.version,
        )
        .await
        .expect("dependent can move back to backlog");
    assert_eq!(
        moved_back.task.status,
        crate::workflow::default_states::BACKLOG
    );

    let cancellable = service
        .create_task(
            project_id,
            "Cancellable dependent",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("cancellable task creates");
    TaskDependencyRepo::add_dependency(&*db, &cancellable.id, &prerequisite.id, &now_rfc3339())
        .await
        .expect("dependency creates");

    let cancelled = service
        .cancel_task(cancellable.id)
        .await
        .expect("dependent can be cancelled");
    assert_eq!(cancelled.status, crate::workflow::default_states::CANCELLED);
}

#[tokio::test]
async fn test_user_claim_bypasses_capacity_check() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = service
        .create_task(
            project_id,
            "Human task",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");

    let claimed = service
        .claim_task(
            task.id,
            Assignee::User("alice@example.com".to_owned()),
            None,
        )
        .await
        .expect("user claim succeeds without an agent");

    assert_eq!(claimed.task.status, "in_progress".to_owned());
    assert!(service
        .coder_assignment(&claimed.task.id)
        .await
        .expect("coder assignment loads")
        .is_none());
    assert_eq!(claimed.execution.agent_id, None);
}
