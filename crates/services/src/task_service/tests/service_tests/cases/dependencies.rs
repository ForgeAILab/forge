use super::super::*;
use crate::task_service::tests::helpers::seed_role_assignment;

#[tokio::test]
async fn create_task_with_dependencies_commits_links_atomically() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;

    let first = service
        .create_task(
            project_id.clone(),
            "First prerequisite",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("first prerequisite creates");
    let second = service
        .create_task(
            project_id.clone(),
            "Second prerequisite",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("second prerequisite creates");

    let dependent = service
        .create_task_with_dependencies(
            project_id.clone(),
            "Dependent task",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            vec![first.id.clone(), second.id.clone()],
        )
        .await
        .expect("dependent creates with prerequisite links");
    let links = TaskDependencyRepo::list_dependencies(&*db, &dependent.id)
        .await
        .expect("dependency links load");
    assert_eq!(links.len(), 2);
    assert!(links.contains(&first.id));
    assert!(links.contains(&second.id));

    let failed = service
        .create_task_with_dependencies(
            project_id,
            "Missing prerequisite",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            vec!["missing-prerequisite".to_owned()],
        )
        .await
        .expect_err("missing prerequisite rejects the whole create");
    assert!(matches!(
        failed,
        ServiceError::NotFound { entity: "task", .. }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task")
            .fetch_one(db.pool())
            .await
            .expect("task count loads"),
        3,
        "failed dependency validation must not leave a Task behind"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_dependency")
            .fetch_one(db.pool())
            .await
            .expect("dependency count loads"),
        2,
        "failed dependency validation must not leave links behind"
    );
}

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
    // Adding a dependency edge advances the Task version.
    let dependent = TaskRepo::get_by_id(&*db, &dependent.id, false)
        .await
        .expect("task reloads after its dependency edge")
        .expect("task exists");

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
async fn done_prerequisite_wakes_dependent_with_stale_dispatch_disposition() {
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
    service
        .add_task_dependency(&dependent.id, &prerequisite.id)
        .await
        .expect("dependency creates");

    // Simulate exactly what the dispatcher persists on the *dependent* Task
    // after the dependency gate refuses a dispatch attempt
    // (`record_dispatch_disposition` in `task_dispatcher::initial_scheduling`):
    // a disposition keyed on the dependent's own, unchanged version.
    let dependent_before = TaskRepo::get_by_id(&*db, &dependent.id, false)
        .await
        .expect("dependent reloads")
        .expect("dependent exists");
    crate::deferred_dispatch::record_dispatch_disposition(
        &db,
        &dependent_before,
        "coder",
        "guard rejected: dependency_gate: task has 1 unsatisfied dependency",
    )
    .await
    .expect("dispatch disposition records");
    // `record_dispatch_disposition` persists through key-level metadata; it
    // does not mutate the snapshot passed to it, so re-read before asserting.
    let dependent_parked = TaskRepo::get_by_id(&*db, &dependent.id, false)
        .await
        .expect("dependent reloads")
        .expect("dependent exists");
    assert!(
        crate::deferred_dispatch::dispatch_disposition_for_test(&dependent_parked).is_some(),
        "the dispatcher's dependency-gate refusal must be persisted before the prerequisite completes"
    );

    // Drive the prerequisite all the way through to `done`.
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
    let done = service
        .transition(review.task.id, "done".to_owned(), review.task.version)
        .await
        .expect("prerequisite completes");
    assert_eq!(done.task.status, "done");

    // Completing the prerequisite wakes the dependent and advances its own
    // version, fencing any in-flight stale disposition writer (F6).
    let dependent_after = TaskRepo::get_by_id(&*db, &dependent.id, false)
        .await
        .expect("dependent reloads")
        .expect("dependent exists");
    assert_eq!(
        dependent_after.version,
        dependent_before.version + 1,
        "the dependency wake advances the dependent's dispatch generation"
    );
    assert!(
        crate::deferred_dispatch::dispatch_disposition_for_test(&dependent_after).is_none(),
        "completing the prerequisite must wake the dependent's stale dispatch disposition"
    );
}

#[tokio::test]
async fn removing_dependency_wakes_dependent_with_stale_dispatch_disposition() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;

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
    service
        .add_task_dependency(&dependent.id, &prerequisite.id)
        .await
        .expect("dependency creates");

    // Simulate the dispatcher's deterministic dependency-gate disposition on
    // the dependent before removing the edge that caused it.
    let dependent_before = TaskRepo::get_by_id(&*db, &dependent.id, false)
        .await
        .expect("dependent reloads")
        .expect("dependent exists");
    crate::deferred_dispatch::record_dispatch_disposition(
        &db,
        &dependent_before,
        "coder",
        "guard rejected: dependency_gate: task has 1 unsatisfied dependency",
    )
    .await
    .expect("dispatch disposition records");
    let dependent_parked = TaskRepo::get_by_id(&*db, &dependent.id, false)
        .await
        .expect("dependent reloads")
        .expect("dependent exists");
    assert!(
        crate::deferred_dispatch::dispatch_disposition_for_test(&dependent_parked).is_some(),
        "the dependency-gate disposition must be persisted before the edge is removed"
    );

    service
        .remove_task_dependency(&dependent.id, &prerequisite.id)
        .await
        .expect("dependency removes");

    let dependent_after = TaskRepo::get_by_id(&*db, &dependent.id, false)
        .await
        .expect("dependent reloads")
        .expect("dependent exists");
    assert!(
        crate::deferred_dispatch::dispatch_disposition_for_test(&dependent_after).is_none(),
        "removing the dependency must wake the dependent's stale dispatch disposition"
    );
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
    // The dependency edge advanced the Task version.
    let dependent = TaskRepo::get_by_id(&*db, &dependent.id, false)
        .await
        .expect("dependent reloads")
        .expect("dependent exists");
    let blocked = service
        .transition(
            dependent.id.clone(),
            crate::workflow::default_states::PLANNING.to_owned(),
            dependent.version,
        )
        .await;
    assert!(
        matches!(
            blocked,
            Err(ServiceError::GuardRejection { ref guard, .. }) if guard == "dependency_gate"
        ),
        "an unsatisfied dependency must reject agent work, got {:?}",
        blocked.as_ref().map(|result| result.task.status.clone())
    );
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
    // Adding a dependency edge advances the Task version.
    let parked = TaskRepo::get_by_id(&*db, &parked.id, false)
        .await
        .expect("task reloads after its dependency edge")
        .expect("task exists");

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
    // Adding a dependency edge advances the Task version.
    let cancellable = TaskRepo::get_by_id(&*db, &cancellable.id, false)
        .await
        .expect("task reloads after its dependency edge")
        .expect("task exists");

    let cancelled = service
        .cancel_task(cancellable.id)
        .await
        .expect("dependent can be cancelled");
    assert_eq!(cancelled.status, crate::workflow::default_states::CANCELLED);
}

#[tokio::test]
async fn cancelled_prerequisite_durably_blocks_dependents_until_link_is_removed() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;

    let prerequisite = service
        .create_task(
            project_id.clone(),
            "Disposable prerequisite",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("prerequisite creates");
    let dependent = service
        .create_task(
            project_id,
            "Dependent work",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("dependent creates");
    service
        .add_task_dependency(&dependent.id, &prerequisite.id)
        .await
        .expect("dependency creates");

    service
        .cancel_task(prerequisite.id.clone())
        .await
        .expect("prerequisite cancels");

    let blocked = TaskRepo::get_by_id(&*db, &dependent.id, false)
        .await
        .expect("dependent reloads")
        .expect("dependent exists");
    let blocker: serde_json::Value =
        serde_json::from_str(blocked.blocked_json.as_deref().expect("durable blocker"))
            .expect("blocker parses");
    assert_eq!(blocker["source"], "dependency_gate");
    assert_eq!(
        blocker["details"]["cancelled_dependency_ids"],
        json!([prerequisite.id])
    );
    let annotation: api_types::TaskBlockingAnnotation = serde_json::from_str(
        blocked
            .error_annotation
            .as_deref()
            .expect("recovery annotation"),
    )
    .expect("annotation parses");
    assert_eq!(annotation.blocking_reason, "dependency_cancelled");
    assert_eq!(
        annotation.recovery_actions,
        vec![api_types::RecoveryAction::CancelTask]
    );

    service
        .remove_task_dependency(&dependent.id, &prerequisite.id)
        .await
        .expect("dependency removes");
    let unblocked = TaskRepo::get_by_id(&*db, &dependent.id, false)
        .await
        .expect("dependent reloads")
        .expect("dependent exists");
    assert!(unblocked.blocked_json.is_none());
    assert!(unblocked.error_annotation.is_none());
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
