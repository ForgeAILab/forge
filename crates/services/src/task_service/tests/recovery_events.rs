use super::helpers::*;
use super::*;

#[tokio::test]
async fn test_reset_retry_window_publishes_recovery_and_resume_events() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let (project_id, _repo_id, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let task =
        seed_task_with_status(&db, &project_id, crate::workflow::default_states::REVIEW).await;
    let execution = seed_execution(
        &db,
        &task.id,
        None,
        crate::workflow::default_roles::REVIEWER,
        ExecutionStatus::Completed,
        Some("review-session"),
        "2026-05-02T10:00:00Z",
    )
    .await;
    seed_failed_review(
        &db,
        &task.id,
        &execution.id,
        1,
        json!({ "ci_steps": [{"command": "cargo test", "exit_code": 1}] }),
    )
    .await;
    seed_review_rejection_log(&db, &task.id, "review failed once").await;
    seed_review_rejection_log(&db, &task.id, "review failed twice").await;
    let task = set_retry_exhausted_metadata(&db, &task).await;
    let mut rx = event_bus.subscribe();

    service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::ResetRetryWindow,
            Some("reason".to_owned()),
            None,
        )
        .await
        .expect("reset retry window succeeds");

    let mut events = Vec::new();
    while let Ok(Ok(event)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await
    {
        events.push(event);
    }

    let recovery_events = events
        .iter()
        .filter(|event| event.event_type == "task.recovery_applied")
        .collect::<Vec<_>>();
    assert_eq!(recovery_events.len(), 2);
    let event = recovery_events
        .iter()
        .find(|event| {
            matches!(
                &event.context,
                EventContext::RecoveryApplied { action, .. } if action == "reset_retry_window"
            )
        })
        .expect("reset retry window recovery event");
    assert_eq!(event.entity_id, task.id);
    match &event.context {
        EventContext::RecoveryApplied {
            project_id: event_project_id,
            task_id,
            action,
            state,
            transition_log_id,
        } => {
            assert_eq!(event_project_id, &project_id);
            assert_eq!(task_id, &task.id);
            assert_eq!(action, "reset_retry_window");
            assert_eq!(
                state.as_deref(),
                Some(crate::workflow::default_states::REVIEW)
            );
            assert!(transition_log_id.is_some());
        }
        other => panic!("unexpected event context: {other:?}"),
    }

    assert!(
        events.iter().any(|event| {
            matches!(
                &event.context,
                EventContext::RecoveryApplied { action, .. } if action == "resume_process"
            )
        }),
        "reset_retry_window should resume process and publish resume_process recovery event"
    );
    assert!(
        events
            .iter()
            .any(|event| event.event_type == "task.status_changed"),
        "reset_retry_window should resume work and publish task.status_changed"
    );
}

async fn seed_assigned_task(db: &SqliteDb, project_id: &str, agent_id: &str) -> Task {
    let now = now_rfc3339();
    TaskRepo::create(
        db,
        db::CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.to_owned(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: Some("agent".to_owned()),
            assignee_id: Some(agent_id.to_owned()),
            title: "assigned task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: crate::workflow::default_states::IN_PROGRESS.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates")
}

#[tokio::test]
async fn test_reset_to_initial_clears_assignee_after_workspace_failure() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_assigned_task(&db, &project_id, &agent_id).await;
    assert_eq!(task.assignee_id.as_deref(), Some(agent_id.as_str()));

    // The workspace-failure path ends in fail_task, which clears any blocking
    // annotation; the reset decision must survive on the failed metadata kind.
    service
        .fail_task(
            task.id.clone(),
            "workspace reset required: task branch no longer exists",
            Some(api_types::FailureKind::WorkspaceFailed),
            None,
        )
        .await
        .expect("task fails");

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::ResetToInitial,
            None,
            None,
        )
        .await
        .expect("task resets");
    assert_eq!(
        recovered.assignee_id, None,
        "workspace failure reset must clear the assignee"
    );
    assert!(recovered.failed_json.is_none());
}

#[tokio::test]
async fn test_reset_to_initial_keeps_assignee_for_non_workspace_failure() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_assigned_task(&db, &project_id, &agent_id).await;
    assert_eq!(task.assignee_id.as_deref(), Some(agent_id.as_str()));

    service
        .fail_task(
            task.id.clone(),
            "executor crashed",
            Some(api_types::FailureKind::ExecutorFailed),
            None,
        )
        .await
        .expect("task fails");

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::ResetToInitial,
            None,
            None,
        )
        .await
        .expect("task resets");
    assert_eq!(
        recovered.assignee_id.as_deref(),
        Some(agent_id.as_str()),
        "non-workspace failure reset keeps the assignee"
    );
}
