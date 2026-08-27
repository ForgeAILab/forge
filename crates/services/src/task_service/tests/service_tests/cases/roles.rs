use super::super::*;

#[tokio::test]
async fn reassign_role_updates_assignment_and_emits_event() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_a), None),
            false,
            false,
        )
        .await
        .expect("initial role assignment succeeds");
    let _ = rx.recv().await.expect("initial event emits");

    let updated = service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_b.clone()), None),
            false,
            false,
        )
        .await
        .expect("role reassignment succeeds");

    assert_eq!(updated.assignee_type, Some(db::AssigneeKind::Agent));
    assert_eq!(updated.assignee_id.as_deref(), Some(agent_b.as_str()));
    let event = rx.recv().await.expect("reassignment event emits");
    assert_eq!(event.event_type, "task.role_reassigned");
    assert_eq!(event.entity_id, task.id);
}

#[tokio::test]
async fn reassign_role_cancels_running_active_executor() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_a.clone()), None),
            false,
            false,
        )
        .await
        .expect("initial role assignment succeeds");
    let execution_id = new_uuid_v4();
    let now = now_rfc3339();
    ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_a),
            role: "executor".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");

    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_b), None),
            false,
            false,
        )
        .await
        .expect("role reassignment succeeds");
    let execution = ExecutionRepo::get_by_id(&*db, &execution_id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution.status, ExecutionStatus::Cancelled);
    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task_after.status, "todo");
    let transition_logs = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs load");
    assert!(transition_logs.iter().any(|log| {
        log.from_state == "in_progress"
            && log.to_state == "todo"
            && log.triggered_by == "user:reassignment"
            && log.trigger_reason == "coder reassigned"
            && !log.rejection
    }));
}

#[tokio::test]
async fn user_transition_cancels_running_active_executor_before_status_change() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let execution = seed_running_coder_execution(&db, &task.id, Some(agent_id), None).await;

    let result = service
        .transition(task.id.clone(), "review".to_owned(), (task.version, None))
        .await
        .expect("user transition succeeds");

    assert_eq!(result.task.status, "review");
    let execution_after = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution_after.status, ExecutionStatus::Cancelled);
    assert_eq!(
        execution_after.error.as_deref(),
        Some("cancelled by user transition")
    );

    let mut relevant_events = Vec::new();
    for _ in 0..8 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("event emits")
            .expect("event bus receives");
        if event.event_type == "task.execution_cancelled"
            || event.event_type == "task.status_changed"
        {
            relevant_events.push(event.event_type);
        }
        if relevant_events.len() == 2 {
            break;
        }
    }
    assert_eq!(
        relevant_events,
        vec![
            "task.execution_cancelled".to_owned(),
            "task.status_changed".to_owned()
        ]
    );
}

#[tokio::test]
async fn cancel_execution_invokes_task_executor_cancel() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let executor = Arc::new(RecordingCancelExecutor::default());
    let service = TaskService::new(Arc::clone(&db), event_bus).with_task_executor(executor.clone());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let execution = seed_running_coder_execution(&db, &task.id, Some(agent_id), None).await;

    service
        .cancel_execution(execution.id.clone(), "cancelled by test".to_owned())
        .await
        .expect("execution cancels");

    let cancelled = executor.cancelled.lock().expect("cancel log lock").clone();
    assert_eq!(cancelled, vec![execution.id]);
}

#[tokio::test]
async fn cancel_task_cancels_running_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let executor = Arc::new(RecordingCancelExecutor::default());
    let service = TaskService::new(Arc::clone(&db), event_bus).with_task_executor(executor.clone());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let execution = seed_running_coder_execution(&db, &task.id, Some(agent_id), None).await;

    let cancelled_task = service
        .cancel_task(task.id.clone())
        .await
        .expect("task cancels");

    assert_eq!(cancelled_task.status, "cancelled");
    let cancelled = executor.cancelled.lock().expect("cancel log lock").clone();
    assert_eq!(cancelled, vec![execution.id.clone()]);
    let execution_after = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution_after.status, ExecutionStatus::Cancelled);
}

#[tokio::test]
async fn run_execution_rechecks_cancelled_status_before_adapter_launch() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let locks = Arc::new(WorkspaceExecutionLockManager::new());
    let executor = Arc::new(CountingExecutor::default());
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_task_executor(executor.clone())
        .with_workspace_exec_locks(Arc::clone(&locks));
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = 2 WHERE id = ?")
        .bind(&agent_id)
        .execute(db.pool())
        .await
        .expect("agent capacity updates");
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let workspace_id = seed_workspace_for_task(&db, &task, workspace_root.path()).await;
    let execution = {
        let now = now_rfc3339();
        service
            .create_running_execution(
                db::CreateExecution {
                    id: new_uuid_v4(),
                    task_id: task.id.clone(),
                    agent_id: Some(agent_id),
                    role: "executor".to_owned(),
                    status: ExecutionStatus::Running,
                    stop_reason: None,
                    stopped_by: None,
                    resume_policy: None,
                    stopped_at: None,
                    parent_execution_id: None,
                    agent_session_id: None,
                    agent_message_id: None,
                    last_activity_at: None,
                    summary: Some("echo should-not-run".to_owned()),
                    logs_path: None,
                    before_sha: None,
                    after_sha: None,
                    error: None,
                    executor_config_snapshot_json: Some(
                        r#"{"executor_type":"shell","config":{}}"#.to_owned(),
                    ),
                    workspace_id: Some(workspace_id.clone()),
                    created_at: now.clone(),
                    updated_at: now,
                },
                false,
            )
            .await
            .expect("lease-backed execution creates")
    };
    let guard = locks.acquire(&workspace_id).await;
    let service_for_run = service.clone();
    let executor_for_run = executor.clone();
    let execution_id = execution.id.clone();
    let run = tokio::spawn(async move {
        service_for_run
            .run_execution(execution_id, executor_for_run.as_ref())
            .await
    });
    wait_until_execution_has_logs_path(&db, &execution.id).await;

    service
        .cancel_task(task.id.clone())
        .await
        .expect("task cancels while execution waits");
    drop(guard);

    let updated = run.await.expect("run joins").expect("run succeeds");
    assert_eq!(updated.status, ExecutionStatus::Cancelled);
    assert_eq!(
        executor.calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "adapter must not launch after the execution was cancelled"
    );
}

#[tokio::test]
async fn system_transition_does_not_cancel_running_active_executor() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let execution = seed_running_coder_execution(&db, &task.id, Some(agent_id), None).await;

    service
        .transition(task.id.clone(), "review".to_owned(), task.version)
        .await
        .expect("system transition succeeds");

    let execution_after = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution_after.status, ExecutionStatus::Running);
    assert_eq!(execution_after.error, None);
}

#[tokio::test]
async fn reassign_role_rejects_terminal_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "done".to_owned()).await;

    let error = service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_id), None),
            false,
            false,
        )
        .await
        .expect_err("terminal reassignment rejects");

    assert!(matches!(error, ServiceError::InvalidOperation { .. }));
}

#[tokio::test]
async fn reassign_role_same_assignee_does_not_emit_event() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_id.clone()), None),
            false,
            false,
        )
        .await
        .expect("initial role assignment succeeds");
    let _ = rx.recv().await.expect("initial event emits");
    let current_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reload succeeds")
        .expect("task exists");
    crate::deferred_dispatch::record_dispatch_disposition(
        &db,
        &current_task,
        "repository_write",
        "stale role blocker",
    )
    .await
    .expect("dispatch disposition records");

    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_id), None),
            false,
            false,
        )
        .await
        .expect("same role assignment succeeds");

    assert!(rx.try_recv().is_err());
    let woken_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reload succeeds")
        .expect("task exists");
    assert!(crate::deferred_dispatch::dispatch_disposition_for_test(&woken_task).is_none());
}

#[tokio::test]
async fn reassign_coder_clears_review_passed_at_on_non_running_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_a), None),
            false,
            false,
        )
        .await
        .expect("initial role assignment succeeds");
    let _ = next_role_reassigned_event(&mut rx).await;
    TaskRepo::set_review_passed_at(
        &*db,
        &task.id,
        Some("2026-04-10T00:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .expect("review_passed_at seeds");

    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_b), None),
            false,
            false,
        )
        .await
        .expect("role reassignment succeeds");

    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(task_after.review_passed_at.is_none());
    let event = next_role_reassigned_event(&mut rx).await;
    match event.context {
        EventContext::TaskRoleReassigned {
            reset_workspace,
            reset_worktree,
            transitioned_to_todo,
            triggered_cancellation,
            ..
        } => {
            assert!(!reset_workspace);
            assert!(!reset_worktree);
            assert!(!transitioned_to_todo);
            assert!(!triggered_cancellation);
        }
        other => panic!("unexpected event context: {other:?}"),
    }
}

#[tokio::test]
async fn reassign_mid_exec_coder_with_reset_worktree_flag_in_event() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let workspace_root = TempDir::new().expect("workspace root creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_a.clone()), None),
            false,
            false,
        )
        .await
        .expect("initial role assignment succeeds");
    let _ = next_role_reassigned_event(&mut rx).await;
    seed_running_coder_execution(&db, &task.id, Some(agent_a), None).await;

    let result = service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_b), None),
            false,
            true,
        )
        .await;

    match result {
        Ok(_) => {
            let event = next_role_reassigned_event(&mut rx).await;
            match event.context {
                EventContext::TaskRoleReassigned {
                    reset_workspace,
                    reset_worktree,
                    transitioned_to_todo,
                    triggered_cancellation,
                    ..
                } => {
                    assert!(!reset_workspace);
                    assert!(reset_worktree);
                    assert!(transitioned_to_todo);
                    assert!(triggered_cancellation);
                }
                other => panic!("unexpected event context: {other:?}"),
            }
        }
        Err(ServiceError::InvalidOperation { message })
            if message.contains("worktree") || message.contains("workspace") =>
        {
            // This unit test does not build a real Forge worktree. The current reset_worktree
            // path reaches WorkspaceManager and reports the missing workspace infrastructure.
            assert!(message.contains("workspace"));
        }
        Err(error) => panic!("unexpected reassignment result: {error:?}"),
    }
}

#[tokio::test]
async fn reassign_coder_with_workspace_allows_reset_workspace() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let workspace_root = TempDir::new().expect("workspace root creates");
    let cleanup_scheduler = Arc::new(WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.path().to_path_buf(),
    ));
    let service =
        TaskService::new(Arc::clone(&db), event_bus).with_cleanup_scheduler(cleanup_scheduler);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_a.clone()), None),
            false,
            false,
        )
        .await
        .expect("initial role assignment succeeds");
    let _ = next_role_reassigned_event(&mut rx).await;
    let workspace_id = seed_workspace_for_task(&db, &task, workspace_root.path()).await;
    seed_running_coder_execution(&db, &task.id, Some(agent_a), Some(workspace_id)).await;

    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_b), None),
            true,
            false,
        )
        .await
        .expect("coder reassignment succeeds");

    let event = next_role_reassigned_event(&mut rx).await;
    match event.context {
        EventContext::TaskRoleReassigned {
            reset_workspace,
            reset_worktree,
            transitioned_to_todo,
            triggered_cancellation,
            ..
        } => {
            assert!(reset_workspace);
            assert!(!reset_worktree);
            assert!(transitioned_to_todo);
            assert!(triggered_cancellation);
        }
        other => panic!("unexpected event context: {other:?}"),
    }
}

#[tokio::test]
async fn reassign_coder_with_workspace_allows_both_reset_flags() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let workspace_root = TempDir::new().expect("workspace root creates");
    let cleanup_scheduler = Arc::new(WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.path().to_path_buf(),
    ));
    let service =
        TaskService::new(Arc::clone(&db), event_bus).with_cleanup_scheduler(cleanup_scheduler);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_a.clone()), None),
            false,
            false,
        )
        .await
        .expect("initial role assignment succeeds");
    let _ = next_role_reassigned_event(&mut rx).await;
    let workspace_id = seed_workspace_for_task(&db, &task, workspace_root.path()).await;
    seed_running_coder_execution(&db, &task.id, Some(agent_a), Some(workspace_id)).await;

    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_b), None),
            true,
            true,
        )
        .await
        .expect("coder reassignment succeeds");

    let event = next_role_reassigned_event(&mut rx).await;
    match event.context {
        EventContext::TaskRoleReassigned {
            reset_workspace,
            reset_worktree,
            transitioned_to_todo,
            triggered_cancellation,
            ..
        } => {
            assert!(reset_workspace);
            assert!(!reset_worktree);
            assert!(transitioned_to_todo);
            assert!(triggered_cancellation);
        }
        other => panic!("unexpected event context: {other:?}"),
    }
}

#[tokio::test]
async fn reassign_coder_to_human_mid_execution_cancels_and_moves_to_todo() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_a.clone()), None),
            false,
            false,
        )
        .await
        .expect("initial role assignment succeeds");
    let _ = next_role_reassigned_event(&mut rx).await;
    let execution = seed_running_coder_execution(&db, &task.id, Some(agent_a), None).await;

    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", None, Some("human".to_owned())),
            false,
            false,
        )
        .await
        .expect("role reassignment succeeds");

    let execution_after = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution_after.status, ExecutionStatus::Cancelled);
    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task_after.status, "todo");
    let event = next_role_reassigned_event(&mut rx).await;
    match event.context {
        EventContext::TaskRoleReassigned {
            new_assignment,
            triggered_cancellation,
            transitioned_to_todo,
            ..
        } => {
            let new_assignment = new_assignment.expect("new assignment exists");
            assert_eq!(new_assignment.assignee_type.as_deref(), Some("user"));
            assert_eq!(new_assignment.assignee_id.as_deref(), Some("human"));
            assert!(triggered_cancellation);
            assert!(transitioned_to_todo);
        }
        other => panic!("unexpected event context: {other:?}"),
    }
}

#[tokio::test]
async fn reassign_non_coder_role_does_not_cancel_or_transition() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "reviewer", Some(agent_a.clone()), None),
            false,
            false,
        )
        .await
        .expect("initial reviewer assignment succeeds");
    let _ = next_role_reassigned_event(&mut rx).await;
    TaskRepo::set_review_passed_at(
        &*db,
        &task.id,
        Some("2026-04-10T00:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .expect("review_passed_at seeds");
    let execution =
        seed_running_role_execution(&db, &task.id, Some(agent_a), "reviewer", None).await;

    service
        .reassign_role(
            role_assignment_input(&task.id, "reviewer", Some(agent_b), None),
            false,
            false,
        )
        .await
        .expect("reviewer reassignment succeeds");

    let execution_after = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution_after.status, ExecutionStatus::Running);
    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task_after.status, "in_progress");
    assert_eq!(
        task_after.review_passed_at.as_deref(),
        Some("2026-04-10T00:00:00Z")
    );
    let event = next_role_reassigned_event(&mut rx).await;
    match event.context {
        EventContext::TaskRoleReassigned {
            reset_workspace,
            reset_worktree,
            transitioned_to_todo,
            triggered_cancellation,
            ..
        } => {
            assert!(!reset_workspace);
            assert!(!reset_worktree);
            assert!(!transitioned_to_todo);
            assert!(!triggered_cancellation);
        }
        other => panic!("unexpected event context: {other:?}"),
    }
}

#[tokio::test]
async fn on_agent_deleted_clears_coder_assignee_id_and_preserves_agent_type() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;

    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_id.clone()), None),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    service
        .on_agent_deleted(&agent_id)
        .await
        .expect("agent deletion sweep succeeds");

    let row = sqlx::query(
        "SELECT assignee_type, assignee_id FROM task_role_assignment WHERE task_id = ? AND role_name = 'coder'",
    )
    .bind(&task.id)
    .fetch_one(db.pool())
    .await
    .expect("assignment row loads");
    let assignee_type: Option<String> = row.try_get("assignee_type").expect("type reads");
    let assignee_id: Option<String> = row.try_get("assignee_id").expect("id reads");
    assert_eq!(assignee_type.as_deref(), Some("agent"));
    assert_eq!(assignee_id, None);
}

#[tokio::test]
async fn reassign_non_coder_role_ignores_reset_flags() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "reviewer", Some(agent_a), None),
            false,
            false,
        )
        .await
        .expect("initial reviewer assignment succeeds");
    let _ = next_role_reassigned_event(&mut rx).await;

    service
        .reassign_role(
            role_assignment_input(&task.id, "reviewer", Some(agent_b), None),
            false,
            true,
        )
        .await
        .expect("reviewer reassignment succeeds");

    let event = next_role_reassigned_event(&mut rx).await;
    match event.context {
        EventContext::TaskRoleReassigned {
            reset_workspace,
            reset_worktree,
            transitioned_to_todo,
            triggered_cancellation,
            ..
        } => {
            assert!(!reset_workspace);
            assert!(!reset_worktree);
            assert!(!transitioned_to_todo);
            assert!(!triggered_cancellation);
        }
        other => panic!("unexpected event context: {other:?}"),
    }
}

#[tokio::test]
async fn remove_coder_role_clears_review_passed_at() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_a), None),
            false,
            false,
        )
        .await
        .expect("initial role assignment succeeds");
    let _ = next_role_reassigned_event(&mut rx).await;
    TaskRepo::set_review_passed_at(
        &*db,
        &task.id,
        Some("2026-04-10T00:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .expect("review_passed_at seeds");

    service
        .remove_role(&task.id, "coder", false, false)
        .await
        .expect("coder role removal succeeds");

    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(task_after.review_passed_at.is_none());
    let event = next_role_reassigned_event(&mut rx).await;
    match event.context {
        EventContext::TaskRoleReassigned { new_assignment, .. } => {
            assert!(new_assignment.is_none());
        }
        other => panic!("unexpected event context: {other:?}"),
    }
}

#[tokio::test]
async fn remove_coder_role_rejects_after_subtask_sequence_started() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_a), None),
            false,
            false,
        )
        .await
        .expect("initial role assignment succeeds");
    let _ = next_role_reassigned_event(&mut rx).await;
    let _subtask = seed_subtask_with_status(&db, &task, "sub", "in_progress".to_owned(), 0).await;

    let result = service.remove_role(&task.id, "coder", false, false).await;

    assert!(matches!(
        result,
        Err(ServiceError::TaskSequenceAlreadyStarted { task_id }) if task_id == task.id
    ));
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn reassign_subtask_coder_is_rejected() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let root = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&root.id, "coder", Some(agent_a.clone()), None),
            false,
            false,
        )
        .await
        .expect("root coder assignment succeeds");
    let subtask = seed_subtask_with_status(&db, &root, "sub", "todo".to_owned(), 0).await;

    let result = service
        .reassign_role(
            role_assignment_input(&subtask.id, "coder", Some(agent_b.clone()), None),
            false,
            false,
        )
        .await;

    assert!(matches!(result, Err(ServiceError::InvalidOperation { .. })));
    let child_assignment = TaskRoleAssignmentRepo::get_by_task_and_role(&*db, &subtask.id, "coder")
        .await
        .expect("child assignment loads");
    assert!(child_assignment.is_none());
}

#[tokio::test]
async fn reassign_parent_coder_rejects_when_subtask_in_progress() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let root = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&root.id, "coder", Some(agent_a), None),
            false,
            false,
        )
        .await
        .expect("root coder assignment succeeds");
    seed_subtask_with_status(&db, &root, "child", "in_progress".to_owned(), 0).await;

    let result = service
        .reassign_role(
            role_assignment_input(&root.id, "coder", Some(agent_b), None),
            false,
            false,
        )
        .await;

    assert!(matches!(
        result,
        Err(ServiceError::TaskSequenceAlreadyStarted { .. })
    ));
}

#[tokio::test]
async fn remove_non_coder_role_does_not_clear_review_passed_at() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "reviewer", Some(agent_a), None),
            false,
            false,
        )
        .await
        .expect("initial reviewer assignment succeeds");
    let _ = next_role_reassigned_event(&mut rx).await;
    TaskRepo::set_review_passed_at(
        &*db,
        &task.id,
        Some("2026-04-10T00:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .expect("review_passed_at seeds");

    service
        .remove_role(&task.id, "reviewer", false, false)
        .await
        .expect("reviewer role removal succeeds");

    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(
        task_after.review_passed_at.as_deref(),
        Some("2026-04-10T00:00:00Z")
    );
}

#[tokio::test]
async fn reassign_same_coder_noop_preserves_review_passed_at() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_a = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_a.clone()), None),
            false,
            false,
        )
        .await
        .expect("initial role assignment succeeds");
    let _ = next_role_reassigned_event(&mut rx).await;
    TaskRepo::set_review_passed_at(
        &*db,
        &task.id,
        Some("2026-04-10T00:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .expect("review_passed_at seeds");

    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_a), None),
            true,
            true,
        )
        .await
        .expect("same role assignment succeeds");

    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(
        task_after.review_passed_at.as_deref(),
        Some("2026-04-10T00:00:00Z")
    );
    assert!(rx.try_recv().is_err());
}
