use super::super::*;

#[tokio::test]
async fn run_execution_dispatches_shell_adapter_and_updates_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Run shell",
            Some("printf service-run-ok".to_owned()),
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
        .claim_task(task.id, Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    ExecutionRepo::update(
        &*db,
        db::UpdateExecution {
            id: claimed.execution.id.clone(),
            status: None,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: Some(Some("test-session".to_owned())),
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("seed executor session id");
    let workspace =
        WorkspaceRepo::get_by_id(&*db, claimed.execution.workspace_id.as_deref().unwrap())
            .await
            .expect("workspace loads")
            .expect("workspace exists");
    std::fs::create_dir_all(&workspace.worktree_path).expect("workspace dir creates");

    let registry = Arc::new(cli_adapters::default_registry());
    let executor = executors::AdapterExecutor::new(registry);
    let execution = service
        .run_execution(claimed.execution.id, &executor)
        .await
        .expect("execution runs");

    assert_eq!(execution.status, ExecutionStatus::Completed);
    let logs_path = execution.logs_path.expect("logs path recorded");
    assert!(
        logs_path.contains(&format!(
            "/.forge/logs/{}/{}/",
            task.project_id, workspace.task_id
        )),
        "logs path should live under durable project/task log dir, got {logs_path}"
    );
    let logs = executors::LogReader::read(std::path::Path::new(&logs_path), 0, 100)
        .await
        .expect("logs read");
    assert!(logs.entries.iter().any(|entry| {
        entry.payload.get("line").and_then(|line| line.as_str()) == Some("service-run-ok")
    }));
}

#[tokio::test]
async fn workflow_dispatched_commitless_completion_fails_and_schedules_retry() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "No-op completion",
            Some("narrate without changing anything".to_owned()),
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
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    sqlx::query("UPDATE execution SET role = 'worker' WHERE id = ?")
        .bind(&claimed.execution.id)
        .execute(db.pool())
        .await
        .expect("execution uses the autonomous worker role");
    // Stamp the dispatcher metadata the workflow dispatch path records, so
    // this run is measured as an autonomous role dispatch (user-claimed runs
    // without the metadata stay exempt).
    let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    let mut snapshot: serde_json::Value = serde_json::from_str(
        execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("claimed execution has a snapshot"),
    )
    .expect("snapshot parses");
    snapshot["dispatch"] = json!({ "target_role": execution.role });
    ExecutionRepo::update(
        &*db,
        db::UpdateExecution {
            id: claimed.execution.id.clone(),
            status: None,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(Some(snapshot.to_string())),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("dispatch metadata stamps");

    let updated = service
        .run_execution(claimed.execution.id.clone(), &NoDiffExecutor)
        .await
        .expect("execution runs");

    assert_eq!(updated.status, ExecutionStatus::Failed);
    assert_eq!(updated.role, "worker");
    assert!(
        updated
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("nothing committed on the Task branch"),
        "error names the commit-less completion: {:?}",
        updated.error
    );

    // The failure enters the normal executor-failure machinery: the retry
    // budget is consumed and the execution becomes dispatcher-resumable.
    let after = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert_eq!(after.resume_policy, Some(db::ResumePolicy::Auto));
    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(task_after.metadata_json.as_deref().unwrap_or("{}"))
            .expect("task metadata parses");
    assert_eq!(metadata["execution_retry_count"], 1);
    assert!(
        metadata.get("deferred_dispatch").is_some(),
        "a deferred redispatch is scheduled: {metadata}"
    );
}

#[tokio::test]
async fn reviewer_provider_unavailability_defers_without_consuming_task_retry_budget() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Review after provider recovery",
            Some("review the current worktree".to_owned()),
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
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    sqlx::query("UPDATE execution SET role = 'reviewer' WHERE id = ?")
        .bind(&claimed.execution.id)
        .execute(db.pool())
        .await
        .expect("execution becomes reviewer-scoped");

    let updated = service
        .run_execution(claimed.execution.id.clone(), &ExecutorUnavailableExecutor)
        .await
        .expect("provider unavailability is persisted");

    assert_eq!(updated.status, ExecutionStatus::Failed);
    assert_eq!(updated.role, "reviewer");
    assert!(updated
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("provider unavailable"));

    let execution_after = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert_eq!(execution_after.resume_policy, Some(db::ResumePolicy::Auto));

    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(task_after.metadata_json.as_deref().unwrap_or("{}"))
            .expect("task metadata parses");
    assert!(
        metadata.get("deferred_dispatch").is_some(),
        "reviewer execution should wait for provider recovery: {metadata}"
    );
    assert!(
        metadata.get("execution_retry_count").is_none(),
        "provider availability must not spend Task retry budget: {metadata}"
    );
    assert!(task_after.blocked_json.is_none());
}

#[tokio::test]
async fn uncommitted_native_worker_failure_prompts_a_retry_and_preserves_the_diff() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Commit the preserved implementation",
            Some("write the implementation and commit it".to_owned()),
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
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let workspace = WorkspaceRepo::get_by_id(
        &*db,
        claimed
            .execution
            .workspace_id
            .as_deref()
            .expect("workspace id exists"),
    )
    .await
    .expect("workspace loads")
    .expect("workspace exists");

    let updated = service
        .run_execution(
            claimed.execution.id.clone(),
            &UncommittedWorktreeFailureExecutor,
        )
        .await
        .expect("execution failure is persisted");

    let expected_error = ExecutorError::Other(
        crate::embedded_task_executor::UNCOMMITTED_WORKTREE_FAILURE.to_owned(),
    )
    .to_string();
    assert_eq!(updated.status, ExecutionStatus::Failed);
    assert_eq!(updated.error.as_deref(), Some(expected_error.as_str()));
    assert!(std::path::Path::new(&workspace.worktree_path)
        .join("uncommitted.txt")
        .exists());
    assert!(
        !git::is_worktree_clean(std::path::Path::new(&workspace.worktree_path))
            .await
            .expect("worktree cleanliness reads")
    );

    let comments = db::TaskCommentRepo::list_comments(
        &*db,
        &task.id,
        db::PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: db::SortBy::CreatedAt,
            sort_order: db::SortOrder::Asc,
        },
    )
    .await
    .expect("comments load");
    assert!(comments.items.iter().any(|comment| {
        comment.content.contains("worktree was preserved")
            && comment.content.contains("commit the result")
    }));

    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(task_after.metadata_json.as_deref().unwrap_or("{}"))
            .expect("task metadata parses");
    assert_eq!(metadata["execution_retry_count"], 1);
    assert!(metadata.get("deferred_dispatch").is_some());
    let execution_after = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert_eq!(execution_after.resume_policy, Some(db::ResumePolicy::Auto));
}

#[tokio::test]
async fn run_execution_reissues_stale_lease_after_task_row_moves() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Lease survives task-row movement",
            Some("printf lease-race-ok".to_owned()),
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
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");

    // Any Task-row mutation between lease issuance and dispatch (role
    // handoff, retry-metadata clear, concurrent transition) bumps the
    // version and breaks the lease's exact match. The dispatch must
    // recover by reissuing this execution's lease, not hard-fail.
    sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("task version bumps");

    let updated = service
        .run_execution(claimed.execution.id, &NoDiffExecutor)
        .await
        .expect("execution recovers from the stale lease instead of failing");

    assert_eq!(updated.status, ExecutionStatus::Completed);
}

#[tokio::test]
async fn user_claimed_commitless_completion_stays_completed() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Claimed no-op",
            Some("user-invoked run".to_owned()),
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
        .claim_task(task.id, Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");

    // No dispatcher metadata: the claim-launched run keeps the historical
    // completion semantics even when it changes nothing.
    let updated = service
        .run_execution(claimed.execution.id, &NoDiffExecutor)
        .await
        .expect("execution runs");

    assert_eq!(updated.status, ExecutionStatus::Completed);
}

#[tokio::test]
async fn run_execution_emits_terminal_execution_event() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Emit terminal event",
            Some("complete".to_owned()),
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
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let mut rx = event_bus.subscribe();

    let execution = service
        .run_execution(claimed.execution.id.clone(), &NoDiffExecutor)
        .await
        .expect("execution runs");

    assert_eq!(execution.status, ExecutionStatus::Completed);
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let event = rx.recv().await.expect("terminal event emits");
            if event.event_type == "execution.completed" {
                break event;
            }
        }
    })
    .await
    .expect("execution.completed event received");
    assert_eq!(event.entity_id, claimed.execution.id);
    match event.context {
        EventContext::ExecutionCompleted { task_id } => assert_eq!(task_id, task.id),
        other => panic!("unexpected event context: {other:?}"),
    }
}

#[tokio::test]
async fn run_execution_rejects_when_terminal_active_in_workspace() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let terminal_activity = Arc::new(TerminalActivityTracker::default());
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_terminal_activity_tracker(Arc::clone(&terminal_activity));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Terminal blocks execution",
            Some("complete".to_owned()),
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
        .claim_task(task.id, Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let workspace_id = claimed
        .execution
        .workspace_id
        .as_deref()
        .expect("execution has workspace");
    assert!(terminal_activity.try_mark_active(workspace_id).await);
    let executor = CountingExecutor::default();

    let error = service
        .run_execution(claimed.execution.id, &executor)
        .await
        .expect_err("active terminal rejects execution");

    assert!(matches!(
        error,
        ServiceError::TerminalActiveExecution { .. }
    ));
    assert_eq!(
        executor.calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "executor must not launch while a terminal is active"
    );
}

#[tokio::test]
async fn run_execution_batches_execution_log_events() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(128));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Batch execution logs",
            Some("emit logs".to_owned()),
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
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let mut rx = event_bus.subscribe();

    let execution = service
        .run_execution(
            claimed.execution.id.clone(),
            &BurstLogExecutor { count: 55 },
        )
        .await
        .expect("execution runs");

    assert_eq!(execution.status, ExecutionStatus::Completed);
    let log_events = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        let mut events = Vec::new();
        while events.len() < 2 {
            let event = rx.recv().await.expect("execution log event emits");
            if event.event_type == "execution.log" {
                events.push(event);
            }
        }
        events
    })
    .await
    .expect("batched execution.log events received");

    assert_eq!(log_events.len(), 2);
    let mut total_logs = 0;
    let mut saw_multi_log_event = false;
    for event in log_events {
        assert_eq!(event.entity_id, claimed.execution.id);
        match event.context {
            EventContext::ExecutionLog { task_id, log, logs } => {
                assert_eq!(task_id, task.id);
                assert!(!log.is_null());
                let logs = logs.expect("batched logs included");
                saw_multi_log_event |= logs.len() > 1;
                total_logs += logs.len();
            }
            other => panic!("unexpected event context: {other:?}"),
        }
    }
    assert_eq!(total_logs, 55);
    assert!(saw_multi_log_event);
}

#[tokio::test]
async fn launch_execution_creates_interactive_execution_and_workspace() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    crate::test_support::clear_project_execution_role_defaults(&db, &project_id).await;
    let task = service
        .create_task(
            project_id,
            "Launch interactive",
            Some("printf launch-ok".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");

    let launched = service
        .launch_execution(task.id.clone(), agent_id.clone(), None, None)
        .await
        .expect("interactive launch succeeds");

    assert_eq!(launched.task.status, "in_progress".to_owned());
    assert_eq!(launched.execution.role, "interactive".to_owned());
    assert_eq!(launched.execution.status, ExecutionStatus::Running);
    assert_eq!(
        launched.execution.agent_id.as_deref(),
        Some(agent_id.as_str())
    );
    assert_eq!(launched.workspace.task_id, task.id);
}

#[tokio::test]
async fn dispatch_initial_role_execution_creates_execution_and_spawns() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;

    let execution = service
        .dispatch_initial_role_execution(
            &task.id,
            &agent_id,
            crate::workflow::default_roles::CODER,
            "implement the task".to_owned(),
        )
        .await
        .expect("initial role dispatch succeeds");

    assert_eq!(execution.role, crate::workflow::default_roles::CODER);
    assert_eq!(execution.status, ExecutionStatus::Running);
    assert_eq!(execution.agent_id.as_deref(), Some(agent_id.as_str()));
    assert_eq!(execution.summary.as_deref(), Some("implement the task"));

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let current = ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .expect("execution loads")
                .expect("execution exists");
            if current.status == ExecutionStatus::Completed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("spawned execution completes");
}

#[tokio::test]
async fn planner_completion_marks_task_awaiting_plan_review_until_approved() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        &repo_id,
        crate::workflow::default_states::PLANNING.to_owned(),
    )
    .await;

    let execution = service
        .dispatch_initial_role_execution(
            &task.id,
            &agent_id,
            crate::workflow::default_roles::PLANNER,
            "plan the task".to_owned(),
        )
        .await
        .expect("planner dispatch succeeds");

    let completed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let current = ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .expect("execution loads")
                .expect("execution exists");
            if current.status == ExecutionStatus::Completed {
                break current;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("planner execution completes");
    let task = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let task = TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .expect("task loads")
                .expect("task exists");
            let metadata = task.metadata().expect("metadata parses");
            if metadata
                .extra
                .get("awaiting_human_reason")
                .and_then(Value::as_str)
                == Some("plan_review")
            {
                break task;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("planner completion marks task awaiting plan review");
    let metadata = task.metadata().expect("metadata parses");
    assert_eq!(
        metadata
            .extra
            .get("awaiting_human_reason")
            .and_then(Value::as_str),
        Some("plan_review")
    );
    assert_eq!(
        metadata
            .extra
            .get("planning_execution_id")
            .and_then(Value::as_str),
        Some(completed.id.as_str())
    );
    assert!(service
        .is_awaiting_human(task.id.clone())
        .await
        .expect("awaiting human resolves"));

    let workspace = WorkspaceRepo::get_by_id(
        &*db,
        completed
            .workspace_id
            .as_deref()
            .expect("execution has workspace"),
    )
    .await
    .expect("workspace loads")
    .expect("workspace exists");
    let plan_path = std::path::Path::new(&workspace.worktree_path)
        .parent()
        .expect("worktree has workspace parent")
        .join("plan.md");
    std::fs::write(plan_path, "- [x] verify plan\n").expect("plan writes");

    let approved = service
        .transition(
            task.id.clone(),
            crate::workflow::default_states::IN_PROGRESS.to_owned(),
            task.version,
        )
        .await
        .expect("planning approval succeeds");
    let metadata = approved.task.metadata().expect("metadata parses");
    assert!(metadata.extra.get("awaiting_human").is_none());
    assert!(metadata.extra.get("awaiting_human_reason").is_none());
}

#[tokio::test]
async fn before_enter_runs_required_before_work_hook_before_role_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "printf required-ok > required-hook.out; exit 0",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(agent_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    let transitioned = service
        .transition(task.id.clone(), "in_progress".to_owned(), task.version)
        .await
        .expect("required hook passes and transition succeeds");

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].role,
        crate::workflow::default_roles::CODER
    );
    assert_eq!(
        executions.items[0].agent_id.as_deref(),
        Some(agent_id.as_str())
    );
    assert_eq!(transitioned.task.entry_barrier_json, None);

    let workspace =
        WorkspaceRepo::get_by_id(&*db, executions.items[0].workspace_id.as_deref().unwrap())
            .await
            .expect("workspace loads")
            .expect("workspace exists");
    let marker = std::fs::read_to_string(
        std::path::Path::new(&workspace.worktree_path).join("required-hook.out"),
    )
    .expect("required hook marker exists");
    assert_eq!(marker, "required-ok");
}

#[tokio::test]
async fn before_enter_blocks_when_required_before_work_hook_fails() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "echo preflight-out; echo preflight-err >&2; exit 9",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(agent_id),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    let result = service
        .transition(task.id.clone(), "in_progress".to_owned(), task.version)
        .await
        .expect("required hook failure records a blocked entry");
    assert_eq!(result.task.status, "in_progress");
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert!(
        executions.items.is_empty(),
        "no execution should be created"
    );

    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(blocked.status, "in_progress");
    let barrier: serde_json::Value = serde_json::from_str(
        blocked
            .entry_barrier_json
            .as_deref()
            .expect("entry barrier remains blocked"),
    )
    .expect("entry barrier parses");
    assert_eq!(barrier["state"], "in_progress");
    assert_eq!(barrier["status"], "blocked");
    let annotation: serde_json::Value = serde_json::from_str(
        blocked
            .error_annotation
            .as_deref()
            .expect("blocking annotation is recorded"),
    )
    .expect("annotation parses");
    assert_eq!(annotation["type"], "before_work_hook_failed");
    assert_eq!(annotation["artifact"]["kind"], "hook");
    assert_eq!(annotation["hook"]["exit_code"], 9);
    assert_eq!(annotation["hook"]["stdout"], "preflight-out\n");
    assert_eq!(annotation["hook"]["stderr"], "preflight-err\n");
    let recovery_actions = annotation["recovery_actions"]
        .as_array()
        .expect("recovery actions array");
    assert!(recovery_actions.iter().any(|value| value == "retry_hook"));
    assert!(recovery_actions
        .iter()
        .any(|value| value == "update_workspace_and_retry_hook"));
    assert!(recovery_actions
        .iter()
        .any(|value| value == "skip_hook_once"));
    assert!(recovery_actions.iter().any(|value| value == "cancel_task"));
    let log_path = annotation["hook"]["log_path"]
        .as_str()
        .expect("hook log path recorded");
    assert!(
        std::path::Path::new(log_path).exists(),
        "hook log path should exist: {log_path}"
    );
}

#[tokio::test]
async fn retry_hook_reruns_blocked_before_enter_and_dispatches_when_it_passes() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let failing_settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "echo preflight-out; echo preflight-err >&2; exit 9",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(failing_settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(agent_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    service
        .transition(task.id.clone(), "in_progress".to_owned(), task.version)
        .await
        .expect("required hook failure records a blocked entry");
    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(blocked.entry_barrier_json.is_some());
    assert!(blocked.error_annotation.is_some());

    let passing_settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "printf retry-ok > retry-hook.out; exit 0",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(passing_settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::RetryHook,
            None,
            None,
        )
        .await
        .expect("retry hook recovers");

    assert_eq!(recovered.status, "in_progress");
    assert_eq!(recovered.entry_barrier_json, None);
    assert_eq!(recovered.error_annotation, None);
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].agent_id.as_deref(),
        Some(agent_id.as_str())
    );
    let workspace =
        WorkspaceRepo::get_by_id(&*db, executions.items[0].workspace_id.as_deref().unwrap())
            .await
            .expect("workspace loads")
            .expect("workspace exists");
    let marker = std::fs::read_to_string(
        std::path::Path::new(&workspace.worktree_path).join("retry-hook.out"),
    )
    .expect("retry hook marker exists");
    assert_eq!(marker, "retry-ok");
}

#[tokio::test]
async fn update_workspace_and_retry_hook_rebases_before_retrying_blocked_hook() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, repo_dir) = seed_project_repo(&db).await;
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "test -f hook-marker.txt",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(agent_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    service
        .transition(task.id.clone(), "in_progress".to_owned(), task.version)
        .await
        .expect("required hook failure records a blocked entry");
    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(blocked.entry_barrier_json.is_some());
    let workspace = WorkspaceRepo::get_by_task_id(&*db, &task.id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    assert!(!std::path::Path::new(&workspace.worktree_path)
        .join("hook-marker.txt")
        .exists());

    std::fs::write(repo_dir.path().join("hook-marker.txt"), "updated\n").expect("marker writes");
    run_git(repo_dir.path(), &["add", "-A"]);
    run_git(repo_dir.path(), &["commit", "-m", "add hook marker"]);

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::UpdateWorkspaceAndRetryHook,
            None,
            None,
        )
        .await
        .expect("update workspace and retry hook recovers");

    assert_eq!(recovered.status, "in_progress");
    assert_eq!(recovered.entry_barrier_json, None);
    assert_eq!(recovered.error_annotation, None);
    assert!(std::path::Path::new(&workspace.worktree_path)
        .join("hook-marker.txt")
        .exists());
}

#[tokio::test]
async fn skip_hook_once_bypasses_only_one_dispatch_attempt() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "test -f hook-marker.txt",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let planner_id = seed_agent(&db).await;
    let coder_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::PLANNER,
                Some(planner_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("planner role assignment succeeds");
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(coder_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    service
        .transition(task.id.clone(), "planning".to_owned(), task.version)
        .await
        .expect("blocking hook failure records a blocked entry");
    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(blocked.entry_barrier_json.is_some());

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::SkipHookOnce,
            None,
            None,
        )
        .await
        .expect("skip hook once recovers");
    assert_eq!(recovered.status, "planning");
    assert_eq!(recovered.entry_barrier_json, None);
    assert_eq!(recovered.error_annotation, None);

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].agent_id.as_deref(),
        Some(planner_id.as_str())
    );

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let transitioned = service
        .transition(
            task.id.clone(),
            "planning".to_owned(),
            (current.version, None, true),
        )
        .await
        .expect("second transition runs hook normally");
    assert_eq!(transitioned.task.status, "planning");
    let blocked_again = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(blocked_again.entry_barrier_json.is_some());
    let annotation: serde_json::Value = serde_json::from_str(
        blocked_again
            .error_annotation
            .as_deref()
            .expect("blocking annotation is recorded"),
    )
    .expect("annotation parses");
    assert_eq!(annotation["type"], "before_work_hook_failed");
}

#[tokio::test]
async fn dispatch_initial_role_execution_runs_reviewer_when_agent_is_busy_on_same_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "review".to_owned()).await;

    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::REVIEWER,
            Some(agent_id.clone()),
            None,
        ),
    )
    .await
    .expect("reviewer assignment created");

    let execution = service
        .dispatch_initial_role_execution(
            &task.id,
            &agent_id,
            crate::workflow::default_roles::REVIEWER,
            "review the task".to_owned(),
        )
        .await
        .expect("reviewer dispatch succeeds");

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let current = ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .expect("execution loads")
                .expect("execution exists");
            if current.status == ExecutionStatus::Completed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("reviewer execution completes");
}

#[tokio::test]
async fn failed_reviewer_execution_marks_running_review_failed() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "review".to_owned()).await;
    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: Some(db::StopReason::ExecutorFailed),
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: Some(db::ResumePolicy::Manual),
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: Some("reviewer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("reviewer quit before verdict".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("claude-code exited with status exit status: 1".to_owned()),
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("failed reviewer execution creates");
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running review creates");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("reviewer failure cascade succeeds");

    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Failed);
    assert!(reviews[0].finished_at.is_some());
    let details: serde_json::Value =
        serde_json::from_str(&reviews[0].step_results_json).expect("details parse");
    assert_eq!(details["auditor"]["verdict"], "fail");
    assert_eq!(
        details["execution"]["error"],
        "claude-code exited with status exit status: 1"
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "review");
    let current_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(
        current_execution.resume_policy,
        Some(db::ResumePolicy::Auto),
        "failed reviewer executions should be scheduled for automatic retry"
    );
}

#[tokio::test]
async fn human_required_review_can_be_rejected_by_the_bound_project_agent() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::REVIEW)
        .and_then(|state| state.gate_config.as_mut())
        .expect("review gate config")
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project workflow updates");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "review".to_owned()).await;
    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("reviewer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("Looks good.\n===REVIEW: PASS===".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("completed reviewer execution creates");
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running review creates");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("reviewer completion cascade succeeds");
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("reviewer completion cascade is idempotent");

    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::AwaitingHuman);
    assert!(reviews[0].finished_at.is_none());
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "review");
    assert!(service
        .is_awaiting_human(task.id.clone())
        .await
        .expect("awaiting human resolves"));
    let comments = TaskCommentRepo::list_comments(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await
    .expect("comments list");
    let reviewer_comments: Vec<_> = comments
        .items
        .iter()
        .filter(|comment| comment.content == "Looks good.")
        .collect();
    assert_eq!(
        reviewer_comments.len(),
        1,
        "reviewer completion should only publish one comment"
    );
    let comment = comments
        .items
        .iter()
        .find(|comment| comment.content.contains("Looks good."))
        .expect("reviewer comment exists");
    assert_eq!(comment.content, "Looks good.");
    assert_eq!(comment.author_type, CommentAuthorType::Agent);
    assert_eq!(comment.author_id.as_deref(), Some(agent_id.as_str()));

    // Model the completed implementation attempt that preceded review and
    // exhaust the optional automatic review-fix retry. This keeps the test
    // focused on Project-Agent authority instead of launching a follow-up
    // executor against this deliberately workspace-free fixture.
    sqlx::query(
        "INSERT INTO execution (
             id, task_id, agent_id, role, status, created_at, updated_at
         ) VALUES (?, ?, ?, 'coder', 'completed', ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&task.id)
    .bind(&agent_id)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .expect("completed coder attempt creates");
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(r#"{"retry_budgets":{"review":0}}"#)
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("review retry budget updates");

    let agent = AgentRepo::get_by_id(&*db, &agent_id)
        .await
        .expect("Project Agent reads")
        .expect("Project Agent exists");
    let binding = ProjectAgentBindingRepo::get_active_project_binding(&*db, &project_id)
        .await
        .expect("Project Agent binding reads")
        .expect("Project Agent binding exists");
    ProjectAgentBindingRepo::replace_project_binding(
        &*db,
        ReplaceProjectAgentBinding {
            project_id: project_id.clone(),
            expected_version: binding.version,
            replacement: CreateProjectAgentBinding {
                id: new_uuid_v4(),
                project_id: project_id.clone(),
                identity_id: Some(agent_id.clone()),
                profile_id: Some(agent.profile_id),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: r#"{"permissions":["propose_task"]}"#.to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 1,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
            replacement_reason: Some("test human-required review".to_owned()),
        },
    )
    .await
    .expect("Project Agent binding updates");
    crate::test_support::clear_project_execution_role_defaults(&db, &project_id).await;

    let unbound = service
        .perform_project_agent_review(
            &project_id,
            task.id.clone(),
            false,
            Some("not authorized".to_owned()),
            current.version,
            "another-agent",
        )
        .await;
    assert!(matches!(
        unbound,
        Err(ServiceError::InvalidOperation { .. })
    ));

    let reviewed = service
        .perform_project_agent_review(
            &project_id,
            task.id.clone(),
            false,
            Some("Please address the review feedback".to_owned()),
            current.version,
            &agent_id,
        )
        .await
        .expect("bound Project Agent rejects human-required review");
    assert_eq!(reviewed.action, api_types::TaskAction::RequestChanges);
    assert_eq!(
        reviewed.task.status,
        crate::workflow::default_states::IN_PROGRESS
    );
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews reload");
    assert_eq!(reviews[0].status, ReviewStatus::Failed);
}

#[tokio::test]
async fn follow_up_execution_creates_interactive_child() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "claude_code", "{}").await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let message = "Please continue with the remaining edge cases".to_owned();
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("parent execution".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"claude_code","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id.clone(), message.clone(), None, None)
        .await
        .expect("follow-up succeeds");

    assert_eq!(
        result.execution.parent_execution_id.as_deref(),
        Some(parent_execution.id.as_str())
    );
    assert_eq!(result.execution.summary.as_deref(), Some(message.as_str()));
    assert_eq!(result.execution.role, "interactive".to_owned());
    let snapshot: serde_json::Value = serde_json::from_str(
        result
            .execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("snapshot exists"),
    )
    .expect("snapshot is valid json");
    assert_eq!(snapshot["config"]["resume_session_id"], "test-session");
}

#[tokio::test]
async fn follow_up_rejects_a_running_repository_role_without_mutating_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "claude_code", "{}").await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("failed-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("failed".to_owned()),
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"claude_code","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("failed parent creates");
    let running = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
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
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"claude_code","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running execution creates");

    let result = service
        .follow_up_execution(parent.id, "continue".to_owned(), None, None)
        .await;

    assert!(matches!(
        result,
        Err(ServiceError::InvalidOperation { message })
            if message.contains("repository execution already running")
                && message.contains(&running.id)
    ));
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert_eq!(executions.items.len(), 2);
    let unchanged = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(unchanged.version, task.version);
    assert!(unchanged.error_annotation.is_none());
}

#[tokio::test]
async fn follow_up_execution_codex_resumes_with_message_only_fallback() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let message = "Please continue with the remaining edge cases".to_owned();
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("codex-thread".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("parent execution".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"codex","config":{"resume_fallback_prompt":"do not send this full prompt"}}"#
                    .to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id.clone(), message.clone(), None, None)
        .await
        .expect("follow-up succeeds");

    assert_eq!(result.execution.summary.as_deref(), Some(message.as_str()));
    let snapshot: serde_json::Value = serde_json::from_str(
        result
            .execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("snapshot exists"),
    )
    .expect("snapshot is valid json");
    assert_eq!(snapshot["config"]["resume_thread_id"], "codex-thread");
    assert_eq!(snapshot["config"]["resume_thread_in_place"], true);
    assert!(snapshot["config"].get("resume_fallback_prompt").is_none());
}

#[tokio::test]
async fn follow_up_execution_rejects_running_parent() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
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
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("running parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id, "continue".to_owned(), None, None)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn follow_up_on_cancelled_execution_with_session_succeeds() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("cancelled parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id, "continue".to_owned(), None, None)
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn follow_up_on_cancelled_execution_without_session_returns_error() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("cancelled parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id, "continue".to_owned(), None, None)
        .await;

    assert!(matches!(
        result,
        Err(ServiceError::InvalidOperation { message })
            if message.contains("no resumable session")
    ));
}

#[tokio::test]
async fn follow_up_execution_rejects_missing_session_id() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("completed parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id, "continue".to_owned(), None, None)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn follow_up_execution_rejects_terminal_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "done".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("completed parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id, "continue".to_owned(), None, None)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn follow_up_execution_on_blocked_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: Some(Some(
                r#"{"reason":"test block","created_at":"2026-04-28T00:00:00Z","kind":"ci_failed"}"#
                    .to_owned(),
            )),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("set blocked_json");
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("completed parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(parent_execution.id, "continue".to_owned(), None, None)
        .await
        .expect("follow-up succeeds");

    assert_eq!(result.execution.role, "interactive".to_owned());
    assert_eq!(result.task.status, "in_progress".to_owned());
}

#[tokio::test]
async fn follow_up_execution_rejects_executor_mismatch() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let shell_agent_id = seed_agent(&db).await;
    let codex_agent_id = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(shell_agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("completed parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .follow_up_execution(
            parent_execution.id,
            "continue".to_owned(),
            Some(codex_agent_id),
            None,
        )
        .await;

    assert!(matches!(
        result,
        Err(ServiceError::InvalidOperation { message })
            if message.contains("same executor type")
    ));
}

#[tokio::test]
async fn re_execute_cancelled_execution_dispatches_fresh() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("cancelled parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .re_execute_execution(parent_execution.id)
        .await
        .expect("re-execute succeeds");

    assert_eq!(result.execution.role, "coder".to_owned());
    assert_eq!(result.execution.status, ExecutionStatus::Running);
    assert_eq!(result.execution.parent_execution_id, None);
    assert_eq!(result.execution.agent_session_id, None);
}

#[tokio::test]
async fn re_execute_rejects_running_parent() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("running parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service.re_execute_execution(parent_execution.id).await;

    assert!(matches!(
        result,
        Err(ServiceError::InvalidOperation { message })
            if message.contains("re-execute requires")
    ));
}

#[tokio::test]
async fn re_execute_rejects_concurrent_running_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("cancelled parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("parent execution creates");
    ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("running sibling".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running execution creates");

    let result = service.re_execute_execution(parent_execution.id).await;

    assert!(matches!(
        result,
        Err(ServiceError::InvalidOperation { message })
            if message.contains("already running")
    ));
}

#[tokio::test]
async fn interactive_execution_completion_does_not_trigger_review_cascade() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    crate::test_support::clear_project_execution_role_defaults(&db, &project_id).await;
    let task = service
        .create_task(
            project_id,
            "Interactive no cascade",
            Some("printf no-cascade".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let launched = service
        .launch_execution(task.id.clone(), agent_id, None, None)
        .await
        .expect("launch succeeds");

    let registry = Arc::new(cli_adapters::default_registry());
    let executor = executors::AdapterExecutor::new(registry);
    let execution = service
        .run_execution(launched.execution.id.clone(), &executor)
        .await
        .expect("interactive execution runs");
    assert_eq!(execution.status, ExecutionStatus::Completed);

    service
        .maybe_cascade_executor_completion(&launched.execution.id)
        .await
        .expect("cascade check succeeds");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "in_progress".to_owned());
}

#[tokio::test]
async fn recover_reexecute_without_blocked_execution_dispatches_current_state_role() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_task_executor(Arc::new(PendingExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::CODER,
            Some(agent_id.clone()),
            None,
        ),
    )
    .await
    .expect("coder assignment created");
    let annotation = json!({
        "type": "recovery_required",
        "blocking_reason": "crash_recovery",
        "blocked_by": "system:crash_recovery",
        "blocked_at": now_rfc3339(),
        "message": "Recovered after server restart",
        "recovery_actions": ["reexecute", "reset_to_initial", "cancel_task"],
    })
    .to_string();
    TaskRepo::update(
        &*db,
        db::UpdateTask {
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
    .expect("task recovery annotation saved");

    let recovered = service
        .recover_task(
            task.id.clone(),
            api_types::RecoveryAction::Reexecute,
            Some("test".to_owned()),
            Some("resume current work".to_owned()),
        )
        .await
        .expect("reexecute recovers");

    assert_eq!(recovered.status, "in_progress");
    assert_eq!(recovered.error_annotation, None);
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].role,
        crate::workflow::default_roles::CODER
    );
    assert_eq!(executions.items[0].status, ExecutionStatus::Running);
    assert!(executions.items[0]
        .summary
        .as_deref()
        .unwrap_or_default()
        .contains("resume current work"));
}

struct PendingExecutor;

#[async_trait::async_trait]
impl TaskExecutor for PendingExecutor {
    async fn execute(
        &self,
        _ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        std::future::pending::<std::result::Result<ExecutionResult, ExecutorError>>().await
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

#[tokio::test]
async fn executor_completion_guard_rejection_follows_up_before_blocking() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service =
        TaskService::new(Arc::clone(&db), event_bus).with_task_executor(Arc::new(PendingExecutor));
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let workspace = seed_workspace_with_plan(&db, &task, "- [ ] finish implementation\n").await;
    let execution =
        seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("guard rejection dispatches follow-up");

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    let resumed = executions
        .items
        .iter()
        .find(|candidate| candidate.status == ExecutionStatus::Running)
        .expect("lease-backed follow-up exists");
    assert_eq!(
        resumed.parent_execution_id.as_deref(),
        Some(execution.id.as_str())
    );
    let execution = ExecutionRepo::get_by_id(&*db, &resumed.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution.status, ExecutionStatus::Running);
    assert!(execution.summary.as_deref().is_some_and(|summary| {
        summary.contains("Workflow guard failed: require_plan_checklist_complete")
    }));
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = task.metadata().expect("metadata parses");
    assert_eq!(metadata.extra["workflow_guard_retry_count"], json!(1));
    assert!(task.blocked_json.is_none());
}

#[tokio::test]
async fn subtask_sequence_guard_rejection_runs_orchestrator_instead_of_coder_follow_up() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service =
        TaskService::new(Arc::clone(&db), event_bus).with_task_executor(Arc::new(PendingExecutor));
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let _subtask = seed_subtask_with_status(&db, &task, "child", "todo".to_owned(), 0).await;
    let workspace = seed_workspace_with_plan(&db, &task, "- [x] parent work\n").await;
    let execution =
        seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("handoff succeeds");

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    let resumed = executions
        .items
        .iter()
        .find(|candidate| candidate.status == ExecutionStatus::Running)
        .expect("lease-backed subtask follow-up exists");
    assert_eq!(
        resumed.parent_execution_id.as_deref(),
        Some(execution.id.as_str())
    );
    let execution = ExecutionRepo::get_by_id(&*db, &resumed.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution.status, ExecutionStatus::Running);
    assert!(
        execution
            .summary
            .as_deref()
            .is_some_and(|s| s.contains("Subtask 1 of 1")),
        "execution summary should contain subtask prompt, got: {:?}",
        execution.summary
    );

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = task.metadata().expect("metadata parses");
    assert!(metadata.extra.get("workflow_guard_retry_count").is_none());
    assert!(task.blocked_json.is_none());
}

#[tokio::test]
async fn executor_completion_guard_rejection_blocks_when_retry_budget_exhausted() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let task = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: Some(Some(r#"{"retry_budgets":{"execution":0}}"#.to_owned())),
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task config updates");
    let workspace = seed_workspace_with_plan(&db, &task, "- [ ] finish implementation\n").await;
    let execution =
        seed_completed_coder_execution(&db, &task, &agent_id, Some(&workspace.id)).await;

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("guard rejection blocks after exhausted budget");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let blocked = task.blocked_json.expect("task blocked");
    assert!(blocked.contains("workflow_guard_rejected"));
    let annotation = task.error_annotation.expect("annotation recorded");
    assert!(annotation.contains("require_plan_checklist_complete"));
}

#[tokio::test]
async fn executor_completion_comment_uses_execution_agent() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("implemented the change".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("cascade check succeeds");

    let comments = TaskCommentRepo::list_comments(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await
    .expect("comments list");
    let comment = comments
        .items
        .iter()
        .find(|comment| comment.content.contains("Agent completed execution"))
        .expect("executor completion comment exists");
    assert_eq!(comment.author_type, CommentAuthorType::Agent);
    assert_eq!(comment.author_id.as_deref(), Some(agent_id.as_str()));
    assert_eq!(comment.author_name, "shell");
}

async fn seed_workspace_with_plan(db: &SqliteDb, task: &Task, plan: &str) -> Workspace {
    let workspace_dir = std::env::temp_dir()
        .join(format!("forge-guard-plan-{}", new_uuid_v4()))
        .join(&task.id);
    let worktree_path = workspace_dir.join("worktree");
    std::fs::create_dir_all(&worktree_path).expect("worktree creates");
    std::fs::write(workspace_dir.join("plan.md"), plan).expect("plan writes");
    git::init(&worktree_path).await.expect("git init succeeds");
    std::fs::write(worktree_path.join("README.md"), "# Test\n").expect("readme writes");
    git::commit_all(&worktree_path, "initial commit")
        .await
        .expect("initial commit creates");
    WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id: task.repo_id.clone().unwrap(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("workspace creates")
}

async fn seed_completed_coder_execution(
    db: &SqliteDb,
    task: &Task,
    agent_id: &str,
    workspace_id: Option<&str>,
) -> Execution {
    let now = now_rfc3339();
    ExecutionRepo::create(
        db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.to_owned()),
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("implemented the change".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: workspace_id.map(str::to_owned),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates")
}

#[tokio::test]
async fn claim_task_records_execution_permission_policy_override_in_snapshot() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id =
        seed_shell_agent_with_config(&db, r#"{"command":"echo","args":["profile-default"]}"#).await;
    let task = service
        .create_task(
            project_id,
            "Snapshot override",
            Some("unused".to_owned()),
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
            Assignee::Agent(agent_id),
            Some(ExecutionOverrides {
                model_id: None,
                reasoning_effort: None,
                permission_policy: Some("auto".to_owned()),
            }),
        )
        .await
        .expect("task claims");
    let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    let snapshot: Value = serde_json::from_str(
        execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("snapshot recorded"),
    )
    .expect("snapshot parses");

    assert_eq!(snapshot["config"]["permission_policy"], "auto");
    let execution_keys = snapshot["overrides_applied"]["execution"]
        .as_array()
        .expect("execution override keys are recorded");
    assert!(execution_keys
        .iter()
        .any(|key| key.as_str() == Some("permission_policy")));
}

#[tokio::test]
async fn claim_task_records_codex_overrides_in_normalized_snapshot() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(
        &db,
        "codex",
        r#"{"model":"agent-model","model_reasoning_effort":"medium","sandbox":"danger-full-access","permission_policy":"supervised"}"#,
    )
    .await;
    let task = service
        .create_task(
            project_id,
            "Snapshot codex overrides",
            Some("unused".to_owned()),
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
            Assignee::Agent(agent_id),
            Some(ExecutionOverrides {
                model_id: Some("gpt-5-codex".to_owned()),
                reasoning_effort: Some("high".to_owned()),
                permission_policy: Some("auto".to_owned()),
            }),
        )
        .await
        .expect("task claims");
    let snapshot: Value = serde_json::from_str(
        claimed
            .execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("snapshot recorded"),
    )
    .expect("snapshot parses");

    assert_eq!(snapshot["executor_type"], "codex");
    assert_eq!(snapshot["config"]["model"], "gpt-5-codex");
    assert_eq!(snapshot["config"]["model_reasoning_effort"], "high");
    assert_eq!(snapshot["config"]["permission_policy"], "auto");
    assert!(snapshot["config"].get("effort").is_none());

    let agent_keys = snapshot["overrides_applied"]["agent"]
        .as_array()
        .expect("agent keys are recorded");
    assert!(agent_keys.iter().any(|key| key.as_str() == Some("model")));
    assert!(agent_keys
        .iter()
        .any(|key| key.as_str() == Some("model_reasoning_effort")));
    assert!(agent_keys
        .iter()
        .any(|key| key.as_str() == Some("permission_policy")));

    let execution_keys = snapshot["overrides_applied"]["execution"]
        .as_array()
        .expect("execution keys are recorded");
    assert!(execution_keys
        .iter()
        .any(|key| key.as_str() == Some("model")));
    assert!(execution_keys
        .iter()
        .any(|key| key.as_str() == Some("model_reasoning_effort")));
    assert!(execution_keys
        .iter()
        .any(|key| key.as_str() == Some("permission_policy")));
    assert!(!execution_keys
        .iter()
        .any(|key| key.as_str() == Some("effort")));
}
