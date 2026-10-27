//! Interactive follow-up executions and reviewer session resumption.
use super::*;

#[tokio::test]
async fn follow_up_execution_creates_interactive_child() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "claude_code", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
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
async fn open_interactive_recovery_starts_the_created_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let parent = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        Some("parent-session"),
        "2026-01-01T00:00:00Z",
    )
    .await;
    let annotation = api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::ManualStop,
        blocking_reason: "user_paused".to_owned(),
        blocked_by: Some("user:api".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: Some(parent.id.clone()),
        artifact: None,
        message: Some("paused".to_owned()),
        hook: None,
    };
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
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation"),
            )),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("manual-stop annotation saves");

    service
        .test_launch_side_session(
            task.id.clone(),
            Some("continue interactively".to_owned()),
            None,
        )
        .await
        .expect("open-interactive recovery succeeds");

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
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
            if executions.items.iter().any(|execution| {
                execution.role == crate::workflow::default_roles::INTERACTIVE
                    && execution.status == ExecutionStatus::Completed
            }) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("recovery execution starts and completes");
}

#[tokio::test]
async fn follow_up_execution_preserves_workflow_role() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "claude_code", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_id.clone()), None),
            false,
            false,
        )
        .await
        .expect("workflow role assignment creates");
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
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
            agent_session_id: Some("workflow-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("coder execution".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("failed".to_owned()),
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"claude_code","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("workflow parent execution creates");

    let result = service
        .follow_up_execution(
            parent_execution.id.clone(),
            "resume coder work".to_owned(),
            None,
            None,
        )
        .await
        .expect("workflow follow-up succeeds");

    assert_eq!(
        result.execution.parent_execution_id.as_deref(),
        Some(parent_execution.id.as_str())
    );
    assert_eq!(result.execution.role, "coder");
    assert_eq!(
        result.execution.agent_session_id.as_deref(),
        Some("workflow-session")
    );
}

#[tokio::test]
async fn reviewer_follow_up_binds_review_candidate_parent_while_resuming_reviewer_session() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace root creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "claude_code", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
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
    .expect("reviewer assignment creates");
    let workspace_id = seed_workspace_for_task(&db, &task, &repo_id, workspace_root.path()).await;
    let now = now_rfc3339();
    let candidate = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        None,
        &now,
    )
    .await;
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    let reviewer = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: Some(candidate.id.clone()),
            agent_session_id: Some("review-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("reviewer execution".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("review failed".to_owned()),
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"claude_code","config":{}}"#.to_owned(),
            ),
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("reviewer execution creates");

    let result = service
        .follow_up_execution(reviewer.id, "resume reviewer".to_owned(), None, None)
        .await
        .expect("reviewer follow-up succeeds");

    assert_eq!(
        result.execution.role,
        crate::workflow::default_roles::REVIEWER
    );
    assert_eq!(
        result.execution.parent_execution_id.as_deref(),
        Some(candidate.id.as_str())
    );
    assert_eq!(
        result
            .execution
            .executor_config_snapshot_json
            .as_deref()
            .and_then(|snapshot| serde_json::from_str::<serde_json::Value>(snapshot).ok())
            .and_then(|snapshot| snapshot["config"]["resume_session_id"]
                .as_str()
                .map(str::to_owned)),
        Some("review-session".to_owned())
    );
    assert_eq!(review.execution_id, candidate.id);
}

#[tokio::test]
async fn reviewer_resume_repairs_legacy_reviewer_parent_to_review_candidate() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace root creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "claude_code", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
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
    .expect("reviewer assignment creates");
    let workspace_id = seed_workspace_for_task(&db, &task, &repo_id, workspace_root.path()).await;
    let now = now_rfc3339();
    let candidate = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        None,
        &now,
    )
    .await;
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    let legacy_reviewer = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::REVIEWER,
        ExecutionStatus::Failed,
        Some("review-session"),
        &now,
    )
    .await;

    let resumed = service
        .create_running_execution(
            db::CreateExecution {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                agent_id: Some(agent_id),
                role: crate::workflow::default_roles::REVIEWER.to_owned(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: Some(legacy_reviewer.id),
                agent_session_id: Some("review-session".to_owned()),
                agent_message_id: None,
                last_activity_at: None,
                summary: Some("resume reviewer".to_owned()),
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: Some(
                    r#"{"executor_type":"claude_code","config":{}}"#.to_owned(),
                ),
                workspace_id: Some(workspace_id),
                created_at: now.clone(),
                updated_at: now,
            },
            false,
        )
        .await
        .expect("reviewer resume creates");

    assert_eq!(
        resumed.parent_execution_id.as_deref(),
        Some(candidate.id.as_str())
    );
    assert_eq!(resumed.agent_session_id.as_deref(), Some("review-session"));
}

#[tokio::test]
async fn follow_up_rejects_a_running_repository_role_without_mutating_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace root creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "claude_code", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let workspace_id = seed_workspace_for_task(&db, &task, &repo_id, workspace_root.path()).await;
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
            workspace_id: Some(workspace_id.clone()),
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
            workspace_id: Some(workspace_id),
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
        &result,
        Err(ServiceError::ExecutionAlreadyRunning { scope, execution_id })
            if scope == "repository" && execution_id == &running.id
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
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
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
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
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
                workflow_execution_snapshot(&db, &project_id).await,
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
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
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
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
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
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
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
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "done".to_owned()).await;
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
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
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
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let shell_agent_id = seed_agent(&db).await;
    let codex_agent_id = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
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
