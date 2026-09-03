use super::super::*;

#[test]
fn test_dependency_gate_surfaces_as_409_variant() {
    let error: ServiceError = DbError::DependencyGate.into();
    assert!(matches!(error, ServiceError::DependencyGate));
}

#[tokio::test]
async fn create_task_rejects_unknown_task_type_before_persistence() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;

    let error = service
        .create_task(
            project_id,
            "Invalid type",
            None,
            None,
            None,
            Some("feature".to_owned()),
            None,
            None,
            None,
        )
        .await
        .expect_err("unknown task types must be rejected by the service boundary");

    assert!(error.to_string().contains("task_type must be"));
}

#[tokio::test]
async fn transition_allows_user_move_for_root_managed_subtask() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, &repo_id, "in_progress".to_owned()).await;
    let subtask = seed_subtask_with_status(&db, &root, "child", "todo".to_owned(), 0).await;
    seed_ordered_sequence_started(&db, &root).await;

    let result = service
        .transition(
            subtask.id.clone(),
            "in_progress".to_owned(),
            (subtask.version, None),
        )
        .await
        .expect("user can manage subtask status");

    assert_eq!(
        result.task.parent_task_id.as_deref(),
        Some(root.id.as_str())
    );
    let current = TaskRepo::get_by_id(&*db, &subtask.id, false)
        .await
        .expect("subtask loads")
        .expect("subtask exists");
    assert_eq!(current.status, "in_progress");
}

#[tokio::test]
async fn transition_rejects_invalid_move_and_cancel_is_idempotent() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = service
        .create_task(project_id, "Done", None, None, None, None, None, None, None)
        .await
        .expect("task creates");

    let result = service
        .transition(task.id.clone(), "done".to_owned(), task.version)
        .await;
    assert!(matches!(
        result,
        Err(ServiceError::Db(DbError::InvalidTransition))
    ));

    let cancelled = service
        .cancel_task(task.id.clone())
        .await
        .expect("task cancels");
    assert_eq!(cancelled.status, "cancelled".to_owned());
    let cancelled_again = service
        .cancel_task(task.id)
        .await
        .expect("cancel is idempotent");
    assert_eq!(cancelled_again.status, "cancelled".to_owned());
}

#[tokio::test]
async fn transition_from_planning_requires_plan_checklist_but_allows_unchecked_work() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    let planning = workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::PLANNING)
        .expect("planning state exists");
    planning
        .gate_config
        .as_mut()
        .expect("planning gate config exists")
        .requires_user_approval = Some(true);
    update_project_workflow(&db, &project_id, &workflow).await;

    let task = seed_task_with_status(
        &db,
        &project_id,
        &repo_id,
        crate::workflow::default_states::PLANNING.to_owned(),
    )
    .await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let workspace_dir = workspace_root.path().join(&task.id);
    let worktree_path = workspace_dir.join("forge");
    std::fs::create_dir_all(&worktree_path).expect("worktree creates");
    WorkspaceRepo::create(
        &*db,
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
    .expect("workspace creates");
    std::fs::write(
        workspace_dir.join("plan.md"),
        "- [x] inspect\n- [ ] verify\n",
    )
    .expect("plan writes");

    let result = service
        .transition(
            task.id.clone(),
            crate::workflow::default_states::IN_PROGRESS.to_owned(),
            task.version,
        )
        .await
        .expect("planning can be approved with pending implementation items");

    assert_eq!(
        result.task.status,
        crate::workflow::default_states::IN_PROGRESS
    );
}

#[tokio::test]
async fn transition_from_active_work_requires_complete_plan_checklist() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;

    let task = seed_task_with_status(
        &db,
        &project_id,
        &repo_id,
        crate::workflow::default_states::IN_PROGRESS.to_owned(),
    )
    .await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let workspace_dir = workspace_root.path().join(&task.id);
    let worktree_path = workspace_dir.join("forge");
    std::fs::create_dir_all(&worktree_path).expect("worktree creates");
    WorkspaceRepo::create(
        &*db,
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
    .expect("workspace creates");
    std::fs::write(
        workspace_dir.join("plan.md"),
        "- [x] inspect\n- [ ] verify\n",
    )
    .expect("plan writes");

    let result = service
        .transition(
            task.id.clone(),
            crate::workflow::default_states::REVIEW.to_owned(),
            task.version,
        )
        .await;

    assert!(matches!(
        result,
        Err(ServiceError::GuardRejection { guard, reason })
            if guard == "require_plan_checklist_complete"
                && reason.contains("unchecked item")
    ));

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    std::fs::write(
        workspace_dir.join("plan.md"),
        "- [x] inspect\n- [x] verify\n",
    )
    .expect("plan updates");

    let result = service
        .transition(
            task.id.clone(),
            crate::workflow::default_states::REVIEW.to_owned(),
            task.version,
        )
        .await
        .expect("complete plan allows work stop");

    assert_eq!(result.task.status, crate::workflow::default_states::MERGING);
}

#[tokio::test]
async fn transition_to_review_runs_configured_review_runner() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let runner = Arc::new(::review::ReviewRunner::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        Arc::new(executors::AdapterRegistry::new()),
    ));
    let service =
        TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)).with_review_runner(runner);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    crate::test_support::clear_project_execution_role_defaults(&db, &project_id).await;
    let task = service
        .create_task(
            project_id,
            "Review with CI",
            None,
            None,
            None,
            None,
            Some(r#"{"review":{"ci_steps":["test -d ."]}}"#.to_owned()),
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id, Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let workspace =
        WorkspaceRepo::get_by_id(&*db, claimed.execution.workspace_id.as_deref().unwrap())
            .await
            .expect("workspace loads")
            .expect("workspace exists");
    std::fs::create_dir_all(&workspace.worktree_path).expect("temp worktree creates");

    let result = service
        .transition(
            claimed.task.id.clone(),
            "review".to_owned(),
            claimed.task.version,
        )
        .await
        .expect("task enters review and review runs");

    assert_eq!(
        result.task.status,
        "merging".to_owned(),
        "passed review auto-cascades to merging; no merge service is configured in this unit test"
    );
    assert!(result.review.is_some());
    assert_eq!(
        result.review.as_ref().map(|review| review.status.clone()),
        Some(ReviewStatus::Passed)
    );
}

#[tokio::test]
async fn is_awaiting_human_stays_false_while_review_entry_barrier_is_running() {
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
            role: default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
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
            updated_at: now.clone(),
        },
    )
    .await
    .expect("completed coder execution creates");
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: execution.id,
            attempt_number: 1,
            status: ReviewStatus::AwaitingHuman,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("awaiting_human review creates");

    assert!(
        service
            .is_awaiting_human(task.id.clone())
            .await
            .expect("awaiting human resolves"),
        "a settled review gate with an awaiting_human review is ready for a decision"
    );

    // Mirror the engine: a transition into review is persisted behind a running
    // entry barrier while the blocking before_enter hooks execute, and the
    // review record can already read awaiting_human before that barrier clears.
    let running = TaskRepo::set_entry_barrier(
        &*db,
        &task.id,
        task.version,
        Some(
            json!({
                "state": "review",
                "status": "running",
                "started_at": now,
            })
            .to_string(),
        ),
        &now_rfc3339(),
    )
    .await
    .expect("running entry barrier sets");
    assert!(
        !service
            .is_task_awaiting_human(&running)
            .await
            .expect("awaiting human resolves"),
        "the gate is not ready for a decision while its entry barrier is still running"
    );

    let cleared =
        TaskRepo::set_entry_barrier(&*db, &task.id, running.version, None, &now_rfc3339())
            .await
            .expect("entry barrier clears");
    assert_eq!(cleared.version, running.version + 1);
    assert!(
        !service
            .is_task_awaiting_human(&running)
            .await
            .expect("stale snapshot readiness resolves"),
        "a stale running-barrier snapshot must not be paired with readiness from a newer version"
    );
    assert!(
        service
            .is_task_awaiting_human(&cleared)
            .await
            .expect("awaiting human resolves"),
        "the gate becomes ready once the entry barrier has cleared"
    );
}
