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
async fn create_task_rejects_charter_requirements_on_discovery_before_persistence() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;

    let error = service
        .create_task(
            project_id,
            "Discovery with implementation scope",
            None,
            None,
            None,
            Some("discovery".to_owned()),
            Some(
                r#"{"review":{"requirement_ids":["charter-r1:/scope/required_deliverables/0"]}}"#
                    .to_owned(),
            ),
            None,
            None,
        )
        .await
        .expect_err("discovery requirements must be rejected by the service boundary");

    assert!(error
        .to_string()
        .contains("discovery Tasks cannot own Charter review requirements"));
}

#[tokio::test]
async fn transition_allows_user_move_for_independent_subtask() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let subtask = seed_subtask_with_status(&db, &root, "child", "todo".to_owned(), 0).await;
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
            repo_id: repo_id.clone(),
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
            repo_id: repo_id.clone(),
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
    let result = crate::test_support::drain_transition(&service, result).await;

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
    std::fs::create_dir_all(
        service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves"),
    )
    .expect("temp worktree creates");
    // Review reviews a finished attempt: the claimed execution is the
    // candidate, and it has to be terminal before the Task can enter review.
    sqlx::query("UPDATE execution SET status = 'completed', stopped_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&claimed.execution.id)
        .execute(db.pool())
        .await
        .expect("candidate execution completes");
    let claimed_task = TaskRepo::get_by_id(&*db, &claimed.task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");

    let result = service
        .transition(
            claimed_task.id.clone(),
            "review".to_owned(),
            claimed_task.version,
        )
        .await
        .expect("task enters review and review runs");
    let result = crate::test_support::drain_transition(&service, result).await;

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
async fn review_rerun_recovers_deleted_worktree_from_existing_branch() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(32));
    let workspace_root = TempDir::new().unwrap();
    let runner = Arc::new(::review::ReviewRunner::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        Arc::new(executors::AdapterRegistry::new()),
    ));
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf())
        .with_review_runner(runner);
    let (project_id, _, repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(r#"{"review":{"ci_steps":["test -f candidate.txt && cat candidate.txt"]}}"#)
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let workspace = prepare_workspace(
        &db,
        workspace_root.path(),
        &task,
        &task.id,
        None,
        &service.workspace_backend_router(),
    )
    .await
    .unwrap();
    let worktree_path_buf =
        std::path::PathBuf::from(workspace.embedded_worktree_path_for_backend());
    let worktree_path = worktree_path_buf.as_path();
    std::fs::write(
        worktree_path.join("candidate.txt"),
        "preserved candidate branch\n",
    )
    .unwrap();
    run_git(worktree_path, &["add", "candidate.txt"]);
    run_git(
        worktree_path,
        &["commit", "-m", "candidate branch evidence"],
    );
    let sha = git::get_current_sha(worktree_path).await.unwrap();
    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: workspace.before_sha.clone(),
            after_sha: Some(sha.clone()),
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace.id.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    // Reproduce the live layout: the Task directory/outbox remains but the
    // linked checkout and Git's worktree registration have disappeared.
    std::fs::create_dir_all(worktree_path.parent().unwrap().join(".forge-outbox")).unwrap();
    std::fs::remove_dir_all(worktree_path).unwrap();
    run_git(repo_dir.path(), &["worktree", "prune"]);
    assert!(git::branch_exists(repo_dir.path(), &workspace.branch)
        .await
        .unwrap());

    let (settled, review) = service
        .rerun_review(Uuid::parse_str(&task.id).unwrap())
        .await
        .unwrap();
    assert_eq!(settled.status, "merging");
    assert_eq!(review.status, ReviewStatus::Passed);
    assert_eq!(review.execution_id, execution.id);
    // CI-only reruns persist the step array directly; the workflow entry
    // hook and auditor reviews use an object containing `ci_steps` instead.
    let steps: Vec<Value> = serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0]["index"], 0);
    assert_eq!(
        steps[0]["command"],
        "test -f candidate.txt && cat candidate.txt"
    );
    assert_eq!(steps[0]["exit_code"], 0);
    assert!(steps[0]["output_tail"]
        .as_str()
        .unwrap()
        .contains("preserved candidate branch"));
    let recovered = WorkspaceRepo::get_by_task_id(&*db, &task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.id, workspace.id);
    assert_eq!(recovered.branch, workspace.branch);
    assert_eq!(git::get_current_sha(worktree_path).await.unwrap(), sha);
}

// While the current entry's hook step (CI, before-work scripts, dispatch) is
// pending or running, the latest Review may be a stale one from an earlier
// entry. Readiness and owner offers wait for the step, as they did for the
// running entry barrier.
#[tokio::test]
async fn readiness_and_offers_wait_while_review_entry_hooks_run() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    crate::task_service::tests::helpers::seed_role_assignment(
        &db,
        &task.id,
        "coder",
        Some(&agent_id),
    )
    .await;
    crate::task_service::tests::helpers::seed_role_assignment(
        &db,
        &task.id,
        "reviewer",
        Some(&agent_id),
    )
    .await;
    let now = now_rfc3339();
    let execution = crate::task_service::tests::helpers::seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        "coder",
        ExecutionStatus::Completed,
        Some("session"),
        &now,
    )
    .await;
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
    .expect("stale review creates");
    let actor = api_types::Actor::user(api_types::UserActionSource::Test);
    let offers = |snapshot: &crate::TaskSnapshot| {
        crate::available_actions(snapshot)
            .into_iter()
            .map(|offer| offer.action.verb().to_owned())
            .collect::<Vec<_>>()
    };
    assert!(service.is_task_awaiting_human(&task).await.unwrap());
    let settled = offers(
        &service
            .task_action_snapshot(&task.id, &actor)
            .await
            .unwrap(),
    );
    assert!(
        settled.iter().any(|verb| verb != "cancel"),
        "a settled gate offers decisions: {settled:?}"
    );

    // The entry's hook step is queued behind CI.
    let step_id = db::TaskStepRepo::enqueue_step(
        &*db,
        &db::EnqueueTaskStep {
            kind: "hooks".into(),
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: "review-entry".into(),
            chain_id: new_uuid_v4(),
            chain_position: 1,
            expected_status: "review".into(),
            expected_version: task.version,
            expected_epoch: None,
            lane: "long".into(),
            available_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    assert!(
        !service.is_task_awaiting_human(&task).await.unwrap(),
        "a stale Review is not a decision point while entry checks run"
    );
    let running = offers(
        &service
            .task_action_snapshot(&task.id, &actor)
            .await
            .unwrap(),
    );
    assert!(
        running.iter().all(|verb| verb == "cancel"),
        "only cancellation while entry checks run: {running:?}"
    );

    // A hooks row of an earlier entry does not hold the current one.
    sqlx::query("UPDATE task_step SET expected_epoch = expected_epoch - 1 WHERE id = ?")
        .bind(&step_id)
        .execute(db.pool())
        .await
        .unwrap();
    assert!(service.is_task_awaiting_human(&task).await.unwrap());
    sqlx::query(
        "UPDATE task_step SET expected_epoch = expected_epoch + 1, status = 'done' WHERE id = ?",
    )
    .bind(&step_id)
    .execute(db.pool())
    .await
    .unwrap();
    assert!(service.is_task_awaiting_human(&task).await.unwrap());
}
