//! Workflow action reviewer dispatch tests.

use super::*;

#[tokio::test]
async fn ci_passes_then_reviewer_dispatched_via_dispatch_role_agent() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-dispatch";
    let mut harness =
        build_reviewer_dispatch_harness(&task_id, reviewer_id, 2, vec!["test -d ."]).await;
    let ctx = harness.ctx.clone();

    let ci_result = run_ci(&ctx).await;
    assert!(
        matches!(ci_result, HookResult::Ok),
        "CI should pass: {ci_result:?}"
    );

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(
        reviews[0].status,
        ReviewStatus::Running,
        "reviewer assigned so review stays Running"
    );

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "dispatch should succeed: {dispatch_result:?}"
    );

    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("executor spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, ctx.task_id);

    let executions = ExecutionRepo::list_by_task_and_role(
        &*ctx.db,
        &ctx.task_id,
        default_roles::REVIEWER,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("reviewer executions load");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(executions.items[0].status, db::ExecutionStatus::Running);
    assert_eq!(executions.items[0].agent_id.as_deref(), Some(reviewer_id));
    assert_eq!(executions.items[0].role, default_roles::REVIEWER);
}

#[tokio::test]
async fn reviewer_follow_up_dispatch_uses_captured_review_admission() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-follow-up-admission";
    let mut harness = build_reviewer_dispatch_harness(&task_id, reviewer_id, 2, Vec::new()).await;
    let mut ctx = harness.ctx.clone();

    let candidate_id = ctx
        .execution_id
        .clone()
        .expect("implementation candidate seeded");
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let now = now_rfc3339();
    let review = ReviewRepo::create_with_task_authority(
        &*ctx.db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate_id.clone(),
            attempt_number: 0,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
        task.version,
        default_states::REVIEW,
        ctx.project_version,
        ctx.project_workflow_definition.as_deref(),
        Some(&candidate_id),
    )
    .await
    .expect("review creates under captured authority");
    let candidate = ExecutionRepo::get_by_id(&*ctx.db, &candidate_id)
        .await
        .expect("candidate loads")
        .expect("candidate exists");

    let prior_reviewer_id = new_uuid_v4();
    let now = now_rfc3339();
    ExecutionRepo::create(
        &*ctx.db,
        CreateExecution {
            id: prior_reviewer_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(reviewer_id.to_owned()),
            role: default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: Some(candidate_id.clone()),
            agent_session_id: Some("review-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("previous reviewer turn".to_owned()),
            logs_path: None,
            before_sha: candidate.before_sha.clone(),
            after_sha: candidate.after_sha.clone(),
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: candidate.workspace_id.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("prior reviewer execution creates");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&prior_reviewer_id)
        .bind(&review.id)
        .execute(ctx.db.pool())
        .await
        .expect("review attempt binds prior reviewer");

    let mut workflow = default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == default_states::REVIEW)
        .expect("review state exists")
        .dispatch
        .as_mut()
        .expect("review dispatch exists")
        .execution_policy = Some(api_types::WorkflowExecutionPolicy::ResumeLatestTargetRoleThread);
    ctx.workflow = Arc::new(workflow);

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "reviewer follow-up dispatch should succeed: {dispatch_result:?}"
    );
    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("follow-up executor spawned in time")
        .expect("follow-up execution context received");

    let executions = ExecutionRepo::list_by_task_and_role(
        &*ctx.db,
        &ctx.task_id,
        default_roles::REVIEWER,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("reviewer executions load");
    assert_eq!(executions.items.len(), 2);
    let follow_up = executions
        .items
        .iter()
        .find(|execution| execution.id == execution_ctx.execution_id)
        .expect("follow-up reviewer execution persists");
    assert_eq!(
        follow_up.parent_execution_id.as_deref(),
        Some(candidate_id.as_str())
    );
    let review_after = ReviewRepo::get_by_id(&*ctx.db, &review.id)
        .await
        .expect("review reloads")
        .expect("review exists");
    assert_eq!(
        review_after.reviewer_execution_id.as_deref(),
        Some(follow_up.id.as_str())
    );
}

#[tokio::test]
async fn subtask_root_still_dispatches_reviewer_after_coder_completion() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-subtask-root";
    let mut harness =
        build_reviewer_dispatch_harness(&task_id, reviewer_id, 2, vec!["test -d ."]).await;
    let mut ctx = harness.ctx.clone();
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let now = now_rfc3339();
    let subtask_id = new_uuid_v4();
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: subtask_id.clone(),
            project_id: task.project_id.clone(),
            parent_task_id: Some(task.id.clone()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "ordered child".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: default_states::DONE.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("subtask creates");
    let root_execution = ExecutionRepo::get_by_id(
        &*ctx.db,
        ctx.execution_id
            .as_deref()
            .expect("root execution is seeded"),
    )
    .await
    .expect("root execution loads")
    .expect("root execution exists");
    let child_execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &*ctx.db,
        CreateExecution {
            id: child_execution_id.clone(),
            task_id: subtask_id,
            agent_id: None,
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: Some(root_execution.id),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("subtask executor summary".to_owned()),
            logs_path: None,
            before_sha: root_execution.before_sha.clone(),
            after_sha: root_execution.after_sha,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: root_execution.workspace_id,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("subtask execution creates");
    ctx.execution_id = Some(child_execution_id);
    let ci_result = run_ci(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "reviewer dispatch should not be skipped for subtask roots: {dispatch_result:?}"
    );

    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("executor spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, ctx.task_id);
}

#[tokio::test]
async fn reviewer_dispatch_ignores_waiting_review_tasks_without_running_execution() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-waiting-review";
    let mut harness =
        build_reviewer_dispatch_harness(&task_id, reviewer_id, 1, vec!["test -d ."]).await;
    let ctx = harness.ctx.clone();
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let other_task_id = "task-waiting-review-no-execution";
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: other_task_id.to_owned(),
            project_id: task.project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "other waiting review task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: default_states::REVIEW.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("other task creates");
    assign_agent_role(&ctx.db, other_task_id, default_roles::REVIEWER, reviewer_id).await;

    let ci_result = run_ci(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "dispatch should not be blocked by waiting review tasks: {dispatch_result:?}"
    );

    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("executor spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, ctx.task_id);
}

#[tokio::test]
async fn ci_fails_reviewer_not_dispatched_cascade_handles_bounce() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-ci-fail";
    let harness = build_reviewer_dispatch_harness(&task_id, reviewer_id, 2, vec!["exit 1"]).await;
    let ctx = harness.ctx.clone();

    let ci_result = run_ci(&ctx).await;
    assert!(
        matches!(ci_result, HookResult::Failed { .. }),
        "CI hook returns Failed on CI failure: {ci_result:?}"
    );

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(
        reviews[0].status,
        ReviewStatus::Failed,
        "failing CI sets review to Failed"
    );

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    match dispatch_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "review already failed"),
        other => panic!("expected skipped, got {other:?}"),
    }

    let cascade_result = AutoCascadeOnReviewPass.execute(&ctx).await;
    match cascade_result {
        HookResult::Cascade { to, reason, .. } => {
            assert_eq!(to, default_states::IN_PROGRESS);
            assert_eq!(reason, "review failed");
        }
        other => panic!("expected cascade to in_progress, got {other:?}"),
    }

    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::REVIEWER)
            .await
            .expect("reviewer execution count"),
        0,
        "no reviewer execution should be created when CI fails"
    );
}

#[tokio::test]
async fn reviewer_dispatch_creates_fresh_attempt_after_prior_failure() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-fresh-attempt";
    let mut harness = build_reviewer_dispatch_harness(&task_id, reviewer_id, 2, Vec::new()).await;
    let mut ctx = harness.ctx.clone();

    let failed_candidate_id = ctx
        .execution_id
        .clone()
        .expect("prior review candidate is present");
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*ctx.db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let now = now_rfc3339();
    ReviewRepo::create_with_task_authority(
        &*ctx.db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: failed_candidate_id.clone(),
            attempt_number: 0,
            status: ReviewStatus::Failed,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
        task.version,
        default_states::REVIEW,
        Some(project.version),
        Some(project.workflow_definition.as_str()),
        Some(failed_candidate_id.as_str()),
    )
    .await
    .expect("prior failed review creates");

    // A remediation run supplies a new implementation candidate. The old
    // Failed Review remains immutable; reviewer dispatch must create a new
    // Running attempt bound to this candidate before launching the reviewer.
    let workspace = WorkspaceRepo::get_by_task_id(&*ctx.db, &ctx.task_id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    let replacement_candidate_id = new_uuid_v4();
    let now = now_rfc3339();
    ExecutionRepo::create(
        &*ctx.db,
        CreateExecution {
            id: replacement_candidate_id.clone(),
            task_id: ctx.task_id.clone(),
            agent_id: None,
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
            summary: Some("replacement executor summary".to_owned()),
            logs_path: None,
            before_sha: workspace.before_sha.clone(),
            after_sha: workspace.before_sha.clone(),
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace.id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("replacement execution creates");
    ctx.execution_id = Some(replacement_candidate_id.clone());
    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "fresh reviewer attempt should dispatch: {dispatch_result:?}"
    );
    let _execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("replacement reviewer spawned in time")
        .expect("replacement reviewer context received");

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 2);
    let failed = reviews
        .iter()
        .find(|review| review.execution_id == failed_candidate_id)
        .expect("failed review remains present");
    assert_eq!(failed.status, ReviewStatus::Failed);
    let running = reviews
        .iter()
        .find(|review| review.execution_id == replacement_candidate_id)
        .expect("fresh review binds replacement candidate");
    assert_eq!(running.status, ReviewStatus::Running);
    assert!(
        running.attempt_number > failed.attempt_number,
        "fresh reviewer attempt must advance the terminal review attempt"
    );
}

#[tokio::test]
async fn reviewer_at_capacity_ci_runs_dispatch_queues() {
    let task_id = new_uuid_v4();
    let reviewer_id = "agent-reviewer-capacity";
    let harness =
        build_reviewer_dispatch_harness(&task_id, reviewer_id, 1, vec!["test -d ."]).await;
    let ctx = harness.ctx.clone();

    let other_task_id = new_uuid_v4();
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: other_task_id.clone(),
            project_id: task.project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "other review task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: default_states::REVIEW.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("other task creates");
    assign_agent_role(
        &ctx.db,
        &other_task_id,
        default_roles::REVIEWER,
        reviewer_id,
    )
    .await;
    seed_running_execution_for_task(
        &ctx.db,
        &other_task_id,
        reviewer_id,
        default_roles::REVIEWER,
    )
    .await;
    crate::test_support::set_test_agent_capacity(&ctx.db, reviewer_id, 1).await;

    let ci_result = run_ci(&ctx).await;
    assert!(
        matches!(ci_result, HookResult::Ok),
        "CI should pass even when reviewer at capacity: {ci_result:?}"
    );

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Running);

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    match dispatch_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "agent at capacity"),
        other => panic!("expected skipped at capacity, got {other:?}"),
    }

    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::REVIEWER)
            .await
            .expect("reviewer execution count"),
        0,
        "no reviewer execution when agent at capacity"
    );
}

#[tokio::test]
async fn no_reviewer_assigned_auto_cascade_to_merging() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.state_config = json!({});

    let ci_result = run_ci(&ctx).await;
    match ci_result {
        HookResult::Skipped { reason } => assert!(reason.contains("ci steps")),
        other => panic!("expected skipped for empty ci, got {other:?}"),
    }

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    match dispatch_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "no reviewer role assigned"),
        other => panic!("expected skipped for no reviewer, got {other:?}"),
    }

    let cascade_result = super::AutoCascadeOnUnconfiguredReview.execute(&ctx).await;
    match cascade_result {
        HookResult::Cascade { to, reason, .. } => {
            assert_eq!(to, default_states::MERGING);
            assert!(reason.contains("no checks or reviewer"));
        }
        other => panic!("expected cascade to merging, got {other:?}"),
    }
}

#[tokio::test]
async fn read_only_task_without_reviewer_ignores_implementation_checks_and_cascades() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    sqlx::query("UPDATE task SET task_type = 'planning_task' WHERE id = ?")
        .bind(&ctx.task_id)
        .execute(ctx.db.pool())
        .await
        .expect("Task kind updates");
    ctx.state_config = json!({"ci_steps":["false"]});

    let cascade = super::AutoCascadeOnUnconfiguredReview.execute(&ctx).await;

    match cascade {
        HookResult::Cascade { to, reason, .. } => {
            assert_eq!(to, default_states::MERGING);
            assert!(reason.contains("no checks or reviewer"));
        }
        other => panic!("expected cascade to merging, got {other:?}"),
    }
}
