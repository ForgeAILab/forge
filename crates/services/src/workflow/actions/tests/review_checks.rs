//! Workflow action review checks tests.

use super::*;

#[tokio::test]
async fn run_ci_steps_empty_reason_contains_ci_steps() {
    let mut ctx = build_test_ctx(
        "task-run-review-ci-steps",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.state_config = json!({ "ci_steps": [] });

    let result = run_ci(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert!(reason.contains("ci steps")),
        other => panic!("expected skipped result, got {other:?}"),
    }
}

#[tokio::test]
async fn run_ci_steps_skips_when_ci_steps_empty() {
    let mut ctx = build_test_ctx(
        "task-run-review-empty",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.state_config = json!({ "ci_steps": [] });

    let result = run_ci(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert!(reason.contains("ci steps")),
        other => panic!("expected skipped result, got {other:?}"),
    }
}

#[tokio::test]
async fn run_ci_steps_skips_project_implementation_checks_for_discovery_tasks() {
    let mut ctx = build_test_ctx(
        "task-run-review-read-only",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    sqlx::query("UPDATE task SET task_type = 'discovery' WHERE id = ?")
        .bind(&ctx.task_id)
        .execute(ctx.db.pool())
        .await
        .expect("Task kind updates");
    ctx.state_config = json!({ "ci_steps": ["false"] });

    let result = run_ci(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert!(reason.contains("read-only Task")),
        other => panic!("expected read-only skip, got {other:?}"),
    }
    assert!(
        ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
            .await
            .expect("reviews load")
            .is_empty(),
        "a read-only Task must not create an implementation CI review attempt"
    );
}

#[tokio::test]
async fn run_ci_steps_skips_when_workspace_missing() {
    let mut ctx = build_test_ctx(
        "task-run-review-no-workspace",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.state_config = json!({ "ci_steps": ["cargo test"] });
    // With a delivered run the missing Workspace is what skips the checks;
    // without one the entry parks as a review with nothing to review.
    let now = db::now_rfc3339();
    db::ExecutionRepo::create(
        &*ctx.db,
        serde_json::from_value(json!({
            "id": db::new_uuid_v4(), "task_id": ctx.task_id, "role": "coder",
            "status": db::ExecutionStatus::Completed,
            "summary": "delivered", "created_at": now, "updated_at": now
        }))
        .unwrap(),
    )
    .await
    .unwrap();

    let result = run_ci(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert!(reason.contains("no workspace")),
        other => panic!("expected skipped result, got {other:?}"),
    }
}

/// Configured checks cannot run without a delivered candidate. The entry no
/// longer skips them silently (which left the Task in review with nothing
/// queued): it states the "nothing to review" park for the owner.
#[tokio::test]
async fn run_ci_steps_parks_when_executor_execution_missing() {
    let mut ctx = build_test_ctx(
        "task-run-review-runner-missing",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.state_config = json!({ "ci_steps": ["cargo test"] });
    ctx.workspace_id = Some("workspace-1".to_owned());

    let result = run_ci(&ctx).await;

    match result {
        HookResult::Skipped { reason } => assert!(reason.contains("nothing to review")),
        other => panic!("expected skipped result, got {other:?}"),
    }
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(
        task.condition
            .read()
            .diagnostic
            .as_ref()
            .is_some_and(|annotation| annotation.blocking_reason
                == crate::task_service::MISSING_REVIEW_CANDIDATE),
        "{:?}",
        task.condition
    );
}

#[tokio::test]
async fn run_ci_steps_creates_passed_review_record() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });

    enter_target_state(&ctx).await;
    let result = run_ci(&ctx).await;

    assert!(matches!(result, HookResult::Ok));
    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Passed);
}

#[tokio::test]
async fn run_ci_steps_pass_then_dispatches_reviewer() {
    let agent_id = "agent-reviewer-dispatch";
    let mut harness = build_role_dispatch_harness(
        "task-run-ci-reviewer-dispatch",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        default_roles::REVIEWER,
        agent_id,
        1,
    )
    .await;
    let mut ctx = harness.ctx.clone();
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });

    let ci_result = run_ci(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Running);

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "{dispatch_result:?}"
    );

    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("reviewer executor spawned in time")
        .expect("reviewer execution context received");
    assert_eq!(execution_ctx.task_id, ctx.task_id);
    // The shell reviewer receives the frozen contract instead of a canned verdict.
    assert!(execution_ctx.description.contains("FORGE_REVIEW_CONTRACT"));
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::REVIEWER)
            .await
            .expect("reviewer execution count loads"),
        1
    );
}

#[tokio::test]
async fn review_create_rejects_stale_task_snapshot_without_inserting_attempt() {
    let task_id = new_uuid_v4();
    let harness =
        build_reviewer_dispatch_harness(&task_id, "agent-review-authority-create", 1, vec!["true"])
            .await;
    let ctx = harness.ctx;
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let candidate_id = ctx
        .execution_id
        .clone()
        .expect("implementation candidate seeded");
    let stale_version = task.version;

    TaskRepo::set_entry_barrier(
        &*ctx.db,
        &task.id,
        stale_version,
        Some(json!({ "authority_race": true }).to_string()),
        &now_rfc3339(),
    )
    .await
    .expect("competing Task update wins");

    let now = now_rfc3339();
    let error = ReviewRepo::create_with_task_authority(
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
        stale_version,
        default_states::REVIEW,
        ctx.project_version,
        ctx.project_workflow_definition.as_deref(),
        Some(&candidate_id),
    )
    .await
    .expect_err("stale Task authority must reject the Review insert");
    assert!(matches!(error, db::DbError::VersionConflict));
    assert!(ReviewRepo::list_by_task(&*ctx.db, &task.id)
        .await
        .expect("reviews load")
        .is_empty());
}

#[tokio::test]
async fn review_settlement_rejects_stale_project_authority_without_mutation() {
    let task_id = new_uuid_v4();
    let harness =
        build_reviewer_dispatch_harness(&task_id, "agent-review-authority-settle", 1, vec!["true"])
            .await;
    let ctx = harness.ctx;
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*ctx.db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let candidate_id = ctx
        .execution_id
        .clone()
        .expect("implementation candidate seeded");
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
        Some(project.version),
        Some(project.workflow_definition.as_str()),
        Some(&candidate_id),
    )
    .await
    .expect("review creates under the captured authority");

    ProjectRepo::update_workflow(
        &*ctx.db,
        &project.id,
        "{\"workflow\":\"edited\"}",
        None,
        project.version,
        &now_rfc3339(),
    )
    .await
    .expect("competing Project workflow update wins");

    let error = ReviewRepo::update_status_with_review_authority_and_task_projection(
        &*ctx.db,
        &review.id,
        ReviewStatus::Failed,
        json!({ "ci_steps": [] }).to_string(),
        Some(now_rfc3339()),
        &now_rfc3339(),
        task.version,
        default_states::REVIEW,
        Some(project.version),
        Some(project.workflow_definition.as_str()),
        review.status.clone(),
        &review.updated_at,
        &candidate_id,
        Some(None),
    )
    .await
    .expect_err("stale Project authority must reject settlement");
    assert!(matches!(error, db::DbError::VersionConflict));

    let review_after = ReviewRepo::get_by_id(&*ctx.db, &review.id)
        .await
        .expect("review reloads")
        .expect("review remains present");
    assert_eq!(review_after.status, ReviewStatus::Running);
    let task_after = TaskRepo::get_by_id(&*ctx.db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task remains present");
    assert_eq!(task_after.version, task.version);
    assert!(task_after.review_passed_at.is_none());
}

#[tokio::test]
async fn review_authority_loss_cancels_after_candidate_moves_without_task_projection() {
    let task_id = new_uuid_v4();
    let harness =
        build_reviewer_dispatch_harness(&task_id, "agent-review-authority-cancel", 1, Vec::new())
            .await;
    let ctx = harness.ctx;
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*ctx.db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let candidate_id = ctx
        .execution_id
        .clone()
        .expect("implementation candidate seeded");
    let now = now_rfc3339();
    let review = ReviewRepo::create_with_task_authority(
        &*ctx.db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate_id,
            attempt_number: 0,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
        task.version,
        default_states::REVIEW,
        Some(project.version),
        Some(project.workflow_definition.as_str()),
        ctx.execution_id.as_deref(),
    )
    .await
    .expect("review creates under the captured authority");
    let workspace = WorkspaceRepo::get_by_task_id(&*ctx.db, &ctx.task_id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    let replacement_candidate_id = new_uuid_v4();
    let now = now_rfc3339();
    ExecutionRepo::create(
        &*ctx.db,
        CreateExecution {
            id: replacement_candidate_id,
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
            after_sha: workspace.before_sha,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace.id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("replacement execution creates");

    super::common::cancel_review_after_authority_loss(&ctx, &review, "candidate moved").await;

    let cancelled = ReviewRepo::get_by_id(&*ctx.db, &review.id)
        .await
        .expect("review reloads")
        .expect("review exists");
    assert_eq!(cancelled.status, ReviewStatus::Cancelled);
    let details: serde_json::Value =
        serde_json::from_str(&cancelled.step_results_json).expect("review details parse");
    assert_eq!(
        details["execution_retry"]["status"],
        "cancelled_authority_lost"
    );
    let task_after = TaskRepo::get_by_id(&*ctx.db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(task_after.version, task.version);
    assert_eq!(task_after.review_passed_at, task.review_passed_at);
}

#[tokio::test]
async fn merge_fix_re_review_still_requires_an_assigned_reviewer() {
    let agent_id = "agent-reviewer-ci-only";
    let mut harness = build_role_dispatch_harness(
        "task-run-ci-reviewer-ci-only",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        default_roles::REVIEWER,
        agent_id,
        1,
    )
    .await;
    let mut ctx = harness.ctx.clone();
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });
    TaskRepo::set_review_passed_at(&*ctx.db, &ctx.task_id, Some(now_rfc3339()), &now_rfc3339())
        .await
        .expect("review_passed_at seeds");

    let ci_result = run_ci(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    // Content changed under the merge fix, so a passing CI run no longer
    // substitutes for the assigned reviewer's conformance assessment.
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Running);
    assert!(!reviews[0].step_results_json.contains("pass_ci_only"));

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    assert!(
        matches!(dispatch_result, HookResult::Ok),
        "{dispatch_result:?}"
    );
    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("reviewer executor spawned in time")
        .expect("reviewer execution context received");
    assert_eq!(execution_ctx.task_id, ctx.task_id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*ctx.db, &ctx.task_id, default_roles::REVIEWER)
            .await
            .expect("reviewer execution count loads"),
        1
    );
}

#[tokio::test]
async fn run_ci_steps_failure_prevents_reviewer_dispatch() {
    let agent_id = "agent-reviewer-ci-fail";
    let mut harness = build_role_dispatch_harness(
        "task-run-ci-reviewer-fail",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        default_roles::REVIEWER,
        agent_id,
        1,
    )
    .await;
    let mut ctx = harness.ctx.clone();
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["false"] });

    let ci_result = run_ci(&ctx).await;
    assert!(
        matches!(ci_result, HookResult::Failed { .. }),
        "{ci_result:?}"
    );

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Failed);

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    match dispatch_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "review already failed"),
        other => panic!("expected skipped dispatch, got {other:?}"),
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn run_ci_steps_keeps_review_running_when_reviewer_at_capacity() {
    let agent_id = "agent-reviewer-capacity";
    let mut harness = build_role_dispatch_harness(
        "task-run-ci-reviewer-capacity",
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        default_roles::REVIEWER,
        agent_id,
        1,
    )
    .await;
    let mut ctx = harness.ctx.clone();
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });

    let current_task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let other_task_id = "task-run-ci-reviewer-capacity-other";
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: other_task_id.to_owned(),
            project_id: current_task.project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "other review task".to_owned(),
            description: Some("echo other".to_owned()),
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
    assign_agent_role(&ctx.db, other_task_id, default_roles::REVIEWER, agent_id).await;
    seed_running_execution_for_task(&ctx.db, other_task_id, agent_id, default_roles::REVIEWER)
        .await;
    crate::test_support::set_test_agent_capacity(&ctx.db, agent_id, 1).await;

    let ci_result = run_ci(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    match dispatch_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "agent at capacity"),
        other => panic!("expected capacity skip, got {other:?}"),
    }

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Running);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn no_ci_review_attempt_starts_before_pause_and_capacity_checks() {
    let agent_id = "agent-reviewer-no-ci-capacity";
    let mut harness = build_role_dispatch_harness(
        "task-review-no-ci-capacity",
        default_states::MERGE_FAILED,
        default_states::REVIEW,
        default_roles::REVIEWER,
        agent_id,
        1,
    )
    .await;
    let mut ctx = harness.ctx.clone();
    ctx.state_config = json!({ "ci_steps": [] });

    let old_candidate = seed_review(&ctx, ReviewStatus::Passed, 1).await;
    sqlx::query("UPDATE execution SET created_at = ?, updated_at = ? WHERE id = ?")
        .bind("2000-01-01T00:00:00Z")
        .bind("2000-01-01T00:00:01Z")
        .bind(&old_candidate)
        .execute(ctx.db.pool())
        .await
        .expect("old candidate timestamp updates");
    let current_candidate = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(current_candidate.clone());

    let current_task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let other_task_id = "task-review-no-ci-capacity-other";
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: other_task_id.to_owned(),
            project_id: current_task.project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "other review task".to_owned(),
            description: Some("echo other".to_owned()),
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
    assign_agent_role(&ctx.db, other_task_id, default_roles::REVIEWER, agent_id).await;
    seed_running_execution_for_task(&ctx.db, other_task_id, agent_id, default_roles::REVIEWER)
        .await;
    crate::test_support::set_test_agent_capacity(&ctx.db, agent_id, 1).await;

    ProjectRepo::set_paused_at(&*ctx.db, &current_task.project_id, Some(now_rfc3339()))
        .await
        .expect("project pauses");
    refresh_project_authority(&mut ctx).await;
    let paused_result = DispatchRoleAgent.execute(&ctx).await;
    match paused_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "project paused"),
        other => panic!("expected pause skip, got {other:?}"),
    }
    let paused_reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load while paused");
    assert_eq!(
        paused_reviews.len(),
        1,
        "paused dispatch must not create a review attempt"
    );
    ProjectRepo::set_paused_at(&*ctx.db, &current_task.project_id, None)
        .await
        .expect("project resumes");
    refresh_project_authority(&mut ctx).await;

    let dispatch_result = DispatchRoleAgent.execute(&ctx).await;
    match dispatch_result {
        HookResult::Skipped { reason } => assert_eq!(reason, "agent at capacity"),
        other => panic!("expected capacity skip, got {other:?}"),
    }

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 2);
    let current_review = reviews
        .iter()
        .max_by_key(|review| review.attempt_number)
        .expect("current review exists");
    assert_eq!(current_review.status, ReviewStatus::Running);
    assert_eq!(current_review.execution_id, current_candidate);
    assert!(matches!(
        AutoCascadeOnReviewPass.execute(&ctx).await,
        HookResult::Ok
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn run_ci_steps_without_reviewer_cascades_to_merging() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });

    enter_target_state(&ctx).await;
    let ci_result = run_ci(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    match AutoCascadeOnReviewPass.execute(&ctx).await {
        HookResult::Cascade { to, reason, .. } => {
            assert_eq!(to, default_states::MERGING);
            assert_eq!(reason, "review passed");
        }
        other => panic!("expected cascade to merging, got {other:?}"),
    }
}

#[tokio::test]
async fn cached_passed_review_cannot_cascade_after_its_authority_is_cleared() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });
    enter_target_state(&ctx).await;
    let ci_result = run_ci(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");
    TaskRepo::set_review_passed_at(&*ctx.db, &ctx.task_id, None, &now_rfc3339())
        .await
        .expect("cached review authority clears");

    let cascade_result = AutoCascadeOnReviewPass.execute(&ctx).await;

    assert!(
        matches!(cascade_result, HookResult::Ok),
        "an old passed row without current review authority must be inert: {cascade_result:?}"
    );
}

#[tokio::test]
async fn passed_review_defers_integration_while_project_is_paused() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    TaskRepo::update_status(
        &*ctx.db,
        UpdateTaskStatus {
            id: task.id,
            expected_version: task.version,
            status: default_states::REVIEW.to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task enters review");
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });
    let ci_result = run_ci(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");
    ProjectRepo::set_paused_at(&*ctx.db, &ctx.project_id, Some(now_rfc3339()))
        .await
        .expect("project pauses");
    refresh_project_authority(&mut ctx).await;

    let cascade_result = AutoCascadeOnReviewPass.execute(&ctx).await;

    match cascade_result {
        HookResult::Skipped { reason } => {
            assert_eq!(reason, "project paused; integration deferred")
        }
        other => panic!("expected paused integration skip, got {other:?}"),
    }
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let deferred = crate::deferred_dispatch::paused_integration(&task)
        .expect("paused integration marker exists");
    assert_eq!(deferred.state, default_states::REVIEW);
}

#[tokio::test]
async fn run_ci_steps_with_user_approval_gate_waits_for_human() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.gate_config
        .as_mut()
        .expect("review gate config")
        .requires_user_approval = Some(true);
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);
    ctx.state_config = json!({ "ci_steps": ["test -d ."] });

    enter_target_state(&ctx).await;
    let ci_result = run_ci(&ctx).await;
    assert!(matches!(ci_result, HookResult::Ok), "{ci_result:?}");

    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::AwaitingHuman);
    assert!(reviews[0].finished_at.is_none());

    let cascade_result = AutoCascadeOnReviewPass.execute(&ctx).await;
    assert!(
        matches!(cascade_result, HookResult::Ok),
        "{cascade_result:?}"
    );
}

#[tokio::test]
async fn unconfigured_review_with_user_approval_gate_waits_for_human() {
    let task_id = new_uuid_v4();
    let mut ctx = build_test_ctx(
        &task_id,
        default_states::IN_PROGRESS,
        default_states::REVIEW,
        None,
    )
    .await;
    ctx.gate_config
        .as_mut()
        .expect("review gate config")
        .requires_user_approval = Some(true);
    let execution_id = seed_completed_executor_execution(&ctx).await;
    ctx.execution_id = Some(execution_id);

    enter_target_state(&ctx).await;
    let result = super::AutoCascadeOnUnconfiguredReview.execute(&ctx).await;

    assert!(matches!(result, HookResult::Ok), "{result:?}");
    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::AwaitingHuman);
}
