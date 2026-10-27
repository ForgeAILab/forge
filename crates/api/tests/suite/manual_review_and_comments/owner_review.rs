//! Manual review configuration, rejection, guidance and merge approval.

use super::*;

// ── Test 13.1: ReviewConfig serialization ──

#[test]
fn review_config_serializes_without_auditor_agent_id() {
    use api_types::ReviewConfig;

    let config = ReviewConfig {
        conformance_checks: Vec::new(),
        requirement_ids: Vec::new(),
        requirement_allocations: Default::default(),
        setup_steps: vec!["cargo fetch".to_owned()],
        ci_steps: vec!["cargo test".to_owned()],
        review_prompt: Some("Check for correctness".to_owned()),
        check_timeout_seconds: None,
        verify: None,
    };
    let json_str = serde_json::to_string(&config).expect("serializes");
    let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("parses");

    assert_eq!(parsed["ci_steps"][0], "cargo test");
    assert_eq!(parsed["setup_steps"][0], "cargo fetch");
    assert_eq!(parsed["review_prompt"], "Check for correctness");
    assert!(
        parsed.get("auditor_agent_id").is_none(),
        "auditor_agent_id should not be present"
    );

    let deserialized: ReviewConfig = serde_json::from_str(&json_str).expect("deserializes");
    assert_eq!(deserialized.ci_steps, vec!["cargo test".to_owned()]);
    assert_eq!(
        deserialized.review_prompt,
        Some("Check for correctness".to_owned())
    );

    let empty = ReviewConfig {
        conformance_checks: Vec::new(),
        requirement_ids: Vec::new(),
        requirement_allocations: Default::default(),
        setup_steps: vec![],
        ci_steps: vec![],
        review_prompt: None,
        check_timeout_seconds: None,
        verify: None,
    };
    let json_str = serde_json::to_string(&empty).expect("serializes");
    let deserialized: ReviewConfig = serde_json::from_str(&json_str).expect("deserializes");
    assert!(deserialized.ci_steps.is_empty());
    assert_eq!(deserialized.review_prompt, None);
}

// ── Test 13.5: reject_review bounces to in_progress ──

#[tokio::test]
async fn reject_review_bounces_to_in_progress() {
    let workspace_root = common::TestDir::new("forge-manual-review-reject");
    let harness = test_app(workspace_root.path()).await;

    let (task_id, _review_id) = seed_awaiting_human_review(
        &harness.state.db,
        r#"{"retry_budgets":{"review":3,"merge_fix":1}}"#,
        workspace_root.path(),
    )
    .await;

    let result: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/actions"),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{task_id}/actions")).await, "action": {"verb":"send_back","guidance":"bad code quality"}}),
        StatusCode::OK,
    )
    .await;

    let decision_review = ReviewRepo::list_by_task(&*harness.state.db, &result.id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|review| review.attempt_number)
        .expect("decision Review persists");
    assert_eq!(decision_review.status, db::ReviewStatus::Failed);
    assert_eq!(result.status, "in_progress".to_owned());
}

#[tokio::test]
async fn send_back_carries_required_guidance() {
    let workspace_root = common::TestDir::new("forge-manual-review-reject-default");
    let harness = test_app(workspace_root.path()).await;

    let (task_id, _review_id) = seed_awaiting_human_review(
        &harness.state.db,
        r#"{"retry_budgets":{"review":3,"merge_fix":1}}"#,
        workspace_root.path(),
    )
    .await;

    let result: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/actions"),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{task_id}/actions")).await, "action": {"verb":"send_back","guidance":"Return for revisions"} }),
        StatusCode::OK,
    )
    .await;

    let decision_review = ReviewRepo::list_by_task(&*harness.state.db, &result.id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|review| review.attempt_number)
        .expect("decision Review persists");
    assert_eq!(decision_review.status, db::ReviewStatus::Failed);
    assert_eq!(result.status, "in_progress".to_owned());
}

#[tokio::test]
async fn reset_retry_window_allows_human_rejection_to_schedule_fresh_follow_up() {
    let workspace_root = common::TestDir::new("forge-manual-review-reset-retry-window");
    let harness = test_app(workspace_root.path()).await;

    let (task_id, _review_id) = seed_awaiting_human_review(
        &harness.state.db,
        r#"{"retry_budgets":{"review":2,"merge_fix":1}}"#,
        workspace_root.path(),
    )
    .await;

    for reason in ["first rejection", "second rejection"] {
        TransitionLogRepo::insert(
            &*harness.state.db,
            CreateTransitionLog {
                id: new_uuid_v4(),
                task_id: task_id.clone(),
                from_state: "review".to_owned(),
                to_state: "in_progress".to_owned(),
                trigger_name: Some("reject".to_owned()),
                triggered_by: "test".to_owned(),
                bridge: Default::default(),
                trigger_reason: reason.to_owned(),
                hook_results_json: None,
                rejection: true,
                created_at: now_rfc3339(),
            },
        )
        .await
        .expect("historical rejection log inserts");
    }

    let exhausted = harness
        .state
        .task_service
        .remaining_retries(&task_id)
        .await
        .expect("retry budget resolves");
    assert_eq!(
        exhausted, 0,
        "the historical review rejections exhaust the window"
    );

    harness
        .state
        .task_service
        .test_apply_action(
            task_id.clone(),
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: None,
                refresh_workspace: None,
                reset_budget: Some(true),
                guidance: None,
            },
            Some("start a fresh review window".to_owned()),
            None,
        )
        .await
        .expect("retry window reset succeeds");

    let result: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/actions"),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{task_id}/actions")).await, "action": {"verb":"send_back","guidance":"request one more pass"}}),
        StatusCode::OK,
    )
    .await;

    let decision_review = ReviewRepo::list_by_task(&*harness.state.db, &result.id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|review| review.attempt_number)
        .expect("decision Review persists");
    assert_eq!(decision_review.status, db::ReviewStatus::Failed);
    assert_eq!(result.status, "in_progress");
    assert_eq!(
        result.remaining_retries.get("review"),
        Some(&2),
        "owner send-back preserves the freshly reset agent budget"
    );
    assert_eq!(
        harness
            .state
            .task_service
            .remaining_retries(&task_id)
            .await
            .expect("post-rejection retry budget resolves"),
        2,
        "owner rejection leaves the freshly reset agent budget untouched"
    );
    // The send-back commits the coder follow-up as queued dispatch intent and
    // leaves the in_progress entry's hooks step pending; that step fences
    // dispatch until it settles. This fixture has no background step worker
    // or dispatcher, so do what production does: the worker drains the
    // Task's step queue (the entry hooks step), then the dispatcher, woken
    // when that step settles, dispatches the queued follow-up.
    harness
        .state
        .task_service
        .drain(&task_id)
        .await
        .expect("the send-back's entry hooks step settles");
    assert!(
        harness
            .state
            .task_service
            .test_dispatch_task_action(&task_id)
            .await
            .expect("the accepted human send-back dispatches its fresh follow-up"),
        "the queued follow-up dispatches once the entry hooks settle"
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*harness.state.db, &task_id, "coder")
            .await
            .expect("coder execution count loads"),
        2,
        "human rejection after reset must dispatch a fresh coder follow-up; task={:?}; steps={:?}",
        db::TaskRepo::get_by_id(&*harness.state.db, &task_id, false)
            .await
            .unwrap(),
        db::TaskStepRepo::task_steps(&*harness.state.db, &task_id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn approve_review_returns_409_when_not_awaiting_human() {
    let workspace_root = common::TestDir::new("forge-manual-review-409");
    let harness = test_app(workspace_root.path()).await;

    let task_id = seed_review_with_status(&harness.state.db, ReviewStatus::Passed).await;

    let response = raw_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/actions"),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{task_id}/actions")).await, "action": {"verb":"approve","override":false} }),
    )
    .await;

    // Returns 409 because the review status doesn't match the expected state
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

// ── Test 13.4: approve + full cascade with real git ──

#[tokio::test]
async fn approve_review_cascades_via_merge() {
    let temp = common::TestDir::new("forge-approve-cascade");
    let repo_path = temp.path().join("repo");
    std::fs::create_dir_all(&repo_path).expect("create repo dir");
    run_git(&repo_path, &["init", "--initial-branch=main"]);
    run_git(&repo_path, &["config", "user.email", "test@test.com"]);
    run_git(&repo_path, &["config", "user.name", "Test"]);
    std::fs::write(repo_path.join("file.txt"), "base\n").expect("write file");
    run_git(&repo_path, &["add", "."]);
    run_git(&repo_path, &["commit", "-m", "initial"]);

    let task_id = new_uuid_v4();
    let worktree_path = temp.path().join("worktrees").join(&task_id).join("repo");
    std::fs::create_dir_all(worktree_path.parent().unwrap()).expect("create worktree parent");
    run_git(
        &repo_path,
        &[
            "worktree",
            "add",
            worktree_path.to_str().unwrap(),
            "-b",
            &::workspace::task_branch_name(&task_id),
        ],
    );
    std::fs::write(worktree_path.join("feature.txt"), "hello\n").expect("write feature");
    run_git(&worktree_path, &["add", "."]);
    run_git(&worktree_path, &["commit", "-m", "feature"]);

    let workspace_root = common::TestDir::new("forge-approve-cascade-workspaces");
    let harness = test_app(workspace_root.path()).await;

    let (seeded_task_id, _review_id) = seed_awaiting_human_review_with_workspace(
        &harness.state.db,
        &task_id,
        &repo_path,
        &worktree_path,
    )
    .await;

    let result: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{seeded_task_id}/actions"),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{seeded_task_id}/actions")).await, "action": {"verb":"approve","override":false} }),
        StatusCode::OK,
    )
    .await;

    let decision_review = ReviewRepo::list_by_task(&*harness.state.db, &result.id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|review| review.attempt_number)
        .expect("decision Review persists");
    assert_eq!(decision_review.status, db::ReviewStatus::Passed);
    let settled = common::drain(&harness.state, &harness.app, &result.id).await;
    assert_eq!(settled.status, "done".to_owned());

    // Verify system comment was created
    let comments: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{seeded_task_id}/comments"),
        StatusCode::OK,
    )
    .await;
    let system_comments: Vec<_> = comments
        .items
        .iter()
        .filter(|c| c.author_type == AuthorType::System)
        .collect();
    assert!(
        system_comments
            .iter()
            .any(|c| c.content.contains("Review passed")),
        "expected 'Review passed' comment, got: {:?}",
        system_comments
            .iter()
            .map(|c| &c.content)
            .collect::<Vec<_>>()
    );
    assert!(
        system_comments
            .iter()
            .any(|c| c.content.contains("Changes merged to")),
        "expected 'Changes merged' comment, got: {:?}",
        system_comments
            .iter()
            .map(|c| &c.content)
            .collect::<Vec<_>>()
    );
}
