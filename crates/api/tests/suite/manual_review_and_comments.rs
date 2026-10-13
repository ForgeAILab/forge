#![allow(dead_code, clippy::assertions_on_constants)]
use crate::common;

use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use api::{build_router, AppState};
use api_types::{
    AuthorType, CommentResponse, PaginatedResponse, ProjectResponse, RepoResponse, TaskResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    Router,
};
use db::{
    new_uuid_v4, now_rfc3339, AgentRepo, AgentStatus, AssigneeKind, CommentAuthorType, CreateAgent,
    CreateExecution, CreateProject, CreateRepo, CreateReview, CreateTask, CreateTaskComment,
    CreateTaskRoleAssignment, CreateTransitionLog, CreateWorkspace, DaemonRepo, DaemonStatus,
    ExecutionRepo, ExecutionStatus, ProjectRepo, RepoRepo, ReviewRepo, ReviewStatus,
    TaskCommentRepo, TaskRepo, TaskRoleAssignmentRepo, TransitionLogRepo, UpdateProject,
    UpdateTask, UpsertDaemon, WorkspaceStatus,
};
use events::EventBus;
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind,
};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::process::Stdio;
use tower::ServiceExt;

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

// ── Test 13.6: comment CRUD ──

#[tokio::test]
async fn comment_create_list_delete() {
    let workspace_root = common::TestDir::new("forge-comments-crud");
    let harness = test_app(workspace_root.path()).await;
    let (repo_path, _) = setup_git_repo(workspace_root.path(), "comments");

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Comment Test" }),
        StatusCode::OK,
    )
    .await;

    let _repo: RepoResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "repo",
            "local_path": repo_path.to_string_lossy(),
            "remote_url": repo_path.to_string_lossy(),
            "default_branch": "main"
        }),
        StatusCode::OK,
    )
    .await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({ "title": "Comment test task",
            "description": "test"
        }),
        StatusCode::OK,
    )
    .await;

    // Create a user comment
    let comment: CommentResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/comments", task.id),
        json!({ "content": "Hello world", "author_name": "Tester" }),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(comment.content, "Hello world");
    assert_eq!(comment.author_name, "Tester");
    assert_eq!(comment.author_type, AuthorType::User);

    // List comments
    let listed: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/comments", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(listed.items.len(), 1);
    assert_eq!(listed.items[0].content, "Hello world");

    // Delete user comment
    let delete_response = raw_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/comments/{}", comment.id),
        json!(null),
    )
    .await;
    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

    // Verify it's gone
    let listed: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/comments", task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(listed.items.len(), 0);
}

#[tokio::test]
async fn system_comments_cannot_be_deleted() {
    let workspace_root = common::TestDir::new("forge-comments-system-delete");
    let harness = test_app(workspace_root.path()).await;

    let task_id = seed_task_with_system_comment(&harness.state.db).await;

    let listed: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/comments"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(listed.items.len(), 1);
    let system_comment_id = &listed.items[0].id;

    let delete_response = raw_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/comments/{system_comment_id}"),
        json!(null),
    )
    .await;
    assert_eq!(delete_response.status(), StatusCode::FORBIDDEN);
}

// ── Test 13.7: system comments auto-created ──

#[tokio::test]
async fn reject_review_creates_system_comment_with_reason() {
    let workspace_root = common::TestDir::new("forge-sys-comment-reject");
    let harness = test_app(workspace_root.path()).await;

    let (task_id, _review_id) = seed_awaiting_human_review(
        &harness.state.db,
        r#"{"retry_budgets":{"review":3,"merge_fix":1}}"#,
        workspace_root.path(),
    )
    .await;

    let _result: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/actions"),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{task_id}/actions")).await, "action": {"verb":"send_back","guidance":"needs refactoring"}}),
        StatusCode::OK,
    )
    .await;

    let comments: PaginatedResponse<CommentResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/comments"),
        StatusCode::OK,
    )
    .await;

    let system_comments: Vec<_> = comments
        .items
        .iter()
        .filter(|c| c.author_type == AuthorType::System)
        .collect();
    assert!(
        system_comments.iter().any(
            |c| c.content.contains("Review failed") && c.content.contains("needs refactoring")
        ),
        "expected system comment with rejection reason, got: {:?}",
        system_comments
            .iter()
            .map(|c| &c.content)
            .collect::<Vec<_>>()
    );
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

/// The owner's Retry of a review entry, end to end through REST, when the
/// server's login shell gives no environment and the review-CI
/// infrastructure budget runs out on that entry. The Retry re-enters
/// `review`; its entry cannot identify its check, asks its bounded number of
/// times and states the interruption; the budget is spent, so the Task
/// parks with the typed exhausted cause and offers Retry (nothing re-enters
/// by itself any more). Once the shell answers, that Retry runs the review:
/// one check run, and a Review with its CI result that waits for the person.
/// The Task has no Agent (a person reviews it): the offered Retry Entry
/// Checks used to be refused with "accepted role action lost its Agent".
#[tokio::test]
async fn the_owners_retry_of_a_review_entry_parks_on_the_exhausted_ci_budget_and_the_offered_retry_runs_the_review(
) {
    let temp = common::TestDir::new("forge-review-entry-retry");
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

    let workspace_root = common::TestDir::new("forge-review-entry-retry-workspaces");
    let harness = test_app(workspace_root.path()).await;
    let (task_id, _) = seed_awaiting_human_review_with_workspace(
        &harness.state.db,
        &task_id,
        &repo_path,
        &worktree_path,
    )
    .await;
    set_review_ci_steps(&harness.state.db, &task_id, &["true"]).await;
    let database = harness.state.db.clone();
    let actions = format!("/api/v1/tasks/{task_id}/actions");
    services::check_runner::policy::silence_login_shell_for_test(&database, true)
        .await
        .unwrap();
    let retry = |app: axum::Router, actions: String| async move {
        let version = common::task_action_version(&app, &actions).await;
        let _: TaskResponse = json_request(
            &app,
            Method::POST,
            &actions,
            json!({"version": version, "action": {"verb": "retry"}}),
            StatusCode::OK,
        )
        .await;
    };
    let reviews_before = ReviewRepo::list_by_task(&*database, &task_id)
        .await
        .unwrap()
        .len();

    retry(harness.app.clone(), actions.clone()).await;
    harness
        .state
        .task_service
        .test_dispatch_task_action(&task_id)
        .await
        .unwrap();
    let interrupted = common::drain(&harness.state, &harness.app, &task_id).await;
    assert_eq!(interrupted.status, "review");
    let stated = |database: std::sync::Arc<db::SqliteDb>, task_id: String| async move {
        let task = TaskRepo::get_by_id(&*database, &task_id, false)
            .await
            .unwrap()
            .unwrap();
        let annotation: serde_json::Value = serde_json::from_str(
            task.error_annotation
                .as_deref()
                .expect("the interruption is stated"),
        )
        .unwrap();
        (task, annotation)
    };
    // The owner's Retry opened a new episode: its first interruption is
    // stated with a re-entry the Task makes by itself.
    let (task, annotation) = stated(database.clone(), task_id.clone()).await;
    assert_eq!(
        annotation["blocking_reason"], "review_ci_infrastructure",
        "{annotation}"
    );
    let metadata: serde_json::Value =
        serde_json::from_str(task.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(metadata["deferred_dispatch"]["target_state"], "review");
    let limit = i64::from(db::budget::Kind::ReviewCiInfrastructure.default_limit());
    assert_eq!(
        db::budget::spent(
            database.pool(),
            &task_id,
            db::budget::Kind::ReviewCiInfrastructure.key()
        )
        .await
        .unwrap(),
        1
    );
    // The episode's later re-entries failed the same way, up to the last
    // one the budget allows; that one is due now and the dispatcher makes
    // it.
    sqlx::query(
        "UPDATE task_budget SET spent=? WHERE task_id=? AND kind='review_ci_infrastructure'",
    )
    .bind(limit - 1)
    .bind(&task_id)
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE task SET metadata_json=json_set(metadata_json,'$.deferred_dispatch.not_before','2020-01-01T00:00:00+00:00') WHERE id=?")
        .bind(&task_id)
        .execute(database.pool())
        .await
        .unwrap();
    let dispatcher = services::TaskDispatcher::new(
        harness.state.db.clone(),
        harness.state.event_bus.clone(),
        harness.state.task_service.clone(),
    );
    dispatcher.check_once().await.unwrap();
    let parked = common::drain(&harness.state, &harness.app, &task_id).await;
    assert_eq!(parked.status, "review");
    let (task, annotation) = stated(database.clone(), task_id.clone()).await;
    assert_eq!(
        annotation["blocking_reason"], "review_ci_infrastructure_exhausted",
        "{annotation}"
    );
    assert!(
        annotation["message"]
            .as_str()
            .unwrap()
            .contains("login shell did not report"),
        "{annotation}"
    );
    let metadata: serde_json::Value =
        serde_json::from_str(task.metadata_json.as_deref().unwrap_or("{}")).unwrap();
    assert!(
        metadata.get("deferred_dispatch").is_none(),
        "an exhausted budget does not re-enter by itself: {metadata}"
    );
    // No command ran: no Review was added, no check exists.
    let reviews = ReviewRepo::list_by_task(&*database, &task_id)
        .await
        .unwrap();
    assert!(
        reviews.len() <= reviews_before
            && reviews.iter().all(|review| !matches!(
                review.status,
                db::ReviewStatus::Running | db::ReviewStatus::Passed | db::ReviewStatus::Failed
            )),
        "{:?}",
        reviews
            .iter()
            .map(|review| (review.attempt_number, review.status.clone()))
            .collect::<Vec<_>>()
    );
    let checks: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM check_run)+(SELECT COUNT(*) FROM check_consumer)",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(checks, 0);
    // The offer the owner sees through REST.
    let offers: api_types::TaskActionsResponse = common::parse_response(
        common::raw_empty_request(&harness.app, Method::GET, &actions).await,
        StatusCode::OK,
    )
    .await;
    assert!(
        offers
            .available_actions
            .iter()
            .any(|offer| offer.action.verb() == "retry"),
        "Retry is offered on the exhausted park: {:?}",
        offers
            .available_actions
            .iter()
            .map(|offer| (offer.action.verb().to_owned(), offer.reason.clone()))
            .collect::<Vec<_>>()
    );

    // The shell answers again; the offered Retry runs the review.
    services::check_runner::policy::silence_login_shell_for_test(&database, false)
        .await
        .unwrap();
    retry(harness.app.clone(), actions.clone()).await;
    harness
        .state
        .task_service
        .test_dispatch_task_action(&task_id)
        .await
        .unwrap();
    let settled = common::drain(&harness.state, &harness.app, &task_id).await;
    let review = ReviewRepo::list_by_task(&*database, &task_id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|review| review.attempt_number)
        .unwrap();
    // The entry ran its CI and the Review waits for the person, as the
    // Task's review did before the Retry.
    assert_eq!(review.status, db::ReviewStatus::AwaitingHuman);
    let details: serde_json::Value = serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(details["ci_steps"][0]["exit_code"], 0, "{details}");
    assert_eq!(settled.status, "review");
    let task = TaskRepo::get_by_id(&*database, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.error_annotation, None, "the park is lifted");
    let runs: Vec<String> = sqlx::query_scalar("SELECT state FROM check_run")
        .fetch_all(database.pool())
        .await
        .unwrap();
    assert_eq!(runs, vec!["succeeded".to_owned()], "one check, run once");
}

#[tokio::test]
async fn rerun_review_pass_cascades_through_merge_instead_of_parking() {
    let temp = common::TestDir::new("forge-rerun-review-pass");
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

    let workspace_root = common::TestDir::new("forge-rerun-review-pass-workspaces");
    let harness = test_app(workspace_root.path()).await;
    let (task_id, _) = seed_awaiting_human_review_with_workspace(
        &harness.state.db,
        &task_id,
        &repo_path,
        &worktree_path,
    )
    .await;
    set_review_ci_steps(&harness.state.db, &task_id, &["true"]).await;

    let accepted: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/actions"),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{task_id}/actions")).await,"action":{"verb":"retry"}}),
        StatusCode::OK,
    )
    .await;

    assert!(accepted.condition.details().diagnostic.is_none());
    harness
        .state
        .task_service
        .test_dispatch_task_action(&task_id)
        .await
        .unwrap();
    // The retry only re-enters `review`: its checks and verdict come from the
    // durable entry that the Task's own steps run.
    let settled = common::drain(&harness.state, &harness.app, &task_id).await;
    let result_review = ReviewRepo::list_by_task(&*harness.state.db, &task_id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|review| review.attempt_number)
        .unwrap();

    assert_eq!(result_review.status, db::ReviewStatus::Passed);
    assert_eq!(result_review.attempt_number, 2);
    assert_eq!(settled.status, "done");
    assert_no_synthetic_reviewer_execution(&harness.state.db, &task_id).await;
}

#[tokio::test]
async fn rerun_review_failure_returns_to_remediation_instead_of_parking() {
    let temp = common::TestDir::new("forge-rerun-review-fail");
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

    let workspace_root = common::TestDir::new("forge-rerun-review-fail-workspaces");
    let harness = test_app(workspace_root.path()).await;
    let (task_id, _) = seed_awaiting_human_review_with_workspace(
        &harness.state.db,
        &task_id,
        &repo_path,
        &worktree_path,
    )
    .await;
    set_review_ci_steps(&harness.state.db, &task_id, &["false"]).await;

    let accepted: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/actions"),
        json!({ "version": common::task_action_version(&harness.app, &format!("/api/v1/tasks/{task_id}/actions")).await,"action":{"verb":"retry"}}),
        StatusCode::OK,
    )
    .await;

    assert!(accepted.condition.details().diagnostic.is_none());
    harness
        .state
        .task_service
        .test_dispatch_task_action(&task_id)
        .await
        .unwrap();
    let result = common::drain(&harness.state, &harness.app, &task_id).await;
    let result_review = ReviewRepo::list_by_task(&*harness.state.db, &task_id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|review| review.attempt_number)
        .unwrap();

    assert_eq!(result_review.status, db::ReviewStatus::Failed);
    assert_eq!(result.status, "in_progress");
    assert!(result.review_passed_at.is_none());
    assert_no_synthetic_reviewer_execution(&harness.state.db, &task_id).await;
    // The owner asked for this entry through the Task action: its failure
    // and its bounce to remediation spend no review retry budget.
    assert_eq!(
        review_budget_spent(&harness.state.db, &task_id).await,
        (0, 0),
        "an owner's failed review re-run spends nothing"
    );
    let requested_by: serde_json::Value = serde_json::from_str(
        &sqlx::query_scalar::<_, String>(
            "SELECT bridge_payload FROM transition_log WHERE task_id = ? AND from_state = 'review' AND to_state = 'review'",
        )
        .bind(&task_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("the re-entry is logged"),
    )
    .expect("bridge payload parses");
    assert_eq!(requested_by["verb"], "review_rerun");
    assert!(
        serde_json::from_value::<api_types::Actor>(requested_by["requested_by"].clone())
            .expect("the requester is recorded")
            .is_owner(),
        "{requested_by}"
    );
}

async fn review_budget_spent(db: &db::SqliteDb, task_id: &str) -> (i64, i64) {
    (
        db::budget::spent(db.pool(), task_id, "review")
            .await
            .expect("review budget reads"),
        db::budget::spent(db.pool(), task_id, db::budget::REVIEW_GATE_KEY)
            .await
            .expect("review gate budget reads"),
    )
}

// The owner retries the review of an unchanged commit whose checks already
// passed. The checks are answered by that certified result (no second run),
// but the review that was asked for is: the assigned reviewer Agent is
// dispatched for the new attempt. A cache hit never stands in for a reviewer.
#[tokio::test]
async fn review_retry_on_an_unchanged_commit_reuses_green_ci_and_dispatches_the_reviewer_again() {
    // The reviewer Agent is paused for the first retry: its checks run and no
    // reviewer starts, so the Task is idle in `review` with a green result.
    let (harness, task_id, reviewer, _dirs) =
        seed_rerun_task("forge-rerun-review-hit-reviewer", Some(true)).await;
    let reviewer = reviewer.unwrap();
    let db = &harness.state.db;
    let dispatcher = harness
        .state
        .task_dispatcher
        .as_ref()
        .expect("the harness composes a dispatcher");
    post_retry(&harness, &task_id, StatusCode::OK).await;
    harness
        .state
        .task_service
        .test_dispatch_task_action(&task_id)
        .await
        .unwrap();
    common::drain(&harness.state, &harness.app, &task_id).await;
    dispatcher.check_once().await.expect("dispatcher pass");
    common::drain(&harness.state, &harness.app, &task_id).await;
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM check_run WHERE ? IS NOT NULL",
            &task_id
        )
        .await,
        1
    );
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM execution WHERE task_id = ? AND role = 'reviewer'",
            &task_id
        )
        .await,
        0
    );

    AgentRepo::set_paused(&**db, &reviewer, false)
        .await
        .expect("reviewer resumes");
    harness
        .state
        .task_service
        .rerun_review(
            task_id.parse().expect("Task id is a UUID"),
            api_types::Actor::user(api_types::UserActionSource::Api),
        )
        .await
        .expect("the idle review entry can be asked for again");
    common::drain(&harness.state, &harness.app, &task_id).await;
    dispatcher.check_once().await.expect("dispatcher pass");
    let settled = common::drain(&harness.state, &harness.app, &task_id).await;
    assert_eq!(settled.status, "review");

    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM check_consumer WHERE task_id = ?",
            &task_id
        )
        .await,
        2,
        "each retried entry asked the check runner"
    );
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM check_run WHERE ? IS NOT NULL",
            &task_id
        )
        .await,
        1,
        "the second was answered by the first one's result: nothing ran again"
    );
    let reviewers: Vec<(Option<String>, String)> = sqlx::query_as(
        "SELECT agent_id, status FROM execution WHERE task_id = ? AND role = 'reviewer'",
    )
    .bind(&task_id)
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        reviewers.len(),
        1,
        "the reviewer is dispatched: {reviewers:?}"
    );
    assert_eq!(reviewers[0].0.as_deref(), Some(reviewer.as_str()));
    let mut reviews = ReviewRepo::list_by_task(&**db, &task_id).await.unwrap();
    reviews.sort_by_key(|review| review.attempt_number);
    assert_eq!(reviews.len(), 3);
    assert_eq!(
        reviews[1].status,
        ReviewStatus::Cancelled,
        "the replaced attempt is closed"
    );
    assert_eq!(reviews[2].status, ReviewStatus::Running);
    assert!(
        reviews[2].reviewer_execution_id.is_some(),
        "the new attempt is the one the reviewer works on"
    );
    let details: serde_json::Value = serde_json::from_str(&reviews[2].step_results_json).unwrap();
    assert_eq!(details["ci_steps"][0]["exit_code"], 0);
    assert_no_synthetic_reviewer_execution(db, &task_id).await;
}

// A Task with a reviewer Agent, whose checks are red. The owner's retry queues
// the reviewer launch that follows a green entry; the entry fails and the
// Task goes to remediation. That launch belonged to the entry that ended: it
// is dropped, and the Task is not parked on "stale queued Task action".
#[tokio::test]
async fn review_retry_with_a_reviewer_and_red_checks_goes_to_remediation_without_a_stale_action() {
    let (harness, task_id, _reviewer, _dirs) =
        seed_rerun_task("forge-rerun-review-reviewer-red", Some(false)).await;
    let db = &harness.state.db;
    set_review_ci_steps(db, &task_id, &["false"]).await;
    let dispatcher = harness
        .state
        .task_dispatcher
        .as_ref()
        .expect("the harness composes a dispatcher");

    post_retry(&harness, &task_id, StatusCode::OK).await;
    let mut settled = common::drain(&harness.state, &harness.app, &task_id).await;
    for _ in 0..6 {
        let _ = harness
            .state
            .task_service
            .test_dispatch_task_action(&task_id)
            .await;
        dispatcher.check_once().await.expect("dispatcher pass");
        settled = common::drain(&harness.state, &harness.app, &task_id).await;
    }

    assert_eq!(settled.status, "in_progress");
    let row: (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT metadata_json, error_annotation, blocked_json, failed_json FROM task WHERE id = ?",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(
        !format!("{row:?} {:?}", settled.condition).contains("stale queued Task action"),
        "{row:?} {:?}",
        settled.condition
    );
    assert!(
        !row.0
            .as_deref()
            .unwrap_or_default()
            .contains("queued_recovery"),
        "the launch queued for the ended entry is gone: {row:?}"
    );
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM execution WHERE task_id = ? AND role = 'reviewer'",
            &task_id
        )
        .await,
        0,
        "no reviewer runs for an entry whose checks failed"
    );
    assert_eq!(review_budget_spent(db, &task_id).await, (0, 0));
}

// The retried entry with an assigned reviewer that passes: checks, then the
// reviewer's verdict on the new attempt, then merging and done, exactly as an
// automatic entry ends. Nothing is spent.
#[tokio::test]
async fn review_retry_with_an_assigned_reviewer_that_passes_goes_on_to_merge() {
    let (harness, task_id, reviewer, _dirs) =
        seed_rerun_task_with_reviewer("forge-rerun-review-reviewer-pass", Some(false), true).await;
    let reviewer = reviewer.unwrap();
    let db = &harness.state.db;

    post_retry(&harness, &task_id, StatusCode::OK).await;
    harness
        .state
        .task_service
        .test_dispatch_task_action(&task_id)
        .await
        .unwrap();
    let dispatcher = harness
        .state
        .task_dispatcher
        .as_ref()
        .expect("the harness composes a dispatcher");
    // The reviewer runs on its own task; its verdict comes back as a step.
    let mut settled = common::drain(&harness.state, &harness.app, &task_id).await;
    for _ in 0..300 {
        if settled.status == "done" {
            break;
        }
        dispatcher.check_once().await.expect("dispatcher pass");
        tokio::time::sleep(Duration::from_millis(50)).await;
        settled = common::drain(&harness.state, &harness.app, &task_id).await;
    }
    assert_eq!(settled.status, "done", "{:?}", settled.condition);

    let mut reviews = ReviewRepo::list_by_task(&**db, &task_id).await.unwrap();
    reviews.sort_by_key(|review| review.attempt_number);
    assert_eq!(reviews.len(), 2, "one new attempt for the retried entry");
    assert_eq!(reviews[1].status, ReviewStatus::Passed);
    let details: serde_json::Value = serde_json::from_str(&reviews[1].step_results_json).unwrap();
    assert_eq!(details["ci_steps"][0]["exit_code"], 0);
    assert_eq!(details["conformance"]["status"], "passed", "{details}");
    let reviewers: Vec<(Option<String>, String)> = sqlx::query_as(
        "SELECT agent_id, status FROM execution WHERE task_id = ? AND role = 'reviewer'",
    )
    .bind(&task_id)
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        reviewers,
        vec![(Some(reviewer), "completed".to_owned())],
        "the reviewer ran once, as the Agent assigned"
    );
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM check_run WHERE ? IS NOT NULL",
            &task_id
        )
        .await,
        1
    );
    assert_eq!(review_budget_spent(db, &task_id).await, (0, 0));
    assert_no_synthetic_reviewer_execution(db, &task_id).await;
}

/// A Task in `review` with a worktree, green-able checks and, when asked, a
/// reviewer Agent assigned (paused or not).
async fn seed_rerun_task(
    name: &str,
    reviewer_paused: Option<bool>,
) -> (TestHarness, String, Option<String>, Vec<common::TestDir>) {
    seed_rerun_task_with_reviewer(name, reviewer_paused, false).await
}

async fn seed_rerun_task_with_reviewer(
    name: &str,
    reviewer_paused: Option<bool>,
    reviewer_passes: bool,
) -> (TestHarness, String, Option<String>, Vec<common::TestDir>) {
    let temp = common::TestDir::new(name);
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
    let workspace_root = common::TestDir::new(&format!("{name}-workspaces"));
    let harness = test_app_with_reviewer(workspace_root.path(), reviewer_passes).await;
    let (task_id, _) = seed_awaiting_human_review_with_workspace(
        &harness.state.db,
        &task_id,
        &repo_path,
        &worktree_path,
    )
    .await;
    set_review_ci_steps(&harness.state.db, &task_id, &["true"]).await;
    let reviewer = match reviewer_paused {
        None => None,
        Some(paused) => {
            let agent_id = seed_codex_agent(&harness.state.db).await;
            if paused {
                AgentRepo::set_paused(&*harness.state.db, &agent_id, true)
                    .await
                    .expect("reviewer pauses");
            }
            let now = now_rfc3339();
            TaskRoleAssignmentRepo::assign(
                &*harness.state.db,
                CreateTaskRoleAssignment {
                    id: new_uuid_v4(),
                    task_id: task_id.clone(),
                    role_name: "reviewer".to_owned(),
                    assignee_type: Some(AssigneeKind::Agent),
                    assignee_id: Some(agent_id.clone()),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await
            .expect("reviewer assignment creates");
            Some(agent_id)
        }
    };
    (harness, task_id, reviewer, vec![temp, workspace_root])
}

async fn count(db: &db::SqliteDb, sql: &str, task_id: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(task_id)
        .fetch_one(db.pool())
        .await
        .expect("count reads")
}

async fn post_retry(
    harness: &TestHarness,
    task_id: &str,
    expected: StatusCode,
) -> serde_json::Value {
    let uri = format!("/api/v1/tasks/{task_id}/actions");
    json_request(
        &harness.app,
        Method::POST,
        &uri,
        json!({ "version": common::task_action_version(&harness.app, &uri).await, "action": {"verb": "retry"} }),
        expected,
    )
    .await
}

/// Start from the shared Git fixture, retaining only the human branch and
/// configured validation. No implementation or Review record is supplied.
async fn human_submission_fixture(
    name: &str,
    commits: bool,
    reviewer_passes: bool,
) -> (TestHarness, String, Vec<common::TestDir>) {
    let (harness, task_id, _, dirs) =
        seed_rerun_task_with_reviewer(name, Some(false), reviewer_passes).await;
    let db = &harness.state.db;
    sqlx::query("DELETE FROM review WHERE task_id=?")
        .bind(&task_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM execution WHERE task_id=?")
        .bind(&task_id)
        .execute(db.pool())
        .await
        .unwrap();
    let workspace = db::WorkspaceRepo::get_by_task_id(&**db, &task_id)
        .await
        .unwrap()
        .unwrap();
    let repo = RepoRepo::get_by_id(&**db, &workspace.repo_id)
        .await
        .unwrap()
        .unwrap();
    let base = git::get_current_sha(Path::new(repo.local_path.as_deref().unwrap()))
        .await
        .unwrap();
    if !commits {
        run_git(
            Path::new(workspace.embedded_worktree_path_for_backend()),
            &[
                "update-ref",
                &format!("refs/heads/{}", workspace.branch),
                &base,
            ],
        );
    }
    sqlx::query("UPDATE workspace SET before_sha=? WHERE id=?")
        .bind(base)
        .bind(workspace.id)
        .execute(db.pool())
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&**db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::update_status(
        &**db,
        db::UpdateTaskStatus {
            id: task_id.clone(),
            expected_version: task.version,
            status: "in_progress".into(),
            assignee_id: None,
            error_annotation: Some(None),
            blocked_json: Some(None),
            failed_json: Some(None),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    (harness, task_id, dirs)
}

#[tokio::test]
async fn review_without_candidate_owner_actions_work_over_http() {
    for verb in ["approve", "approve_override", "send_back", "cancel"] {
        let (harness, task_id, _dirs) =
            human_submission_fixture(&format!("forge-review-no-candidate-{verb}"), false, false)
                .await;
        let uri = format!("/api/v1/tasks/{task_id}/actions");
        let _: TaskResponse = json_request(&harness.app, Method::POST, &uri,
            json!({"version":common::task_action_version(&harness.app,&uri).await,
                "action":{"verb":"approve","override":true,"reason":"Submit human work for review"}}), StatusCode::OK).await;
        let parked = common::drain(&harness.state, &harness.app, &task_id).await;
        assert_eq!(parked.status, "review");
        assert!(
            parked
                .condition
                .details()
                .diagnostic
                .as_ref()
                .is_some_and(|annotation| annotation.blocking_reason == "review_candidate_missing"),
            "{:?}",
            parked.condition
        );
        assert!(ReviewRepo::list_by_task(&*harness.state.db, &task_id)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            count(
                &harness.state.db,
                "SELECT COUNT(*) FROM check_consumer WHERE task_id=?",
                &task_id
            )
            .await,
            0,
            "missing work is no check verdict"
        );
        let action = match verb {
            "send_back" => {
                json!({"verb":"send_back","guidance":"Implement and commit the missing work"})
            }
            "cancel" => json!({"verb":"cancel"}),
            _ => {
                json!({"verb":"approve","override":verb == "approve_override","reason":"Owner decides the empty review"})
            }
        };
        let result: serde_json::Value = json_request(
            &harness.app,
            Method::POST,
            &uri,
            json!({"version":common::task_action_version(&harness.app,&uri).await,"action":action}),
            StatusCode::OK,
        )
        .await;
        // The owner's Approve is a deliberate decision and keeps working: it
        // passes the gate into merging and the park is gone.
        assert_eq!(
            result["status"],
            match verb {
                "cancel" => "cancelled",
                "send_back" => "in_progress",
                _ => "merging",
            },
            "{verb}: {result}"
        );
        let after = common::drain(&harness.state, &harness.app, &task_id).await;
        assert!(
            !after
                .condition
                .details()
                .diagnostic
                .as_ref()
                .is_some_and(|annotation| annotation.blocking_reason == "review_candidate_missing"),
            "{verb}: {:?}",
            after.condition
        );
    }
}

#[tokio::test]
async fn review_of_human_branch_submission_runs_checks_and_reviewer_before_merge() {
    let (harness, task_id, _dirs) =
        human_submission_fixture("forge-human-review-candidate", true, true).await;
    let uri = format!("/api/v1/tasks/{task_id}/actions");
    let _: TaskResponse = json_request(&harness.app, Method::POST, &uri,
        json!({"version":common::task_action_version(&harness.app,&uri).await,
            "action":{"verb":"approve","override":true,"reason":"Submit human commits for review"}}), StatusCode::OK).await;
    let mut settled = common::drain(&harness.state, &harness.app, &task_id).await;
    for _ in 0..300 {
        if settled.status == "done" {
            break;
        }
        harness
            .state
            .task_dispatcher
            .as_ref()
            .unwrap()
            .check_once()
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        settled = common::drain(&harness.state, &harness.app, &task_id).await;
    }
    assert_eq!(settled.status, "done", "{:?}", settled.condition);
    let reviews = ReviewRepo::list_by_task(&*harness.state.db, &task_id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Passed);
    let details: serde_json::Value = serde_json::from_str(&reviews[0].step_results_json).unwrap();
    assert_eq!(details["ci_steps"][0]["exit_code"], 0, "{details}");
    assert_eq!(details["conformance"]["status"], "passed", "{details}");
    assert_eq!(
        count(
            &harness.state.db,
            "SELECT COUNT(*) FROM check_run WHERE ? IS NOT NULL",
            &task_id
        )
        .await,
        1
    );
    let candidate = ExecutionRepo::get_by_id(&*harness.state.db, &reviews[0].execution_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        candidate.agent_id.is_none()
            && candidate.lease_owner.is_none()
            && candidate.logs_path.is_none()
    );
    assert!(candidate
        .summary
        .unwrap()
        .contains("human branch submission"));
    let reviewed = db::ReviewConformanceRepo::review_contract(
        &*harness.state.db,
        reviews[0].reviewer_execution_id.as_deref().unwrap(),
    )
    .await
    .unwrap();
    let reviewed = reviewed.unwrap();
    assert_ne!(reviewed.commit_sha, reviewed.base_sha);
    assert_eq!(
        candidate.after_sha.as_deref(),
        Some(reviewed.commit_sha.as_str())
    );
    assert_eq!(count(&harness.state.db, "SELECT COUNT(*) FROM execution WHERE task_id=? AND agent_id IS NOT NULL AND role='coder'", &task_id).await, 0);
    assert_eq!(count(&harness.state.db, "SELECT COUNT(*) FROM execution WHERE task_id=? AND role='reviewer' AND status='completed'", &task_id).await, 1);
}

// With a reviewer assigned, the owner's review retry runs the entry's CI once
// through the check runner and then dispatches the reviewer as an ordinary,
// Agent-backed reviewer execution. While that reviewer works, a second retry
// starts nothing: the action is not offered and is refused 409.
#[tokio::test]
async fn review_retry_runs_entry_ci_once_then_the_assigned_reviewer_and_refuses_a_second_while_busy(
) {
    let (harness, task_id, reviewer, _dirs) =
        seed_rerun_task("forge-rerun-review-reviewer", Some(false)).await;
    let reviewer = reviewer.unwrap();
    let db = &harness.state.db;

    post_retry(&harness, &task_id, StatusCode::OK).await;
    harness
        .state
        .task_service
        .test_dispatch_task_action(&task_id)
        .await
        .unwrap();
    common::drain(&harness.state, &harness.app, &task_id).await;
    // The entry leaves reviewer dispatch to the Task dispatcher, as every
    // review entry does.
    harness
        .state
        .task_dispatcher
        .as_ref()
        .expect("the harness composes a dispatcher")
        .check_once()
        .await
        .expect("dispatcher pass");
    let settled = common::drain(&harness.state, &harness.app, &task_id).await;
    assert_eq!(settled.status, "review");

    let reviews = ReviewRepo::list_by_task(&**db, &task_id).await.unwrap();
    let latest = reviews.iter().max_by_key(|r| r.attempt_number).unwrap();
    assert_eq!(reviews.len(), 2, "one new attempt for the retried entry");
    assert_eq!(latest.status, ReviewStatus::Running);
    let details: serde_json::Value = serde_json::from_str(&latest.step_results_json).unwrap();
    assert_eq!(details["ci_steps"][0]["exit_code"], 0);
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM check_consumer WHERE task_id = ?",
            &task_id
        )
        .await,
        1,
        "the entry asked the check runner once"
    );
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM check_run WHERE ? IS NOT NULL",
            &task_id
        )
        .await,
        1,
        "and one run answered"
    );
    let reviewers: Vec<(Option<String>, String)> = sqlx::query_as(
        "SELECT agent_id, status FROM execution WHERE task_id = ? AND role = 'reviewer'",
    )
    .bind(&task_id)
    .fetch_all(db.pool())
    .await
    .unwrap();
    let entry_log: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT to_state, trigger_reason, hook_results_json FROM transition_log WHERE task_id = ? ORDER BY created_at",
    )
    .bind(&task_id)
    .fetch_all(db.pool())
    .await
    .unwrap();
    let task_row: (Option<String>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT metadata_json, error_annotation, blocked_json FROM task WHERE id = ?",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        reviewers.len(),
        1,
        "the reviewer runs once: {reviewers:?}; entries {entry_log:?}; task {task_row:?}"
    );
    assert_eq!(reviewers[0].0.as_deref(), Some(reviewer.as_str()));
    assert_no_synthetic_reviewer_execution(db, &task_id).await;

    // Busy: the reviewer is still working.
    let offers: serde_json::Value = json_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/actions"),
        json!(null),
        StatusCode::OK,
    )
    .await;
    assert!(
        !offers["available_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|offer| offer["action"]["verb"] == "retry"),
        "{offers}"
    );
    let refused = post_retry(&harness, &task_id, StatusCode::CONFLICT).await;
    assert_eq!(refused["code"], "action_unavailable", "{refused}");
    assert_eq!(
        ReviewRepo::list_by_task(&**db, &task_id)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM check_consumer WHERE task_id = ?",
            &task_id
        )
        .await,
        1
    );
}

// A paused reviewer Agent holds the retried entry where it holds an automatic
// one: the checks run and are recorded, and no reviewer starts.
#[tokio::test]
async fn review_retry_with_a_paused_reviewer_runs_its_checks_and_starts_no_reviewer() {
    let (harness, task_id, _reviewer, _dirs) =
        seed_rerun_task("forge-rerun-review-paused-agent", Some(true)).await;
    let db = &harness.state.db;

    post_retry(&harness, &task_id, StatusCode::OK).await;
    harness
        .state
        .task_service
        .test_dispatch_task_action(&task_id)
        .await
        .unwrap();
    common::drain(&harness.state, &harness.app, &task_id).await;
    // The entry leaves reviewer dispatch to the Task dispatcher, as every
    // review entry does.
    harness
        .state
        .task_dispatcher
        .as_ref()
        .expect("the harness composes a dispatcher")
        .check_once()
        .await
        .expect("dispatcher pass");
    let settled = common::drain(&harness.state, &harness.app, &task_id).await;
    assert_eq!(settled.status, "review");

    let reviews = ReviewRepo::list_by_task(&**db, &task_id).await.unwrap();
    let latest = reviews.iter().max_by_key(|r| r.attempt_number).unwrap();
    assert_eq!(reviews.len(), 2);
    assert_eq!(latest.status, ReviewStatus::Running);
    let details: serde_json::Value = serde_json::from_str(&latest.step_results_json).unwrap();
    assert_eq!(details["ci_steps"][0]["exit_code"], 0);
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM execution WHERE task_id = ? AND role = 'reviewer' AND status = 'running'",
            &task_id
        )
        .await,
        0,
        "a paused Agent starts no reviewer"
    );
    assert_no_synthetic_reviewer_execution(db, &task_id).await;
}

async fn set_review_ci_steps(db: &db::SqliteDb, task_id: &str, steps: &[&str]) {
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await
        .expect("Task lookup")
        .expect("Task exists");
    TaskRepo::update(
        db,
        UpdateTask {
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
            task_state_config: Some(Some(json!({"review":{"ci_steps":steps}}).to_string())),
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("review CI config updates");
}

/// A review-check retry runs no reviewer execution of its own: the only
/// executions a Task has afterwards are the ones real roles ran.
async fn assert_no_synthetic_reviewer_execution(db: &db::SqliteDb, task_id: &str) {
    let agentless: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution WHERE task_id = ? AND role = 'reviewer' AND agent_id IS NULL",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .expect("executions count");
    assert_eq!(agentless, 0, "no synthetic reviewer execution is created");
}

// ── Harness ──

struct TestHarness {
    app: Router,
    state: Arc<AppState>,
    _web_dist_dir: common::TestDir,
}

async fn test_app(workspace_root: &Path) -> TestHarness {
    test_app_with_reviewer(workspace_root, false).await
}

async fn test_app_with_reviewer(workspace_root: &Path, reviewer_passes: bool) -> TestHarness {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");

    let db = Arc::new(db::SqliteDb::new(pool));
    let mut registry = executors::AdapterRegistry::new();
    registry.register(Box::new(DelayedCodexAdapter { reviewer_passes }));
    let adapter_registry = Arc::new(registry);
    services::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(EventBus::new(256));
    let merge_service = Arc::new(services::MergeService::new_for_test(
        Arc::clone(&db),
        workspace_root.to_path_buf(),
    ));
    let cleanup_scheduler = Arc::new(services::WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.to_path_buf(),
    ));
    let state = Arc::new(AppState::with_adapter_registry_services_and_shutdown(
        db,
        event_bus,
        true,
        adapter_registry,
        merge_service,
        cleanup_scheduler,
        api::state::ShutdownSignal::new(),
        api::state::test_workflows_dir(),
        api::state::test_jwt_secret(),
        api::state::test_bcrypt_cost(),
    ));

    let web_dist_dir = common::TestDir::new("forge-manual-review-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        app,
        state,
        _web_dist_dir: web_dist_dir,
    }
}

/// Every run takes 30 seconds, except, when `reviewer_passes`, a reviewer's:
/// it reports a passing assessment at once.
struct DelayedCodexAdapter {
    reviewer_passes: bool,
}

impl CodingExecutorAdapter for DelayedCodexAdapter {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Codex
    }

    fn check_availability(&self) -> AvailabilityInfo {
        AvailabilityInfo {
            status: AvailabilityStatus::Authenticated,
            authenticated_at: None,
            config_path: None,
        }
    }

    fn discover_options<'life0, 'async_trait>(
        &'life0 self,
        _ctx: DiscoverContext,
    ) -> Pin<Box<dyn Future<Output = Result<DiscoveredOptions, ExecutorError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { Ok(DiscoveredOptions::default()) })
    }

    fn execute<'life0, 'async_trait>(
        &'life0 self,
        ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<ExecutionResult, ExecutorError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let reviewer_passes = self.reviewer_passes;
        Box::pin(async move {
            if reviewer_passes && common::is_conformance_review_prompt(&ctx.description) {
                if let Some(parent) = Path::new(&ctx.logs_path).parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let mut writer = executors::LogWriter::new(
                    &ctx.logs_path,
                    ctx.execution_id.clone(),
                    1024 * 1024,
                );
                writer
                    .write(
                        executors::LogKind::Assistant,
                        executors::LogStream::Main,
                        json!({
                            "text": common::passing_review_assessment(
                                &ctx.description,
                                Path::new(&ctx.worktree_path),
                            )
                        }),
                    )
                    .await?;
                return Ok(ExecutionResult {
                    status: ExecutionOutcome::Completed,
                    after_sha: None,
                    agent_session_id: Some("manual-review-reviewer-session".to_owned()),
                    summary: Some("reviewer passed the implementation".to_owned()),
                    error: None,
                    ..Default::default()
                });
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                after_sha: None,
                agent_session_id: Some("manual-review-follow-up-session".to_owned()),
                summary: Some("follow-up deferred".to_owned()),
                error: None,
                ..Default::default()
            })
        })
    }

    fn cancel<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _execution_id: &'life1 str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutorError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { Ok(()) })
    }
}

// ── Seed helpers ──

async fn seed_awaiting_human_review(
    db: &db::SqliteDb,
    settings: &str,
    workspace_root: &Path,
) -> (String, String) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let execution_id = new_uuid_v4();
    let review_id = new_uuid_v4();
    let workspace_id = new_uuid_v4();
    let (repo_path, worktree_path) = setup_git_repo(workspace_root, &task_id);
    let agent_id = seed_codex_agent(db).await;

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "ManualReview".to_owned(),
            settings: settings.to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            local_path: Some(repo_path.to_string_lossy().into_owned()),
            remote_url: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");

    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Manual review task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "review".to_owned(),
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
    .expect("task creates");

    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("coder assignment creates");

    db::WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&task_id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");

    ExecutionRepo::create(
        db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("manual-review-parent-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                json!({
                    "executor_type": "codex",
                    "config": {},
                    "capabilities": [],
                    "overrides_applied": { "profile": [], "agent": [], "execution": [] },
                    "snapshotted_at": now
                })
                .to_string(),
            ),
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution creates");

    ReviewRepo::create(
        db,
        CreateReview {
            id: review_id.clone(),
            task_id: task_id.clone(),
            execution_id,
            attempt_number: 1,
            status: ReviewStatus::AwaitingHuman,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("review creates");

    (task_id, review_id)
}

async fn seed_codex_agent(db: &db::SqliteDb) -> String {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            machine_id: services::embedded_daemon::embedded_machine_id(),
            hostname: "manual-review-host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon creates");
    DaemonRepo::update_report(
        db,
        db::UpdateDaemonReport {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            detected_clis_json: r#"[{"kind":"codex","availability":"authenticated"}]"#.to_owned(),
            labels_json: None,
            status: DaemonStatus::Online,
            last_report_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon report updates");

    let agent_id = new_uuid_v4();
    AgentRepo::create(
        db,
        CreateAgent {
            id: agent_id.clone(),
            name: "manual-review-codex".to_owned(),
            description: None,
            executor_type: "codex".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: 2,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent creates");

    agent_id
}

async fn seed_awaiting_human_review_with_workspace(
    db: &db::SqliteDb,
    task_id: &str,
    repo_path: &Path,
    worktree_path: &Path,
) -> (String, String) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let execution_id = new_uuid_v4();
    let review_id = new_uuid_v4();
    let workspace_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "CascadeApprove".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            local_path: Some(repo_path.to_string_lossy().into_owned()),
            remote_url: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");

    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.to_owned(),
            project_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Cascade approve task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "review".to_owned(),
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
    .expect("task creates");

    db::WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.to_owned(),
            repo_id,
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(task_id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");

    ExecutionRepo::create(
        db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.to_owned(),
            agent_id: None,
            role: "coder".to_owned(),
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
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution creates");

    ReviewRepo::create(
        db,
        CreateReview {
            id: review_id.clone(),
            task_id: task_id.to_owned(),
            execution_id,
            attempt_number: 1,
            status: ReviewStatus::AwaitingHuman,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("review creates");

    (task_id.to_owned(), review_id)
}

async fn seed_review_with_status(db: &db::SqliteDb, status: ReviewStatus) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let execution_id = new_uuid_v4();
    let review_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "ReviewStatus".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            local_path: Some("/tmp/forge-review-status-repo".to_owned()),
            remote_url: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Status test".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "review".to_owned(),
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
    .expect("task creates");
    ExecutionRepo::create(
        db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: None,
            role: "coder".to_owned(),
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
    .expect("execution creates");
    ReviewRepo::create(
        db,
        CreateReview {
            id: review_id,
            task_id: task_id.clone(),
            execution_id,
            attempt_number: 1,
            status,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("review creates");

    task_id
}

async fn seed_task_with_system_comment(db: &db::SqliteDb) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "SystemComment".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            local_path: Some("/tmp/forge-sys-comment-repo".to_owned()),
            remote_url: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "System comment test".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
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
    .expect("task creates");

    TaskCommentRepo::create_comment(
        db,
        CreateTaskComment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            author_type: CommentAuthorType::System,
            author_id: None,
            author_name: "Forge".to_owned(),
            content: "System generated comment".to_owned(),
            execution_id: None,
            role: None,
            worklog_kind: None,
            idempotency_key: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("system comment creates");

    task_id
}

// ── Git helper ──

fn setup_git_repo(workspace_root: &Path, task_id: &str) -> (PathBuf, PathBuf) {
    let repo_path = workspace_root.join("repos").join(task_id);
    std::fs::create_dir_all(&repo_path).expect("create repo dir");
    run_git(&repo_path, &["init", "--initial-branch=main"]);
    run_git(&repo_path, &["config", "user.email", "test@test.com"]);
    run_git(&repo_path, &["config", "user.name", "Test"]);
    std::fs::write(repo_path.join("file.txt"), "base\n").expect("write base file");
    run_git(&repo_path, &["add", "."]);
    run_git(&repo_path, &["commit", "-m", "initial"]);

    let worktree_path = workspace_root.join("worktrees").join(task_id).join("repo");
    std::fs::create_dir_all(worktree_path.parent().unwrap()).expect("create worktree parent");
    run_git(
        &repo_path,
        &[
            "worktree",
            "add",
            worktree_path.to_str().unwrap(),
            "-b",
            &::workspace::task_branch_name(task_id),
        ],
    );
    std::fs::write(worktree_path.join("file.txt"), "feature\n").expect("write feature file");
    run_git(&worktree_path, &["add", "."]);
    run_git(&worktree_path, &["commit", "-m", "feature"]);

    (repo_path, worktree_path)
}

fn run_git(path: &Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {} failed\nstdout: {}\nstderr: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

// ── Request helpers ──

fn test_jwt() -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = serde_json::json!({
        "sub": "test-user-id",
        "email": "test@example.com",
        "iat": now,
        "exp": now + 900,
    });
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .expect("encode test jwt")
}

async fn json_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    body: serde_json::Value,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    common::json_request(app, method, uri, body, expected_status).await
}

async fn empty_request<T>(app: &Router, method: Method, uri: &str, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let token = test_jwt();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("router response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert_eq!(
        status,
        expected_status,
        "unexpected status with body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON")
}

async fn raw_request(
    app: &Router,
    method: Method,
    uri: &str,
    body: serde_json::Value,
) -> axum::http::Response<Body> {
    common::raw_json_request(app, method, uri, body).await
}
