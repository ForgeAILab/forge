//! Review retry budgets, cached CI and assigned reviewer dispatch.

use super::*;

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
