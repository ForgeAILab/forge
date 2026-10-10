//! A manual review-check retry (`TaskService::rerun_review`, reached through
//! the closed Task action's `retry`) re-enters the ordinary durable review
//! entry. There is no second pipeline: the checks run through the check
//! runner, the Review row is the entry's own attempt, and no reviewer
//! execution exists that a real role did not run.
use super::durable_23b::{approval_ci_fixture, count, enter_review, suspend_on_entry_check};
use super::*;
use crate::worker_runtime::queue::TaskStepWorker;

fn task_uuid(fixture: &FailedCiFixture) -> uuid::Uuid {
    uuid::Uuid::parse_str(&fixture.task.id).expect("the fixture Task id is a UUID")
}

async fn reviews(fixture: &FailedCiFixture) -> Vec<db::Review> {
    let mut reviews = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    reviews.sort_by_key(|review| review.attempt_number);
    reviews
}

fn details(review: &db::Review) -> serde_json::Value {
    serde_json::from_str(&review.step_results_json).unwrap()
}

async fn set_ci_steps(fixture: &FailedCiFixture, steps: serde_json::Value) {
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(json!({"retry_budgets":{"review":3},"review":{"ci_steps":steps}}).to_string())
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
}

fn quote(path: &std::path::Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\"'\"'"))
}

/// The Task sits in `review` and its only review attempt failed on CI.
async fn seed_red_review(fixture: &FailedCiFixture) {
    sqlx::query("UPDATE task SET status = 'review' WHERE id = ?")
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let candidate: String = sqlx::query_scalar("SELECT id FROM execution WHERE task_id = ?")
        .bind(&fixture.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO review (id, task_id, execution_id, attempt_number, status, step_results_json, started_at, finished_at, created_at, updated_at)
         VALUES (?, ?, ?, 1, 'failed', ?, ?, ?, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&fixture.task.id)
    .bind(&candidate)
    .bind(json!({"ci_steps":[{"index":0,"command":"false","exit_code":1,"stderr_tail":"","output_tail":"","started_at":now,"finished_at":now}]}).to_string())
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .execute(fixture.db.pool())
    .await
    .unwrap();
}

async fn synthetic_reviewer_executions(fixture: &FailedCiFixture) -> i64 {
    count(
        fixture,
        "SELECT COUNT(*) FROM execution WHERE role IN ('reviewer','auditor')",
    )
    .await
}

// A Task sits in `review` and its attempt failed on CI. The owner fixes what
// broke and asks for the review again: the entry's CI runs through the check
// runner (one consumer, one run), the new attempt passes, and the Task leaves
// for `merging`.
#[tokio::test]
async fn a_rerun_after_red_ci_runs_the_entry_ci_once_more_and_passes_to_merging() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    let control = TempDir::new().unwrap();
    let green = control.path().join("green");
    set_ci_steps(&fixture, json!([format!("test -f {}", quote(&green))])).await;
    seed_red_review(&fixture).await;
    std::fs::write(&green, "").unwrap();
    let rerun = fixture
        .engine
        .rerun_review(task_uuid(&fixture))
        .await
        .unwrap();
    assert_eq!(rerun.status, "review", "the retry only re-enters review");
    drain(fixture.engine.clone(), &fixture.task.id).await;

    let reviews = reviews(&fixture).await;
    assert_eq!(reviews.len(), 2, "one attempt per entry");
    assert_eq!(reviews[0].status, db::ReviewStatus::Failed);
    assert_eq!(reviews[1].status, db::ReviewStatus::Passed);
    assert_eq!(details(&reviews[1])["ci_steps"][0]["exit_code"], 0);
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        1,
        "the retried entry asked once"
    );
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        1,
        "and its CI ran once, through the check runner"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM transition_log WHERE from_state='review' AND to_state='merging'"
        )
        .await,
        1,
        "the passed re-run goes on to merging"
    );
    assert_eq!(synthetic_reviewer_executions(&fixture).await, 0);
}

// The commit, the worktree and the environment did not change since the green
// entry: the retried entry reuses that CI result (no run), but the review
// itself is asked for again. The attempt the owner replaced is closed, so
// only the new one can be approved.
#[tokio::test]
async fn a_rerun_on_an_unchanged_commit_reuses_green_ci_and_asks_for_the_review_again() {
    let fixture = approval_ci_fixture(json!(["echo reviewed"])).await;
    let first = enter_review(&fixture, "first entry").await;
    let first = drain_result(fixture.engine.clone(), first).await;
    assert_eq!(first.task.status, "review");
    assert_eq!(
        first.review.as_ref().map(|review| review.status.clone()),
        Some(db::ReviewStatus::AwaitingHuman)
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);

    fixture
        .engine
        .rerun_review(task_uuid(&fixture))
        .await
        .unwrap();
    let settled = drain(fixture.engine.clone(), &fixture.task.id).await;
    assert_eq!(settled.status, "review");

    let reviews = reviews(&fixture).await;
    assert_eq!(reviews.len(), 2);
    assert_eq!(reviews[0].status, db::ReviewStatus::Cancelled);
    assert_eq!(
        details(&reviews[0])["execution_retry"]["status"],
        "superseded_by_rerun"
    );
    assert_eq!(reviews[1].status, db::ReviewStatus::AwaitingHuman);
    assert_eq!(details(&reviews[1])["ci_steps"][0]["exit_code"], 0);
    assert_eq!(
        details(&reviews[1])["ci_steps"][0]["output_tail"],
        "reviewed\n"
    );
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        2,
        "the retried entry asked"
    );
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        1,
        "and was answered by the first entry's result"
    );
    assert_eq!(synthetic_reviewer_executions(&fixture).await, 0);
}

// While the entry is still running (its hooks step waits suspended on the
// check) a retry starts no second pipeline: the command is refused as busy,
// and the action system offers no retry to ask with.
#[tokio::test]
async fn a_rerun_while_the_entry_is_still_running_is_refused_as_busy() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    suspend_on_entry_check(&fixture).await;

    let refused = fixture
        .engine
        .rerun_review(task_uuid(&fixture))
        .await
        .unwrap_err();
    assert!(
        matches!(refused, crate::ServiceError::TaskBusy { .. }),
        "{refused:?}"
    );
    let owner = api_types::Actor::user(api_types::UserActionSource::Api);
    let snapshot = fixture
        .engine
        .task_action_snapshot(&fixture.task.id, &owner)
        .await
        .unwrap();
    assert!(
        !crate::available_actions(&snapshot)
            .iter()
            .any(|offer| offer.action.verb() == "retry"),
        "no retry is offered while the entry runs"
    );
    let refused = fixture
        .engine
        .perform_task_action(
            fixture.task.id.clone(),
            api_types::TaskAction::retry(),
            snapshot.task.version,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(refused, crate::ServiceError::TaskActionUnavailable { .. }),
        "{refused:?}"
    );

    let reviews = reviews(&fixture).await;
    assert_eq!(reviews.len(), 1, "no second attempt");
    assert_eq!(reviews[0].status, db::ReviewStatus::Running);
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        1
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE kind='hooks' AND status='suspended'"
        )
        .await,
        1,
        "the running entry keeps waiting for its check"
    );
}

// A reviewer that is still working is not replaced either.
#[tokio::test]
async fn a_rerun_while_a_reviewer_is_running_is_refused_as_busy() {
    let fixture = approval_ci_fixture(json!(["echo reviewed"])).await;
    let first = enter_review(&fixture, "first entry").await;
    drain_result(fixture.engine.clone(), first).await;
    let candidate: String = sqlx::query_scalar("SELECT id FROM execution WHERE task_id = ?")
        .bind(&fixture.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO execution (id, task_id, role, status, parent_execution_id, workspace_id, created_at, updated_at)
         VALUES (?, ?, 'reviewer', 'running', ?, ?, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&fixture.task.id)
    .bind(&candidate)
    .bind(&fixture.workspace.id)
    .bind(&now)
    .bind(&now)
    .execute(fixture.db.pool())
    .await
    .unwrap();

    let refused = fixture
        .engine
        .rerun_review(task_uuid(&fixture))
        .await
        .unwrap_err();
    assert!(
        matches!(refused, crate::ServiceError::TaskBusy { .. }),
        "{refused:?}"
    );
    let reviews = reviews(&fixture).await;
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, db::ReviewStatus::AwaitingHuman);
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
}

// A paused Project holds the retried entry exactly where it holds an
// automatic one: the checks run and the attempt passes, but integration is
// deferred and the Task stays in `review`.
#[tokio::test]
async fn a_rerun_in_a_paused_project_passes_its_checks_and_defers_integration() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    let control = TempDir::new().unwrap();
    let green = control.path().join("green");
    set_ci_steps(&fixture, json!([format!("test -f {}", quote(&green))])).await;
    seed_red_review(&fixture).await;
    std::fs::write(&green, "").unwrap();
    sqlx::query("UPDATE project SET paused_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();

    fixture
        .engine
        .rerun_review(task_uuid(&fixture))
        .await
        .unwrap();
    let settled = drain(fixture.engine.clone(), &fixture.task.id).await;

    assert_eq!(
        settled.status, "review",
        "a paused Project integrates nothing"
    );
    let reviews = reviews(&fixture).await;
    assert_eq!(reviews.len(), 2);
    assert_eq!(reviews[1].status, db::ReviewStatus::Passed);
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM transition_log WHERE to_state='merging'"
        )
        .await,
        0
    );
}

// Cancelling the Task while the retried entry's CI runs behaves as it does
// for an automatic entry: the suspended step is superseded, the run stops,
// its consumer is cancelled and no review attempt stays running.
#[tokio::test]
async fn cancel_during_a_retried_entry_stops_the_run_and_closes_the_attempt() {
    let fixture = approval_ci_fixture(json!(["echo reviewed"])).await;
    let first = enter_review(&fixture, "first entry").await;
    drain_result(fixture.engine.clone(), first).await;
    let control = TempDir::new().unwrap();
    let started = control.path().join("started");
    let completed = control.path().join("completed");
    set_ci_steps(
        &fixture,
        json!([format!(
            "touch {}; sleep 120; touch {}",
            quote(&started),
            quote(&completed)
        )]),
    )
    .await;

    fixture
        .engine
        .rerun_review(task_uuid(&fixture))
        .await
        .unwrap();
    let checks = fixture.engine.check_worker_or_embedded();
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal.clone());
    let periodic = crate::worker_runtime::PeriodicWorkers::new(fixture.db.clone());
    let check_worker = checks.start(&periodic, signal);
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while !started.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the retried entry's CI starts through the check runner");

    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let cancelled = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &current.id,
            "cancelled",
            current.version,
            &fixture.workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "Cancel during the retried CI",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    assert_eq!(cancelled.task.status, "cancelled");
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let reviews = reviews(&fixture).await;
            let live = count(
                &fixture,
                "SELECT COUNT(*) FROM check_run WHERE state NOT IN ('succeeded','failed','timed_out','cancelled','infrastructure_failed')",
            )
            .await;
            let suspended = count(
                &fixture,
                "SELECT COUNT(*) FROM task_step WHERE kind='hooks' AND status='suspended'",
            )
            .await;
            if reviews.len() == 2
                && reviews[1].status == db::ReviewStatus::Cancelled
                && live == 0
                && suspended == 0
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the retried attempt is cancelled and its check run stops");
    assert!(!completed.exists());
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_consumer WHERE cancelled_at IS NOT NULL"
        )
        .await,
        1,
        "only the retried entry's consumer is cancelled"
    );
    stop.send(true).unwrap();
    worker.await.unwrap();
    check_worker.await.unwrap();
}

// The server dies while the retried entry waits suspended on its check. The
// next runtime wakes the same step: one attempt for the entry, one run.
#[tokio::test]
async fn restart_during_a_retried_entry_settles_the_same_attempt_with_one_run() {
    let fixture = approval_ci_fixture(json!(["echo reviewed"])).await;
    let first = enter_review(&fixture, "first entry").await;
    drain_result(fixture.engine.clone(), first).await;
    // A different command is a different check identity: it has to run.
    set_ci_steps(&fixture, json!(["echo reviewed again"])).await;

    fixture
        .engine
        .rerun_review(task_uuid(&fixture))
        .await
        .unwrap();
    // Compose the check runtime but run no check worker: the step suspends
    // and its run stays queued, as if the server died right there.
    let _ = fixture.engine.check_worker_or_embedded();
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE kind='hooks' AND status='suspended'",
        )
        .await
            == 0
        {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the retried entry's hooks step suspends on its check");
    stop.send(true).unwrap();
    worker.await.unwrap();
    let waiting = reviews(&fixture).await;
    assert_eq!(waiting.len(), 2);
    assert_eq!(waiting[1].status, db::ReviewStatus::Running);
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 2);
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_result").await,
        1
    );

    // A new runtime on the same database.
    let mut restarted = engine(fixture.db.clone(), Arc::new(EventBus::new(32)));
    restarted.workspace_root = fixture._workspace_root.path().to_path_buf();
    restarted = restarted.with_workspace_root(fixture._workspace_root.path().to_path_buf());
    drain(restarted, &fixture.task.id).await;

    let settled = reviews(&fixture).await;
    assert_eq!(settled.len(), 2, "the woken step opens no further attempt");
    assert_eq!(settled[1].id, waiting[1].id);
    assert_eq!(settled[1].status, db::ReviewStatus::AwaitingHuman);
    assert_eq!(
        details(&settled[1])["ci_steps"][0]["output_tail"],
        "reviewed again\n"
    );
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        2,
        "the retried entry ran once"
    );
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_result").await,
        2
    );
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        2
    );
}

// A server is upgraded while the retired inline re-run is in flight. Its rows
// are still there: a reviewer execution no role ran, `running` and leased to
// `review-owner:<uuid>`, and the review attempt it opened. Recovery stops the
// execution with a reason that says what it was, and the Task's review entry
// runs again through the durable path, which closes the old attempt.
#[tokio::test]
async fn an_upgrade_stops_a_leftover_inline_rerun_and_runs_the_review_entry_again() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    set_ci_steps(&fixture, json!(["echo upgraded"])).await;
    sqlx::query("UPDATE task SET status = 'review' WHERE id = ?")
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let candidate: String = sqlx::query_scalar("SELECT id FROM execution WHERE task_id = ?")
        .bind(&fixture.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    // The attempt as the old pipeline created it: a bare step array.
    sqlx::query(
        "INSERT INTO review (id, task_id, execution_id, attempt_number, status, step_results_json, started_at, created_at, updated_at)
         VALUES (?, ?, ?, 1, 'running', '[]', ?, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&fixture.task.id)
    .bind(&candidate)
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .execute(fixture.db.pool())
    .await
    .unwrap();
    let leftover = new_uuid_v4();
    let logs_path = fixture
        ._workspace_root
        .path()
        .join(".forge/logs")
        .join(&fixture.task.project_id)
        .join(&fixture.task.id)
        .join(format!("review-{candidate}.jsonl"));
    sqlx::query(
        "INSERT INTO execution (id, task_id, role, status, parent_execution_id, workspace_id, logs_path, created_at, updated_at)
         VALUES (?, ?, 'reviewer', 'running', ?, ?, ?, ?, ?)",
    )
    .bind(&leftover)
    .bind(&fixture.task.id)
    .bind(&candidate)
    .bind(&fixture.workspace.id)
    .bind(logs_path.to_string_lossy().as_ref())
    .bind(&now)
    .bind(&now)
    .execute(fixture.db.pool())
    .await
    .unwrap();
    let row = db::ExecutionRepo::get_by_id(&*fixture.db, &leftover)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        db::ExecutionRepo::claim_lease(
            &*fixture.db,
            db::ClaimExecutionLease {
                execution_id: leftover.clone(),
                expected_version: row.execution_version,
                owner: format!("review-owner:{}", new_uuid_v4()),
                // The old process renewed this every few seconds; it is gone.
                lease_expires_at: "1970-01-01T00:00:00+00:00".to_owned(),
                hard_deadline_at: None,
                now,
            },
        )
        .await
        .unwrap(),
        db::ExecutionLeaseMutation::Updated(_)
    ));

    let monitor = crate::recovery::HeartbeatMonitor::new(
        Arc::clone(&fixture.db),
        Arc::new(EventBus::new(32)),
    )
    .with_task_service(Arc::new(fixture.engine.clone()));
    monitor.check_once().await.unwrap();
    drain(fixture.engine.clone(), &fixture.task.id).await;

    let stopped = db::ExecutionRepo::get_by_id(&*fixture.db, &leftover)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stopped.status, db::ExecutionStatus::Failed);
    assert!(
        stopped
            .error
            .as_deref()
            .is_some_and(|error| error.contains("inline review re-run")),
        "{:?}",
        stopped.error
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM execution WHERE status = 'running'"
        )
        .await,
        0,
        "nothing of the old pipeline keeps a run slot"
    );
    let reviews = reviews(&fixture).await;
    assert_eq!(reviews.len(), 2);
    assert_eq!(reviews[0].status, db::ReviewStatus::Cancelled);
    assert_eq!(reviews[1].status, db::ReviewStatus::Passed);
    assert_eq!(
        details(&reviews[1])["ci_steps"][0]["output_tail"],
        "upgraded\n"
    );
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        1,
        "the entry's CI ran once, through the check runner"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM transition_log WHERE from_state='review' AND to_state='merging'"
        )
        .await,
        1
    );
}
