//! A manual review-check retry (`TaskService::rerun_review`, reached through
//! the closed Task action's `retry`) re-enters the ordinary durable review
//! entry. There is no second pipeline: the checks run through the check
//! runner, the Review row is the entry's own attempt, and no reviewer
//! execution exists that a real role did not run.
use super::durable_23b::{approval_ci_fixture, count, enter_review, suspend_on_entry_check};
use super::*;
use crate::worker_runtime::queue::TaskStepWorker;

fn owner() -> api_types::Actor {
    api_types::Actor::user(api_types::UserActionSource::Api)
}

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
        .rerun_review(task_uuid(&fixture), owner())
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
        .rerun_review(task_uuid(&fixture), owner())
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
        .rerun_review(task_uuid(&fixture), owner())
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
        .rerun_review(task_uuid(&fixture), owner())
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
        .rerun_review(task_uuid(&fixture), owner())
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
        .rerun_review(task_uuid(&fixture), owner())
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
        .rerun_review(task_uuid(&fixture), owner())
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
/// The rows the retired inline re-run left behind when the server was
/// upgraded under it: the review attempt it opened (a bare step array) and
/// its reviewer execution, which no role ran, `running` and leased to
/// `review-owner:<uuid>`. Returns the candidate and that execution.
async fn seed_inline_rerun_in_flight(
    fixture: &FailedCiFixture,
    lease_expires_at: &str,
) -> (String, String) {
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
    let leftover =
        seed_inline_rerun_execution(fixture, "reviewer", &candidate, lease_expires_at).await;
    (candidate, leftover)
}

async fn seed_inline_rerun_execution(
    fixture: &FailedCiFixture,
    role: &str,
    candidate: &str,
    lease_expires_at: &str,
) -> String {
    let now = now_rfc3339();
    let leftover = new_uuid_v4();
    let logs_path = fixture
        ._workspace_root
        .path()
        .join(".forge/logs")
        .join(&fixture.task.project_id)
        .join(&fixture.task.id)
        .join(if role == "reviewer" {
            format!("review-{candidate}.jsonl")
        } else {
            format!("review-{candidate}-auditor-{leftover}.jsonl")
        });
    sqlx::query(
        "INSERT INTO execution (id, task_id, role, status, parent_execution_id, workspace_id, logs_path, executor_config_snapshot_json, created_at, updated_at)
         VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?)",
    )
    .bind(&leftover)
    .bind(&fixture.task.id)
    .bind(role)
    .bind(candidate)
    .bind(&fixture.workspace.id)
    .bind(logs_path.to_string_lossy().as_ref())
    // The auditor child ran a real executor; the reviewer execution ran none.
    .bind((role == "auditor").then(|| "{}".to_owned()))
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
                lease_expires_at: lease_expires_at.to_owned(),
                hard_deadline_at: None,
                now,
            },
        )
        .await
        .unwrap(),
        db::ExecutionLeaseMutation::Updated(_)
    ));
    leftover
}

/// The old process renewed its lease every few seconds; it is gone.
const LEASE_EXPIRED: &str = "1970-01-01T00:00:00+00:00";
/// The server came back before the old pipeline's lease ran out.
const LEASE_STILL_HELD: &str = "2999-01-01T00:00:00+00:00";

async fn assert_stopped_as_retired(fixture: &FailedCiFixture, execution_id: &str) {
    let stopped = db::ExecutionRepo::get_by_id(&*fixture.db, execution_id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(stopped.status, db::ExecutionStatus::Running);
    assert!(
        stopped
            .error
            .as_deref()
            .is_some_and(|error| error.contains("inline review re-run")),
        "{:?}",
        stopped.error
    );
}

#[tokio::test]
async fn an_upgrade_stops_a_leftover_inline_rerun_and_runs_the_review_entry_again() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    set_ci_steps(&fixture, json!(["echo upgraded"])).await;
    let (_, leftover) = seed_inline_rerun_in_flight(&fixture, LEASE_EXPIRED).await;

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

async fn review_budget_spent(fixture: &FailedCiFixture) -> (i64, i64) {
    (
        db::budget::spent(fixture.db.pool(), &fixture.task.id, "review")
            .await
            .unwrap(),
        db::budget::spent(
            fixture.db.pool(),
            &fixture.task.id,
            db::budget::REVIEW_GATE_KEY,
        )
        .await
        .unwrap(),
    )
}

/// Put the Task back in `review` after a failed entry sent it to remediation,
/// as the coder's next hand-off would.
async fn back_to_review(fixture: &FailedCiFixture) {
    sqlx::query(
        "UPDATE task SET status = 'review', blocked_json = NULL, entry_barrier_json = NULL, error_annotation = NULL WHERE id = ?",
    )
    .bind(&fixture.task.id)
    .execute(fixture.db.pool())
    .await
    .unwrap();
    sqlx::query(
        "DELETE FROM task_step WHERE task_id = ? AND status IN ('pending','claimed','suspended')",
    )
    .bind(&fixture.task.id)
    .execute(fixture.db.pool())
    .await
    .unwrap();
}

// The owner asks for the review again and it fails on red CI, again and
// again. An owner's action spends no retry budget: the review budget is where
// it was after any number of failed re-runs, so they can never exhaust it.
// The same re-entry asked for by Forge itself is an automatic entry and spends.
#[tokio::test]
async fn failed_owner_reruns_leave_the_review_budget_where_it_was_and_an_automatic_one_spends() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    set_ci_steps(&fixture, json!(["false"])).await;
    seed_red_review(&fixture).await;
    let before = review_budget_spent(&fixture).await;

    for round in 1..=5 {
        fixture
            .engine
            .rerun_review(task_uuid(&fixture), owner())
            .await
            .unwrap();
        let settled = drain(fixture.engine.clone(), &fixture.task.id).await;
        assert_eq!(
            review_budget_spent(&fixture).await,
            before,
            "failed owner re-run {round} (Task now {})",
            settled.status
        );
        assert!(
            !settled
                .blocked_json
                .as_deref()
                .unwrap_or_default()
                .contains("review retry budget exhausted")
                && !settled
                    .entry_barrier_json
                    .as_deref()
                    .unwrap_or_default()
                    .contains("review retry budget exhausted"),
            "owner re-runs never exhaust the review budget: {settled:?}"
        );
        let reviews = reviews(&fixture).await;
        assert_eq!(reviews.len(), round + 1, "each re-run is one attempt");
        assert_eq!(
            reviews.last().unwrap().status,
            db::ReviewStatus::Failed,
            "red CI fails the attempt"
        );
        back_to_review(&fixture).await;
    }

    fixture
        .engine
        .rerun_review(
            task_uuid(&fixture),
            api_types::Actor::system(api_types::SystemComponent::Workflow),
        )
        .await
        .unwrap();
    drain(fixture.engine.clone(), &fixture.task.id).await;
    let after = review_budget_spent(&fixture).await;
    assert_eq!(
        after.0,
        before.0 + 1,
        "an entry nobody with owner authority asked for spends one review attempt"
    );
}

async fn assert_review_entry_ran_again_once(fixture: &FailedCiFixture) {
    assert_eq!(
        count(
            fixture,
            "SELECT COUNT(*) FROM execution WHERE status = 'running'"
        )
        .await,
        0,
        "nothing of the old pipeline keeps a run slot"
    );
    let reviews = reviews(fixture).await;
    assert_eq!(reviews.len(), 2, "one re-entry, one new attempt");
    assert_eq!(reviews[0].status, db::ReviewStatus::Cancelled);
    assert_eq!(reviews[1].status, db::ReviewStatus::Passed);
    assert_eq!(
        count(fixture, "SELECT COUNT(*) FROM check_run").await,
        1,
        "the entry's CI ran once, through the check runner"
    );
    assert_eq!(
        count(
            fixture,
            "SELECT COUNT(*) FROM transition_log WHERE from_state='review' AND to_state='merging'"
        )
        .await,
        1
    );
    let task = db::TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(task.blocked_json.is_none(), "{:?}", task.blocked_json);
}

// The server is restarted quickly: the old pipeline's lease has not expired,
// so the lease monitor would leave its execution alone for as long as the
// lease still says. Start-time recovery does not wait: it stops the leftover
// and the review entry runs again.
#[tokio::test]
async fn start_time_recovery_stops_a_leftover_inline_rerun_whose_lease_is_still_held() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    set_ci_steps(&fixture, json!(["echo upgraded"])).await;
    let (_, leftover) = seed_inline_rerun_in_flight(&fixture, LEASE_STILL_HELD).await;

    let monitor = crate::recovery::HeartbeatMonitor::new(
        Arc::clone(&fixture.db),
        Arc::new(EventBus::new(32)),
    )
    .with_task_service(Arc::new(fixture.engine.clone()));
    monitor.check_once().await.unwrap();
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM execution WHERE status = 'running'"
        )
        .await,
        1,
        "the lease monitor honours a lease that is still held"
    );

    let recovery =
        crate::recovery::CrashRecovery::new(Arc::clone(&fixture.db), Arc::new(EventBus::new(32)));
    recovery.run_recovery().await.unwrap();
    // A second start before the queue drained stacks no second settlement.
    recovery.run_recovery().await.unwrap();
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE kind='command' AND json_extract(payload_json,'$.operation')='settle_retired_review_rerun'"
        )
        .await,
        1
    );
    drain(fixture.engine.clone(), &fixture.task.id).await;

    assert_stopped_as_retired(&fixture, &leftover).await;
    assert_review_entry_ran_again_once(&fixture).await;
}

// The old pipeline was running its auditor child when the server was
// upgraded: two leftover executions, the reviewer one and the auditor under
// the same lease scheme. Both are stopped with the same explicit reason and
// the Task is settled once, by one re-entry, without being blocked on the
// first of them.
#[tokio::test]
async fn an_upgrade_settles_a_leftover_auditor_child_with_its_reviewer_execution() {
    for at_start in [false, true] {
        let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
        set_ci_steps(&fixture, json!(["echo upgraded"])).await;
        let lease = if at_start {
            LEASE_STILL_HELD
        } else {
            LEASE_EXPIRED
        };
        let (candidate, reviewer) = seed_inline_rerun_in_flight(&fixture, lease).await;
        let auditor = seed_inline_rerun_execution(&fixture, "auditor", &candidate, lease).await;

        if at_start {
            crate::recovery::CrashRecovery::new(
                Arc::clone(&fixture.db),
                Arc::new(EventBus::new(32)),
            )
            .run_recovery()
            .await
            .unwrap();
        } else {
            crate::recovery::HeartbeatMonitor::new(
                Arc::clone(&fixture.db),
                Arc::new(EventBus::new(32)),
            )
            .with_task_service(Arc::new(fixture.engine.clone()))
            .check_once()
            .await
            .unwrap();
        }
        drain(fixture.engine.clone(), &fixture.task.id).await;

        assert_stopped_as_retired(&fixture, &reviewer).await;
        assert_stopped_as_retired(&fixture, &auditor).await;
        assert_review_entry_ran_again_once(&fixture).await;
    }
}

// The review entry cannot run again at all (the Task has nothing to review).
// The Task is not left in `review` with a stopped execution and no way
// forward: it is blocked on that execution, typed, with the reason.
#[tokio::test]
async fn an_upgrade_blocks_the_task_typed_when_the_review_entry_cannot_run_again() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    set_ci_steps(&fixture, json!(["echo upgraded"])).await;
    let (candidate, leftover) = seed_inline_rerun_in_flight(&fixture, LEASE_STILL_HELD).await;
    // No execution of the Task is a review candidate any more.
    sqlx::query("UPDATE execution SET role = 'planner' WHERE id = ?")
        .bind(&candidate)
        .execute(fixture.db.pool())
        .await
        .unwrap();

    crate::recovery::CrashRecovery::new(Arc::clone(&fixture.db), Arc::new(EventBus::new(32)))
        .run_recovery()
        .await
        .unwrap();
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;

    assert_stopped_as_retired(&fixture, &leftover).await;
    assert_eq!(task.status, "review");
    let blocked: serde_json::Value =
        serde_json::from_str(task.blocked_json.as_deref().expect("the Task is blocked")).unwrap();
    assert_eq!(blocked["kind"], "internal_command_failed", "{blocked}");
    assert_eq!(blocked["execution_id"], leftover, "{blocked}");
    assert!(
        blocked["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("inline review re-run")
                && reason.contains("the review entry could not be run again")),
        "{blocked}"
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 0);
    let owner = owner();
    let snapshot = fixture
        .engine
        .task_action_snapshot(&fixture.task.id, &owner)
        .await
        .unwrap();
    assert!(
        !crate::available_actions(&snapshot).is_empty(),
        "the owner is offered a way forward"
    );
}

// A `rerun_review` command step queued by the previous version names only
// the Task. It still runs after the upgrade, as the owner's re-run it was:
// the entry runs again and, failing, spends no review budget.
#[tokio::test]
async fn a_rerun_review_command_queued_before_the_upgrade_still_runs_as_the_owners() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    set_ci_steps(&fixture, json!(["false"])).await;
    seed_red_review(&fixture).await;
    let before = review_budget_spent(&fixture).await;
    let task = db::TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let id = new_uuid_v4();
    db::TaskStepRepo::enqueue_step(
        &*fixture.db,
        &db::EnqueueTaskStep {
            id: id.clone(),
            task_id: task.id.clone(),
            kind: "command".into(),
            // Exactly what v0.13 wrote.
            payload_json: json!({"operation":"rerun_review","arguments":[task.id],"preempt":false})
                .to_string(),
            causation_step_id: None,
            causation_key: format!("rerun_review:{id}"),
            chain_id: id,
            chain_position: 1,
            expected_status: task.status.clone(),
            expected_version: task.version,
            expected_epoch: None,
            lane: "long".into(),
            available_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    drain(fixture.engine.clone(), &fixture.task.id).await;

    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE kind='command' AND status='done' AND json_extract(payload_json,'$.operation')='rerun_review'"
        )
        .await,
        1,
        "the queued command ran"
    );
    let reviews = reviews(&fixture).await;
    assert_eq!(reviews.len(), 2, "the entry ran again");
    assert_eq!(reviews[1].status, db::ReviewStatus::Failed);
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
    assert_eq!(review_budget_spent(&fixture).await, before);
    let requested_by: String = sqlx::query_scalar(
        "SELECT json_extract(bridge_payload,'$.verb') FROM transition_log WHERE task_id = ? AND from_state='review' AND to_state='review'",
    )
    .bind(&fixture.task.id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(requested_by, "review_rerun");
}

/// A second implementation run of the Task: the candidate the next review
/// entry has to bind.
async fn seed_newer_candidate(
    fixture: &FailedCiFixture,
    task_id: &str,
    workspace_id: &str,
) -> String {
    let sha = git::get_current_sha(std::path::Path::new(
        &fixture.workspace.embedded_worktree_path_for_backend(),
    ))
    .await
    .unwrap();
    let id = new_uuid_v4();
    // Strictly after everything the fixture created.
    let now = (chrono::Utc::now() + chrono::Duration::seconds(1)).to_rfc3339();
    db::ExecutionRepo::create(
        &*fixture.db,
        db::CreateExecution {
            id: id.clone(),
            task_id: task_id.to_owned(),
            agent_id: None,
            role: default_roles::CODER.to_owned(),
            status: db::ExecutionStatus::Completed,
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
            before_sha: Some(sha.clone()),
            after_sha: Some(sha),
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id.to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    id
}

// Ported from `rerun_binds_new_candidate_when_prior_review_names_old_candidate`.
// The candidate changed since the prior review: the coder ran again. The
// retried entry binds the NEW candidate; the attempt on the old one is closed
// and stays the old candidate's; nothing of the old candidate's verdict is
// carried to the new attempt.
#[tokio::test]
async fn a_rerun_binds_the_new_candidate_when_the_prior_review_names_the_old_one() {
    // The prior attempt failed on the old candidate.
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    set_ci_steps(&fixture, json!(["echo second-candidate"])).await;
    seed_red_review(&fixture).await;
    let old: String = sqlx::query_scalar("SELECT id FROM execution WHERE task_id = ?")
        .bind(&fixture.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    let new = seed_newer_candidate(&fixture, &fixture.task.id, &fixture.workspace.id).await;
    fixture
        .engine
        .rerun_review(task_uuid(&fixture), owner())
        .await
        .unwrap();
    drain(fixture.engine.clone(), &fixture.task.id).await;
    let attempts = reviews(&fixture).await;
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].execution_id, old);
    assert_eq!(attempts[0].status, db::ReviewStatus::Failed);
    assert_eq!(
        attempts[1].execution_id, new,
        "the re-run reviews the new candidate"
    );
    assert_eq!(attempts[1].status, db::ReviewStatus::Passed);
    assert_eq!(
        details(&attempts[1])["ci_steps"][0]["output_tail"],
        "second-candidate\n",
        "the new attempt carries its own check results, not the old candidate's"
    );

    // The prior attempt is still open on the old candidate (it waits for a
    // person). The re-run closes it and opens one on the new candidate, and
    // an approval can only land on that one.
    let fixture = approval_ci_fixture(json!(["echo reviewed"])).await;
    let first = enter_review(&fixture, "first entry").await;
    let first = drain_result(fixture.engine.clone(), first).await;
    let old = first.review.expect("the first entry opened an attempt");
    assert_eq!(old.status, db::ReviewStatus::AwaitingHuman);
    let new = seed_newer_candidate(&fixture, &fixture.task.id, &fixture.workspace.id).await;
    fixture
        .engine
        .rerun_review(task_uuid(&fixture), owner())
        .await
        .unwrap();
    drain(fixture.engine.clone(), &fixture.task.id).await;
    let attempts = reviews(&fixture).await;
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].id, old.id);
    assert_eq!(attempts[0].status, db::ReviewStatus::Cancelled);
    assert_eq!(attempts[0].execution_id, old.execution_id);
    assert_ne!(old.execution_id, new);
    assert_eq!(attempts[1].execution_id, new);
    assert_eq!(attempts[1].status, db::ReviewStatus::AwaitingHuman);
    assert!(
        details(&attempts[1]).get("execution_retry").is_none(),
        "the new attempt is not the superseded one"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM review WHERE status IN ('running','awaiting_human')"
        )
        .await,
        1,
        "exactly one attempt is open"
    );
}

// Ported from `review_runner_accepts_direct_child_candidate_for_coordination_root`.
// A coordination root has no implementation run of its own: its candidate is
// a direct child's. A re-run on the root is not refused for want of a
// candidate, and the attempt it opens belongs to the root and names the
// child's run.
#[tokio::test]
async fn a_rerun_on_a_coordination_root_reviews_its_direct_childs_candidate() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    set_ci_steps(&fixture, json!(["echo root-review"])).await;
    sqlx::query("UPDATE task SET status = 'review' WHERE id = ?")
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let child_id = new_uuid_v4();
    let now = now_rfc3339();
    TaskRepo::create(
        &*fixture.db,
        db::CreateTask {
            id: child_id.clone(),
            project_id: fixture.task.project_id.clone(),
            parent_task_id: Some(fixture.task.id.clone()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "coordination review child".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "done".to_owned(),
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
    .unwrap();
    // The root never ran an implementation role; the only run is the child's.
    let candidate: String = sqlx::query_scalar("SELECT id FROM execution WHERE task_id = ?")
        .bind(&fixture.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE execution SET task_id = ? WHERE id = ?")
        .bind(&child_id)
        .bind(&candidate)
        .execute(fixture.db.pool())
        .await
        .unwrap();

    fixture
        .engine
        .rerun_review(task_uuid(&fixture), owner())
        .await
        .expect("a root whose candidate is a child's run can be re-reviewed");
    let settled = drain(fixture.engine.clone(), &fixture.task.id).await;

    let attempts = reviews(&fixture).await;
    assert_eq!(attempts.len(), 1, "{settled:?}");
    assert_eq!(attempts[0].task_id, fixture.task.id);
    assert_eq!(attempts[0].execution_id, candidate);
    assert_eq!(attempts[0].status, db::ReviewStatus::Passed, "{settled:?}");
    assert_eq!(
        details(&attempts[0])["ci_steps"][0]["output_tail"],
        "root-review\n"
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
}

// The owner removed the checks that failed and no reviewer is assigned: there
// is nothing to run. The re-run still answers. The failure it was asked to
// replace is not taken for its verdict (the Task is not bounced to
// remediation on it), an empty passed attempt records the re-run, as the
// inline re-run recorded one, and the Task goes on to merging.
#[tokio::test]
async fn a_rerun_with_nothing_to_run_records_an_empty_passed_attempt_and_goes_to_merging() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    set_ci_steps(&fixture, json!([])).await;
    seed_red_review(&fixture).await;
    let before = review_budget_spent(&fixture).await;

    fixture
        .engine
        .rerun_review(task_uuid(&fixture), owner())
        .await
        .unwrap();
    let settled = drain(fixture.engine.clone(), &fixture.task.id).await;

    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM transition_log WHERE from_state='review' AND to_state='merging'"
        )
        .await,
        1,
        "{settled:?}"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM transition_log WHERE from_state='review' AND to_state='in_progress'"
        )
        .await,
        0,
        "the replaced failure does not send the Task to remediation"
    );
    let attempts = reviews(&fixture).await;
    assert_eq!(attempts.len(), 2, "the re-run recorded its attempt, once");
    assert_eq!(attempts[0].status, db::ReviewStatus::Failed);
    assert_eq!(attempts[1].status, db::ReviewStatus::Passed);
    assert_eq!(details(&attempts[1]), json!({"ci_steps": []}));
    assert!(attempts[1].finished_at.is_some());
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 0);
    assert_eq!(synthetic_reviewer_executions(&fixture).await, 0);
    assert_eq!(review_budget_spent(&fixture).await, before);
}

// `review.auditor_agent_id` asked the retired inline re-run for a second,
// auditor pass. There is no such pass: a configuration that names an auditor
// is refused when it is written, and a Task that still carries one from
// before is not re-run to a pass on its checks alone. Once a reviewer Agent is
// assigned (the review the key stood for), the re-run is accepted.
#[tokio::test]
async fn a_configured_auditor_is_refused_at_write_and_never_silently_dropped_by_a_rerun() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    seed_red_review(&fixture).await;
    let task = db::TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let refused = fixture
        .engine
        .update_task(
            task.id.clone(),
            api_types::UpdateTaskRequest {
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                task_state_config: Some(
                    json!({"review":{"ci_steps":["true"],"auditor_agent_id":"some-agent"}}),
                ),
                review_requirement_ids: None,
                parent_task_id: None,
                version: task.version,
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&refused, crate::ServiceError::InvalidOperation { message }
            if message.contains("review.auditor_agent_id is no longer supported")),
        "{refused:?}"
    );

    // Written by an earlier version.
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(json!({"retry_budgets":{"review":3},"review":{"ci_steps":["true"],"auditor_agent_id":"some-agent"}}).to_string())
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let refused = fixture
        .engine
        .rerun_review(task_uuid(&fixture), owner())
        .await
        .unwrap_err();
    assert!(
        matches!(&refused, crate::ServiceError::Conflict(_))
            && refused
                .to_string()
                .contains("names an auditor and has no reviewer Agent"),
        "{refused:?}"
    );
    assert_eq!(reviews(&fixture).await.len(), 1, "nothing ran");
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        0
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM transition_log WHERE from_state='review' AND to_state='review'"
        )
        .await,
        0
    );
}

// The entry's hooks step is not suspended on its check but claimed (between
// its claim and its suspend, or woken and applying the check's delivery) or
// queued again (woken, not yet claimed). In every one of these a re-run is
// refused as busy: the attempt that is open is never closed under a step
// that still drives it, and no second pipeline starts.
#[tokio::test]
async fn a_rerun_is_refused_while_the_entry_step_is_claimed_or_woken() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    suspend_on_entry_check(&fixture).await;
    for status in ["claimed", "pending", "suspended"] {
        sqlx::query("UPDATE task_step SET status = ? WHERE task_id = ? AND kind = 'hooks' AND status IN ('suspended','claimed','pending')")
            .bind(status)
            .bind(&fixture.task.id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        let refused = fixture
            .engine
            .rerun_review(task_uuid(&fixture), owner())
            .await
            .unwrap_err();
        assert!(
            matches!(refused, crate::ServiceError::TaskBusy { .. }),
            "{status}: {refused:?}"
        );
        let attempts = reviews(&fixture).await;
        assert_eq!(attempts.len(), 1, "{status}");
        assert_eq!(attempts[0].status, db::ReviewStatus::Running, "{status}");
        assert_eq!(
            count(
                &fixture,
                "SELECT COUNT(*) FROM check_consumer WHERE cancelled_at IS NULL"
            )
            .await,
            1,
            "{status}: the open attempt keeps its one live check"
        );
    }
}

pub(super) async fn seed_reviewer_agent(fixture: &FailedCiFixture) -> String {
    let db = &*fixture.db;
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    db::DaemonRepo::upsert_by_machine_id(
        db,
        db::UpsertDaemon {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            machine_id: format!("machine-{daemon_id}"),
            hostname: "test-host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: db::DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::DaemonRepo::update_report(
        db,
        db::UpdateDaemonReport {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            detected_clis_json: r#"[{"kind":"shell","availability":"authenticated"}]"#.to_owned(),
            labels_json: None,
            status: db::DaemonStatus::Online,
            last_report_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let agent_id = new_uuid_v4();
    db::AgentRepo::create(
        db,
        db::CreateAgent {
            id: agent_id.clone(),
            name: "reviewer".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
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
    .unwrap();
    assign_agent_role(db, &fixture.task.id, "reviewer", &agent_id).await;
    agent_id
}

/// The Task is a coordination root (its one child is done and owns the only
/// implementation run) parked in `review` on an exhausted review budget.
async fn park_root_on_exhausted_review_budget(fixture: &FailedCiFixture) {
    if count(
        fixture,
        "SELECT COUNT(*) FROM task WHERE parent_task_id IS NOT NULL",
    )
    .await
        == 0
    {
        let child_id = new_uuid_v4();
        let now = now_rfc3339();
        TaskRepo::create(
            &*fixture.db,
            db::CreateTask {
                id: child_id.clone(),
                project_id: fixture.task.project_id.clone(),
                parent_task_id: Some(fixture.task.id.clone()),
                subtask_order: Some(0),
                assignee_type: None,
                assignee_id: None,
                title: "the root's only subtask".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "done".to_owned(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        sqlx::query("UPDATE execution SET task_id = ? WHERE task_id = ?")
            .bind(&child_id)
            .bind(&fixture.task.id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
    }
    // An automatic entry fails on the red check and spends the last of the
    // review budget: the root parks in `review`.
    sqlx::query("UPDATE task SET status = 'review' WHERE id = ?")
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let spent = review_budget_spent(fixture).await.0;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(
            json!({"retry_budgets":{"review":spent + 1},"review":{"ci_steps":["false"]}})
                .to_string(),
        )
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    fixture
        .engine
        .rerun_review(
            task_uuid(fixture),
            api_types::Actor::system(api_types::SystemComponent::Workflow),
        )
        .await
        .unwrap();
    let parked = drain(fixture.engine.clone(), &fixture.task.id).await;
    assert_eq!(parked.status, "review", "{parked:?}");
    assert!(
        parked
            .blocked_json
            .as_deref()
            .unwrap_or_default()
            .contains("review retry budget exhausted"),
        "{parked:?}"
    );
}

async fn owner_offers(fixture: &FailedCiFixture) -> (crate::TaskSnapshot, Vec<(String, String)>) {
    let actor = owner();
    let snapshot = fixture
        .engine
        .task_action_snapshot(&fixture.task.id, &actor)
        .await
        .unwrap();
    let offers = crate::available_actions(&snapshot)
        .iter()
        .map(|offer| (offer.action.verb().to_owned(), offer.reason.clone()))
        .collect();
    (snapshot, offers)
}

/// Run the Task's queued action and steps until nothing moves any more.
async fn settle_actions(fixture: &FailedCiFixture) -> db::Task {
    let mut task = drain(fixture.engine.clone(), &fixture.task.id).await;
    for _ in 0..6 {
        let _ = fixture
            .engine
            .test_dispatch_task_action(&fixture.task.id)
            .await;
        task = drain(fixture.engine.clone(), &fixture.task.id).await;
    }
    task
}

// Found by the model (`a_coordination_root_that_fails_aggregate_review_can_recover`
// under load). A coordination root with a reviewer Agent is parked on an
// exhausted review budget and its aggregate check is still red. The owner's
// `retry` resets the budget and re-runs the review; the re-run queues the
// reviewer launch that would follow a green entry. The entry fails and the
// workflow sends the root to its working state. That queued launch belonged to
// the entry that just ended: it is dropped. It used to be consumed in the new
// state as "invalid operation: stale queued Task action", which parked the
// root with only `restart` on offer, and `restart` led back to the exhausted
// budget, for ever. The root is now where an automatic failed review leaves
// it, unparked, for the scheduler to take to its aggregate review again; and
// from the exhausted budget the owner can still reopen it for a corrective
// subtask, which is the recovery that makes the check fixable.
#[tokio::test]
async fn an_owner_retry_on_an_exhausted_root_whose_check_is_still_red_never_parks_it_on_a_stale_action(
) {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    seed_reviewer_agent(&fixture).await;
    park_root_on_exhausted_review_budget(&fixture).await;

    let (snapshot, offers) = owner_offers(&fixture).await;
    assert!(snapshot.coordination_root);
    assert!(
        offers.contains(&("retry".to_owned(), "retry_budget_exhausted".to_owned()))
            && offers.contains(&("send_back".to_owned(), "root_review_reopen".to_owned())),
        "{offers:?}"
    );

    const RERUNS: &str = "SELECT COUNT(*) FROM transition_log WHERE from_state='review' AND to_state='review' AND json_extract(bridge_payload,'$.verb')='review_rerun'";
    let reruns = count(&fixture, RERUNS).await;
    fixture
        .engine
        .perform_task_action(
            fixture.task.id.clone(),
            api_types::TaskAction::retry(),
            snapshot.task.version,
        )
        .await
        .expect("the offered retry is accepted");
    let after_retry = settle_actions(&fixture).await;

    assert_eq!(
        count(&fixture, RERUNS).await,
        reruns + 1,
        "the retry re-ran the aggregate review"
    );
    assert_eq!(
        after_retry.status, "in_progress",
        "the red check sent it back"
    );
    let visible = format!(
        "{:?} {:?} {:?} {:?}",
        after_retry.error_annotation,
        after_retry.blocked_json,
        after_retry.failed_json,
        after_retry.metadata_json
    );
    assert!(!visible.contains("stale queued Task action"), "{visible}");
    assert!(
        after_retry.error_annotation.is_none()
            && after_retry.blocked_json.is_none()
            && after_retry.failed_json.is_none(),
        "the root is not parked: {visible}"
    );
    assert!(
        crate::deferred_dispatch::queued_recovery(&after_retry).is_none()
            && crate::deferred_dispatch::pending_until(&after_retry).is_none(),
        "the launch queued for the ended entry is gone: {visible}"
    );
    assert_eq!(synthetic_reviewer_executions(&fixture).await, 0);
    assert_eq!(
        review_budget_spent(&fixture).await.0,
        0,
        "the reset budget is untouched by the owner's failed re-run"
    );

    // The budget runs out again (the check is still red). The owner reopens
    // the root for a corrective subtask: it goes to its working state with a
    // fresh budget and accepts the subtask.
    park_root_on_exhausted_review_budget(&fixture).await;
    let (snapshot, offers) = owner_offers(&fixture).await;
    assert!(
        offers.contains(&("send_back".to_owned(), "root_review_reopen".to_owned())),
        "{offers:?}"
    );
    fixture
        .engine
        .perform_task_action(
            fixture.task.id.clone(),
            api_types::TaskAction::SendBack {
                guidance: "add the missing file".to_owned(),
            },
            snapshot.task.version,
        )
        .await
        .expect("the offered reopen is accepted");
    let reopened = settle_actions(&fixture).await;
    assert_eq!(reopened.status, "in_progress", "{reopened:?}");
    assert_eq!(review_budget_spent(&fixture).await.0, 0, "a fresh budget");
    let visible = format!(
        "{:?} {:?}",
        reopened.error_annotation, reopened.blocked_json
    );
    assert!(!visible.contains("stale queued Task action"), "{visible}");
    let (_, offers) = owner_offers(&fixture).await;
    assert!(
        !offers.iter().any(|(verb, _)| verb == "restart") || offers.len() > 1,
        "the reopened root is not left with `restart` alone: {offers:?}"
    );
}
