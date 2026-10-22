//! A re-run cannot strand the old attempt, and a human wait is quiescent.
use super::*;
use db::TaskStepRepo;

struct ReviewFixture {
    db: Arc<db::SqliteDb>,
    task: Task,
    review: db::Review,
    reviewer: db::Execution,
    dispatcher: TaskDispatcher,
    rx: mpsc::UnboundedReceiver<ExecutionContext>,
    _dirs: (TempDir, TempDir),
}

async fn fixture(status: ReviewStatus) -> ReviewFixture {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "review hardening", "review", 0).await;
    assign_role(&db, &task.id, "reviewer", &agent).await;
    let candidate = seed_completed_coder_execution(&db, &task.id).await;
    seed_running_review(&db, &task.id, &candidate, r#"{"ci_steps":[]}"#).await;
    let reviewer = seed_completed_reviewer_execution(&db, &task.id, &agent, Some(&candidate)).await;
    sqlx::query("UPDATE review SET reviewer_execution_id = ?, status = ? WHERE task_id = ?")
        .bind(&reviewer.id)
        .bind(status.to_string())
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap()
        .remove(0);
    let (dispatcher, rx) = build_dispatcher(db.clone(), workspace.path()).await;
    ReviewFixture {
        db,
        task,
        review,
        reviewer,
        dispatcher,
        rx,
        _dirs: (repo, workspace),
    }
}

#[tokio::test]
async fn rerun_entry_write_failure_preserves_attempt_and_dispatcher_recovers_it() {
    let mut f = fixture(ReviewStatus::Running).await;
    // Fail after the old attempt is cancelled, while writing the replacement
    // entry's durable hooks. Both writes must roll back together.
    sqlx::query("CREATE TRIGGER reject_rerun_hooks BEFORE INSERT ON task_step WHEN NEW.kind='hooks' BEGIN SELECT RAISE(ABORT,'injected rerun entry failure'); END")
        .execute(f.db.pool()).await.unwrap();
    let error = f
        .dispatcher
        .task_service
        .rerun_review(
            uuid::Uuid::parse_str(&f.task.id).unwrap(),
            Actor::user(api_types::UserActionSource::Test),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("injected rerun entry failure"),
        "{error}"
    );
    let reviews = ReviewRepo::list_by_task(&*f.db, &f.task.id).await.unwrap();
    assert_eq!(
        reviews,
        vec![f.review.clone()],
        "a failed entry must not cancel the old attempt"
    );
    let task = TaskRepo::get_by_id(&*f.db, &f.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.version, f.task.version, "the entry rolls back too");
    let cancelled_events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type='review.status_changed' AND json_extract(payload_json,'$.status')='cancelled'")
        .fetch_one(f.db.pool()).await.unwrap();
    assert_eq!(cancelled_events, 0, "the cancellation event rolls back too");
    sqlx::query("DROP TRIGGER reject_rerun_hooks")
        .execute(f.db.pool())
        .await
        .unwrap();

    // No second owner request: the dispatcher settles the completed reviewer
    // and automatically retries its missing evidence after the normal delay.
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            f.dispatcher.check_once_and_drain().await.unwrap();
            if let Ok(ctx) = f.rx.try_recv() {
                assert_eq!(ctx.task_id, f.task.id);
                assert_ne!(ctx.execution_id, f.reviewer.id);
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the dispatcher recovers without another Re-run Review");
    let review = ReviewRepo::get_by_id(&*f.db, &f.review.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(review.status, ReviewStatus::Running);
    assert_ne!(review.reviewer_execution_id, Some(f.reviewer.id));
}

#[tokio::test]
async fn awaiting_human_review_adds_no_task_steps_across_120_dispatcher_ticks() {
    let f = fixture(ReviewStatus::AwaitingHuman).await;
    let before = f.db.task_steps(&f.task.id).await.unwrap().len();
    for _ in 0..120 {
        // Include forced sweeps so quiescence cannot merely hide behind the
        // dispatcher's generation cache.
        assert_eq!(f.dispatcher.reconcile_all().await.unwrap(), 0);
        f.dispatcher.task_service.drain(&f.task.id).await.unwrap();
    }
    assert_eq!(
        f.db.task_steps(&f.task.id).await.unwrap().len(),
        before,
        "a completed reviewer awaiting a person has nothing to reconcile"
    );
    assert_eq!(
        ReviewRepo::get_by_id(&*f.db, &f.review.id).await.unwrap(),
        Some(f.review)
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*f.db, &f.task.id, "reviewer")
            .await
            .unwrap(),
        1
    );
}

fn task_uuid(f: &ReviewFixture) -> uuid::Uuid {
    uuid::Uuid::parse_str(&f.task.id).unwrap()
}

fn dispatched(f: &mut ReviewFixture) -> Vec<String> {
    let mut ids = Vec::new();
    while let Ok(ctx) = f.rx.try_recv() {
        assert_eq!(ctx.task_id, f.task.id);
        ids.push(ctx.execution_id);
    }
    ids
}

async fn reviews_by_attempt(f: &ReviewFixture) -> Vec<db::Review> {
    let mut reviews = ReviewRepo::list_by_task(&*f.db, &f.task.id).await.unwrap();
    reviews.sort_by_key(|review| review.attempt_number);
    reviews
}

#[tokio::test]
async fn awaiting_human_rerun_dispatches_exactly_one_replacement_reviewer() {
    let mut f = fixture(ReviewStatus::AwaitingHuman).await;
    f.dispatcher
        .task_service
        .rerun_review(
            task_uuid(&f),
            Actor::user(api_types::UserActionSource::Test),
        )
        .await
        .unwrap();
    // A sweep that runs while the entry's hooks step may still be queued
    // must not start a second reviewer beside the entry's own dispatch.
    f.dispatcher.reconcile_all().await.unwrap();
    f.dispatcher.task_service.drain(&f.task.id).await.unwrap();
    for _ in 0..20 {
        f.dispatcher.reconcile_all().await.unwrap();
        f.dispatcher.task_service.drain(&f.task.id).await.unwrap();
    }
    let launched = dispatched(&mut f);
    assert_eq!(launched.len(), 1, "one replacement reviewer: {launched:?}");
    assert_ne!(launched[0], f.reviewer.id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(&*f.db, &f.task.id, "reviewer")
            .await
            .unwrap(),
        2
    );
    let reviews = reviews_by_attempt(&f).await;
    assert_eq!(reviews.len(), 2, "{reviews:?}");
    assert_eq!(reviews[0].id, f.review.id);
    assert_eq!(reviews[0].status, ReviewStatus::Cancelled);
    let details: serde_json::Value = serde_json::from_str(&reviews[0].step_results_json).unwrap();
    assert_eq!(details["execution_retry"]["status"], "superseded_by_rerun");
    assert_eq!(reviews[1].status, ReviewStatus::Running);
    assert_eq!(
        reviews[1].reviewer_execution_id.as_deref(),
        Some(launched[0].as_str())
    );
}

/// A re-run whose entry committed but whose reviewer dispatch failed (the
/// dispatch hook only logs a failure) leaves the completed reviewer bound to
/// the cancelled newest Review. That attempt has nothing left to settle: the
/// dispatcher must start the next attempt instead of settling it forever.
#[tokio::test]
async fn rerun_whose_dispatch_failed_recovers_without_another_owner_request() {
    let mut f = fixture(ReviewStatus::AwaitingHuman).await;
    sqlx::query("CREATE TRIGGER reject_review_open BEFORE INSERT ON review BEGIN SELECT RAISE(ABORT,'injected review open failure'); END")
        .execute(f.db.pool())
        .await
        .unwrap();
    let _ = f
        .dispatcher
        .task_service
        .rerun_review(
            task_uuid(&f),
            Actor::user(api_types::UserActionSource::Test),
        )
        .await;
    f.dispatcher.task_service.drain(&f.task.id).await.unwrap();
    sqlx::query("DROP TRIGGER reject_review_open")
        .execute(f.db.pool())
        .await
        .unwrap();
    let reviews = reviews_by_attempt(&f).await;
    assert_eq!(reviews.len(), 1, "{reviews:?}");
    assert_eq!(
        reviews[0].status,
        ReviewStatus::Cancelled,
        "the re-run committed"
    );
    assert!(dispatched(&mut f).is_empty());

    let launched = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            f.dispatcher.reconcile_all().await.unwrap();
            f.dispatcher.task_service.drain(&f.task.id).await.unwrap();
            let launched = dispatched(&mut f);
            if !launched.is_empty() {
                return launched;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the dispatcher starts the next attempt without another Re-run Review");
    assert_eq!(launched.len(), 1);
    assert_ne!(launched[0], f.reviewer.id);
    let reviews = reviews_by_attempt(&f).await;
    assert_eq!(reviews.len(), 2, "{reviews:?}");
    assert_eq!(reviews[1].status, ReviewStatus::Running);
    assert_eq!(
        reviews[1].reviewer_execution_id.as_deref(),
        Some(launched[0].as_str())
    );
}
