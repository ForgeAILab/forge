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
