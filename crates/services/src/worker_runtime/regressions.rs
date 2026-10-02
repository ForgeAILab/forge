use super::*;

// S2: a new declaration under the same stable worker identity updates health
// before entering a handler which cannot update that health itself.
#[tokio::test]
async fn changed_subscription_is_visible_while_new_handler_is_hung() {
    let db = database().await;
    let mut old = TinyWorker::new("subscription-upgrade");
    old.subscription = Subscription::Exact(vec!["a".into()]);
    WorkerRuntime::new(Arc::clone(&db), Arc::new(old))
        .run_once(1)
        .await
        .unwrap();
    append(&db, "b", "hang").await;
    let mut new = TinyWorker::new("subscription-upgrade");
    new.subscription = Subscription::Exact(vec!["a".into(), "b".into()]);
    let worker = Arc::new(new);
    let runtime = Arc::new(WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker)));
    let (tx, rx) = watch::channel(false);
    let handle = runtime.start(rx);
    notified(&worker.entered).await;
    let status = db
        .domain_event_consumer_lag(&["subscription-upgrade"])
        .await
        .unwrap();
    assert_eq!(status[0].lag, 1);
    assert!(status[0].oldest_unprocessed_at.is_some());
    stop(tx, handle).await;
}

// S3: matching rows stay fixed while irrelevant backlog grows. VM work must
// follow the matching index ranges, including a missing prefix, not the head.
#[tokio::test]
async fn live_lag_and_prefix_lookup_use_matching_index_ranges() {
    let mut work = Vec::new();
    for ignored in [100, 20_000] {
        let db = database().await;
        sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at)
            WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x < ?)
            SELECT 'ignored-'||x, 'zzz', 'test', 'x', 'system', 'system', 'test', 'test', '2000-01-01T00:00:00Z' FROM n")
            .bind(ignored).execute(db.pool()).await.unwrap();
        for i in 0..30 {
            append(&db, ["a", "b", "c"][i % 3], "good").await;
        }
        let mut counts = Vec::new();
        for (sub, wanted) in [
            (
                Subscription::Exact(vec!["a".into(), "b".into(), "c".into()]),
                30,
            ),
            (
                Subscription::Prefix(vec!["a".into(), "ab".into(), "a".into()]),
                10,
            ),
            (Subscription::Prefix(vec!["agent.wake.".into()]), 0),
        ] {
            let mut worker = TinyWorker::new("index-work");
            worker.subscription = sub.clone();
            let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(worker));
            runtime.initialize().await.unwrap();
            let steps = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&steps);
            let mut conn = db.pool().acquire().await.unwrap();
            conn.lock_handle()
                .await
                .unwrap()
                .set_progress_handler(1, move || {
                    observed.fetch_add(1, Ordering::Relaxed);
                    true
                });
            drop(conn);
            let status = db.domain_event_consumer_lag(&["index-work"]).await.unwrap();
            assert_eq!(status[0].lag, wanted);
            let found = db
                .next_subscribed_domain_event(0, &sub.normalized())
                .await
                .unwrap();
            assert_eq!(found.is_some(), wanted > 0);
            counts.push(steps.load(Ordering::Relaxed));
            let mut conn = db.pool().acquire().await.unwrap();
            conn.lock_handle().await.unwrap().remove_progress_handler();
        }
        work.push(counts);
    }
    for i in 0..3 {
        assert!(work[1][i] <= work[0][i] + 100, "indexed VM work: {work:?}");
    }
}

struct CycleWorker {
    ticks: AtomicUsize,
    handled: AtomicUsize,
    failing_tick: AtomicBool,
    defer: bool,
}
#[async_trait]
impl Worker for CycleWorker {
    type Prepared = ();
    fn name(&self) -> &str {
        "cycle-worker"
    }
    fn subscription(&self) -> Subscription {
        Subscription::All
    }
    async fn tick(&self) -> std::result::Result<(), WorkerError> {
        self.ticks.fetch_add(1, Ordering::SeqCst);
        if self.failing_tick.load(Ordering::SeqCst) {
            Err(WorkerError::new("tick unavailable"))
        } else {
            Ok(())
        }
    }
    async fn handle(&self, _: &DomainEvent) -> std::result::Result<Outcome<()>, WorkerError> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(if self.defer {
            Outcome::Defer {
                after: Duration::from_secs(3600),
                reason: "whole stream waiting".into(),
            }
        } else {
            Outcome::Done(())
        })
    }
    async fn commit(
        &self,
        _: &mut Transaction<'_, Sqlite>,
        _: &DomainEvent,
        _: &(),
    ) -> std::result::Result<(), WorkerError> {
        Ok(())
    }
}
#[tokio::test]
async fn tick_runs_once_per_cycle_and_failures_do_not_gate_events() {
    let db = database().await;
    for _ in 0..10 {
        append(&db, "a", "good").await;
    }
    let worker = Arc::new(CycleWorker {
        ticks: AtomicUsize::new(0),
        handled: AtomicUsize::new(0),
        failing_tick: AtomicBool::new(true),
        defer: false,
    });
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker));
    runtime.initialize().await.unwrap();
    sqlx::raw_sql("CREATE TABLE tick_writes(n INTEGER); CREATE TRIGGER tick_write AFTER UPDATE OF tick_error ON worker_health BEGIN INSERT INTO tick_writes VALUES (1); END;")
        .execute(db.pool()).await.unwrap();
    assert_eq!(runtime.run_once(10).await.unwrap(), 10);
    assert_eq!(worker.ticks.load(Ordering::SeqCst), 1);
    assert_eq!(worker.handled.load(Ordering::SeqCst), 10);
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM tick_writes").await, 1);
    for expected in [2, 4, 5] {
        runtime.tick_schedule.lock().unwrap().due = None;
        runtime.run_once(1).await.unwrap();
        let remaining = runtime
            .tick_schedule
            .lock()
            .unwrap()
            .due
            .unwrap()
            .duration_since(Instant::now());
        assert!(remaining <= Duration::from_secs(expected));
        assert!(remaining > Duration::from_secs(expected - 1));
    }
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM tick_writes").await,
        1,
        "identical tick errors are deduplicated"
    );
    worker.failing_tick.store(false, Ordering::SeqCst);
    runtime.tick_schedule.lock().unwrap().due = None;
    runtime.run_once(1).await.unwrap();
    let error: Option<String> = sqlx::query_scalar("SELECT tick_error FROM worker_health")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(error.is_none());
}

// S4: wakes during a one-hour deferral run tick without re-handling the event.
#[tokio::test]
async fn long_deferral_ticks_on_wakes_and_bounds_each_sleep() {
    let db = database().await;
    append(&db, "a", "good").await;
    let worker = Arc::new(CycleWorker {
        ticks: AtomicUsize::new(0),
        handled: AtomicUsize::new(0),
        failing_tick: AtomicBool::new(false),
        defer: true,
    });
    let runtime = Arc::new(WorkerRuntime::new(db, Arc::clone(&worker)));
    let (tx, rx) = watch::channel(false);
    let handle = Arc::clone(&runtime).start(rx);
    notified(&runtime.cycle_finished).await;
    for _ in 0..3 {
        runtime.notify.notify_waiters();
        notified(&runtime.cycle_finished).await;
    }
    assert_eq!(worker.handled.load(Ordering::SeqCst), 1);
    assert_eq!(worker.ticks.load(Ordering::SeqCst), 4);
    // Without another notification, the wait is still capped at five seconds.
    tokio::time::timeout(Duration::from_secs(10), runtime.cycle_finished.notified())
        .await
        .expect("bounded wait ticks again");
    assert!(worker.ticks.load(Ordering::SeqCst) >= 5);
    stop(tx, handle).await;
}

// S5: no overflow, hot loop, or unreadable durable deadline for extreme input.
#[tokio::test]
async fn extreme_defer_and_policy_durations_are_bounded_and_readable() {
    for requested in [Duration::ZERO, Duration::MAX] {
        let db = database().await;
        append(&db, "wanted", "defer").await;
        let mut worker = TinyWorker::new("bounds");
        worker.defer_for = requested;
        let worker = Arc::new(worker);
        let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker));
        let before = Utc::now();
        assert_eq!(runtime.run_once(1).await.unwrap(), 0);
        let expected = db::clamp_worker_deferral(requested);
        let state = runtime.health.wait_state().await.unwrap();
        let date = DateTime::parse_from_rfc3339(&state.defer_at.unwrap()).unwrap();
        assert!(date.with_timezone(&Utc) >= before + chrono::Duration::from_std(expected).unwrap());
        assert!(
            date.with_timezone(&Utc) <= Utc::now() + chrono::Duration::from_std(expected).unwrap()
        );
        let fresh = WorkerRuntime::new(Arc::clone(&db), worker);
        assert_eq!(fresh.run_once(1).await.unwrap(), 0);
    }
    for cap in [Duration::ZERO, Duration::from_secs(7), Duration::MAX] {
        let policy = RetryPolicy {
            max_attempts: 8,
            initial_backoff: Duration::MAX,
            max_backoff: cap,
        };
        let delay = policy.delay(1);
        assert!(delay >= Duration::from_secs(1));
        assert!(Instant::now().checked_add(delay).is_some());
    }
}

struct CommitValueWorker {
    post_steps: AtomicUsize,
    committed: bool,
}
#[async_trait]
impl Worker<bool> for CommitValueWorker {
    type Prepared = ();
    fn name(&self) -> &str {
        "commit-value"
    }
    fn subscription(&self) -> Subscription {
        Subscription::All
    }
    async fn handle(&self, _: &DomainEvent) -> std::result::Result<Outcome<()>, WorkerError> {
        Ok(Outcome::Done(()))
    }
    async fn commit(
        &self,
        _: &mut Transaction<'_, Sqlite>,
        _: &DomainEvent,
        _: &(),
    ) -> std::result::Result<bool, WorkerError> {
        Ok(self.committed)
    }
    async fn after_commit(
        &self,
        _: &DomainEvent,
        _: &(),
        admitted: &bool,
    ) -> std::result::Result<(), WorkerError> {
        if *admitted {
            self.post_steps.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}
#[tokio::test]
async fn post_commit_receives_actual_commit_result() {
    for admitted in [false, true] {
        let db = database().await;
        append(&db, "wanted", "good").await;
        let worker = Arc::new(CommitValueWorker {
            post_steps: AtomicUsize::new(0),
            committed: admitted,
        });
        let runtime = WorkerRuntime::new(db, Arc::clone(&worker));
        assert_eq!(runtime.run_once(1).await.unwrap(), 1);
        assert_eq!(
            worker.post_steps.load(Ordering::SeqCst),
            usize::from(admitted)
        );
    }
}
struct SkipWorker;
#[async_trait]
impl Worker for SkipWorker {
    type Prepared = ();
    fn name(&self) -> &str {
        "skip-worker"
    }
    fn subscription(&self) -> Subscription {
        Subscription::All
    }
    async fn handle(&self, event: &DomainEvent) -> std::result::Result<Outcome<()>, WorkerError> {
        Ok(if event.entity_id == "good" {
            Outcome::Done(())
        } else {
            Outcome::Skip
        })
    }
    async fn commit(
        &self,
        _: &mut Transaction<'_, Sqlite>,
        _: &DomainEvent,
        _: &(),
    ) -> std::result::Result<(), WorkerError> {
        Ok(())
    }
}
#[tokio::test]
async fn skipped_subscribed_events_share_lazy_ignored_checkpoint() {
    let db = database().await;
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(SkipWorker));
    runtime.initialize().await.unwrap();
    let before = runtime.write_transaction_count();
    for _ in 0..20 {
        append(&db, "whatever", "unclassified").await;
        assert_eq!(runtime.run_once(1).await.unwrap(), 0);
    }
    assert_eq!(runtime.write_transaction_count(), before);
    assert_eq!(runtime.source.cursor().await.unwrap(), 0);
    let wanted = append(&db, "whatever", "good").await;
    assert_eq!(runtime.run_once(1).await.unwrap(), 1);
    assert_eq!(runtime.write_transaction_count() - before, 1);
    assert_eq!(runtime.source.cursor().await.unwrap(), wanted.sequence);
}

#[tokio::test]
async fn runtime_error_cause_clears_on_empty_poll_but_item_error_survives() {
    let db = database().await;
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(TinyWorker::new("health-scopes")));
    runtime.initialize().await.unwrap();
    runtime
        .health
        .report_error("specific runtime cause")
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE clears(n INTEGER); CREATE TRIGGER runtime_clear AFTER UPDATE OF runtime_error ON worker_health
        WHEN OLD.runtime_error IS NOT NULL AND NEW.runtime_error IS NULL BEGIN INSERT INTO clears VALUES (1); END;").execute(db.pool()).await.unwrap();
    runtime.run_once(1).await.unwrap();
    runtime.run_once(1).await.unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM clears").await, 1);
    let event = append(&db, "wanted", "poison").await;
    runtime.run_once(1).await.unwrap();
    runtime
        .health
        .report_error("temporary database cause")
        .await
        .unwrap();
    runtime.run_once(1).await.unwrap();
    let state: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT item_error, runtime_error FROM worker_health")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(state.0.as_deref(), Some("bad event"));
    assert!(state.1.is_none());
    assert_eq!(runtime.source.cursor().await.unwrap(), event.sequence - 1);
}

#[tokio::test]
async fn transient_retries_have_an_independent_exponential_idle_cap() {
    let db = database().await;
    append(&db, "wanted", "good").await;
    let worker = Arc::new(TinyWorker::new("transient-schedule"));
    worker.transient.store(true, Ordering::SeqCst);
    let runtime = WorkerRuntime::new(Arc::clone(&db), worker);
    for expected in [1, 2, 4, 5, 5] {
        assert!(
            matches!(runtime.poll_once().await.unwrap(), PollResult::InfrastructureRetry(delay) if delay == Duration::from_secs(expected))
        );
        due(&runtime).await;
    }
    assert_eq!(
        scalar(&db, "SELECT retry_attempts FROM worker_health").await,
        0
    );
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM worker_dead_letter").await,
        0
    );
}
