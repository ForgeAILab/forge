use super::*;
use db::{create_sqlite_pool, new_uuid_v4, run_migrations, CreateDomainEvent, DomainEventRepo};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

struct TinyWorker {
    name: &'static str,
    seen: Mutex<Vec<i64>>,
    commit_failures: AtomicUsize,
    transient: AtomicBool,
    commit_transient: AtomicBool,
    commit_terminal_once: AtomicBool,
    commit_terminal_always: bool,
    skip_on_refresh: bool,
    ticks: AtomicUsize,
    tick_timeout: Duration,
    active: AtomicUsize,
    after_failure: bool,
    tick_panics: AtomicUsize,
    tick_hangs: AtomicBool,
    defer_for: Duration,
    policy: RetryPolicy,
    timeout: Duration,
    subscription: Subscription,
    entered: Notify,
    append_in_commit: bool,
}
impl TinyWorker {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            seen: Mutex::new(Vec::new()),
            commit_failures: AtomicUsize::new(0),
            transient: AtomicBool::new(false),
            commit_transient: AtomicBool::new(false),
            commit_terminal_once: AtomicBool::new(false),
            commit_terminal_always: false,
            skip_on_refresh: false,
            ticks: AtomicUsize::new(0),
            tick_timeout: Duration::from_secs(30),
            active: AtomicUsize::new(0),
            after_failure: false,
            tick_panics: AtomicUsize::new(0),
            tick_hangs: AtomicBool::new(false),
            defer_for: Duration::ZERO,
            policy: RetryPolicy {
                max_attempts: 3,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
            },
            timeout: Duration::from_secs(300),
            subscription: Subscription::Exact(vec!["wanted".into()]),
            entered: Notify::new(),
            append_in_commit: false,
        }
    }
}
#[async_trait]
impl Worker for TinyWorker {
    type Prepared = i64;
    fn name(&self) -> &str {
        self.name
    }
    fn subscription(&self) -> Subscription {
        self.subscription.clone()
    }
    fn retry_policy(&self) -> RetryPolicy {
        self.policy
    }
    fn handle_timeout(&self) -> Duration {
        self.timeout
    }
    fn tick_timeout(&self) -> Duration {
        self.tick_timeout
    }
    async fn tick(&self) -> std::result::Result<(), WorkerError> {
        self.ticks.fetch_add(1, Ordering::SeqCst);
        if self.tick_hangs.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self
            .tick_panics
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            panic!("runtime hook panic");
        }
        Ok(())
    }
    async fn handle(&self, event: &DomainEvent) -> std::result::Result<Outcome<i64>, WorkerError> {
        self.seen.lock().unwrap().push(event.sequence);
        self.entered.notify_one();
        if self.skip_on_refresh && !self.commit_terminal_once.load(Ordering::SeqCst) {
            return Ok(Outcome::Skip);
        }
        if self.transient.load(Ordering::SeqCst) {
            return Err(WorkerError::transient("database unavailable"));
        }
        match event.entity_id.as_str() {
            "panic" => panic!("poison handler"),
            "poison" => Err(WorkerError::new("bad event")),
            "defer" => Ok(Outcome::Defer {
                after: self.defer_for,
                reason: "waiting for readiness".into(),
            }),
            "terminal" => Ok(Outcome::DeadLetter {
                reason: "deleted source".into(),
            }),
            "hang" => {
                self.active.fetch_add(1, Ordering::SeqCst);
                struct Active<'a>(&'a AtomicUsize);
                impl Drop for Active<'_> {
                    fn drop(&mut self) {
                        self.0.fetch_sub(1, Ordering::SeqCst);
                    }
                }
                let _active = Active(&self.active);
                // Intentional hung worker; production timeout/shutdown and
                // the test's bounded outer waits are the guards.
                std::future::pending().await
            }
            _ => Ok(Outcome::Done(event.sequence)),
        }
    }
    async fn commit(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        event: &DomainEvent,
        prepared: &i64,
    ) -> std::result::Result<(), WorkerError> {
        sqlx::query("INSERT INTO worker_test_effect (sequence) VALUES (?)")
            .bind(prepared)
            .execute(&mut **tx)
            .await
            .map_err(|e| WorkerError::transient(e.to_string()))?;
        if self.commit_terminal_once.swap(false, Ordering::SeqCst) || self.commit_terminal_always {
            return Err(WorkerError::terminal("stale or rejected domain snapshot"));
        }
        if self.commit_transient.load(Ordering::SeqCst) {
            return Err(WorkerError::transient("commit concurrency conflict"));
        }
        if event.entity_id == "commit-panic" {
            panic!("poison commit");
        }
        if self
            .commit_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(WorkerError::new("failure after effect write"));
        }
        if self.append_in_commit {
            // Same event append implementation used by main_genesis_commands:
            // caller-owned transaction, no explicit notifier at the call site.
            sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id,
                actor_type, scope_type, scope_id, correlation_id, created_at)
                VALUES (?, 'emitted', 'test', 'from-commit', 'system', 'system', 'test', 'test', ?)")
                .bind(new_uuid_v4()).bind(db::now_rfc3339()).execute(&mut **tx).await
                .map_err(|e| WorkerError::transient(e.to_string()))?;
        }
        Ok(())
    }
    async fn after_commit(
        &self,
        _event: &DomainEvent,
        _prepared: &i64,
        _committed: &(),
    ) -> std::result::Result<(), WorkerError> {
        if self.after_failure {
            Err(WorkerError::new("post-commit failed"))
        } else {
            Ok(())
        }
    }
}
async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    sqlx::query("CREATE TABLE worker_test_effect (sequence INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    Arc::new(SqliteDb::new(pool))
}
fn input(event_type: &str, entity: &str) -> CreateDomainEvent {
    let id = new_uuid_v4();
    CreateDomainEvent {
        id: id.clone(),
        event_type: event_type.into(),
        entity_type: "test".into(),
        entity_id: entity.into(),
        actor_type: "system".into(),
        actor_id: None,
        scope_type: "system".into(),
        scope_id: "test".into(),
        correlation_id: id,
        causation_id: None,
        causation_depth: 0,
        dedupe_key: None,
        payload_json: "{}".into(),
        created_at: "2000-01-01T00:00:00Z".into(),
    }
}
async fn append(db: &SqliteDb, kind: &str, entity: &str) -> DomainEvent {
    db.append_event(input(kind, entity)).await.unwrap()
}
async fn scalar(db: &SqliteDb, query: &str) -> i64 {
    sqlx::query_scalar(query)
        .fetch_one(db.pool())
        .await
        .unwrap()
}
async fn effects(db: &SqliteDb) -> Vec<i64> {
    sqlx::query_scalar("SELECT sequence FROM worker_test_effect ORDER BY sequence")
        .fetch_all(db.pool())
        .await
        .unwrap()
}
async fn stop(tx: watch::Sender<bool>, handle: JoinHandle<()>) {
    tx.send_replace(true);
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("prompt shutdown")
        .unwrap();
}
async fn wait_for<F, Fut>(mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(10), async {
        while !check().await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition reached");
}

async fn due<W: Worker<C>, C: Send + Sync + 'static>(runtime: &WorkerRuntime<W, C>) {
    *runtime.wait.lock().unwrap() = None;
    sqlx::query(
        "UPDATE worker_health SET
        retry_not_before = CASE WHEN retry_source_key IS NOT NULL THEN '2000-01-01T00:00:00Z' END,
        defer_not_before = CASE WHEN deferred_source_key IS NOT NULL THEN '2000-01-01T00:00:00Z' END
        WHERE worker_name = ?",
    )
    .bind(runtime.worker.name())
    .execute(runtime.db.pool())
    .await
    .unwrap();
}
async fn notified(signal: &Notify) {
    tokio::time::timeout(Duration::from_secs(30), signal.notified())
        .await
        .expect("notification arrived");
}

#[tokio::test]
async fn effect_and_cursor_commit_together() {
    let db = database().await;
    let event = append(&db, "wanted", "atomic").await;
    let mut worker = TinyWorker::new("atomic");
    worker.commit_failures = AtomicUsize::new(1);
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(worker));
    assert_eq!(runtime.run_once(1).await.unwrap(), 0);
    assert!(effects(&db).await.is_empty());
    assert_eq!(runtime.source.cursor().await.unwrap(), 0);
    due(&runtime).await;
    assert_eq!(runtime.run_once(1).await.unwrap(), 1);
    assert_eq!(effects(&db).await, [event.sequence]);
    assert_eq!(runtime.source.cursor().await.unwrap(), event.sequence);
}

#[tokio::test]
async fn strict_order_sql_filter_and_no_per_ignored_event_writes() {
    let db = database().await;
    for _ in 0..25 {
        append(&db, "ignored", "ignored").await;
    }
    let first = append(&db, "wanted", "first").await;
    for _ in 0..25 {
        append(&db, "ignored", "ignored").await;
    }
    let second = append(&db, "wanted", "second").await;
    append(&db, "ignored", "tail").await;
    let worker = Arc::new(TinyWorker::new("filtered"));
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker));
    runtime.initialize().await.unwrap();
    let before = runtime.write_transaction_count();
    assert_eq!(runtime.run_once(10).await.unwrap(), 2);
    assert_eq!(
        *worker.seen.lock().unwrap(),
        [first.sequence, second.sequence]
    );
    assert_eq!(runtime.write_transaction_count() - before, 2);
    assert_eq!(runtime.source.cursor().await.unwrap(), second.sequence);
}

// P4. Advance explicit Tokio time while each ignored append is observed.
#[tokio::test]
async fn trickled_ignored_events_are_checkpointed_at_most_once_per_idle_interval() {
    let db = database().await;
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(TinyWorker::new("trickle")));
    runtime.initialize().await.unwrap();
    let before = runtime.write_transaction_count();
    for _ in 0..20 {
        append(&db, "ignored", "ignored").await;
        assert!(matches!(
            runtime.poll_once().await.unwrap(),
            PollResult::Idle
        ));
    }
    assert_eq!(runtime.write_transaction_count(), before);
    runtime.source.make_flush_due();
    assert!(matches!(
        runtime.poll_once().await.unwrap(),
        PollResult::Progress
    ));
    assert_eq!(runtime.write_transaction_count() - before, 1);
    assert_eq!(runtime.source.cursor().await.unwrap(), 20);
}

#[tokio::test]
async fn retry_attempts_survive_new_runtime_then_cap_and_process_next() {
    let db = database().await;
    let first = append(&db, "wanted", "poison").await;
    let second = append(&db, "wanted", "next").await;
    let worker = Arc::new(TinyWorker::new("retry"));
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker));
    assert_eq!(runtime.run_once(10).await.unwrap(), 0);
    due(&runtime).await;
    drop(runtime);
    let runtime = WorkerRuntime::new(Arc::clone(&db), worker);
    assert_eq!(runtime.run_once(10).await.unwrap(), 0);
    assert_eq!(
        scalar(
            &db,
            "SELECT retry_attempts FROM worker_health WHERE worker_name = 'retry'"
        )
        .await,
        2
    );
    due(&runtime).await;
    assert_eq!(runtime.run_once(10).await.unwrap(), 2);
    let dead: (String, i64) = sqlx::query_as("SELECT source_key, attempts FROM worker_dead_letter")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(dead, (first.sequence.to_string(), 3));
    assert_eq!(effects(&db).await, [second.sequence]);
}

// P2 replaces the old test that enshrined infinite supervisor restarts.
#[tokio::test]
async fn panicking_handle_and_commit_are_capped_without_supervisor_restarts() {
    for poison in ["panic", "commit-panic"] {
        let db = database().await;
        let bad = append(&db, "wanted", poison).await;
        let next = append(&db, "wanted", "next").await;
        let worker = Arc::new(TinyWorker::new("panic"));
        let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker));
        for _ in 0..2 {
            assert_eq!(runtime.run_once(1).await.unwrap(), 0);
            due(&runtime).await;
        }
        assert_eq!(runtime.run_once(10).await.unwrap(), 2);
        let dead: (String, i64, String) =
            sqlx::query_as("SELECT source_key, attempts, last_error FROM worker_dead_letter")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(dead.0, bad.sequence.to_string());
        assert_eq!(dead.1, 3);
        assert!(dead.2.contains("panicked"));
        assert_eq!(effects(&db).await, [next.sequence]);
    }
}

// P3. Failing the runtime's health update rolls back the worker effect and
// acknowledgement, WITHOUT incrementing attempts even after exceeding the cap.
#[tokio::test]
async fn runtime_internal_failures_never_strike_and_recovery_applies_effect_once() {
    let db = database().await;
    let event = append(&db, "wanted", "good").await;
    let worker = Arc::new(TinyWorker::new("infra"));
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker));
    runtime.initialize().await.unwrap();
    sqlx::query(
        "CREATE TRIGGER infra_failure BEFORE UPDATE OF last_success_at ON worker_health
        BEGIN SELECT RAISE(ABORT, 'simulated disk I/O error'); END",
    )
    .execute(db.pool())
    .await
    .unwrap();
    for _ in 0..10 {
        assert!(runtime.run_once(1).await.is_err());
    }
    assert_eq!(
        scalar(
            &db,
            "SELECT retry_attempts FROM worker_health WHERE worker_name = 'infra'"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM worker_dead_letter").await,
        0
    );
    assert_eq!(runtime.source.cursor().await.unwrap(), 0);
    assert!(effects(&db).await.is_empty());
    let cause: String =
        sqlx::query_scalar("SELECT runtime_error FROM worker_health WHERE worker_name = 'infra'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert!(cause.contains("simulated disk I/O error"));
    sqlx::query("DROP TRIGGER infra_failure")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(runtime.run_once(10).await.unwrap(), 1);
    assert_eq!(effects(&db).await, [event.sequence]);
    assert_eq!(worker.seen.lock().unwrap().len(), 11);
}

#[tokio::test]
async fn transient_worker_errors_never_strike_or_dead_letter() {
    let db = database().await;
    let event = append(&db, "wanted", "good").await;
    let worker = Arc::new(TinyWorker::new("transient"));
    worker.transient.store(true, Ordering::SeqCst);
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker));
    for _ in 0..10 {
        assert_eq!(runtime.run_once(1).await.unwrap(), 0);
        due(&runtime).await;
    }
    assert_eq!(
        scalar(
            &db,
            "SELECT retry_attempts FROM worker_health WHERE worker_name = 'transient'"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM worker_dead_letter").await,
        0
    );
    worker.transient.store(false, Ordering::SeqCst);
    worker.commit_transient.store(true, Ordering::SeqCst);
    for _ in 0..10 {
        assert_eq!(runtime.run_once(1).await.unwrap(), 0);
        due(&runtime).await;
    }
    assert!(effects(&db).await.is_empty());
    assert_eq!(
        scalar(
            &db,
            "SELECT retry_attempts FROM worker_health WHERE worker_name = 'transient'"
        )
        .await,
        0
    );
    worker.commit_transient.store(false, Ordering::SeqCst);
    assert_eq!(runtime.run_once(1).await.unwrap(), 1);
    assert_eq!(effects(&db).await, [event.sequence]);
}

#[tokio::test]
async fn defer_is_uncapped_keeps_order_reason_and_first_time() {
    let db = database().await;
    let first = append(&db, "wanted", "defer").await;
    append(&db, "wanted", "next").await;
    let worker = Arc::new(TinyWorker::new("defer"));
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker));
    assert_eq!(runtime.run_once(10).await.unwrap(), 0);
    let initial: String =
        sqlx::query_scalar("SELECT deferred_since FROM worker_health WHERE worker_name = 'defer'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    for _ in 0..20 {
        due(&runtime).await;
        assert_eq!(runtime.run_once(10).await.unwrap(), 0);
    }
    let state: (i64, String, String, Option<String>) = sqlx::query_as(
        "SELECT retry_attempts, deferred_reason, deferred_since, last_error FROM worker_health WHERE worker_name = 'defer'")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(state, (0, "waiting for readiness".into(), initial, None));
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM worker_dead_letter").await,
        0
    );
    assert_eq!(runtime.source.cursor().await.unwrap(), 0);
    assert!(worker
        .seen
        .lock()
        .unwrap()
        .iter()
        .all(|s| *s == first.sequence));
}

#[tokio::test]
async fn shutdown_interrupts_deferral_and_strike_backoff() {
    for (name, entity) in [("defer-shutdown", "defer"), ("strike-shutdown", "poison")] {
        let db = database().await;
        append(&db, "wanted", entity).await;
        let mut worker = TinyWorker::new(name);
        worker.defer_for = Duration::from_secs(3600);
        worker.policy.initial_backoff = Duration::from_secs(300);
        worker.policy.max_backoff = Duration::from_secs(300);
        let runtime = Arc::new(WorkerRuntime::new(Arc::clone(&db), Arc::new(worker)));
        let (tx, rx) = watch::channel(false);
        let handle = runtime.start(rx);
        wait_for(|| async { scalar(&db, "SELECT COUNT(*) FROM worker_health WHERE retry_attempts > 0 OR deferred_since IS NOT NULL").await == 1 }).await;
        stop(tx, handle).await;
    }
}

#[tokio::test]
async fn direct_dead_letter_advances_without_a_strike() {
    let db = database().await;
    let bad = append(&db, "wanted", "terminal").await;
    let next = append(&db, "wanted", "next").await;
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(TinyWorker::new("terminal")));
    assert_eq!(runtime.run_once(10).await.unwrap(), 2);
    let dead: (String, i64) = sqlx::query_as("SELECT source_key, attempts FROM worker_dead_letter")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(dead, (bad.sequence.to_string(), 0));
    assert_eq!(effects(&db).await, [next.sequence]);
}

#[tokio::test]
async fn health_error_persists_until_success_and_backlog_is_live() {
    let db = database().await;
    let first = append(&db, "wanted", "first").await;
    append(&db, "ignored", "ignored").await;
    let second = append(&db, "wanted", "second").await;
    let mut worker = TinyWorker::new("health");
    worker.commit_failures = AtomicUsize::new(1);
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(worker));
    assert_eq!(runtime.run_once(1).await.unwrap(), 0);
    runtime.initialize().await.unwrap();
    assert!(sqlx::query_scalar::<_, Option<String>>(
        "SELECT last_error FROM worker_health WHERE worker_name = 'health'"
    )
    .fetch_one(db.pool())
    .await
    .unwrap()
    .unwrap()
    .contains("failure after effect write"));
    let lag = db.domain_event_consumer_lag(&["health"]).await.unwrap();
    assert_eq!(lag[0].lag, 2);
    assert_eq!(
        lag[0].oldest_unprocessed_at.as_deref(),
        Some("2000-01-01T00:00:00Z")
    );
    due(&runtime).await;
    assert_eq!(runtime.run_once(10).await.unwrap(), 2);
    let lag = db.domain_event_consumer_lag(&["health"]).await.unwrap();
    assert_eq!(lag[0].lag, 0);
    assert!(lag[0].oldest_unprocessed_at.is_none());
    let state: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT last_error, last_success_at FROM worker_health WHERE worker_name = 'health'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(state.0.is_none());
    assert!(state.1.is_some());
    assert_eq!(effects(&db).await, [first.sequence, second.sequence]);
}

// P1 and P5. Explicit entry barrier, then paused time for the timeout. A false
// shutdown-channel update cannot create a second concurrent handler.
#[tokio::test]
async fn hung_handler_has_live_lag_timeout_strikes_and_false_watch_does_not_duplicate() {
    let db = database().await;
    append(&db, "wanted", "hang").await;
    let mut tiny = TinyWorker::new("hung");
    tiny.timeout = Duration::from_secs(300);
    tiny.policy.initial_backoff = Duration::from_secs(300);
    tiny.policy.max_backoff = Duration::from_secs(300);
    let worker = Arc::new(tiny);
    let runtime = Arc::new(WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker)));
    let (tx, rx) = watch::channel(false);
    let handle = Arc::clone(&runtime).start(rx);
    tokio::time::timeout(Duration::from_secs(10), worker.entered.notified())
        .await
        .unwrap();
    let reported = db.domain_event_consumer_lag(&["hung"]).await.unwrap();
    assert_eq!(reported[0].lag, 1);
    assert_eq!(
        reported[0].oldest_unprocessed_at.as_deref(),
        Some("2000-01-01T00:00:00Z")
    );
    assert_eq!(worker.active.load(Ordering::SeqCst), 1);
    tx.send_replace(false);
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::time::sleep(Duration::from_secs(1)),
    )
    .await
    .unwrap();
    assert_eq!(worker.seen.lock().unwrap().len(), 1);
    assert_eq!(runtime.loop_starts.load(Ordering::SeqCst), 1);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    // Resume for DB persistence, avoiding paused-time auto-advance during SQLx IO.
    tokio::time::resume();
    wait_for(|| async {
        scalar(
            &db,
            "SELECT retry_attempts FROM worker_health WHERE worker_name = 'hung'",
        )
        .await
            == 1
    })
    .await;
    assert_eq!(
        scalar(
            &db,
            "SELECT restart_count FROM worker_health WHERE worker_name = 'hung'"
        )
        .await,
        0
    );
    assert_eq!(
        worker.active.load(Ordering::SeqCst),
        0,
        "timeout drops the hung future"
    );
    stop(tx, handle).await;
}

#[tokio::test]
async fn after_commit_failure_reports_health_without_rollback_or_strike() {
    let db = database().await;
    let event = append(&db, "wanted", "good").await;
    let mut tiny = TinyWorker::new("after");
    tiny.after_failure = true;
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(tiny));
    assert_eq!(runtime.run_once(10).await.unwrap(), 1);
    assert_eq!(effects(&db).await, [event.sequence]);
    assert_eq!(runtime.source.cursor().await.unwrap(), event.sequence);
    let state: (i64, String) = sqlx::query_as(
        "SELECT retry_attempts, last_error FROM worker_health WHERE worker_name = 'after'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(state, (0, "post-commit failed".into()));
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM worker_dead_letter").await,
        0
    );
}

#[tokio::test]
async fn prefix_and_all_subscriptions_and_tick_support_non_event_work() {
    for subscription in [
        Subscription::Prefix(vec!["agent.wake.".into()]),
        Subscription::All,
    ] {
        let db = database().await;
        append(&db, "ignored", "ignored").await;
        let wanted = append(&db, "agent.wake.ready", "good").await;
        let mut tiny = TinyWorker::new("subscription");
        tiny.subscription = subscription.clone();
        tiny.tick_panics = AtomicUsize::new(1);
        let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(tiny));
        let expected = if matches!(subscription, Subscription::All) {
            2
        } else {
            1
        };
        assert_eq!(runtime.run_once(10).await.unwrap(), expected);
        assert!(effects(&db).await.contains(&wanted.sequence));
        assert_eq!(
            scalar(&db, "SELECT retry_attempts FROM worker_health").await,
            0
        );
    }
}

// P6. SQLite VM instruction counts include every poll/commit statement, not
// wall time. Reintroducing COUNT(backlog) in a write transaction fails this.
#[tokio::test]
async fn per_event_commit_cost_is_independent_of_wanted_backlog() {
    let mut counts = Vec::new();
    for backlog in [10, 20_000] {
        let db = database().await;
        sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type,
            scope_type, scope_id, correlation_id, created_at)
            WITH RECURSIVE counter(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM counter WHERE x < ?)
            SELECT 'event-' || x, CASE WHEN x % 3 = 0 THEN 'wanted-two' ELSE 'wanted' END, 'test', 'good', 'system', 'system', 'test', 'test', '2000-01-01T00:00:00Z' FROM counter")
            .bind(backlog).execute(db.pool()).await.unwrap();
        let mut worker = TinyWorker::new("cost");
        worker.subscription = Subscription::Exact(vec![
            "wanted".into(),
            "wanted-two".into(),
            "wanted-three".into(),
        ]);
        let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(worker));
        runtime.initialize().await.unwrap();
        let steps = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&steps);
        let mut connection = db.pool().acquire().await.unwrap();
        connection
            .lock_handle()
            .await
            .unwrap()
            .set_progress_handler(1, move || {
                counter.fetch_add(1, Ordering::Relaxed);
                true
            });
        drop(connection);
        assert_eq!(runtime.run_once(1).await.unwrap(), 1);
        counts.push(steps.load(Ordering::Relaxed));
        let mut connection = db.pool().acquire().await.unwrap();
        connection
            .lock_handle()
            .await
            .unwrap()
            .remove_progress_handler();
    }
    assert!(
        counts[1] <= counts[0] + 100,
        "VM work must not scale with backlog: {counts:?}"
    );
}

#[tokio::test]
async fn caught_up_idle_poll_opens_no_write_transaction_or_pool_write() {
    let db = database().await;
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(TinyWorker::new("idle")));
    runtime.initialize().await.unwrap();
    let before = runtime.write_transaction_count();
    sqlx::query("CREATE TABLE observed_metadata_write (n INTEGER)")
        .execute(db.pool())
        .await
        .unwrap();
    for table in ["worker_health", "event_consumer_cursor"] {
        for operation in ["INSERT", "UPDATE", "DELETE"] {
            sqlx::query(&format!(
                "CREATE TRIGGER observed_{table}_{operation} AFTER {operation} ON {table}
                BEGIN INSERT INTO observed_metadata_write VALUES (1); END"
            ))
            .execute(db.pool())
            .await
            .unwrap();
        }
    }
    for _ in 0..10 {
        assert!(matches!(
            runtime.poll_once().await.unwrap(),
            PollResult::Idle
        ));
    }
    assert_eq!(runtime.write_transaction_count(), before);
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM observed_metadata_write").await,
        0
    );
}

// P8: old live leases neither block nor duplicate delivery; cleanup belongs to
// the memory-only migration (the generic library preserves other names).
#[tokio::test]
async fn upgrade_reuses_cursor_ignores_legacy_leases_and_handles_once() {
    let db = database().await;
    let old = append(&db, "wanted", "old").await;
    let next = append(&db, "wanted", "new").await;
    sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES ('upgrade', ?, ?)")
        .bind(old.sequence).bind(db::now_rfc3339()).execute(db.pool()).await.unwrap();
    legacy_delivery(&db, "upgrade").await;
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(TinyWorker::new("upgrade")));
    assert_eq!(runtime.run_once(10).await.unwrap(), 1);
    assert_eq!(runtime.run_once(10).await.unwrap(), 0);
    assert_eq!(effects(&db).await, [next.sequence]);
    assert_eq!(
        scalar(
            &db,
            "SELECT COUNT(*) FROM event_processing_lease WHERE consumer_name = 'upgrade'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn deleted_health_is_recreated_and_retry_policy_defaults_are_real() {
    let db = database().await;
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(TinyWorker::new("recreated")));
    runtime.initialize().await.unwrap();
    sqlx::query("DELETE FROM worker_health")
        .execute(db.pool())
        .await
        .unwrap();
    append(&db, "wanted", "good").await;
    assert_eq!(runtime.run_once(1).await.unwrap(), 1);
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM worker_health").await, 1);
    let policy = RetryPolicy::default();
    assert_eq!(policy.max_attempts, 8);
    assert_eq!(policy.delay(1), Duration::from_secs(1));
    assert_eq!(policy.delay(50), Duration::from_secs(300));
}

#[tokio::test]
async fn supervisor_restarts_runtime_panics_counts_initialization_failures_and_stops_in_backoff() {
    let db = database().await;
    let health = WorkerHealth::new(Arc::clone(&db), "supervised");
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let (tx, rx) = watch::channel(false);
    let handle = WorkerSupervisor::new(
        health,
        SupervisorPolicy {
            initial_backoff: Duration::from_secs(300),
            max_backoff: Duration::from_secs(300),
            healthy_period: Duration::from_secs(60),
        },
    )
    .start(
        move |_| {
            let count = Arc::clone(&count);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                panic!("runtime-level panic");
                #[allow(unreachable_code)]
                Ok(())
            }
        },
        rx,
    );
    wait_for(|| async {
        scalar(
            &db,
            "SELECT COUNT(*) FROM worker_health WHERE restart_count = 1",
        )
        .await
            == 1
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    stop(tx, handle).await;

    // Fail event-source initialization, while the source-neutral health table
    // remains available. The restart must still be persisted.
    sqlx::query(
        "CREATE TRIGGER bad_init BEFORE INSERT ON event_consumer_cursor
        BEGIN SELECT RAISE(ABORT, 'initialization fails'); END",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let runtime = Arc::new(WorkerRuntime::new(
        Arc::clone(&db),
        Arc::new(TinyWorker::new("bad-init")),
    ));
    let (tx, rx) = watch::channel(false);
    let handle = runtime.start(rx);
    wait_for(|| async { scalar(&db, "SELECT COUNT(*) FROM worker_health WHERE worker_name = 'bad-init' AND restart_count >= 1").await == 1 }).await;
    stop(tx, handle).await;
}

// A new append path has NO notify call. Hold the loop at maximum idle backoff,
// append in a caller-owned transaction, and require entry before that timer.
#[tokio::test]
async fn append_in_caller_transaction_and_runtime_commit_wake_without_callsite_notify() {
    for from_runtime in [false, true] {
        // Multiple connections expose the pre-visibility wake-up race that
        // an in-memory, single-connection pool would mask.
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}",
            directory.path().join("wake.sqlite").display()
        );
        let pool = create_sqlite_pool(&url).await.unwrap();
        run_migrations(&pool).await.unwrap();
        sqlx::query("CREATE TABLE worker_test_effect (sequence INTEGER PRIMARY KEY)")
            .execute(&pool)
            .await
            .unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        let mut receiving = TinyWorker::new("receiver");
        receiving.subscription = Subscription::Exact(vec!["emitted".into()]);
        let receiver = Arc::new(receiving);
        let runtime = Arc::new(WorkerRuntime::new(Arc::clone(&db), Arc::clone(&receiver)));
        runtime.initialize().await.unwrap();
        let (tx, rx) = watch::channel(false);
        let running = Arc::clone(&runtime);
        let handle = tokio::spawn(async move {
            running.run_loop_with_idle(rx, MAX_IDLE).await.unwrap();
        });
        tokio::time::timeout(Duration::from_secs(10), runtime.idle_entered.notified())
            .await
            .unwrap();
        if from_runtime {
            let mut emitting = TinyWorker::new("emitter");
            emitting.append_in_commit = true;
            let emit_runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(emitting));
            append(&db, "wanted", "good").await;
            // Consume the setup append's wake and return the receiver to its
            // maximum idle wait. Only the runtime commit can wake it next.
            tokio::time::timeout(Duration::from_secs(10), runtime.idle_entered.notified())
                .await
                .unwrap();
            assert_eq!(emit_runtime.run_once(1).await.unwrap(), 1);
        } else {
            let mut transaction = db::begin_immediate(db.pool()).await.unwrap();
            db.append_event_in_tx(&mut transaction, &input("emitted", "from-new-path"))
                .await
                .unwrap();
            transaction.commit().await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), receiver.entered.notified())
            .await
            .expect("connection hook wakes inside maximum 5-second idle backoff");
        stop(tx, handle).await;
    }
}

#[tokio::test]
async fn source_neutral_poison_policy_accepts_step_keys_without_event_cursor() {
    let db = database().await;
    let health = WorkerHealth::new(Arc::clone(&db), "steps");
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    let decision = health
        .failure_in_tx(
            &mut tx,
            WorkItem {
                source_key: "task/a/step/b",
                item_type: "step",
            },
            RetryPolicy {
                max_attempts: 1,
                ..RetryPolicy::default()
            },
            "step failed",
            false,
        )
        .await
        .unwrap();
    assert!(matches!(decision, PoisonDecision::DeadLettered));
    assert!(matches!(
        RetryPolicy::default().decision(8),
        PoisonDecision::DeadLettered
    ));
    // A leased source can supply its own accounting without touching the
    // serial event adapter's retry state.
    health
        .dead_letter_in_tx(
            &mut tx,
            WorkItem {
                source_key: "task/c/step/d",
                item_type: "step",
            },
            FailureState {
                attempts: 8,
                first_failed_at: "2000-01-01T00:00:00Z",
            },
            "leased step failed",
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let key: String =
        sqlx::query_scalar("SELECT source_key FROM worker_dead_letter ORDER BY source_key LIMIT 1")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(key, "task/a/step/b");
    assert_eq!(
        scalar(
            &db,
            "SELECT COUNT(*) FROM event_consumer_cursor WHERE consumer_name = 'steps'"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn supervisor_backoff_resets_after_a_healthy_period() {
    let db = database().await;
    let health = WorkerHealth::new(Arc::clone(&db), "reset");
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let backoff = Arc::new(Notify::new());
    let count = Arc::clone(&calls);
    let started = Arc::clone(&entered);
    let release = Arc::clone(&finish);
    let (tx, rx) = watch::channel(false);
    let handle = WorkerSupervisor::new(
        health,
        SupervisorPolicy {
            initial_backoff: Duration::from_secs(10),
            max_backoff: Duration::from_secs(60),
            healthy_period: Duration::from_secs(60),
        },
    )
    .with_backoff_signal(Arc::clone(&backoff))
    .start(
        move |_| {
            let count = Arc::clone(&count);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                let call = count.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                if call > 0 {
                    tokio::time::timeout(Duration::from_secs(3600), release.notified())
                        .await
                        .expect("test release");
                }
                Ok(())
            }
        },
        rx,
    );
    notified(&entered).await;
    notified(&backoff).await;
    // Pause only the pure timer/notification portion. SQLite persistence runs
    // with real time so SQLx acquisition timers cannot auto-advance during IO.
    tokio::time::pause();
    notified(&entered).await;
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::time::resume();
    finish.notify_one();
    notified(&backoff).await;
    tokio::time::pause();
    let waiting_since = Instant::now();
    notified(&entered).await;
    // Reset waits ten seconds; an accumulated restart backoff would be twenty.
    assert!(waiting_since.elapsed() < Duration::from_secs(15));
    tokio::time::resume();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    stop(tx, handle).await;
}

#[tokio::test]
async fn worker_runtime_migration_removes_only_memory_delivery_metadata() {
    let db = database().await;
    let event = append(&db, "wanted", "legacy").await;
    for name in [crate::memory_consumer_name(), "unmigrated"] {
        legacy_delivery(&db, name).await;
        sqlx::query("INSERT INTO event_projection_receipt (consumer_name, event_id, dedupe_key, processed_at) VALUES (?, ?, ?, ?)")
            .bind(name).bind(&event.id).bind(&event.id).bind(db::now_rfc3339()).execute(db.pool()).await.unwrap();
    }
    let cursors_before: Vec<(String, i64, i64, String)> = sqlx::query_as(
        "SELECT consumer_name, last_sequence, version, updated_at FROM event_consumer_cursor ORDER BY consumer_name")
        .fetch_all(db.pool()).await.unwrap();
    // Replay ONLY this unreleased migration against the legacy seeded state.
    sqlx::raw_sql("DROP TABLE worker_health; DROP TABLE worker_dead_letter;")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../db/migrations/V202610012200__worker_runtime.sql"
    ))
    .execute(db.pool())
    .await
    .unwrap();
    for table in ["event_processing_lease", "event_projection_receipt"] {
        let query = format!("SELECT COUNT(*) FROM {table} WHERE consumer_name = ?");
        let memory: i64 = sqlx::query_scalar(&query)
            .bind(crate::memory_consumer_name())
            .fetch_one(db.pool())
            .await
            .unwrap();
        let unmigrated: i64 = sqlx::query_scalar(&query)
            .bind("unmigrated")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(memory, 0);
        assert_eq!(unmigrated, 1);
    }
    let cursors_after: Vec<(String, i64, i64, String)> = sqlx::query_as(
        "SELECT consumer_name, last_sequence, version, updated_at FROM event_consumer_cursor ORDER BY consumer_name")
        .fetch_all(db.pool()).await.unwrap();
    assert_eq!(cursors_after, cursors_before);
    assert!(db.get_event(&event.id).await.unwrap().is_some());
}

#[tokio::test]
async fn shutdown_aborts_a_hung_handle_and_drops_its_future() {
    let db = database().await;
    append(&db, "wanted", "hang").await;
    let worker = Arc::new(TinyWorker::new("abort"));
    let runtime = Arc::new(WorkerRuntime::new(db, Arc::clone(&worker)));
    let (tx, rx) = watch::channel(false);
    let handle = runtime.start(rx);
    tokio::time::timeout(Duration::from_secs(10), worker.entered.notified())
        .await
        .unwrap();
    assert_eq!(worker.active.load(Ordering::SeqCst), 1);
    stop(tx, handle).await;
    assert_eq!(worker.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn supervisor_shutdown_interrupts_restart_health_persistence() {
    let db = database().await;
    let health = WorkerHealth::new(Arc::clone(&db), "blocked-health");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let started = Arc::clone(&entered);
    let finish = Arc::clone(&release);
    let (tx, rx) = watch::channel(false);
    let handle = WorkerSupervisor::new(health, SupervisorPolicy::default()).start(
        move |_| {
            let started = Arc::clone(&started);
            let finish = Arc::clone(&finish);
            async move {
                started.notify_one();
                tokio::time::timeout(Duration::from_secs(30), finish.notified())
                    .await
                    .expect("test finish");
                Ok(())
            }
        },
        rx,
    );
    notified(&entered).await;
    let connection = db.pool().acquire().await.unwrap();
    release.notify_one();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    stop(tx, handle).await;
    drop(connection);
}

#[tokio::test]
async fn invalid_retry_timestamp_is_due_and_recovers_without_resetting_strikes() {
    let db = database().await;
    append(&db, "wanted", "poison").await;
    let worker = Arc::new(TinyWorker::new("timestamp"));
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker));
    assert_eq!(runtime.run_once(1).await.unwrap(), 0);
    sqlx::query(
        "UPDATE worker_health SET retry_not_before = 'invalid' WHERE worker_name = 'timestamp'",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let runtime = WorkerRuntime::new(Arc::clone(&db), worker);
    assert_eq!(runtime.run_once(1).await.unwrap(), 0);
    assert_eq!(
        scalar(
            &db,
            "SELECT retry_attempts FROM worker_health WHERE worker_name = 'timestamp'"
        )
        .await,
        2
    );
    let repaired: String = sqlx::query_scalar(
        "SELECT retry_not_before FROM worker_health WHERE worker_name = 'timestamp'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(DateTime::parse_from_rfc3339(&repaired).is_ok());
    due(&runtime).await;
    assert_eq!(runtime.run_once(1).await.unwrap(), 1);
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM worker_dead_letter").await,
        1
    );
}

#[path = "regressions.rs"]
mod regressions;

async fn legacy_delivery(db: &SqliteDb, name: &str) {
    let exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE name = 'event_processing_lease'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    if exists == 0 {
        sqlx::raw_sql(include_str!(
            "../../../db/tests/fixtures/event_delivery.sql"
        ))
        .execute(db.pool())
        .await
        .unwrap();
    }
    sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES (?, 0, ?) ON CONFLICT DO NOTHING").bind(name).bind(db::now_rfc3339()).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO event_processing_lease (consumer_name, event_sequence, lease_owner, leased_until, attempts, updated_at) SELECT ?, sequence, 'legacy', '2999-01-01T00:00:00Z', 1, ? FROM domain_event WHERE sequence > (SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = ?)")
        .bind(name).bind(db::now_rfc3339()).bind(name).execute(db.pool()).await.unwrap();
}

#[tokio::test]
async fn audit_timed_out_tick_is_reported_and_does_not_gate_events() {
    let db = database().await;
    let mut worker = TinyWorker::new("hung-tick");
    worker.tick_hangs.store(true, Ordering::SeqCst);
    worker.tick_timeout = Duration::from_millis(10);
    let event = append(&db, "wanted", "good").await;
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::new(worker));
    assert_eq!(runtime.run_once(10).await.unwrap(), 1);
    assert_eq!(effects(&db).await, vec![event.sequence]);
    let error: Option<String> =
        sqlx::query_scalar("SELECT tick_error FROM worker_health WHERE worker_name = 'hung-tick'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(error.as_deref(), Some("worker tick timed out"));
}

#[tokio::test]
async fn audit2_terminal_commit_refreshes_preparation_once_before_quarantine() {
    for (skip, always_terminal) in [(false, false), (true, false), (false, true)] {
        let db = database().await;
        let mut worker = TinyWorker::new("fresh-terminal");
        worker.commit_terminal_once.store(true, Ordering::SeqCst);
        worker.skip_on_refresh = skip;
        worker.commit_terminal_always = always_terminal;
        let worker = Arc::new(worker);
        let event = append(&db, "wanted", "good").await;
        let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker));
        runtime.run_once(10).await.unwrap();
        assert_eq!(
            worker.seen.lock().unwrap().as_slice(),
            &[event.sequence, event.sequence]
        );
        assert_eq!(
            effects(&db).await,
            if skip || always_terminal {
                vec![]
            } else {
                vec![event.sequence]
            }
        );
        let count = scalar(&db, "SELECT COUNT(*) FROM worker_dead_letter").await;
        assert_eq!(count, i64::from(always_terminal));
        assert_eq!(
            scalar(
                &db,
                "SELECT retry_attempts FROM worker_health WHERE worker_name = 'fresh-terminal'"
            )
            .await,
            0
        );
    }
}

#[tokio::test]
async fn audit2_hung_tick_does_not_repeat_during_failure_backoff() {
    let db = database().await;
    let mut worker = TinyWorker::new("tick-backoff");
    worker.tick_hangs.store(true, Ordering::SeqCst);
    worker.tick_timeout = Duration::from_millis(10);
    let worker = Arc::new(worker);
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker));
    // Exercise the long failure backoff without spending minutes in test sleeps.
    runtime.tick_schedule.lock().unwrap().failures = 9;
    let mut wanted = Vec::new();
    for n in 0..20 {
        wanted.push(append(&db, "wanted", &format!("good-{n}")).await.sequence);
        runtime.run_once(1).await.unwrap();
    }
    assert_eq!(effects(&db).await, wanted);
    assert_eq!(
        worker.ticks.load(Ordering::SeqCst),
        1,
        "only one tick timeout across twenty event cycles"
    );
    let mut schedule = Schedule::default();
    for seconds in [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300] {
        assert_eq!(
            schedule.failed_with_cap(MAX_TICK_RETRY),
            Duration::from_secs(seconds)
        );
    }
    schedule.reset();
    assert!(schedule.ready());
}
