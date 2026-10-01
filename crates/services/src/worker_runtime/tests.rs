use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

use db::{create_sqlite_pool, new_uuid_v4, run_migrations, CreateDomainEvent, DomainEventRepo};

use super::*;

struct TinyWorker {
    name: &'static str,
    seen: Mutex<Vec<i64>>,
    commit_failures: AtomicUsize,
    retry_entity: Option<&'static str>,
    panic_once: AtomicBool,
    always_panic: bool,
}

impl TinyWorker {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            seen: Mutex::new(Vec::new()),
            commit_failures: AtomicUsize::new(0),
            retry_entity: None,
            panic_once: AtomicBool::new(false),
            always_panic: false,
        }
    }

    fn with_commit_failure(mut self) -> Self {
        self.commit_failures = AtomicUsize::new(1);
        self
    }

    fn retrying(mut self, entity: &'static str) -> Self {
        self.retry_entity = Some(entity);
        self
    }

    fn panics_once(mut self) -> Self {
        self.panic_once = AtomicBool::new(true);
        self
    }

    fn always_panics(mut self) -> Self {
        self.always_panic = true;
        self
    }
}

#[async_trait]
impl Worker for TinyWorker {
    type Prepared = i64;

    fn name(&self) -> &str {
        self.name
    }

    fn event_types(&self) -> &'static [&'static str] {
        &["wanted"]
    }

    async fn handle(
        &self,
        event: &DomainEvent,
    ) -> std::result::Result<WorkerOutcome<Self::Prepared>, WorkerError> {
        self.seen.lock().unwrap().push(event.sequence);
        assert_ne!(event.event_type, "ignored");
        assert!(!event.payload_json.contains("secret"));
        if self.always_panic || self.panic_once.swap(false, Ordering::SeqCst) {
            panic!("test worker panic");
        }
        if self.retry_entity == Some(event.entity_id.as_str()) {
            return Ok(WorkerOutcome::RetryAfter(Duration::from_millis(1)));
        }
        Ok(WorkerOutcome::Done(event.sequence))
    }

    async fn commit(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        _event: &DomainEvent,
        prepared: &Self::Prepared,
    ) -> std::result::Result<(), WorkerError> {
        sqlx::query("INSERT INTO worker_test_effect (sequence) VALUES (?)")
            .bind(prepared)
            .execute(&mut **transaction)
            .await
            .map_err(|error| WorkerError::new(format!("test effect write failed: {error}")))?;
        if self
            .commit_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(WorkerError::new("failure after effect write"));
        }
        Ok(())
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

async fn append_event(
    db: &SqliteDb,
    event_type: &str,
    entity_id: &str,
    created_at: &str,
) -> DomainEvent {
    let id = new_uuid_v4();
    DomainEventRepo::append_event(
        db,
        CreateDomainEvent {
            id: id.clone(),
            event_type: event_type.to_owned(),
            entity_type: "test".to_owned(),
            entity_id: entity_id.to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "system".to_owned(),
            scope_id: "worker-runtime-test".to_owned(),
            correlation_id: id,
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: if event_type == "ignored" {
                r#"{"marker":"secret payload never reaches ignored handlers"}"#.to_owned()
            } else {
                "{}".to_owned()
            },
            created_at: created_at.to_owned(),
        },
    )
    .await
    .unwrap()
}

fn fast_options(max_attempts: u32) -> WorkerRuntimeOptions {
    WorkerRuntimeOptions {
        max_attempts,
        min_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(5),
    }
}

#[tokio::test]
async fn effect_and_cursor_commit_together() {
    let db = database().await;
    let event = append_event(&db, "wanted", "atomic", "2026-10-01T21:00:00Z").await;
    let worker = Arc::new(TinyWorker::new("atomic-worker").with_commit_failure());
    let runtime = WorkerRuntime::new(Arc::clone(&db), worker).with_options(fast_options(3));

    assert_eq!(runtime.run_once(1).await.unwrap(), 0);
    let effects: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_test_effect")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let cursor: i64 = sqlx::query_scalar(
        "SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = 'atomic-worker'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(effects, 0);
    assert_eq!(cursor, 0);

    tokio::time::sleep(Duration::from_millis(3)).await;
    assert_eq!(runtime.run_once(1).await.unwrap(), 1);
    let stored: i64 = sqlx::query_scalar("SELECT sequence FROM worker_test_effect")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let cursor: i64 = sqlx::query_scalar(
        "SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = 'atomic-worker'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(stored, event.sequence);
    assert_eq!(cursor, event.sequence);
}

#[tokio::test]
async fn strict_order_filters_in_sql_and_skips_without_delivery_rows() {
    let db = database().await;
    for index in 0..25 {
        append_event(
            &db,
            "ignored",
            &format!("ignored-{index}"),
            "2026-10-01T21:00:00Z",
        )
        .await;
    }
    let first = append_event(&db, "wanted", "first", "2026-10-01T21:01:00Z").await;
    for index in 0..25 {
        append_event(
            &db,
            "ignored",
            &format!("middle-{index}"),
            "2026-10-01T21:02:00Z",
        )
        .await;
    }
    let second = append_event(&db, "wanted", "second", "2026-10-01T21:03:00Z").await;
    let tail = append_event(&db, "ignored", "tail", "2026-10-01T21:04:00Z").await;
    let worker = Arc::new(TinyWorker::new("filtered-worker"));
    let runtime = WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker));

    assert_eq!(runtime.run_once(10).await.unwrap(), 3);
    assert_eq!(
        *worker.seen.lock().unwrap(),
        [first.sequence, second.sequence]
    );
    let cursor: i64 = sqlx::query_scalar(
        "SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = 'filtered-worker'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(cursor, tail.sequence);
    for table in ["event_processing_lease", "event_projection_receipt"] {
        let query = format!("SELECT COUNT(*) FROM {table} WHERE consumer_name = ?");
        let rows: i64 = sqlx::query_scalar(&query)
            .bind("filtered-worker")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(rows, 0, "{table} must stay empty");
    }
}

#[tokio::test]
async fn retry_cap_dead_letters_once_then_processes_next_event() {
    let db = database().await;
    let first = append_event(&db, "wanted", "retry", "2026-10-01T21:00:00Z").await;
    let second = append_event(&db, "wanted", "next", "2026-10-01T21:01:00Z").await;
    let worker = Arc::new(TinyWorker::new("retry-worker").retrying("retry"));
    let runtime =
        WorkerRuntime::new(Arc::clone(&db), Arc::clone(&worker)).with_options(fast_options(2));

    assert_eq!(runtime.run_once(10).await.unwrap(), 0);
    tokio::time::sleep(Duration::from_millis(3)).await;
    assert_eq!(runtime.run_once(10).await.unwrap(), 2);

    let dead_letter: (i64, i64, String) = sqlx::query_as(
        "SELECT event_sequence, attempts, event_type FROM worker_dead_letter
         WHERE worker_name = 'retry-worker'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(dead_letter, (first.sequence, 2, "wanted".to_owned()));
    let cursor: i64 = sqlx::query_scalar(
        "SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = 'retry-worker'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(cursor, second.sequence);
}

#[tokio::test]
async fn health_error_persists_until_success_and_backlog_is_exact() {
    let db = database().await;
    let first = append_event(&db, "wanted", "first", "2026-10-01T20:00:00Z").await;
    append_event(&db, "ignored", "ignored", "2026-10-01T20:01:00Z").await;
    let second = append_event(&db, "wanted", "second", "2026-10-01T20:02:00Z").await;
    let worker = Arc::new(TinyWorker::new("health-worker").with_commit_failure());
    let runtime = WorkerRuntime::new(Arc::clone(&db), worker).with_options(fast_options(3));

    assert_eq!(runtime.run_once(1).await.unwrap(), 0);
    let failed: (i64, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT lag, oldest_pending_at, last_error FROM worker_health
         WHERE worker_name = 'health-worker'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(failed.0, 2);
    assert_eq!(failed.1.as_deref(), Some("2026-10-01T20:00:00Z"));
    assert!(failed.2.is_some());

    let operator = db
        .domain_event_consumer_lag(&["health-worker"])
        .await
        .unwrap();
    assert_eq!(operator[0].lag, 2);
    assert_eq!(operator[0].oldest_unprocessed_at, failed.1);

    tokio::time::sleep(Duration::from_millis(3)).await;
    assert_eq!(runtime.run_once(2).await.unwrap(), 2);
    let healthy: (i64, Option<String>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT lag, oldest_pending_at, last_error, last_success_at
         FROM worker_health WHERE worker_name = 'health-worker'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(healthy.0, 0);
    assert!(healthy.1.is_none());
    assert!(healthy.2.is_none());
    assert!(healthy.3.is_some());
    let effects: Vec<i64> =
        sqlx::query_scalar("SELECT sequence FROM worker_test_effect ORDER BY sequence")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(effects, [first.sequence, second.sequence]);
}

#[tokio::test]
async fn panic_is_restarted_and_shutdown_during_backoff_is_prompt() {
    let db = database().await;
    let event = append_event(&db, "wanted", "panic", "2026-10-01T21:00:00Z").await;
    let worker = Arc::new(TinyWorker::new("panic-worker").panics_once());
    let runtime =
        Arc::new(WorkerRuntime::new(Arc::clone(&db), worker).with_options(fast_options(3)));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = Arc::clone(&runtime).start(shutdown_rx);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = sqlx::query_as::<_, (i64, i64)>(
                "SELECT restart_count, cursor_sequence FROM worker_health
                 WHERE worker_name = 'panic-worker'",
            )
            .fetch_optional(db.pool())
            .await
            .unwrap();
            if state.is_some_and(|(restarts, cursor)| restarts >= 1 && cursor == event.sequence) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown_tx.send_replace(true);
    tokio::time::timeout(Duration::from_millis(100), handle)
        .await
        .expect("supervised worker stops promptly")
        .unwrap();

    let backoff_worker = Arc::new(TinyWorker::new("backoff-worker").always_panics());
    let backoff_runtime = Arc::new(
        WorkerRuntime::new(Arc::clone(&db), backoff_worker).with_options(WorkerRuntimeOptions {
            max_attempts: 3,
            min_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(5),
        }),
    );
    append_event(&db, "wanted", "backoff", "2026-10-01T21:01:00Z").await;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = backoff_runtime.start(shutdown_rx);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let restarts = sqlx::query_scalar::<_, i64>(
                "SELECT restart_count FROM worker_health WHERE worker_name = 'backoff-worker'",
            )
            .fetch_optional(db.pool())
            .await
            .unwrap()
            .unwrap_or(0);
            if restarts >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown_tx.send_replace(true);
    tokio::time::timeout(Duration::from_millis(100), handle)
        .await
        .expect("shutdown interrupts restart backoff")
        .unwrap();
}

#[tokio::test]
async fn caught_up_idle_poll_opens_no_write_transaction() {
    let db = database().await;
    let worker = Arc::new(TinyWorker::new("idle-worker"));
    let runtime = WorkerRuntime::new(db, worker);
    runtime.initialize().await.unwrap();
    let before = runtime.write_transaction_count();
    assert!(matches!(
        runtime.poll_once().await.unwrap(),
        PollResult::Idle
    ));
    assert_eq!(runtime.write_transaction_count(), before);
}
